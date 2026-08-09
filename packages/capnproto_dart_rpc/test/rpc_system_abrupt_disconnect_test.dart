// Issue #86: abrupt TCP/WebSocket disconnect lifecycle cleanup.
//
// These tests drive real dart:io sockets end to end -- they're not the
// deterministic in-memory pipe used throughout rpc_test.dart -- so every
// synchronization point below is event-driven (a Completer, a
// DispatchCancellationContext, a retried round trip) rather than a blind
// `Future.delayed`, and every abrupt disconnect is a genuine
// `socket.destroy()` (or, for WebSocket, a raw TCP-level severance during
// the handshake), never a graceful `close()` standing in for one.
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:capnproto_dart_rpc/capnproto_dart_rpc.dart';
import 'package:capnproto_dart_rpc/src/rpc/two_party_connection.dart'
    show TwoPartyRpcConnection;
import 'package:test/test.dart';

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// Adapts a raw [WebSocket] to [StreamSink<Uint8List>] -- same shape as
// rpc_system.dart's own private _WebSocketSink, needed here because
// [WebSocket] itself implements StreamSink<dynamic>, not
// StreamSink<Uint8List>.
class _RawWebSocketSink implements StreamSink<Uint8List> {
  final WebSocket _ws;
  _RawWebSocketSink(this._ws);

  @override
  void add(Uint8List data) => _ws.add(data);

  @override
  void addError(Object error, [StackTrace? stackTrace]) =>
      _ws.addError(error, stackTrace);

  @override
  Future<void> addStream(Stream<Uint8List> stream) => _ws.addStream(stream);

  @override
  Future<void> close() => _ws.close();

  @override
  Future<void> get done => _ws.done;
}

// Adapts a raw [Socket] to [StreamSink<Uint8List>] -- same shape as
// rpc_system.dart's own private _SocketSink, needed here because a test
// that wants to `.destroy()` the raw socket itself must build the
// [TwoPartyRpcConnection] directly rather than through RpcSystem.connect().
class _RawSocketSink implements StreamSink<Uint8List> {
  final IOSink _sink;
  _RawSocketSink(this._sink);

  @override
  void add(Uint8List data) => _sink.add(data);

  @override
  void addError(Object error, [StackTrace? stackTrace]) =>
      _sink.addError(error, stackTrace);

  @override
  Future<void> addStream(Stream<Uint8List> stream) => _sink.addStream(stream);

  @override
  Future<void> close() => _sink.close();

  @override
  Future<void> get done => _sink.done;
}

class _RawCapabilityFactory extends CapabilityFactory<Capability> {
  @override
  Capability fromCapability(Capability cap) => cap;
}

// A bootstrap whose dispatch() blocks until release() is called -- lets a
// test observe server-side teardown (RpcServer.close(), an abrupt
// disconnect) while a real dispatch is genuinely still in flight, instead
// of only ever tearing down an idle connection. Tracks the DispatchCancellationContext
// it's given so a test can wait for the server's own peer-loss detection
// (context.canceled) as an independent signal, distinct from -- and prior
// to -- any explicit RpcServer.close() the test itself calls afterward.
class _SlowCountingBootstrap extends Capability {
  final Completer<void> started = Completer<void>();
  final Completer<void> release = Completer<void>();
  int disposeCount = 0;
  DispatchCancellationContext? lastContext;

  @override
  Future<DispatchResult> dispatchWithContext(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
    DispatchCancellationContext? context,
  }) async {
    lastContext = context ?? DispatchCancellationContext.neverCanceled;
    if (!started.isCompleted) started.complete();
    await release.future;
    return DispatchResult(payload: RpcPayload.fromBytes(_emptyParams));
  }

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) => dispatchWithContext(
    interfaceId,
    methodId,
    params,
    paramsCapabilities: paramsCapabilities,
  );

  @override
  Future<void> dispose() async {
    disposeCount++;
  }
}

// Minimal validly-framed message (1 segment, 1 word, null root pointer) —
// enough to get past decoding so the call reaches a bootstrap's dispatch(),
// which is what's actually under test here.
final _emptyParams = Uint8List.fromList([
  0, 0, 0, 0, //
  1, 0, 0, 0, //
  0, 0, 0, 0, //
  0, 0, 0, 0, //
]);

// ---------------------------------------------------------------------------
// Connection helpers
// ---------------------------------------------------------------------------

/// Connects a real TCP socket to [port] and wraps it directly in a
/// [TwoPartyRpcConnection], returning both. Every test here needs the raw
/// [Socket] itself, not just the connection, to call `.destroy()` on for a
/// true transport-level severance -- something [RpcSystem.connect] never
/// exposes a handle for.
Future<({TwoPartyRpcConnection connection, Socket socket})>
_connectRawTcpClient(int port) async {
  final socket = await Socket.connect('127.0.0.1', port);
  final connection = TwoPartyRpcConnection.client(
    incoming: socket.cast<Uint8List>(),
    outgoing: _RawSocketSink(socket),
  );
  return (connection: connection, socket: socket);
}

/// Hand-rolls the WebSocket upgrade handshake (mirroring what
/// [WebSocket.connect] does internally) so the caller keeps its own handle
/// to the raw [Socket] underneath -- [RpcSystem.connect]'s own
/// [WebSocket.connect] call never exposes it, and a graceful
/// [WebSocket.close] only ever sends a Close frame, never the true
/// TCP-level severance an abrupt-disconnect test needs.
///
/// The returned [HttpClient] is the caller's to close (e.g. via
/// `addTearDown`) -- it can't be closed here, since closing it before the
/// handshake completes would tear down the very socket this helper just
/// detached from it. On any failure partway through the handshake, this
/// closes the client itself before rethrowing, so a caller never has to
/// clean up after a failed attempt.
Future<
  ({TwoPartyRpcConnection connection, Socket socket, WebSocket ws, HttpClient httpClient})
>
_connectAbruptWebSocketClient(int port) async {
  final httpClient = HttpClient();
  try {
    final nonce = base64Encode(
      List<int>.generate(16, (_) => Random().nextInt(256)),
    );
    final request = await httpClient.openUrl(
      'GET',
      Uri.parse('http://127.0.0.1:$port/'),
    );
    request.headers
      ..set(HttpHeaders.connectionHeader, 'Upgrade')
      ..set(HttpHeaders.upgradeHeader, 'websocket')
      ..set('Sec-WebSocket-Key', nonce)
      ..set('Sec-WebSocket-Version', '13');
    final response = await request.close();
    if (response.statusCode != HttpStatus.switchingProtocols) {
      throw StateError(
        'expected a WebSocket upgrade, got ${response.statusCode}',
      );
    }
    final socket = await response.detachSocket();
    final ws = WebSocket.fromUpgradedSocket(socket, serverSide: false);
    final connection = TwoPartyRpcConnection.client(
      incoming: ws.map(
        (d) => d is Uint8List ? d : Uint8List.fromList(d as List<int>),
      ),
      outgoing: _RawWebSocketSink(ws),
      preFramed: true,
    );
    return (
      connection: connection,
      socket: socket,
      ws: ws,
      httpClient: httpClient,
    );
  } catch (_) {
    httpClient.close(force: true);
    rethrow;
  }
}

/// Retries [attempt] with a short backoff until it succeeds or [timeout]
/// elapses -- used to probe that a `maxConnections`-capped server's
/// registry actually dropped an abruptly-disconnected connection. That
/// removal is not synchronous with the peer-loss signal a test can
/// otherwise observe (a dispatch's `DispatchCancellationContext.canceled`
/// fires from `AnswerTable.tearDown`, a step earlier in
/// `TwoPartyRpcConnection._tearDown` than the registry's own removal, which
/// runs off `conn.done` completing at the very end of teardown) -- so a
/// single immediate attempt right after observing cancellation would be a
/// race, not a proof.
Future<T> _retryUntilSuccess<T>(
  Future<T> Function() attempt, {
  Duration timeout = const Duration(seconds: 5),
}) async {
  final deadline = DateTime.now().add(timeout);
  Object? lastError;
  while (DateTime.now().isBefore(deadline)) {
    try {
      return await attempt();
    } catch (e) {
      lastError = e;
      await Future<void>.delayed(const Duration(milliseconds: 20));
    }
  }
  throw StateError('timed out retrying (last error: $lastError)');
}

void main() {
  group('RpcSystem abrupt disconnect lifecycle cleanup (issue #86)', () {
    group('TCP', () {
      test('server.close() disposes the bootstrap even while a dispatch through '
          'it is still genuinely in flight, and the in-flight call itself '
          'still fails once released', () async {
        final bootstrap = _SlowCountingBootstrap();
        final server = await RpcSystem.serve(
          Uri.parse('tcp://127.0.0.1:0'),
          bootstrap,
        );
        final client = await RpcSystem.connect(
          Uri.parse('tcp://127.0.0.1:${server.port}'),
        );
        addTearDown(client.close);

        final callFuture = client
            .bootstrap(_RawCapabilityFactory())
            .dispatch(0, 0, RpcPayload.fromBytes(_emptyParams));
        callFuture.ignore();
        await bootstrap.started.future.timeout(const Duration(seconds: 2));

        // _tearDown never awaits a still-running dispatch itself -- the
        // server-lifetime bootstrap lease is released regardless, racing
        // ahead of _SlowCountingBootstrap's own dispatch() (still blocked
        // on `release`) actually returning.
        await server.close();
        expect(bootstrap.disposeCount, equals(1));

        bootstrap.release.complete();
        await expectLater(callFuture, throwsA(isA<RpcException>()));
      });

      test(
        'an abrupt TCP disconnect (socket.destroy(), not a graceful close) '
        'on a connection with a genuinely in-flight dispatch fails the '
        'caller, tears down cleanly, and frees the registry slot for a '
        'replacement connection',
        () async {
          final bootstrap = _SlowCountingBootstrap();
          final unhandledErrors = <Object>[];

          await runZonedGuarded(() async {
            final server = await RpcSystem.serve(
              Uri.parse('tcp://127.0.0.1:0'),
              bootstrap,
              maxConnections: 1,
            );
            addTearDown(server.close);

            final (:connection, :socket) = await _connectRawTcpClient(
              server.port,
            );

            final callFuture = connection
                .bootstrap(_RawCapabilityFactory())
                .dispatch(0, 0, RpcPayload.fromBytes(_emptyParams));
            callFuture.ignore();
            await bootstrap.started.future.timeout(const Duration(seconds: 2));

            // Immediate transport destruction without a graceful
            // application-level close (unlike TwoPartyRpcConnection.close(),
            // which closes the outgoing sink cleanly).
            socket.destroy();

            // The caller's own in-flight call must observe the disconnect
            // too, not just the server side.
            await expectLater(
              callFuture,
              throwsA(
                isA<RpcException>().having(
                  (error) => error.kind,
                  'kind',
                  ErrorKind.disconnected,
                ),
              ),
            );
            await connection.done.catchError((_) {});

            // Wait for the SERVER's own peer-loss detection specifically --
            // not for server.close() below, which would force every tracked
            // connection closed regardless of whether the abrupt disconnect
            // was ever actually noticed. Proves teardown here is driven by
            // the dead transport itself, not merely by the explicit
            // shutdown call that follows.
            await bootstrap.lastContext!.canceled.timeout(
              const Duration(seconds: 5),
            );
            expect(bootstrap.lastContext!.isCanceled, isTrue);

            // Unblocks _SlowCountingBootstrap's single, shared gate so the
            // registry-cleanup probe below (a real dispatch through this
            // same bootstrap on a fresh connection) can actually resolve --
            // proving the registry dropped the destroyed connection
            // requires a genuine round trip, which would otherwise stay
            // blocked on this same gate forever. Completing it here (rather
            // than only at the very end, once every assertion has already
            // run) also exercises this dispatch's own late completion
            // racing the now-finished teardown of the connection it
            // belonged to -- which must not resurrect anything or attempt
            // to send on the dead socket.
            bootstrap.release.complete();

            // maxConnections: 1 means a second connection can only be
            // accepted if the registry actually dropped the destroyed one
            // -- but TCP's OS-level accept completes before the server's
            // own capacity check ever runs, so a bare connect() succeeding
            // would not by itself prove anything (see
            // rpc_system_test.dart's own maxConnections-cap test for the
            // same asymmetry). Retrying a full connect-then-dispatch round
            // trip until it succeeds is the only reliable proof here.
            final replacement = await _retryUntilSuccess(() async {
              final probe = await _connectRawTcpClient(server.port);
              try {
                await probe.connection
                    .bootstrap(_RawCapabilityFactory())
                    .dispatch(0, 0, RpcPayload.fromBytes(_emptyParams))
                    .timeout(const Duration(seconds: 1));
                return probe;
              } catch (_) {
                await probe.connection.close();
                rethrow;
              }
            });
            addTearDown(replacement.connection.close);

            await server.close().timeout(const Duration(seconds: 5));
            expect(bootstrap.disposeCount, equals(1));
          }, (error, stackTrace) => unhandledErrors.add(error));

          expect(
            unhandledErrors,
            isEmpty,
            reason:
                'expected no unhandled top-level errors from the abrupt '
                'disconnect, got: $unhandledErrors',
          );
        },
      );
    });

    group('WebSocket', () {
      test(
        'an abrupt TCP-level disconnect underneath an active WebSocket '
        'connection (not a graceful WebSocket close) fails the caller, '
        'tears down cleanly, and frees the registry slot for a replacement '
        'connection',
        () async {
          final bootstrap = _SlowCountingBootstrap();
          final server = await RpcSystem.serve(
            Uri.parse('ws://127.0.0.1:0'),
            bootstrap,
            maxConnections: 1,
          );

          final (:connection, :socket, :ws, :httpClient) =
              await _connectAbruptWebSocketClient(server.port);
          addTearDown(() => ws.close());
          addTearDown(() => httpClient.close(force: true));

          final callFuture = connection
              .bootstrap(_RawCapabilityFactory())
              .dispatch(0, 0, RpcPayload.fromBytes(_emptyParams));
          callFuture.ignore();
          await bootstrap.started.future.timeout(const Duration(seconds: 2));

          // True TCP-level severance, bypassing the WebSocket close
          // handshake entirely (unlike WebSocket.close(), which only ever
          // sends a Close frame).
          socket.destroy();

          // The caller's own in-flight call must observe the disconnect
          // too, not just the server side -- otherwise a broken
          // client-side teardown path could hide behind the server ever
          // getting torn down at all.
          await expectLater(
            callFuture,
            throwsA(
              isA<RpcException>().having(
                (error) => error.kind,
                'kind',
                ErrorKind.disconnected,
              ),
            ),
          );
          await connection.done.catchError((_) {});

          // Wait for the SERVER's own peer-loss detection specifically --
          // not for server.close() below, which would force every tracked
          // connection closed regardless of whether the abrupt disconnect
          // was ever actually noticed. Proves teardown here is driven by
          // the dead transport itself, not merely by the explicit shutdown
          // call that follows.
          await bootstrap.lastContext!.canceled.timeout(
            const Duration(seconds: 5),
          );
          expect(bootstrap.lastContext!.isCanceled, isTrue);

          // Unlike the TCP variant of this test, WebSocket's capacity
          // check runs before the upgrade response is ever sent -- so, in
          // contrast to TCP (whose OS-level accept completes before the
          // app-level capacity check runs at all), a reconnect succeeding
          // is already definitive proof the registry dropped the
          // abruptly-disconnected connection; no dispatch round trip (and
          // so no need to unblock `release` early) is needed here.
          final replacement = await _retryUntilSuccess(
            () => RpcSystem.connect(Uri.parse('ws://127.0.0.1:${server.port}')),
          );
          await replacement.close();

          await server.close().timeout(const Duration(seconds: 5));
          expect(bootstrap.disposeCount, equals(1));

          bootstrap.release.complete();
        },
      );
    });
  });
}
