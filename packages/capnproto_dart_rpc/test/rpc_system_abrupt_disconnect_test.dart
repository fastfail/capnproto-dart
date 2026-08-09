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

// A minimal StructFactory for a struct with 0 dataWords and 1 ptrWord --
// just enough to hold a single returned capability at ptr slot 0 (same
// shape as rpc_test.dart's own _TextParamFactory, duplicated locally per
// this file's existing no-shared-test-helpers convention).
final class _CapResultFactory
    extends StructFactory<_CapResultReader, _CapResultBuilder> {
  @override
  int get dataWords => 0;
  @override
  int get ptrWords => 1;
  @override
  _CapResultReader fromRawReader(RawStructReader r) => _CapResultReader(r);
  @override
  _CapResultBuilder fromRawBuilder(RawStructBuilder r) =>
      _CapResultBuilder(r);
}

class _CapResultReader extends StructReader {
  _CapResultReader(super.raw);
}

class _CapResultBuilder extends StructBuilder {
  _CapResultBuilder(super.raw);
  @override
  StructReader asReader() => throw UnimplementedError();
}

// A capability whose dispatch() blocks until release() is called, exactly
// like _SlowCountingBootstrap's own gate -- a simpler, single-purpose
// sibling for tests that just need "a call is genuinely still in flight"
// (as a bootstrap, a tail-forward redirect target, or a pipelining child)
// without also needing dispose-count tracking.
class _GatedCapability extends Capability {
  final Completer<void> started = Completer<void>();
  final Completer<void> release = Completer<void>();
  // Completes once this dispatch has actually settled (i.e. dispatch()'s
  // own Future has resolved) after `release` -- lets a test that completes
  // `release` well after teardown prove the *coordinator's* own
  // continuation over that late-resolving Future runs cleanly too, not
  // just that this method returned without throwing.
  final Completer<void> finished = Completer<void>();
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
    if (!finished.isCompleted) finished.complete();
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
  Future<void> dispose() async {}
}

// A capability whose dispatch() blocks until release() is called, like
// _GatedCapability, but tracks how many separate dispatches have reached
// it -- via waitForStarted(n), which completes once that count has
// reached (not just once, ever) n. _GatedCapability's own `started`
// completes on the *first* dispatch only, which proves "at least one
// call arrived" but not "n calls are all genuinely in flight
// simultaneously" or, across a loop reusing the same bootstrap instance,
// "cycle n's specific call has arrived" -- both of which this exists for.
class _CountingGatedBootstrap extends Capability {
  final Completer<void> release = Completer<void>();
  int disposeCount = 0;
  int startedCount = 0;
  final StreamController<int> _startedCountChanges =
      StreamController<int>.broadcast();

  Future<void> waitForStarted(int n) {
    if (startedCount >= n) return Future<void>.value();
    return _startedCountChanges.stream
        .firstWhere((count) => count >= n)
        .then((_) {});
  }

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    startedCount++;
    _startedCountChanges.add(startedCount);
    await release.future;
    return DispatchResult(payload: RpcPayload.fromBytes(_emptyParams));
  }

  @override
  Future<void> dispose() async {
    disposeCount++;
  }
}

// A capability that, once released (immediately unless [gated]), returns
// [child] as caps[0] -- used both for import-refcount scenarios (ungated:
// the call resolves immediately) and for wire-level promise-pipelining
// scenarios (gated: the parent dispatch, and so the child's identity, stays
// unresolved until the test releases it).
class _ReturningCapability extends Capability {
  final Capability child;
  final Completer<void> started = Completer<void>();
  final Completer<void> release;

  _ReturningCapability({required this.child, bool gated = false})
    : release = gated ? Completer<void>() : (Completer<void>()..complete());

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    if (!started.isCompleted) started.complete();
    await release.future;
    final mb = MessageBuilder();
    final root = mb.initRoot(_CapResultFactory());
    root.setCapabilityField(0, 0);
    return DispatchResult(payload: RpcPayload.fromBuilder(root), caps: [child]);
  }

  @override
  Future<void> dispose() async {}
}

// A capability that records and retains whatever capability arrives as
// paramsCapabilities[0], without disposing it -- gives a connection's
// export table a live entry (the caller's argument) that outlives the call
// itself, for export-refcount scenarios.
class _ParamRetainingCapability extends Capability {
  Capability? lastAccepted;

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    lastAccepted = paramsCapabilities.isEmpty ? null : paramsCapabilities[0];
    return DispatchResult(payload: RpcPayload.fromBytes(_emptyParams));
  }

  @override
  Future<void> dispose() async {}
}

// A capability whose tryTailCall() unconditionally redirects to
// paramsCapabilities[0] -- the Level 1 tail-call wire optimization
// (Call.sendResultsTo=yourself / Return.takeFromOtherQuestion) applies
// whenever that target is itself hosted on the connection's peer, which is
// exactly the case every test using this fixture sets up (the target lives
// on the caller, this capability lives on the callee).
class _TailForwardCapability extends Capability {
  @override
  TailCallRequest? tryTailCall(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) {
    if (paramsCapabilities.isEmpty) return null;
    return TailCallRequest(paramsCapabilities[0], interfaceId, methodId, params);
  }

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) => Future.error(const RpcException('should have been tail-called'));

  @override
  Future<void> dispose() async {}
}

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

      test(
        'an abrupt TCP disconnect while an outgoing question is awaiting '
        'Return fails the caller with a disconnected error and clears '
        'debugPendingQuestionCount',
        () async {
          final bootstrap = _GatedCapability();
          final server = await RpcSystem.serve(
            Uri.parse('tcp://127.0.0.1:0'),
            bootstrap,
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

          socket.destroy();

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
          expect(connection.debugPendingQuestionCount, equals(0));

          bootstrap.release.complete();
        },
      );

      test(
        'an abrupt TCP disconnect while a promised-answer pipelined call is '
        'pending fails both the parent call and the pipelined call with '
        'disconnected errors',
        () async {
          final bootstrap = _ReturningCapability(
            gated: true,
            child: _GatedCapability(),
          );
          final server = await RpcSystem.serve(
            Uri.parse('tcp://127.0.0.1:0'),
            bootstrap,
          );
          addTearDown(server.close);

          final (:connection, :socket) = await _connectRawTcpClient(
            server.port,
          );

          final call = connection
              .bootstrap(_RawCapabilityFactory())
              .dispatchForPipelining(0, 0, RpcPayload.fromBytes(_emptyParams));
          call.result.ignore();
          final pipelinedCallFuture = call
              .pipelinedCapability(0)
              .dispatch(0, 0, RpcPayload.fromBytes(_emptyParams));
          pipelinedCallFuture.ignore();
          await bootstrap.started.future.timeout(const Duration(seconds: 2));

          socket.destroy();

          await expectLater(
            call.result,
            throwsA(
              isA<RpcException>().having(
                (error) => error.kind,
                'kind',
                ErrorKind.disconnected,
              ),
            ),
          );
          await expectLater(
            pipelinedCallFuture,
            throwsA(
              isA<RpcException>().having(
                (error) => error.kind,
                'kind',
                ErrorKind.disconnected,
              ),
            ),
          );
          await connection.done.catchError((_) {});
          expect(connection.debugPendingQuestionCount, equals(0));

          bootstrap.release.complete();
        },
      );

      test(
        'an abrupt TCP disconnect with a live imported capability releases '
        'it exactly once and rejects a late release attempt cleanly',
        () async {
          final bootstrap = _ReturningCapability(child: _GatedCapability());
          final server = await RpcSystem.serve(
            Uri.parse('tcp://127.0.0.1:0'),
            bootstrap,
          );
          addTearDown(server.close);

          final (:connection, :socket) = await _connectRawTcpClient(
            server.port,
          );

          final result = await connection
              .bootstrap(_RawCapabilityFactory())
              .dispatch(0, 0, RpcPayload.fromBytes(_emptyParams));
          // Deliberately never disposed before teardown -- that's the
          // live, non-zero import refcount this test is about. The count
          // is 2, not 1: the bootstrap capability itself is import id 0
          // (see two_party_connection.dart's own "no Bootstrap round trip
          // needed... import id 0" convention), alongside the one this
          // test explicitly cares about.
          final imported = requireCapabilityFromResult(result, 0);
          expect(connection.debugImportCount, equals(2));

          socket.destroy();
          await connection.done.catchError((_) {});

          expect(connection.debugImportCount, equals(0));

          // A dispose attempt arriving after teardown must resolve cleanly
          // -- never throw, never hang, never attempt to send on the dead
          // socket or otherwise resurrect the connection's import table.
          await imported.dispose();
        },
      );

      test(
        'an abrupt TCP disconnect with a live exported capability releases '
        'it exactly once',
        () async {
          final bootstrap = _ParamRetainingCapability();
          final server = await RpcSystem.serve(
            Uri.parse('tcp://127.0.0.1:0'),
            bootstrap,
          );
          addTearDown(server.close);

          final (:connection, :socket) = await _connectRawTcpClient(
            server.port,
          );
          final localCap = _GatedCapability();

          // Fully awaited so the bootstrap has genuinely retained
          // localCap (rather than it having already been released via
          // Return.releaseParamCaps) by the time the socket is destroyed.
          await connection
              .bootstrap(_RawCapabilityFactory())
              .dispatch(
                0,
                0,
                RpcPayload.fromBytes(_emptyParams),
                paramsCapabilities: [localCap],
              );
          expect(connection.debugExportCount, equals(1));

          socket.destroy();
          await connection.done.catchError((_) {});

          expect(connection.debugExportCount, equals(0));
        },
      );

      test(
        'an abrupt TCP disconnect immediately after import disposals are '
        'batched does not crash or hang once the already-scheduled Release '
        'flush later runs against the dead transport',
        () async {
          final bootstrap = _ReturningCapability(child: _GatedCapability());
          final server = await RpcSystem.serve(
            Uri.parse('tcp://127.0.0.1:0'),
            bootstrap,
          );
          addTearDown(server.close);

          final (:connection, :socket) = await _connectRawTcpClient(
            server.port,
          );

          // Three separate leases to the same underlying import (the
          // bootstrap always hands back the same `child`) -- disposing
          // all three without awaiting batches into a single Release
          // for that one import id, carrying referenceCount 3.
          final imports = <Capability>[];
          for (var i = 0; i < 3; i++) {
            final result = await connection
                .bootstrap(_RawCapabilityFactory())
                .dispatch(0, 0, RpcPayload.fromBytes(_emptyParams));
            imports.add(requireCapabilityFromResult(result, 0));
          }
          // 2, not 1: the bootstrap capability's own import (id 0)
          // alongside `child`'s single, shared import id.
          expect(connection.debugImportCount, equals(2));

          for (final cap in imports) {
            cap.dispose().ignore();
          }
          // Each dispose() only yields at its own already-resolved
          // `await _importIdFuture` (a pure microtask hop) before actually
          // batching the release -- so a single microtask turn (not a real
          // async gap, and specifically not Future.delayed, which would
          // also let the flush microtask those batches schedule run to
          // completion) is enough for all three to have batched, but not
          // yet for the flush itself -- which they schedule *during* that
          // same turn, so it lands strictly after this one on the
          // microtask queue -- to have run.
          await Future.microtask(() {});
          expect(connection.debugPendingReleaseCount, greaterThan(0));

          socket.destroy();

          await connection.done.catchError((_) {});
          expect(connection.debugPendingReleaseCount, equals(0));
          expect(connection.debugImportCount, equals(0));
          expect(connection.debugBrokenImportCount, equals(0));
        },
      );

      test(
        'an abrupt TCP disconnect while a tail-forwarded call is unresolved '
        'fails the original caller, cancels the forwarded dispatch, and '
        "clears the client's own answer/cancellation tables -- including "
        'once the forwarded dispatch actually finishes late',
        () async {
          final bootstrap = _TailForwardCapability();
          final unhandledErrors = <Object>[];

          await runZonedGuarded(() async {
            final server = await RpcSystem.serve(
              Uri.parse('tcp://127.0.0.1:0'),
              bootstrap,
            );
            addTearDown(server.close);

            final (:connection, :socket) = await _connectRawTcpClient(
              server.port,
            );
            // Hosted on the CLIENT and passed as a call argument -- the
            // server's bootstrap tail-calls back into it, making the
            // client itself the one genuinely answering an incoming call.
            final target = _GatedCapability();

            final callFuture = connection
                .bootstrap(_RawCapabilityFactory())
                .dispatch(
                  0,
                  0,
                  RpcPayload.fromBytes(_emptyParams),
                  paramsCapabilities: [target],
                );
            callFuture.ignore();

            await target.started.future.timeout(const Duration(seconds: 2));
            expect(connection.debugAnswerCount, equals(1));
            expect(connection.debugCancellationCount, equals(1));

            socket.destroy();

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
            await target.lastContext!.canceled.timeout(
              const Duration(seconds: 5),
            );
            expect(target.lastContext!.isCanceled, isTrue);

            await connection.done.catchError((_) {});
            expect(connection.debugAnswerCount, equals(0));
            expect(connection.debugCancellationCount, equals(0));

            // The forwarded dispatch itself keeps running in the
            // background regardless -- completing it late (after the
            // original caller has already moved on, and after teardown
            // already cleared the table above) must not resurrect an
            // AnswerTable/cancellation entry or attempt to send on the
            // dead socket. `target.finished` proves this fixture's own
            // dispatchWithContext() actually returned; the extra
            // microtask turn afterward lets IncomingCallCoordinator's own
            // continuation over that now-resolved Future run too, since
            // that -- not this method returning -- is where a "late
            // completion resurrects connection state" bug would actually
            // manifest.
            target.release.complete();
            await target.finished.future.timeout(const Duration(seconds: 2));
            await Future.microtask(() {});
            expect(connection.debugAnswerCount, equals(0));
            expect(connection.debugCancellationCount, equals(0));
          }, (error, stackTrace) => unhandledErrors.add(error));

          expect(
            unhandledErrors,
            isEmpty,
            reason:
                'expected no unhandled top-level errors from the late '
                'forwarded-dispatch completion, got: $unhandledErrors',
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

      test(
        'an abrupt TCP-level disconnect underneath an active WebSocket '
        'connection with a live imported capability releases it exactly '
        'once and rejects a late release attempt cleanly',
        () async {
          final bootstrap = _ReturningCapability(child: _GatedCapability());
          final server = await RpcSystem.serve(
            Uri.parse('ws://127.0.0.1:0'),
            bootstrap,
          );
          addTearDown(server.close);

          final (:connection, :socket, :ws, :httpClient) =
              await _connectAbruptWebSocketClient(server.port);
          addTearDown(() => ws.close());
          addTearDown(() => httpClient.close(force: true));

          final result = await connection
              .bootstrap(_RawCapabilityFactory())
              .dispatch(0, 0, RpcPayload.fromBytes(_emptyParams));
          // Deliberately never disposed before teardown -- that's the
          // live, non-zero import refcount this test is about. 2, not 1:
          // the bootstrap capability's own import (id 0) alongside the
          // one this test explicitly cares about (see the TCP variant of
          // this test for the same accounting).
          final imported = requireCapabilityFromResult(result, 0);
          expect(connection.debugImportCount, equals(2));

          socket.destroy();
          await connection.done.catchError((_) {});

          expect(connection.debugImportCount, equals(0));

          // A dispose attempt arriving after teardown must resolve cleanly
          // -- never throw, never hang, never attempt to send on the dead
          // socket or otherwise resurrect the connection's import table.
          await imported.dispose();
        },
      );

      test(
        'an abrupt TCP-level disconnect underneath an active WebSocket '
        'connection with a live exported capability releases it exactly '
        'once',
        () async {
          final bootstrap = _ParamRetainingCapability();
          final server = await RpcSystem.serve(
            Uri.parse('ws://127.0.0.1:0'),
            bootstrap,
          );
          addTearDown(server.close);

          final (:connection, :socket, :ws, :httpClient) =
              await _connectAbruptWebSocketClient(server.port);
          addTearDown(() => ws.close());
          addTearDown(() => httpClient.close(force: true));
          final localCap = _GatedCapability();

          // Fully awaited so the bootstrap has genuinely retained
          // localCap (rather than it having already been released via
          // Return.releaseParamCaps) by the time the socket is destroyed.
          await connection
              .bootstrap(_RawCapabilityFactory())
              .dispatch(
                0,
                0,
                RpcPayload.fromBytes(_emptyParams),
                paramsCapabilities: [localCap],
              );
          expect(connection.debugExportCount, equals(1));

          socket.destroy();
          await connection.done.catchError((_) {});

          expect(connection.debugExportCount, equals(0));
        },
      );

      test(
        'server.close() concurrently tears down every active WebSocket '
        'client, including calls still genuinely in flight through a '
        'shared bootstrap, and the bootstrap is disposed exactly once',
        () async {
          final bootstrap = _CountingGatedBootstrap();
          final server = await RpcSystem.serve(
            Uri.parse('ws://127.0.0.1:0'),
            bootstrap,
          );

          // All three share the one bootstrap's single release gate --
          // concurrently awaiting the same (not yet completed) `release`
          // future is fine, and proves the shared server-lifetime
          // bootstrap lease is only actually disposed once close() has
          // torn every one of them down, not once per connection.
          final clients = await Future.wait(
            List.generate(
              3,
              (_) => RpcSystem.connect(Uri.parse('ws://127.0.0.1:${server.port}')),
            ),
          );
          final callFutures =
              clients
                  .map(
                    (c) => c
                        .bootstrap(_RawCapabilityFactory())
                        .dispatch(0, 0, RpcPayload.fromBytes(_emptyParams)),
                  )
                  .toList();
          for (final f in callFutures) {
            f.ignore();
          }
          // All three, not just the first -- _CountingGatedBootstrap's
          // waitForStarted(3) (unlike _SlowCountingBootstrap's own
          // single-complete `started`) proves every connection genuinely
          // has a call in flight before close() runs below, not just that
          // one of them raced ahead of the other two.
          await bootstrap.waitForStarted(3).timeout(const Duration(seconds: 2));

          await server.close().timeout(const Duration(seconds: 5));
          expect(bootstrap.disposeCount, equals(1));

          bootstrap.release.complete();
          for (final f in callFutures) {
            await expectLater(f, throwsA(isA<RpcException>()));
          }
        },
      );

      test(
        'repeated abrupt WebSocket connect/disconnect cycles free the '
        "server's registry slot every time, with no accumulating retained "
        'state (smoke variant -- see the TCP stress group for the full-size '
        'version of this check)',
        () async {
          final bootstrap = _CountingGatedBootstrap();
          final server = await RpcSystem.serve(
            Uri.parse('ws://127.0.0.1:0'),
            bootstrap,
            maxConnections: 1,
          );
          addTearDown(server.close);

          for (var cycle = 0; cycle < 8; cycle++) {
            // WebSocket's capacity check runs before the upgrade response
            // is sent, so -- unlike the TCP stress loop -- a bare
            // reconnect succeeding is already definitive proof the
            // previous cycle's abruptly-disconnected connection was
            // dropped from the registry; retried since that removal isn't
            // synchronous with anything this loop can otherwise observe.
            final probe = await _retryUntilSuccess(
              () => _connectAbruptWebSocketClient(server.port),
            );

            // Live-state action: issue a call and leave it pending
            // (bootstrap's gate is never released in this test) before
            // abruptly destroying the socket underneath it.
            final callFuture = probe.connection
                .bootstrap(_RawCapabilityFactory())
                .dispatch(0, 0, RpcPayload.fromBytes(_emptyParams));
            callFuture.ignore();

            // Proves this cycle's call genuinely reached the server's
            // bootstrap -- not just that the socket connected -- before
            // tearing it down; waitForStarted(cycle + 1) tracks the
            // cumulative count across the whole loop, since this one
            // bootstrap instance is reused every cycle.
            await bootstrap
                .waitForStarted(cycle + 1)
                .timeout(const Duration(seconds: 2));

            probe.socket.destroy();

            // Full teardown proof per cycle, not just "the next connect
            // succeeds": the call itself must fail with a disconnected
            // error, and the connection's own question table must end up
            // empty -- otherwise this loop could accumulate retained
            // state without ever being caught, defeating its own purpose.
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
            await probe.connection.done.catchError((_) {});
            expect(probe.connection.debugPendingQuestionCount, equals(0));

            probe.httpClient.close(force: true);
          }

          final replacement = await _retryUntilSuccess(
            () => RpcSystem.connect(Uri.parse('ws://127.0.0.1:${server.port}')),
          );
          await replacement.close();

          await server.close().timeout(const Duration(seconds: 5));
        },
      );
    });
  });
}
