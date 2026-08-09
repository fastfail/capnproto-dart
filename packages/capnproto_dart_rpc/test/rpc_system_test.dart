import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:capnproto_dart_rpc/capnproto_dart_rpc.dart';
import 'package:capnproto_dart_rpc/src/capability/capability.dart'
    show NullCapability;
import 'package:test/test.dart';

class _RawCapabilityFactory extends CapabilityFactory<Capability> {
  @override
  Capability fromCapability(Capability cap) => cap;
}

class _CountingBootstrap extends Capability {
  int disposeCount = 0;

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) => Future.error(const RpcException('unsupported'));

  @override
  Future<void> dispose() async {
    disposeCount++;
  }
}

// Minimal validly-framed message (1 segment, 1 word, null root pointer) —
// enough to get past decoding so the call reaches NullCapability.dispatch(),
// which is what's actually under test here.
final _emptyParams = Uint8List.fromList([
  0,
  0,
  0,
  0,
  1,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
  0,
]);

void main() {
  group('RpcSystem.serve validation', () {
    test('negative maxConnections is rejected before binding', () async {
      await expectLater(
        RpcSystem.serve(
          Uri.parse('tcp://127.0.0.1:0'),
          NullCapability(),
          maxConnections: -1,
        ),
        throwsA(isA<ArgumentError>()),
      );
    });

    test('serve() actually attempts to resolve/bind a non-IP-literal host '
        'instead of silently falling back to 127.0.0.1', () async {
      // Regression test: serve() used to compute its bind address as
      // `InternetAddress.tryParse(address.host) ?? InternetAddress.loopbackIPv4`
      // — any host that wasn't a literal IP address (a real hostname, a
      // container name, ...) silently became 127.0.0.1, with no error
      // and no indication the caller's actual host was ignored.
      // '.invalid' is a reserved TLD (RFC 2606) guaranteed to never
      // resolve, so under that old behavior this call would have quietly
      // *succeeded* anyway (bound to loopback instead) rather than
      // surfacing the bogus hostname as the resolution failure it is.
      await expectLater(
        RpcSystem.serve(
          Uri.parse('tcp://this-host-does-not-exist.invalid:0'),
          NullCapability(),
        ),
        throwsA(anything),
      );
    });
  });

  group('RpcSystem.serve / RpcSystem.connect (TCP)', () {
    test('server.close() tears down already-accepted client connections, not '
        'just the listening socket', () async {
      // Regression test: RpcServer.close() previously only closed the
      // listening ServerSocket — accepted client connections (and their
      // underlying TCP sockets) were never tracked, so they stayed open
      // indefinitely after close().
      final server = await RpcSystem.serve(
        Uri.parse('tcp://127.0.0.1:0'),
        NullCapability(),
      );
      addTearDown(server.close);

      final client = await RpcSystem.connect(
        Uri.parse('tcp://127.0.0.1:${server.port}'),
      );

      // Confirm the connection is actually up before closing the server:
      // bootstrap() alone doesn't wait for the handshake, so do a real
      // (albeit unsupported-on-NullCapability) dispatch and observe a
      // clean RPC-level exception rather than a connection error.
      final boot = client.bootstrap(_RawCapabilityFactory());
      await expectLater(
        boot.dispatch(0, 0, RpcPayload.fromBytes(_emptyParams)),
        throwsA(isA<RpcException>()),
      );

      await server.close();

      // Give the client's incoming stream a moment to observe the server
      // socket closing.
      await Future<void>.delayed(const Duration(milliseconds: 200));

      // The connection server.close() was supposed to tear down must now
      // be closed: a new call on it must fail as "connection closed" (or
      // equivalent) — bootstrap() itself throws synchronously once the
      // client side has observed the closure, so this must be a closure
      // (not a pre-evaluated Future) for `throwsA` to catch that.
      expect(
        () => client
            .bootstrap(_RawCapabilityFactory())
            .dispatch(0, 0, RpcPayload.fromBytes(_emptyParams)),
        throwsA(isA<RpcException>()),
      );

      await client.close();
    });

    test('the bootstrap capability is not disposed while the server is still '
        'running, even after every currently-connected client disconnects — '
        'only server.close() finally releases it', () async {
      // Regression test: every accepted connection is handed the same
      // `bootstrap` instance and (per TwoPartyRpcConnection.server's
      // ownership contract) disposes its own reference on close. With
      // connections coming and going sequentially (not all simultaneously
      // alive) rather than a fresh lease per connection, the *last*
      // connection open at any moment closing would previously drop the
      // shared refcount to zero and trigger real disposal of `bootstrap`
      // — even with other clients yet to connect and reuse it.
      final bootstrap = _CountingBootstrap();
      final server = await RpcSystem.serve(
        Uri.parse('tcp://127.0.0.1:0'),
        bootstrap,
      );
      addTearDown(server.close);

      final client1 = await RpcSystem.connect(
        Uri.parse('tcp://127.0.0.1:${server.port}'),
      );
      await expectLater(
        client1
            .bootstrap(_RawCapabilityFactory())
            .dispatch(0, 0, RpcPayload.fromBytes(_emptyParams)),
        throwsA(isA<RpcException>()),
      );
      await client1.close();
      await Future<void>.delayed(const Duration(milliseconds: 200));

      // No client is connected right now, but the server itself is still
      // running — bootstrap must still be alive.
      expect(bootstrap.disposeCount, equals(0));

      // A second, later connection must still be able to use it.
      final client2 = await RpcSystem.connect(
        Uri.parse('tcp://127.0.0.1:${server.port}'),
      );
      await expectLater(
        client2
            .bootstrap(_RawCapabilityFactory())
            .dispatch(0, 0, RpcPayload.fromBytes(_emptyParams)),
        throwsA(isA<RpcException>()),
      );
      expect(bootstrap.disposeCount, equals(0));

      await client2.close();
      await server.close();
      await Future<void>.delayed(const Duration(milliseconds: 200));

      expect(bootstrap.disposeCount, equals(1));
    });

    test('passing an already-acquired lease as bootstrap still keeps the '
        'underlying identity alive across sequential connections, not just a '
        'wrapper around a wrapper', () async {
      // Regression test: serve() used to lease serverBootstrapRef directly
      // from whatever `bootstrap` argument it was given, without
      // unwrapping first. acquireCapabilityLease() is itself public API, so
      // a caller can legitimately pass an already-acquired lease (rather
      // than a bare capability) as bootstrap — leasing *that* creates a
      // second, disconnected refcount cycle keyed on the lease object
      // instead of on the real underlying identity every connection's own
      // export ends up sharing, so serverBootstrapRef ends up protecting
      // nothing real: the underlying identity's shared refcount could
      // still drop to zero between sequential connections exactly like
      // before the original fix.
      final bootstrap = _CountingBootstrap();
      final wrapped = acquireCapabilityLease(bootstrap);
      final server = await RpcSystem.serve(
        Uri.parse('tcp://127.0.0.1:0'),
        wrapped,
      );
      addTearDown(server.close);

      final client1 = await RpcSystem.connect(
        Uri.parse('tcp://127.0.0.1:${server.port}'),
      );
      await expectLater(
        client1
            .bootstrap(_RawCapabilityFactory())
            .dispatch(0, 0, RpcPayload.fromBytes(_emptyParams)),
        throwsA(isA<RpcException>()),
      );
      await client1.close();
      await Future<void>.delayed(const Duration(milliseconds: 200));

      expect(bootstrap.disposeCount, equals(0));

      final client2 = await RpcSystem.connect(
        Uri.parse('tcp://127.0.0.1:${server.port}'),
      );
      await expectLater(
        client2
            .bootstrap(_RawCapabilityFactory())
            .dispatch(0, 0, RpcPayload.fromBytes(_emptyParams)),
        throwsA(isA<RpcException>()),
      );
      expect(bootstrap.disposeCount, equals(0));

      await client2.close();
      await server.close();
      await Future<void>.delayed(const Duration(milliseconds: 200));

      expect(bootstrap.disposeCount, equals(1));
    });

    test('a serve() call that fails before ever binding (an unsupported '
        'scheme) does not touch bootstrap at all — matching serve()\'s own '
        'doc comment, the same bootstrap can be reused for a later, '
        'successful call', () async {
      // Regression test: serve() used to lease its server-lifetime
      // bootstrap reference *before* scheme validation / listener
      // binding, unconditionally disposing it (via the caller's own
      // lease-release step, or the shared refcount it created) on any
      // failure afterward — including scheme validation, which happens
      // before anything is bound and touches nothing else. That
      // permanently disposed the real bootstrap identity even though
      // serve() never actually took ownership of it, directly
      // contradicting this method's own doc comment ("If this call
      // throws instead ... bootstrap is left exactly as it was passed
      // in").
      final bootstrap = _CountingBootstrap();

      await expectLater(
        RpcSystem.serve(Uri.parse('bogus://127.0.0.1:0'), bootstrap),
        throwsA(isA<RpcException>()),
      );

      expect(bootstrap.disposeCount, equals(0));

      // The same instance — untouched by the failed call above — must
      // still be usable for a real server.
      final server = await RpcSystem.serve(
        Uri.parse('tcp://127.0.0.1:0'),
        bootstrap,
      );
      addTearDown(server.close);
      final client = await RpcSystem.connect(
        Uri.parse('tcp://127.0.0.1:${server.port}'),
      );
      await expectLater(
        client
            .bootstrap(_RawCapabilityFactory())
            .dispatch(0, 0, RpcPayload.fromBytes(_emptyParams)),
        throwsA(isA<RpcException>()),
      );
      await client.close();
    });

    test('a serve() call that fails only after successfully binding the '
        'listener — because bootstrap itself is unusable (e.g. already fully '
        'disposed through a prior acquireCapabilityLease cycle) — still closes '
        'that listener, instead of leaking an open port', () async {
      // Learn a currently-free port, then release it immediately — used
      // only to give the failing serve() call below a specific port
      // number to bind (rather than an ephemeral 0), so a subsequent
      // successful bind to that exact same port number can prove the
      // first call's listener socket was actually closed, not merely
      // abandoned.
      final probe = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
      final port = probe.port;
      await probe.close();

      final spentBootstrap = _CountingBootstrap();
      // Triggers disposal for spentBootstrap's identity — any later
      // acquireCapabilityLease(spentBootstrap) throws (see that function's
      // own doc comment).
      await acquireCapabilityLease(spentBootstrap).dispose();
      expect(spentBootstrap.disposeCount, equals(1));

      await expectLater(
        RpcSystem.serve(Uri.parse('tcp://127.0.0.1:$port'), spentBootstrap),
        throwsA(anything),
      );

      // Only possible if the failed call above actually closed the
      // listener it had already bound on `port`, rather than leaking it
      // open forever.
      final server = await RpcSystem.serve(
        Uri.parse('tcp://127.0.0.1:$port'),
        _CountingBootstrap(),
      );
      addTearDown(server.close);
      expect(server.port, equals(port));
    });

    test(
      'a malformed/aborted connection does not surface as an unhandled '
      'top-level error, and does not destabilize later connections',
      () async {
        // Regression test: RpcSystem.serve's per-connection tracking (added
        // for the fix above) used `conn.done.whenComplete(...)` to remove a
        // connection from the tracked set once it closes. TwoPartyRpcConnection
        // itself calls `.ignore()` on that same completer before erroring it,
        // specifically so an unobserved `.done` doesn't print as unhandled —
        // but `.whenComplete()` replays the same error onto the *new* future
        // it returns, and that one was left unobserved. A single malformed
        // connection (e.g. a port-liveness probe that sends one byte and
        // disconnects, exactly like the `echo > /dev/tcp/...` idiom used in
        // this repo's own ci/run-tests.sh) was therefore printed as a
        // top-level "Unhandled exception" — which terminates the isolate,
        // breaking every connection accepted afterward too.
        final unhandledErrors = <Object>[];

        await runZonedGuarded(() async {
          final server = await RpcSystem.serve(
            Uri.parse('tcp://127.0.0.1:0'),
            NullCapability(),
          );
          addTearDown(server.close);

          // A stray connection that can never form a complete Cap'n Proto
          // message: one byte, then disconnect.
          final probe = await Socket.connect('127.0.0.1', server.port);
          probe.add([10]);
          await probe.flush();
          await probe.close();

          // Give the server a moment to observe and tear down the bad
          // connection.
          await Future<void>.delayed(const Duration(milliseconds: 300));

          // A legitimate client connecting afterward must still work —
          // the process/isolate must not have been brought down by the
          // probe connection's error.
          final client = await RpcSystem.connect(
            Uri.parse('tcp://127.0.0.1:${server.port}'),
          );
          await expectLater(
            client
                .bootstrap(_RawCapabilityFactory())
                .dispatch(0, 0, RpcPayload.fromBytes(_emptyParams)),
            throwsA(isA<RpcException>()),
          );
          await client.close();
        }, (error, stackTrace) => unhandledErrors.add(error));

        expect(
          unhandledErrors,
          isEmpty,
          reason:
              'expected no unhandled top-level errors from the malformed '
              'connection, got: $unhandledErrors',
        );
      },
    );

    test('maxConnections rejects a tcp:// connection beyond the cap', () async {
      final server = await RpcSystem.serve(
        Uri.parse('tcp://127.0.0.1:0'),
        NullCapability(),
        maxConnections: 1,
      );
      addTearDown(server.close);

      final first = await RpcSystem.connect(
        Uri.parse('tcp://127.0.0.1:${server.port}'),
      );
      addTearDown(first.close);
      // Confirm the first connection is actually accepted before probing
      // the cap.
      await expectLater(
        first
            .bootstrap(_RawCapabilityFactory())
            .dispatch(0, 0, RpcPayload.fromBytes(_emptyParams)),
        throwsA(isA<RpcException>()),
      );

      // Unlike the WebSocket transport, a rejected tcp:// connection has no
      // application-level handshake to fail — the server just destroys the
      // raw socket (atCapacity() -> socket.destroy()) the moment it's
      // accepted. Observed here as the probe socket closing with no bytes
      // ever received, rather than staying open or getting any RPC data.
      final probe = await Socket.connect('127.0.0.1', server.port);
      final received = <int>[];
      final probeDone = Completer<void>();
      probe.listen(
        received.addAll,
        onDone: () {
          if (!probeDone.isCompleted) probeDone.complete();
        },
        onError: (Object _) {
          if (!probeDone.isCompleted) probeDone.complete();
        },
        cancelOnError: true,
      );
      await probeDone.future.timeout(const Duration(seconds: 2));
      expect(received, isEmpty);

      await first.close();
      await Future<void>.delayed(const Duration(milliseconds: 50));

      final replacement = await RpcSystem.connect(
        Uri.parse('tcp://127.0.0.1:${server.port}'),
      );
      addTearDown(replacement.close);
      await expectLater(
        replacement
            .bootstrap(_RawCapabilityFactory())
            .dispatch(0, 0, RpcPayload.fromBytes(_emptyParams)),
        throwsA(isA<RpcException>()),
      );
    });

  });

  group('RpcSystem.serve / RpcSystem.connect (WebSocket)', () {
    test(
      'a ws:// client can reach a ws:// server and dispatch a call',
      () async {
        final server = await RpcSystem.serve(
          Uri.parse('ws://127.0.0.1:0'),
          NullCapability(),
        );
        addTearDown(server.close);

        final client = await RpcSystem.connect(
          Uri.parse('ws://127.0.0.1:${server.port}'),
        );
        addTearDown(client.close);

        // Same shape as the TCP test above: NullCapability rejects every
        // dispatch, so a clean RpcException (not a connection-level failure)
        // proves the message actually made the full round trip over the
        // WebSocket transport.
        final boot = client.bootstrap(_RawCapabilityFactory());
        await expectLater(
          boot.dispatch(0, 0, RpcPayload.fromBytes(_emptyParams)),
          throwsA(isA<RpcException>()),
        );
      },
    );

    test('maxConnections rejects a ws:// connection beyond the cap', () async {
      final server = await RpcSystem.serve(
        Uri.parse('ws://127.0.0.1:0'),
        NullCapability(),
        maxConnections: 1,
      );
      addTearDown(server.close);

      final first = await RpcSystem.connect(
        Uri.parse('ws://127.0.0.1:${server.port}'),
      );
      addTearDown(first.close);
      // Confirm the first connection is actually accepted before probing
      // the cap.
      await expectLater(
        first
            .bootstrap(_RawCapabilityFactory())
            .dispatch(0, 0, RpcPayload.fromBytes(_emptyParams)),
        throwsA(isA<RpcException>()),
      );

      // The second connection's WebSocket handshake itself must fail (the
      // server responds 503 instead of upgrading), not silently connect and
      // then hang or error at the RPC layer.
      await expectLater(
        RpcSystem.connect(Uri.parse('ws://127.0.0.1:${server.port}')),
        throwsA(anything),
      );

      await first.close();
      await Future<void>.delayed(const Duration(milliseconds: 50));

      final replacement = await RpcSystem.connect(
        Uri.parse('ws://127.0.0.1:${server.port}'),
      );
      addTearDown(replacement.close);
      await expectLater(
        replacement
            .bootstrap(_RawCapabilityFactory())
            .dispatch(0, 0, RpcPayload.fromBytes(_emptyParams)),
        throwsA(isA<RpcException>()),
      );
    });

    test(
      'server path is enforced while query parameters remain allowed',
      () async {
        final server = await RpcSystem.serve(
          Uri.parse('ws://127.0.0.1:0/capnp'),
          NullCapability(),
        );
        addTearDown(server.close);

        await expectLater(
          RpcSystem.connect(Uri.parse('ws://127.0.0.1:${server.port}/wrong')),
          throwsA(anything),
        );

        final client = await RpcSystem.connect(
          Uri.parse('ws://127.0.0.1:${server.port}/capnp?token=test'),
        );
        addTearDown(client.close);
        await expectLater(
          client
              .bootstrap(_RawCapabilityFactory())
              .dispatch(0, 0, RpcPayload.fromBytes(_emptyParams)),
          throwsA(isA<RpcException>()),
        );
      },
    );

    test('concurrent upgrades cannot oversubscribe maxConnections', () async {
      final server = await RpcSystem.serve(
        Uri.parse('ws://127.0.0.1:0'),
        NullCapability(),
        maxConnections: 1,
      );
      addTearDown(server.close);

      final attempts = await Future.wait(
        List.generate(12, (_) async {
          try {
            return await RpcSystem.connect(
              Uri.parse('ws://127.0.0.1:${server.port}'),
            );
          } catch (_) {
            return null;
          }
        }),
      );
      final accepted = attempts.whereType<RpcConnection>().toList();
      expect(accepted, hasLength(1));
      await Future.wait(accepted.map((connection) => connection.close()));
    });

    test('wss:// server without a securityContext is rejected', () async {
      await expectLater(
        RpcSystem.serve(Uri.parse('wss://127.0.0.1:0'), NullCapability()),
        throwsA(isA<RpcException>()),
      );
    });

  });
}
