import 'dart:async';
import 'dart:typed_data';

import 'package:capnproto_dart_rpc/capnproto_dart_rpc.dart';
import 'package:capnproto_dart_rpc/src/capability/capability.dart';
import 'package:capnproto_dart_rpc/src/rpc/capabilities/rpc_capability.dart';
import 'package:capnproto_dart_rpc/src/rpc/capabilities/wire_capability_reference.dart';
import 'package:capnproto_dart_rpc/src/rpc/rpc_message_codec.dart';
import 'package:capnproto_dart_rpc/src/rpc/two_party_connection.dart';
import 'package:test/test.dart';

/// Extracts the export id from a senderHosted/senderPromise
/// [WireCapabilityReference] — a test-only helper for assertions against a
/// decoded message's `capabilityTableReferences`, wherever the test already
/// knows (from how it built the corresponding outgoing message) that the
/// entry in question is one of those two variants.
int _exportIdOf(WireCapabilityReference reference) => switch (reference) {
  SenderHostedCapabilityReference(:final exportId) => exportId,
  SenderPromiseCapabilityReference(:final exportId) => exportId,
  _ =>
    throw StateError(
      'expected a senderHosted/senderPromise reference, got $reference',
    ),
};

/// [_exportIdOf], applied to every senderHosted/senderPromise entry among
/// [references] — mirrors the old `RpcMessage.capTableExportIds` convenience
/// field for assertions, without needing the field itself (a
/// senderHosted/senderPromise `WireCapabilityReference`'s `exportId` is
/// already directly derivable, so keeping a separate decoded field around
/// just for this would be redundant).
List<int> _exportIdsOf(List<WireCapabilityReference> references) => [
  for (final r in references)
    if (r
        case SenderHostedCapabilityReference(:final exportId) ||
            SenderPromiseCapabilityReference(:final exportId))
      exportId,
];

class _SynchronousThrowingSink implements StreamSink<Uint8List> {
  final Completer<void> _done = Completer<void>();

  @override
  void add(Uint8List data) => throw StateError('deliberate sink failure');

  @override
  void addError(Object error, [StackTrace? stackTrace]) {}

  @override
  Future<void> addStream(Stream<Uint8List> stream) async {
    await for (final data in stream) {
      add(data);
    }
  }

  @override
  Future<void> close() async {
    if (!_done.isCompleted) _done.complete();
  }

  @override
  Future<void> get done => _done.future;
}

// Forwards every message to [_inner] except the [throwOnReleaseNumber]-th
// Release message (1-based, counting only RpcMessageType.release), which
// throws instead of forwarding — for exercising a batched flush
// (_flushPendingReleases) that fails partway through.
class _ThrowOnNthReleaseSink implements StreamSink<Uint8List> {
  final StreamSink<Uint8List> _inner;
  final int throwOnReleaseNumber;
  int _releasesSeen = 0;

  _ThrowOnNthReleaseSink(this._inner, this.throwOnReleaseNumber);

  @override
  void add(Uint8List data) {
    if (parseRpcMessage(data).type == RpcMessageType.release) {
      _releasesSeen++;
      if (_releasesSeen == throwOnReleaseNumber) {
        throw StateError('deliberate failure on release #$_releasesSeen');
      }
    }
    _inner.add(data);
  }

  @override
  void addError(Object error, [StackTrace? stackTrace]) =>
      _inner.addError(error, stackTrace);

  @override
  Future<void> addStream(Stream<Uint8List> stream) => _inner.addStream(stream);

  @override
  Future<void> close() => _inner.close();

  @override
  Future<void> get done => _inner.done;
}

// ---------------------------------------------------------------------------
// Minimal in-memory schema: Echo interface
//   method echo(message :Text) -> (reply :Text)
//   interfaceId = 0x0001
//   methodId = 0
// ---------------------------------------------------------------------------

const int _echoInterfaceId = 0x0001;
const int _echoMethodId = 0;
const int _pipelineMethodId = 1; // returns a capability in caps[0]
const int _mixedMethodId = 2; // result has non-cap at slot 0, cap at slot 1
const int _duplicateCapsMethodId = 3; // returns the same cap in two slots
const int _largeCapResultMethodId = 4; // result has large data + cap
const int _largeCapParamMethodId = 5; // params have large data + cap
const int _listCapsResultMethodId = 6; // result has List(Interface) in ptr[0]
const int _listCapsParamMethodId = 7; // params have List(Interface) in ptr[0]
const int _equalCapsMethodId = 10; // returns equal-but-not-identical caps
const int _throwingTryTailCallMethodId = 11;

// Simple factory to build param message { message :Text } (ptr 0)
Uint8List _buildEchoParams(String message) {
  final mb = MessageBuilder();
  final root = mb.initRoot(_TextParamFactory());
  root.setTextField(0, message);
  return mb.serialize();
}

String? _parseEchoResult(RpcPayload payload) =>
    payload.getTyped(_TextParamFactory()).getTextField(0);

int _segmentCount(Uint8List bytes) =>
    ByteData.sublistView(bytes, 0, 4).getUint32(0, Endian.little) + 1;

Uint8List _largeData(int size) =>
    Uint8List.fromList(List<int>.generate(size, (i) => i & 0xff));

Uint8List _buildLargeDataParams(int size) {
  final mb = MessageBuilder();
  mb.initRoot(_TextParamFactory()).setDataField(0, _largeData(size));
  final bytes = mb.serialize();
  expect(_segmentCount(bytes), greaterThan(1));
  return bytes;
}

Uint8List _buildLargeDataAndCapResult(int size) {
  final mb = MessageBuilder();
  final root = mb.initRoot(_TwoPtrFactory());
  root.setDataField(0, _largeData(size));
  root.setCapabilityField(1, 0);
  final bytes = mb.serialize();
  expect(_segmentCount(bytes), greaterThan(1));
  return bytes;
}

// A minimal StructFactory for a struct with 0 dataWords and 1 ptrWord (Text).
final class _TextParamFactory
    extends StructFactory<_TextParamReader, _TextParamBuilder> {
  @override
  int get dataWords => 0;
  @override
  int get ptrWords => 1;
  @override
  _TextParamReader fromRawReader(RawStructReader r) => _TextParamReader(r);
  @override
  _TextParamBuilder fromRawBuilder(RawStructBuilder r) => _TextParamBuilder(r);
}

// A struct factory with 0 dataWords and 2 ptrWords (for mixed-result tests).
final class _TwoPtrFactory
    extends StructFactory<_TextParamReader, _TextParamBuilder> {
  @override
  int get dataWords => 0;
  @override
  int get ptrWords => 2;
  @override
  _TextParamReader fromRawReader(RawStructReader r) => _TextParamReader(r);
  @override
  _TextParamBuilder fromRawBuilder(RawStructBuilder r) => _TextParamBuilder(r);
}

Uint8List _callWithCapDescriptorDisc(int disc) {
  final paramsBuilder = MessageBuilder();
  paramsBuilder.initRoot(_TextParamFactory());
  final params = paramsBuilder.serialize();
  final withNone = buildCallMessage(
    questionId: 1,
    targetImportId: 0,
    interfaceId: _echoInterfaceId,
    methodId: _echoMethodId,
    paramsBytes: params,
    capabilityTableReferences: const [NoCapabilityReference()],
  );
  final withSenderHosted = buildCallMessage(
    questionId: 1,
    targetImportId: 0,
    interfaceId: _echoInterfaceId,
    methodId: _echoMethodId,
    paramsBytes: params,
    capabilityTableReferences: const [SenderHostedCapabilityReference(0)],
  );
  final differences = <int>[
    for (var i = 0; i < withNone.length; i++)
      if (withNone[i] != withSenderHosted[i]) i,
  ];
  expect(differences, hasLength(1));
  final result = Uint8List.fromList(withNone);
  result[differences.single] = disc;
  return result;
}

// Same byte-patching trick as _callWithCapDescriptorDisc, but for a
// two-entry capTable whose *first* entry is a real, valid
// receiverHosted(0) descriptor and whose *second* entry's disc byte is
// patched to [disc] — used to probe cleanup when a later entry fails to
// decode after an earlier one already succeeded.
Uint8List _callWithReceiverHostedThenCapDescriptorDisc(int disc) {
  final paramsBuilder = MessageBuilder();
  paramsBuilder.initRoot(_TextParamFactory());
  final params = paramsBuilder.serialize();
  final withNoneSecond = buildCallMessage(
    questionId: 1,
    targetImportId: 0,
    interfaceId: _echoInterfaceId,
    methodId: _echoMethodId,
    paramsBytes: params,
    capabilityTableReferences: const [
      ReceiverHostedCapabilityReference(0),
      NoCapabilityReference(),
    ],
  );
  final withSenderHostedSecond = buildCallMessage(
    questionId: 1,
    targetImportId: 0,
    interfaceId: _echoInterfaceId,
    methodId: _echoMethodId,
    paramsBytes: params,
    capabilityTableReferences: const [
      ReceiverHostedCapabilityReference(0),
      SenderHostedCapabilityReference(0),
    ],
  );
  final differences = <int>[
    for (var i = 0; i < withNoneSecond.length; i++)
      if (withNoneSecond[i] != withSenderHostedSecond[i]) i,
  ];
  expect(differences, hasLength(1));
  final result = Uint8List.fromList(withNoneSecond);
  result[differences.single] = disc;
  return result;
}

class _TextParamReader extends StructReader {
  _TextParamReader(super.raw);
}

class _TextParamBuilder extends StructBuilder {
  _TextParamBuilder(super.raw);
  @override
  StructReader asReader() => throw UnimplementedError();
}

// ---------------------------------------------------------------------------
// Echo server implementation
// ---------------------------------------------------------------------------

class EchoServer extends Capability {
  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    if (interfaceId != _echoInterfaceId) {
      throw RpcException('wrong interface: $interfaceId');
    }
    if (methodId != _echoMethodId) {
      throw RpcException('unknown method: $methodId');
    }

    final req = params.getTyped(_TextParamFactory());
    final message = req.getTextField(0) ?? '';
    return DispatchResult(
      payload: RpcPayload.fromBytes(_buildEchoParams('echo: $message')),
    );
  }

  @override
  Future<void> dispose() async {}
}

// Throws synchronously inside dispatchWithContext (before returning a Future).
// #50: throws an RpcException carrying a specific ErrorKind, to verify it
// round-trips end to end (dispatch -> wire Exception.type -> caller).
class _KindThrowingCapability extends Capability {
  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) => Future.error(
    const RpcException('peer is gone', kind: ErrorKind.disconnected),
  );

  @override
  Future<void> dispose() async {}
}

class _SyncThrowingCapability extends Capability {
  @override
  Future<DispatchResult> dispatchWithContext(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
    DispatchCancellationContext? context,
  }) {
    throw StateError('deliberate synchronous throw');
  }

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) => Future.error(UnsupportedError('not reached'));

  @override
  Future<void> dispose() async {}
}

// Throws synchronously only on the first call; subsequent calls echo normally.
class _FirstCallSyncThrowCapability extends Capability {
  int _callCount = 0;

  @override
  Future<DispatchResult> dispatchWithContext(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
    DispatchCancellationContext? context,
  }) {
    _callCount++;
    if (_callCount == 1) throw StateError('deliberate synchronous throw');
    return dispatch(
      interfaceId,
      methodId,
      params,
      paramsCapabilities: paramsCapabilities,
    );
  }

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    final message = params.getTyped(_TextParamFactory()).getTextField(0) ?? '';
    return DispatchResult(
      payload: RpcPayload.fromBytes(_buildEchoParams('echo: $message')),
    );
  }

  @override
  Future<void> dispose() async {}
}

class ThrowingDisposeCapability extends Capability {
  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) => Future.error(UnsupportedError('not used'));

  @override
  Future<void> dispose() async {
    await Future<void>.delayed(Duration.zero);
    throw StateError('dispose failed');
  }
}

// Unlike ThrowingDisposeCapability, this one is deliberately NOT `async`: it
// throws before ever returning a Future, exercising the case where
// Capability.dispose() (typed Future<void>) is implemented by something
// that isn't actually asynchronous under the hood.
class SyncThrowingDisposeCapability extends Capability {
  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) => Future.error(UnsupportedError('not used'));

  @override
  Future<void> dispose() {
    throw StateError('sync dispose failed');
  }
}

// Lets a test control exactly when a real (asynchronous, multi-event-loop-
// -turn) dispose() call completes — e.g. to probe acquireCapabilityLease's
// behavior for a lease acquired *while* an earlier cycle's disposal of the
// same target is still in flight, not yet actually finished.
class SlowDisposeCapability extends Capability {
  int disposeCount = 0;
  final Completer<void> _gate = Completer<void>();

  /// Lets a pending dispose() call complete.
  void releaseDispose() {
    if (!_gate.isCompleted) _gate.complete();
  }

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) => Future.error(UnsupportedError('not used'));

  @override
  Future<void> dispose() async {
    await _gate.future;
    disposeCount++;
  }
}

class CountingCapability extends EchoServer {
  int disposeCount = 0;
  int dispatchCount = 0;

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    dispatchCount++;
    return super.dispatch(
      interfaceId,
      methodId,
      params,
      paramsCapabilities: paramsCapabilities,
    );
  }

  @override
  Future<void> dispose() async {
    disposeCount++;
  }
}

// ---------------------------------------------------------------------------
// Echo client stub
// ---------------------------------------------------------------------------

class EchoClient extends Capability {
  final Capability cap;
  EchoClient(this.cap);

  Future<String> echo(String message) async {
    final result = await cap.dispatch(
      _echoInterfaceId,
      _echoMethodId,
      RpcPayload.fromBytes(_buildEchoParams(message)),
    );
    return _parseEchoResult(result.payload) ?? '';
  }

  @override
  Future<DispatchResult> dispatch(
    int iid,
    int mid,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) => Future.error(UnsupportedError('client stub'));

  @override
  Future<void> dispose() => cap.dispose();
}

class EchoClientFactory extends CapabilityFactory<EchoClient> {
  @override
  EchoClient fromCapability(Capability cap) => EchoClient(cap);
}

// ---------------------------------------------------------------------------
// In-memory pipe helper
// ---------------------------------------------------------------------------

/// Creates a bidirectional in-memory pipe: returns (client conn, server conn).
(TwoPartyRpcConnection, TwoPartyRpcConnection) _makePipe(
  Capability serverBootstrap,
) {
  final clientToServer = StreamController<Uint8List>();
  final serverToClient = StreamController<Uint8List>();

  final client = TwoPartyRpcConnection.client(
    incoming: serverToClient.stream,
    outgoing: clientToServer.sink,
  );
  final server = TwoPartyRpcConnection.server(
    incoming: clientToServer.stream,
    outgoing: serverToClient.sink,
    bootstrap: serverBootstrap,
  );
  return (client, server);
}

// ---------------------------------------------------------------------------
// PipelineServer — method 1 returns itself as caps[0] for pipelining tests
// ---------------------------------------------------------------------------

class PipelineServer extends Capability {
  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    if (methodId == _echoMethodId) {
      final message =
          params.getTyped(_TextParamFactory()).getTextField(0) ?? '';
      return DispatchResult(
        payload: RpcPayload.fromBytes(_buildEchoParams('echo: $message')),
      );
    }
    if (methodId == _pipelineMethodId) {
      // Result struct: 1 pointer slot.
      // slot 0: CapabilityPointer(capTableIndex=0) → caps[0] = this server.
      final mb = MessageBuilder();
      final root = mb.initRoot(_TextParamFactory());
      root.setCapabilityField(0, 0);
      return DispatchResult(
        payload: RpcPayload.fromBuilder(root),
        caps: [this],
      );
    }
    throw RpcException('unknown method: $methodId');
  }

  @override
  Future<void> dispose() async {}
}

// MixedResultServer: method 2 returns a result struct with 2 pointer slots
// where slot 0 is NOT a capability and slot 1 IS a capability (cap table index 0).
// This is the scenario that exposed the RPC-001 bug (ptrIndex ≠ capTableIndex).
class MixedResultServer extends Capability {
  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    if (methodId == _echoMethodId) {
      final message =
          params.getTyped(_TextParamFactory()).getTextField(0) ?? '';
      return DispatchResult(
        payload: RpcPayload.fromBytes(_buildEchoParams('echo: $message')),
      );
    }
    if (methodId == _mixedMethodId) {
      // Result struct: 2 pointer slots.
      // slot 0: null (not a capability)
      // slot 1: CapabilityPointer(capTableIndex=0) → caps[0] = this server.
      final mb = MessageBuilder();
      final root = mb.initRoot(_TwoPtrFactory());
      root.setCapabilityField(1, 0);
      return DispatchResult(
        payload: RpcPayload.fromBuilder(root),
        caps: [this],
      );
    }
    throw RpcException('unknown method: $methodId');
  }

  @override
  Future<void> dispose() async {}
}

class ChildPipelineServer extends Capability {
  final Completer<void>? completer;
  final Capability child;

  ChildPipelineServer({this.completer, Capability? child})
    : child = child ?? EchoServer();

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    if (methodId == _pipelineMethodId) {
      final c = completer;
      if (c != null) await c.future;
      final mb = MessageBuilder();
      final root = mb.initRoot(_TextParamFactory());
      root.setCapabilityField(0, 0);
      return DispatchResult(
        payload: RpcPayload.fromBuilder(root),
        caps: [child],
      );
    }
    if (methodId == _echoMethodId) {
      return DispatchResult(
        payload: RpcPayload.fromBytes(_buildEchoParams('ok')),
      );
    }
    throw RpcException('unknown method: $methodId');
  }

  @override
  Future<void> dispose() async {}
}

// Returns the same capability instance (passed in at construction) as a
// result capability on every _pipelineMethodId call — unlike
// ChildPipelineServer, which allocates its own child. Used to test that
// exporting the *same* capability across multiple Returns reuses one export
// entry instead of allocating a new one per call.
class FixedCapServer extends Capability {
  final Capability target;
  FixedCapServer(this.target);

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    if (methodId == _pipelineMethodId) {
      final mb = MessageBuilder();
      final root = mb.initRoot(_TextParamFactory());
      root.setCapabilityField(0, 0);
      return DispatchResult(
        payload: RpcPayload.fromBuilder(root),
        caps: [target],
      );
    }
    if (methodId == _echoMethodId) {
      return DispatchResult(
        payload: RpcPayload.fromBytes(_buildEchoParams('ok')),
      );
    }
    throw RpcException('unknown method: $methodId');
  }

  @override
  Future<void> dispose() async {}
}

class PromisedReturnServer extends Capability {
  final Completer<Capability> completer = Completer<Capability>();
  late final DeferredCapability promised = DeferredCapability(completer.future);

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    if (methodId == _pipelineMethodId) {
      final mb = MessageBuilder();
      final root = mb.initRoot(_TextParamFactory());
      root.setCapabilityField(0, 0);
      return DispatchResult(
        payload: RpcPayload.fromBuilder(root),
        caps: [promised],
      );
    }
    if (methodId == _echoMethodId) {
      return DispatchResult(
        payload: RpcPayload.fromBytes(_buildEchoParams('ok')),
      );
    }
    throw RpcException('unknown method: $methodId');
  }

  @override
  Future<void> dispose() async {}
}

class DuplicateCapsServer extends Capability {
  final Capability child = EchoServer();

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    if (methodId == _duplicateCapsMethodId) {
      final mb = MessageBuilder();
      final root = mb.initRoot(_TwoPtrFactory());
      root.setCapabilityField(0, 0);
      root.setCapabilityField(1, 1);
      return DispatchResult(
        payload: RpcPayload.fromBuilder(root),
        caps: [child, child],
      );
    }
    if (methodId == _echoMethodId) {
      return DispatchResult(
        payload: RpcPayload.fromBytes(_buildEchoParams('ok')),
      );
    }
    throw RpcException('unknown method: $methodId');
  }

  @override
  Future<void> dispose() async {}
}

class LargeCapabilityPayloadServer extends Capability {
  final Capability child = EchoServer();
  int? lastDataLength;
  String? lastParamCapReply;

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    if (methodId == _largeCapResultMethodId) {
      return DispatchResult(
        payload: RpcPayload.fromBytes(_buildLargeDataAndCapResult(10000)),
        caps: [child],
      );
    }

    if (methodId == _largeCapParamMethodId) {
      final root = params.getTyped(_TwoPtrFactory());
      lastDataLength = root.getDataField(0)?.length;
      final cap = root.getCapabilityField(1);
      if (cap != 0 || paramsCapabilities.isEmpty) {
        throw RpcException('large cap param did not carry capability');
      }
      final reply = await paramsCapabilities[0].dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('from server')),
      );
      lastParamCapReply = _parseEchoResult(reply.payload);
      return DispatchResult(
        payload: RpcPayload.fromBytes(_buildEchoParams('ok')),
      );
    }

    if (methodId == _echoMethodId) {
      return DispatchResult(
        payload: RpcPayload.fromBytes(_buildEchoParams('ok')),
      );
    }

    throw RpcException('unknown method: $methodId');
  }

  @override
  Future<void> dispose() async {}
}

// ---------------------------------------------------------------------------
// ListCapsServer — tests List(Interface) over RPC
//   method 6 (_listCapsResultMethodId): returns struct with List(Interface) in ptr[0]
//   method 7 (_listCapsParamMethodId):  reads List(Interface) from params, calls each
// ---------------------------------------------------------------------------

class ListCapsServer extends Capability {
  final EchoServer child0 = EchoServer();
  final EchoServer child1 = EchoServer();

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    if (methodId == _echoMethodId) {
      return DispatchResult(
        payload: RpcPayload.fromBytes(_buildEchoParams('ok')),
      );
    }
    if (methodId == _listCapsResultMethodId) {
      final mb = MessageBuilder();
      final root = mb.initRoot(_TextParamFactory());
      final list = root.initCapabilityListField(0, 2);
      list[0] = 0;
      list[1] = 1;
      return DispatchResult(
        payload: RpcPayload.fromBuilder(root),
        caps: [child0, child1],
      );
    }
    if (methodId == _listCapsParamMethodId) {
      final root = params.getTyped(_TextParamFactory());
      final rawList = root.getCapabilityListField(0);
      if (rawList == null || rawList.length < 2) {
        throw const RpcException(
          'expected List(Interface) with 2 caps in ptr[0]',
        );
      }
      final cap0 = paramsCapabilities[rawList[0]];
      final cap1 = paramsCapabilities[rawList[1]];
      final r0 = await cap0.dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('a')),
      );
      final r1 = await cap1.dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('b')),
      );
      final reply =
          '${_parseEchoResult(r0.payload)}|${_parseEchoResult(r1.payload)}';
      return DispatchResult(
        payload: RpcPayload.fromBytes(_buildEchoParams(reply)),
      );
    }
    throw RpcException('unknown method: $methodId');
  }

  @override
  Future<void> dispose() async {}
}

// ---------------------------------------------------------------------------
// CapReceivingServer — captures paramsCapabilities for inspection
// ---------------------------------------------------------------------------

// A capability with no behavior of its own beyond tracking whether
// dispose() was called — used to observe whether a rolled-back/released
// export reference actually disposed the underlying capability.
class _TrackedCapability extends Capability {
  bool disposed = false;

  @override
  Future<void> dispose() async {
    disposed = true;
  }
}

// Returns [returnedCapability] as caps[0] on `_pipelineMethodId`, matching
// PipelineServer's result-encoding convention above.
class _CapabilityReturningServer extends Capability {
  final Capability returnedCapability;
  _CapabilityReturningServer(this.returnedCapability);

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    if (methodId == _pipelineMethodId) {
      final mb = MessageBuilder();
      final root = mb.initRoot(_TextParamFactory());
      root.setCapabilityField(0, 0);
      return DispatchResult(
        payload: RpcPayload.fromBuilder(root),
        caps: [returnedCapability],
      );
    }
    throw RpcException('unknown method: $methodId');
  }

  @override
  Future<void> dispose() async {}
}

// Disposes every params capability it receives, simulating a peer that's
// done with a relayed capability the moment its call completes.
class _DisposingReceiver extends Capability {
  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    for (final c in paramsCapabilities) {
      await c.dispose();
    }
    return DispatchResult(
      payload: RpcPayload.fromBytes(_buildEchoParams('ok')),
    );
  }

  @override
  Future<void> dispose() async {}
}

class CapReceivingServer extends Capability {
  List<Capability> lastParams = const [];

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    lastParams = List.of(paramsCapabilities);
    return DispatchResult(
      payload: RpcPayload.fromBytes(_buildEchoParams('ok')),
    );
  }

  @override
  Future<void> dispose() async {}
}

// Combines CapReceivingServer's params-recording (_echoMethodId) with
// PipelineServer's result-capability-returning (_pipelineMethodId, returning
// `caps: [leaf]`) and dispose-count tracking on itself — used to probe
// _ReceiverAnswerCapability's handling of a capability that's actually this
// same vat's own prior answer, handed back via a raw receiverAnswer
// capability descriptor.
class ReceiverAnswerProbeServer extends Capability {
  final Capability leaf;
  ReceiverAnswerProbeServer(this.leaf);

  List<Capability> lastParams = const [];
  int disposeCount = 0;

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    if (methodId == _echoMethodId) {
      lastParams = List.of(paramsCapabilities);
      return DispatchResult(
        payload: RpcPayload.fromBytes(_buildEchoParams('ok')),
      );
    }
    if (methodId == _pipelineMethodId) {
      final mb = MessageBuilder();
      final root = mb.initRoot(_TextParamFactory());
      root.setCapabilityField(0, 0);
      return DispatchResult(
        payload: RpcPayload.fromBuilder(root),
        caps: [leaf],
      );
    }
    throw RpcException('unknown method: $methodId');
  }

  @override
  Future<void> dispose() async {
    disposeCount++;
  }
}

// Records whether dispose() ever runs while a dispatch() call through this
// same capability is still in flight — used to empirically check whether
// _ReceiverAnswerCapability's dispose() can tear down its resolved target
// out from under a still-running dispatch() (see the
// "does not dispose the resolved target while a call is in flight" test
// below). dispatch() blocks on `dispatchGate` until released, giving a wide
// window for a concurrent dispose() to land.
class DisposeOrderProbeCapability extends Capability {
  Completer<void> dispatchGate = Completer<void>();
  int _dispatchInFlight = 0;
  bool disposedWhileDispatchInFlight = false;
  int disposeCount = 0;

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    _dispatchInFlight++;
    try {
      await dispatchGate.future;
      return DispatchResult(
        payload: RpcPayload.fromBytes(_buildEchoParams('ok')),
      );
    } finally {
      _dispatchInFlight--;
    }
  }

  @override
  Future<void> dispose() async {
    disposeCount++;
    if (_dispatchInFlight > 0) disposedWhileDispatchInFlight = true;
  }
}

// #53: tail calls. On _tailCallMethodId, redirects the entire dispatch to
// paramsCapabilities[0]'s echo method instead of running its own dispatch.
const int _tailCallMethodId = 8;

class TailCallServer extends Capability {
  @override
  TailCallRequest? tryTailCall(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) {
    if (methodId != _tailCallMethodId) return null;
    return TailCallRequest(
      paramsCapabilities[0],
      _echoInterfaceId,
      _echoMethodId,
      RpcPayload.fromBytes(_buildEchoParams('via tail call')),
    );
  }

  // Used only for the bootstrap "warmup" call (methodId == _echoMethodId);
  // _tailCallMethodId is always intercepted by tryTailCall above and never
  // reaches here.
  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    if (methodId == _echoMethodId) {
      final message =
          params.getTyped(_TextParamFactory()).getTextField(0) ?? '';
      return DispatchResult(
        payload: RpcPayload.fromBytes(_buildEchoParams('echo: $message')),
      );
    }
    throw RpcException('method $methodId should have been tail-called');
  }

  @override
  Future<void> dispose() async {}
}

// #53: tail calls, fallback path. On _tailCallLocalMethodId, redirects to a
// capability that is NOT a same-connection import (a plain local instance),
// exercising the transparent-proxy fallback rather than the wire optimization.
const int _tailCallLocalMethodId = 9;

class TailCallLocalServer extends Capability {
  final Capability local;
  TailCallLocalServer(this.local);

  @override
  TailCallRequest? tryTailCall(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) {
    if (methodId != _tailCallLocalMethodId) return null;
    return TailCallRequest(
      local,
      _echoInterfaceId,
      _echoMethodId,
      RpcPayload.fromBytes(_buildEchoParams('local tail call')),
    );
  }

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) => Future.error(
    RpcException('method $methodId should have been tail-called'),
  );

  @override
  Future<void> dispose() async {}
}

class ThrowingTryTailCallServer extends Capability {
  @override
  TailCallRequest? tryTailCall(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) {
    if (methodId == _throwingTryTailCallMethodId) {
      throw const RpcException('tryTailCall failed synchronously');
    }
    return null;
  }

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    if (methodId == _echoMethodId) {
      return DispatchResult(
        payload: RpcPayload.fromBytes(_buildEchoParams('still usable')),
      );
    }
    throw RpcException('unexpected dispatch: $methodId');
  }

  @override
  Future<void> dispose() async {}
}

class ThrowingEchoServer extends Capability {
  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) {
    throw const RpcException('tail target failed synchronously');
  }

  @override
  Future<void> dispose() async {}
}

class EqualEchoCapability extends Capability {
  final String name;
  EqualEchoCapability(this.name);

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    final message = params.getTyped(_TextParamFactory()).getTextField(0) ?? '';
    return DispatchResult(
      payload: RpcPayload.fromBytes(_buildEchoParams('$name: $message')),
    );
  }

  @override
  bool operator ==(Object other) => other is EqualEchoCapability;

  @override
  int get hashCode => 1;

  @override
  Future<void> dispose() async {}
}

class EqualCapsServer extends Capability {
  final Capability left = EqualEchoCapability('left');
  final Capability right = EqualEchoCapability('right');

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    if (methodId == _equalCapsMethodId) {
      final mb = MessageBuilder();
      final root = mb.initRoot(_TwoPtrFactory());
      root.setCapabilityField(0, 0);
      root.setCapabilityField(1, 1);
      return DispatchResult(
        payload: RpcPayload.fromBuilder(root),
        caps: [left, right],
      );
    }
    if (methodId == _echoMethodId) {
      return DispatchResult(
        payload: RpcPayload.fromBytes(_buildEchoParams('ok')),
      );
    }
    throw RpcException('unknown method: $methodId');
  }

  @override
  Future<void> dispose() async {}
}

class SlowEchoServer extends Capability {
  final Completer<void> started = Completer<void>();
  final Completer<void> canceled = Completer<void>();
  final Completer<void> complete = Completer<void>();
  DispatchCancellationContext? lastContext;

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    if (!started.isCompleted) started.complete();
    await complete.future;
    return DispatchResult(
      payload: RpcPayload.fromBytes(_buildEchoParams('done')),
    );
  }

  @override
  Future<DispatchResult> dispatchWithContext(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
    DispatchCancellationContext? context,
  }) {
    final dispatchContext =
        context ?? DispatchCancellationContext.neverCanceled;
    lastContext = dispatchContext;
    dispatchContext.canceled.then((_) {
      if (!canceled.isCompleted) canceled.complete();
    }).ignore();
    return dispatch(
      interfaceId,
      methodId,
      params,
      paramsCapabilities: paramsCapabilities,
    );
  }

  @override
  Future<void> dispose() async {}
}

// A server whose dispatch stays pending until complete() is called, and
// then resolves with a result carrying [resultCaps] (which may repeat the
// same instance, to test dedup) — used to test that a dispatch result
// discarded before it could be sent as a Return (connection closed, or a
// Finish canceled the answer first) still gets its capabilities disposed
// instead of leaked.
class SlowCapResultServer extends Capability {
  final Completer<void> started = Completer<void>();
  final Completer<void> complete = Completer<void>();
  final List<Capability> resultCaps;

  SlowCapResultServer(this.resultCaps);

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    if (!started.isCompleted) started.complete();
    await complete.future;
    final mb = MessageBuilder();
    final root = mb.initRoot(_TwoPtrFactory());
    for (var i = 0; i < resultCaps.length; i++) {
      root.setCapabilityField(i, i);
    }
    return DispatchResult(
      payload: RpcPayload.fromBuilder(root),
      caps: resultCaps,
    );
  }

  @override
  Future<void> dispose() async {}
}

// A server whose calls each stay pending until individually released via
// completeNext(), in call order — used to control exactly when each
// streaming call's "ack" (Return) lands, to test StreamingCallFlowController windowing
// deterministically.
class QueuedSlowServer extends Capability {
  final List<Completer<DispatchResult>> _pending = [];
  int dispatchCount = 0;

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) {
    dispatchCount++;
    final c = Completer<DispatchResult>();
    _pending.add(c);
    return c.future;
  }

  /// Completes the oldest still-pending call successfully, allowing its
  /// Return to be sent.
  void completeNext() {
    if (_pending.isNotEmpty) {
      _pending.removeAt(0).complete(DispatchResult.empty);
    }
  }

  /// Fails the oldest still-pending call, causing a Return-exception to be
  /// sent for it.
  void failNext(Object error) {
    if (_pending.isNotEmpty) _pending.removeAt(0).completeError(error);
  }

  @override
  Future<void> dispose() async {}
}

Future<void> _waitForRelease(List<Uint8List> captured) async {
  for (var i = 0; i < 20; i++) {
    if (captured
        .map(parseRpcMessage)
        .any((m) => m.type == RpcMessageType.release)) {
      return;
    }
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
  throw TestFailure('no Release message captured');
}

List<RpcMessage> _releaseMessages(List<Uint8List> captured) =>
    captured
        .map(parseRpcMessage)
        .where((m) => m.type == RpcMessageType.release)
        .toList();

Future<void> _waitForReleaseCount(
  List<Uint8List> captured,
  int expectedCount,
) async {
  for (var i = 0; i < 20; i++) {
    if (_releaseMessages(captured).length >= expectedCount) return;
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
  throw TestFailure('expected $expectedCount Release messages');
}

Future<RpcMessage> _waitForMessageType(
  List<Uint8List> captured,
  RpcMessageType type,
) async {
  for (var i = 0; i < 20; i++) {
    final messages = captured.map(parseRpcMessage).where((m) => m.type == type);
    if (messages.isNotEmpty) return messages.last;
    await Future<void>.delayed(const Duration(milliseconds: 10));
  }
  throw TestFailure('no $type message captured');
}

/// Polls [condition] until it's true, instead of guessing how long some
/// async processing takes with a fixed delay. Use whenever there's a
/// concrete, already-exposed piece of state to wait for (a debug counter,
/// a captured message, ...).
Future<void> _waitUntil(
  bool Function() condition, {
  Duration timeout = const Duration(seconds: 2),
}) async {
  final deadline = DateTime.now().add(timeout);
  while (!condition()) {
    if (DateTime.now().isAfter(deadline)) {
      throw TimeoutException('condition was not reached within $timeout');
    }
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

void main() {
  group('rpc_message_codec — RPC-001 promisedAnswer encoding', () {
    test('buildCallMessage with promisedAnswer target encodes disc=1', () {
      final mb = MessageBuilder();
      mb.initRoot(_TextParamFactory()).setTextField(0, 'x');
      final params = mb.serialize();

      final bytes = buildCallMessage(
        questionId: 5,
        targetPromisedAnswerQid: 3,
        targetTransformPath: [0],
        interfaceId: 0xABCD,
        methodId: 1,
        paramsBytes: params,
      );
      final msg = parseRpcMessage(bytes);
      expect(msg.type, RpcMessageType.call);
      expect(msg.questionId, 5);
      expect(msg.targetIsPromisedAnswer, isTrue);
      expect(msg.targetPromisedAnswerQid, 3);
      expect(msg.targetTransformPath, [0]);
    });

    test('importedCap target still parses correctly', () {
      final mb = MessageBuilder();
      mb.initRoot(_TextParamFactory());
      final params = mb.serialize();

      final bytes = buildCallMessage(
        questionId: 7,
        targetImportId: 42,
        interfaceId: 0x1234,
        methodId: 0,
        paramsBytes: params,
      );
      final msg = parseRpcMessage(bytes);
      expect(msg.targetIsPromisedAnswer, isFalse);
      expect(msg.targetImportId, 42);
    });

    test('a multi-hop promisedAnswer transform round-trips every hop, not '
        'just the first (regression: the parser used to read only '
        'transform[0])', () {
      final mb = MessageBuilder();
      mb.initRoot(_TextParamFactory()).setTextField(0, 'x');
      final params = mb.serialize();

      final bytes = buildCallMessage(
        questionId: 5,
        targetPromisedAnswerQid: 3,
        targetTransformPath: [0, 2, 1],
        interfaceId: 0xABCD,
        methodId: 1,
        paramsBytes: params,
      );
      final msg = parseRpcMessage(bytes);
      expect(msg.targetTransformPath, [0, 2, 1]);
    });

    test(
      'a receiverAnswer CapDescriptor with a multi-hop transform round-trips '
      'every hop',
      () {
        final mb = MessageBuilder();
        mb.initRoot(_TextParamFactory());
        final params = mb.serialize();

        final bytes = buildCallMessage(
          questionId: 1,
          targetImportId: 0,
          interfaceId: 0xABCD,
          methodId: 0,
          paramsBytes: params,
          capabilityTableReferences: const [
            ReceiverAnswerCapabilityReference(9, [0, 2, 1]),
          ],
        );
        final msg = parseRpcMessage(bytes);
        expect(
          msg.capabilityTableReferences.single,
          isA<ReceiverAnswerCapabilityReference>().having(
            (r) => r.transformPath,
            'transformPath',
            equals([0, 2, 1]),
          ),
        );
      },
    );
  });

  group('capability — receiverAnswer multi-hop resolution (#59)', () {
    test('requireCapabilityFromResultPath walks every hop to reach a '
        'capability nested two structs deep (regression: resolving via just '
        'the first hop would find a struct pointer, not a capability, and '
        'fail)', () async {
      // result = (outer: (inner: (cap: <capability 0>)))
      // i.e. a capability reachable at result.getPointerField(0)
      //        .getPointerField(1), two hops deep — field 0 of the root
      // is itself a struct (not a capability), so resolving via path [0]
      // alone (the pre-fix behavior) would hit a struct pointer and fail
      // with "not a capability", while [0, 1] correctly reaches it.
      final mb = MessageBuilder();
      final root = mb.initRoot(_TwoPtrFactory());
      final inner = root.initStructFieldWith(0, _TextParamBuilder.new, 0, 2);
      inner.setCapabilityField(1, 0);
      final bytes = mb.serialize();

      final result = DispatchResult(
        payload: RpcPayload.fromBytes(bytes),
        caps: [NullCapability()],
      );

      // The old single-hop behavior: resolving pointer slot 0 of the root
      // directly hits a struct pointer, not a capability.
      expect(
        () => requireCapabilityFromResultPath(result, const [0]),
        throwsA(
          isA<RpcException>().having(
            (e) => e.message,
            'message',
            contains('not a capability'),
          ),
        ),
      );

      // The fixed multi-hop behavior: [0, 1] walks into the nested
      // struct first, then resolves the capability at its slot 1 — proven
      // by delegating to the same underlying NullCapability (whose
      // dispatch always fails with this specific message), rather than
      // throwing "not a capability" like the single-hop path above did.
      final cap = requireCapabilityFromResultPath(result, const [0, 1]);
      await expectLater(
        cap.dispatch(0, 0, RpcPayload.fromBytes(Uint8List(0))),
        throwsA(
          isA<RpcException>().having(
            (e) => e.message,
            'message',
            'null capability',
          ),
        ),
      );
    });

    test(
      'tryGetCapabilityFromResultPath returns null (not throw) for an empty path',
      () {
        final mb = MessageBuilder();
        final root = mb.initRoot(_TwoPtrFactory());
        final result = DispatchResult(
          payload: RpcPayload.fromBuilder(root),
          caps: const [],
        );
        expect(tryGetCapabilityFromResultPath(result, const []), isNull);
      },
    );
  });

  group('Capability.dispatchWithParamsBuilder — Stage 3A zero-copy send path', () {
    test(
      'local (default) dispatchWithParamsBuilder round-trips through EchoServer',
      () async {
        final server = EchoServer();
        final result = await server.dispatchWithParamsBuilder(
          _echoInterfaceId,
          _echoMethodId,
          (anyPtr) =>
              anyPtr.initStruct(_TextParamFactory()).setTextField(0, 'hi'),
        );
        expect(_parseEchoResult(result.payload), 'echo: hi');
      },
    );

    test(
      'RPC-connected dispatchWithParamsBuilder (_ImportedCapability) builds params '
      'directly into the outgoing Call and round-trips',
      () async {
        final (client, serverConn) = _makePipe(EchoServer());
        final bootstrapCap = client.bootstrap(EchoClientFactory());
        await bootstrapCap.echo('warmup');

        final result = await bootstrapCap.cap.dispatchWithParamsBuilder(
          _echoInterfaceId,
          _echoMethodId,
          (anyPtr) => anyPtr
              .initStruct(_TextParamFactory())
              .setTextField(0, 'via dispatchWithParamsBuilder'),
        );
        expect(
          _parseEchoResult(result.payload),
          'echo: via dispatchWithParamsBuilder',
        );

        await client.close();
        await serverConn.close();
      },
    );

    test(
      'dispatchWithParamsBuilder paramsCapabilities populated during build is read '
      'correctly by the callee',
      () async {
        final child = EchoServer();
        final server = CapReceivingServer();
        final (client, serverConn) = _makePipe(server);
        final bootstrapCap = client.bootstrap(EchoClientFactory());
        await bootstrapCap.echo('warmup');

        final typedCapabilities = <Capability>[];
        final result = await bootstrapCap.cap.dispatchWithParamsBuilder(
          _echoInterfaceId,
          _echoMethodId,
          (anyPtr) {
            // Simulates a Typed field encoder discovering a capability as a
            // side effect of encoding — appended to paramsCapabilities only
            // once build starts running, exactly like a real
            // setXxxTyped(codec, value, capabilities: typedCapabilities)
            // call would.
            typedCapabilities.add(child);
            anyPtr.initStruct(_TextParamFactory()).setTextField(0, 'x');
          },
          paramsCapabilities: typedCapabilities,
        );
        expect(_parseEchoResult(result.payload), 'ok');
        expect(server.lastParams.length, 1);

        await client.close();
        await serverConn.close();
      },
    );
  });

  group('rpc_message_codec — #53 tail call (Level 1) wire encoding', () {
    test('buildCallMessage(sendResultsToYourself: true) round-trips', () {
      final bytes = buildCallMessage(
        questionId: 9,
        targetImportId: 1,
        interfaceId: _echoInterfaceId,
        methodId: _echoMethodId,
        paramsBytes: _buildEchoParams('x'),
        sendResultsToYourself: true,
      );
      final msg = parseRpcMessage(bytes);
      expect(msg.type, RpcMessageType.call);
      expect(msg.sendResultsToDisc, 1);
    });

    test('buildCallMessage default (no sendResultsToYourself) still encodes '
        'disc=0 (caller)', () {
      final bytes = buildCallMessage(
        questionId: 9,
        targetImportId: 1,
        interfaceId: _echoInterfaceId,
        methodId: _echoMethodId,
        paramsBytes: _buildEchoParams('x'),
      );
      final msg = parseRpcMessage(bytes);
      expect(msg.sendResultsToDisc, 0);
    });

    test('buildReturnTakeFromOtherQuestionMessage round-trips the redirect '
        'question id', () {
      final bytes = buildReturnTakeFromOtherQuestionMessage(
        answerId: 7,
        questionId: 0x12345678,
      );
      final msg = parseRpcMessage(bytes);
      expect(msg.type, RpcMessageType.return_);
      expect(msg.answerId, 7);
      expect(msg.isReturnTakeFromOtherQuestion, isTrue);
      expect(msg.takeFromOtherQuestion, 0x12345678);
      expect(msg.isReturnResults, isFalse);
      expect(msg.isReturnException, isFalse);
      // Byte-offset placement (union disc=4, UInt32 payload at data-section
      // offset 8) was independently verified against the real `capnp` CLI
      // during development:
      //   echo '(answerId = 7, takeFromOtherQuestion = 305419896)' |
      //     capnp encode rpc.capnp Return | od -An -tx1
      // which confirmed the payload lands at byte offset 8 — matching the
      // `_returnTakeFromOtherQuestionOff` constant these builders use.
    });

    test('buildReturnResultsSentElsewhereMessage round-trips with no '
        'payload', () {
      final bytes = buildReturnResultsSentElsewhereMessage(answerId: 11);
      final msg = parseRpcMessage(bytes);
      expect(msg.type, RpcMessageType.return_);
      expect(msg.answerId, 11);
      expect(msg.isReturnResults, isFalse);
      expect(msg.isReturnException, isFalse);
      expect(msg.isReturnTakeFromOtherQuestion, isFalse);
      expect(describeReturnVariant(msg.returnDisc), 'resultsSentElsewhere');
    });
  });

  group(
    'TwoPartyRpcConnection — #53 tail calls (Level 1 wire optimization)',
    () {
      test(
        'synchronous tryTailCall failure becomes a Return exception and keeps '
        'the connection usable',
        () async {
          final (client, serverConn) = _makePipe(ThrowingTryTailCallServer());
          final bootstrapCap = client.bootstrap(EchoClientFactory());

          await expectLater(
            bootstrapCap.cap.dispatch(
              _echoInterfaceId,
              _throwingTryTailCallMethodId,
              RpcPayload.fromBytes(_buildEchoParams('')),
            ),
            throwsA(
              allOf(
                isA<RpcException>(),
                predicate<Object>(
                  (e) => e.toString().contains('tryTailCall failed'),
                ),
              ),
            ),
          );

          final result = await bootstrapCap.cap.dispatch(
            _echoInterfaceId,
            _echoMethodId,
            RpcPayload.fromBytes(_buildEchoParams('after')),
          );
          expect(_parseEchoResult(result.payload), 'still usable');

          await client.close();
          await serverConn.close();
        },
      );

      test('tail call to a same-connection import avoids a second results '
          'round trip', () async {
        // target is hosted on the CLIENT; TailCallServer (on the server)
        // receives it as an import and tail-calls into it.
        final target = EchoServer();

        final s2c = StreamController<Uint8List>();
        final c2s = StreamController<Uint8List>();
        final serverCaptured = <RpcMessage>[];
        final clientCaptured = <RpcMessage>[];
        final serverOutgoing =
            StreamController<Uint8List>()
              ..stream.listen((b) {
                serverCaptured.add(parseRpcMessage(b));
                s2c.add(b);
              });
        final clientOutgoing =
            StreamController<Uint8List>()
              ..stream.listen((b) {
                clientCaptured.add(parseRpcMessage(b));
                c2s.add(b);
              });

        TwoPartyRpcConnection.server(
          incoming: c2s.stream,
          outgoing: serverOutgoing.sink,
          bootstrap: TailCallServer(),
        );
        final client = TwoPartyRpcConnection.client(
          incoming: s2c.stream,
          outgoing: clientOutgoing.sink,
        );

        final bootstrapCap = client.bootstrap(EchoClientFactory());
        await bootstrapCap.echo('warmup');

        serverCaptured.clear();
        clientCaptured.clear();

        final result = await bootstrapCap.cap.dispatch(
          _echoInterfaceId,
          _tailCallMethodId,
          RpcPayload.fromBytes(_buildEchoParams('')),
          paramsCapabilities: [target],
        );
        expect(_parseEchoResult(result.payload), 'echo: via tail call');

        // The server answered the original call with takeFromOtherQuestion,
        // not a full results payload — exactly one Return, redirecting.
        final serverReturns =
            serverCaptured
                .where((m) => m.type == RpcMessageType.return_)
                .toList();
        expect(serverReturns.length, 1);
        expect(serverReturns.single.isReturnTakeFromOtherQuestion, isTrue);

        // The server forwarded a new Call, flagged sendResultsTo=yourself,
        // targeting the import id it was given for `target` — and its
        // question id is exactly what the takeFromOtherQuestion Return
        // pointed at.
        final serverCalls =
            serverCaptured.where((m) => m.type == RpcMessageType.call).toList();
        expect(serverCalls.length, 1);
        expect(serverCalls.single.sendResultsToDisc, 1);
        expect(
          serverCalls.single.questionId,
          serverReturns.single.takeFromOtherQuestion,
        );

        // The client (as the forwarded call's dispatcher) answered it with
        // resultsSentElsewhere, not a normal results Return.
        final clientReturns =
            clientCaptured
                .where((m) => m.type == RpcMessageType.return_)
                .toList();
        expect(clientReturns.length, 1);
        expect(
          describeReturnVariant(clientReturns.single.returnDisc),
          'resultsSentElsewhere',
        );

        await client.close();
      });

      test('tryTailCall target that is not a same-connection import falls back '
          'to a transparent proxy dispatch', () async {
        final (client, serverConn) = _makePipe(
          TailCallLocalServer(EchoServer()),
        );
        final bootstrapCap = client.bootstrap(EchoClientFactory());

        final result = await bootstrapCap.cap.dispatch(
          _echoInterfaceId,
          _tailCallLocalMethodId,
          RpcPayload.fromBytes(_buildEchoParams('')),
        );
        expect(_parseEchoResult(result.payload), 'echo: local tail call');

        await client.close();
      });

      test('an incoming Call flagged sendResultsTo=yourself is answered with '
          'resultsSentElsewhere, independent of tryTailCall', () async {
        // Raw wire injection (no tryTailCall involved on either side) —
        // proves the *receiving* half of the mechanism, which matters for
        // interop with a peer (e.g. a real capnp implementation) that
        // tail-calls into this vat.
        final clientToServer = StreamController<Uint8List>();
        final serverToClient = StreamController<Uint8List>();
        final captured = <RpcMessage>[];
        serverToClient.stream.listen(
          (bytes) => captured.add(parseRpcMessage(bytes)),
        );

        TwoPartyRpcConnection.server(
          incoming: clientToServer.stream,
          outgoing: serverToClient.sink,
          bootstrap: EchoServer(),
        );

        clientToServer.add(
          buildCallMessage(
            questionId: 1,
            targetImportId: 0,
            interfaceId: _echoInterfaceId,
            methodId: _echoMethodId,
            paramsBytes: _buildEchoParams('hi'),
            sendResultsToYourself: true,
          ),
        );
        await Future<void>.delayed(const Duration(milliseconds: 20));

        final returns =
            captured.where((m) => m.type == RpcMessageType.return_).toList();
        expect(returns.length, 1);
        expect(
          describeReturnVariant(returns.single.returnDisc),
          'resultsSentElsewhere',
        );
        expect(returns.single.resultsContent, isNull);

        await clientToServer.close();
      });

      test(
        'takeFromOtherQuestion preserves the forwarded call failure reason',
        () async {
          final target = ThrowingEchoServer();
          final (client, serverConn) = _makePipe(TailCallServer());
          final bootstrapCap = client.bootstrap(EchoClientFactory());
          await bootstrapCap.echo('warmup');

          await expectLater(
            bootstrapCap.cap.dispatch(
              _echoInterfaceId,
              _tailCallMethodId,
              RpcPayload.fromBytes(_buildEchoParams('')),
              paramsCapabilities: [target],
            ),
            throwsA(
              allOf(
                isA<RpcException>(),
                predicate<Object>(
                  (e) =>
                      e.toString().contains('tail target failed') &&
                      !e.toString().contains('unknown question id'),
                ),
              ),
            ),
          );

          await client.close();
          await serverConn.close();
        },
      );

      test('pipelining onto an answer resolved via takeFromOtherQuestion fails '
          'clearly, not silently — a documented limitation', () async {
        final target = EchoServer();
        final (client, serverConn) = _makePipe(TailCallServer());
        final bootstrapCap = client.bootstrap(EchoClientFactory());
        await bootstrapCap.echo('warmup');

        final call = bootstrapCap.cap.dispatchForPipelining(
          _echoInterfaceId,
          _tailCallMethodId,
          RpcPayload.fromBytes(_buildEchoParams('')),
          paramsCapabilities: [target],
        );
        final pipelinedCap = call.pipelinedCapability(0);

        await expectLater(
          pipelinedCap.dispatch(
            _echoInterfaceId,
            _echoMethodId,
            RpcPayload.fromBytes(_buildEchoParams('x')),
          ),
          throwsA(isA<RpcException>()),
        );

        // The original tail call itself must still complete correctly —
        // the failed pipeline attempt must not have corrupted it.
        final result = await call.result;
        expect(_parseEchoResult(result.payload), 'echo: via tail call');

        await client.close();
      });

      test('a tail-call-forwarded original call fails with a connection '
          'error on teardown, exactly like every other pending call -- even '
          'though the (uncancelled) forwarded dispatch keeps running '
          'locally in the background', () async {
        // target is hosted on the CLIENT; TailCallServer (on the server)
        // receives it as an import and tail-calls into it -- same wiring
        // as "tail call to a same-connection import" above, but with a
        // target whose dispatch never finishes on its own, to observe
        // what happens if the connection dies while it's still running.
        final target = SlowEchoServer();

        final s2c = StreamController<Uint8List>();
        final c2s = StreamController<Uint8List>();
        final serverCaptured = <Uint8List>[];
        final serverOutgoing =
            StreamController<Uint8List>()
              ..stream.listen(
                (b) {
                  serverCaptured.add(b);
                  s2c.add(b);
                },
                onDone: s2c.close,
                onError: s2c.addError,
              );
        final clientOutgoing =
            StreamController<Uint8List>()
              ..stream.listen(
                c2s.add,
                onDone: c2s.close,
                onError: c2s.addError,
              );

        final serverConn = TwoPartyRpcConnection.server(
          incoming: c2s.stream,
          outgoing: serverOutgoing.sink,
          bootstrap: TailCallServer(),
        );
        final client = TwoPartyRpcConnection.client(
          incoming: s2c.stream,
          outgoing: clientOutgoing.sink,
        );

        final bootstrapCap = client.bootstrap(EchoClientFactory());
        await bootstrapCap.echo('warmup');
        serverCaptured.clear();

        final callFuture = bootstrapCap.cap.dispatch(
          _echoInterfaceId,
          _tailCallMethodId,
          RpcPayload.fromBytes(_buildEchoParams('')),
          paramsCapabilities: [target],
        );

        await target.started.future.timeout(const Duration(seconds: 2));
        final redirect = await _waitForMessageType(
          serverCaptured,
          RpcMessageType.return_,
        );
        expect(redirect.isReturnTakeFromOtherQuestion, isTrue);

        // _waitForMessageType only confirms the SERVER sent the redirect —
        // not that the CLIENT has received and processed it yet (its own
        // async deframing/dispatch pipeline still needs a beat). Closing
        // too early would cancel the incoming subscription while those
        // already-in-flight bytes are still queued, discarding them
        // before _awaitAndProcessReturn ever gets to call _resolveLocalAnswer —
        // which would make this test exercise "outgoing question dropped
        // before its Return arrived" (already covered elsewhere) instead
        // of the tail-call-specific race this test is about. Waiting for
        // debugPendingQuestionCount to drop is deterministic here:
        // _handleReturn's very first line (QuestionTable.takeReturn)
        // clears this synchronously, before _awaitAndProcessReturn's own
        // _resolveLocalAnswer continuation even starts running.
        await _waitUntil(() => client.debugPendingQuestionCount == 0);

        // Fixed, was https://github.com/AngryMane/capnproto-dart/issues/99:
        // AnswerTable.tornDown races the raw dispatch Future
        // _awaitAndProcessReturn extracted via resolveLocalAnswer against the
        // connection tearing down, so the original call observes the same
        // disconnection failure every other still-pending call gets on
        // teardown -- instead of staying pending and later succeeding from
        // purely local state after the connection that correlated it is
        // already gone. This does not abort the forwarded dispatch itself:
        // SlowEchoServer still legally ignores cancellation and keeps
        // running (isCanceled is still observed true, cooperative
        // cancellation is still requested exactly as before) -- only what
        // the *original caller* is told about it changes.
        //
        // expectLater is started (not awaited) *before* close() below, not
        // after: callFuture's whole chain settles synchronously-ish inside
        // close() once AnswerTable.tearDown runs, and attaching a listener
        // only afterward would leave a real window where it has already
        // rejected with nothing listening yet -- which Dart reports as an
        // unhandled async error regardless of a listener arriving moments
        // later, independent of anything AnswerTable/IncomingCallCoordinator
        // do differently on their own end.
        final callFailsWithConnectionError = expectLater(
          callFuture,
          throwsA(
            isA<RpcException>().having(
              (error) => error.kind,
              'kind',
              ErrorKind.disconnected,
            ),
          ),
        );

        await client.close();
        await serverConn.done.catchError((_) {});
        await callFailsWithConnectionError;
        expect(target.lastContext?.isCanceled, isTrue);

        // The forwarded dispatch itself keeps running in the background
        // regardless -- completing it late (after the original caller has
        // already moved on) must not throw or otherwise misbehave.
        target.complete.complete();
        await Future<void>.delayed(const Duration(milliseconds: 20));
      });
    },
  );

  group('TwoPartyRpcConnection — capability identity', () {
    test('export table keys use object identity, not operator ==', () async {
      final (client, serverConn) = _makePipe(EqualCapsServer());
      final bootstrapCap = client.bootstrap(EchoClientFactory());
      await bootstrapCap.echo('warmup');

      final result = await bootstrapCap.cap.dispatch(
        _echoInterfaceId,
        _equalCapsMethodId,
        RpcPayload.fromBytes(_buildEchoParams('')),
      );
      final left = requireCapabilityFromResult(result, 0);
      final right = requireCapabilityFromResult(result, 1);

      final leftReply = await left.dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('one')),
      );
      final rightReply = await right.dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('two')),
      );

      expect(_parseEchoResult(leftReply.payload), 'left: one');
      expect(_parseEchoResult(rightReply.payload), 'right: two');

      await client.close();
      await serverConn.close();
    });
  });

  group('TwoPartyRpcConnection — RPC-001 wire-level promise pipelining', () {
    test('two pipelined calls are sent before first Return arrives', () async {
      // Intercept all bytes from client to server.
      final clientToServer = StreamController<Uint8List>();
      final serverToClient = StreamController<Uint8List>();
      final captured = <Uint8List>[];

      final interceptSink =
          StreamController<Uint8List>()
            ..stream.listen((b) {
              captured.add(b);
              clientToServer.add(b);
            });

      final server = PipelineServer();
      TwoPartyRpcConnection.server(
        incoming: clientToServer.stream,
        outgoing: serverToClient.sink,
        bootstrap: server,
      );
      final client = TwoPartyRpcConnection.client(
        incoming: serverToClient.stream,
        outgoing: interceptSink.sink,
      );

      final bootstrapCap = client.bootstrap(EchoClientFactory());
      await bootstrapCap.echo('warmup'); // complete bootstrap

      // Call getPipeline (returns a cap) and immediately call echo on the
      // pipelined result — both should be sent without waiting for getPipeline
      // to complete.
      captured.clear();
      final call = bootstrapCap.cap.dispatchForPipelining(
        _echoInterfaceId,
        _pipelineMethodId,
        RpcPayload.fromBytes(_buildEchoParams('')),
      );
      final pipelinedCap = call.pipelinedCapability(0);

      // Dispatch a second call on the pipelined cap before the first returns.
      final secondCall = pipelinedCap.dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('hi')),
      );

      // Await both to complete the exchange.
      await call.result;
      await secondCall;

      // Verify both Call messages were sent as a batch (before any Return).
      final calls =
          captured
              .map(parseRpcMessage)
              .where((m) => m.type == RpcMessageType.call)
              .toList();
      expect(calls.length, greaterThanOrEqualTo(2));

      // The second call must target promisedAnswer, not importedCap.
      final pipelinedCall = calls.firstWhere(
        (m) => m.targetIsPromisedAnswer,
        orElse:
            () =>
                throw TestFailure(
                  'no promisedAnswer-targeted call found in captured messages',
                ),
      );
      expect(pipelinedCall.targetPromisedAnswerQid, calls.first.questionId);
      expect(pipelinedCall.targetTransformPath, [0]);

      await client.close();
    });

    test(
      'pipelined call on ptr slot 1 (cap after non-cap slot) resolves correctly',
      () async {
        // This is the RPC-001 regression test.
        // The result struct has slot 0 = null (not a cap) and slot 1 = capability.
        // ptrIndex=1 must resolve to caps[0], not caps[1] (which would be OOB).
        final server = MixedResultServer();
        final (client, serverConn) = _makePipe(server);
        final bootstrapCap = client.bootstrap(EchoClientFactory());
        await bootstrapCap.echo('warmup');

        final call = bootstrapCap.cap.dispatchForPipelining(
          _echoInterfaceId,
          _mixedMethodId,
          RpcPayload.fromBytes(_buildEchoParams('')),
        );
        // Pipeline onto ptr slot 1 (not slot 0), where the capability lives.
        final pipelinedCap = call.pipelinedCapability(1);

        final secondResult = await pipelinedCap.dispatch(
          _echoInterfaceId,
          _echoMethodId,
          RpcPayload.fromBytes(_buildEchoParams('piped')),
        );
        expect(_parseEchoResult(secondResult.payload), equals('echo: piped'));

        await client.close();
        await serverConn.close();
      },
    );

    test(
      'pipelined capability parameter is encoded as receiverAnswer',
      () async {
        final clientToServer = StreamController<Uint8List>();
        final serverToClient = StreamController<Uint8List>();
        final captured = <Uint8List>[];
        final completeParent = Completer<void>();

        final interceptSink =
            StreamController<Uint8List>()
              ..stream.listen((b) {
                captured.add(b);
                clientToServer.add(b);
              });

        TwoPartyRpcConnection.server(
          incoming: clientToServer.stream,
          outgoing: serverToClient.sink,
          bootstrap: ChildPipelineServer(completer: completeParent),
        );
        final client = TwoPartyRpcConnection.client(
          incoming: serverToClient.stream,
          outgoing: interceptSink.sink,
        );

        final bootstrapCap = client.bootstrap(EchoClientFactory());
        await bootstrapCap.echo('warmup');

        captured.clear();
        final parent = bootstrapCap.cap.dispatchForPipelining(
          _echoInterfaceId,
          _pipelineMethodId,
          RpcPayload.fromBytes(_buildEchoParams('')),
        );
        final pipelinedCap = parent.pipelinedCapability(0);
        final paramCall = bootstrapCap.cap.dispatch(
          _echoInterfaceId,
          _echoMethodId,
          RpcPayload.fromBytes(_buildEchoParams('param')),
          paramsCapabilities: [pipelinedCap],
        );

        final (parentCall, callWithCap) = await () async {
          for (var i = 0; i < 20; i++) {
            final calls =
                captured
                    .map(parseRpcMessage)
                    .where((m) => m.type == RpcMessageType.call)
                    .toList();
            final parentCalls =
                calls.where((m) => m.methodId == _pipelineMethodId).toList();
            final capCalls =
                calls
                    .where((m) => m.capabilityTableReferences.isNotEmpty)
                    .toList();
            if (parentCalls.isNotEmpty && capCalls.isNotEmpty) {
              return (parentCalls.single, capCalls.single);
            }
            await Future<void>.delayed(const Duration(milliseconds: 10));
          }
          throw TestFailure('no Call with capTable captured');
        }();

        final capReference = callWithCap.capabilityTableReferences.single;
        expect(capReference, isA<ReceiverAnswerCapabilityReference>());
        capReference as ReceiverAnswerCapabilityReference;
        expect(capReference.questionId, parentCall.questionId);
        expect(capReference.transformPath, [0]);

        completeParent.complete();
        await parent.result;
        await paramCall;
        await client.close();
        await interceptSink.close();
      },
    );

    test(
      'Capability.dispatchForPipelining on non-RPC cap falls back to DeferredCapability',
      () async {
        final server = EchoServer();
        final call = server.dispatchForPipelining(
          _echoInterfaceId,
          _echoMethodId,
          RpcPayload.fromBytes(_buildEchoParams('test')),
        );
        // pipelinedCapability on a non-RPC cap returns a DeferredCapability.
        final piped = call.pipelinedCapability(0);
        expect(piped, isA<DeferredCapability>());
        // The result future still completes correctly.
        final result = await call.result;
        final text = _parseEchoResult(result.payload);
        expect(text, 'echo: test');
      },
    );

    test('DeferredCapability is locally failed after dispose', () async {
      final completer = Completer<Capability>();
      final deferred = DeferredCapability(completer.future);
      final local = CountingCapability();

      final disposeFuture = deferred.dispose();
      completer.complete(local);
      await disposeFuture;
      await deferred.dispose();

      expect(local.disposeCount, equals(1));

      await expectLater(
        deferred.dispatch(
          _echoInterfaceId,
          _echoMethodId,
          RpcPayload.fromBytes(_buildEchoParams('after-dispose')),
        ),
        throwsA(
          allOf(
            isA<RpcException>(),
            predicate<Object>(
              (e) => e.toString().contains('capability is disposed'),
            ),
          ),
        ),
      );
      expect(local.dispatchCount, equals(0));

      final call = deferred.dispatchForPipelining(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('after-dispose')),
      );
      await expectLater(call.result, throwsA(isA<RpcException>()));
      expect(local.dispatchCount, equals(0));
    });

    test(
      'disposing resolved pipelined capability releases imported cap',
      () async {
        final clientToServer = StreamController<Uint8List>();
        final serverToClient = StreamController<Uint8List>();
        final captured = <Uint8List>[];

        final interceptSink =
            StreamController<Uint8List>()
              ..stream.listen((b) {
                captured.add(b);
                clientToServer.add(b);
              });

        TwoPartyRpcConnection.server(
          incoming: clientToServer.stream,
          outgoing: serverToClient.sink,
          bootstrap: ChildPipelineServer(),
        );
        final client = TwoPartyRpcConnection.client(
          incoming: serverToClient.stream,
          outgoing: interceptSink.sink,
        );

        final bootstrapCap = client.bootstrap(EchoClientFactory());
        await bootstrapCap.echo('warmup');

        captured.clear();
        final call = bootstrapCap.cap.dispatchForPipelining(
          _echoInterfaceId,
          _pipelineMethodId,
          RpcPayload.fromBytes(_buildEchoParams('')),
        );
        final pipelinedCap = call.pipelinedCapability(0);
        await call.result;

        await pipelinedCap.dispose();
        await _waitForRelease(captured);

        final releases =
            captured
                .map(parseRpcMessage)
                .where((m) => m.type == RpcMessageType.release)
                .toList();
        expect(releases, hasLength(1));
        expect(releases.single.referenceCount, equals(1));

        await client.close();
        await interceptSink.close();
      },
    );

    test(
      'disposed imported capability fails locally without sending Call',
      () async {
        final clientToServer = StreamController<Uint8List>();
        final serverToClient = StreamController<Uint8List>();
        final captured = <Uint8List>[];

        final interceptSink =
            StreamController<Uint8List>()
              ..stream.listen((b) {
                captured.add(b);
                clientToServer.add(b);
              });

        TwoPartyRpcConnection.server(
          incoming: clientToServer.stream,
          outgoing: serverToClient.sink,
          bootstrap: PipelineServer(),
        );
        final client = TwoPartyRpcConnection.client(
          incoming: serverToClient.stream,
          outgoing: interceptSink.sink,
        );

        final bootstrapCap = client.bootstrap(EchoClientFactory());
        await bootstrapCap.echo('warmup');

        final call = bootstrapCap.cap.dispatchForPipelining(
          _echoInterfaceId,
          _pipelineMethodId,
          RpcPayload.fromBytes(_buildEchoParams('')),
        );
        final pipelinedCap = call.pipelinedCapability(0);
        await call.result;

        captured.clear();
        await pipelinedCap.dispose();
        await _waitForRelease(captured);

        captured.clear();
        await expectLater(
          pipelinedCap.dispatch(
            _echoInterfaceId,
            _echoMethodId,
            RpcPayload.fromBytes(_buildEchoParams('after-dispose')),
          ),
          throwsA(
            allOf(
              isA<RpcException>(),
              predicate<Object>(
                (e) => e.toString().contains('capability is disposed'),
              ),
            ),
          ),
        );
        await Future<void>.delayed(const Duration(milliseconds: 20));
        expect(
          captured
              .map(parseRpcMessage)
              .where((m) => m.type == RpcMessageType.call),
          isEmpty,
        );

        await client.close();
        await interceptSink.close();
      },
    );

    test(
      'disposing pending pipelined capability releases after parent resolves',
      () async {
        final clientToServer = StreamController<Uint8List>();
        final serverToClient = StreamController<Uint8List>();
        final captured = <Uint8List>[];
        final completeParent = Completer<void>();

        final interceptSink =
            StreamController<Uint8List>()
              ..stream.listen((b) {
                captured.add(b);
                clientToServer.add(b);
              });

        TwoPartyRpcConnection.server(
          incoming: clientToServer.stream,
          outgoing: serverToClient.sink,
          bootstrap: ChildPipelineServer(completer: completeParent),
        );
        final client = TwoPartyRpcConnection.client(
          incoming: serverToClient.stream,
          outgoing: interceptSink.sink,
        );

        final bootstrapCap = client.bootstrap(EchoClientFactory());
        await bootstrapCap.echo('warmup');

        captured.clear();
        final call = bootstrapCap.cap.dispatchForPipelining(
          _echoInterfaceId,
          _pipelineMethodId,
          RpcPayload.fromBytes(_buildEchoParams('')),
        );
        final pipelinedCap = call.pipelinedCapability(0);

        await pipelinedCap.dispose();
        expect(
          captured
              .map(parseRpcMessage)
              .where((m) => m.type == RpcMessageType.release),
          isEmpty,
        );

        completeParent.complete();
        await call.result;
        await _waitForRelease(captured);

        final releases =
            captured
                .map(parseRpcMessage)
                .where((m) => m.type == RpcMessageType.release)
                .toList();
        expect(releases, hasLength(1));
        expect(releases.single.referenceCount, equals(1));

        await client.close();
        await interceptSink.close();
      },
    );

    test(
      'disposing pending pipelined capability waits for in-flight pipelined call',
      () async {
        final clientToServer = StreamController<Uint8List>();
        final serverToClient = StreamController<Uint8List>();
        final captured = <Uint8List>[];
        final completeParent = Completer<void>();
        final uncaught = <Object>[];

        final interceptSink =
            StreamController<Uint8List>()
              ..stream.listen((b) {
                captured.add(b);
                clientToServer.add(b);
              });

        TwoPartyRpcConnection.server(
          incoming: clientToServer.stream,
          outgoing: serverToClient.sink,
          bootstrap: ChildPipelineServer(completer: completeParent),
        );
        final client = TwoPartyRpcConnection.client(
          incoming: serverToClient.stream,
          outgoing: interceptSink.sink,
        );

        final bodyDone = Completer<void>();
        runZonedGuarded(() {
          () async {
            try {
              final bootstrapCap = client.bootstrap(EchoClientFactory());
              await bootstrapCap.echo('warmup');

              captured.clear();
              final parent = bootstrapCap.cap.dispatchForPipelining(
                _echoInterfaceId,
                _pipelineMethodId,
                RpcPayload.fromBytes(_buildEchoParams('')),
              );
              final pipelinedCap = parent.pipelinedCapability(0);
              final pipelinedCall = pipelinedCap.dispatch(
                _echoInterfaceId,
                _echoMethodId,
                RpcPayload.fromBytes(_buildEchoParams('piped')),
              );

              await pipelinedCap.dispose();
              completeParent.complete();

              await parent.result.timeout(const Duration(seconds: 2));
              final pipedResult = await pipelinedCall.timeout(
                const Duration(seconds: 2),
              );
              expect(
                _parseEchoResult(pipedResult.payload),
                equals('echo: piped'),
              );
              await _waitForRelease(captured);
              bodyDone.complete();
            } catch (error, stackTrace) {
              bodyDone.completeError(error, stackTrace);
            }
          }();
        }, (error, _) => uncaught.add(error));

        await bodyDone.future;
        expect(uncaught, isEmpty);

        final releases = _releaseMessages(captured);
        expect(releases, hasLength(1));
        expect(releases.single.referenceCount, equals(1));

        await client.close();
        await interceptSink.close();
      },
    );

    test(
      'parent failure is preserved for resolved pipelined capability',
      () async {
        final server = PipelineServer();
        final (client, serverConn) = _makePipe(server);
        final bootstrapCap = client.bootstrap(EchoClientFactory());
        await bootstrapCap.echo('warmup');

        final call = bootstrapCap.cap.dispatchForPipelining(
          _echoInterfaceId,
          999,
          RpcPayload.fromBytes(_buildEchoParams('')),
        );
        final pipelinedCap = call.pipelinedCapability(0);

        await expectLater(call.result, throwsA(isA<RpcException>()));
        await Future<void>.delayed(Duration.zero);

        await expectLater(
          pipelinedCap.dispatch(
            _echoInterfaceId,
            _echoMethodId,
            RpcPayload.fromBytes(_buildEchoParams('piped')),
          ),
          throwsA(
            allOf(
              isA<RpcException>(),
              predicate<Object>(
                (e) =>
                    e.toString().contains('unknown method') &&
                    !e.toString().contains('null capability'),
              ),
            ),
          ),
        );

        await client.close();
        await serverConn.close();
      },
    );

    test('disposing one duplicate import keeps the other usable', () async {
      final clientToServer = StreamController<Uint8List>();
      final serverToClient = StreamController<Uint8List>();
      final captured = <Uint8List>[];

      final interceptSink =
          StreamController<Uint8List>()
            ..stream.listen((b) {
              captured.add(b);
              clientToServer.add(b);
            });

      TwoPartyRpcConnection.server(
        incoming: clientToServer.stream,
        outgoing: serverToClient.sink,
        bootstrap: DuplicateCapsServer(),
      );
      final client = TwoPartyRpcConnection.client(
        incoming: serverToClient.stream,
        outgoing: interceptSink.sink,
      );

      final bootstrapCap = client.bootstrap(EchoClientFactory());
      await bootstrapCap.echo('warmup');

      captured.clear();
      final call = bootstrapCap.cap.dispatchForPipelining(
        _echoInterfaceId,
        _duplicateCapsMethodId,
        RpcPayload.fromBytes(_buildEchoParams('')),
      );
      final result = await call.result;
      final capA = requireCapabilityFromResult(result, 0);
      final capB = requireCapabilityFromResult(result, 1);

      await capA.dispose();
      await _waitForReleaseCount(captured, 1);
      var releases = _releaseMessages(captured);
      expect(releases, hasLength(1));
      expect(releases.single.referenceCount, equals(1));

      final secondResult = await capB.dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('still-live')),
      );
      expect(
        _parseEchoResult(secondResult.payload),
        equals('echo: still-live'),
      );

      await capB.dispose();
      await _waitForReleaseCount(captured, 2);
      releases = _releaseMessages(captured);
      expect(releases, hasLength(2));
      expect(releases.map((m) => m.referenceCount), everyElement(equals(1)));
      expect(
        releases.fold<int>(0, (sum, msg) => sum + msg.referenceCount),
        equals(2),
      );

      await capB.dispose();
      await Future<void>.delayed(const Duration(milliseconds: 10));
      expect(_releaseMessages(captured), hasLength(2));

      await client.close();
      await interceptSink.close();
    });

    test(
      'multi-segment result with capability pointer returns callable cap',
      () async {
        final server = LargeCapabilityPayloadServer();
        final (client, serverConn) = _makePipe(server);
        final bootstrapCap = client.bootstrap(EchoClientFactory());
        await bootstrapCap.echo('warmup');

        final result = await bootstrapCap.cap.dispatch(
          _echoInterfaceId,
          _largeCapResultMethodId,
          RpcPayload.fromBytes(_buildEchoParams('')),
        );
        final root = result.payload.getTyped(_TwoPtrFactory());

        expect(_segmentCount(result.payload.bytes), greaterThan(1));
        expect(root.getDataField(0), orderedEquals(_largeData(10000)));
        expect(root.getCapabilityField(1), equals(0));

        final returnedCap = requireCapabilityFromResult(result, 1);
        final reply = await returnedCap.dispatch(
          _echoInterfaceId,
          _echoMethodId,
          RpcPayload.fromBytes(_buildEchoParams('through returned cap')),
        );
        expect(
          _parseEchoResult(reply.payload),
          equals('echo: through returned cap'),
        );

        await client.close();
        await serverConn.close();
      },
    );

    test(
      'multi-segment params with capability pointer deliver callable cap',
      () async {
        final server = LargeCapabilityPayloadServer();
        final (client, serverConn) = _makePipe(server);
        final bootstrapCap = client.bootstrap(EchoClientFactory());
        await bootstrapCap.echo('warmup');

        final localCap = EchoServer();
        final result = await bootstrapCap.cap.dispatch(
          _echoInterfaceId,
          _largeCapParamMethodId,
          RpcPayload.fromBytes(_buildLargeDataAndCapResult(10000)),
          paramsCapabilities: [localCap],
        );

        expect(_parseEchoResult(result.payload), equals('ok'));
        expect(server.lastDataLength, equals(10000));
        expect(server.lastParamCapReply, equals('echo: from server'));

        await client.close();
        await serverConn.close();
      },
    );

    test('invalid result pointer is preserved for resolved pipeline', () async {
      final server = MixedResultServer();
      final (client, serverConn) = _makePipe(server);
      final bootstrapCap = client.bootstrap(EchoClientFactory());
      await bootstrapCap.echo('warmup');

      final call = bootstrapCap.cap.dispatchForPipelining(
        _echoInterfaceId,
        _mixedMethodId,
        RpcPayload.fromBytes(_buildEchoParams('')),
      );
      final pipelinedCap = call.pipelinedCapability(0);
      await call.result;
      await Future<void>.delayed(Duration.zero);

      await expectLater(
        pipelinedCap.dispatch(
          _echoInterfaceId,
          _echoMethodId,
          RpcPayload.fromBytes(_buildEchoParams('piped')),
        ),
        throwsA(
          allOf(
            isA<RpcException>(),
            predicate<Object>(
              (e) =>
                  e.toString().contains('not a capability') &&
                  !e.toString().contains('null capability'),
            ),
          ),
        ),
      );

      await client.close();
      await serverConn.close();
    });

    test('disposing a pipelined capability does not invalidate the same '
        'capability independently read from the awaited result', () async {
      // Regression test: the eagerly-pipelined capability
      // (call.pipelinedCapability(0)) and the capability independently resolved
      // from the awaited result (requireCapabilityFromResult(result, 0))
      // both resolve to the identical underlying capability object (the
      // same DispatchResult.caps entry). Disposing one must not silently
      // invalidate the other — this is exactly how generated code exposes
      // the same field twice: eagerly via `XxxPipeline.someCap` and again
      // via the corresponding getter on the awaited `.result` reader.
      final server = PipelineServer();
      final (client, serverConn) = _makePipe(server);
      final bootstrapCap = client.bootstrap(EchoClientFactory());
      await bootstrapCap.echo('warmup');

      final call = bootstrapCap.cap.dispatchForPipelining(
        _echoInterfaceId,
        _pipelineMethodId,
        RpcPayload.fromBytes(_buildEchoParams('')),
      );
      final pipelinedCap = call.pipelinedCapability(0);

      final result = await call.result;
      final resolvedCap = requireCapabilityFromResult(result, 0);

      // Let pipelinedCap's internal resolution (which runs off the same
      // `call.result` future, asynchronously) settle before disposing it.
      await Future<void>.delayed(Duration.zero);
      await pipelinedCap.dispose();

      // resolvedCap must still work — it was never disposed.
      final reply = await resolvedCap.dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('still alive')),
      );
      expect(_parseEchoResult(reply.payload), equals('echo: still alive'));

      await resolvedCap.dispose();
      await client.close();
      await serverConn.close();
    });

    test('negative result pointer index is rejected explicitly', () async {
      final server = DuplicateCapsServer();
      final (client, serverConn) = _makePipe(server);
      final bootstrapCap = client.bootstrap(EchoClientFactory());
      await bootstrapCap.echo('warmup');

      final result = await bootstrapCap.cap.dispatch(
        _echoInterfaceId,
        _duplicateCapsMethodId,
        RpcPayload.fromBytes(_buildEchoParams('')),
      );

      expect(
        () => requireCapabilityFromResult(result, -1),
        throwsA(
          allOf(
            isA<RpcException>(),
            predicate<Object>(
              (e) => e.toString().contains('pointer slot -1 is out of range'),
            ),
          ),
        ),
      );

      await client.close();
      await serverConn.close();
    });

    test(
      'failed call send preparation cleans pending question state',
      () async {
        final clientToServer = StreamController<Uint8List>();
        final serverToClient = StreamController<Uint8List>();
        final outgoing = StreamController<Uint8List>();

        outgoing.stream.listen(clientToServer.add);
        TwoPartyRpcConnection.server(
          incoming: clientToServer.stream,
          outgoing: serverToClient.sink,
          bootstrap: PipelineServer(),
        );
        final client = TwoPartyRpcConnection.client(
          incoming: serverToClient.stream,
          outgoing: outgoing.sink,
        );

        final bootstrapCap = client.bootstrap(EchoClientFactory());
        await bootstrapCap.echo('warmup');
        expect(client.debugPendingQuestionCount, equals(0));
        expect(client.debugPendingQuestionSentCount, equals(0));

        await outgoing.close();
        final result = bootstrapCap.cap.dispatch(
          _echoInterfaceId,
          _echoMethodId,
          RpcPayload.fromBytes(_buildEchoParams('will-fail-before-send')),
        );

        await expectLater(result, throwsA(anything));
        expect(client.debugPendingQuestionCount, equals(0));
        expect(client.debugPendingQuestionSentCount, equals(0));

        await client.close();
      },
    );
  });

  group('TwoPartyRpcConnection — RPC-005/RPC-006 lifecycle', () {
    test(
      'Release ignores async exported capability dispose failures',
      () async {
        final clientToServer = StreamController<Uint8List>();
        final serverToClient = StreamController<Uint8List>();
        final captured = <Uint8List>[];
        final uncaught = <Object>[];

        final interceptSink =
            StreamController<Uint8List>()
              ..stream.listen((b) {
                captured.add(b);
                clientToServer.add(b);
              });

        TwoPartyRpcConnection.server(
          incoming: clientToServer.stream,
          outgoing: serverToClient.sink,
          bootstrap: EchoServer(),
        );
        final client = TwoPartyRpcConnection.client(
          incoming: serverToClient.stream,
          outgoing: interceptSink.sink,
        );

        final bodyDone = Completer<void>();
        runZonedGuarded(() {
          () async {
            try {
              final bootstrapCap = client.bootstrap(EchoClientFactory());
              await bootstrapCap.echo('warmup');

              captured.clear();
              await bootstrapCap.cap.dispatch(
                _echoInterfaceId,
                _echoMethodId,
                RpcPayload.fromBytes(_buildEchoParams('with-cap')),
                paramsCapabilities: [ThrowingDisposeCapability()],
              );

              final callWithCap =
                  captured
                      .map(parseRpcMessage)
                      .where(
                        (m) =>
                            m.type == RpcMessageType.call &&
                            m.capabilityTableReferences.isNotEmpty,
                      )
                      .single;
              final exportId = _exportIdOf(
                callWithCap.capabilityTableReferences.single,
              );

              serverToClient.add(buildReleaseMessage(exportId, 1));
              await Future<void>.delayed(const Duration(milliseconds: 20));
              bodyDone.complete();
            } catch (error, stackTrace) {
              bodyDone.completeError(error, stackTrace);
            }
          }();
        }, (error, _) => uncaught.add(error));

        await bodyDone.future;
        expect(uncaught, isEmpty);

        await client.close();
        await interceptSink.close();
      },
    );

    test('onDisposeError observes an exported capability dispose failure '
        'instead of it being silently swallowed', () async {
      final clientToServer = StreamController<Uint8List>();
      final serverToClient = StreamController<Uint8List>();
      final captured = <Uint8List>[];
      final uncaught = <Object>[];
      final observedErrors = <Object>[];

      final interceptSink =
          StreamController<Uint8List>()
            ..stream.listen((b) {
              captured.add(b);
              clientToServer.add(b);
            });

      TwoPartyRpcConnection.server(
        incoming: clientToServer.stream,
        outgoing: serverToClient.sink,
        bootstrap: EchoServer(),
      );
      final client = TwoPartyRpcConnection.client(
        incoming: serverToClient.stream,
        outgoing: interceptSink.sink,
        onDisposeError: (error, stackTrace) => observedErrors.add(error),
      );

      final bodyDone = Completer<void>();
      runZonedGuarded(() {
        () async {
          try {
            final bootstrapCap = client.bootstrap(EchoClientFactory());
            await bootstrapCap.echo('warmup');

            captured.clear();
            await bootstrapCap.cap.dispatch(
              _echoInterfaceId,
              _echoMethodId,
              RpcPayload.fromBytes(_buildEchoParams('with-cap')),
              paramsCapabilities: [ThrowingDisposeCapability()],
            );

            final callWithCap =
                captured
                    .map(parseRpcMessage)
                    .where(
                      (m) =>
                          m.type == RpcMessageType.call &&
                          m.capabilityTableReferences.isNotEmpty,
                    )
                    .single;
            final exportId = _exportIdOf(
              callWithCap.capabilityTableReferences.single,
            );

            serverToClient.add(buildReleaseMessage(exportId, 1));
            await Future<void>.delayed(const Duration(milliseconds: 20));
            bodyDone.complete();
          } catch (error, stackTrace) {
            bodyDone.completeError(error, stackTrace);
          }
        }();
      }, (error, _) => uncaught.add(error));

      await bodyDone.future;
      // The dispose failure must reach onDisposeError...
      expect(observedErrors, hasLength(1));
      expect(observedErrors.single, isA<StateError>());
      // ...instead of leaking as an unhandled zone error.
      expect(uncaught, isEmpty);

      await client.close();
      await interceptSink.close();
    });

    test('a capability whose dispose() throws synchronously does not abort '
        "teardown's export-disposal loop — later capabilities are still "
        'disposed and close()/done still complete normally', () async {
      final clientToServer = StreamController<Uint8List>();
      final serverToClient = StreamController<Uint8List>();

      TwoPartyRpcConnection.server(
        incoming: clientToServer.stream,
        outgoing: serverToClient.sink,
        bootstrap: EchoServer(),
      );

      final observedErrors = <Object>[];
      final client = TwoPartyRpcConnection.client(
        incoming: serverToClient.stream,
        outgoing: clientToServer.sink,
        onDisposeError: (error, stackTrace) => observedErrors.add(error),
      );

      final bootstrapCap = client.bootstrap(EchoClientFactory());
      await bootstrapCap.echo('warmup');

      // Exporting a capability as a call *parameter* makes the sending
      // side (the client, here) the one that hosts/exports it — the same
      // mechanism the pre-existing Release-triggered dispose-failure
      // tests above use, just reaching _exports via teardown instead of
      // an explicit Release message.
      final syncFailingCap = SyncThrowingDisposeCapability();
      final okCap = CountingCapability();
      await bootstrapCap.cap.dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('a')),
        paramsCapabilities: [syncFailingCap],
      );
      await bootstrapCap.cap.dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('b')),
        paramsCapabilities: [okCap],
      );
      expect(client.debugExportCount, equals(2));

      // If the synchronous throw from syncFailingCap.dispose() escaped
      // _disposeIgnoringErrors unguarded, this would either hang (the
      // export loop/teardown never finishing) or reject with a
      // StateError instead of completing normally.
      await client.close().timeout(const Duration(milliseconds: 200));
      await client.done.timeout(const Duration(milliseconds: 200));

      expect(client.debugExportCount, equals(0));
      expect(okCap.disposeCount, equals(1));
      expect(observedErrors, hasLength(1));
      expect(observedErrors.single, isA<StateError>());
    });

    test('a throwing onDisposeError callback does not break dispose-error '
        'reporting for other capabilities, nor teardown completion', () async {
      final clientToServer = StreamController<Uint8List>();
      final serverToClient = StreamController<Uint8List>();

      TwoPartyRpcConnection.server(
        incoming: clientToServer.stream,
        outgoing: serverToClient.sink,
        bootstrap: EchoServer(),
      );

      final uncaught = <Object>[];
      final client = TwoPartyRpcConnection.client(
        incoming: serverToClient.stream,
        outgoing: clientToServer.sink,
        onDisposeError:
            (error, stackTrace) =>
                throw StateError('onDisposeError callback exploded'),
      );
      final okCap = CountingCapability();

      final bodyDone = Completer<void>();
      runZonedGuarded(() {
        () async {
          try {
            final bootstrapCap = client.bootstrap(EchoClientFactory());
            await bootstrapCap.echo('warmup');

            final firstFailingCap = ThrowingDisposeCapability();
            final secondFailingCap = ThrowingDisposeCapability();
            await bootstrapCap.cap.dispatch(
              _echoInterfaceId,
              _echoMethodId,
              RpcPayload.fromBytes(_buildEchoParams('a')),
              paramsCapabilities: [firstFailingCap],
            );
            await bootstrapCap.cap.dispatch(
              _echoInterfaceId,
              _echoMethodId,
              RpcPayload.fromBytes(_buildEchoParams('b')),
              paramsCapabilities: [secondFailingCap],
            );
            await bootstrapCap.cap.dispatch(
              _echoInterfaceId,
              _echoMethodId,
              RpcPayload.fromBytes(_buildEchoParams('c')),
              paramsCapabilities: [okCap],
            );

            await client.close().timeout(const Duration(milliseconds: 200));
            await client.done.timeout(const Duration(milliseconds: 200));
            bodyDone.complete();
          } catch (error, stackTrace) {
            bodyDone.completeError(error, stackTrace);
          }
        }();
      }, (error, _) => uncaught.add(error));

      await bodyDone.future;

      expect(okCap.disposeCount, equals(1));
      expect(uncaught, isEmpty);
    });

    test('Release with referenceCount exceeding the outstanding remote '
        'refcount tears the connection down as a protocol violation', () async {
      final clientToServer = StreamController<Uint8List>();
      final serverToClient = StreamController<Uint8List>();
      final captured = <Uint8List>[];

      final interceptSink =
          StreamController<Uint8List>()
            ..stream.listen((b) {
              captured.add(b);
              clientToServer.add(b);
            });

      TwoPartyRpcConnection.server(
        incoming: clientToServer.stream,
        outgoing: serverToClient.sink,
        bootstrap: EchoServer(),
      );
      final client = TwoPartyRpcConnection.client(
        incoming: serverToClient.stream,
        outgoing: interceptSink.sink,
      );

      final bootstrapCap = client.bootstrap(EchoClientFactory());
      await bootstrapCap.echo('warmup');

      captured.clear();
      // Exports a capability with exactly one outstanding remote reference.
      await bootstrapCap.cap.dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('with-cap')),
        paramsCapabilities: [EchoServer()],
      );

      final callWithCap =
          captured
              .map(parseRpcMessage)
              .where(
                (m) =>
                    m.type == RpcMessageType.call &&
                    m.capabilityTableReferences.isNotEmpty,
              )
              .single;
      final exportId = _exportIdOf(
        callWithCap.capabilityTableReferences.single,
      );

      // Peer claims to release 2 references when only 1 was ever granted.
      serverToClient.add(buildReleaseMessage(exportId, 2));

      await expectLater(
        client.done,
        throwsA(
          predicate<Object>((e) => e.toString().contains('protocol violation')),
        ),
      );

      // Teardown must actually happen, not just report on `done`.
      await expectLater(
        bootstrapCap.echo('after-violation'),
        throwsA(anything),
      );

      await interceptSink.close();
    });

    test('Release with referenceCount 0 tears the connection down as a '
        'protocol violation', () async {
      final clientToServer = StreamController<Uint8List>();
      final serverToClient = StreamController<Uint8List>();
      final captured = <Uint8List>[];

      final interceptSink =
          StreamController<Uint8List>()
            ..stream.listen((b) {
              captured.add(b);
              clientToServer.add(b);
            });

      TwoPartyRpcConnection.server(
        incoming: clientToServer.stream,
        outgoing: serverToClient.sink,
        bootstrap: EchoServer(),
      );
      final client = TwoPartyRpcConnection.client(
        incoming: serverToClient.stream,
        outgoing: interceptSink.sink,
      );

      final bootstrapCap = client.bootstrap(EchoClientFactory());
      await bootstrapCap.echo('warmup');

      captured.clear();
      await bootstrapCap.cap.dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('with-cap')),
        paramsCapabilities: [EchoServer()],
      );

      final callWithCap =
          captured
              .map(parseRpcMessage)
              .where(
                (m) =>
                    m.type == RpcMessageType.call &&
                    m.capabilityTableReferences.isNotEmpty,
              )
              .single;
      final exportId = _exportIdOf(
        callWithCap.capabilityTableReferences.single,
      );

      // Releasing zero references is meaningless — a legitimate peer
      // never sends one — and must not be silently accepted as a no-op.
      serverToClient.add(buildReleaseMessage(exportId, 0));

      await expectLater(
        client.done,
        throwsA(
          predicate<Object>((e) => e.toString().contains('protocol violation')),
        ),
      );

      await expectLater(
        bootstrapCap.echo('after-violation'),
        throwsA(anything),
      );

      await interceptSink.close();
    });

    test(
      'bootstrap Return without a capability fails the bootstrap cap',
      () async {
        final clientToServer = StreamController<Uint8List>();
        final serverToClient = StreamController<Uint8List>();
        clientToServer.stream.listen((_) {});

        final client = TwoPartyRpcConnection.client(
          incoming: serverToClient.stream,
          outgoing: clientToServer.sink,
        );
        final stub = client.bootstrap(EchoClientFactory());

        serverToClient.add(
          buildReturnResultsMessage(
            answerId: 0,
            resultsBytes: _buildEchoParams('no-cap'),
          ),
        );

        await expectLater(
          stub.echo('hello').timeout(const Duration(milliseconds: 100)),
          throwsA(
            allOf(
              isA<RpcException>(),
              predicate<Object>(
                (e) => e.toString().contains(
                  'bootstrap Return had no capability in cap table',
                ),
              ),
            ),
          ),
        );

        await serverToClient.close();
        await client.close();
      },
    );

    test(
      'Finish before Return suppresses the completed dispatch result',
      () async {
        final clientToServer = StreamController<Uint8List>();
        final serverToClient = StreamController<Uint8List>();
        final captured = <RpcMessage>[];
        serverToClient.stream.listen(
          (bytes) => captured.add(parseRpcMessage(bytes)),
        );

        final server = SlowEchoServer();
        final serverConn = TwoPartyRpcConnection.server(
          incoming: clientToServer.stream,
          outgoing: serverToClient.sink,
          bootstrap: server,
        );

        clientToServer.add(
          buildCallMessage(
            questionId: 1,
            targetImportId: 0,
            interfaceId: _echoInterfaceId,
            methodId: _echoMethodId,
            paramsBytes: _buildEchoParams('slow'),
          ),
        );
        await server.started.future;

        clientToServer.add(buildFinishMessage(1));
        await server.canceled.future.timeout(const Duration(milliseconds: 100));
        expect(server.lastContext?.isCanceled, isTrue);
        server.complete.complete();
        await Future<void>.delayed(const Duration(milliseconds: 20));

        // The dispatch's own (real) result is suppressed — never sent as a
        // normal Return — but this vat still answers the question with
        // Return(canceled) once the dispatch actually settles (see
        // AnswerTable.handleRequesterFinishedAnswer/IncomingCallCoordinator
        // ._sendCanceledReturn), instead of leaving it hanging forever.
        final returns =
            captured.where((m) => m.type == RpcMessageType.return_).toList();
        expect(returns, hasLength(1));
        expect(returns.single.answerId, equals(1));
        expect(returns.single.returnDisc, equals(2)); // canceled
        // Finish-triggered suppression must leave no answer/cancellation
        // state behind, same as teardown-triggered suppression.
        expect(serverConn.debugAnswerCount, equals(0));
        expect(serverConn.debugCancellationCount, equals(0));

        await clientToServer.close();
        await serverConn.done;
      },
    );

    test('connection close cancels an in-progress dispatch context', () async {
      final clientToServer = StreamController<Uint8List>();
      final serverToClient = StreamController<Uint8List>();
      serverToClient.stream.listen((_) {});

      final server = SlowEchoServer();
      final serverConn = TwoPartyRpcConnection.server(
        incoming: clientToServer.stream,
        outgoing: serverToClient.sink,
        bootstrap: server,
      );

      clientToServer.add(
        buildCallMessage(
          questionId: 1,
          targetImportId: 0,
          interfaceId: _echoInterfaceId,
          methodId: _echoMethodId,
          paramsBytes: _buildEchoParams('slow'),
        ),
      );
      await server.started.future;

      await clientToServer.close();
      await server.canceled.future.timeout(const Duration(milliseconds: 100));
      expect(server.lastContext?.isCanceled, isTrue);
      server.complete.complete();

      await serverConn.done;

      // Teardown must have cleared the cancellation/answer tables even
      // though the dispatch itself only completes afterwards.
      expect(serverConn.debugCancellationCount, equals(0));
      expect(serverConn.debugAnswerCount, equals(0));
    });

    test('a result capability is disposed exactly once when the connection '
        'closes before the dispatch that produced it resolves', () async {
      final clientToServer = StreamController<Uint8List>();
      final serverToClient = StreamController<Uint8List>();
      serverToClient.stream.listen((_) {});

      final resultCap = CountingCapability();
      final server = SlowCapResultServer([resultCap]);
      final serverConn = TwoPartyRpcConnection.server(
        incoming: clientToServer.stream,
        outgoing: serverToClient.sink,
        bootstrap: server,
      );

      clientToServer.add(
        buildCallMessage(
          questionId: 1,
          targetImportId: 0,
          interfaceId: _echoInterfaceId,
          methodId: _pipelineMethodId,
          paramsBytes: _buildEchoParams(''),
        ),
      );
      await server.started.future;

      await clientToServer.close();
      await serverConn.done;

      // Teardown finished before the dispatch itself resolved; the result
      // capability only becomes reachable once it does.
      expect(resultCap.disposeCount, equals(0));
      server.complete.complete();
      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(resultCap.disposeCount, equals(1));
    });

    test('a result capability is disposed instead of leaked when the answer '
        'was already canceled by a Finish that arrived before dispatch '
        'resolved', () async {
      final clientToServer = StreamController<Uint8List>();
      final serverToClient = StreamController<Uint8List>();
      final captured = <RpcMessage>[];
      serverToClient.stream.listen(
        (bytes) => captured.add(parseRpcMessage(bytes)),
      );

      final resultCap = CountingCapability();
      final server = SlowCapResultServer([resultCap]);
      final serverConn = TwoPartyRpcConnection.server(
        incoming: clientToServer.stream,
        outgoing: serverToClient.sink,
        bootstrap: server,
      );

      clientToServer.add(
        buildCallMessage(
          questionId: 1,
          targetImportId: 0,
          interfaceId: _echoInterfaceId,
          methodId: _pipelineMethodId,
          paramsBytes: _buildEchoParams(''),
        ),
      );
      await server.started.future;

      clientToServer.add(buildFinishMessage(1));
      await Future<void>.delayed(const Duration(milliseconds: 20));

      // The dispatch ignores cancellation and succeeds anyway; its result
      // capability was never going to be sent (Finish already suppressed
      // this answer), so it must be disposed instead of dropped — this
      // vat still answers with Return(canceled) once the dispatch settles,
      // rather than leaving the question hanging forever.
      server.complete.complete();
      await Future<void>.delayed(const Duration(milliseconds: 20));

      final returns =
          captured.where((m) => m.type == RpcMessageType.return_).toList();
      expect(returns, hasLength(1));
      expect(returns.single.answerId, equals(1));
      expect(returns.single.returnDisc, equals(2)); // canceled
      expect(resultCap.disposeCount, equals(1));

      await clientToServer.close();
      await serverConn.done;
    });

    test('the same capability instance appearing twice in a discarded result '
        'is disposed once, not twice', () async {
      final clientToServer = StreamController<Uint8List>();
      final serverToClient = StreamController<Uint8List>();
      serverToClient.stream.listen((_) {});

      final resultCap = CountingCapability();
      final server = SlowCapResultServer([resultCap, resultCap]);
      final serverConn = TwoPartyRpcConnection.server(
        incoming: clientToServer.stream,
        outgoing: serverToClient.sink,
        bootstrap: server,
      );

      clientToServer.add(
        buildCallMessage(
          questionId: 1,
          targetImportId: 0,
          interfaceId: _echoInterfaceId,
          methodId: _pipelineMethodId,
          paramsBytes: _buildEchoParams(''),
        ),
      );
      await server.started.future;

      await clientToServer.close();
      await serverConn.done;
      server.complete.complete();
      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(resultCap.disposeCount, equals(1));
    });

    test('one capability failing to dispose does not stop others in the same '
        'discarded result from being disposed', () async {
      final clientToServer = StreamController<Uint8List>();
      final serverToClient = StreamController<Uint8List>();
      serverToClient.stream.listen((_) {});

      final failingCap = ThrowingDisposeCapability();
      final okCap = CountingCapability();
      final onDisposeErrors = <Object>[];
      final server = SlowCapResultServer([failingCap, okCap]);
      final serverConn = TwoPartyRpcConnection.server(
        incoming: clientToServer.stream,
        outgoing: serverToClient.sink,
        bootstrap: server,
        onDisposeError: (error, stackTrace) => onDisposeErrors.add(error),
      );

      clientToServer.add(
        buildCallMessage(
          questionId: 1,
          targetImportId: 0,
          interfaceId: _echoInterfaceId,
          methodId: _pipelineMethodId,
          paramsBytes: _buildEchoParams(''),
        ),
      );
      await server.started.future;

      await clientToServer.close();
      await serverConn.done;
      server.complete.complete();
      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(okCap.disposeCount, equals(1));
      expect(onDisposeErrors, hasLength(1));
      expect(onDisposeErrors.single, isA<StateError>());
    });

    test('debugExportCount / debugImportCount track an exchanged capability '
        'and return to zero after it is released', () async {
      // ChildPipelineServer returns a distinct capability (not itself) so
      // the pipelined result is a genuinely new export/import, not just a
      // second reference to the already-exported bootstrap capability.
      final (client, serverConn) = _makePipe(ChildPipelineServer());

      final bootstrapCap = client.bootstrap(EchoClientFactory());
      await bootstrapCap.echo('warmup');

      // The bootstrap capability itself is export 0 / import 0 at this point.
      expect(serverConn.debugExportCount, equals(1));
      expect(client.debugImportCount, equals(1));

      final call = bootstrapCap.cap.dispatchForPipelining(
        _echoInterfaceId,
        _pipelineMethodId,
        RpcPayload.fromBytes(_buildEchoParams('')),
      );
      final pipelinedCap = call.pipelinedCapability(0);
      await call.result;
      await Future<void>.delayed(Duration.zero);

      expect(serverConn.debugExportCount, equals(2));
      expect(client.debugImportCount, equals(2));

      await pipelinedCap.dispose();
      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(serverConn.debugExportCount, equals(1));
      expect(client.debugImportCount, equals(1));

      await client.close();
      await serverConn.close();
    });

    test('debugCancellationCount / debugAnswerCount reflect an in-flight '
        'dispatch and settle back to zero once teardown completes', () async {
      final clientToServer = StreamController<Uint8List>();
      final serverToClient = StreamController<Uint8List>();
      serverToClient.stream.listen((_) {});

      final server = SlowEchoServer();
      final serverConn = TwoPartyRpcConnection.server(
        incoming: clientToServer.stream,
        outgoing: serverToClient.sink,
        bootstrap: server,
      );

      clientToServer.add(
        buildCallMessage(
          questionId: 1,
          targetImportId: 0,
          interfaceId: _echoInterfaceId,
          methodId: _echoMethodId,
          paramsBytes: _buildEchoParams('slow'),
        ),
      );
      await server.started.future;

      // Dispatch is running: one live cancellation controller, one
      // in-flight answer.
      expect(serverConn.debugCancellationCount, equals(1));
      expect(serverConn.debugAnswerCount, equals(1));

      await clientToServer.close();
      await server.canceled.future.timeout(const Duration(milliseconds: 100));
      server.complete.complete();
      await serverConn.done;

      expect(serverConn.debugCancellationCount, equals(0));
      expect(serverConn.debugAnswerCount, equals(0));
    });

    test('a pipelined (promised-answer) call still pending on teardown fails '
        'instead of hanging, and the parent settling afterwards does not '
        'resurrect answer-table state', () async {
      final gate = Completer<void>();
      final child = CountingCapability();
      final (client, serverConn) = _makePipe(
        ChildPipelineServer(completer: gate, child: child),
      );

      final bootstrapCap = client.bootstrap(EchoClientFactory());
      await bootstrapCap.echo('warmup');

      final parent = bootstrapCap.cap.dispatchForPipelining(
        _echoInterfaceId,
        _pipelineMethodId,
        RpcPayload.fromBytes(_buildEchoParams('')),
      );
      final pipelinedCap = parent.pipelinedCapability(0);
      final pipelinedCall = pipelinedCap.dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('x')),
      );
      await _waitUntil(() => serverConn.debugAnswerCount == 1);

      // The parent dispatch hasn't resolved yet: it's the one live answer,
      // and the pipelined call rides on top of it wire-side without an
      // answer entry of its own yet.
      expect(serverConn.debugCancellationCount, equals(1));

      await client.close();

      await expectLater(parent.result, throwsA(isA<RpcException>()));
      await expectLater(pipelinedCall, throwsA(isA<RpcException>()));
      expect(client.debugPendingQuestionCount, equals(0));

      await serverConn.done.catchError((_) {});
      expect(serverConn.debugAnswerCount, equals(0));
      expect(serverConn.debugCancellationCount, equals(0));

      // ChildPipelineServer only overrides the context-less dispatch(), so
      // it never observes the cancellation signal above and keeps running
      // regardless of teardown. Letting it finish late must not resurrect
      // any answer-table state that teardown already cleared. Waiting on
      // child.disposeCount (rather than a fixed delay) proves the late-
      // completion path actually ran: once the parent dispatch resolves
      // after the connection is already closed, _executeIncomingDispatch's
      // _closedError branch disposes its result capabilities (since they
      // were never going to be sent as a Return) instead of leaking them
      // -- child's disposal is a direct signal that path executed, not
      // just that some arbitrary amount of time passed.
      gate.complete();
      await _waitUntil(() => child.disposeCount == 1);
      expect(serverConn.debugAnswerCount, equals(0));
      expect(serverConn.debugCancellationCount, equals(0));
    });

    test(
      'an export and an import both still holding a live remote reference '
      'are each disposed exactly once when the connection tears down',
      () async {
        final serverSideCap = CountingCapability();
        final clientSideCap = CountingCapability();
        final (client, serverConn) = _makePipe(
          _CapabilityReturningServer(serverSideCap),
        );

        final bootstrapCap = client.bootstrap(EchoClientFactory());

        // _CapabilityReturningServer's _pipelineMethodId never disposes
        // paramsCapabilities and never returns them either, so
        // clientSideCap stays a live import on the server / live export on
        // the client, and the capability lease for serverSideCap (deliberately never
        // disposed here) stays a live export on the server / live import on
        // the client -- beyond just the single bootstrap pair.
        final call = bootstrapCap.cap.dispatchForPipelining(
          _echoInterfaceId,
          _pipelineMethodId,
          RpcPayload.fromBytes(_buildEchoParams('')),
          paramsCapabilities: [clientSideCap],
        );
        call.pipelinedCapability(
          0,
        ); // capability lease, deliberately left undisposed
        await call.result;
        await Future<void>.delayed(Duration.zero);

        expect(client.debugExportCount, equals(1)); // clientSideCap
        expect(
          client.debugImportCount,
          equals(2),
        ); // bootstrap + capability lease
        expect(
          serverConn.debugExportCount,
          equals(2),
        ); // bootstrap + capability lease
        expect(serverConn.debugImportCount, equals(1)); // clientSideCap

        await client.close();
        await serverConn.done.catchError((_) {});

        expect(client.debugExportCount, equals(0));
        expect(client.debugImportCount, equals(0));
        expect(serverConn.debugExportCount, equals(0));
        expect(serverConn.debugImportCount, equals(0));
        expect(serverSideCap.disposeCount, equals(1));
        expect(clientSideCap.disposeCount, equals(1));
      },
    );

    test("a params capability's capTable resolution, suspended awaiting an "
        'import id mid-list when tearDown runs, does not resurrect '
        'ExportTable state for a capability later in the same list once that '
        'import id resolves', () async {
      // Regression coverage for a gap the earlier startCallWithAllocatedQuestion()/
      // _throwIfTornDown() guards (see OutgoingCallCoordinator) don't
      // close on their own: they stop a *build* from resuming after
      // tearDown, but resolveParameterCapabilityReferences's own loop can itself be
      // suspended mid-params-list -- on an unresolved import id here,
      // just as easily on a pipelined param's parent being sent -- with
      // some entries already resolved (and exported) and others not yet
      // reached. QuestionTable.tearDown drops this call's qid entirely,
      // so nothing rolls back a *new* export the loop creates after
      // resuming from that suspension -- ensureActive() (threaded into
      // _resolveCapTableAsync as this call's OutgoingCallCoordinator
      // resolveParameterCapabilityReferences's ensureActive parameter) exists
      // specifically to stop the loop from ever reaching that new export
      // in the first place.
      final localCapA = CountingCapability();
      final localCapB = CountingCapability();
      final (client, _) = _makePipe(EchoServer());

      final bootstrapCap = client.bootstrap(EchoClientFactory());
      await bootstrapCap.echo('warmup');

      // debugRpcCapabilityDelegate/createImportedCapability (see issue #64) are
      // the only way to get an _ImportedCapability whose import id is
      // still genuinely unresolved and under this test's own control --
      // every import a real Return/param descriptor hands the app is
      // already cached by the time application code sees it.
      final importId = Completer<int>();
      final asyncCap = createImportedCapability(
        client.debugRpcCapabilityDelegate,
        importId.future,
      );

      // dispatch() resolves bootstrapCap's own (already-cached) target
      // state synchronously, so this call's build runs synchronously up
      // to the point _resolveCapTableAsync's loop reaches asyncCap: by
      // the time this statement returns, localCapA (ahead of asyncCap in
      // the list) has already been resolved and exported, and the loop is
      // suspended awaiting asyncCap._importIdFuture -- localCapB (behind
      // it) not yet reached.
      final callFuture = bootstrapCap.cap.dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('x')),
        paramsCapabilities: [localCapA, asyncCap, localCapB],
      );
      callFuture.ignore();

      expect(client.debugExportCount, equals(1)); // localCapA only

      await client.close();
      expect(client.debugExportCount, equals(0));
      expect(client.debugPendingQuestionCount, equals(0));

      // The import id resolving now, well after tearDown, must not
      // resurrect ExportTable state for localCapB.
      importId.complete(9);
      await Future<void>.delayed(Duration.zero);

      expect(client.debugExportCount, equals(0));
      expect(localCapB.disposeCount, equals(0));
    });

    test('exporting the same capability twice reuses the export id; only the '
        'final Release disposes it', () async {
      final incoming = StreamController<Uint8List>();
      final outgoingCaptured = <RpcMessage>[];
      final outgoing =
          StreamController<Uint8List>()
            ..stream.listen((b) => outgoingCaptured.add(parseRpcMessage(b)));

      final target = CountingCapability();
      final serverConn = TwoPartyRpcConnection.server(
        incoming: incoming.stream,
        outgoing: outgoing.sink,
        bootstrap: FixedCapServer(target),
      );

      // First call: server returns `target` as a result capability.
      incoming.add(
        buildCallMessage(
          questionId: 1,
          targetImportId: 0,
          interfaceId: _echoInterfaceId,
          methodId: _pipelineMethodId,
          paramsBytes: _buildEchoParams(''),
        ),
      );
      await Future<void>.delayed(Duration.zero);

      final return1 =
          outgoingCaptured
              .where((m) => m.type == RpcMessageType.return_ && m.answerId == 1)
              .single;
      expect(return1.capabilityTableReferences, hasLength(1));
      final exportId = _exportIdOf(return1.capabilityTableReferences.single);
      // Export 0 is the bootstrap (FixedCapServer); export [exportId] is target.
      expect(serverConn.debugExportCount, equals(2));
      expect(target.disposeCount, equals(0));

      incoming.add(buildFinishMessage(1, releaseResultCaps: false));
      await Future<void>.delayed(Duration.zero);

      outgoingCaptured.clear();
      // Second call: the *same* target capability is returned again.
      incoming.add(
        buildCallMessage(
          questionId: 2,
          targetImportId: 0,
          interfaceId: _echoInterfaceId,
          methodId: _pipelineMethodId,
          paramsBytes: _buildEchoParams(''),
        ),
      );
      await Future<void>.delayed(Duration.zero);

      final return2 =
          outgoingCaptured
              .where((m) => m.type == RpcMessageType.return_ && m.answerId == 2)
              .single;
      // Same capability -> the existing export id is reused, not a new one.
      expect(
        _exportIdOf(return2.capabilityTableReferences.single),
        equals(exportId),
      );
      expect(serverConn.debugExportCount, equals(2));

      incoming.add(buildFinishMessage(2, releaseResultCaps: false));
      await Future<void>.delayed(Duration.zero);

      // Peer now releases the two references it was granted, one at a time.
      incoming.add(buildReleaseMessage(exportId, 1));
      await Future<void>.delayed(Duration.zero);
      expect(
        target.disposeCount,
        equals(0),
        reason: 'one remote reference is still outstanding',
      );
      expect(serverConn.debugExportCount, equals(2));

      incoming.add(buildReleaseMessage(exportId, 1));
      await Future<void>.delayed(Duration.zero);
      expect(target.disposeCount, equals(1));
      expect(serverConn.debugExportCount, equals(1)); // only bootstrap remains

      await serverConn.close();
    });

    test('closing the connection while Bootstrap is in flight fails it instead '
        'of hanging, and a late Bootstrap Return is safely ignored', () async {
      final incoming = StreamController<Uint8List>();
      final outgoing = StreamController<Uint8List>()..stream.listen((_) {});

      final client = TwoPartyRpcConnection.client(
        incoming: incoming.stream,
        outgoing: outgoing.sink,
      );

      final bootstrapCap = client.bootstrap(EchoClientFactory());
      // Bootstrap message sent; no Return has arrived yet.
      expect(client.debugPendingQuestionCount, equals(1));

      await client.close();

      // The pending bootstrap call must fail, not hang forever.
      await expectLater(
        bootstrapCap.echo('after-close-race'),
        throwsA(anything),
      );
      expect(client.debugPendingQuestionCount, equals(0));

      // A Bootstrap Return that was already in flight when close() ran
      // must be safely ignored — no crash, no state resurrected.
      incoming.add(buildBootstrapReturnMessage(answerId: 0, exportId: 0));
      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(client.debugPendingQuestionCount, equals(0));
    });

    test(
      'a duplicate incoming question ID while the first dispatch is still '
      'in flight tears the connection down as a protocol violation',
      () async {
        final clientToServer = StreamController<Uint8List>();
        final serverToClient = StreamController<Uint8List>();
        serverToClient.stream.listen((_) {});

        final server = SlowEchoServer();
        final serverConn = TwoPartyRpcConnection.server(
          incoming: clientToServer.stream,
          outgoing: serverToClient.sink,
          bootstrap: server,
        );

        clientToServer.add(
          buildCallMessage(
            questionId: 1,
            targetImportId: 0,
            interfaceId: _echoInterfaceId,
            methodId: _echoMethodId,
            paramsBytes: _buildEchoParams('first'),
          ),
        );
        await server.started.future;

        // Same question ID reused while the first dispatch it named is
        // still running — a legitimate peer never does this.
        clientToServer.add(
          buildCallMessage(
            questionId: 1,
            targetImportId: 0,
            interfaceId: _echoInterfaceId,
            methodId: _echoMethodId,
            paramsBytes: _buildEchoParams('second'),
          ),
        );

        await expectLater(
          serverConn.done,
          throwsA(
            predicate<Object>(
              (e) =>
                  e.toString().contains('protocol violation') &&
                  e.toString().contains('duplicate incoming question ID'),
            ),
          ),
        );

        server.complete.complete();
        await clientToServer.close();
      },
    );

    test('a Bootstrap reusing a question ID with an in-flight Call tears the '
        'connection down as a protocol violation', () async {
      final clientToServer = StreamController<Uint8List>();
      final serverToClient = StreamController<Uint8List>();
      serverToClient.stream.listen((_) {});

      final server = SlowEchoServer();
      final serverConn = TwoPartyRpcConnection.server(
        incoming: clientToServer.stream,
        outgoing: serverToClient.sink,
        bootstrap: server,
      );

      clientToServer.add(
        buildCallMessage(
          questionId: 5,
          targetImportId: 0,
          interfaceId: _echoInterfaceId,
          methodId: _echoMethodId,
          paramsBytes: _buildEchoParams('slow'),
        ),
      );
      await server.started.future;

      clientToServer.add(buildBootstrapMessage(5));

      await expectLater(
        serverConn.done,
        throwsA(
          predicate<Object>(
            (e) =>
                e.toString().contains('protocol violation') &&
                e.toString().contains('duplicate incoming question ID'),
          ),
        ),
      );

      server.complete.complete();
      await clientToServer.close();
    });

    test(
      'close cancels an independently-owned input stream subscription',
      () async {
        final inputCanceled = Completer<void>();
        final incoming = StreamController<Uint8List>(
          onCancel: inputCanceled.complete,
        );
        final outgoing = StreamController<Uint8List>()..stream.listen((_) {});

        final connection = TwoPartyRpcConnection.client(
          incoming: incoming.stream,
          outgoing: outgoing.sink,
        );

        await connection.close();
        await inputCanceled.future.timeout(const Duration(milliseconds: 100));
        expect(incoming.hasListener, isFalse);
        await incoming.close();
      },
    );

    test(
      'outgoing sink completion tears down the connection even when input stays open',
      () async {
        final inputCanceled = Completer<void>();
        final incoming = StreamController<Uint8List>(
          onCancel: inputCanceled.complete,
        );
        final outgoing = StreamController<Uint8List>()..stream.listen((_) {});

        final connection = TwoPartyRpcConnection.client(
          incoming: incoming.stream,
          outgoing: outgoing.sink,
        );
        final bootstrap = connection.bootstrap(EchoClientFactory());
        expect(connection.debugPendingQuestionCount, equals(1));

        await outgoing.close();
        await connection.done.timeout(const Duration(milliseconds: 100));
        await inputCanceled.future.timeout(const Duration(milliseconds: 100));
        expect(connection.debugPendingQuestionCount, equals(0));
        await expectLater(
          bootstrap.echo('after-output-close'),
          throwsA(anything),
        );
        await incoming.close();
      },
    );

    test('a synchronous throw from the outgoing sink tears the connection '
        'down instead of leaking as an unhandled error', () async {
      final clientToServer = StreamController<Uint8List>();
      final serverToClient = _SynchronousThrowingSink();

      final serverConn = TwoPartyRpcConnection.server(
        incoming: clientToServer.stream,
        outgoing: serverToClient,
        bootstrap: EchoServer(),
      );

      Object? doneError;
      serverConn.done.catchError((Object e) => doneError = e);

      final uncaught = <Object>[];
      final bodyDone = Completer<void>();
      runZonedGuarded(() {
        () async {
          try {
            clientToServer.add(
              buildCallMessage(
                questionId: 1,
                targetImportId: 0,
                interfaceId: _echoInterfaceId,
                methodId: _echoMethodId,
                paramsBytes: _buildEchoParams('hi'),
              ),
            );
            await Future<void>.delayed(const Duration(milliseconds: 20));
            bodyDone.complete();
          } catch (error, stackTrace) {
            bodyDone.completeError(error, stackTrace);
          }
        }();
      }, (error, _) => uncaught.add(error));

      await bodyDone.future;

      // The send failure must be routed through teardown (surfacing as
      // the sink's own StateError, not miscategorized as a malformed
      // incoming message) rather than becoming an unhandled zone error.
      expect(uncaught, isEmpty);
      expect(doneError, isA<StateError>());

      await clientToServer.close();
    });
  });

  group('TwoPartyRpcConnection — streaming flow control', () {
    test('dispatchStreaming applies window backpressure and unblocks as calls '
        'are acked, in order', () async {
      final clientToServer = StreamController<Uint8List>();
      final serverToClient = StreamController<Uint8List>();

      final server = QueuedSlowServer();
      TwoPartyRpcConnection.server(
        incoming: clientToServer.stream,
        outgoing: serverToClient.sink,
        bootstrap: server,
      );

      // Measure one real (empty) params message so the window arithmetic
      // below is exact regardless of framing overhead.
      final params = _buildEchoParams('');
      final messageSize = params.lengthInBytes;

      // windowSize = 2x message size: sends 1 and 2 fit (in-flight <=
      // 2*size, limit = window(2*size) + maxMessage(size) = 3*size); send
      // 3 does not (in-flight 3*size is not < limit 3*size).
      final client = TwoPartyRpcConnection.client(
        incoming: serverToClient.stream,
        outgoing: clientToServer.sink,
        streamWindowSize: messageSize * 2,
      );

      final bootstrapCap = client.bootstrap(EchoClientFactory());
      final cap = bootstrapCap.cap;

      final order = <int>[];
      Future<void> streamCall(int n) => cap
          .dispatchStreaming(
            _echoInterfaceId,
            _echoMethodId,
            RpcPayload.fromBytes(params),
          )
          .then((_) => order.add(n));

      unawaited(streamCall(1));
      unawaited(streamCall(2));
      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(
        order,
        equals([1, 2]),
        reason: 'first two calls fit in the window',
      );
      expect(server.dispatchCount, equals(2));

      unawaited(streamCall(3));
      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(
        order,
        equals([1, 2]),
        reason:
            'third call is blocked by the full window, not yet sent '
            'to the flow-control caller — but it IS already on the wire',
      );
      // The call was still sent immediately despite being window-blocked
      // (message order on the wire is never delayed by flow control).
      expect(server.dispatchCount, equals(3));

      // Acking the oldest call frees enough window for the third send's
      // future to resolve.
      server.completeNext();
      await Future<void>.delayed(const Duration(milliseconds: 20));
      expect(order, equals([1, 2, 3]));

      server.completeNext();
      server.completeNext();
      await client.close();
    });

    test(
      'a failed streaming call poisons that capability\'s flow-control '
      'window for later streaming calls, but not for regular calls',
      () async {
        final clientToServer = StreamController<Uint8List>();
        final serverToClient = StreamController<Uint8List>();

        final server = QueuedSlowServer();
        TwoPartyRpcConnection.server(
          incoming: clientToServer.stream,
          outgoing: serverToClient.sink,
          bootstrap: server,
        );

        final params = _buildEchoParams('');
        final messageSize = params.lengthInBytes;

        // windowSize == one message: the first send is never blocked by its
        // own size, but a second concurrent send is — giving a
        // genuinely-blocked call to observe the poisoning propagate through.
        final client = TwoPartyRpcConnection.client(
          incoming: serverToClient.stream,
          outgoing: clientToServer.sink,
          streamWindowSize: messageSize,
        );

        final bootstrapCap = client.bootstrap(EchoClientFactory());
        final cap = bootstrapCap.cap;

        var firstResolved = false;
        unawaited(
          cap
              .dispatchStreaming(
                _echoInterfaceId,
                _echoMethodId,
                RpcPayload.fromBytes(params),
              )
              .then((_) => firstResolved = true),
        );
        await Future<void>.delayed(const Duration(milliseconds: 20));
        expect(
          firstResolved,
          isTrue,
          reason: 'the first send is never blocked by its own size',
        );

        // Attach the listener immediately so the pending rejection below
        // isn't briefly unobserved and flagged as an unhandled zone error.
        final blockedResult = cap.dispatchStreaming(
          _echoInterfaceId,
          _echoMethodId,
          RpcPayload.fromBytes(params),
        );
        Object? blockedError;
        unawaited(blockedResult.catchError((Object e) => blockedError = e));
        await Future<void>.delayed(const Duration(milliseconds: 20));
        expect(server.dispatchCount, equals(2));
        expect(
          blockedError,
          isNull,
          reason: 'second send is still window-blocked, not yet failed',
        );

        // Failing the first call's ack poisons the flow controller: the
        // still-blocked second send now rejects with that failure.
        server.failNext(StateError('write failed'));
        await Future<void>.delayed(const Duration(milliseconds: 20));
        expect(blockedError, isA<RpcException>());

        // Poisoning persists for later streaming sends on the same
        // capability — this one is rejected immediately, without even
        // waiting on its own ack.
        await expectLater(
          cap.dispatchStreaming(
            _echoInterfaceId,
            _echoMethodId,
            RpcPayload.fromBytes(params),
          ),
          throwsA(isA<RpcException>()),
        );

        // But poisoning is scoped to the streaming flow controller, not the
        // capability itself — a regular (non-streaming) dispatch still
        // completes normally once acked.
        final regularResult = cap.dispatch(
          _echoInterfaceId,
          _echoMethodId,
          RpcPayload.fromBytes(params),
        );
        await Future<void>.delayed(const Duration(milliseconds: 20));
        server.completeNext();
        server.completeNext();
        server.completeNext();
        await expectLater(regularResult, completes);

        await client.close();
      },
    );
  });

  group('rpc_message_codec — message encoding/decoding', () {
    test('bootstrap round-trip', () {
      final bytes = buildBootstrapMessage(42);
      final msg = parseRpcMessage(bytes);
      expect(msg.type, RpcMessageType.bootstrap);
      expect(msg.questionId, 42);
    });

    test('call round-trip', () {
      // Build a valid Cap'n Proto message to use as params.
      final mb = MessageBuilder();
      mb.initRoot(_TextParamFactory()).setTextField(0, 'hello');
      final params = mb.serialize();

      final bytes = buildCallMessage(
        questionId: 7,
        targetImportId: 3,
        interfaceId: 0xDEADBEEF,
        methodId: 5,
        paramsBytes: params,
      );
      final msg = parseRpcMessage(bytes);
      expect(msg.type, RpcMessageType.call);
      expect(msg.questionId, 7);
      expect(msg.targetImportId, 3);
      expect(msg.interfaceId, 0xDEADBEEF);
      expect(msg.methodId, 5);
      // Verify the params round-trip correctly (semantic equality).
      final paramsReq = msg.paramsContent!.asStruct(_TextParamFactory())!;
      expect(paramsReq.getTextField(0), 'hello');
    });

    test(
      'buildCallMessageWithParamsBuilderSync builds params directly into the '
      'envelope (matches buildCallMessage semantics)',
      () {
        final bytes = buildCallMessageWithParamsBuilderSync(
          questionId: 7,
          targetImportId: 3,
          interfaceId: 0xDEADBEEF,
          methodId: 5,
          buildParams:
              (anyPtr) => anyPtr
                  .initStruct(_TextParamFactory())
                  .setTextField(0, 'hello'),
          references: const [],
        );
        final msg = parseRpcMessage(bytes);
        expect(msg.type, RpcMessageType.call);
        expect(msg.questionId, 7);
        expect(msg.targetImportId, 3);
        expect(msg.interfaceId, 0xDEADBEEF);
        expect(msg.methodId, 5);
        final paramsReq = msg.paramsContent!.asStruct(_TextParamFactory())!;
        expect(paramsReq.getTextField(0), 'hello');
      },
    );

    test(
      'buildCallMessageWithParamsBuilder resolves the capTable after buildParams '
      'runs',
      () async {
        final order = <String>[];
        final bytes = await buildCallMessageWithParamsBuilder(
          questionId: 9,
          targetImportId: 3,
          interfaceId: 0xDEADBEEF,
          methodId: 6,
          buildParams: (anyPtr) {
            order.add('build');
            anyPtr.initStruct(_TextParamFactory()).setTextField(0, 'world');
          },
          resolveCapTable: () async {
            order.add('resolve');
            return const [];
          },
        );
        expect(order, ['build', 'resolve']);
        final msg = parseRpcMessage(bytes);
        expect(msg.type, RpcMessageType.call);
        final paramsReq = msg.paramsContent!.asStruct(_TextParamFactory())!;
        expect(paramsReq.getTextField(0), 'world');
      },
    );

    test('call params round-trip multi-segment payload semantics', () {
      final params = _buildLargeDataParams(10000);

      final bytes = buildCallMessage(
        questionId: 8,
        targetImportId: 3,
        interfaceId: 0xDEADBEEF,
        methodId: 5,
        paramsBytes: params,
      );
      expect(_segmentCount(bytes), greaterThan(1));

      final msg = parseRpcMessage(bytes);
      final decoded = msg.paramsContent!.asStruct(_TextParamFactory())!;

      expect(
        _segmentCount(msg.paramsContent!.asMessageBytes()!),
        greaterThan(1),
      );
      expect(decoded.getDataField(0), orderedEquals(_largeData(10000)));
    });

    test('return results round-trip', () {
      // Build a valid Cap'n Proto message to use as results.
      final mb = MessageBuilder();
      mb.initRoot(_TextParamFactory()).setTextField(0, 'world');
      final results = mb.serialize();

      final bytes = buildReturnResultsMessage(
        answerId: 99,
        resultsBytes: results,
      );
      final msg = parseRpcMessage(bytes);
      expect(msg.type, RpcMessageType.return_);
      expect(msg.answerId, 99);
      expect(msg.isReturnResults, isTrue);
      // Verify the results round-trip correctly (semantic equality).
      final resultsReq = msg.resultsContent!.asStruct(_TextParamFactory())!;
      expect(resultsReq.getTextField(0), 'world');
    });

    test('return results round-trip multi-segment payload semantics', () {
      final results = _buildLargeDataParams(10000);

      final bytes = buildReturnResultsMessage(
        answerId: 100,
        resultsBytes: results,
      );
      expect(_segmentCount(bytes), greaterThan(1));

      final msg = parseRpcMessage(bytes);
      final decoded = msg.resultsContent!.asStruct(_TextParamFactory())!;

      expect(
        _segmentCount(msg.resultsContent!.asMessageBytes()!),
        greaterThan(1),
      );
      expect(decoded.getDataField(0), orderedEquals(_largeData(10000)));
    });

    test(
      'return results preserve capability pointers with multi-segment payload',
      () {
        final results = _buildLargeDataAndCapResult(10000);

        final bytes = buildReturnResultsWithCapsMessage(
          answerId: 101,
          resultsBytes: results,
          exportIds: const [123],
        );
        expect(_segmentCount(bytes), greaterThan(1));

        final msg = parseRpcMessage(bytes);
        final decoded = msg.resultsContent!.asStruct(_TwoPtrFactory())!;

        expect(
          _segmentCount(msg.resultsContent!.asMessageBytes()!),
          greaterThan(1),
        );
        expect(decoded.getDataField(0), orderedEquals(_largeData(10000)));
        expect(decoded.getCapabilityField(1), equals(0));
        expect(_exportIdsOf(msg.capabilityTableReferences), equals([123]));
      },
    );

    test('return exception round-trip', () {
      final bytes = buildReturnExceptionMessage(
        answerId: 5,
        reason: 'something broke',
      );
      final msg = parseRpcMessage(bytes);
      expect(msg.type, RpcMessageType.return_);
      expect(msg.answerId, 5);
      expect(msg.isReturnException, isTrue);
      expect(msg.exceptionReason, 'something broke');
      expect(msg.exceptionKind, ErrorKind.failed);
    });

    test('return exception round-trip preserves a non-default kind (#50)', () {
      final bytes = buildReturnExceptionMessage(
        answerId: 5,
        reason: 'peer gone',
        kind: ErrorKind.disconnected,
      );
      final msg = parseRpcMessage(bytes);
      expect(msg.exceptionKind, ErrorKind.disconnected);
    });

    test('bootstrap return round-trip (capTable)', () {
      final bytes = buildBootstrapReturnMessage(answerId: 1, exportId: 42);
      final msg = parseRpcMessage(bytes);
      expect(msg.type, RpcMessageType.return_);
      expect(msg.answerId, 1);
      expect(msg.isReturnResults, isTrue);
      expect(
        msg.capabilityTableReferences.single,
        isA<SenderHostedCapabilityReference>().having(
          (r) => r.exportId,
          'exportId',
          equals(42),
        ),
      );
    });

    test('resolve cap round-trip', () {
      final bytes = buildResolveCapMessage(
        promiseId: 11,
        reference: const SenderPromiseCapabilityReference(42),
      );
      final msg = parseRpcMessage(bytes);
      expect(msg.type, RpcMessageType.resolve);
      expect(msg.promiseId, 11);
      expect(msg.isResolveCap, isTrue);
      expect(
        msg.resolutionCapabilityReference,
        isA<SenderPromiseCapabilityReference>().having(
          (r) => r.exportId,
          'exportId',
          equals(42),
        ),
      );
    });

    test('resolve exception round-trip', () {
      final bytes = buildResolveExceptionMessage(
        promiseId: 11,
        reason: 'promise failed',
      );
      final msg = parseRpcMessage(bytes);
      expect(msg.type, RpcMessageType.resolve);
      expect(msg.promiseId, 11);
      expect(msg.isResolveException, isTrue);
      expect(msg.exceptionReason, 'promise failed');
      expect(msg.exceptionKind, ErrorKind.failed);
    });

    test('resolve exception round-trip preserves a non-default kind (#50)', () {
      final bytes = buildResolveExceptionMessage(
        promiseId: 11,
        reason: 'overloaded',
        kind: ErrorKind.overloaded,
      );
      final msg = parseRpcMessage(bytes);
      expect(msg.exceptionKind, ErrorKind.overloaded);
    });

    test('disembargo senderLoopback round-trip', () {
      final bytes = buildDisembargoMessage(
        targetPromisedAnswerQid: 7,
        targetTransformPath: [1],
        contextDisc: 0,
        contextId: 123,
      );
      final msg = parseRpcMessage(bytes);
      expect(msg.type, RpcMessageType.disembargo);
      expect(msg.disembargoContextDisc, 0);
      expect(msg.disembargoContextId, 123);
      expect(msg.disembargoTargetIsPromisedAnswer, isTrue);
      expect(msg.disembargoTargetPromisedAnswerQid, 7);
      expect(msg.disembargoTargetTransformPath, [1]);
    });

    test('finish round-trip', () {
      final bytes = buildFinishMessage(3);
      final msg = parseRpcMessage(bytes);
      expect(msg.type, RpcMessageType.finish);
      expect(msg.questionId, 3);
      expect(msg.releaseResultCaps, isTrue);
    });

    test('release round-trip', () {
      final bytes = buildReleaseMessage(7, 2);
      final msg = parseRpcMessage(bytes);
      expect(msg.type, RpcMessageType.release);
      expect(msg.releaseId, 7);
      expect(msg.referenceCount, 2);
    });

    test('abort round-trip', () {
      final bytes = buildAbortMessage('fatal error');
      final msg = parseRpcMessage(bytes);
      expect(msg.type, RpcMessageType.abort);
      expect(msg.exceptionReason, 'fatal error');
      expect(msg.exceptionKind, ErrorKind.failed);
    });

    test('abort round-trip preserves a non-default kind (#50)', () {
      final bytes = buildAbortMessage(
        'unimplemented feature',
        kind: ErrorKind.unimplemented,
      );
      final msg = parseRpcMessage(bytes);
      expect(msg.exceptionKind, ErrorKind.unimplemented);
    });

    test('an out-of-range wire Exception.type decodes to a safe fallback '
        'instead of throwing (#50)', () {
      // A future peer-side ErrorKind variant this vat doesn't know about
      // yet. No builder exposes an out-of-range type directly, so mangle
      // the byte for it instead (same technique as the "unknown disc"
      // test above) — found empirically by diffing two otherwise-
      // identical messages that only differ in `kind`, rather than
      // hardcoding an offset derived by hand.
      final withDefaultKind = buildReturnExceptionMessage(
        answerId: 9,
        reason: 'mystery error',
      );
      final withOtherKind = buildReturnExceptionMessage(
        answerId: 9,
        reason: 'mystery error',
        kind: ErrorKind.unimplemented,
      );
      expect(withDefaultKind.length, withOtherKind.length);
      var typeByteOffset = -1;
      for (var i = 0; i < withDefaultKind.length; i++) {
        if (withDefaultKind[i] != withOtherKind[i]) {
          typeByteOffset = i;
          break;
        }
      }
      expect(typeByteOffset, greaterThanOrEqualTo(0));

      final mangled = Uint8List.fromList(withDefaultKind);
      mangled[typeByteOffset] = 99; // out of ErrorKind.values range
      expect(parseRpcMessage(mangled).exceptionKind, ErrorKind.failed);
    });
  });

  group('rpc_message_codec — RPC-003 receiverHosted encoding', () {
    test('preserves an unsupported thirdPartyHosted descriptor', () {
      final reference =
          parseRpcMessage(
            _callWithCapDescriptorDisc(5),
          ).capabilityTableReferences.single;
      expect(
        reference,
        isA<UnsupportedCapabilityReference>().having(
          (r) => r.discriminant,
          'discriminant',
          5,
        ),
      );
    });

    test('buildCallMessage with receiverHosted entry encodes disc=3', () {
      final mb = MessageBuilder();
      mb.initRoot(_TextParamFactory()).setTextField(0, 'x');
      final params = mb.serialize();

      final bytes = buildCallMessage(
        questionId: 1,
        targetImportId: 0,
        interfaceId: 0xABCD,
        methodId: 0,
        paramsBytes: params,
        capabilityTableReferences: const [
          ReceiverHostedCapabilityReference(42),
        ],
      );
      final msg = parseRpcMessage(bytes);
      expect(msg.capabilityTableReferences, hasLength(1));
      expect(
        msg.capabilityTableReferences.single,
        isA<ReceiverHostedCapabilityReference>().having(
          (r) => r.importId,
          'importId',
          equals(42),
        ),
      );
    });

    test('buildCallMessage with senderHosted entry encodes disc=1', () {
      final mb = MessageBuilder();
      mb.initRoot(_TextParamFactory());
      final params = mb.serialize();

      final bytes = buildCallMessage(
        questionId: 1,
        targetImportId: 0,
        interfaceId: 0xABCD,
        methodId: 0,
        paramsBytes: params,
        capabilityTableReferences: const [SenderHostedCapabilityReference(7)],
      );
      final msg = parseRpcMessage(bytes);
      expect(
        msg.capabilityTableReferences.single,
        isA<SenderHostedCapabilityReference>().having(
          (r) => r.exportId,
          'exportId',
          equals(7),
        ),
      );
    });

    test('buildCallMessage with receiverAnswer entry encodes disc=4', () {
      final mb = MessageBuilder();
      mb.initRoot(_TextParamFactory());
      final params = mb.serialize();

      final bytes = buildCallMessage(
        questionId: 1,
        targetImportId: 0,
        interfaceId: 0xABCD,
        methodId: 0,
        paramsBytes: params,
        capabilityTableReferences: const [
          ReceiverAnswerCapabilityReference(9, [2]),
        ],
      );
      final msg = parseRpcMessage(bytes);
      expect(msg.capabilityTableReferences, hasLength(1));
      expect(
        msg.capabilityTableReferences[0],
        isA<ReceiverAnswerCapabilityReference>()
            .having((r) => r.questionId, 'questionId', equals(9))
            .having((r) => r.transformPath, 'transformPath', equals([2])),
      );
    });
  });

  group('rpc_message_codec — RPC-007 Unimplemented encoding', () {
    test('buildUnimplementedMessage has disc=0 (unimplemented)', () {
      final original = buildAbortMessage('test');
      final unimpl = buildUnimplementedMessage(original);
      final msg = parseRpcMessage(unimpl);
      expect(msg.type, equals(RpcMessageType.unimplemented));
    });

    test('unknown disc value parses as RpcMessageType.other', () {
      // Start from a known message and overwrite the disc field with an
      // unknown value (99).  Message layout in the framed bytes:
      //   [0..7]  framing header (8 bytes, 1 segment)
      //   [8..15] segment word 0: root struct pointer
      //   [16..23] segment word 1: Message data section (bytes 0-1 = disc)
      final releaseBytes = buildReleaseMessage(1, 1);
      final mangled = Uint8List.fromList(releaseBytes);
      mangled[16] = 99; // disc lo byte
      mangled[17] = 0; // disc hi byte
      expect(parseRpcMessage(mangled).type, equals(RpcMessageType.other));
    });
  });

  group(
    'TwoPartyRpcConnection — RPC-003 receiverHosted (imported cap returned to same peer)',
    () {
      test(
        'capability received from server and sent back arrives as server-side object',
        () async {
          final server = CapReceivingServer();
          final (client, serverConn) = _makePipe(server);

          // Bootstrap: returns the server object itself.
          final bootstrapCap = client.bootstrap(EchoClientFactory());

          // Warm up so the bootstrap exchange completes.
          await bootstrapCap.echo('warmup');

          // Call the server, passing the bootstrap capability back as a param.
          // With RPC-003 fixed, this should be sent as receiverHosted so the server
          // receives its own capability object — not a proxy.
          await bootstrapCap.cap.dispatch(
            _echoInterfaceId,
            _echoMethodId,
            RpcPayload.fromBytes(_buildEchoParams('test')),
            paramsCapabilities: [bootstrapCap.cap],
          );

          // The server should have received its own capability (identity
          // check) — through a fresh acquireCapabilityLease, per the
          // cross-connection/receiverHosted ownership fix (disposing a
          // received params capability must not be able to tear down the
          // export's own still-live reference to the same identity), so
          // the check unwraps first, exactly as any code that needs to
          // recognize a capability wrapped in a CapabilityLease's concrete identity
          // must (see unwrapCapabilityLease's doc comment).
          expect(server.lastParams, hasLength(1));
          expect(unwrapCapabilityLease(server.lastParams[0]), same(server));

          await client.close();
          await serverConn.close();
        },
      );

      test(
        'capTable wire encoding uses disc=3 (receiverHosted) for imported cap',
        () async {
          // Intercept the bytes going from client to server.
          final clientToServer = StreamController<Uint8List>();
          final serverToClient = StreamController<Uint8List>();
          final captured = <Uint8List>[];

          final interceptSink =
              StreamController<Uint8List>()
                ..stream.listen((b) {
                  captured.add(b);
                  clientToServer.add(b);
                });

          final client = TwoPartyRpcConnection.client(
            incoming: serverToClient.stream,
            outgoing: interceptSink.sink,
          );
          TwoPartyRpcConnection.server(
            incoming: clientToServer.stream,
            outgoing: serverToClient.sink,
            bootstrap: EchoServer(),
          );

          final stub = client.bootstrap(EchoClientFactory());
          await stub.echo('warmup'); // ensure bootstrap is resolved

          // Call with the bootstrap cap itself as a capability param.
          await stub.cap.dispatch(
            _echoInterfaceId,
            _echoMethodId,
            RpcPayload.fromBytes(_buildEchoParams('')),
            paramsCapabilities: [stub.cap],
          );

          // Find the Call message that has a non-empty capTable.
          final callWithCap =
              captured
                  .map(parseRpcMessage)
                  .where(
                    (m) =>
                        m.type == RpcMessageType.call &&
                        m.capabilityTableReferences.isNotEmpty,
                  )
                  .toList();

          expect(callWithCap, hasLength(1));
          // receiverHosted — the peer's own export, no proxy.
          expect(
            callWithCap.first.capabilityTableReferences.first,
            isA<ReceiverHostedCapabilityReference>(),
          );

          await client.close();
          await interceptSink.close();
        },
      );
    },
  );

  group('acquireCapabilityFromWireReference: receiverHosted validation', () {
    test('a receiverHosted descriptor naming an export id we never exported '
        'fails only that one call with Return.exception, and does not tear '
        'down the connection', () async {
      // Regression test: CapabilityProtocol.acquireCapabilityFromWireReference's
      // receiverHosted case
      // (disc=3) used to silently map an unknown export id to
      // NullCapability instead of treating it as the protocol violation
      // it is (a well-behaved peer, honoring the protocol's causal
      // ordering guarantees, never references an export id it wasn't
      // actually given) — conflating "the schema says this field is
      // legitimately absent" (disc=0/none) with "the peer referenced
      // something that was never exported to it".
      final serverInput = StreamController<Uint8List>();
      final serverOutput = StreamController<Uint8List>();
      final receivedMessages = <RpcMessage>[];
      serverOutput.stream.listen((bytes) {
        receivedMessages.add(parseRpcMessage(bytes));
      });
      TwoPartyRpcConnection.server(
        incoming: serverInput.stream,
        outgoing: serverOutput.sink,
        bootstrap: EchoServer(),
      );

      // A Call targeting bootstrap (export 0, always valid), carrying a
      // receiverHosted capTable entry naming an export id this vat never
      // actually exported.
      serverInput.add(
        buildCallMessage(
          questionId: 1,
          targetImportId: 0,
          interfaceId: _echoInterfaceId,
          methodId: _echoMethodId,
          paramsBytes: _buildEchoParams(''),
          capabilityTableReferences: const [
            ReceiverHostedCapabilityReference(99999),
          ],
        ),
      );
      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(receivedMessages, hasLength(1));
      expect(
        receivedMessages.single.isReturnException,
        isTrue,
        reason:
            'expected only the offending call to fail with '
            'Return.exception, got: ${receivedMessages.single.type}',
      );

      // The connection itself must still be alive and able to serve
      // further calls — unlike a genuinely unimplemented descriptor
      // (disc >= 5; see the "tears down the connection" test above), a
      // receiverHosted descriptor naming an unknown export id is only
      // this one call's problem.
      serverInput.add(
        buildCallMessage(
          questionId: 2,
          targetImportId: 0,
          interfaceId: _echoInterfaceId,
          methodId: _echoMethodId,
          paramsBytes: _buildEchoParams('ok'),
        ),
      );
      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(receivedMessages, hasLength(2));
      expect(receivedMessages[1].isReturnResults, isTrue);

      await serverInput.close();
    });

    test(
      'a descriptor that fails partway through a multi-entry capTable does '
      'not leak the capabilities that resolved successfully before it',
      () async {
        // Regression test: when acquireCapabilityFromWireReference throws partway
        // through decoding a Call's capTable, everything already decoded
        // before the failing entry (an import refcount bump, in this
        // case) used to just sit in the local `paramsCapabilities` list
        // forever — nothing ever disposed it, since the call itself never
        // reaches a real dispatch. A peer could repeat this (a valid
        // senderHosted entry followed by an invalid one) to leak import
        // refcounts indefinitely.
        final serverInput = StreamController<Uint8List>();
        final serverOutput = StreamController<Uint8List>();
        final receivedMessages = <RpcMessage>[];
        serverOutput.stream.listen((bytes) {
          receivedMessages.add(parseRpcMessage(bytes));
        });
        final serverConn = TwoPartyRpcConnection.server(
          incoming: serverInput.stream,
          outgoing: serverOutput.sink,
          bootstrap: EchoServer(),
        );

        serverInput.add(
          buildCallMessage(
            questionId: 1,
            targetImportId: 0,
            interfaceId: _echoInterfaceId,
            methodId: _echoMethodId,
            paramsBytes: _buildEchoParams(''),
            capabilityTableReferences: const [
              SenderHostedCapabilityReference(10),
              ReceiverHostedCapabilityReference(99999),
            ],
          ),
        );
        await Future<void>.delayed(const Duration(milliseconds: 20));

        final returns = receivedMessages.where((m) => m.isReturnException);
        expect(returns, hasLength(1));
        expect(
          returns.single.returnReleaseParamCaps,
          isFalse,
          reason:
              'an explicit Release for the already-resolved '
              'senderHosted(10) import is sent separately (checked below) '
              '— releaseParamCaps: true here too would tell the peer to '
              'also decrement its own export refcount for it a second '
              'time',
        );

        expect(
          serverConn.debugImportCount,
          equals(0),
          reason:
              'the senderHosted(10) import resolved before the failing '
              'descriptor was never disposed',
        );

        final releases = receivedMessages.where(
          (m) => m.type == RpcMessageType.release,
        );
        expect(releases, hasLength(1));
        expect(releases.single.releaseId, equals(10));
        expect(releases.single.referenceCount, equals(1));

        await serverInput.close();
      },
    );

    test(
      'after an error Return from a mid-capTable decode failure, the peer '
      'reusing the same question id before Finish is rejected as a '
      'protocol violation instead of being processed as a fresh call',
      () async {
        // Regression test: the error Return sent for a mid-capTable decode
        // failure used to leave `qid` registered in none of
        // _pendingCaps/_answerCaps/_answers/_finishedAnswers, so
        // _rejectDuplicateQuestionId (which checks exactly those tables)
        // couldn't distinguish a peer illegally reusing that same question
        // id before sending Finish for it from a legitimate fresh call.
        final serverInput = StreamController<Uint8List>();
        final serverOutput = StreamController<Uint8List>();
        serverOutput.stream.listen((_) {});
        final serverConn = TwoPartyRpcConnection.server(
          incoming: serverInput.stream,
          outgoing: serverOutput.sink,
          bootstrap: EchoServer(),
        );

        final doneExpectation = expectLater(
          serverConn.done,
          throwsA(
            isA<RpcException>().having(
              (e) => e.message,
              'message',
              contains('duplicate incoming question ID 1'),
            ),
          ),
        );

        serverInput.add(
          buildCallMessage(
            questionId: 1,
            targetImportId: 0,
            interfaceId: _echoInterfaceId,
            methodId: _echoMethodId,
            paramsBytes: _buildEchoParams(''),
            capabilityTableReferences: const [
              ReceiverHostedCapabilityReference(99999),
            ],
          ),
        );
        await Future<void>.delayed(const Duration(milliseconds: 20));

        // Reuses qid=1 before ever sending Finish for the first (failed)
        // attempt — a well-behaved peer never does this; a malformed or
        // hostile one might.
        serverInput.add(
          buildCallMessage(
            questionId: 1,
            targetImportId: 0,
            interfaceId: _echoInterfaceId,
            methodId: _echoMethodId,
            paramsBytes: _buildEchoParams(''),
          ),
        );

        await doneExpectation;
      },
    );
  });

  group('ownership: capability relayed across two different connections', () {
    test('a peer B releasing a relayed capability does not invalidate the '
        'relay\'s own still-held lease to it, and vat A only sees a Release '
        'once the relay disposes that lease too', () async {
      // peer A → relay → peer B: vat A returns `probe`, and the relay reads it
      // as `capabilityLease` through a generated accessor,
      // forwards it to vat B as a params capability of a call on a
      // completely different TwoPartyRpcConnection, and vat B immediately
      // releases its own reference to it. Before the _ExportEntry
      // identity/ownedReference split, the relay's connection-B export
      // table disposed the *raw* underlying capability directly on that
      // Release — invalidating `capabilityLease` out from under the relay even
      // though the relay never disposed it itself.
      final probe = EchoServer();
      final (relayToA, vatAConn) = _makePipe(_CapabilityReturningServer(probe));
      final vatABootstrap = relayToA.bootstrap(EchoClientFactory()).cap;

      final capabilityResult = await vatABootstrap.dispatch(
        _echoInterfaceId,
        _pipelineMethodId,
        RpcPayload.fromBytes(_buildEchoParams('')),
      );
      final capabilityLease = requireCapabilityFromResult(capabilityResult, 0);

      final (relayToB, vatBConn) = _makePipe(_DisposingReceiver());
      final vatBBootstrap = relayToB.bootstrap(EchoClientFactory()).cap;
      await vatBBootstrap.dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('')),
        paramsCapabilities: [capabilityLease],
      );
      // Let vat B's Release of its params capability reach the relay's
      // connection-B export table.
      await Future<void>.delayed(const Duration(milliseconds: 20));

      // The relay's own lease `capabilityLease` must still be live: it must neither be disposed
      // nor fail subsequent calls.
      final echoViaLease = await capabilityLease.dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('still alive')),
      );
      expect(
        _parseEchoResult(echoViaLease.payload),
        equals('echo: still alive'),
      );

      // Vat A must not have seen a Release yet for `probe`'s export — the
      // relay hasn't disposed its own lease. (2, not 1: the bootstrap
      // capability itself is also export 0 on this connection.)
      expect(vatAConn.debugExportCount, equals(2));

      // Now the relay disposes its own lease: only *now* should vat A's
      // export of `probe` be released and the underlying capability torn
      // down — leaving only the still-live bootstrap export behind.
      await capabilityLease.dispose();
      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(vatAConn.debugExportCount, equals(1));

      await relayToA.close();
      await vatAConn.close();
      await relayToB.close();
      await vatBConn.close();
    });

    test('a capability lease returned in DispatchResult.caps does not leak vat '
        'A\'s export once the recipient releases it', () async {
      // Same peer A → relay → peer C shape as the params-forwarding test
      // above, but this time relay forwards `capabilityLease` as its own
      // *result*
      // capability (DispatchResult.caps) instead of a call parameter —
      // the ownership-transfer contract there means relay's dispatch
      // handler does *not* separately dispose `capabilityLease` itself; the
      // runtime is
      // solely responsible for it from that point on. Before
      // CapabilityProtocol.exportResultCapabilityAsWireReference disposed a redundant
      // a [CapabilityLease] passed as `cap` once its own owning export reference was established,
      // `capabilityLease` was simply dropped — leaking its share of the underlying identity's
      // refcount forever, so vat A's export never actually cleared even
      // after C released its own reference to the result.
      final probe = EchoServer();
      final (relayToA, vatAConn) = _makePipe(_CapabilityReturningServer(probe));
      final vatABootstrap = relayToA.bootstrap(EchoClientFactory()).cap;

      final capabilityResult = await vatABootstrap.dispatch(
        _echoInterfaceId,
        _pipelineMethodId,
        RpcPayload.fromBytes(_buildEchoParams('')),
      );
      final capabilityLease = requireCapabilityFromResult(capabilityResult, 0);

      // Relay is the SERVER for connection C: its dispatch handler hands
      // off `capabilityLease` as its own result capability without disposing it
      // itself.
      final (cClient, relayConnForC) = _makePipe(
        _CapabilityReturningServer(capabilityLease),
      );
      final cBootstrap = cClient.bootstrap(EchoClientFactory()).cap;
      final resultForC = await cBootstrap.dispatch(
        _echoInterfaceId,
        _pipelineMethodId,
        RpcPayload.fromBytes(_buildEchoParams('')),
      );
      final cLease = requireCapabilityFromResult(resultForC, 0);

      expect(vatAConn.debugExportCount, equals(2));

      await cLease.dispose();
      await Future<void>.delayed(const Duration(milliseconds: 30));

      // Vat A's export of `probe` must now be gone — the redundant
      // `capabilityLease` reference inherited via DispatchResult.caps
      // no longer keeps it pinned forever.
      expect(vatAConn.debugExportCount, equals(1));

      await relayToA.close();
      await vatAConn.close();
      await cClient.close();
      await relayConnForC.close();
    });

    test('disposing a receiverHosted params capability does not corrupt an '
        'existing, still-referenced export of the same identity', () async {
      // peer A → relay → peer B, then B hands the *same* capability back
      // to relay as a params capability of a further call — wire-encoded
      // as receiverHosted, since it's relay's own export as far as B's
      // connection is concerned. Before acquireCapabilityFromWireReference's
      // receiverHosted case acquired a fresh lease instead of returning
      // the export's raw identity directly, relay's dispatch handler
      // disposing that received params capability tore down the shared
      // underlying identity directly — invalidating vatB's own,
      // still-live, never-released reference to the same capability out
      // from under it.
      final probe = EchoServer();
      final (relayToA, vatAConn) = _makePipe(_CapabilityReturningServer(probe));
      final vatABootstrap = relayToA.bootstrap(EchoClientFactory()).cap;

      final capabilityResult = await vatABootstrap.dispatch(
        _echoInterfaceId,
        _pipelineMethodId,
        RpcPayload.fromBytes(_buildEchoParams('')),
      );
      final capabilityLease = requireCapabilityFromResult(capabilityResult, 0);

      // Relay forwards both `capabilityLease` and a callback capability (playing
      // relay's own dispatch handler for the "B sends it back" leg) to
      // vatB in one call, creating relay's own export for `capabilityLease`'s identity
      // on relayToB.
      final vatB = CapReceivingServer();
      final (relayToB, vatBConn) = _makePipe(vatB);
      final vatBBootstrap = relayToB.bootstrap(EchoClientFactory()).cap;
      await vatBBootstrap.dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('')),
        paramsCapabilities: [capabilityLease, _DisposingReceiver()],
      );
      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(vatB.lastParams, hasLength(2));
      final vatBsT =
          vatB.lastParams[0]; // vatB's own import of the relayed capability
      final vatBsDisposer =
          vatB.lastParams[1]; // vatB's own import of relay's disposer

      // vatB "sends it back" to relay by using it as a params capability
      // of a call targeting the disposer callback — both are vatB's own
      // imports on vatBConn, so this is encoded as receiverHosted on the
      // wire. The disposer (relay's own dispatch handler for this call)
      // disposes it immediately, per the usual "done with this param"
      // pattern.
      await vatBsDisposer.dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('')),
        paramsCapabilities: [vatBsT],
      );
      await Future<void>.delayed(const Duration(milliseconds: 20));

      // vatB never disposed or released its own reference — it must
      // still work.
      final stillWorks = await vatBsT.dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('still alive')),
      );
      expect(_parseEchoResult(stillWorks.payload), equals('echo: still alive'));

      await relayToA.close();
      await vatAConn.close();
      await relayToB.close();
      await vatBConn.close();
    });
  });

  group('ownership: bootstrap capability identity normalization', () {
    test(
      'a bootstrap capability passed as an already-acquired lease normalizes '
      'to its unwrapped identity, so a later export of the same underlying '
      'capability dedupes against it instead of creating a redundant export',
      () async {
        final rawServer = PipelineServer();
        final vendedBootstrap = acquireCapabilityLease(rawServer);
        final (client, serverConn) = _makePipe(vendedBootstrap);

        final bootstrapCap = client.bootstrap(EchoClientFactory());
        await bootstrapCap.echo('warmup');
        expect(serverConn.debugExportCount, equals(1));

        // PipelineServer's _pipelineMethodId returns `caps: [this]`, i.e.
        // `rawServer` itself (unwrapped) — a second, ordinary export of the
        // exact same underlying capability bootstrap was registered with.
        await bootstrapCap.cap.dispatch(
          _echoInterfaceId,
          _pipelineMethodId,
          RpcPayload.fromBytes(_buildEchoParams('')),
        );

        // If bootstrap's identity had not been normalized (unwrapped) at
        // registration time, `_exportIds` would have been keyed by the
        // capability lease instead of `rawServer`, so this second export
        // wouldn't dedupe against export 0 and would show up as a second,
        // redundant entry.
        expect(serverConn.debugExportCount, equals(1));

        await client.close();
        await serverConn.close();
      },
    );
  });

  group('acquireCapabilityLease delegates tryTailCall', () {
    test('a capability lease forwards tryTailCall to its target instead of '
        'silently disabling the tail-call optimization', () {
      final target = TailCallServer();
      final lease = acquireCapabilityLease(target);
      expect(lease, isA<CapabilityLease>());

      final direct = target.tryTailCall(
        _echoInterfaceId,
        _tailCallMethodId,
        RpcPayload.fromBytes(_buildEchoParams('')),
        paramsCapabilities: [EchoServer()],
      );
      final viaLease = lease.tryTailCall(
        _echoInterfaceId,
        _tailCallMethodId,
        RpcPayload.fromBytes(_buildEchoParams('')),
        paramsCapabilities: [EchoServer()],
      );

      expect(direct, isNotNull);
      expect(viaLease, isNotNull);
      expect(viaLease!.interfaceId, equals(direct!.interfaceId));
      expect(viaLease.methodId, equals(direct.methodId));
    });
  });

  group(
    'acquireCapabilityLease: rejects leasing after disposal was triggered',
    () {
      test('leasing again after a prior cycle already fully disposed throws — '
          'unconditionally, not just in debug/test builds — instead of silently '
          'starting a fresh cycle that can never truly resurrect the '
          'already-torn-down target', () async {
        final target = CountingCapability();

        final h1 = acquireCapabilityLease(target);
        await h1.dispose();
        expect(target.disposeCount, equals(1));

        // The target itself is already torn down for real — leasing a
        // "fresh" lease for it now would be a lie (it would look live but
        // dispatch through it would just hit whatever broken state
        // target.dispose() already left behind).
        expect(
          () => acquireCapabilityLease(target),
          throwsA(isA<StateError>()),
        );
      });

      test('leasing while the previous cycle\'s dispose() is still in flight '
          '(not yet actually finished) also throws, rather than racing a fresh '
          'lease against a disposal that could finish tearing the target down '
          'at any moment', () async {
        final target = SlowDisposeCapability();

        final h1 = acquireCapabilityLease(target);
        // Don't await yet — dispose() is blocked on the gate, so this cycle
        // is "triggered" (disposeFuture assigned) but not yet finished.
        final h1DisposeFuture = h1.dispose();

        expect(
          () => acquireCapabilityLease(target),
          throwsA(isA<StateError>()),
        );

        target.releaseDispose();
        await h1DisposeFuture;
        expect(target.disposeCount, equals(1));
      });
    },
  );

  group('_ReceiverAnswerCapability: resolved lease reuse and disposal', () {
    test('a receiverAnswer-decoded capability dispatched multiple times reuses '
        'one resolved lease instead of leaking a fresh one per call, and '
        'dispose() releases exactly that one reference', () async {
      final clientToServer = StreamController<Uint8List>();
      final serverToClient = StreamController<Uint8List>();
      serverToClient.stream.listen((_) {});

      final leaf = CountingCapability();
      final probe = ReceiverAnswerProbeServer(leaf);
      TwoPartyRpcConnection.server(
        incoming: clientToServer.stream,
        outgoing: serverToClient.sink,
        bootstrap: probe,
      );

      // Call 1: probe.pipeline() -> caps: [leaf] at ptr slot 0. This vat
      // (the server) now has an outstanding answer #1 whose result
      // capability is `leaf`.
      clientToServer.add(
        buildCallMessage(
          questionId: 1,
          targetImportId: 0,
          interfaceId: _echoInterfaceId,
          methodId: _pipelineMethodId,
          paramsBytes: _buildEchoParams(''),
        ),
      );
      await Future<void>.delayed(const Duration(milliseconds: 20));

      // Call 2: probe.echo(...), with a receiverAnswer(1, [0]) param —
      // telling this same vat "the capability I'm passing you is your
      // own answer #1's capability at path [0]" — i.e. handing `leaf`
      // back to the vat that produced it.
      clientToServer.add(
        buildCallMessage(
          questionId: 2,
          targetImportId: 0,
          interfaceId: _echoInterfaceId,
          methodId: _echoMethodId,
          paramsBytes: _buildEchoParams(''),
          capabilityTableReferences: [
            ReceiverAnswerCapabilityReference(1, [0]),
          ],
        ),
      );
      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(probe.lastParams, hasLength(1));
      final receiverAnswerCap = probe.lastParams[0];

      // Dispatch through it twice, *while* answer #1 is still tracked
      // (_answerCaps[1]/_pendingCaps[1]) — before the fix, each call
      // acquired (and never disposed) a fresh lease to `leaf`.
      await receiverAnswerCap.dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('a')),
      );
      await receiverAnswerCap.dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('b')),
      );

      // Finish question 1 now — safe only *after* the receiverAnswer
      // resolution above already captured its own reference, since
      // Finish drops `_answerCaps[1]` unconditionally. This releases the
      // *separate* export-owned reference to `leaf` that sending call 1's
      // own Return created (every result capability gets exported to the
      // peer, independent of whether anything ever names it via
      // receiverAnswer) — the one other outstanding reference besides
      // whatever receiverAnswerCap itself is holding.
      clientToServer.add(buildFinishMessage(1, releaseResultCaps: true));
      await Future<void>.delayed(const Duration(milliseconds: 20));

      // Only the receiverAnswer-side reference(s) remain now — none of
      // them disposed yet.
      expect(leaf.disposeCount, equals(0));

      await receiverAnswerCap.dispose();
      // Both dispatch() calls sharing one resolved lease (rather than
      // leaking a fresh one each) means this single dispose() call is
      // enough to release the last remaining reference for real.
      expect(leaf.disposeCount, equals(1));

      await clientToServer.close();
    });

    test(
      'dispose() does not tear down the resolved target while a dispatch() '
      'call through the same receiverAnswer capability is still in flight',
      () async {
        // Empirical check for a reviewed concern: unlike _PipelinedCapability
        // (which defers disposing its resolved target until every pipelined
        // call made *before* resolution has settled — see
        // _PipelinedCapability's _pendingPipelinedCalls),
        // _ReceiverAnswerCapability's dispose() has no equivalent tracking at
        // all. This drives a dispatch() call all the way to actually blocking
        // inside the resolved target (DisposeOrderProbeCapability, gated on
        // dispatchGate) and then disposes the receiverAnswer capability while
        // that call is still pending, to see whether the shared resolved
        // lease's real target.dispose() actually runs concurrently with it.
        final clientToServer = StreamController<Uint8List>();
        final serverToClient = StreamController<Uint8List>();
        serverToClient.stream.listen((_) {});

        final leaf = DisposeOrderProbeCapability();
        final probe = ReceiverAnswerProbeServer(leaf);
        TwoPartyRpcConnection.server(
          incoming: clientToServer.stream,
          outgoing: serverToClient.sink,
          bootstrap: probe,
        );

        clientToServer.add(
          buildCallMessage(
            questionId: 1,
            targetImportId: 0,
            interfaceId: _echoInterfaceId,
            methodId: _pipelineMethodId,
            paramsBytes: _buildEchoParams(''),
          ),
        );
        await Future<void>.delayed(const Duration(milliseconds: 20));

        clientToServer.add(
          buildCallMessage(
            questionId: 2,
            targetImportId: 0,
            interfaceId: _echoInterfaceId,
            methodId: _echoMethodId,
            paramsBytes: _buildEchoParams(''),
            capabilityTableReferences: [
              ReceiverAnswerCapabilityReference(1, [0]),
            ],
          ),
        );
        await Future<void>.delayed(const Duration(milliseconds: 20));

        expect(probe.lastParams, hasLength(1));
        final receiverAnswerCap = probe.lastParams[0];

        // Start a call and let it actually enter DisposeOrderProbeCapability
        // .dispatch(), where it now sits blocked on dispatchGate.
        final dispatchFuture = receiverAnswerCap.dispatch(
          _echoInterfaceId,
          _echoMethodId,
          RpcPayload.fromBytes(_buildEchoParams('x')),
        );
        await Future<void>.delayed(const Duration(milliseconds: 20));

        // Release the other outstanding reference to `leaf` (answer #1's own
        // export), same as the test above, so receiverAnswerCap's lease is
        // the last one left.
        clientToServer.add(buildFinishMessage(1, releaseResultCaps: true));
        await Future<void>.delayed(const Duration(milliseconds: 20));

        // Dispose while the dispatch() above is still blocked mid-call.
        final disposeFuture = receiverAnswerCap.dispose();
        await Future<void>.delayed(const Duration(milliseconds: 20));

        expect(
          leaf.disposedWhileDispatchInFlight,
          isFalse,
          reason:
              'dispose() tore down the resolved target while a dispatch() '
              'call through the same capability was still in flight',
        );

        leaf.dispatchGate.complete();
        await dispatchFuture;
        await disposeFuture;

        await clientToServer.close();
      },
    );

    test(
      'dispose() itself does not resolve until the resolved target\'s real '
      'dispose() has actually completed — not merely been scheduled — when '
      'disposed while a call through the same capability is still in flight',
      () async {
        // Reviewed concern about the fix above: tracking in-flight calls so
        // the real target isn't disposed while one is still running is
        // necessary but not sufficient on its own — dispose()'s own
        // returned Future must also not resolve until that deferred real
        // disposal has actually finished (Capability.dispose()'s own doc
        // comment: "frees any associated resources"), not merely been
        // scheduled to run once the last pending call drains.
        final clientToServer = StreamController<Uint8List>();
        final serverToClient = StreamController<Uint8List>();
        serverToClient.stream.listen((_) {});

        final leaf = DisposeOrderProbeCapability();
        final probe = ReceiverAnswerProbeServer(leaf);
        TwoPartyRpcConnection.server(
          incoming: clientToServer.stream,
          outgoing: serverToClient.sink,
          bootstrap: probe,
        );

        clientToServer.add(
          buildCallMessage(
            questionId: 1,
            targetImportId: 0,
            interfaceId: _echoInterfaceId,
            methodId: _pipelineMethodId,
            paramsBytes: _buildEchoParams(''),
          ),
        );
        await Future<void>.delayed(const Duration(milliseconds: 20));

        clientToServer.add(
          buildCallMessage(
            questionId: 2,
            targetImportId: 0,
            interfaceId: _echoInterfaceId,
            methodId: _echoMethodId,
            paramsBytes: _buildEchoParams(''),
            capabilityTableReferences: [
              ReceiverAnswerCapabilityReference(1, [0]),
            ],
          ),
        );
        await Future<void>.delayed(const Duration(milliseconds: 20));

        expect(probe.lastParams, hasLength(1));
        final receiverAnswerCap = probe.lastParams[0];

        final dispatchFuture = receiverAnswerCap.dispatch(
          _echoInterfaceId,
          _echoMethodId,
          RpcPayload.fromBytes(_buildEchoParams('x')),
        );
        await Future<void>.delayed(const Duration(milliseconds: 20));

        clientToServer.add(buildFinishMessage(1, releaseResultCaps: true));
        await Future<void>.delayed(const Duration(milliseconds: 20));

        var disposeCompleted = false;
        final disposeFuture = receiverAnswerCap.dispose().whenComplete(() {
          disposeCompleted = true;
        });
        await Future<void>.delayed(const Duration(milliseconds: 20));
        expect(
          disposeCompleted,
          isFalse,
          reason:
              'dispose() resolved before the in-flight dispatch() call — '
              'and therefore the resolved target\'s real dispose() — ever '
              'settled',
        );

        leaf.dispatchGate.complete();
        await dispatchFuture;
        await disposeFuture;

        expect(disposeCompleted, isTrue);
        expect(leaf.disposeCount, equals(1));

        await clientToServer.close();
      },
    );

    test('a failure from the resolved target\'s own dispose() propagates '
        'through receiverAnswerCap.dispose() itself, instead of being '
        'silently reported as success', () async {
      // Reviewed concern about the fix above: the deferred real disposal
      // used to be forwarded to _disposeCompleter via whenComplete(...),
      // which runs identically on success *and* failure and can't tell
      // them apart — so a real failure from the resolved target's own
      // dispose() was always reported as a successful dispose() to
      // receiverAnswerCap's own caller, silently discarding it. That
      // directly contradicts Capability.dispose()'s own doc comment
      // ("frees any associated resources") — a caller awaiting dispose()
      // has no way to learn cleanup actually failed.
      final clientToServer = StreamController<Uint8List>();
      final serverToClient = StreamController<Uint8List>();
      serverToClient.stream.listen((_) {});

      final leaf = ThrowingDisposeCapability();
      final probe = ReceiverAnswerProbeServer(leaf);
      TwoPartyRpcConnection.server(
        incoming: clientToServer.stream,
        outgoing: serverToClient.sink,
        bootstrap: probe,
      );

      clientToServer.add(
        buildCallMessage(
          questionId: 1,
          targetImportId: 0,
          interfaceId: _echoInterfaceId,
          methodId: _pipelineMethodId,
          paramsBytes: _buildEchoParams(''),
        ),
      );
      await Future<void>.delayed(const Duration(milliseconds: 20));

      clientToServer.add(
        buildCallMessage(
          questionId: 2,
          targetImportId: 0,
          interfaceId: _echoInterfaceId,
          methodId: _echoMethodId,
          paramsBytes: _buildEchoParams(''),
          capabilityTableReferences: [
            ReceiverAnswerCapabilityReference(1, [0]),
          ],
        ),
      );
      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(probe.lastParams, hasLength(1));
      final receiverAnswerCap = probe.lastParams[0];

      // Forces resolution (ThrowingDisposeCapability.dispatch() itself
      // just rejects — irrelevant here, only resolving matters).
      await expectLater(
        receiverAnswerCap.dispatch(
          _echoInterfaceId,
          _echoMethodId,
          RpcPayload.fromBytes(_buildEchoParams('x')),
        ),
        throwsA(isA<UnsupportedError>()),
      );

      // Release the other outstanding reference to `leaf` (answer #1's
      // own export), so receiverAnswerCap's lease is the last one left —
      // its dispose() below is what actually tears `leaf` down for real.
      clientToServer.add(buildFinishMessage(1, releaseResultCaps: true));
      await Future<void>.delayed(const Duration(milliseconds: 20));

      await expectLater(
        receiverAnswerCap.dispose(),
        throwsA(isA<StateError>()),
      );

      await clientToServer.close();
    });
  });

  group('_ImportState.replacement: disposed when the import is released', () {
    test('a promise import that resolved to a replacement capability disposes '
        'that replacement once the import itself is fully released', () async {
      final clientToServer = StreamController<Uint8List>();
      final serverToClient = StreamController<Uint8List>();
      final server = PromisedReturnServer();
      final resolvedTarget = CountingCapability();

      final serverConn = TwoPartyRpcConnection.server(
        incoming: clientToServer.stream,
        outgoing: serverToClient.sink,
        bootstrap: server,
      );
      final client = TwoPartyRpcConnection.client(
        incoming: serverToClient.stream,
        outgoing: clientToServer.sink,
      );

      final bootstrapCap = client.bootstrap(EchoClientFactory());
      await bootstrapCap.echo('warmup');

      // A plain (non-pipelined) call: awaits the full Return directly, so
      // the only reference to the promise's import is the one this test
      // holds itself — no _PipelinedCapability involved to also
      // retain it.
      final result = await bootstrapCap.cap.dispatch(
        _echoInterfaceId,
        _pipelineMethodId,
        RpcPayload.fromBytes(_buildEchoParams('')),
      );
      final promiseCap = requireCapabilityFromResult(result, 0);

      // Resolve the promise to resolvedTarget — the server exports it and
      // sends Resolve(cap); the client's _ImportState for the promise's
      // import id now has `replacement` set to a fresh import of it.
      server.completer.complete(resolvedTarget);
      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(resolvedTarget.disposeCount, equals(0));

      // Releasing the *promise* import (not `replacement` directly —
      // nothing in this test ever touches `replacement` itself) must
      // cascade to disposing `replacement`, since every call through the
      // promise import forwards to it and nothing else references it.
      await promiseCap.dispose();
      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(resolvedTarget.disposeCount, equals(1));

      await client.close();
      await serverConn.close();
    });
  });

  group('TwoPartyRpcConnection — RPC-007 Unimplemented for unknown messages', () {
    test(
      'thirdPartyHosted tears down the connection as unimplemented',
      () async {
        final serverInput = StreamController<Uint8List>();
        final serverOutput = StreamController<Uint8List>();
        serverOutput.stream.listen((_) {});
        final server = TwoPartyRpcConnection.server(
          incoming: serverInput.stream,
          outgoing: serverOutput.sink,
          bootstrap: EchoServer(),
        );

        final doneExpectation = expectLater(
          server.done,
          throwsA(
            isA<RpcException>().having(
              (error) => error.cause,
              'cause',
              isA<RpcException>()
                  .having(
                    (cause) => cause.kind,
                    'kind',
                    ErrorKind.unimplemented,
                  )
                  .having(
                    (cause) => cause.message,
                    'message',
                    contains('reference (disc=5)'),
                  ),
            ),
          ),
        );

        serverInput.add(_callWithCapDescriptorDisc(5));
        await doneExpectation;
        await serverInput.close();
        await serverOutput.close();
      },
    );

    test('an entry that resolved before an unimplemented descriptor is still '
        'disposed before the connection tears down', () async {
      // Regression test: the unimplemented rethrow used to skip the
      // dispose loop entirely (it ran only in the per-call-failure
      // branch below it) — any capTable entry that had already resolved
      // successfully before the unimplemented one (e.g. a receiverHosted
      // descriptor acquiring a fresh lease to bootstrap/an export) was
      // simply abandoned. _tearDown only ever disposes each export's own
      // single `ownedReference` — it has no way to know about this
      // *additional* capability lease, so the shared refcount for that
      // identity never actually reached zero, and the real capability's
      // own dispose() never ran even though the connection (and every
      // other reference to it) was long gone.
      final bootstrap = CountingCapability();
      final serverInput = StreamController<Uint8List>();
      final serverOutput = StreamController<Uint8List>();
      serverOutput.stream.listen((_) {});
      final server = TwoPartyRpcConnection.server(
        incoming: serverInput.stream,
        outgoing: serverOutput.sink,
        bootstrap: bootstrap,
      );

      final doneExpectation = expectLater(
        server.done,
        throwsA(
          isA<RpcException>().having(
            (error) => error.cause,
            'cause',
            isA<RpcException>().having(
              (cause) => cause.kind,
              'kind',
              ErrorKind.unimplemented,
            ),
          ),
        ),
      );

      // [0] receiverHosted(0) — resolves successfully, acquiring a fresh
      // lease to bootstrap (export 0) in addition to the export's own
      // ownedReference. [1] disc=5 (thirdPartyHosted) — unimplemented.
      serverInput.add(_callWithReceiverHostedThenCapDescriptorDisc(5));
      await doneExpectation;

      expect(
        bootstrap.disposeCount,
        equals(1),
        reason:
            'the extra lease acquired for the receiverHosted(0) entry '
            'was never disposed, so the shared refcount for bootstrap '
            'never reached zero even after teardown released the '
            "export's own reference",
      );

      await serverInput.close();
      await serverOutput.close();
    });

    test(
      'server sends Unimplemented when it receives a message with unknown disc',
      () async {
        final serverInput = StreamController<Uint8List>();
        final serverOutput = StreamController<Uint8List>();
        final serverReceived = <RpcMessage>[];

        serverOutput.stream.listen(
          (bytes) => serverReceived.add(parseRpcMessage(bytes)),
        );

        TwoPartyRpcConnection.server(
          incoming: serverInput.stream,
          outgoing: serverOutput.sink,
          bootstrap: EchoServer(),
        );

        // Build a message with an unknown disc (99) by mangling a Release message.
        final releaseBytes = buildReleaseMessage(1, 1);
        final mangled = Uint8List.fromList(releaseBytes);
        mangled[16] = 99; // overwrite disc lo byte
        mangled[17] = 0;

        serverInput.add(mangled);
        await Future<void>.delayed(Duration.zero); // let the event loop run

        expect(
          serverReceived.any((m) => m.type == RpcMessageType.unimplemented),
          isTrue,
          reason:
              'server should reply with Unimplemented for unknown message disc',
        );

        await serverInput.close();
      },
    );
  });

  group('TwoPartyRpcConnection — RPC Level 1 Resolve / Disembargo', () {
    test(
      'server returns DeferredCapability as senderPromise and sends Resolve',
      () async {
        final clientToServer = StreamController<Uint8List>();
        final serverToClient = StreamController<Uint8List>();
        final serverCaptured = <Uint8List>[];
        final server = PromisedReturnServer();

        final serverIntercept =
            StreamController<Uint8List>()
              ..stream.listen((bytes) {
                serverCaptured.add(bytes);
                serverToClient.add(bytes);
              });

        final serverConn = TwoPartyRpcConnection.server(
          incoming: clientToServer.stream,
          outgoing: serverIntercept.sink,
          bootstrap: server,
        );
        final client = TwoPartyRpcConnection.client(
          incoming: serverToClient.stream,
          outgoing: clientToServer.sink,
        );

        final bootstrapCap = client.bootstrap(EchoClientFactory());
        await bootstrapCap.echo('warmup');

        serverCaptured.clear();
        final parent = bootstrapCap.cap.dispatchForPipelining(
          _echoInterfaceId,
          _pipelineMethodId,
          RpcPayload.fromBytes(_buildEchoParams('')),
        );
        final pipelinedCap = parent.pipelinedCapability(0);
        final pipelinedCall = pipelinedCap.dispatch(
          _echoInterfaceId,
          _echoMethodId,
          RpcPayload.fromBytes(_buildEchoParams('before-resolve')),
        );

        final ret = await _waitForMessageType(
          serverCaptured,
          RpcMessageType.return_,
        );
        expect(ret.isReturnResults, isTrue);
        expect(ret.capabilityTableReferences, hasLength(1));
        expect(
          ret.capabilityTableReferences.single,
          isA<SenderPromiseCapabilityReference>(),
        );
        final promiseId = _exportIdOf(ret.capabilityTableReferences.single);

        var pipelinedCompleted = false;
        pipelinedCall.then((_) => pipelinedCompleted = true).ignore();
        await Future<void>.delayed(const Duration(milliseconds: 20));
        expect(pipelinedCompleted, isFalse);

        server.completer.complete(EchoServer());
        final pipelinedResult = await pipelinedCall.timeout(
          const Duration(seconds: 2),
        );
        expect(
          _parseEchoResult(pipelinedResult.payload),
          equals('echo: before-resolve'),
        );

        final resolve = await _waitForMessageType(
          serverCaptured,
          RpcMessageType.resolve,
        );
        expect(resolve.promiseId, promiseId);
        expect(resolve.isResolveCap, isTrue);
        expect(
          resolve.resolutionCapabilityReference,
          isA<SenderHostedCapabilityReference>(),
        );

        await parent.result;
        await client.close();
        await serverConn.close();
        await serverIntercept.close();
      },
    );

    test("connection torn down while waiting on a "
        "senderPromise's Resolve -- the pending pipelined call fails "
        "without hanging", () async {
      final clientToServer = StreamController<Uint8List>();
      final serverToClient = StreamController<Uint8List>();
      final server = PromisedReturnServer();

      final serverConn = TwoPartyRpcConnection.server(
        incoming: clientToServer.stream,
        outgoing: serverToClient.sink,
        bootstrap: server,
      );
      final client = TwoPartyRpcConnection.client(
        incoming: serverToClient.stream,
        outgoing: clientToServer.sink,
      );

      final bootstrapCap = client.bootstrap(EchoClientFactory());
      await bootstrapCap.echo('warmup');

      final parent = bootstrapCap.cap.dispatchForPipelining(
        _echoInterfaceId,
        _pipelineMethodId,
        RpcPayload.fromBytes(_buildEchoParams('')),
      );
      final pipelinedCap = parent.pipelinedCapability(0);
      final pipelinedCall = pipelinedCap.dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('before-resolve')),
      );
      // The parent call itself resolves immediately (PromisedReturnServer
      // returns the still-unresolved promise as its result right away);
      // only the pipelined call rides on that promise's eventual Resolve.
      await parent.result;

      await client.close();
      await serverConn.done.catchError((_) {});

      await expectLater(pipelinedCall, throwsA(isA<RpcException>()));
      expect(client.debugImportCount, equals(0));
      expect(client.debugBrokenImportCount, equals(0));

      await server.promised.dispose().timeout(const Duration(seconds: 1));
    });

    test('server sends Resolve(exception) when senderPromise fails', () async {
      final clientToServer = StreamController<Uint8List>();
      final serverToClient = StreamController<Uint8List>();
      final serverCaptured = <Uint8List>[];
      final server = PromisedReturnServer();

      final serverIntercept =
          StreamController<Uint8List>()
            ..stream.listen((bytes) {
              serverCaptured.add(bytes);
              serverToClient.add(bytes);
            });

      final serverConn = TwoPartyRpcConnection.server(
        incoming: clientToServer.stream,
        outgoing: serverIntercept.sink,
        bootstrap: server,
      );
      final client = TwoPartyRpcConnection.client(
        incoming: serverToClient.stream,
        outgoing: clientToServer.sink,
      );

      final bootstrapCap = client.bootstrap(EchoClientFactory());
      await bootstrapCap.echo('warmup');

      serverCaptured.clear();
      final parent = bootstrapCap.cap.dispatchForPipelining(
        _echoInterfaceId,
        _pipelineMethodId,
        RpcPayload.fromBytes(_buildEchoParams('')),
      );
      final pipelinedCap = parent.pipelinedCapability(0);
      await parent.result;

      final ret = await _waitForMessageType(
        serverCaptured,
        RpcMessageType.return_,
      );
      final promiseId = _exportIdOf(ret.capabilityTableReferences.single);

      server.completer.completeError(const RpcException('promise failed'));
      final resolve = await _waitForMessageType(
        serverCaptured,
        RpcMessageType.resolve,
      );
      expect(resolve.promiseId, promiseId);
      expect(resolve.isResolveException, isTrue);
      expect(resolve.exceptionReason, contains('promise failed'));

      await expectLater(
        pipelinedCap.dispatch(
          _echoInterfaceId,
          _echoMethodId,
          RpcPayload.fromBytes(_buildEchoParams('after-failure')),
        ),
        throwsA(
          allOf(
            isA<RpcException>(),
            predicate<Object>((e) => e.toString().contains('promise failed')),
          ),
        ),
      );

      await client.close();
      await serverConn.close();
      await serverIntercept.close();
    });

    test(
      'incoming Resolve(cap) is handled and releases unused descriptor',
      () async {
        final input = StreamController<Uint8List>();
        final output = StreamController<Uint8List>();
        final received = <RpcMessage>[];
        output.stream.listen((bytes) => received.add(parseRpcMessage(bytes)));

        final conn = TwoPartyRpcConnection.server(
          incoming: input.stream,
          outgoing: output.sink,
          bootstrap: EchoServer(),
        );

        input.add(
          buildResolveCapMessage(
            promiseId: 9,
            reference: const SenderHostedCapabilityReference(42),
          ),
        );
        await Future<void>.delayed(Duration.zero);

        expect(
          received.where((m) => m.type == RpcMessageType.unimplemented),
          isEmpty,
        );
        final releases =
            received.where((m) => m.type == RpcMessageType.release).toList();
        expect(releases, hasLength(1));
        expect(releases.single.releaseId, 42);
        expect(releases.single.referenceCount, 1);

        await input.close();
        await conn.done;
      },
    );

    test(
      'incoming Resolve(exception) is handled without Unimplemented',
      () async {
        final input = StreamController<Uint8List>();
        final output = StreamController<Uint8List>();
        final received = <RpcMessage>[];
        output.stream.listen((bytes) => received.add(parseRpcMessage(bytes)));

        final conn = TwoPartyRpcConnection.server(
          incoming: input.stream,
          outgoing: output.sink,
          bootstrap: EchoServer(),
        );

        input.add(
          buildResolveExceptionMessage(promiseId: 9, reason: 'promise failed'),
        );
        await Future<void>.delayed(Duration.zero);

        expect(
          received.where((m) => m.type == RpcMessageType.unimplemented),
          isEmpty,
        );

        await input.close();
        await conn.done;
      },
    );

    test('a Resolve(exception) that arrives after the senderPromise import it '
        'names was already released does not resurrect import/broken-import '
        'state', () async {
      final input = StreamController<Uint8List>();
      final output = StreamController<Uint8List>();
      output.stream.listen((_) {});

      final capReceiver = CapReceivingServer();
      final conn = TwoPartyRpcConnection.server(
        incoming: input.stream,
        outgoing: output.sink,
        bootstrap: capReceiver,
      );

      // A Call whose params capTable carries a senderPromise(10)
      // descriptor makes the server genuinely import it — matching how a
      // real peer references a promise capability it's hosting.
      input.add(
        buildCallMessage(
          questionId: 1,
          targetImportId: 0,
          interfaceId: _echoInterfaceId,
          methodId: _echoMethodId,
          paramsBytes: _buildEchoParams(''),
          capabilityTableReferences: const [
            SenderPromiseCapabilityReference(10),
          ],
        ),
      );
      await Future<void>.delayed(Duration.zero);

      expect(capReceiver.lastParams, hasLength(1));
      expect(conn.debugImportCount, equals(1));

      // Release our only reference to the promise import.
      await capReceiver.lastParams.single.dispose();
      expect(conn.debugImportCount, equals(0));
      expect(conn.debugBrokenImportCount, equals(0));

      // A delayed Resolve(exception) for the same (now fully-released)
      // promiseId must be a no-op, not resurrect tracking state for it.
      input.add(
        buildResolveExceptionMessage(promiseId: 10, reason: 'too late'),
      );
      await Future<void>.delayed(Duration.zero);

      expect(conn.debugImportCount, equals(0));
      expect(conn.debugBrokenImportCount, equals(0));

      await input.close();
      await conn.done;
    });

    test('a Call whose params-capTable resolution fails partway through rolls '
        'back the export refcount bump(s) already made, for both a '
        'brand-new export and an already-existing one — instead of leaking '
        'them because the Call itself never reached the wire', () async {
      final clientToServer = StreamController<Uint8List>();
      final serverToClient = StreamController<Uint8List>();
      final captured = <Uint8List>[];
      serverToClient.stream.listen(captured.add);

      final capReceiver = CapReceivingServer();
      final server = TwoPartyRpcConnection.server(
        incoming: clientToServer.stream,
        outgoing: serverToClient.sink,
        bootstrap: capReceiver,
      );

      // One incoming Call hands the server two imports: a plain
      // senderHosted one (id=20, the "target" this test dispatches
      // through — stays healthy throughout) and a senderPromise one
      // (id=10, broken below) — used as a params capability whose
      // resolution fails mid-loop.
      clientToServer.add(
        buildCallMessage(
          questionId: 1,
          targetImportId: 0,
          interfaceId: _echoInterfaceId,
          methodId: _echoMethodId,
          paramsBytes: _buildEchoParams(''),
          capabilityTableReferences: const [
            SenderHostedCapabilityReference(20),
            SenderPromiseCapabilityReference(10),
          ],
        ),
      );
      await Future<void>.delayed(Duration.zero);
      expect(capReceiver.lastParams, hasLength(2));
      final target = capReceiver.lastParams[0]; // import 20
      final brokenParam = capReceiver.lastParams[1]; // import 10

      clientToServer.add(
        buildResolveExceptionMessage(promiseId: 10, reason: 'deliberate'),
      );
      await Future<void>.delayed(Duration.zero);
      expect(server.debugBrokenImportCount, equals(1));

      final baselineExportCount = server.debugExportCount; // bootstrap only

      // --- Case 1: a brand-new export, created then rolled back -----------
      //
      // dispatch()'s params list is processed in order: freshCap (not an
      // import) creates a fresh export first, *then* brokenParam's
      // throwIfBroken throws — exercising
      // CapabilityProtocol.resolveParameterCapabilityReferences's partial-list-then-throw
      // path.
      final freshCap = _TrackedCapability();
      await expectLater(
        target.dispatch(
          _echoInterfaceId,
          _echoMethodId,
          RpcPayload.fromBytes(_buildEchoParams('x')),
          paramsCapabilities: [freshCap, brokenParam],
        ),
        throwsA(isA<RpcException>()),
      );
      // No export leaked: back to just the bootstrap export.
      expect(server.debugExportCount, equals(baselineExportCount));
      // The rollback's refcount decrement reached zero for a
      // never-referenced-elsewhere export, so it disposed freshCap too —
      // same as a real Release would.
      expect(freshCap.disposed, isTrue);

      // --- Case 2: an existing export, refcount rolled back (not removed) -
      //
      // reusedCap is first exported for real via a call that actually
      // completes (so its export legitimately has remoteRefCount == 1),
      // then reused as a param of a second, failing call — the rollback
      // must bring remoteRefCount back to 1, not 0: this reference is
      // still live and must not be disposed as a side effect.
      final reusedCap = _TrackedCapability();
      final successFuture = target.dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('first')),
        paramsCapabilities: [reusedCap],
      );
      final firstCall = await _waitForMessageType(
        captured,
        RpcMessageType.call,
      );
      clientToServer.add(
        buildReturnResultsMessageFromReader(
          answerId: firstCall.questionId,
          resultsRoot:
              MessageReader.deserialize(_buildEchoParams('ok')).getRootRaw(),
          // Simulates a peer that still holds reusedCap's reference after
          // this call — the default (true) would have this vat release it
          // immediately (correctly — see the releaseParamCaps feature this
          // rollback complements), which would defeat this case's whole
          // point of exercising an *existing, still-live* export.
          releaseParamCaps: false,
        ),
      );
      await successFuture;
      final exportCountAfterFirst = server.debugExportCount;
      expect(exportCountAfterFirst, equals(baselineExportCount + 1));

      await expectLater(
        target.dispatch(
          _echoInterfaceId,
          _echoMethodId,
          RpcPayload.fromBytes(_buildEchoParams('y')),
          paramsCapabilities: [reusedCap, brokenParam],
        ),
        throwsA(isA<RpcException>()),
      );
      expect(server.debugExportCount, equals(exportCountAfterFirst));
      expect(reusedCap.disposed, isFalse);

      await clientToServer.close();
      await server.done;
    });

    test('Release batching: a sink failure partway through a flush tears the '
        'connection down cleanly instead of hanging or leaking bookkeeping — '
        'the flush Future still resolves, the pending-release map ends up '
        'empty, and every import is released regardless of which Release '
        'messages actually made it out', () async {
      final input = StreamController<Uint8List>();
      final realOutput = StreamController<Uint8List>();
      final delivered = <Uint8List>[];
      realOutput.stream.listen(delivered.add);
      // Throws once _sendRaw's add() delivers the *second* Release
      // message of the batch — exercising both "a send failure partway
      // through the flush loop" and "the flush must stop trying the
      // remaining entries once torn down" (see _flushPendingReleases).
      final throwingSink = _ThrowOnNthReleaseSink(realOutput.sink, 2);

      final capReceiver = CapReceivingServer();
      final conn = TwoPartyRpcConnection.server(
        incoming: input.stream,
        outgoing: throwingSink,
        bootstrap: capReceiver,
      );

      // One incoming Call hands the server three imports at once, so
      // disposing all three below batches into a single flush.
      input.add(
        buildCallMessage(
          questionId: 1,
          targetImportId: 0,
          interfaceId: _echoInterfaceId,
          methodId: _echoMethodId,
          paramsBytes: _buildEchoParams(''),
          capabilityTableReferences: const [
            SenderHostedCapabilityReference(20),
            SenderHostedCapabilityReference(21),
            SenderHostedCapabilityReference(22),
          ],
        ),
      );
      await Future<void>.delayed(Duration.zero);
      expect(capReceiver.lastParams, hasLength(3));
      expect(conn.debugImportCount, equals(3));

      // Dispose all three without awaiting in between: each dispose()
      // only yields at its own `await _importIdFuture` (already resolved,
      // so a pure microtask hop), and every one of those continuations
      // runs before the flush microtask they schedule — so all three
      // Release sends land in the same _flushPendingReleases call.
      final disposals = Future.wait(
        capReceiver.lastParams.map((cap) => cap.dispose()),
      );

      // Must resolve on its own (never hang, never surface the sink's
      // error) even though the batched flush failed partway through.
      await disposals.timeout(const Duration(seconds: 2));

      expect(conn.debugPendingReleaseCount, equals(0));
      expect(conn.debugImportCount, equals(0));
      expect(conn.debugBrokenImportCount, equals(0));

      // The connection is torn down as a result (matching every other
      // _sendRaw failure) — done completes, carrying the sink's error.
      await expectLater(conn.done, throwsA(isA<StateError>()));

      // Teardown clears every table, not just the import-related ones the
      // assertions above already cover.
      expect(conn.debugExportCount, equals(0));
      expect(conn.debugAnswerCount, equals(0));
      expect(conn.debugCancellationCount, equals(0));
      expect(conn.debugEmbargoCount, equals(0));

      // Exactly one Release actually reached the wire (the first of the
      // batch, before the sink's deliberate failure on the second) — the
      // third was never even attempted, since _closedError was already set
      // by the time the flush loop would have reached it. Proves
      // _flushPendingReleases really does stop trying remaining entries
      // once torn down, rather than merely tolerating one send failure and
      // continuing.
      final releasesDelivered =
          delivered
              .map(parseRpcMessage)
              .where((m) => m.type == RpcMessageType.release)
              .toList();
      expect(releasesDelivered, hasLength(1));
      expect(releasesDelivered.single.releaseId, equals(20));

      await input.close();
    });

    test(
      'incoming Disembargo(senderLoopback) is echoed as receiverLoopback',
      () async {
        final input = StreamController<Uint8List>();
        final output = StreamController<Uint8List>();
        final received = <RpcMessage>[];
        output.stream.listen((bytes) => received.add(parseRpcMessage(bytes)));

        final conn = TwoPartyRpcConnection.server(
          incoming: input.stream,
          outgoing: output.sink,
          bootstrap: EchoServer(),
        );

        input.add(
          buildDisembargoMessage(
            targetImportId: 0,
            contextDisc: 0,
            contextId: 123,
          ),
        );
        await Future<void>.delayed(Duration.zero);

        final disembargo =
            received.where((m) => m.type == RpcMessageType.disembargo).toList();
        expect(disembargo, hasLength(1));
        expect(disembargo.single.disembargoContextDisc, 1);
        expect(disembargo.single.disembargoContextId, 123);
        expect(disembargo.single.disembargoTargetImportId, 0);

        await input.close();
        await conn.done;
      },
    );

    test(
      'promise resolving to local capability waits for receiverLoopback',
      () async {
        final clientToServer = StreamController<Uint8List>();
        final serverToClient = StreamController<Uint8List>();
        final captured = <Uint8List>[];

        final interceptSink =
            StreamController<Uint8List>()
              ..stream.listen((b) {
                captured.add(b);
                clientToServer.add(b);
              });

        clientToServer.stream.listen((_) {});
        final client = TwoPartyRpcConnection.client(
          incoming: serverToClient.stream,
          outgoing: interceptSink.sink,
        );
        final stub = client.bootstrap(EchoClientFactory());

        // Resolve bootstrap to a senderPromise import.
        serverToClient.add(
          buildReturnResultsWithCapabilityReferencesMessage(
            answerId: 0,
            resultsBytes: _buildEchoParams(''),
            references: const [SenderPromiseCapabilityReference(10)],
          ),
        );
        await Future<void>.delayed(Duration.zero);

        captured.clear();
        final local = EchoServer();
        final firstCall = stub.cap.dispatch(
          _echoInterfaceId,
          _echoMethodId,
          RpcPayload.fromBytes(_buildEchoParams('before')),
          paramsCapabilities: [local],
        );
        firstCall.ignore();

        final call = await _waitForMessageType(captured, RpcMessageType.call);
        expect(call.targetImportId, 10);
        expect(call.capabilityTableReferences, hasLength(1));
        expect(
          call.capabilityTableReferences.single,
          isA<SenderHostedCapabilityReference>(),
        ); // local cap exported

        captured.clear();
        serverToClient.add(
          buildResolveCapMessage(
            promiseId: 10,
            reference: const ReceiverHostedCapabilityReference(1),
          ),
        );
        final disembargo = await _waitForMessageType(
          captured,
          RpcMessageType.disembargo,
        );
        expect(disembargo.disembargoContextDisc, 0);
        expect(disembargo.disembargoTargetImportId, 10);
        expect(client.debugEmbargoCount, equals(1));

        final afterCall = stub.echo('after');
        var afterCompleted = false;
        afterCall.then((_) => afterCompleted = true).ignore();
        await Future<void>.delayed(const Duration(milliseconds: 20));
        expect(afterCompleted, isFalse);

        serverToClient.add(
          buildDisembargoMessage(
            targetImportId: 10,
            contextDisc: 1,
            contextId: disembargo.disembargoContextId,
          ),
        );
        expect(await afterCall, 'echo: after');
        expect(client.debugEmbargoCount, equals(0));

        await client.close();
        await interceptSink.close();
      },
    );

    test(
      'missing receiverLoopback fails the waiting call at timeout',
      () async {
        final clientToServer = StreamController<Uint8List>();
        final serverToClient = StreamController<Uint8List>();
        final captured = <Uint8List>[];
        final interceptSink =
            StreamController<Uint8List>()
              ..stream.listen((bytes) {
                captured.add(bytes);
                clientToServer.add(bytes);
              });
        clientToServer.stream.listen((_) {});

        final client = TwoPartyRpcConnection.client(
          incoming: serverToClient.stream,
          outgoing: interceptSink.sink,
          disembargoTimeout: const Duration(milliseconds: 20),
        );
        final stub = client.bootstrap(EchoClientFactory());
        serverToClient.add(
          buildReturnResultsWithCapabilityReferencesMessage(
            answerId: 0,
            resultsBytes: _buildEchoParams(''),
            references: const [SenderPromiseCapabilityReference(10)],
          ),
        );
        await Future<void>.delayed(Duration.zero);

        captured.clear();
        final firstCall = stub.cap.dispatch(
          _echoInterfaceId,
          _echoMethodId,
          RpcPayload.fromBytes(_buildEchoParams('before')),
          paramsCapabilities: [EchoServer()],
        );
        firstCall.ignore();
        await _waitForMessageType(captured, RpcMessageType.call);

        captured.clear();
        serverToClient.add(
          buildResolveCapMessage(
            promiseId: 10,
            reference: const ReceiverHostedCapabilityReference(1),
          ),
        );
        await _waitForMessageType(captured, RpcMessageType.disembargo);
        expect(client.debugEmbargoCount, equals(1));

        await expectLater(
          stub.echo('after'),
          throwsA(
            isA<RpcException>().having(
              (error) => error.kind,
              'kind',
              ErrorKind.overloaded,
            ),
          ),
        );
        expect(client.debugEmbargoCount, equals(0));

        await client.close();
        await serverToClient.close();
        await interceptSink.close();
        await clientToServer.close();
      },
    );

    test('connection torn down while waiting on a Disembargo round trip: the '
        'waiting call fails as a connection error (not the timeout-specific '
        'one), and the embargo table clears', () async {
      final clientToServer = StreamController<Uint8List>();
      final serverToClient = StreamController<Uint8List>();
      final captured = <Uint8List>[];
      final interceptSink =
          StreamController<Uint8List>()
            ..stream.listen((bytes) {
              captured.add(bytes);
              clientToServer.add(bytes);
            });
      clientToServer.stream.listen((_) {});

      final client = TwoPartyRpcConnection.client(
        incoming: serverToClient.stream,
        outgoing: interceptSink.sink,
      );
      final stub = client.bootstrap(EchoClientFactory());
      serverToClient.add(
        buildReturnResultsWithCapabilityReferencesMessage(
          answerId: 0,
          resultsBytes: _buildEchoParams(''),
          references: const [SenderPromiseCapabilityReference(10)],
        ),
      );
      await Future<void>.delayed(Duration.zero);

      captured.clear();
      final firstCall = stub.cap.dispatch(
        _echoInterfaceId,
        _echoMethodId,
        RpcPayload.fromBytes(_buildEchoParams('before')),
        paramsCapabilities: [EchoServer()],
      );
      firstCall.ignore();
      await _waitForMessageType(captured, RpcMessageType.call);

      captured.clear();
      serverToClient.add(
        buildResolveCapMessage(
          promiseId: 10,
          reference: const ReceiverHostedCapabilityReference(1),
        ),
      );
      await _waitForMessageType(captured, RpcMessageType.disembargo);
      expect(client.debugEmbargoCount, equals(1));

      final afterCall = stub.echo('after')..ignore();
      await client.close();

      await expectLater(
        afterCall,
        throwsA(
          isA<RpcException>().having(
            (error) => error.kind,
            'kind',
            ErrorKind.disconnected,
          ),
        ),
      );
      expect(client.debugEmbargoCount, equals(0));
      expect(client.debugPendingQuestionCount, equals(0));

      await serverToClient.close();
      await interceptSink.close();
      await clientToServer.close();
    });
  });

  group('TwoPartyRpcConnection resource-limit validation', () {
    test('negative disembargoTimeout is rejected by both factories', () {
      const incoming = Stream<Uint8List>.empty();
      final outgoing = StreamController<Uint8List>()..stream.listen((_) {});
      addTearDown(outgoing.close);

      expect(
        () => TwoPartyRpcConnection.client(
          incoming: incoming,
          outgoing: outgoing.sink,
          disembargoTimeout: const Duration(microseconds: -1),
        ),
        throwsA(isA<ArgumentError>()),
      );
      expect(
        () => TwoPartyRpcConnection.server(
          incoming: incoming,
          outgoing: outgoing.sink,
          bootstrap: EchoServer(),
          disembargoTimeout: const Duration(microseconds: -1),
        ),
        throwsA(isA<ArgumentError>()),
      );
    });
  });

  group('TwoPartyRpcConnection — in-memory', () {
    test('bootstrap returns a capability', () async {
      final (client, _) = _makePipe(EchoServer());
      final stub = client.bootstrap(EchoClientFactory());
      expect(stub, isA<EchoClient>());
      await client.close();
    });

    test('echo call succeeds', () async {
      final (client, server) = _makePipe(EchoServer());
      final stub = client.bootstrap(EchoClientFactory());
      final reply = await stub.echo('hello');
      expect(reply, 'echo: hello');
      await client.close();
      await server.close();

      // A plain call/reply round-trip must leave both sides' internal
      // tables empty after close — nothing should ever have needed to
      // linger for a call this simple, including the bootstrap
      // export/import entry itself (teardown clears it, not just refcounting).
      for (final conn in [client, server]) {
        expect(conn.debugPendingQuestionCount, equals(0));
        expect(conn.debugExportCount, equals(0));
        expect(conn.debugImportCount, equals(0));
        expect(conn.debugAnswerCount, equals(0));
        expect(conn.debugCancellationCount, equals(0));
        expect(conn.debugEmbargoCount, equals(0));
      }
    });

    test('multiple calls on the same connection', () async {
      final (client, server) = _makePipe(EchoServer());
      final stub = client.bootstrap(EchoClientFactory());
      final replies = await Future.wait([
        stub.echo('a'),
        stub.echo('b'),
        stub.echo('c'),
      ]);
      expect(replies, ['echo: a', 'echo: b', 'echo: c']);
      await client.close();
      await server.close();
    });

    test('repeated calls do not accumulate lifecycle table entries', () async {
      final (client, server) = _makePipe(EchoServer());
      final stub = client.bootstrap(EchoClientFactory());

      for (var i = 0; i < 200; i++) {
        expect(await stub.echo('call-$i'), equals('echo: call-$i'));
      }
      await Future<void>.delayed(const Duration(milliseconds: 20));

      expect(client.debugPendingQuestionCount, equals(0));
      expect(server.debugPendingQuestionCount, equals(0));
      expect(server.debugAnswerCount, equals(0));
      expect(server.debugCancellationCount, equals(0));
      expect(client.debugEmbargoCount, equals(0));

      await client.close();
      await server.close();
      for (final connection in [client, server]) {
        expect(connection.debugExportCount, equals(0));
        expect(connection.debugImportCount, equals(0));
        expect(connection.debugAnswerCount, equals(0));
      }
    });

    test('server dispatches unknown method as exception', () async {
      final (client, server) = _makePipe(EchoServer());
      final stub = client.bootstrap(EchoClientFactory());
      // Warm up so bootstrap completes, then dispatch with wrong methodId.
      await stub.echo('warmup');
      final wrongCall = stub.cap.dispatch(
        _echoInterfaceId,
        99,
        RpcPayload.fromBytes(_buildEchoParams('x')),
      );
      await expectLater(wrongCall, throwsA(isA<RpcException>()));
      await client.close();
      await server.close();
    });

    test('call on closed connection is rejected', () async {
      final (client, server) = _makePipe(EchoServer());
      final stub = client.bootstrap(EchoClientFactory());
      await stub.echo('warmup');
      await client.close();
      await expectLater(stub.echo('after close'), throwsA(isA<RpcException>()));
      await server.close();

      // The rejected post-close call must not have left a phantom pending
      // question behind on the client.
      expect(client.debugPendingQuestionCount, equals(0));
      expect(server.debugPendingQuestionCount, equals(0));
    });

    test("a dispatch's RpcException.kind round-trips to the caller over the "
        'wire (#50)', () async {
      final (client, server) = _makePipe(_KindThrowingCapability());
      final stub = client.bootstrap(EchoClientFactory());
      try {
        await stub.echo('x');
        fail('expected an RpcException');
      } on RpcException catch (e) {
        expect(e.kind, ErrorKind.disconnected);
        expect(e.message, 'peer is gone');
      }
      await client.close();
      await server.close();
    });
  });

  group('TwoPartyRpcConnection — List(Interface) over RPC', () {
    test(
      'server returns List(Interface) in result, client reads and calls each cap',
      () async {
        final server = ListCapsServer();
        final (client, serverConn) = _makePipe(server);
        final bootstrapCap = client.bootstrap(EchoClientFactory());
        await bootstrapCap.echo('warmup');

        final result = await bootstrapCap.cap.dispatch(
          _echoInterfaceId,
          _listCapsResultMethodId,
          DispatchResult.empty.payload,
        );

        final root = result.payload.getTyped(_TextParamFactory());
        final rawList = root.getCapabilityListField(0);
        expect(rawList?.length, 2);

        final cap0 = result.caps[rawList![0]];
        final cap1 = result.caps[rawList[1]];

        final r0 = await cap0.dispatch(
          _echoInterfaceId,
          _echoMethodId,
          RpcPayload.fromBytes(_buildEchoParams('foo')),
        );
        expect(_parseEchoResult(r0.payload), 'echo: foo');

        final r1 = await cap1.dispatch(
          _echoInterfaceId,
          _echoMethodId,
          RpcPayload.fromBytes(_buildEchoParams('bar')),
        );
        expect(_parseEchoResult(r1.payload), 'echo: bar');

        await bootstrapCap.dispose();
        await client.close();
        await serverConn.close();
      },
    );

    test(
      'client sends List(Interface) in params, server reads and calls each cap',
      () async {
        final echoA = EchoServer();
        final echoB = EchoServer();
        final server = ListCapsServer();
        final (client, serverConn) = _makePipe(server);
        final bootstrapCap = client.bootstrap(EchoClientFactory());
        await bootstrapCap.echo('warmup');

        final mb = MessageBuilder();
        final root = mb.initRoot(_TextParamFactory());
        final list = root.initCapabilityListField(0, 2);
        list[0] = 0;
        list[1] = 1;

        final result = await bootstrapCap.cap.dispatch(
          _echoInterfaceId,
          _listCapsParamMethodId,
          RpcPayload.fromBuilder(root),
          paramsCapabilities: [echoA, echoB],
        );

        expect(_parseEchoResult(result.payload), 'echo: a|echo: b');

        await bootstrapCap.dispose();
        await echoA.dispose();
        await echoB.dispose();
        await client.close();
        await serverConn.close();
      },
    );
  });

  // ─── Fix 1: synchronous exception in dispatchWithContext ─────────────────────

  group(
    'TwoPartyRpcConnection — synchronous dispatchWithContext exception',
    () {
      test('sync throw is returned as RPC exception, not leaked', () async {
        final (client, serverConn) = _makePipe(_SyncThrowingCapability());
        final bootstrapCap = client.bootstrap(EchoClientFactory());

        // The call must fail — the synchronous throw must be converted to a
        // Return(exception) rather than crashing the stream listener.
        await expectLater(bootstrapCap.echo('test'), throwsA(anything));

        // Connection close must succeed (stream must not have crashed).
        await client.close();
        await serverConn.close();
      });

      test('connection leases subsequent calls after sync throw', () async {
        final server = _FirstCallSyncThrowCapability();
        final (client, serverConn) = _makePipe(server);
        final bootstrapCap = client.bootstrap(EchoClientFactory());

        // First call: server throws synchronously.
        await expectLater(bootstrapCap.echo('call1'), throwsA(anything));

        // Second call: server echoes normally.
        final result = await bootstrapCap.echo('call2');
        expect(result, 'echo: call2');

        await client.close();
        await serverConn.close();
      });
    },
  );

  // ─── Malformed incoming bytes: _runMessageLoop try/catch ─────────────────────

  group('TwoPartyRpcConnection — malformed incoming message teardown', () {
    // A valid Cap'n Proto frame (1 segment, 1 word) whose root struct pointer
    // references a data section that lies outside the segment bounds.
    // parseRpcMessage() throws DecodeException when processing this frame.
    //
    // Header (8 bytes): numSegments-1=0 (→1 seg), seg0 size=1 word
    // Segment (8 bytes): struct ptr offset=1, dataWords=1, ptrWords=0
    //   → struct data starts at word 2 but segment only has 1 word → out-of-bounds
    final malformedFrame = Uint8List.fromList([
      0x00, 0x00, 0x00, 0x00, // numSegments-1 = 0 → 1 segment
      0x01, 0x00, 0x00, 0x00, // segment 0: 1 word (8 bytes)
      0x04, 0x00, 0x00, 0x00, // struct ptr: kind=0, offset=1
      0x01, 0x00, 0x00, 0x00, // dataWords=1, ptrWords=0
    ]);

    test(
      'malformed frame tears down connection and rejects pending calls',
      () async {
        // Manually wire up a client without a real server.
        final serverToClient = StreamController<Uint8List>();
        final clientToServer =
            StreamController<Uint8List>()..stream.listen((_) {});

        final client = TwoPartyRpcConnection.client(
          incoming: serverToClient.stream,
          outgoing: clientToServer.sink,
        );

        // Kick off an echo call. It awaits bootstrap resolution internally,
        // creating a pending question that tearDown must reject.
        final callFuture = client.bootstrap(EchoClientFactory()).echo('hello');

        // Inject the malformed frame. parseRpcMessage() will throw
        // DecodeException, which the try/catch in _runMessageLoop catches
        // and converts to a _tearDown() call.
        serverToClient.add(malformedFrame);

        // The pending call must fail (tearDown rejected the bootstrap completer).
        await expectLater(callFuture, throwsA(anything));

        // Teardown must leave every internal table empty, not just reject
        // the pending call.
        expect(client.debugPendingQuestionCount, equals(0));
        expect(client.debugExportCount, equals(0));
        expect(client.debugImportCount, equals(0));
        expect(client.debugAnswerCount, equals(0));
        expect(client.debugCancellationCount, equals(0));
        expect(client.debugEmbargoCount, equals(0));

        await serverToClient.close();
        await clientToServer.close();
      },
    );

    test(
      'malformed frame tears down connection cleanly (no pending calls)',
      () async {
        final serverToClient = StreamController<Uint8List>();
        final clientToServer =
            StreamController<Uint8List>()..stream.listen((_) {});

        final client = TwoPartyRpcConnection.client(
          incoming: serverToClient.stream,
          outgoing: clientToServer.sink,
        );

        serverToClient.add(malformedFrame);

        // Give the event loop a chance to process the malformed frame.
        await Future<void>.delayed(const Duration(milliseconds: 20));

        // After teardown, new calls must be rejected immediately.
        // bootstrap() may throw synchronously, so wrap in Future.sync.
        await expectLater(
          Future.sync(
            () => client.bootstrap(EchoClientFactory()).echo('hello'),
          ),
          throwsA(anything),
        );

        expect(client.debugPendingQuestionCount, equals(0));
        expect(client.debugExportCount, equals(0));
        expect(client.debugImportCount, equals(0));
        expect(client.debugAnswerCount, equals(0));
        expect(client.debugCancellationCount, equals(0));
        expect(client.debugEmbargoCount, equals(0));

        await serverToClient.close();
        await clientToServer.close();
      },
    );
  });

  group('TwoPartyRpcConnection — Bootstrap Finish message', () {
    test('client sends Finish after receiving Bootstrap Return', () async {
      final serverToClient = StreamController<Uint8List>();
      final clientToServer = StreamController<Uint8List>();
      final captured = <RpcMessage>[];
      clientToServer.stream.listen(
        (bytes) => captured.add(parseRpcMessage(bytes)),
      );

      final client = TwoPartyRpcConnection.client(
        incoming: serverToClient.stream,
        outgoing: clientToServer.sink,
      );

      // Trigger bootstrap — sends Bootstrap(QID=0).
      final stub = client.bootstrap(EchoClientFactory());

      // Let the Bootstrap message reach the captured list.
      await Future<void>.delayed(Duration.zero);

      // Simulate server sending Bootstrap Return with one senderHosted cap.
      serverToClient.add(buildBootstrapReturnMessage(answerId: 0, exportId: 0));

      // Let the Return be processed and the Finish be sent.
      await Future<void>.delayed(const Duration(milliseconds: 20));

      // Verify Bootstrap was sent first (QID=0).
      expect(
        captured,
        contains(
          predicate<RpcMessage>(
            (m) => m.type == RpcMessageType.bootstrap && m.questionId == 0,
          ),
        ),
      );

      // Verify Finish(QID=0, releaseResultCaps=false) was sent after.
      final finishMsg = captured.firstWhere(
        (m) => m.type == RpcMessageType.finish && m.questionId == 0,
        orElse: () => throw TestFailure('expected Finish(0) but not found'),
      );
      expect(finishMsg.releaseResultCaps, isFalse);

      // Verify Finish appears after Bootstrap in message order.
      final bootstrapIndex = captured.indexWhere(
        (m) => m.type == RpcMessageType.bootstrap && m.questionId == 0,
      );
      final finishIndex = captured.indexWhere(
        (m) => m.type == RpcMessageType.finish && m.questionId == 0,
      );
      expect(finishIndex, greaterThan(bootstrapIndex));

      // The bootstrap cap must be usable after the exchange completes.
      stub.dispose();

      await serverToClient.close();
      await clientToServer.close();
    });

    test(
      'client sends Finish even when Bootstrap Return carries an exception',
      () async {
        final serverToClient = StreamController<Uint8List>();
        final clientToServer = StreamController<Uint8List>();
        final captured = <RpcMessage>[];
        clientToServer.stream.listen(
          (bytes) => captured.add(parseRpcMessage(bytes)),
        );

        final client = TwoPartyRpcConnection.client(
          incoming: serverToClient.stream,
          outgoing: clientToServer.sink,
        );

        final stub = client.bootstrap(EchoClientFactory());

        await Future<void>.delayed(Duration.zero);

        // Simulate server sending a Bootstrap Return exception.
        serverToClient.add(
          buildReturnExceptionMessage(answerId: 0, reason: 'no bootstrap cap'),
        );

        await Future<void>.delayed(const Duration(milliseconds: 20));

        // Finish(QID=0) must still be sent, even for a failed bootstrap.
        final finishMsg = captured.firstWhere(
          (m) => m.type == RpcMessageType.finish && m.questionId == 0,
          orElse: () => throw TestFailure('expected Finish(0) but not found'),
        );
        expect(finishMsg.releaseResultCaps, isFalse);

        // The stub itself should fail.
        await expectLater(stub.echo('hello'), throwsA(isA<RpcException>()));

        await serverToClient.close();
        await clientToServer.close();
      },
    );
  });

  group('TwoPartyRpcConnection — bootstrap export refcount', () {
    // Regression coverage: each Bootstrap request hands the peer a new
    // reference to export 0. If the server-side refcount doesn't reflect
    // that, a peer that bootstraps twice and later releases just one of the
    // two resulting local capabilities would incorrectly drop the server's
    // count to 0 — and since a peer's second, independent Release(id=0,
    // referenceCount=1) would then exceed the (wrongly tracked) outstanding
    // count, CapabilityProtocol.handleRelease's protocol-violation guard
    // tears the whole connection down.
    test('two Bootstrap requests followed by two separate single Releases do '
        'not tear the connection down or drop the export early', () async {
      final clientToServer = StreamController<Uint8List>();
      final serverToClient =
          StreamController<Uint8List>()..stream.listen((_) {});

      final server = TwoPartyRpcConnection.server(
        incoming: clientToServer.stream,
        outgoing: serverToClient.sink,
        bootstrap: EchoServer(),
      );

      // Simulate a peer that calls bootstrap() twice on the same
      // connection — each is a legitimate, independent Bootstrap request.
      clientToServer.add(buildBootstrapMessage(0));
      clientToServer.add(buildBootstrapMessage(1));
      await Future<void>.delayed(Duration.zero);

      expect(server.debugExportCount, equals(1));

      // Peer disposes only ONE of its two local capability objects.
      clientToServer.add(buildReleaseMessage(0, 1));
      await Future<void>.delayed(Duration.zero);

      // The export must still be alive — the peer's other bootstrap
      // reference is still outstanding — and the connection must not have
      // been torn down as a (false) protocol violation.
      expect(server.debugExportCount, equals(1));

      // Peer disposes its second (and last) reference.
      clientToServer.add(buildReleaseMessage(0, 1));
      await Future<void>.delayed(Duration.zero);

      expect(server.debugExportCount, equals(0));

      await clientToServer.close();
      await serverToClient.close();
    });
  });

  group('TwoPartyRpcConnection — unsupported Return variants', () {
    // Regression coverage: a peer sending a Return variant this vat doesn't
    // implement (canceled, resultsSentElsewhere, takeFromOtherQuestion,
    // acceptFromThirdParty) must surface as an explicit error to the waiting
    // caller, not silently resolve as an empty-struct success — this matters
    // most for resultsSentElsewhere, which a peer performing a tail call
    // would send instead of the real result.
    for (final (name, disc) in [
      ('canceled', 2),
      ('resultsSentElsewhere', 3),
      ('takeFromOtherQuestion', 4),
      ('acceptFromThirdParty', 5),
    ]) {
      test('Return.$name is rejected, not treated as empty success', () async {
        final serverToClient = StreamController<Uint8List>();
        final clientToServer = StreamController<Uint8List>();
        final captured = <RpcMessage>[];
        clientToServer.stream.listen(
          (bytes) => captured.add(parseRpcMessage(bytes)),
        );

        final client = TwoPartyRpcConnection.client(
          incoming: serverToClient.stream,
          outgoing: clientToServer.sink,
        );

        final stub = client.bootstrap(EchoClientFactory());
        await Future<void>.delayed(Duration.zero);
        serverToClient.add(
          buildBootstrapReturnMessage(answerId: 0, exportId: 0),
        );
        await Future<void>.delayed(Duration.zero);

        final echoFuture = stub.echo('hello');
        await Future<void>.delayed(Duration.zero);
        final callMsg = captured.firstWhere(
          (m) => m.type == RpcMessageType.call,
          orElse:
              () => throw TestFailure('expected a Call message but none found'),
        );

        serverToClient.add(
          buildRawReturnVariantMessage(
            answerId: callMsg.questionId,
            disc: disc,
          ),
        );

        await expectLater(
          echoFuture,
          throwsA(
            allOf(
              isA<RpcException>(),
              predicate<Object>((e) => e.toString().contains(name)),
            ),
          ),
        );

        await serverToClient.close();
        await clientToServer.close();
      });
    }
  });
}
