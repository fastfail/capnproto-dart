import 'dart:async';
import 'dart:typed_data';

import 'package:capnproto_dart/capnproto_dart.dart';
import 'package:capnproto_dart_rpc/src/capability/capability.dart';
import 'package:capnproto_dart_rpc/src/capability/rpc_payload.dart';
import 'package:capnproto_dart_rpc/src/rpc/calls/answer_table.dart';
import 'package:capnproto_dart_rpc/src/rpc/calls/incoming_call_coordinator.dart';
import 'package:capnproto_dart_rpc/src/rpc/calls/outgoing_call.dart';
import 'package:capnproto_dart_rpc/src/rpc/calls/outgoing_call_coordinator.dart';
import 'package:capnproto_dart_rpc/src/rpc/calls/question_table.dart';
import 'package:capnproto_dart_rpc/src/rpc/capabilities/export_table.dart';
import 'package:capnproto_dart_rpc/src/rpc/capabilities/import_table.dart';
import 'package:capnproto_dart_rpc/src/rpc/capabilities/rpc_capability_reference.dart';
import 'package:capnproto_dart_rpc/src/rpc/capabilities/wire_capability_reference.dart';
import 'package:capnproto_dart_rpc/src/rpc/rpc_exception.dart';
import 'package:capnproto_dart_rpc/src/rpc/rpc_message_codec.dart';
import 'package:test/test.dart';

/// Pre-built 16-byte message: single segment (1 word), null root pointer —
/// same shape as `IncomingCallCoordinator`'s own `_emptyResultBytes`. Used
/// both as [buildCallMessage]'s `paramsBytes` and, for a "resolved answer"
/// fixture, as a result whose root has zero pointer words — so a transform
/// path into it never resolves to a capability (`_tryGetCapabilityFromAnswerPath` returns
/// null).
final _emptyMessageBytes = Uint8List.fromList([
  0, 0, 0, 0, 1, 0, 0, 0, //
  0, 0, 0, 0, 0, 0, 0, 0, //
]);

/// 24-byte message: struct with 0 data words, 1 pointer word =
/// CapabilityPointer(index=0) — same shape as `IncomingCallCoordinator`'s
/// own `_bootstrapResultBytes`. Used as a "resolved answer" fixture whose
/// caps[0] is reachable at transform path [0].
final _singleCapResultBytes = Uint8List.fromList([
  0, 0, 0, 0, 2, 0, 0, 0, // header: 1 segment, 2 words
  0, 0, 0, 0, 0, 0, 1, 0, // struct ptr: offset=0, data=0, ptrs=1
  3, 0, 0, 0, 0, 0, 0, 0, // ptr[0] = CapabilityPointer(index=0)
]);

typedef _StartUsingCall =
    ({
      OutgoingCallTarget target,
      List<Capability> paramsCapabilities,
      bool sendResultsToYourself,
    });

class _FakeCapability extends Capability {
  var disposeCount = 0;
  final dispatches = <(int, int)>[];

  /// Settable per test. Defaults to an empty, no-capabilities success.
  DispatchResult Function(int interfaceId, int methodId, RpcPayload params)?
  onDispatch;

  /// Settable per test — thrown from [dispatch] instead of running
  /// [onDispatch], if set.
  Object? throwOnDispatch;

  /// Settable per test — defaults to never tail-calling, matching
  /// [Capability]'s own default.
  TailCallRequest? Function(int interfaceId, int methodId, RpcPayload params)?
  onTryTailCall;

  /// Settable per test — if non-null, [dispatch] awaits this before
  /// returning [onDispatch]'s result, so a test can observe state exactly
  /// while this capability's own dispatch is still in flight (started, but
  /// not yet finished).
  Future<void>? dispatchGate;

  @override
  Future<DispatchResult> dispatch(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) async {
    dispatches.add((interfaceId, methodId));
    final failure = throwOnDispatch;
    if (failure != null) throw failure;
    final gate = dispatchGate;
    if (gate != null) await gate;
    return onDispatch?.call(interfaceId, methodId, params) ??
        DispatchResult.empty;
  }

  @override
  TailCallRequest? tryTailCall(
    int interfaceId,
    int methodId,
    RpcPayload params, {
    List<Capability> paramsCapabilities = const [],
  }) => onTryTailCall?.call(interfaceId, methodId, params);

  @override
  Future<void> dispose() async {
    disposeCount++;
  }
}

/// Builds an [IncomingCallCoordinator] with fake, observable dependencies —
/// no `TwoPartyRpcConnection`/sockets — proving the class extracted in
/// Stage 5 is genuinely testable standalone. Mirrors
/// `capability_protocol_test.dart`/`outgoing_call_coordinator_test.dart`'s
/// harness pattern: real lightweight tables, fake closures for everything
/// that would otherwise need a real connection.
class _Harness {
  final exportTable = ExportTable();
  final answerTable = AnswerTable();
  final questions = QuestionTable();
  final sentBytes = <Uint8List>[];
  final disposedFromTable = <Object>[];
  final tearDownCalls = <RpcException>[];
  final startUsingCalls = <_StartUsingCall>[];

  /// Settable per test — defaults to just recording the bytes in
  /// [sentBytes]. Tests that need to exercise reentrancy (a peer reacting
  /// to a Return through a synchronously-reentrant sink) replace this to
  /// call back into the coordinator from inside the send itself — the
  /// replacement is responsible for still recording into [sentBytes] if
  /// the test needs both.
  late void Function(Uint8List bytes) sendBytes = sentBytes.add;

  /// Settable per test — defaults to "connection never closes".
  bool Function() isClosed = () => false;

  /// Settable per test — defaults to only recording the teardown request.
  /// Tests that need the real teardown ordering can release the tables here.
  void Function(RpcException error) onTearDownConnection = (_) {};

  /// Settable per test — defaults to no reusable RPC capability reference,
  /// matching a real connection's answer for any capability it did not
  /// construct itself.
  RpcCapabilityReference? Function(Capability cap)
  tryExtractCapabilityReference = (cap) => null;

  /// Settable per test — defaults to a fresh [_FakeCapability] per
  /// reference.
  Capability Function(WireCapabilityReference reference)
  acquireCapabilityFromWireReference = (reference) => _FakeCapability();

  /// Settable per test — defaults to a `none` reference for every result
  /// capability.
  WireCapabilityReference Function(Capability cap)
  exportResultCapabilityAsWireReference =
      (cap) => const NoCapabilityReference();

  /// Settable per test — defaults to completing [OutgoingQuestion.sentCompleter]
  /// immediately (mirrors `OutgoingCallCoordinator.startCallWithAllocatedQuestion`'s synchronous
  /// send fast path), recording the call in [startUsingCalls].
  void Function(OutgoingQuestion question, _StartUsingCall call) onStartUsing =
      (question, call) => question.sentCompleter?.complete();

  /// Settable per test — defaults to "nothing to track".
  Object? Function(List<Capability> paramsCapabilities)
  startParameterCapabilityDisposalTracking = (paramsCapabilities) => null;

  /// Settable per test — defaults to "always safe to fold into
  /// releaseParamCaps=true", matching a null/empty ticket.
  ({bool allDisposed, List<int> explicitReleaseIds}) Function(Object? ticket)
  finishParameterCapabilityDisposalTracking =
      (ticket) => (allDisposed: true, explicitReleaseIds: const []);

  late final coordinator = IncomingCallCoordinator(
    exportTable: exportTable,
    answerTable: answerTable,
    questions: questions,
    sendBytes: (bytes) => sendBytes(bytes),
    disposeIgnoringErrors: disposedFromTable.add,
    disposePipelinedTargetIgnoringErrors: (lease) => lease.dispose(),
    disposeLeaseForConnectionTeardown:
        (lease) => lease.disposeForConnectionTeardown(),
    isClosed: () => isClosed(),
    tearDownConnection: (error) {
      tearDownCalls.add(error);
      onTearDownConnection(error);
    },
    tryExtractCapabilityReference: (cap) => tryExtractCapabilityReference(cap),
    acquireCapabilityFromWireReference:
        (reference) => acquireCapabilityFromWireReference(reference),
    exportResultCapabilityAsWireReference:
        (cap) => exportResultCapabilityAsWireReference(cap),
    startCallWithAllocatedQuestion: ({
      required OutgoingQuestion question,
      required OutgoingCallTarget target,
      required OutgoingParams params,
      required int interfaceId,
      required int methodId,
      required List<Capability> paramsCapabilities,
      bool sendResultsToYourself = false,
    }) {
      final call = (
        target: target,
        paramsCapabilities: paramsCapabilities,
        sendResultsToYourself: sendResultsToYourself,
      );
      startUsingCalls.add(call);
      onStartUsing(question, call);
    },
    startParameterCapabilityDisposalTracking:
        (paramsCapabilities) =>
            startParameterCapabilityDisposalTracking(paramsCapabilities),
    finishParameterCapabilityDisposalTracking:
        (ticket) => finishParameterCapabilityDisposalTracking(ticket),
  );
}

/// A Call message targeting an export id directly (not a promisedAnswer),
/// with no params capabilities unless [capabilityTableReferences] is given.
Uint8List _buildCall({
  required int questionId,
  required int targetExportId,
  int interfaceId = 1,
  int methodId = 2,
  bool sendResultsToYourself = false,
  List<WireCapabilityReference>? capabilityTableReferences,
}) => buildCallMessage(
  questionId: questionId,
  targetImportId: targetExportId,
  interfaceId: interfaceId,
  methodId: methodId,
  paramsBytes: _emptyMessageBytes,
  capabilityTableReferences: capabilityTableReferences ?? const [],
  sendResultsToYourself: sendResultsToYourself,
);

Uint8List _buildPipelinedCall({
  required int questionId,
  required int parentQid,
  List<int> transformPath = const [0],
  int interfaceId = 1,
  int methodId = 2,
  List<WireCapabilityReference>? capabilityTableReferences,
}) => buildCallMessage(
  questionId: questionId,
  targetPromisedAnswerQid: parentQid,
  targetTransformPath: transformPath,
  interfaceId: interfaceId,
  methodId: methodId,
  paramsBytes: _emptyMessageBytes,
  capabilityTableReferences: capabilityTableReferences ?? const [],
);

void main() {
  group('IncomingCallCoordinator', () {
    test('handleBootstrap sends a Return carrying export id 0 and registers '
        'a resolved answer, so a pipelined call targeting it resolves '
        'immediately', () {
      final h = _Harness();
      final bootstrapCap =
          _FakeCapability()..onDispatch = (_, _, _) => DispatchResult.empty;
      h.exportTable.registerBootstrap(bootstrapCap);

      h.coordinator.handleBootstrap(parseRpcMessage(buildBootstrapMessage(7)));

      expect(h.sentBytes, hasLength(1));
      final sent = parseRpcMessage(h.sentBytes.single);
      expect(sent.type, equals(RpcMessageType.return_));
      expect(
        sent.capabilityTableReferences.single,
        isA<SenderHostedCapabilityReference>().having(
          (r) => r.exportId,
          'exportId',
          equals(0),
        ),
      );
      expect(h.answerTable.isTracked(7), isTrue);

      // A pipelined call targeting {receiverAnswer: {questionId: 7, path: []}}
      // (empty transform, normalized to [0]) resolves against the bootstrap
      // capability immediately, with no queuing.
      h.coordinator.handleCall(
        parseRpcMessage(
          _buildPipelinedCall(
            questionId: 100,
            parentQid: 7,
            transformPath: const [],
          ),
        ),
      );
      expect(bootstrapCap.dispatches, hasLength(1));
    });

    test('handleCall with an already-resolved promisedAnswer parent '
        'dispatches immediately against the resolved path, and reports a '
        'protocol-level error for a path that is not a capability', () {
      final h = _Harness();
      final targetCap =
          _FakeCapability()..onDispatch = (_, _, _) => DispatchResult.empty;
      h.answerTable.handleAnswerReadyWithoutDispatch(
        1,
        resolved: ResolvedAnswer(_singleCapResultBytes, [targetCap]),
      );

      h.coordinator.handleCall(
        parseRpcMessage(_buildPipelinedCall(questionId: 200, parentQid: 1)),
      );
      expect(targetCap.dispatches, hasLength(1));

      // A second, already-resolved parent whose result has no pointer
      // fields at all: path [0] can never be a capability there.
      h.answerTable.handleAnswerReadyWithoutDispatch(
        2,
        resolved: ResolvedAnswer(_emptyMessageBytes, const []),
      );
      h.coordinator.handleCall(
        parseRpcMessage(_buildPipelinedCall(questionId: 201, parentQid: 2)),
      );
      final sent = parseRpcMessage(h.sentBytes.single);
      expect(sent.isReturnException, isTrue);
      expect(sent.answerId, equals(201));
      expect(sent.exceptionReason, contains('is not a capability'));
    });

    test('duplicate pipelined question id releases its target lease with '
        'teardown semantics after export teardown', () async {
      final h = _Harness();
      final resolution = Completer<Capability>();
      final deferred = DeferredCapability(resolution.future);

      h.exportTable.retainOrCreateExportId(deferred);
      h.answerTable.handleAnswerReadyWithoutDispatch(
        1,
        resolved: ResolvedAnswer(_singleCapResultBytes, [deferred]),
      );
      h.answerTable.handleAnswerReadyWithoutDispatch(200);
      h.onTearDownConnection = (error) {
        h.answerTable.tearDown(error);
        h.exportTable.tearDown((lease) => lease.disposeForConnectionTeardown());
      };

      h.coordinator.handleCall(
        parseRpcMessage(_buildPipelinedCall(questionId: 200, parentQid: 1)),
      );

      expect(h.tearDownCalls, hasLength(1));
      await deferred.dispose().timeout(const Duration(seconds: 1));

      final resolved = _FakeCapability();
      resolution.complete(resolved);
      await Future<void>.delayed(Duration.zero);
      expect(resolved.disposeCount, equals(1));
    });

    test('handleCall with a still-pending promisedAnswer parent queues and '
        'dispatches once the parent resolves, or reports an exception if '
        'the parent fails', () async {
      final h = _Harness();
      final targetCap =
          _FakeCapability()..onDispatch = (_, _, _) => DispatchResult.empty;
      final pending = Completer<ResolvedAnswer>();
      h.answerTable.handleDispatchStarted(
        1,
        pending.future,
        DispatchCancellationController(),
      );

      h.coordinator.handleCall(
        parseRpcMessage(_buildPipelinedCall(questionId: 200, parentQid: 1)),
      );
      expect(targetCap.dispatches, isEmpty);

      pending.complete(ResolvedAnswer(_singleCapResultBytes, [targetCap]));
      await Future<void>.delayed(Duration.zero);
      expect(targetCap.dispatches, hasLength(1));

      final failingParent = Completer<ResolvedAnswer>();
      h.answerTable.handleDispatchStarted(
        2,
        failingParent.future,
        DispatchCancellationController(),
      );
      h.coordinator.handleCall(
        parseRpcMessage(_buildPipelinedCall(questionId: 201, parentQid: 2)),
      );
      failingParent.completeError(const RpcException('parent broke'));
      await Future<void>.delayed(Duration.zero);

      final sent = parseRpcMessage(h.sentBytes.last);
      expect(sent.isReturnException, isTrue);
      expect(sent.answerId, equals(201));
      expect(sent.exceptionReason, contains('parent call failed'));
    });

    test('a tail call to a same-connection import forwards a Call flagged '
        'sendResultsToYourself, and answers the original call with '
        'takeFromOtherQuestion only after the forward is sent', () async {
      final h = _Harness();
      final forwardTarget = _FakeCapability();
      final originalCap =
          _FakeCapability()
            ..onTryTailCall =
                (interfaceId, methodId, params) => TailCallRequest(
                  forwardTarget,
                  interfaceId,
                  methodId,
                  params,
                );
      // Cached plain int, matching the realistic path (an _ImportedCapability
      // constructed via .fromState sets _cachedState synchronously).
      h.tryExtractCapabilityReference =
          (cap) =>
              identical(cap, forwardTarget)
                  ? const ImportedCapabilityReference(9)
                  : null;
      final exportId = h.exportTable.retainOrCreateExportId(originalCap);

      // Holds the forward "on the wire" open until the test explicitly lets
      // it through — proves the redirect actually waits for it, rather than
      // just happening to run after it because the default fake completes
      // sentCompleter synchronously either way.
      final sentGate = Completer<void>();
      h.onStartUsing = (question, call) {
        sentGate.future.then((_) => question.sentCompleter?.complete());
      };

      h.coordinator.handleCall(
        parseRpcMessage(_buildCall(questionId: 50, targetExportId: exportId)),
      );
      await Future<void>.delayed(Duration.zero);

      expect(h.startUsingCalls, hasLength(1));
      expect(h.startUsingCalls.single.sendResultsToYourself, isTrue);
      expect(
        (h.startUsingCalls.single.target as ImportedCapabilityTarget).importId,
        equals(9),
      );
      // Nothing sent yet — the forward hasn't reached the wire.
      expect(h.sentBytes, isEmpty);

      sentGate.complete();
      await Future<void>.delayed(Duration.zero);

      final sent = parseRpcMessage(h.sentBytes.single);
      expect(sent.isReturnTakeFromOtherQuestion, isTrue);
      expect(sent.answerId, equals(50));
      // The forwarded call got its own, different question id.
      expect(sent.takeFromOtherQuestion, isNot(equals(50)));
      expect(originalCap.dispatches, isEmpty);
      expect(forwardTarget.dispatches, isEmpty);
    });

    test('a tail call to a target that is not a same-connection import '
        'falls back to a transparent proxy dispatch, never forwarding a '
        'Call', () async {
      final h = _Harness();
      final forwardTarget =
          _FakeCapability()..onDispatch = (_, _, _) => DispatchResult.empty;
      final originalCap =
          _FakeCapability()
            ..onTryTailCall =
                (interfaceId, methodId, params) => TailCallRequest(
                  forwardTarget,
                  interfaceId,
                  methodId,
                  params,
                );
      // tryExtractCapabilityReference defaults to `null`.
      final exportId = h.exportTable.retainOrCreateExportId(originalCap);

      h.coordinator.handleCall(
        parseRpcMessage(_buildCall(questionId: 51, targetExportId: exportId)),
      );
      await Future<void>.delayed(Duration.zero);

      expect(h.startUsingCalls, isEmpty);
      expect(originalCap.dispatches, isEmpty);
      expect(forwardTarget.dispatches, hasLength(1));
      final sent = parseRpcMessage(h.sentBytes.single);
      expect(sent.isReturnResults, isTrue);
      expect(sent.answerId, equals(51));
    });

    test('a successful dispatch with no result capabilities sends '
        'Return.results with noFinishNeeded=true and drops the answer '
        'bookkeeping immediately', () async {
      final h = _Harness();
      final cap =
          _FakeCapability()..onDispatch = (_, _, _) => DispatchResult.empty;
      final exportId = h.exportTable.retainOrCreateExportId(cap);

      h.coordinator.handleCall(
        parseRpcMessage(_buildCall(questionId: 60, targetExportId: exportId)),
      );
      await Future<void>.delayed(Duration.zero);

      final sent = parseRpcMessage(h.sentBytes.single);
      expect(sent.isReturnResults, isTrue);
      expect(sent.returnNoFinishNeeded, isTrue);
      expect(h.answerTable.isTracked(60), isFalse);
    });

    test('noFinishNeeded cleanup tolerates a synchronously-reentrant peer '
        'Finish that consumes the answer state during send', () async {
      final h = _Harness();
      final cap =
          _FakeCapability()..onDispatch = (_, _, _) => DispatchResult.empty;
      final exportId = h.exportTable.retainOrCreateExportId(cap);
      var sentReentrantFinish = false;
      h.sendBytes = (bytes) {
        h.sentBytes.add(bytes);
        final msg = parseRpcMessage(bytes);
        if (msg.isReturnResults && msg.returnNoFinishNeeded) {
          sentReentrantFinish = true;
          h.coordinator.handleFinish(
            parseRpcMessage(buildFinishMessage(msg.answerId)),
          );
        }
      };

      h.coordinator.handleCall(
        parseRpcMessage(_buildCall(questionId: 62, targetExportId: exportId)),
      );
      await Future<void>.delayed(Duration.zero);

      expect(sentReentrantFinish, isTrue);
      expect(h.answerTable.isTracked(62), isFalse);
      expect(h.sentBytes, hasLength(1));
    });

    test('a failed dispatch sends Return.exception with the thrown reason/ '
        'kind and noFinishNeeded=true', () async {
      final h = _Harness();
      final cap =
          _FakeCapability()
            ..throwOnDispatch = const RpcException(
              'boom',
              kind: ErrorKind.overloaded,
            );
      final exportId = h.exportTable.retainOrCreateExportId(cap);

      h.coordinator.handleCall(
        parseRpcMessage(_buildCall(questionId: 61, targetExportId: exportId)),
      );
      await Future<void>.delayed(Duration.zero);

      final sent = parseRpcMessage(h.sentBytes.single);
      expect(sent.isReturnException, isTrue);
      expect(sent.exceptionReason, equals('boom'));
      expect(sent.exceptionKind, equals(ErrorKind.overloaded));
      expect(sent.returnNoFinishNeeded, isTrue);
    });

    test('sendResultsToYourself=true sends Return.resultsSentElsewhere on '
        'both success and failure, never a normal results/exception Return, '
        'and never consults tryTailCall', () async {
      final h = _Harness();
      final succeedingCap = _FakeCapability();
      succeedingCap.onDispatch = (_, _, _) => DispatchResult.empty;
      succeedingCap.onTryTailCall =
          (_, _, _) => throw StateError('must not be called');
      final exportId1 = h.exportTable.retainOrCreateExportId(succeedingCap);
      h.coordinator.handleCall(
        parseRpcMessage(
          _buildCall(
            questionId: 70,
            targetExportId: exportId1,
            sendResultsToYourself: true,
          ),
        ),
      );
      await Future<void>.delayed(Duration.zero);
      final sent1 = parseRpcMessage(h.sentBytes.single);
      expect(sent1.returnDisc, equals(3)); // resultsSentElsewhere
      expect(sent1.isReturnResults, isFalse);

      final failingCap =
          _FakeCapability()..throwOnDispatch = const RpcException('boom');
      final exportId2 = h.exportTable.retainOrCreateExportId(failingCap);
      h.coordinator.handleCall(
        parseRpcMessage(
          _buildCall(
            questionId: 71,
            targetExportId: exportId2,
            sendResultsToYourself: true,
          ),
        ),
      );
      await Future<void>.delayed(Duration.zero);
      final sent2 = parseRpcMessage(h.sentBytes.last);
      expect(sent2.returnDisc, equals(3)); // resultsSentElsewhere
      expect(sent2.isReturnException, isFalse);
    });

    test('params-caps deferred release: an all-disposed ticket folds into '
        'releaseParamCaps=true with no explicit Release, a partial one sends '
        'explicit Releases and releaseParamCaps=false; a duplicate question '
        'id tears the connection down as a protocol violation', () async {
      final h = _Harness();
      h.startParameterCapabilityDisposalTracking =
          (paramsCapabilities) => 'ticket';

      h.finishParameterCapabilityDisposalTracking =
          (ticket) => (allDisposed: true, explicitReleaseIds: const []);
      final cap1 =
          _FakeCapability()..onDispatch = (_, _, _) => DispatchResult.empty;
      final exportId1 = h.exportTable.retainOrCreateExportId(cap1);
      h.coordinator.handleCall(
        parseRpcMessage(_buildCall(questionId: 80, targetExportId: exportId1)),
      );
      await Future<void>.delayed(Duration.zero);
      var sent = parseRpcMessage(h.sentBytes.single);
      expect(sent.returnReleaseParamCaps, isTrue);

      h.finishParameterCapabilityDisposalTracking =
          (ticket) => (allDisposed: false, explicitReleaseIds: const [7, 8]);
      final cap2 =
          _FakeCapability()..onDispatch = (_, _, _) => DispatchResult.empty;
      final exportId2 = h.exportTable.retainOrCreateExportId(cap2);
      h.coordinator.handleCall(
        parseRpcMessage(_buildCall(questionId: 81, targetExportId: exportId2)),
      );
      await Future<void>.delayed(Duration.zero);
      sent = parseRpcMessage(h.sentBytes.last);
      expect(sent.returnReleaseParamCaps, isFalse);
      final releaseMessages =
          h.sentBytes
              .map(parseRpcMessage)
              .where((m) => m.type == RpcMessageType.release)
              .toList();
      expect(releaseMessages.map((m) => m.releaseId), unorderedEquals([7, 8]));

      // The case the (bool, List<int>) split exists for: zero disposed out
      // of N is NOT the same as N-of-N disposed, even though both report an
      // empty explicitReleaseIds list — only the latter is safe to fold
      // into releaseParamCaps=true.
      h.sentBytes.clear();
      h.finishParameterCapabilityDisposalTracking =
          (ticket) => (allDisposed: false, explicitReleaseIds: const []);
      final cap3 =
          _FakeCapability()..onDispatch = (_, _, _) => DispatchResult.empty;
      final exportId3 = h.exportTable.retainOrCreateExportId(cap3);
      h.coordinator.handleCall(
        parseRpcMessage(_buildCall(questionId: 82, targetExportId: exportId3)),
      );
      await Future<void>.delayed(Duration.zero);
      sent = parseRpcMessage(h.sentBytes.single);
      expect(sent.returnReleaseParamCaps, isFalse);
      expect(
        h.sentBytes
            .map(parseRpcMessage)
            .where((m) => m.type == RpcMessageType.release),
        isEmpty,
      );

      // Duplicate question id: handleCall is called twice with the same
      // questionId, the second reusing still-tracked answer state.
      final dupCap =
          _FakeCapability()..onDispatch = (_, _, _) => DispatchResult.empty;
      final dupExportId = h.exportTable.retainOrCreateExportId(dupCap);
      h.coordinator.handleCall(
        parseRpcMessage(
          _buildCall(questionId: 999, targetExportId: dupExportId),
        ),
      );
      h.coordinator.handleCall(
        parseRpcMessage(
          _buildCall(questionId: 999, targetExportId: dupExportId),
        ),
      );
      expect(h.tearDownCalls, hasLength(1));
    });

    test('a pending promised-answer resolving after teardown does not '
        'decode capabilities or dispatch its target', () async {
      final h = _Harness();
      final target =
          _FakeCapability()..onDispatch = (_, _, _) => DispatchResult.empty;
      final pending = Completer<ResolvedAnswer>();
      h.answerTable.handleDispatchStarted(
        1,
        pending.future,
        DispatchCancellationController(),
      );

      var descriptorDecodeCount = 0;
      h.acquireCapabilityFromWireReference = (reference) {
        descriptorDecodeCount++;
        return _FakeCapability();
      };

      h.coordinator.handleCall(
        parseRpcMessage(
          _buildPipelinedCall(
            questionId: 2,
            parentQid: 1,
            capabilityTableReferences: const [
              SenderHostedCapabilityReference(7),
            ],
          ),
        ),
      );

      h.isClosed = () => true;
      h.answerTable.tearDown(const RpcException('connection torn down'));

      pending.complete(ResolvedAnswer(_singleCapResultBytes, [target]));
      await Future<void>.delayed(Duration.zero);

      expect(target.dispatches, isEmpty);
      expect(descriptorDecodeCount, equals(0));
      expect(h.answerTable.count, equals(0));
      expect(h.sentBytes, isEmpty);

      // Companion case: the parent dispatch fails instead of succeeding —
      // the catchError continuation must equally refuse to send once closed.
      final failingPending = Completer<ResolvedAnswer>();
      h.answerTable.handleDispatchStarted(
        3,
        failingPending.future,
        DispatchCancellationController(),
      );
      h.coordinator.handleCall(
        parseRpcMessage(_buildPipelinedCall(questionId: 4, parentQid: 3)),
      );
      failingPending.completeError(const RpcException('parent broke'));
      await Future<void>.delayed(Duration.zero);

      expect(h.sentBytes, isEmpty);
    });

    test('handleBootstrap records answer state before sending its Return, '
        'so a Finish arriving synchronously as a reaction to that Return is '
        'not silently dropped', () {
      final h = _Harness();
      final bootstrapCap =
          _FakeCapability()..onDispatch = (_, _, _) => DispatchResult.empty;
      h.exportTable.registerBootstrap(bootstrapCap);

      h.sendBytes = (bytes) {
        h.sentBytes.add(bytes);
        final msg = parseRpcMessage(bytes);
        if (msg.type == RpcMessageType.return_) {
          // Simulates a peer reacting to the Return through a
          // synchronously-reentrant sink (an in-memory or `sync: true`
          // transport) by immediately sending Finish for it.
          h.coordinator.handleFinish(
            parseRpcMessage(buildFinishMessage(msg.answerId)),
          );
        }
      };

      h.coordinator.handleBootstrap(parseRpcMessage(buildBootstrapMessage(7)));

      // If the Return had been sent before the answer state existed, this
      // reentrant Finish would have found nothing to finish, and the
      // bootstrap answer would be stuck tracked forever.
      expect(h.answerTable.isTracked(7), isFalse);
    });

    test('a tail-call redirect records answer state before sending its '
        'takeFromOtherQuestion Return, so a Finish arriving synchronously '
        'as a reaction to it is not silently dropped', () async {
      final h = _Harness();
      final forwardTarget = _FakeCapability();
      final originalCap =
          _FakeCapability()
            ..onTryTailCall =
                (interfaceId, methodId, params) => TailCallRequest(
                  forwardTarget,
                  interfaceId,
                  methodId,
                  params,
                );
      h.tryExtractCapabilityReference =
          (cap) =>
              identical(cap, forwardTarget)
                  ? const ImportedCapabilityReference(9)
                  : null;
      final exportId = h.exportTable.retainOrCreateExportId(originalCap);

      h.sendBytes = (bytes) {
        h.sentBytes.add(bytes);
        final msg = parseRpcMessage(bytes);
        if (msg.isReturnTakeFromOtherQuestion) {
          h.coordinator.handleFinish(
            parseRpcMessage(buildFinishMessage(msg.answerId)),
          );
        }
      };

      h.coordinator.handleCall(
        parseRpcMessage(_buildCall(questionId: 50, targetExportId: exportId)),
      );
      await Future<void>.delayed(Duration.zero);

      expect(h.answerTable.isTracked(50), isFalse);
    });

    group('early Finish while a pipelined call still depends on it '
        '(issue #109)', () {
      test('a pipelined call queued behind a pending parent when Finish '
          'arrives for the parent: the parent still completes normally '
          'with Return.results (noFinishNeeded: false, never '
          'Return.canceled, since the result carries a live capability the '
          'dependent needs), the dependent still dispatches successfully '
          'once it resolves, and the parent export is released once the '
          'dependent has drained', () async {
        final h = _Harness();
        final targetCap =
            _FakeCapability()..onDispatch = (_, _, _) => DispatchResult.empty;
        final resultExportId = h.exportTable.retainOrCreateExportId(targetCap);
        h.exportResultCapabilityAsWireReference =
            (cap) => SenderHostedCapabilityReference(resultExportId);
        final parentCap =
            _FakeCapability()
              ..onDispatch =
                  (_, _, _) => DispatchResult(
                    payload: RpcPayload.fromBytes(_singleCapResultBytes),
                    caps: [targetCap],
                  );
        final parentExportId = h.exportTable.retainOrCreateExportId(parentCap);

        h.coordinator.handleCall(
          parseRpcMessage(
            _buildCall(questionId: 1, targetExportId: parentExportId),
          ),
        );
        h.coordinator.handleCall(
          parseRpcMessage(_buildPipelinedCall(questionId: 2, parentQid: 1)),
        );
        h.coordinator.handleFinish(parseRpcMessage(buildFinishMessage(1)));

        expect(targetCap.dispatches, isEmpty);
        expect(h.sentBytes, isEmpty);

        await Future<void>.delayed(Duration.zero);

        expect(targetCap.dispatches, hasLength(1));
        final parentReturn = h.sentBytes
            .map(parseRpcMessage)
            .firstWhere((m) => m.answerId == 1);
        expect(parentReturn.isReturnResults, isTrue);
        expect(parentReturn.returnNoFinishNeeded, isFalse);
        final dependentReturn = h.sentBytes
            .map(parseRpcMessage)
            .firstWhere((m) => m.answerId == 2);
        expect(dependentReturn.isReturnResults, isTrue);
        expect(
          h.disposedFromTable,
          hasLength(1),
          reason: 'the parent export is released exactly once',
        );
      });

      test("a pipelined dependent's parent export is released once the "
          'dependent has started its own dispatch, without waiting for '
          'that dispatch to finish — proves export release depends on the '
          'dependent having started, not completed', () async {
        final h = _Harness();
        final targetCap = _FakeCapability();
        final resultExportId = h.exportTable.retainOrCreateExportId(targetCap);
        h.exportResultCapabilityAsWireReference =
            (cap) => SenderHostedCapabilityReference(resultExportId);
        final dispatchGate = Completer<void>();
        targetCap.dispatchGate = dispatchGate.future;
        targetCap.onDispatch = (_, _, _) => DispatchResult.empty;

        final parentCap =
            _FakeCapability()
              ..onDispatch =
                  (_, _, _) => DispatchResult(
                    payload: RpcPayload.fromBytes(_singleCapResultBytes),
                    caps: [targetCap],
                  );
        final parentExportId = h.exportTable.retainOrCreateExportId(parentCap);

        h.coordinator.handleCall(
          parseRpcMessage(
            _buildCall(questionId: 1, targetExportId: parentExportId),
          ),
        );
        h.coordinator.handleCall(
          parseRpcMessage(_buildPipelinedCall(questionId: 2, parentQid: 1)),
        );
        h.coordinator.handleFinish(parseRpcMessage(buildFinishMessage(1)));

        await Future<void>.delayed(Duration.zero);

        // The dependent has started its own dispatch (recorded) but not
        // finished it (still gated open) — the parent's export must
        // already be released.
        expect(targetCap.dispatches, hasLength(1));
        expect(h.disposedFromTable, hasLength(1));
        expect(
          h.sentBytes.map(parseRpcMessage).where((m) => m.answerId == 2),
          isEmpty,
          reason: "the dependent's own Return hasn't gone out yet",
        );

        dispatchGate.complete();
        await Future<void>.delayed(Duration.zero);

        final dependentReturn = h.sentBytes
            .map(parseRpcMessage)
            .firstWhere((m) => m.answerId == 2);
        expect(dependentReturn.isReturnResults, isTrue);
      });

      test('releaseResultCaps=false on the early Finish: the pipelined '
          'dependent still dispatches normally, but the parent export is '
          'never auto-released', () async {
        final h = _Harness();
        final targetCap =
            _FakeCapability()..onDispatch = (_, _, _) => DispatchResult.empty;
        final resultExportId = h.exportTable.retainOrCreateExportId(targetCap);
        h.exportResultCapabilityAsWireReference =
            (cap) => SenderHostedCapabilityReference(resultExportId);
        final parentCap =
            _FakeCapability()
              ..onDispatch =
                  (_, _, _) => DispatchResult(
                    payload: RpcPayload.fromBytes(_singleCapResultBytes),
                    caps: [targetCap],
                  );
        final parentExportId = h.exportTable.retainOrCreateExportId(parentCap);

        h.coordinator.handleCall(
          parseRpcMessage(
            _buildCall(questionId: 1, targetExportId: parentExportId),
          ),
        );
        h.coordinator.handleCall(
          parseRpcMessage(_buildPipelinedCall(questionId: 2, parentQid: 1)),
        );
        h.coordinator.handleFinish(
          parseRpcMessage(buildFinishMessage(1, releaseResultCaps: false)),
        );

        await Future<void>.delayed(Duration.zero);

        expect(targetCap.dispatches, hasLength(1));
        expect(h.disposedFromTable, isEmpty);
      });

      test(
        'a pending parent Finished with no pipelined dependents: '
        'nothing is sent synchronously from handleFinish; once the '
        'dispatch settles, Return.canceled is sent with the '
        'correctly-resolved releaseParamCaps (covering all-disposed / '
        'partial / none-disposed parameter capability combinations)',
        () async {
          Future<void> run(
            ({bool allDisposed, List<int> explicitReleaseIds}) trackingResult,
          ) async {
            final h = _Harness();
            h.startParameterCapabilityDisposalTracking =
                (paramsCapabilities) => 'ticket';
            h.finishParameterCapabilityDisposalTracking =
                (ticket) => trackingResult;
            final cap = _FakeCapability();
            final dispatchGate = Completer<void>();
            cap.dispatchGate = dispatchGate.future;
            cap.onDispatch = (_, _, _) => DispatchResult.empty;
            final exportId = h.exportTable.retainOrCreateExportId(cap);

            h.coordinator.handleCall(
              parseRpcMessage(
                _buildCall(questionId: 90, targetExportId: exportId),
              ),
            );
            await Future<void>.delayed(Duration.zero);
            expect(h.sentBytes, isEmpty, reason: 'dispatch still in flight');

            h.coordinator.handleFinish(parseRpcMessage(buildFinishMessage(90)));
            expect(
              h.sentBytes,
              isEmpty,
              reason: 'Return(canceled) is deferred to settle time',
            );

            dispatchGate.complete();
            await Future<void>.delayed(Duration.zero);

            final sent = h.sentBytes
                .map(parseRpcMessage)
                .firstWhere((m) => m.type == RpcMessageType.return_);
            expect(sent.answerId, equals(90));
            expect(sent.returnDisc, equals(2)); // canceled
            expect(
              sent.returnReleaseParamCaps,
              equals(trackingResult.allDisposed),
            );
            final releaseMessages =
                h.sentBytes
                    .map(parseRpcMessage)
                    .where((m) => m.type == RpcMessageType.release)
                    .toList();
            expect(
              releaseMessages.map((m) => m.releaseId),
              unorderedEquals(trackingResult.explicitReleaseIds),
            );
          }

          await run((allDisposed: true, explicitReleaseIds: const []));
          await run((allDisposed: false, explicitReleaseIds: const [7, 8]));
          await run((allDisposed: false, explicitReleaseIds: const []));
        },
      );

      test('a new pipelined call targeting a parent that already has an '
          'outstanding dependent and already received Finish is rejected '
          'with "unknown promisedAnswer questionId"', () async {
        final h = _Harness();
        final dispatchGate = Completer<void>();
        final targetCap =
            _FakeCapability()
              ..dispatchGate = dispatchGate.future
              ..onDispatch = (_, _, _) => DispatchResult.empty;
        final resultExportId = h.exportTable.retainOrCreateExportId(targetCap);
        h.exportResultCapabilityAsWireReference =
            (cap) => SenderHostedCapabilityReference(resultExportId);
        final parentCap =
            _FakeCapability()
              ..dispatchGate = dispatchGate.future
              ..onDispatch =
                  (_, _, _) => DispatchResult(
                    payload: RpcPayload.fromBytes(_singleCapResultBytes),
                    caps: [targetCap],
                  );
        final parentExportId = h.exportTable.retainOrCreateExportId(parentCap);

        h.coordinator.handleCall(
          parseRpcMessage(
            _buildCall(questionId: 1, targetExportId: parentExportId),
          ),
        );
        h.coordinator.handleCall(
          parseRpcMessage(_buildPipelinedCall(questionId: 2, parentQid: 1)),
        );
        h.coordinator.handleFinish(parseRpcMessage(buildFinishMessage(1)));

        // A brand-new pipelined call targeting the same still-pending
        // (dispatch gated open), already-Finished parent must be rejected
        // immediately — it doesn't need to wait for the parent to settle.
        h.coordinator.handleCall(
          parseRpcMessage(_buildPipelinedCall(questionId: 3, parentQid: 1)),
        );

        final rejected = parseRpcMessage(h.sentBytes.single);
        expect(rejected.answerId, equals(3));
        expect(rejected.isReturnException, isTrue);
        expect(
          rejected.exceptionReason,
          contains('unknown promisedAnswer questionId'),
        );

        dispatchGate.complete();
        await Future<void>.delayed(Duration.zero);
      });

      test('two Finish messages for the same still-pending, '
          'dependent-having parent: the second is a no-op — cancellation '
          'is not double-notified and the dependent still resolves '
          'normally', () async {
        final h = _Harness();
        final targetCap =
            _FakeCapability()..onDispatch = (_, _, _) => DispatchResult.empty;
        final resultExportId = h.exportTable.retainOrCreateExportId(targetCap);
        h.exportResultCapabilityAsWireReference =
            (cap) => SenderHostedCapabilityReference(resultExportId);
        final parentCap =
            _FakeCapability()
              ..onDispatch =
                  (_, _, _) => DispatchResult(
                    payload: RpcPayload.fromBytes(_singleCapResultBytes),
                    caps: [targetCap],
                  );
        final parentExportId = h.exportTable.retainOrCreateExportId(parentCap);

        h.coordinator.handleCall(
          parseRpcMessage(
            _buildCall(questionId: 1, targetExportId: parentExportId),
          ),
        );
        h.coordinator.handleCall(
          parseRpcMessage(_buildPipelinedCall(questionId: 2, parentQid: 1)),
        );
        h.coordinator.handleFinish(parseRpcMessage(buildFinishMessage(1)));
        h.coordinator.handleFinish(
          parseRpcMessage(buildFinishMessage(1)),
        ); // duplicate

        await Future<void>.delayed(Duration.zero);

        expect(targetCap.dispatches, hasLength(1));
        final parentReturn = h.sentBytes
            .map(parseRpcMessage)
            .firstWhere((m) => m.answerId == 1);
        expect(parentReturn.isReturnResults, isTrue);
        expect(
          h.disposedFromTable,
          hasLength(1),
          reason: 'released exactly once, not twice',
        );
      });

      test('sendResultsToYourself with an early Finish and no pipelined '
          'dependents: Return.canceled is sent at settle time instead of '
          'Return.resultsSentElsewhere, on both success and failure', () async {
        final h1 = _Harness();
        final succeedingCap = _FakeCapability();
        final dispatchGate = Completer<void>();
        succeedingCap.dispatchGate = dispatchGate.future;
        succeedingCap.onDispatch = (_, _, _) => DispatchResult.empty;
        final exportId1 = h1.exportTable.retainOrCreateExportId(succeedingCap);
        h1.coordinator.handleCall(
          parseRpcMessage(
            _buildCall(
              questionId: 40,
              targetExportId: exportId1,
              sendResultsToYourself: true,
            ),
          ),
        );
        h1.coordinator.handleFinish(parseRpcMessage(buildFinishMessage(40)));
        expect(h1.sentBytes, isEmpty);
        dispatchGate.complete();
        await Future<void>.delayed(Duration.zero);
        final sent1 = parseRpcMessage(h1.sentBytes.single);
        expect(sent1.answerId, equals(40));
        expect(
          sent1.returnDisc,
          equals(2),
        ); // canceled, not resultsSentElsewhere(3)

        final h2 = _Harness();
        final failingCap =
            _FakeCapability()..throwOnDispatch = const RpcException('boom');
        final exportId2 = h2.exportTable.retainOrCreateExportId(failingCap);
        h2.coordinator.handleCall(
          parseRpcMessage(
            _buildCall(
              questionId: 41,
              targetExportId: exportId2,
              sendResultsToYourself: true,
            ),
          ),
        );
        h2.coordinator.handleFinish(parseRpcMessage(buildFinishMessage(41)));
        expect(h2.sentBytes, isEmpty);
        await Future<void>.delayed(Duration.zero);
        final sent2 = parseRpcMessage(h2.sentBytes.single);
        expect(sent2.answerId, equals(41));
        expect(sent2.returnDisc, equals(2));
      });

      test('resolveLocalAnswer still resolves a still-pending '
          'sendResultsToYourself dispatch correctly — regression guard '
          'that the pipelined-dependency rewrite of '
          'getResolvedAnswerFor/getDispatchResultFor lookups left this unrelated '
          'correlation path untouched', () async {
        final h = _Harness();
        final resultCap = _FakeCapability();
        final cap =
            _FakeCapability()
              ..onDispatch =
                  (_, _, _) => DispatchResult(
                    payload: RpcPayload.fromBytes(_singleCapResultBytes),
                    caps: [resultCap],
                  );
        final exportId = h.exportTable.retainOrCreateExportId(cap);

        h.coordinator.handleCall(
          parseRpcMessage(
            _buildCall(
              questionId: 60,
              targetExportId: exportId,
              sendResultsToYourself: true,
            ),
          ),
        );

        final resolved = await h.coordinator.resolveLocalAnswer(60);
        expect(resolved.caps, equals([resultCap]));
      });

      test('a resolveLocalAnswer call captured before a real Finish '
          'arrives still resolves correctly afterward — regression test '
          'for PR #117 review point 1: resolveLocalAnswer must be called '
          'synchronously, as the correlating takeFromOtherQuestion Return '
          'is first processed (see OutgoingCallCoordinator.handleReturn), '
          'so the returned Future is fixed to this generation before a '
          'Finish (which only ends the requester\'s own reference) can '
          'affect it', () async {
        final h = _Harness();
        final resultCap = _FakeCapability();
        final cap =
            _FakeCapability()
              ..onDispatch =
                  (_, _, _) => DispatchResult(
                    payload: RpcPayload.fromBytes(_singleCapResultBytes),
                    caps: [resultCap],
                  );
        final exportId = h.exportTable.retainOrCreateExportId(cap);

        h.coordinator.handleCall(
          parseRpcMessage(
            _buildCall(
              questionId: 62,
              targetExportId: exportId,
              sendResultsToYourself: true,
            ),
          ),
        );
        await Future<void>.delayed(Duration.zero);

        // Captured synchronously, exactly as
        // OutgoingCallCoordinator.handleReturn does the moment it sees
        // ret.isReturnTakeFromOtherQuestion — before anything else runs.
        final captured = h.coordinator.resolveLocalAnswer(62);

        h.coordinator.handleFinish(parseRpcMessage(buildFinishMessage(62)));

        final resolved = await captured;
        expect(resolved.caps, equals([resultCap]));
      });

      test('a resolveLocalAnswer call captured before a real Finish '
          'arrives still surfaces the original error afterward, for a '
          'failed sendResultsToYourself dispatch', () async {
        final h = _Harness();
        final cap =
            _FakeCapability()..throwOnDispatch = const RpcException('boom');
        final exportId = h.exportTable.retainOrCreateExportId(cap);

        h.coordinator.handleCall(
          parseRpcMessage(
            _buildCall(
              questionId: 63,
              targetExportId: exportId,
              sendResultsToYourself: true,
            ),
          ),
        );
        await Future<void>.delayed(Duration.zero);

        final captured = h.coordinator.resolveLocalAnswer(63);
        captured.ignore();

        h.coordinator.handleFinish(parseRpcMessage(buildFinishMessage(63)));

        await expectLater(
          captured,
          throwsA(
            isA<RpcException>().having((e) => e.message, 'message', 'boom'),
          ),
        );
      });

      test('resolveLocalAnswer called *after* a real Finish has already '
          'removed the normal answer state reports "unknown question id" '
          '— capturing late, instead of synchronously as the correlating '
          'Return is first processed, is not a supported usage', () async {
        final h = _Harness();
        final cap =
            _FakeCapability()..onDispatch = (_, _, _) => DispatchResult.empty;
        final exportId = h.exportTable.retainOrCreateExportId(cap);

        h.coordinator.handleCall(
          parseRpcMessage(
            _buildCall(
              questionId: 64,
              targetExportId: exportId,
              sendResultsToYourself: true,
            ),
          ),
        );
        await Future<void>.delayed(Duration.zero);

        h.coordinator.handleFinish(parseRpcMessage(buildFinishMessage(64)));

        await expectLater(
          h.coordinator.resolveLocalAnswer(64),
          throwsA(
            isA<RpcException>().having(
              (e) => e.message,
              'message',
              contains('unknown question id'),
            ),
          ),
        );
      });

      test('a reentrant peer that synchronously reuses a qid during its '
          'own noFinishNeeded Return (early Finish + an existing pipelined '
          'dependent, no result capabilities of its own) does not have '
          'that new answer state corrupted — regression test for the '
          'id-reuse-during-send hazard the dependency ticket design exists '
          'to avoid', () async {
        final h = _Harness();
        final dispatchGate = Completer<void>();
        final parentCap =
            _FakeCapability()
              ..dispatchGate = dispatchGate.future
              ..onDispatch = (_, _, _) => DispatchResult.empty;
        final parentExportId = h.exportTable.retainOrCreateExportId(parentCap);

        final newCallGate = Completer<void>();
        final newCap =
            _FakeCapability()
              ..dispatchGate = newCallGate.future
              ..onDispatch = (_, _, _) => DispatchResult.empty;
        final newExportId = h.exportTable.retainOrCreateExportId(newCap);

        var reentered = false;
        h.sendBytes = (bytes) {
          h.sentBytes.add(bytes);
          final msg = parseRpcMessage(bytes);
          if (!reentered &&
              msg.answerId == 1 &&
              msg.isReturnResults &&
              msg.returnNoFinishNeeded) {
            reentered = true;
            // Simulates a peer reacting to this vat's own Return through a
            // synchronously-reentrant sink (an in-memory or `sync: true`
            // transport): question id 1's protocol lifecycle is over from
            // the peer's perspective the instant it sees this Return, so
            // it legally reuses that same id for a brand-new, unrelated
            // Call before this send even returns.
            h.coordinator.handleCall(
              parseRpcMessage(
                _buildCall(questionId: 1, targetExportId: newExportId),
              ),
            );
          }
        };

        h.coordinator.handleCall(
          parseRpcMessage(
            _buildCall(questionId: 1, targetExportId: parentExportId),
          ),
        );
        h.coordinator.handleCall(
          parseRpcMessage(_buildPipelinedCall(questionId: 2, parentQid: 1)),
        );
        h.coordinator.handleFinish(parseRpcMessage(buildFinishMessage(1)));

        dispatchGate.complete();
        await Future<void>.delayed(Duration.zero);

        expect(reentered, isTrue);
        // The new call reusing qid 1 must be completely untouched by the
        // old call's post-send cleanup — still a live, pending answer, not
        // thrown away or corrupted by a StateError leaking into the old
        // dispatch's own completion handling.
        expect(h.answerTable.getDispatchResultFor(1), isNotNull);
        expect(newCap.dispatches, hasLength(1));

        newCallGate.complete();
        await Future<void>.delayed(Duration.zero);
        final newReturn = h.sentBytes
            .map(parseRpcMessage)
            .lastWhere((m) => m.answerId == 1);
        expect(newReturn.isReturnResults, isTrue);
      });

      test('a reentrant peer that sends Finish then immediately reuses a '
          'qid during an ordinary, no-pipelined-dependents, '
          'capability-bearing Return does not have the new call corrupted '
          '— regression test for PR #117 review point 1: this exact '
          'scenario used to reach a post-send handleAnswerIssued(qid) '
          're-lookup that could observe a different call\'s state under '
          'the same qid; that whole hazard class is gone now that '
          'AnswerTable decides retention in the same call as installing '
          'the answer, before the Return is ever sent', () async {
        final h = _Harness();
        final resultCap = _FakeCapability();
        final parentCap =
            _FakeCapability()
              ..onDispatch =
                  (_, _, _) => DispatchResult(
                    payload: RpcPayload.fromBytes(_singleCapResultBytes),
                    caps: [resultCap],
                  );
        final parentExportId = h.exportTable.retainOrCreateExportId(parentCap);

        final newCap =
            _FakeCapability()..onDispatch = (_, _, _) => DispatchResult.empty;
        final newExportId = h.exportTable.retainOrCreateExportId(newCap);

        var reentered = false;
        h.sendBytes = (bytes) {
          h.sentBytes.add(bytes);
          final msg = parseRpcMessage(bytes);
          if (!reentered && msg.answerId == 1 && msg.isReturnResults) {
            reentered = true;
            // The Return carries a live capability, so a well-behaved peer
            // still owes a real Finish for it — but nothing stops it from
            // sending that Finish and then reusing qid 1 for a brand-new
            // Call, both synchronously as a reaction to this very Return.
            h.coordinator.handleFinish(parseRpcMessage(buildFinishMessage(1)));
            h.coordinator.handleCall(
              parseRpcMessage(
                _buildCall(questionId: 1, targetExportId: newExportId),
              ),
            );
          }
        };

        h.coordinator.handleCall(
          parseRpcMessage(
            _buildCall(questionId: 1, targetExportId: parentExportId),
          ),
        );
        await Future<void>.delayed(Duration.zero);

        expect(reentered, isTrue);
        // The new call reusing qid 1 must be untouched by anything the old
        // call's completion handler does after sendBytes() returns.
        expect(newCap.dispatches, hasLength(1));
        final newReturn = h.sentBytes
            .map(parseRpcMessage)
            .lastWhere((m) => m.answerId == 1);
        expect(newReturn.isReturnResults, isTrue);
      });

      test('an ordinary (non-sendResultsToYourself) call with a pipelined '
          'dependent that receives an early Finish, and then fails '
          'instead of succeeding: still answers with a normal exception '
          'Return, and the dependent still gets its own exception Return '
          'too — regression test for PR #117 review point 2, where the '
          'caller used to treat this exact AnswerRecorded outcome as '
          'unreachable and throw', () async {
        final h = _Harness();
        final dispatchGate = Completer<void>();
        final parentCap =
            _FakeCapability()
              ..dispatchGate = dispatchGate.future
              ..onDispatch =
                  (_, _, _) => throw const RpcException('parent broke');
        final parentExportId = h.exportTable.retainOrCreateExportId(parentCap);

        h.coordinator.handleCall(
          parseRpcMessage(
            _buildCall(questionId: 1, targetExportId: parentExportId),
          ),
        );
        h.coordinator.handleCall(
          parseRpcMessage(_buildPipelinedCall(questionId: 2, parentQid: 1)),
        );
        h.coordinator.handleFinish(parseRpcMessage(buildFinishMessage(1)));

        dispatchGate.complete();
        await Future<void>.delayed(Duration.zero);

        final parentReturn = h.sentBytes
            .map(parseRpcMessage)
            .firstWhere((m) => m.answerId == 1);
        expect(parentReturn.isReturnException, isTrue);
        expect(parentReturn.exceptionReason, equals('parent broke'));
        final dependentReturn = h.sentBytes
            .map(parseRpcMessage)
            .firstWhere((m) => m.answerId == 2);
        expect(dependentReturn.isReturnException, isTrue);
      });
    });

    group(
      'generation-safe local-answer capture across a real OutgoingCallCoordinator '
      '(PR #117 review rounds 5-6)',
      () {
        // Wires an OutgoingCallCoordinator's resolveLocalAnswer straight to
        // [h]'s own IncomingCallCoordinator — the same relationship
        // TwoPartyRpcConnection sets up in production, minus the socket.
        OutgoingCallCoordinator outgoingFor(_Harness h) {
          return OutgoingCallCoordinator(
            questions: QuestionTable(),
            imports: ImportTable(),
            sendBytes: (_) {},
            resolveParameterCapabilityReferences:
                (paramsCapabilities, {qid, required ensureActive}) => const [],
            releaseParameterCapabilityExports: (_) {},
            acquireCapabilityFromWireReference: (_) => _FakeCapability(),
            resolveLocalAnswer: h.coordinator.resolveLocalAnswer,
          );
        }

        test(
          'a takeFromOtherQuestion Return captured while the target '
          'sendResultsToYourself dispatch is still pending resolves to the '
          'original result — unaffected by a real Finish and a qid reuse for '
          'an unrelated new generation that both happen before it settles',
          () async {
            final h = _Harness();
            final outgoing = outgoingFor(h);

            final dispatchGate = Completer<void>();
            final resultCap = _FakeCapability();
            final cap =
                _FakeCapability()
                  ..dispatchGate = dispatchGate.future
                  ..onDispatch =
                      (_, _, _) => DispatchResult(
                        payload: RpcPayload.fromBytes(_singleCapResultBytes),
                        caps: [resultCap],
                      );
            final exportId = h.exportTable.retainOrCreateExportId(cap);

            // Y: an incoming call flagged sendResultsToYourself, dispatch
            // deliberately held open.
            h.coordinator.handleCall(
              parseRpcMessage(
                _buildCall(
                  questionId: 5,
                  targetExportId: exportId,
                  sendResultsToYourself: true,
                ),
              ),
            );

            // X: this vat's own outgoing call, correlated to Y via
            // takeFromOtherQuestion — captured synchronously here, while Y
            // is still pending.
            final started = outgoing.start(
              target: const ImportedCapabilityTarget(0),
              params: SerializedParams(_emptyMessageBytes),
              interfaceId: 1,
              methodId: 2,
            );
            outgoing.handleReturn(
              parseRpcMessage(
                buildReturnTakeFromOtherQuestionMessage(
                  answerId: started.questionId,
                  questionId: 5,
                ),
              ),
            );

            // Y settles, a real Finish arrives for it, and the peer then
            // legally reuses qid 5 for a brand-new, unrelated generation —
            // all before X's own result is ever awaited.
            dispatchGate.complete();
            await Future<void>.delayed(Duration.zero);
            h.coordinator.handleFinish(parseRpcMessage(buildFinishMessage(5)));

            final newCap =
                _FakeCapability()
                  ..onDispatch = (_, _, _) => DispatchResult.empty;
            final newExportId = h.exportTable.retainOrCreateExportId(newCap);
            h.coordinator.handleCall(
              parseRpcMessage(
                _buildCall(questionId: 5, targetExportId: newExportId),
              ),
            );

            // A *fresh* local lookup against reused qid 5 must never observe
            // generation #1's result — there is no qid-keyed stash left
            // that could leak it through, and generation #2 itself was
            // never sendResultsToYourself, so takeRedirectedResult reports
            // that instead of returning generation #2's own data either.
            await expectLater(
              h.coordinator.resolveLocalAnswer(5),
              throwsA(
                isA<RpcException>().having(
                  (e) => e.message,
                  'message',
                  contains('did not use sendResultsTo.yourself'),
                ),
              ),
            );

            final resolved = await started.result;
            expect(resolved.caps, equals([resultCap]));
          },
        );

        test('a takeFromOtherQuestion Return captured while the target '
            'sendResultsToYourself dispatch is still pending surfaces the '
            'original failure reason — unaffected by a later Finish and qid '
            'reuse', () async {
          final h = _Harness();
          final outgoing = outgoingFor(h);

          final dispatchGate = Completer<void>();
          final cap =
              _FakeCapability()
                ..dispatchGate = dispatchGate.future
                ..onDispatch = (_, _, _) => throw const RpcException('boom');
          final exportId = h.exportTable.retainOrCreateExportId(cap);

          h.coordinator.handleCall(
            parseRpcMessage(
              _buildCall(
                questionId: 6,
                targetExportId: exportId,
                sendResultsToYourself: true,
              ),
            ),
          );

          final started = outgoing.start(
            target: const ImportedCapabilityTarget(0),
            params: SerializedParams(_emptyMessageBytes),
            interfaceId: 1,
            methodId: 2,
          );
          outgoing.handleReturn(
            parseRpcMessage(
              buildReturnTakeFromOtherQuestionMessage(
                answerId: started.questionId,
                questionId: 6,
              ),
            ),
          );
          started.result.ignore();

          dispatchGate.complete();
          await Future<void>.delayed(Duration.zero);
          h.coordinator.handleFinish(parseRpcMessage(buildFinishMessage(6)));

          final newCap =
              _FakeCapability()..onDispatch = (_, _, _) => DispatchResult.empty;
          final newExportId = h.exportTable.retainOrCreateExportId(newCap);
          h.coordinator.handleCall(
            parseRpcMessage(
              _buildCall(questionId: 6, targetExportId: newExportId),
            ),
          );

          await expectLater(
            started.result,
            throwsA(
              isA<RpcException>().having((e) => e.message, 'message', 'boom'),
            ),
          );
        });

        test('a takeFromOtherQuestion Return captured *after* the target '
            'sendResultsToYourself dispatch already settled still resolves to '
            'the original result even after a real Finish and a qid reuse for '
            'an unrelated new generation', () async {
          final h = _Harness();
          final outgoing = outgoingFor(h);

          final resultCap = _FakeCapability();
          final cap =
              _FakeCapability()
                ..onDispatch =
                    (_, _, _) => DispatchResult(
                      payload: RpcPayload.fromBytes(_singleCapResultBytes),
                      caps: [resultCap],
                    );
          final exportId = h.exportTable.retainOrCreateExportId(cap);

          // Y settles *before* the correlating takeFromOtherQuestion Return
          // is even processed.
          h.coordinator.handleCall(
            parseRpcMessage(
              _buildCall(
                questionId: 7,
                targetExportId: exportId,
                sendResultsToYourself: true,
              ),
            ),
          );
          await Future<void>.delayed(Duration.zero);

          final started = outgoing.start(
            target: const ImportedCapabilityTarget(0),
            params: SerializedParams(_emptyMessageBytes),
            interfaceId: 1,
            methodId: 2,
          );
          outgoing.handleReturn(
            parseRpcMessage(
              buildReturnTakeFromOtherQuestionMessage(
                answerId: started.questionId,
                questionId: 7,
              ),
            ),
          );

          h.coordinator.handleFinish(parseRpcMessage(buildFinishMessage(7)));

          final newCap =
              _FakeCapability()..onDispatch = (_, _, _) => DispatchResult.empty;
          final newExportId = h.exportTable.retainOrCreateExportId(newCap);
          h.coordinator.handleCall(
            parseRpcMessage(
              _buildCall(questionId: 7, targetExportId: newExportId),
            ),
          );

          final resolved = await started.result;
          expect(resolved.caps, equals([resultCap]));
        });

        test('a second takeFromOtherQuestion Return naming the same '
            'still-current generation is a protocol violation, not a '
            'second delivery of the same result — one-shot ownership '
            'transfer, mirroring capnp-rpc\'s own '
            'Answer.redirected_results.take()', () async {
          final h = _Harness();

          final resultCap = _FakeCapability();
          final cap =
              _FakeCapability()
                ..onDispatch =
                    (_, _, _) => DispatchResult(
                      payload: RpcPayload.fromBytes(_singleCapResultBytes),
                      caps: [resultCap],
                    );
          final exportId = h.exportTable.retainOrCreateExportId(cap);

          h.coordinator.handleCall(
            parseRpcMessage(
              _buildCall(
                questionId: 8,
                targetExportId: exportId,
                sendResultsToYourself: true,
              ),
            ),
          );

          final first = await h.coordinator.resolveLocalAnswer(8);
          expect(first.caps, equals([resultCap]));

          await expectLater(
            h.coordinator.resolveLocalAnswer(8),
            throwsA(
              isA<RpcException>().having(
                (e) => e.message,
                'message',
                contains('already taken'),
              ),
            ),
          );
        });
      },
    );
  });
}
