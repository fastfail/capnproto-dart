import 'dart:async';
import 'dart:typed_data';

import 'package:capnproto_dart/capnproto_dart.dart';

import '../../capability/capability.dart';
import '../../capability/rpc_payload.dart';
import '../capabilities/import_table.dart';
import '../capabilities/wire_capability_reference.dart';
import '../rpc_exception.dart';
import '../rpc_message_codec.dart';
import 'answer_table.dart';
import 'outgoing_call.dart';
import 'question_table.dart';

/// Pre-built 16-byte message: single segment (1 word), null root pointer.
/// Used as fallback for `-> stream` and void methods that return no
/// content. Connection-independent, so it's duplicated here rather than
/// injected — see `IncomingCallCoordinator`'s own identical constant, used
/// for the same reason on the incoming side.
final _emptyResultBytes = Uint8List.fromList([
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

/// Outgoing-call construction and sending: target/params resolution,
/// capTable encoding for a fresh Call, question tracking, and handling the
/// matching Return once it arrives. Extracted from `TwoPartyRpcConnection`
/// as a standalone, constructor-injected class — unlike the other
/// protocol-flow pieces, which remain private extensions on
/// `TwoPartyRpcConnection`, this one is a plain top-level class (like
/// [QuestionTable]/[ImportTable]) so it can be constructed and tested
/// directly, without a real connection or sockets.
///
/// [start] is the normal entry point;
/// [startCallWithAllocatedQuestion] is the lower-level one `IncomingCallCoordinator`'s tail-call forwarding needs.
final class OutgoingCallCoordinator {
  /// Shared with the owning connection — also read directly by
  /// `IncomingCallCoordinator` (tail-call forwarding) and
  /// `CapabilityProtocol` (`sentCompleterFor`, for pipelining-safety
  /// checks), so this class does not own it exclusively.
  final QuestionTable questions;

  /// Shared with the owning connection, same reasoning as [questions].
  final ImportTable imports;

  final void Function(Uint8List bytes) sendBytes;

  /// Resolves a Call's params capabilities into wire references,
  /// synchronously when possible — see the matching doc comment this method
  /// carries as `CapabilityProtocol.resolveParameterCapabilityReferences`.
  ///
  /// [ensureActive] must be called before any side effect with lasting
  /// state (an export creation, a refcount bump, recording bookkeeping
  /// against [qid]) and again immediately after resuming from any `await` —
  /// it throws once this coordinator has torn down, same as
  /// [_throwIfTornDown]. A resolver implementation that never suspends
  /// between its start and its side effects only needs the one entry call;
  /// one that awaits something (an import id, a pipelined call's parent
  /// being sent) must re-check right after each resume, since `tearDown`
  /// may have run during that wait — see `_resolveCapTableAsync`'s own use
  /// of this for the concrete scenario this guards against: this
  /// coordinator's own post-teardown guards (`_failIfTornDown`,
  /// `_throwIfTornDown`) can only roll back state recorded against [qid] in
  /// `QuestionTable`, which `tearDown` has, by then, already dropped
  /// entirely — so a side effect started *after* that point would never be
  /// rolled back by anything.
  final FutureOr<List<WireCapabilityReference>> Function(
    List<Capability> paramsCapabilities, {
    int? qid,
    required void Function() ensureActive,
  })
  resolveParameterCapabilityReferences;

  final void Function(List<int> exportIds) releaseParameterCapabilityExports;
  final Capability Function(WireCapabilityReference reference)
  acquireCapabilityFromWireReference;
  final Future<ResolvedAnswer> Function(int qid) resolveLocalAnswer;

  /// Invoked from [handleReturn], right after confirming a live completer
  /// for the Return's answerId, before that completer is completed — the
  /// hook the owning connection uses for bootstrap-Return handling (state
  /// that belongs to the connection, not this coordinator).
  final void Function(RpcMessage msg)? onReturn;

  RpcException? _tornDownError;

  OutgoingCallCoordinator({
    required this.questions,
    required this.imports,
    required this.sendBytes,
    required this.resolveParameterCapabilityReferences,
    required this.releaseParameterCapabilityExports,
    required this.acquireCapabilityFromWireReference,
    required this.resolveLocalAnswer,
    this.onReturn,
  });

  /// True when starting a Call against [target] needs to `await` something
  /// before any side effect (an export refcount bump, [sendBytes]) can run —
  /// see [resolveParameterCapabilityReferences]'s matching doc comment for why
  /// this must be checked before touching anything with a side effect.
  bool _targetRequiresAsyncResolution(OutgoingCallTarget target) =>
      switch (target) {
        ImportedCapabilityTarget(importId: final id) => id is! int,
        PromisedAnswerTarget(questionId: final qid) =>
          questions.sentCompleterFor(qid) != null,
      };

  /// Builds an outgoing Call's wire bytes against [target]/[params],
  /// synchronously when possible (mirrors the old `_startResolvedImportCall`
  /// fast path: no `Future`/microtask indirection when [target]'s import id
  /// is already known and [paramsCapabilities] resolves synchronously) and
  /// asynchronously otherwise, via [_buildOutgoingCallBytesAsync].
  FutureOr<Uint8List> _buildOutgoingCallBytes({
    required int qid,
    required OutgoingCallTarget target,
    required OutgoingParams params,
    required int interfaceId,
    required int methodId,
    required List<Capability> paramsCapabilities,
    bool sendResultsToYourself = false,
  }) {
    final buildParams = switch (params) {
      SerializedParams(bytes: final b) =>
        (AnyPointerBuilder p) =>
            p.setMessageBytes(b, preserveCapabilityPointers: true),
      BuilderParams(build: final b) => b,
    };

    if (_targetRequiresAsyncResolution(target)) {
      return _buildOutgoingCallBytesAsync(
        qid: qid,
        target: target,
        buildParams: buildParams,
        interfaceId: interfaceId,
        methodId: methodId,
        paramsCapabilities: paramsCapabilities,
        sendResultsToYourself: sendResultsToYourself,
      );
    }

    int targetImportId = 0;
    int? targetPromisedAnswerQid;
    var targetTransformPath = const <int>[];
    switch (target) {
      case ImportedCapabilityTarget(importId: final id):
        targetImportId =
            id as int; // _targetRequiresAsyncResolution confirmed this above
        imports.throwIfBroken(targetImportId);
      case PromisedAnswerTarget(
        questionId: final pqid,
        transformPath: final path,
      ):
        targetPromisedAnswerQid = pqid;
        targetTransformPath = path;
    }
    return buildCallMessageWithParamsBuilderMaybeSync(
      questionId: qid,
      targetImportId: targetImportId,
      targetPromisedAnswerQid: targetPromisedAnswerQid,
      targetTransformPath: targetTransformPath,
      interfaceId: interfaceId,
      methodId: methodId,
      buildParams: buildParams,
      resolveReferences:
          () => resolveParameterCapabilityReferences(
            paramsCapabilities,
            qid: qid,
            ensureActive: _throwIfTornDown,
          ),
      sendResultsToYourself: sendResultsToYourself,
    );
  }

  /// Async branch of [_buildOutgoingCallBytes] — awaits whatever [target]
  /// needs (an import id, or the promisedAnswer target's parent being sent),
  /// then resolves the capTable.
  ///
  /// Uses [buildCallMessageWithParamsBuilder] (a `resolveCapTable` callback invoked
  /// only *after* [buildParams] has returned), not a pre-resolve-then-sync-
  /// build sequence: [paramsCapabilities] may still be being appended to by
  /// [buildParams] itself for a builder-based Call (see [Capability.
  /// dispatchWithParamsBuilder]'s contract) — resolving the capTable before
  /// [buildParams] runs would silently miss those. This is safe and
  /// equivalent for a serialized-params Call too, since the synthetic
  /// `setMessageBytes` [buildParams] built by [_buildOutgoingCallBytes]
  /// never appends to [paramsCapabilities].
  Future<Uint8List> _buildOutgoingCallBytesAsync({
    required int qid,
    required OutgoingCallTarget target,
    required void Function(AnyPointerBuilder) buildParams,
    required int interfaceId,
    required int methodId,
    required List<Capability> paramsCapabilities,
    required bool sendResultsToYourself,
  }) async {
    int targetImportId = 0;
    int? targetPromisedAnswerQid;
    var targetTransformPath = const <int>[];
    switch (target) {
      case ImportedCapabilityTarget(importId: final id):
        targetImportId = id is int ? id : await id;
        imports.throwIfBroken(targetImportId);
      case PromisedAnswerTarget(
        questionId: final pqid,
        transformPath: final path,
      ):
        // For promisedAnswer targets, wait until the parent Call is on the
        // wire so the server always receives the parent before the
        // pipelined call.
        final parentSent = questions.sentCompleterFor(pqid);
        if (parentSent != null) await parentSent.future;
        targetPromisedAnswerQid = pqid;
        targetTransformPath = path;
    }
    // Whatever we just awaited (the target import id, or the promisedAnswer
    // target's parent being sent) may have taken long enough for tearDown()
    // to run in the meantime. resolveParameterCapabilityReferences has real side effects
    // (export creation, refcount bumps) that nothing will ever clean up on a
    // torn-down connection — bail out before it even starts, same as
    // [startCallWithAllocatedQuestion]'s own entry guard does for the fully-synchronous path.
    // This alone isn't enough once resolveParameterCapabilityReferences itself starts
    // running, though: it may need its own further `await`s (an unresolved
    // *params* capability's import id, a *different* pipelined param's
    // parent being sent) — [ensureActive] is threaded into it precisely so
    // it can re-check after each of those too, not just at its own entry.
    _throwIfTornDown();
    return buildCallMessageWithParamsBuilder(
      questionId: qid,
      targetImportId: targetImportId,
      targetPromisedAnswerQid: targetPromisedAnswerQid,
      targetTransformPath: targetTransformPath,
      interfaceId: interfaceId,
      methodId: methodId,
      buildParams: buildParams,
      resolveCapTable:
          () async => await resolveParameterCapabilityReferences(
            paramsCapabilities,
            qid: qid,
            ensureActive: _throwIfTornDown,
          ),
      sendResultsToYourself: sendResultsToYourself,
    );
  }

  void _throwIfTornDown() {
    final error = _tornDownError;
    if (error != null) throw error;
  }

  /// Fails [question] with [_tornDownError] and rolls back any params
  /// export refs it already recorded — reuses
  /// [startCallWithAllocatedQuestion]'s own `onError`
  /// rollback path (see its doc comment) so a call that's already torn down
  /// when [startCallWithAllocatedQuestion] is entered, or that tears down
  /// while its async build
  /// is still in flight, never reaches [sendBytes]. Returns whether
  /// [question] was failed this way.
  bool _failIfTornDown(OutgoingQuestion question) {
    final error = _tornDownError;
    if (error == null) return false;
    final ids = questions.failBeforeSend(question, error, StackTrace.current);
    if (ids != null) releaseParameterCapabilityExports(ids);
    return true;
  }

  /// The single site wiring [QuestionTable.markSent] (on success) and
  /// [QuestionTable.failBeforeSend] plus
  /// [releaseParameterCapabilityExports] (on failure)
  /// together for an outgoing Call — shared by [start] and
  /// `_sendForwardedTailCall` (via this method directly), so this pairing is
  /// never wired up ad hoc at a third call site. Every path that reaches
  /// [onError] does so before [sendBytes] ever runs (nothing after that
  /// point in [_buildOutgoingCallBytes]/[_buildOutgoingCallBytesAsync] can
  /// throw) — any params export refs already bumped for [question] never
  /// actually reached the peer and must be rolled back here.
  ///
  /// Also guarded, via [_failIfTornDown], against `tearDown` — both at entry
  /// (this coordinator is already closed) and again once an async build
  /// finishes (`tearDown` ran while it was still in flight): [sendBytes]
  /// must never run for either case, and any params export refs
  /// `resolveParameterCapabilityReferences` already recorded against [qid] *before*
  /// tearDown ran are rolled back here the same way a build failure's would
  /// be. That rollback only reaches bookkeeping recorded before tearDown,
  /// though — `QuestionTable.tearDown` drops [question]'s tracking entirely,
  /// so nothing here can roll back a side effect `resolveParameterCapabilityReferences`
  /// starts *after* that point (e.g. resuming from an `await` mid-resolution
  /// with tearDown having landed during the wait). Preventing that is
  /// `resolveParameterCapabilityReferences`'s own job, via the `ensureActive` callback
  /// this class passes it — see [resolveParameterCapabilityReferences]'s doc comment.
  void startCallWithAllocatedQuestion({
    required OutgoingQuestion question,
    required OutgoingCallTarget target,
    required OutgoingParams params,
    required int interfaceId,
    required int methodId,
    required List<Capability> paramsCapabilities,
    bool sendResultsToYourself = false,
  }) {
    if (_failIfTornDown(question)) return;

    final qid = question.id;
    void onError(Object e, StackTrace st) {
      final ids = questions.failBeforeSend(question, e, st);
      if (ids != null) releaseParameterCapabilityExports(ids);
    }

    try {
      final builtOrFuture = _buildOutgoingCallBytes(
        qid: qid,
        target: target,
        params: params,
        interfaceId: interfaceId,
        methodId: methodId,
        paramsCapabilities: paramsCapabilities,
        sendResultsToYourself: sendResultsToYourself,
      );
      if (builtOrFuture is Uint8List) {
        sendBytes(builtOrFuture);
        questions.markSent(qid);
      } else {
        builtOrFuture.then((bytes) {
          // The build may have taken long enough for tearDown() to run in
          // the meantime — see _throwIfTornDown's matching guard earlier in
          // the async build itself for why this can't just be assumed away.
          if (_failIfTornDown(question)) return;
          sendBytes(bytes);
          questions.markSent(qid);
        }, onError: onError).ignore();
      }
    } catch (e, st) {
      onError(e, st);
    }
  }

  /// Starts an outgoing Call against [target] with [params]. Allocates a
  /// question ID immediately (available synchronously for pipelining, via
  /// [StartedOutgoingCall.questionId]), then builds and sends the Call message —
  /// synchronously when possible, asynchronously otherwise — via [startCallWithAllocatedQuestion].
  /// [StartedOutgoingCall.result] resolves once the matching Return arrives.
  StartedOutgoingCall start({
    required OutgoingCallTarget target,
    required OutgoingParams params,
    required int interfaceId,
    required int methodId,
    List<Capability> paramsCapabilities = const [],
  }) {
    if (_tornDownError != null) {
      throw RpcException('connection is closed', kind: ErrorKind.disconnected);
    }
    // Mirrors the old _startResolvedImportCall's up-front check: a broken
    // import already known synchronously throws immediately, before a
    // question id is even allocated. An import id that still needs
    // awaiting, or a promisedAnswer target, is checked later instead, once
    // _buildOutgoingCallBytes[Async] actually reaches it.
    if (target case ImportedCapabilityTarget(
      importId: final id,
    ) when id is int) {
      imports.throwIfBroken(id);
    }

    final question = questions.allocate();
    question.sentCompleter!.future.ignore();

    startCallWithAllocatedQuestion(
      question: question,
      target: target,
      params: params,
      interfaceId: interfaceId,
      methodId: methodId,
      paramsCapabilities: paramsCapabilities,
    );

    return StartedOutgoingCall(
      question.id,
      _awaitAndProcessReturn(question.id, question.returnCompleter!),
    );
  }

  Future<DispatchResult> _awaitAndProcessReturn(
    int qid,
    Completer<ReceivedReturn> completer,
  ) async {
    final ReceivedReturn received;
    final List<int>? paramExportIds;
    try {
      received = await completer.future;
    } finally {
      // Whether or not a params-caps entry was ever recorded for this qid
      // (see `CapabilityProtocol`'s internal `_recordParamExportIds`),
      // drop it now — nothing past this point reads
      // _questionParamExportIds[qid] again, on
      // any path (success, exception, or completer failing before a Return
      // ever arrived). Captured into a local first so the success path below
      // still has it even though `finally` runs before that code does.
      paramExportIds = questions.takeParamExportIds(qid);
    }
    final ret = switch (received) {
      OrdinaryReceivedReturn(:final message) => message,
      TakeFromLocalAnswerReturn(:final message) => message,
    };

    // Only Return-results/Return-exception ever legitimately carry these —
    // see RpcMessage.returnReleaseParamCaps/returnNoFinishNeeded's doc
    // comment. releaseParamCaps applying to an exception Return, not just
    // results, mirrors buildReturnExceptionMessage's own support for it.
    final answersCall = ret.isReturnResults || ret.isReturnException;
    if (answersCall && ret.returnReleaseParamCaps && paramExportIds != null) {
      releaseParameterCapabilityExports(paramExportIds);
    }
    if (!(answersCall && ret.returnNoFinishNeeded)) {
      sendBytes(buildFinishMessage(qid, releaseResultCaps: false));
    }

    if (ret.isReturnException) {
      throw RpcException(
        ret.exceptionReason ?? 'remote exception',
        kind: ret.exceptionKind,
      );
    }
    if (received case TakeFromLocalAnswerReturn(:final localAnswer)) {
      // The peer tail-called this call onward to a capability it imports
      // from us — i.e. back to a capability WE host. The real answer is
      // therefore already tracked, locally, under our own incoming-answer
      // bookkeeping for that forwarded call: no extra wire round trip
      // needed to fetch it. [localAnswer] was already captured,
      // synchronously, when [handleReturn] first processed this Return —
      // see [TakeFromLocalAnswerReturn]'s own doc comment for why this
      // never re-derives it from `ret.takeFromOtherQuestion` here.
      final resolved = await localAnswer;
      return DispatchResult(
        payload: RpcPayload.fromBytes(resolved.resultBytes),
        caps: resolved.caps,
      );
    }
    if (!ret.isReturnResults) {
      // canceled / resultsSentElsewhere / acceptFromThirdParty — none of
      // these are implemented by this vat. Surfacing them as an explicit
      // error is important specifically for resultsSentElsewhere: it's only
      // ever valid as the Return to a call *we* sent with
      // sendResultsTo=yourself (see `_sendForwardedTailCall`, which never
      // routes through [_awaitAndProcessReturn]), so seeing it here means a peer sent
      // it unprompted — treating it as an empty success would silently hand
      // the caller a bogus empty-struct result instead of the real one.
      throw RpcException(
        'unsupported Return variant: ${describeReturnVariant(ret.returnDisc)}',
      );
    }

    // Convert capTable entries into ImportedCapabilities.
    final caps = <Capability>[];
    for (final reference in ret.capabilityTableReferences) {
      caps.add(acquireCapabilityFromWireReference(reference));
    }

    final resultsContent = ret.resultsContent;
    return DispatchResult(
      payload:
          resultsContent != null
              ? RpcPayload.fromEnvelope(resultsContent)
              : RpcPayload.fromBytes(_emptyResultBytes),
      caps: caps,
    );
  }

  /// For a `takeFromOtherQuestion` Return, captures [resolveLocalAnswer]'s
  /// result *synchronously*, right here — not deferred to
  /// [_awaitAndProcessReturn]'s later continuation — so the captured
  /// reference is fixed to whichever Answer generation exists under
  /// `msg.takeFromOtherQuestion` at the moment this Return is actually
  /// processed, immune to anything that happens to that question id
  /// afterward (a real Finish, or the peer legally reusing it) — see
  /// [TakeFromLocalAnswerReturn]'s own doc comment.
  void handleReturn(RpcMessage msg) {
    final completer = questions.takeReturn(msg.answerId);
    if (completer == null) return;

    onReturn?.call(msg);

    if (completer.isCompleted) return;
    completer.complete(
      msg.isReturnTakeFromOtherQuestion
          ? TakeFromLocalAnswerReturn(
            msg,
            _captureLocalAnswer(msg.takeFromOtherQuestion),
          )
          : OrdinaryReceivedReturn(msg),
    );
  }

  /// [resolveLocalAnswer] is caller-injected and typed to always return a
  /// Future, but nothing in the type system stops an implementation from
  /// throwing synchronously instead — that must not propagate out of
  /// [handleReturn] before the completer it feeds even gets constructed, or
  /// that completer (and whoever awaits it) would hang forever.
  ///
  /// The returned Future is only actually awaited once
  /// [_awaitAndProcessReturn] unwraps it from the [ReceivedReturn] this
  /// gets wrapped into — `.ignore()` tells the runtime that gap is
  /// deliberate, not an abandoned error going unhandled.
  Future<ResolvedAnswer> _captureLocalAnswer(int qid) {
    Future<ResolvedAnswer> localAnswer;
    try {
      localAnswer = resolveLocalAnswer(qid);
    } catch (e, st) {
      localAnswer = Future.error(e, st);
    }
    localAnswer.ignore();
    return localAnswer;
  }

  /// Fails every pending outgoing Call with [error] and rejects any future
  /// [start] call the same way. Called once, from the owning connection's
  /// own teardown.
  void tearDown(RpcException error) {
    _tornDownError = error;
    questions.tearDown(error);
  }
}
