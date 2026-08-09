import 'dart:async';
import 'dart:collection';
import 'dart:typed_data';

import 'package:capnproto_dart/capnproto_dart.dart';

import '../../capability/capability.dart';
import '../../capability/rpc_payload.dart';
import '../capabilities/export_table.dart';
import '../capabilities/rpc_capability_reference.dart';
import '../capabilities/wire_capability_reference.dart';
import '../rpc_exception.dart';
import '../rpc_message_codec.dart';
import 'answer_table.dart';
import 'outgoing_call.dart';
import 'question_table.dart';

// 24-byte message: struct with 0 data words, 1 pointer word = CapabilityPointer(0).
// Used as the synthesised result for Bootstrap answers so that pipelined
// calls targeting {receiverAnswer: {questionId: <boot>, transform: []}}
// can resolve ptr[0] → the resolved answer's caps[0] (see
// AnswerTable.getResolvedAnswerFor).
// hi = (dataWords & 0xFFFF) | (ptrWords << 16)
// For dataWords=0, ptrWords=1: hi = 0x00010000 → LE bytes [0,0,1,0]
final _bootstrapResultBytes = Uint8List.fromList([
  0, 0, 0, 0, 2, 0, 0, 0, // header: 1 segment, 2 words
  0, 0, 0, 0, 0, 0, 1, 0, // struct ptr: offset=0, data=0, ptrs=1
  3, 0, 0, 0, 0, 0, 0, 0, // ptr[0] = CapabilityPointer(index=0)
]);

/// Pre-built 16-byte message: single segment (1 word), null root pointer.
/// Used as fallback for `-> stream` and void methods that return no
/// content. Connection-independent, so it's duplicated here rather than
/// injected — see `OutgoingCallCoordinator`'s own identical constant, used
/// for the same reason on the outgoing side.
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

/// Incoming Call handling: Bootstrap requests, promised-answer targets,
/// resolving an incoming Call to the [Capability] it dispatches against,
/// running that dispatch (with cancellation and tail-call support), and
/// building/sending its Return. Extracted from `TwoPartyRpcConnection` as a
/// standalone, constructor-injected class — like `OutgoingCallCoordinator`/
/// `CapabilityProtocol`, a plain top-level class (not a `part`-file
/// extension) so it can be constructed and tested directly, without a real
/// connection or sockets.
///
/// Reaches `CapabilityProtocol` and `OutgoingCallCoordinator` through
/// narrow closures rather than holding either object directly: both are
/// declared `final class`, so a test file in a different library can't
/// fake either by implementing it — narrow closures keep this class's own
/// test harness as lightweight as its two siblings'. [startCallWithAllocatedQuestion] closes
/// the one genuine circular dependency in this refactor:
/// `_sendForwardedTailCall` needs `OutgoingCallCoordinator.startCallWithAllocatedQuestion`, while
/// `OutgoingCallCoordinator`'s own `resolveLocalAnswer` field needs
/// [resolveLocalAnswer] — see the wiring site
/// (`two_party_connection.dart`) for how both directions are kept
/// deferred (closures, not eager reads) so neither side's construction
/// depends on the other's having already finished.
///
/// [startParameterCapabilityDisposalTracking] and
/// [finishParameterCapabilityDisposalTracking] bridge a different
/// boundary from [tryExtractCapabilityReference]/[acquireCapabilityFromWireReference]/
/// [exportResultCapabilityAsWireReference]. Those callbacks only extract a public
/// [RpcCapabilityReference] or construct a [Capability]. Params-capability
/// release tracking instead mutates private
/// `_ImportedCapability._deferredReleaseSink` state and reads accumulated
/// results back later, so it is threaded through as an opaque `Object?`
/// ticket this class never inspects — see
/// [startParameterCapabilityDisposalTracking]'s doc.
final class IncomingCallCoordinator {
  /// Shared with the owning connection — also read directly by
  /// `CapabilityProtocol`/`OutgoingCallCoordinator`, so this class does not
  /// own it exclusively.
  final ExportTable exportTable;

  /// Shared with the owning connection, same reasoning as [exportTable].
  final AnswerTable answerTable;

  /// Shared with the owning connection, same reasoning as [exportTable].
  final QuestionTable questions;

  final void Function(Uint8List bytes) sendBytes;
  final void Function(Capability) disposeIgnoringErrors;
  final void Function(CapabilityLease) disposePipelinedTargetIgnoringErrors;
  final void Function(CapabilityLease) disposeLeaseForConnectionTeardown;

  /// Whether the owning connection has already torn down — checked in
  /// [_dispatchTailCall]/[_executeIncomingDispatch]'s async continuations, which may
  /// run after teardown has already cleared the answer tables they'd
  /// otherwise touch.
  final bool Function() isClosed;

  /// Tears the whole connection down — called only from
  /// [_rejectDuplicateQuestionId] on a detected protocol violation.
  final void Function(RpcException error) tearDownConnection;

  /// Tries to extract a reusable RPC capability reference. This is the same
  /// closure used by `CapabilityProtocol`, shared here so tail-call
  /// forwarding follows the same connection-relative extraction rules.
  final RpcCapabilityReference? Function(Capability cap)
  tryExtractCapabilityReference;

  final Capability Function(WireCapabilityReference reference)
  acquireCapabilityFromWireReference;
  final WireCapabilityReference Function(Capability cap)
  exportResultCapabilityAsWireReference;

  /// `OutgoingCallCoordinator.startCallWithAllocatedQuestion` — see this class's own doc
  /// comment for why this closure, not a direct reference to that
  /// coordinator, is what closes the circular dependency between them.
  final void Function({
    required OutgoingQuestion question,
    required OutgoingCallTarget target,
    required OutgoingParams params,
    required int interfaceId,
    required int methodId,
    required List<Capability> paramsCapabilities,
    bool sendResultsToYourself,
  })
  startCallWithAllocatedQuestion;

  /// Starts a deferred-release tracking window for whichever of a call's
  /// params capabilities are same-connection imports freshly created for
  /// it, returning an opaque ticket (`null` if there's nothing to track)
  /// to pass back to [finishParameterCapabilityDisposalTracking] once the call
  /// settles — see [_executeIncomingDispatch]'s own doc comment for why this
  /// tracking exists.
  /// Opaque because setting it up means writing a deferred-release sink
  /// onto each `_ImportedCapability` wrapper, private to
  /// `rpc_capability.dart`'s library.
  final Object? Function(List<Capability> paramsCapabilities)
  startParameterCapabilityDisposalTracking;

  /// Ends the tracking window that
  /// [startParameterCapabilityDisposalTracking] started (a no-op,
  /// reporting `allDisposed` true, for a null ticket) and reports two
  /// independent things: `allDisposed` decides `Return.releaseParamCaps`
  /// directly; `explicitReleaseIds` is exactly the import ids that were
  /// disposed but still need their own wire Release sent — only ever
  /// non-empty when `allDisposed` is false, and can legitimately be
  /// *empty even when `allDisposed` is false* (nothing disposed yet at
  /// all). A genuinely named record (curly-brace syntax) — not just a
  /// positional one with names in the type signature, which wouldn't
  /// actually create `.allDisposed`/`.explicitReleaseIds` getters — so the
  /// two can't be collapsed into one ambiguous empty list at the call site
  /// (see [_finishParameterCapabilityDisposalTrackingAndSendReleases], this
  /// class's own wrapper around this closure that does the actual sending).
  final ({bool allDisposed, List<int> explicitReleaseIds}) Function(
    Object? ticket,
  )
  finishParameterCapabilityDisposalTracking;

  IncomingCallCoordinator({
    required this.exportTable,
    required this.answerTable,
    required this.questions,
    required this.sendBytes,
    required this.disposeIgnoringErrors,
    required this.disposePipelinedTargetIgnoringErrors,
    required this.disposeLeaseForConnectionTeardown,
    required this.isClosed,
    required this.tearDownConnection,
    required this.tryExtractCapabilityReference,
    required this.acquireCapabilityFromWireReference,
    required this.exportResultCapabilityAsWireReference,
    required this.startCallWithAllocatedQuestion,
    required this.startParameterCapabilityDisposalTracking,
    required this.finishParameterCapabilityDisposalTracking,
  });

  void handleBootstrap(RpcMessage msg) {
    if (_rejectDuplicateQuestionId(msg.questionId)) return;
    // Each Bootstrap request hands the peer a new reference to export 0,
    // exactly like ExportTable.retainOrCreateExportId does for capabilities returned
    // from ordinary calls — without this, a peer that bootstraps twice and
    // later disposes just one of the two resulting capabilities would drop
    // this side's refcount to 0 and dispose the capability out from under
    // the peer's other, still-live reference.
    // Register the bootstrap answer so pipelined calls targeting
    // {receiverAnswer: {questionId: msg.questionId, transform: []}} can
    // resolve ptr[0] → the bootstrap capability.
    //
    // Recorded *before* sending the Return — a peer that reacts to the
    // Return through a synchronously-reentrant sink (an in-memory or
    // `sync: true` transport) could otherwise observe a pipelined call
    // targeting this answer, a Finish for it, or a Release for export 0,
    // before this bookkeeping exists (see _executeIncomingDispatch's own matching
    // comment for why the same ordering matters there).
    final bootstrapCap = exportTable.retainExisting(0);
    if (bootstrapCap != null) {
      answerTable.handleAnswerReadyWithoutDispatch(
        msg.questionId,
        resolved: ResolvedAnswer(_bootstrapResultBytes, [bootstrapCap]),
      );
    }
    // Server side: send Return with our bootstrap capability (export 0).
    sendBytes(
      buildBootstrapReturnMessage(answerId: msg.questionId, exportId: 0),
    );
  }

  void handleCall(RpcMessage msg) {
    if (msg.targetIsPromisedAnswer) {
      _handlePipelinedCall(msg);
      return;
    }

    final target = exportTable.getCapability(msg.targetImportId);
    if (target == null) {
      sendBytes(
        buildReturnExceptionMessage(
          answerId: msg.questionId,
          reason: 'unknown export id: ${msg.targetImportId}',
        ),
      );
      return;
    }
    _dispatchToCapability(msg, target);
  }

  void _handlePipelinedCall(RpcMessage msg) {
    final parentQid = msg.targetPromisedAnswerQid;
    // An empty/noop-only transform is normalized to a single hop at pointer
    // slot 0. The only case where a peer legitimately sends one is a
    // promisedAnswer targeting a Bootstrap answer's capability directly —
    // Bootstrap's result has no wrapping wire struct (there's no field to
    // traverse), and this vat's own synthesized _bootstrapResultBytes
    // wrapper always places that capability at ptr slot 0 to match (see
    // handleBootstrap). A real method's result is always a real struct, so
    // a well-behaved peer never sends an empty transform for anything else.
    final path =
        msg.targetTransformPath.isEmpty ? const [0] : msg.targetTransformPath;

    // tryBeginPipelinedDependency is the one atomic check for both "is this
    // even a legal target right now" (unknown/failed/already-Finished-by-
    // the-peer all reject) and, for a still-pending parent, registering
    // this call as a dependent so AnswerTable defers cancellation/export
    // cleanup until it's done with the eventual result — see that method's
    // own doc comment and AnswerTable's `_PipelineDependencyTracker`.
    final dependency = answerTable.tryBeginPipelinedDependency(parentQid);
    switch (dependency) {
      case null:
        sendBytes(
          buildReturnExceptionMessage(
            answerId: msg.questionId,
            reason: 'unknown promisedAnswer questionId: $parentQid',
          ),
        );
        return;
      case ResolvedPipelineDependency(:final resolved):
        _dispatchPipelinedResolved(msg, resolved, path);
        return;
      case PendingPipelineDependency(
        :final parentDispatchResult,
        :final ticket,
      ):
        parentDispatchResult
            .then((resolved) => _dispatchPipelinedResolved(msg, resolved, path))
            .catchError((Object err) {
              // The connection may have torn down while this call sat
              // queued behind the parent dispatch — tearDownConnection
              // already cleared the tables that would otherwise be
              // touched, and sendBytes() below would silently no-op
              // anyway, so there's nothing left to do for a peer that's no
              // longer there.
              if (isClosed()) return;
              sendBytes(
                buildReturnExceptionMessage(
                  answerId: msg.questionId,
                  reason: 'parent call failed: $err',
                ),
              );
            })
            // Ends this dependency exactly once, regardless of whether the
            // parent resolved or failed — after the value/error branch
            // above has already run, so a same-tick export release (see
            // _finalizePipelineDependency) can never race this dependent's
            // own (already-started, by this point) dispatch.
            .whenComplete(() => _finalizePipelineDependency(ticket))
            .ignore();
        return;
    }
  }

  /// Resolves a pipelined call's target capability from its parent's
  /// [resolved] answer and [path], and dispatches to it — shared by both
  /// the already-resolved and the was-still-pending branches of
  /// [_handlePipelinedCall].
  void _dispatchPipelinedResolved(
    RpcMessage msg,
    ResolvedAnswer resolved,
    List<int> path,
  ) {
    if (isClosed()) return;
    final cap = _tryGetCapabilityFromAnswerPath(resolved, path);
    if (cap == null) {
      sendBytes(
        buildReturnExceptionMessage(
          answerId: msg.questionId,
          reason: 'pointer path $path in result struct is not a capability',
        ),
      );
      return;
    }
    _dispatchToCapability(msg, cap, disposeTargetAfterDispatch: true);
  }

  /// Ends a pipelined call's dependency on its parent's answer (see
  /// [AnswerTable.endPipelinedDependency]) and releases whatever result
  /// export ids that call reports are now safe to release — either because
  /// this was the last outstanding dependent and the parent's own
  /// settle-time bookkeeping had already run, or because it's the one that
  /// finally lets an earlier-drained tracker's rendezvous complete.
  void _finalizePipelineDependency(Object ticket) {
    final exportIds = answerTable.endPipelinedDependency(ticket);
    if (exportIds == null || isClosed()) return;
    for (final eid in exportIds) {
      exportTable.releaseReference(eid, disposeIgnoringErrors);
    }
  }

  Capability? _tryGetCapabilityFromAnswerPath(
    ResolvedAnswer resolved,
    List<int> path,
  ) => tryGetCapabilityFromResultPath(
    DispatchResult(
      payload: RpcPayload.fromBytes(resolved.resultBytes),
      caps: resolved.caps,
    ),
    path,
  );

  void _dispatchToCapability(
    RpcMessage msg,
    Capability cap, {
    bool disposeTargetAfterDispatch = false,
  }) {
    void disposeOwnedTarget() {
      if (disposeTargetAfterDispatch) {
        disposePipelinedTargetIgnoringErrors(cap as CapabilityLease);
      }
    }

    final qid = msg.questionId;
    if (_rejectDuplicateQuestionId(qid)) {
      if (disposeTargetAfterDispatch) {
        disposeLeaseForConnectionTeardown(cap as CapabilityLease);
      }
      return;
    }
    final paramsContent = msg.paramsContent;
    final params =
        paramsContent != null
            ? RpcPayload.fromEnvelope(paramsContent)
            : RpcPayload.fromBytes(_emptyResultBytes);

    // Resolve capabilities from the incoming capTable.
    // Each entry in the list must correspond 1-to-1 with the capTable index,
    // because capability pointers in the params struct reference these indices.
    // `none` descriptors get a NullCapability placeholder so subsequent
    // indices remain correct. Unsupported descriptors are protocol errors:
    // silently treating them as null loses information and can change the
    // meaning of an otherwise valid call.
    final paramsCapabilities = <Capability>[];
    try {
      for (final reference in msg.capabilityTableReferences) {
        paramsCapabilities.add(acquireCapabilityFromWireReference(reference));
      }
    } catch (error) {
      // Every entry decoded successfully before whatever failed is a real,
      // live reference (an import refcount bump, a receiverHosted
      // capability lease, ...) — dispose them *before* deciding what to do
      // with the error itself, including the unimplemented/rethrow path below,
      // which tears the whole connection down: tearDownConnection only
      // ever disposes each export's own single `ownedReference` (see that
      // field's doc comment) — it has no way to know about an *additional*
      // lease acquired into a local variable like this one, so leaving one
      // undisposed here would leak a permanent share of that identity's
      // refcount, potentially high enough that its own real capability
      // never actually gets disposed even once every other reference to it
      // (including the export's own) is long gone. A malicious peer could
      // repeat this pattern — one valid entry, then an invalid one — every
      // connection to accumulate exactly such leaked references.
      for (final capability in paramsCapabilities) {
        disposeIgnoringErrors(capability);
      }
      disposeOwnedTarget();

      // A disc this vat doesn't implement at all (e.g. thirdPartyHosted) is
      // a bigger deal than a single bad call — see the `default` case in
      // CapabilityProtocol.acquireCapabilityFromWireReference and the "tears down the
      // connection as unimplemented" test for this exact behavior — so let
      // that kind keep propagating to this listener's own outer try/catch,
      // which tears the whole connection down. Same for anything that
      // isn't even an RpcException: acquireCapabilityFromWireReference itself never
      // throws anything else today, but this being a peer-triggered decode
      // loop, silently downgrading an unexpected failure type to an
      // ordinary per-call Return.exception would be the wrong default.
      if (error is! RpcException || error.kind == ErrorKind.unimplemented) {
        rethrow;
      }

      // Every other decode failure here (e.g. a receiverHosted descriptor
      // naming an export id we don't have) is just this one call's
      // problem: fail only it, with a normal Return.exception, and keep
      // serving the connection.
      //
      // Registers qid as answered — with no result-capability export ids
      // to release later, since this call never reached a real dispatch —
      // so _rejectDuplicateQuestionId can still catch a peer illegally
      // reusing this same qid before sending Finish for it, exactly like
      // every other Return sent without a real dispatch throughout this
      // file (see the sibling `answerTable.handleAnswerReadyWithoutDispatch(qid)`
      // sites).
      answerTable.handleAnswerReadyWithoutDispatch(qid);

      sendBytes(
        buildReturnExceptionMessage(
          answerId: qid,
          reason: error.message,
          // The dispose() calls above already sent a real wire Release for
          // every import successfully resolved before the failing
          // descriptor (see _ImportedCapability.dispose()) — leaving this
          // at its default (true) would additionally tell the peer it
          // doesn't need to send its own Release for those same export
          // ids, so it would apply *both*: its own remoteRefCount would be
          // decremented twice for what was really only one release,
          // potentially tearing its own capability down while some other
          // legitimate reference to it is still outstanding.
          releaseParamCaps: false,
        ),
      );
      return;
    }

    // sendResultsTo=yourself: the peer is asking us to forward this call's
    // real answer onward ourselves (tail call). We never consult
    // tryTailCall for such a call — that would mean chaining a tail call
    // off another tail call, which isn't supported (see doc/rpc.md) — just
    // dispatch normally and answer with resultsSentElsewhere instead of a
    // real Return once it settles.
    final sendResultsToYourself = msg.sendResultsToDisc == 1;
    if (!sendResultsToYourself) {
      final TailCallRequest? tailCallRequest;
      try {
        tailCallRequest = cap.tryTailCall(
          msg.interfaceId,
          msg.methodId,
          params,
          paramsCapabilities: paramsCapabilities,
        );
      } catch (error) {
        disposeOwnedTarget();
        answerTable.handleAnswerReadyWithoutDispatch(qid);
        sendBytes(
          buildReturnExceptionMessage(
            answerId: qid,
            reason: error is CapnpException ? error.message : error.toString(),
            kind: error is CapnpException ? error.kind : ErrorKind.failed,
          ),
        );
        return;
      }
      if (tailCallRequest != null) {
        disposeOwnedTarget();
        _dispatchTailCall(qid, tailCallRequest);
        return;
      }
    }

    _executeIncomingDispatch(
      qid,
      cap,
      msg.interfaceId,
      msg.methodId,
      params,
      paramsCapabilities,
      sendResultsToYourself: sendResultsToYourself,
      disposeTargetAfterDispatch: disposeTargetAfterDispatch,
    );
  }

  /// Handles a [Capability.tryTailCall] result for the call answered by
  /// [qid]. When [tryExtractCapabilityReference] yields an
  /// [ImportedCapabilityReference] for [request]'s target, that target is
  /// an import from this same peer connection, so this applies the Level 1
  /// wire optimization: forwards a new Call (flagged
  /// `sendResultsTo=yourself`) to that peer and answers [qid] immediately
  /// with `takeFromOtherQuestion`, without waiting for the forwarded call
  /// to complete. Otherwise, falls back to a transparent proxy —
  /// dispatching the tail-called method directly and answering [qid]
  /// normally, with no wire-level difference from an ordinary call.
  void _dispatchTailCall(int qid, TailCallRequest request) {
    final target = request.target;
    final reference = tryExtractCapabilityReference(target);
    if (reference is ImportedCapabilityReference) {
      final (forwardQid, sent) = _sendForwardedTailCall(
        reference.importId,
        request,
      );
      // Must wait for the forwarded Call to actually be on the wire before
      // answering qid with takeFromOtherQuestion — otherwise the peer could
      // see the redirect before the call it points at, and fail to
      // correlate it (see resolveLocalAnswer).
      sent
          .then((_) {
            if (isClosed()) return;
            // Nothing was exported directly for this answer — the real
            // result (and any capabilities in it) live under forwardQid's
            // own answer bookkeeping, released independently when the peer
            // finishes that call. Pipelining further off qid itself is not
            // supported: a pipelined call targeting qid will fail with
            // "unknown promisedAnswer questionId", since qid's resolved/
            // pending answer state is deliberately never populated here.
            //
            // Recorded *before* sending the Return — same reentrancy
            // reasoning as _executeIncomingDispatch/handleBootstrap: a peer reacting to
            // this Return through a synchronously-reentrant sink could
            // otherwise send a Finish for qid before this bookkeeping
            // exists, which would then be silently dropped as a no-op
            // instead of ever clearing it.
            answerTable.handleAnswerReadyWithoutDispatch(qid);
            sendBytes(
              buildReturnTakeFromOtherQuestionMessage(
                answerId: qid,
                questionId: forwardQid,
              ),
            );
          })
          .catchError((Object err) {
            if (isClosed()) return;
            answerTable.handleAnswerReadyWithoutDispatch(qid);
            sendBytes(
              buildReturnExceptionMessage(
                answerId: qid,
                reason: err is RpcException ? err.message : err.toString(),
              ),
            );
          });
      return;
    }
    // Not a same-connection import: no wire optimization possible, just
    // dispatch the tail-called method directly and answer qid normally.
    _executeIncomingDispatch(
      qid,
      target,
      request.interfaceId,
      request.methodId,
      request.params,
      request.paramsCapabilities,
    );
  }

  /// Sends a forwarded Call (flagged `sendResultsTo=yourself`) to
  /// [targetImportId]'s peer, as part of applying the tail-call wire
  /// optimization in [_dispatchTailCall]. Returns `(questionId, sent)`,
  /// where [sent] completes once the Call has actually been written to the
  /// outgoing sink — callers must wait for it before answering the original
  /// call with takeFromOtherQuestion, so the peer never observes the
  /// redirect before the call it references.
  ///
  /// The forwarded call's actual outcome is irrelevant to this vat — it's
  /// delivered to whichever of this vat's own outgoing calls the peer
  /// correlates via `takeFromOtherQuestion` (see [resolveLocalAnswer]), not
  /// to us. This just needs to send Finish once any Return arrives, so it
  /// talks to the wire directly via [startCallWithAllocatedQuestion] rather than going through
  /// `OutgoingCallCoordinator.start`/its internal `_awaitAndProcessReturn` (which
  /// expects a real result).
  (int, Future<void>) _sendForwardedTailCall(
    FutureOr<int> targetImportId,
    TailCallRequest request,
  ) {
    final question = questions.allocate();
    final qid = question.id;
    final completer = question.returnCompleter;
    final sentCompleter = question.sentCompleter!;

    // Usually a no-op rollback target: request's params are almost always
    // _ImportedCapability from this same connection, which capTable
    // resolution categorizes as receiverHosted (no export created) — but a
    // receiverHosted-descriptor param on the *original* incoming call
    // resolves to this vat's own capability object (see
    // CapabilityProtocol.acquireCapabilityFromWireReference's
    // ReceiverHostedCapabilityReference case), which
    // *does* get a fresh senderHosted export when forwarded here.
    startCallWithAllocatedQuestion(
      question: question,
      target: ImportedCapabilityTarget(targetImportId),
      params: SerializedParams(request.params.bytes),
      interfaceId: request.interfaceId,
      methodId: request.methodId,
      paramsCapabilities: request.paramsCapabilities,
      sendResultsToYourself: true,
    );

    completer!.future
        .then(
          (_) {
            questions.takeParamExportIds(qid);
            if (isClosed()) return;
            sendBytes(buildFinishMessage(qid, releaseResultCaps: false));
          },
          onError: (Object error, StackTrace stackTrace) {
            questions.takeParamExportIds(qid);
          },
        )
        .ignore();

    return (qid, sentCompleter.future);
  }

  /// Runs [cap]'s dispatch for [interfaceId]/[methodId] and answers [qid]
  /// once it settles. This is [_dispatchToCapability]'s original body,
  /// generalized so it also serves [_dispatchTailCall]'s fallback path and
  /// calls received with `sendResultsTo=yourself` — [sendResultsToYourself]
  /// only changes which kind of Return is sent on completion.
  void _executeIncomingDispatch(
    int qid,
    Capability cap,
    int interfaceId,
    int methodId,
    RpcPayload params,
    List<Capability> paramsCapabilities, {
    bool sendResultsToYourself = false,
    bool disposeTargetAfterDispatch = false,
  }) {
    final cancellation = DispatchCancellationController();

    // Params capabilities freshly imported for this call (see
    // _dispatchToCapability/CapabilityProtocol.acquireCapabilityFromWireReference —
    // every senderHosted/senderPromise entry in the incoming Call's
    // capTable creates a brand new _ImportedCapability wrapper) get a
    // deferred release sink for the lifetime of this dispatch, so
    // Return.releaseParamCaps can be set without an extra wire Release
    // when the callee turns out not to need them past the call — see
    // _finishParameterCapabilityDisposalTrackingAndSendReleases.
    final parameterCapabilityDisposalTicket =
        startParameterCapabilityDisposalTracking(paramsCapabilities);

    final dispatchFuture = Future.sync(
      () => cap.dispatchWithContext(
        interfaceId,
        methodId,
        params,
        paramsCapabilities: paramsCapabilities,
        context: cancellation.context,
      ),
    );

    if (disposeTargetAfterDispatch) {
      // Pipelined-answer lookup acquired cap as an independent lease.
      // Release it when dispatch settles or teardown abandons the dispatch.
      // CapabilityLease.dispose() is idempotent, so these paths may race.
      cancellation.context.canceled
          .then(
            (_) => disposeLeaseForConnectionTeardown(cap as CapabilityLease),
          )
          .ignore();
      dispatchFuture
          .then(
            (_) => disposePipelinedTargetIgnoringErrors(cap as CapabilityLease),
            onError:
                (_) => disposePipelinedTargetIgnoringErrors(
                  cap as CapabilityLease,
                ),
          )
          .ignore();
    }

    // Track the resolved-answer future so pipelined calls can queue behind it.
    // Attach .ignore() to prevent unhandled-rejection if dispatch throws —
    // pipelined callers handle the error via their own catchError.
    final resolvedFuture = dispatchFuture.then(
      (r) => ResolvedAnswer(r.payload.bytes, r.caps),
    );
    resolvedFuture.ignore();
    answerTable.handleDispatchStarted(
      qid,
      resolvedFuture,
      cancellation,
      sendResultsToYourself: sendResultsToYourself,
    );

    dispatchFuture
        .then((result) {
          // The connection was torn down while this dispatch was still
          // running. tearDownConnection already synchronously ran
          // answerTable.tearDown() before this could ever observe
          // isClosed() true (see TwoPartyRpcConnection._tearDown, which
          // sets the closed flag and tears the answer table down with no
          // `await` between the two) — qid's bookkeeping is already gone,
          // so there's nothing left to report here. The result is never
          // sent as a Return, so any capabilities it carries would
          // otherwise never be disposed — dispose them here instead.
          if (isClosed()) {
            _disposeResultCapabilities(result);
            _finishParameterCapabilityDisposalTrackingAndSendReleases(
              parameterCapabilityDisposalTicket,
            );
            return;
          }

          if (sendResultsToYourself) {
            // Results are consumed locally by whichever of the peer's own
            // outgoing calls receives Return.takeFromOtherQuestion=qid —
            // nothing is put on the wire for this Return.
            // The answer table is a non-owning rendezvous point in this path:
            // `_awaitAndProcessReturn()` hands the same local capabilities to the
            // original caller as its DispatchResult, and the later Finish for
            // this forwarded question uses releaseResultCaps=false. Therefore
            // Finish must only drop bookkeeping here, not dispose result.caps.
            //
            // handleDispatchSucceeded() runs *before* sendBytes(): if it
            // ran after (like the plain answerTable.handleAnswerReadyWithoutDispatch()
            // this used to be), qid would sit briefly untracked between the
            // two calls, which a peer that reacts to this very Return
            // through a synchronously-reentrant sink (e.g. an in-memory or
            // `sync: true` transport) could observe — see that method's doc
            // comment.
            final settlement = answerTable.handleDispatchSucceeded(
              qid,
              resolved: ResolvedAnswer(result.payload.bytes, result.caps),
            );
            if (settlement is AnswerAlreadyFinished) {
              _sendCanceledReturn(
                qid,
                parameterCapabilityDisposalTicket,
                result: result,
              );
              return;
            }
            final AnswerSettled(:releaseResultExportsAfterSend) =
                settlement as AnswerSettled;
            sendBytes(buildReturnResultsSentElsewhereMessage(answerId: qid));
            // No Return field exists on this variant to carry
            // releaseParamCaps, so just flush any deferred params
            // releases as ordinary Release messages.
            _finishParameterCapabilityDisposalTrackingAndSendReleases(
              parameterCapabilityDisposalTicket,
            );
            _releaseResultExportsIfAny(releaseResultExportsAfterSend);
            return;
          }

          final resultReferences = <WireCapabilityReference>[];
          for (final c in result.caps) {
            resultReferences.add(exportResultCapabilityAsWireReference(c));
          }
          // No capabilities anywhere in the results means no future
          // message — a peer Finish, or a pipelined call naming this qid —
          // could ever resolve to a usable capability against it, so the
          // peer doesn't need to send Finish for it either. This stays
          // keyed purely on the result's own shape — never on whether the
          // peer already sent an early Finish (see
          // AnswerTable.handleRequesterFinishedAnswer's own doc comment for
          // why those two are kept separate) — so a real peer reading it is
          // never misled about a Return that does carry live capability
          // references. AnswerTable.handleDispatchSucceeded (below) makes
          // this same determination independently, to decide whether to
          // keep tracking this qid at all — this is the wire-facing half of
          // the same fact, computed from the same source.
          final noFinishNeeded = resultReferences.isEmpty;
          // Record the answer before sending — see the comment on the
          // sendResultsToYourself branch above for why the ordering matters.
          // AnswerTable decides for itself, in this same call, whether qid
          // needs to stay tracked at all (see handleDispatchSucceeded's own
          // doc comment) — there is deliberately no separate post-send
          // event for that decision: a caller-visible one would have to
          // re-look-up qid after sendBytes() below, which a synchronously-
          // reentrant peer could have already legally reused for an
          // unrelated new Call by then.
          final settlement = answerTable.handleDispatchSucceeded(
            qid,
            resolved: ResolvedAnswer(result.payload.bytes, result.caps),
            resultExportIds: [
              for (final d in resultReferences)
                if (d
                    case SenderHostedCapabilityReference(:final exportId) ||
                        SenderPromiseCapabilityReference(:final exportId))
                  exportId,
            ],
          );
          if (settlement is AnswerAlreadyFinished) {
            _sendCanceledReturn(
              qid,
              parameterCapabilityDisposalTicket,
              result: result,
            );
            return;
          }
          final AnswerSettled(:releaseResultExportsAfterSend) =
              settlement as AnswerSettled;
          final releaseParamCaps =
              _finishParameterCapabilityDisposalTrackingAndSendReleases(
                parameterCapabilityDisposalTicket,
              );
          // getRootRaw() resolves in place for an envelope- or
          // builder-backed payload (no serialize-then-reparse round trip;
          // see RpcPayload/buildReturnResultsMessageFromReader) and only
          // falls back to parsing bytes for a genuinely bytes-backed one.
          sendBytes(
            buildReturnResultsMessageFromReader(
              answerId: qid,
              resultsRoot: result.payload.getRootRaw(),
              references: resultReferences,
              releaseParamCaps: releaseParamCaps,
              noFinishNeeded: noFinishNeeded,
            ),
          );
          // Only non-null when the peer had already sent an early Finish
          // while pipelined dependents were still outstanding — releasing
          // (if requested) only after the Return above is actually sent,
          // never before, exactly like a real peer Finish would.
          _releaseResultExportsIfAny(releaseResultExportsAfterSend);
        })
        .catchError((Object err) {
          // Same reasoning as the success branch's isClosed() check above:
          // answerTable.tearDown() has already run synchronously by the
          // time this can ever observe isClosed() true, so qid's
          // bookkeeping is already gone — nothing left to report.
          if (isClosed()) {
            _finishParameterCapabilityDisposalTrackingAndSendReleases(
              parameterCapabilityDisposalTicket,
            );
            return;
          }
          final rpcError =
              err is CapnpException
                  ? err
                  : RpcException(err.toString(), kind: ErrorKind.failed);
          // handleDispatchFailed() decides retain-vs-discard for itself,
          // from the sendResultsToYourself fact this dispatch was started
          // with (see handleDispatchStarted) — every failure goes through
          // the same call here, regardless of sendResultsToYourself.
          if (sendResultsToYourself) {
            // See the matching comment in the success branch above for why
            // this runs before sendBytes().
            final settlement = answerTable.handleDispatchFailed(qid, rpcError);
            if (settlement is AnswerAlreadyFinished) {
              _sendCanceledReturn(qid, parameterCapabilityDisposalTicket);
              return;
            }
            final AnswerSettled(:releaseResultExportsAfterSend) =
                settlement as AnswerSettled;
            sendBytes(buildReturnResultsSentElsewhereMessage(answerId: qid));
            _finishParameterCapabilityDisposalTrackingAndSendReleases(
              parameterCapabilityDisposalTicket,
            );
            _releaseResultExportsIfAny(releaseResultExportsAfterSend);
            return;
          }
          final settlement = answerTable.handleDispatchFailed(qid, rpcError);
          if (settlement is AnswerAlreadyFinished) {
            _sendCanceledReturn(qid, parameterCapabilityDisposalTicket);
            return;
          }
          // An ordinary (non-sendResultsToYourself) failure always answers
          // with a normal exception Return — never carries capabilities, so
          // noFinishNeeded is unconditionally true.
          final AnswerSettled(:releaseResultExportsAfterSend) =
              settlement as AnswerSettled;
          final releaseParamCaps =
              _finishParameterCapabilityDisposalTrackingAndSendReleases(
                parameterCapabilityDisposalTicket,
              );
          sendBytes(
            buildReturnExceptionMessage(
              answerId: qid,
              reason: rpcError.message,
              kind: rpcError.kind,
              releaseParamCaps: releaseParamCaps,
              noFinishNeeded: true,
            ),
          );
          _releaseResultExportsIfAny(releaseResultExportsAfterSend);
        });
  }

  /// Answers [qid] with `Return(canceled)` in place of a normal Return,
  /// because its dispatch was accepted as canceled by an early Finish with
  /// no pipelined dependents ever registered for it (see
  /// `AnswerTable.handleRequesterFinishedAnswer`/`handleDispatchSucceeded`/
  /// `handleDispatchFailed`'s "was finished early" outcomes). Disposes
  /// [result]'s capabilities, if any — they were never exported, so this is
  /// the only remaining chance to release them. Deliberately not sent until
  /// here (dispatch-settle time), not the moment Finish arrived: sending it
  /// earlier would let the peer legally reuse [qid] before this vat's own
  /// pending dispatch actually finished with it, and `releaseParamCaps`
  /// isn't knowable until [parameterCapabilityDisposalTicket]'s window
  /// resolves, which only happens once the dispatch settles.
  void _sendCanceledReturn(
    int qid,
    Object? parameterCapabilityDisposalTicket, {
    DispatchResult? result,
  }) {
    if (result != null) _disposeResultCapabilities(result);
    final releaseParamCaps =
        _finishParameterCapabilityDisposalTrackingAndSendReleases(
          parameterCapabilityDisposalTicket,
        );
    if (isClosed()) return;
    sendBytes(
      buildReturnCanceledMessage(
        answerId: qid,
        releaseParamCaps: releaseParamCaps,
      ),
    );
  }

  /// Releases each export id in [exportIds] (a no-op for `null`, or once
  /// the connection has torn down) — shared by every call site that reports
  /// result-export ids to release, whether from a real peer Finish
  /// ([handleFinish]) or from AnswerTable's own dependency-tracker
  /// rendezvous settling late ([_finalizePipelineDependency], or the
  /// `releaseResultExportsAfterSend` case in [_executeIncomingDispatch]).
  void _releaseResultExportsIfAny(List<int>? exportIds) {
    if (exportIds == null || isClosed()) return;
    for (final eid in exportIds) {
      exportTable.releaseReference(eid, disposeIgnoringErrors);
    }
  }

  void handleFinish(RpcMessage msg) {
    final resultExportIds = answerTable.handleRequesterFinishedAnswer(
      msg.questionId,
      releaseResultCaps: msg.releaseResultCaps,
    );
    _releaseResultExportsIfAny(resultExportIds);
  }

  /// Resolves [qid] against this vat's own incoming-answer bookkeeping, for
  /// correlating a `Return.takeFromOtherQuestion` from the peer — the
  /// closure `OutgoingCallCoordinator`'s own `resolveLocalAnswer` field is
  /// wired to (see the wiring site). A thin name-preserving wrapper around
  /// [AnswerTable.takeRedirectedResult] — see that method's own doc
  /// comment for the synchronous-capture requirement, the teardown race,
  /// and the never-throws-synchronously contract, all of which apply here
  /// unchanged.
  Future<ResolvedAnswer> resolveLocalAnswer(int qid) =>
      answerTable.takeRedirectedResult(qid);

  /// If [qid] already has tracked answer-lifecycle state (from Bootstrap or
  /// an in-flight/finished Call — see [AnswerTable.isTracked]), tears the
  /// connection down as a protocol violation and returns true. A
  /// well-behaved peer never reuses a question ID before it has fully
  /// settled (Finish sent and Return received) — if it does anyway,
  /// registering the new dispatch would silently clobber the cancellation
  /// and pending/resolved-answer state for the still-live one, corrupting
  /// cancellation and Return/Finish bookkeeping for both.
  bool _rejectDuplicateQuestionId(int qid) {
    if (!answerTable.isTracked(qid)) return false;
    tearDownConnection(
      RpcException('protocol violation: duplicate incoming question ID $qid'),
    );
    return true;
  }

  /// Disposes every capability in a completed dispatch [result] that will
  /// never be sent to the peer as a Return (the connection closed, or a
  /// Finish arrived and canceled this answer before dispatch finished).
  /// Ownership of `result.caps` passes to the RPC runtime the moment the
  /// dispatch future resolves; if the result isn't going out on the wire,
  /// this is the only remaining chance to release those capabilities.
  ///
  /// These capabilities were never exported (that only happens on the send
  /// path we're skipping here), so there's no refcount to fall back on if
  /// the same capability instance appears more than once in `result.caps` —
  /// each distinct instance is disposed exactly once, by identity, rather
  /// than once per occurrence. A dispose failure on one capability doesn't
  /// stop the rest from being disposed.
  void _disposeResultCapabilities(DispatchResult result) {
    final disposed = HashSet<Capability>.identity();
    for (final cap in result.caps) {
      if (disposed.add(cap)) {
        disposeIgnoringErrors(cap);
      }
    }
  }

  /// Thin wrapper around [finishParameterCapabilityDisposalTracking] that also
  /// sends a wire Release for each reported import ID and forwards
  /// `allDisposed` as `Return.releaseParamCaps` — see
  /// [finishParameterCapabilityDisposalTracking]'s own doc comment for the
  /// actual decision logic.
  bool _finishParameterCapabilityDisposalTrackingAndSendReleases(
    Object? ticket,
  ) {
    final (
      :allDisposed,
      :explicitReleaseIds,
    ) = finishParameterCapabilityDisposalTracking(ticket);
    // sendBytes() would silently no-op post-teardown anyway (a real
    // connection's own send path already does), but this coordinator
    // shouldn't rely on that — skip the send outright rather than depend on
    // a caller's hidden no-op behavior.
    if (!isClosed()) {
      for (final id in explicitReleaseIds) {
        sendBytes(buildReleaseMessage(id, 1));
      }
    }
    return allDisposed;
  }
}
