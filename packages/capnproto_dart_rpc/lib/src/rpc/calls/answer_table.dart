import 'dart:async';
import 'dart:typed_data';

import 'package:capnproto_dart/capnproto_dart.dart';

import '../../capability/capability.dart'
    show Capability, DispatchCancellationController;
import '../rpc_exception.dart';

/// Pointer slots in [resultBytes] map to [caps] indices via a
/// `CapabilityPointer`, not by slot number — `IncomingCallCoordinator` must
/// parse the pointer to resolve a pipelined call against this.
class ResolvedAnswer {
  final Uint8List resultBytes;
  final List<Capability> caps;
  ResolvedAnswer(this.resultBytes, this.caps);
}

/// Outlives the question id it was created under once this vat sends its
/// own Return for it — see [AnswerTable.endPipelinedDependency].
final class _PipelineDependencyTracker {
  int dependentCount = 0;
  bool peerFinished = false;

  /// Only meaningful once [peerFinished] is true.
  bool releaseResultCapsOnFinish = true;

  /// `null` means not yet determined, distinct from "determined, empty".
  List<int>? resultExportIds;

  bool _resultReleaseTaken = false;

  /// Safe to call from either side of the [dependentCount]-draining vs.
  /// [resultExportIds]-recording race, in any order.
  List<int>? tryTakeResultExports() {
    if (!peerFinished) return null;
    if (dependentCount != 0) return null;
    final ids = resultExportIds;
    if (ids == null) return null;
    if (!releaseResultCapsOnFinish) return null;
    if (_resultReleaseTaken) return null;
    _resultReleaseTaken = true;
    return ids;
  }
}

final class _PipelineDependencyTicket {
  final _PipelineDependencyTracker tracker;
  bool ended = false;
  _PipelineDependencyTicket(this.tracker);
}

/// What [AnswerTable.tryBeginPipelinedDependency] found for a promisedAnswer
/// target, exactly one of which applies.
sealed class PipelineDependency {
  const PipelineDependency();
}

/// No queuing, and no matching [AnswerTable.endPipelinedDependency] call
/// needed.
final class ResolvedPipelineDependency extends PipelineDependency {
  final ResolvedAnswer resolved;
  const ResolvedPipelineDependency(this.resolved);
}

/// Call [AnswerTable.endPipelinedDependency] with [ticket] exactly once
/// regardless of outcome — never re-derive the call from the parent's
/// question id, which may already be legally reused by the peer for an
/// unrelated new Call by the time this settles.
final class PendingPipelineDependency extends PipelineDependency {
  final Future<ResolvedAnswer> parentDispatchResult;
  final Object ticket;
  const PendingPipelineDependency(this.parentDispatchResult, this.ticket);
}

/// One incoming question's answer-lifecycle state, exactly one of which
/// applies at a time.
sealed class AnswerState {
  const AnswerState();
}

/// [cancellation] only fires with no pipelined dependents left outstanding
/// — see [AnswerTable.handleRequesterFinishedAnswer].
final class PendingAnswerState extends AnswerState {
  final Future<ResolvedAnswer> dispatchResult;
  final DispatchCancellationController cancellation;
  final _PipelineDependencyTracker _dependents = _PipelineDependencyTracker();
  PendingAnswerState(this.dispatchResult, this.cancellation);
}

/// Installed *before* the Return is sent (see
/// [AnswerTable.handleDispatchSucceeded]), so this does not mean the Return
/// already went out — only that the answer itself is fixed. [resolved] is
/// null for a Return with no result payload of its own (an exception, or a
/// takeFromOtherQuestion forward).
final class AnsweredState extends AnswerState {
  final ResolvedAnswer? resolved;
  final List<int> resultExportIds;
  const AnsweredState({this.resolved, this.resultExportIds = const []});
}

/// The error is retained — rather than just sent as Return.exception and
/// forgotten — so a `takeFromOtherQuestion` racing the failure still
/// observes it instead of "unknown question id".
final class FailedAnswerState extends AnswerState {
  final CapnpException error;
  const FailedAnswerState(this.error);
}

/// Reached only with no pipelined dependents outstanding at Finish time —
/// cancellation was notified immediately. The eventual result is
/// discarded; caller answers with `Return(canceled)` once it settles (see
/// `IncomingCallCoordinator._sendCanceledReturn`).
final class FinishedBeforeCompletionState extends AnswerState {
  const FinishedBeforeCompletionState();
}

/// What [AnswerTable.handleDispatchSucceeded]/[AnswerTable.handleDispatchFailed]
/// need their caller to do about answering [qid], now that its dispatch has
/// settled — exactly one of which applies.
sealed class DispatchSettlement {
  const DispatchSettlement();
}

/// Caller answers with `Return(canceled)` instead of a normal Return (see
/// `IncomingCallCoordinator._sendCanceledReturn`).
final class AnswerAlreadyFinished extends DispatchSettlement {
  const AnswerAlreadyFinished();
}

final class AnswerSettled extends DispatchSettlement {
  /// Only non-null when the peer's own early Finish already determined
  /// this while dependents were still outstanding. Apply only *after* the
  /// Return has actually been sent.
  final List<int>? releaseResultExportsAfterSend;

  const AnswerSettled({this.releaseResultExportsAfterSend});
}

/// The one-shot redirected-result lifecycle for one Answer generation, kept
/// separate from [AnswerState] since it must survive that state's own
/// `Pending` → `Answered`/`Failed` transitions untouched — see
/// [AnswerTable.takeRedirectedResult]. Mirrors `capnp-rpc`'s own
/// `Answer.redirected_results: Option<Promise<...>>`, taken via
/// `Option::take()`, except split into three explicit cases instead of
/// collapsing "never had one" and "already taken" into the same `None`:
/// [AnswerTable.handleRequesterFinishedAnswer] needs to tell those two
/// apart, since only [RedirectedResultTaken] means a live local consumer
/// still needs this dispatch's real result.
sealed class RedirectedResult {
  const RedirectedResult();
}

/// This generation was never `sendResultsToYourself` — nothing ever to
/// take.
final class NoRedirectedResult extends RedirectedResult {
  const NoRedirectedResult();
}

/// Set once, at [AnswerTable.handleDispatchStarted], from
/// `sendResultsToYourself: true` — not yet taken by a correlating
/// `Return.takeFromOtherQuestion`.
final class RedirectedResultAvailable extends RedirectedResult {
  final Future<ResolvedAnswer> result;
  const RedirectedResultAvailable(this.result);
}

/// [AnswerTable.takeRedirectedResult] already transferred the Future this
/// state used to carry to a local consumer, which now holds it directly —
/// a second take is a protocol violation, and (critically) a peer Finish
/// arriving afterward must not cancel the still-running dispatch on this
/// consumer's behalf: it still needs the *real* result, not whatever a
/// cooperative cancellation would produce instead. See
/// [AnswerTable.handleRequesterFinishedAnswer].
final class RedirectedResultTaken extends RedirectedResult {
  const RedirectedResultTaken();
}

/// One incoming question's full tracked state — [state] is the transition-
/// driven half ([AnswerState]'s own sealed hierarchy); [redirectedResult]
/// is independent of those transitions, so a `sendResultsTo=yourself`
/// dispatch's eventual result survives a `Pending` → `Answered`/`Failed`
/// transition intact instead of having to be re-derived from whichever
/// state currently applies.
final class AnswerEntry {
  AnswerState state;
  RedirectedResult redirectedResult;

  AnswerEntry(this.state, {this.redirectedResult = const NoRedirectedResult()});
}

/// Invalid combinations (e.g. a retained error alongside a live dispatch)
/// can't be constructed through this table's API. Doesn't send
/// Returns/Finishes or release exports itself — the caller (today,
/// `IncomingCallCoordinator`) acts on what each method returns.
///
/// [PendingAnswerState.dispatchResult] is **local**: it settles purely
/// from this vat's own capability dispatch, so connection teardown only
/// *requests* cancellation. Once settled into [AnsweredState], waiting for
/// the peer's Finish is **wire-driven** instead, like `QuestionTable`'s
/// Return wait.
class AnswerTable {
  final Map<int, AnswerEntry> _answers = {};

  Object? _tearDownError;
  final List<Completer<Never>> _tearDownWaiters = [];

  // ---------------------------------------------------------------------
  // Query
  // ---------------------------------------------------------------------

  int get count => _answers.length;

  int get cancellationCount =>
      _answers.values.where((e) => e.state is PendingAnswerState).length;

  /// Used to reject a peer illegally reusing a question id before it has
  /// fully settled.
  bool isTracked(int qid) => _answers.containsKey(qid);

  ResolvedAnswer? getResolvedAnswerFor(int qid) {
    final state = _answers[qid]?.state;
    return state is AnsweredState ? state.resolved : null;
  }

  Future<ResolvedAnswer>? getDispatchResultFor(int qid) {
    final state = _answers[qid]?.state;
    return state is PendingAnswerState ? state.dispatchResult : null;
  }

  CapnpException? getDispatchErrorFor(int qid) {
    final state = _answers[qid]?.state;
    return state is FailedAnswerState ? state.error : null;
  }

  // ---------------------------------------------------------------------
  // Event — each method below reports a fact to this table and returns
  // whatever the caller must now do about it; this table (not the caller)
  // decides the consequence from its own current state.
  // ---------------------------------------------------------------------

  /// [sendResultsToYourself]: `true` exactly when this call was itself a
  /// forwarded tail call — sets up [AnswerEntry.redirectedResult] as
  /// [RedirectedResultAvailable] for [takeRedirectedResult] to hand to this
  /// same vat's own correlating `Return.takeFromOtherQuestion`. A real peer
  /// Finish is always still expected for it regardless of whether that
  /// redirect has been taken yet — see [RedirectedResult]'s own doc
  /// comment.
  void handleDispatchStarted(
    int qid,
    Future<ResolvedAnswer> dispatchResult,
    DispatchCancellationController cancellation, {
    bool sendResultsToYourself = false,
  }) {
    _answers[qid] = AnswerEntry(
      PendingAnswerState(dispatchResult, cancellation),
      redirectedResult:
          sendResultsToYourself
              ? RedirectedResultAvailable(dispatchResult)
              : const NoRedirectedResult(),
    );
  }

  /// Call this *before* sending the Return: sending first and recording
  /// this separately would risk a synchronously-delivered Finish, duplicate
  /// Call, or pipelined Call (e.g. over an in-memory or `sync: true`
  /// transport) observing state this table never held.
  DispatchSettlement handleDispatchSucceeded(
    int qid, {
    ResolvedAnswer? resolved,
    List<int> resultExportIds = const [],
  }) {
    final entry = _answers[qid];
    switch (entry?.state) {
      case FinishedBeforeCompletionState():
        _answers.remove(qid);
        return const AnswerAlreadyFinished();
      case PendingAnswerState(:final _dependents) when _dependents.peerFinished:
        _dependents.resultExportIds = resultExportIds;
        final release = _dependents.tryTakeResultExports();
        _answers.remove(qid);
        return AnswerSettled(releaseResultExportsAfterSend: release);
      case PendingAnswerState():
        final stillNeeded =
            entry!.redirectedResult is! NoRedirectedResult ||
            resultExportIds.isNotEmpty ||
            (resolved?.caps.isNotEmpty ?? false);
        if (stillNeeded) {
          entry.state = AnsweredState(
            resolved: resolved,
            resultExportIds: resultExportIds,
          );
        } else {
          _answers.remove(qid);
        }
        return const AnswerSettled();
      default:
        _answers[qid] = AnswerEntry(
          AnsweredState(resolved: resolved, resultExportIds: resultExportIds),
        );
        return const AnswerSettled();
    }
  }

  /// Same atomicity contract as [handleDispatchSucceeded]. A failure has no
  /// peer-visible Finish/pipelining need of its own, so an untaken/not-yet-
  /// taken [AnswerEntry.redirectedResult] is the only reason to retain it
  /// as a [FailedAnswerState] instead of discarding outright — the
  /// discarded case is still an [AnswerSettled] with no export ids to
  /// release, indistinguishable to the caller from the retained one after
  /// the Return has been sent.
  DispatchSettlement handleDispatchFailed(int qid, CapnpException error) {
    final entry = _answers[qid];
    switch (entry?.state) {
      case FinishedBeforeCompletionState():
        _answers.remove(qid);
        return const AnswerAlreadyFinished();
      case PendingAnswerState(:final _dependents) when _dependents.peerFinished:
        _dependents.resultExportIds = const [];
        final release = _dependents.tryTakeResultExports();
        _answers.remove(qid);
        return AnswerSettled(releaseResultExportsAfterSend: release);
      case PendingAnswerState():
        if (entry!.redirectedResult is! NoRedirectedResult) {
          entry.state = FailedAnswerState(error);
        } else {
          _answers.remove(qid);
        }
        return const AnswerSettled();
      default:
        _answers.remove(qid);
        return const AnswerSettled();
    }
  }

  /// So no early-Finish race is possible (contrast [handleDispatchSucceeded],
  /// which must guard against one): a directly-resolved answer such as
  /// Bootstrap's, or a Return with no result payload at all (exception, or
  /// takeFromOtherQuestion forward). Every current caller's Return
  /// genuinely still owes the peer a Finish.
  void handleAnswerReadyWithoutDispatch(
    int qid, {
    ResolvedAnswer? resolved,
    List<int> resultExportIds = const [],
  }) {
    _answers[qid] = AnswerEntry(
      AnsweredState(resolved: resolved, resultExportIds: resultExportIds),
    );
  }

  /// Named for *who* finished, not the Answer's own lifecycle: a
  /// still-outstanding pipelined dependent — or a local consumer that
  /// already holds this generation's [RedirectedResultTaken] result —
  /// can keep this question's bookkeeping (and its still-running dispatch)
  /// alive well past this call: the requester no longer wanting the result
  /// doesn't mean *nothing* still does. Once neither is true, dropping
  /// [qid] here also drops [AnswerEntry.redirectedResult] itself, but only
  /// [RedirectedResultAvailable] can still be sitting there at that point —
  /// an untaken redirect nobody will ever take now, exactly like this
  /// dispatch's discarded result. A [RedirectedResultTaken] one is by
  /// definition already out of this table's hands.
  List<int>? handleRequesterFinishedAnswer(
    int qid, {
    required bool releaseResultCaps,
  }) {
    final entry = _answers[qid];
    switch (entry?.state) {
      case null:
      case FinishedBeforeCompletionState():
        return null;
      case PendingAnswerState(:final _dependents, :final cancellation):
        if (_dependents.peerFinished) return null;
        _dependents.peerFinished = true;
        _dependents.releaseResultCapsOnFinish = releaseResultCaps;
        final hasLiveRedirectConsumer =
            entry!.redirectedResult is RedirectedResultTaken;
        if (_dependents.dependentCount == 0 && !hasLiveRedirectConsumer) {
          entry.state = const FinishedBeforeCompletionState();
          cancellation.cancel();
        }
        return null;
      case AnsweredState(:final resultExportIds):
        _answers.remove(qid);
        return releaseResultCaps ? resultExportIds : null;
      case FailedAnswerState():
        _answers.remove(qid);
        return null;
    }
  }

  /// Takes [qid]'s not-yet-taken redirected result, if any — the local
  /// counterpart to `capnp-rpc`'s `Answer.redirected_results.take()`.
  /// One-shot: a second call for the same still-tracked generation (or a
  /// [qid] that was never `sendResultsToYourself` in the first place) gets
  /// the same "nothing to take" error as an unknown question id, since
  /// both are protocol violations from a well-behaved peer's perspective.
  ///
  /// Must be called synchronously, as the correlating `takeFromOtherQuestion`
  /// Return is first processed (see `OutgoingCallCoordinator.handleReturn`),
  /// not deferred to a later continuation: this reads [qid]'s *current*
  /// generation once, right here — a real Finish or the peer legally
  /// reusing [qid] for an unrelated new Call afterward can no longer affect
  /// the `Future` this returns. Nothing past this call ever needs to
  /// re-look-up [qid] again for it.
  ///
  /// A still-pending redirected result is raced against connection
  /// teardown via [failOnTearDown] rather than returned directly: dispatch
  /// cancellation on teardown is only ever a cooperative request the
  /// dispatch is free to ignore (see that method's own doc comment), so
  /// without this race, a caller correlating a tail-called dispatch
  /// through this method could observe it succeed later from purely local
  /// state even though the connection that correlated it is already gone
  /// — unlike every other still-pending call on this connection (see issue
  /// #99). An already-settled redirected result needs no such race: it
  /// already exists, independent of the connection's own health.
  ///
  /// Never throws synchronously — always returns a Future, even for the
  /// error cases — since `OutgoingCallCoordinator.handleReturn` calls this
  /// (via `IncomingCallCoordinator.resolveLocalAnswer`) from ordinary
  /// (non-`async`) code: a synchronous throw there would escape before the
  /// completer it's about to feed even gets constructed, leaving that
  /// completer (and whoever is awaiting it) hanging forever.
  Future<ResolvedAnswer> takeRedirectedResult(int qid) {
    final entry = _answers[qid];
    if (entry == null) {
      return Future.error(
        RpcException(
          'takeFromOtherQuestion referenced unknown question id $qid',
        ),
      );
    }
    final redirected = entry.redirectedResult;
    if (redirected is! RedirectedResultAvailable) {
      return Future.error(
        RpcException(
          'takeFromOtherQuestion referenced a call that did not use '
          'sendResultsTo.yourself, or whose result was already taken',
        ),
      );
    }
    entry.redirectedResult = const RedirectedResultTaken();
    return entry.state is PendingAnswerState
        ? failOnTearDown(redirected.result)
        : redirected.result;
  }

  // ---------------------------------------------------------------------
  // Lifecycle (table-wide, not per-qid)
  // ---------------------------------------------------------------------

  /// Test/introspection only: should return to `0` after every
  /// [failOnTearDown] race settles, whichever side wins.
  int get pendingTearDownRegistrationCount => _tearDownWaiters.length;

  /// [operation] itself keeps running either way; cancellation
  /// (`DispatchCancellationController.cancel`) is only ever a cooperative
  /// request a dispatch is free to ignore, so this is the only way to stop
  /// *waiting* on it.
  ///
  /// Used where a dispatch's settlement is awaited directly, bypassing this
  /// table's own Return/Finish bookkeeping — today, only
  /// [takeRedirectedResult], resolving a `takeFromOtherQuestion` for a
  /// still-pending tail-called dispatch. Without it, that caller would
  /// observe [operation] succeed from purely local state even after the
  /// connection is gone (issue #99, matching `QuestionTable.tearDown`).
  ///
  /// Deliberately has no caller-visible "watch" handle to cancel — that
  /// would risk a leaked registration per call if a caller forgot.
  Future<T> failOnTearDown<T>(Future<T> operation) {
    final error = _tearDownError;
    if (error != null) return Future.error(error);
    final waiter = Completer<Never>();
    _tearDownWaiters.add(waiter);
    return Future.any<T>([
      operation,
      waiter.future,
    ]).whenComplete(() => _tearDownWaiters.remove(waiter));
  }

  /// Fails every outstanding *and future* [failOnTearDown] race with
  /// [error] — called once when the owning connection tears down.
  void tearDown(Object error) {
    for (final entry in _answers.values) {
      if (entry.state case PendingAnswerState(:final cancellation)) {
        cancellation.cancel();
      }
    }
    _answers.clear();
    if (_tearDownError == null) {
      _tearDownError = error;
      for (final waiter in _tearDownWaiters) {
        waiter.completeError(error);
      }
      _tearDownWaiters.clear();
    }
  }

  // ---------------------------------------------------------------------
  // Pipelining (pipeline-dependency tracking)
  // ---------------------------------------------------------------------

  /// Registers this call as a dependent if [qid]'s dispatch is still
  /// running, deferring its cancellation/export cleanup — pairs with
  /// [endPipelinedDependency] (see [PendingPipelineDependency]'s own doc
  /// for the exactly-once contract).
  PipelineDependency? tryBeginPipelinedDependency(int qid) {
    final state = _answers[qid]?.state;
    switch (state) {
      case AnsweredState(:final resolved):
        return resolved != null ? ResolvedPipelineDependency(resolved) : null;
      case PendingAnswerState(:final dispatchResult, :final _dependents):
        if (_dependents.peerFinished) return null;
        _dependents.dependentCount++;
        return PendingPipelineDependency(
          dispatchResult,
          _PipelineDependencyTicket(_dependents),
        );
      case FailedAnswerState():
      case FinishedBeforeCompletionState():
      case null:
        return null;
    }
  }

  /// [ticket] carries its own reference, so this remains correct even if
  /// the owning qid has since been reused — see
  /// [_PipelineDependencyTracker.tryTakeResultExports] for when this
  /// returns non-null.
  List<int>? endPipelinedDependency(Object ticket) {
    final t = ticket as _PipelineDependencyTicket;
    if (t.ended) {
      throw StateError('pipeline dependency ticket already ended');
    }
    t.ended = true;
    final tracker = t.tracker;
    tracker.dependentCount--;
    assert(tracker.dependentCount >= 0);
    return tracker.tryTakeResultExports();
  }
}
