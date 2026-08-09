import 'dart:async';
import 'dart:typed_data';

import 'package:capnproto_dart/capnproto_dart.dart';
import 'package:capnproto_dart_rpc/src/capability/capability.dart';
import 'package:capnproto_dart_rpc/src/rpc/calls/answer_table.dart';
import 'package:capnproto_dart_rpc/src/rpc/rpc_exception.dart';
import 'package:test/test.dart';

ResolvedAnswer _answer([List<Capability> caps = const []]) =>
    ResolvedAnswer(Uint8List(0), caps);

const _tornDownProbeError = CapnpException('connection torn down (probe)');

/// Installs a genuinely retained [FailedAnswerState] for [qid] — the real
/// two-call path (`handleDispatchStarted(sendResultsToYourself: true)` then
/// `handleDispatchFailed`) a `sendResultsToYourself` dispatch takes, used
/// here as a fixture wherever a test just needs a failed, tracked answer to
/// already exist. Completes [pending] with [error] too — the same
/// underlying settlement `handleDispatchFailed`'s own [error] argument
/// reports, in production — so [AnswerEntry.redirectedResult] (the same
/// Future) is actually observable rather than hanging forever.
void _installFailedAnswer(AnswerTable table, int qid, CapnpException error) {
  final pending = Completer<ResolvedAnswer>();
  pending.future.ignore();
  table.handleDispatchStarted(
    qid,
    pending.future,
    DispatchCancellationController(),
    sendResultsToYourself: true,
  );
  pending.completeError(error);
  table.handleDispatchFailed(qid, error);
}

void main() {
  group('AnswerTable', () {
    test('Finish arriving while dispatch is still pending with no '
        'pipelined dependents: handleRequesterFinishedAnswer() returns null, marks the '
        'answer finished, and cancels the live dispatch — the eventual '
        'dispatch result must then be dropped instead of resurrecting '
        'answer state', () async {
      final table = AnswerTable();
      final pending = Completer<ResolvedAnswer>();
      final cancellation = DispatchCancellationController();
      pending.future.ignore();
      table.handleDispatchStarted(1, pending.future, cancellation);

      final resultExportIds = table.handleRequesterFinishedAnswer(
        1,
        releaseResultCaps: true,
      );
      expect(resultExportIds, isNull);
      expect(cancellation.context.isCanceled, isTrue);
      expect(
        table.isTracked(1),
        isTrue,
        reason: 'finished-before-completion is still tracked',
      );

      // The dispatch eventually settles — _executeIncomingDispatch calls
      // handleDispatchSucceeded()/handleDispatchFailed() either way to
      // both drop dispatch-in-flight bookkeeping and learn whether Finish
      // already arrived, in one call.
      expect(
        table.handleDispatchFailed(1, const CapnpException('boom')),
        isA<AnswerAlreadyFinished>(),
      );
      expect(table.isTracked(1), isFalse);
      // A second clear for the same qid must not resurrect anything, and
      // must not report "finished early" again now that it's untracked.
      expect(
        table.handleDispatchFailed(1, const CapnpException('boom')),
        isA<AnswerSettled>(),
      );
    });

    test('a second Finish for a qid already marked finished-before-'
        'completion is a no-op — it must not double-cancel the dispatch '
        'or resurrect any state', () {
      final table = AnswerTable();
      final pending = Completer<ResolvedAnswer>();
      final cancellation = DispatchCancellationController();
      pending.future.ignore();
      table.handleDispatchStarted(1, pending.future, cancellation);

      expect(
        table.handleRequesterFinishedAnswer(1, releaseResultCaps: true),
        isNull,
      );
      expect(
        table.handleRequesterFinishedAnswer(1, releaseResultCaps: true),
        isNull,
      );
      expect(table.isTracked(1), isTrue);
      expect(
        table.handleDispatchFailed(1, const CapnpException('boom')),
        isA<AnswerAlreadyFinished>(),
      );
    });

    test('Finish for an unknown qid is a no-op returning null', () {
      final table = AnswerTable();
      expect(
        table.handleRequesterFinishedAnswer(42, releaseResultCaps: true),
        isNull,
      );
    });

    test('handleDispatchSucceeded drops a successful answer immediately '
        'when nothing needs it retained: no result capabilities, no fresh '
        'export ids, and not started with sendResultsToYourself — decided '
        'in this same call, with no separate post-install event needed', () {
      final table = AnswerTable();
      final pending = Completer<ResolvedAnswer>();
      final cancellation = DispatchCancellationController();
      pending.future.ignore();
      table.handleDispatchStarted(1, pending.future, cancellation);

      table.handleDispatchSucceeded(1, resolved: _answer());

      expect(table.isTracked(1), isFalse);
      expect(cancellation.context.isCanceled, isFalse);
    });

    test('handleDispatchSucceeded keeps a successful answer tracked when '
        'its result carries capabilities of its own, even with no fresh '
        'export ids (e.g. a receiverHosted capability handed back to the '
        'peer) — pipelining can still target it, and a real Finish is '
        'still expected', () {
      final table = AnswerTable();
      final pending = Completer<ResolvedAnswer>();
      pending.future.ignore();
      table.handleDispatchStarted(
        1,
        pending.future,
        DispatchCancellationController(),
      );

      table.handleDispatchSucceeded(1, resolved: _answer([NullCapability()]));

      expect(table.isTracked(1), isTrue);
      expect(
        table.handleRequesterFinishedAnswer(1, releaseResultCaps: true),
        isEmpty,
        reason: 'no result export ids of its own to release',
      );
    });

    test('handleDispatchSucceeded keeps a successful answer tracked when '
        'the dispatch was started with sendResultsToYourself: true, even '
        'with a capability-free result — regression test for review point '
        '6 on PR #117: a sendResultsToYourself dispatch always answers via '
        'resultsSentElsewhere, which (unlike the ordinary results/exception '
        'Return) has no noFinishNeeded optimization at all, so a real '
        'Finish is always still expected regardless of the result content', () {
      final table = AnswerTable();
      final pending = Completer<ResolvedAnswer>();
      pending.future.ignore();
      table.handleDispatchStarted(
        1,
        pending.future,
        DispatchCancellationController(),
        sendResultsToYourself: true,
      );

      table.handleDispatchSucceeded(1, resolved: _answer());

      expect(
        table.isTracked(1),
        isTrue,
        reason: 'this vat may still need it via resolveLocalAnswer later',
      );
    });

    test('an answer installed via handleAnswerReadyWithoutDispatch always '
        'stays tracked, regardless of capabilities — regression test for '
        'the hidden precondition PR #117 review originally flagged: unlike '
        'handleDispatchSucceeded, none of this method\'s current callers '
        '(e.g. a decode-failure exception Return) ever set '
        'Return.noFinishNeeded, so a real Finish is always still expected', () {
      final table = AnswerTable();
      table.handleAnswerReadyWithoutDispatch(1);
      expect(
        table.isTracked(1),
        isTrue,
        reason: 'still genuinely awaiting a real peer Finish',
      );
    });

    test('handleDispatchSucceeded installs the answer atomically for a '
        'live dispatch: qid is never observably untracked, unlike a '
        '"check whether finished early, then record the answer" pattern '
        'split across two separate calls', () {
      final table = AnswerTable();
      final pending = Completer<ResolvedAnswer>();
      pending.future.ignore();
      table.handleDispatchStarted(
        1,
        pending.future,
        DispatchCancellationController(),
      );

      final resolved = _answer();
      final recorded = table.handleDispatchSucceeded(
        1,
        resolved: resolved,
        resultExportIds: [7],
      );
      expect(
        recorded,
        isA<AnswerSettled>().having(
          (r) => r.releaseResultExportsAfterSend,
          'releaseResultExportsAfterSend',
          isNull,
        ),
      );
      expect(table.isTracked(1), isTrue);
      expect(table.getResolvedAnswerFor(1), same(resolved));
      expect(
        table.getDispatchResultFor(1),
        isNull,
        reason: 'no longer pending',
      );
      expect(
        table.handleRequesterFinishedAnswer(1, releaseResultCaps: true),
        equals([7]),
      );
    });

    test('handleDispatchSucceeded for a qid finished early with no pipelined '
        'dependents: returns AnswerAlreadyFinished, drops every trace of '
        'it, and does not record the answer — the caller must discard the '
        'dispatch result instead', () {
      final table = AnswerTable();
      final pending = Completer<ResolvedAnswer>();
      pending.future.ignore();
      table.handleDispatchStarted(
        1,
        pending.future,
        DispatchCancellationController(),
      );
      expect(
        table.handleRequesterFinishedAnswer(1, releaseResultCaps: true),
        isNull,
      ); // Finish arrives before dispatch settles.

      final recorded = table.handleDispatchSucceeded(1, resolved: _answer());
      expect(recorded, isA<AnswerAlreadyFinished>());
      expect(table.isTracked(1), isFalse);
      expect(table.getResolvedAnswerFor(1), isNull);
    });

    test('handleDispatchFailed installs the retained error atomically '
        'for a live dispatch started with sendResultsToYourself: true', () {
      final table = AnswerTable();
      final pending = Completer<ResolvedAnswer>();
      pending.future.ignore();
      table.handleDispatchStarted(
        2,
        pending.future,
        DispatchCancellationController(),
        sendResultsToYourself: true,
      );

      final error = const CapnpException('boom');
      final recorded = table.handleDispatchFailed(2, error);
      expect(
        recorded,
        isA<AnswerSettled>().having(
          (r) => r.releaseResultExportsAfterSend,
          'releaseResultExportsAfterSend',
          isNull,
        ),
      );
      expect(table.getDispatchErrorFor(2), same(error));
      // A retained error never carries result capabilities, so there is
      // nothing for a Finish to release, regardless of releaseResultCaps.
      expect(
        table.handleRequesterFinishedAnswer(2, releaseResultCaps: true),
        isNull,
      );
    });

    test('handleDispatchFailed discards the failure outright when the '
        'dispatch was started without sendResultsToYourself (the '
        'default): nothing is recorded, and a later takeFromOtherQuestion '
        'correlation finds nothing to observe', () {
      final table = AnswerTable();
      final pending = Completer<ResolvedAnswer>();
      pending.future.ignore();
      table.handleDispatchStarted(
        2,
        pending.future,
        DispatchCancellationController(),
      );

      final recorded = table.handleDispatchFailed(
        2,
        const CapnpException('boom'),
      );
      expect(recorded, isA<AnswerSettled>());
      expect(table.isTracked(2), isFalse);
      expect(table.getDispatchErrorFor(2), isNull);
    });

    test('handleDispatchFailed for a qid finished early with no '
        'pipelined dependents: returns AnswerAlreadyFinished and does not '
        'retain the error', () {
      final table = AnswerTable();
      final pending = Completer<ResolvedAnswer>();
      pending.future.ignore();
      table.handleDispatchStarted(
        2,
        pending.future,
        DispatchCancellationController(),
      );
      expect(
        table.handleRequesterFinishedAnswer(2, releaseResultCaps: true),
        isNull,
      );

      final recorded = table.handleDispatchFailed(
        2,
        const CapnpException('boom'),
      );
      expect(recorded, isA<AnswerAlreadyFinished>());
      expect(table.isTracked(2), isFalse);
      expect(table.getDispatchErrorFor(2), isNull);
    });

    test('Finish arriving for a qid with no pending dispatch (already '
        'resolved) returns its recorded result export ids and clears the '
        'resolved answer', () {
      final table = AnswerTable();
      table.handleAnswerReadyWithoutDispatch(
        2,
        resolved: _answer(),
        resultExportIds: [10, 11],
      );

      final resultExportIds = table.handleRequesterFinishedAnswer(
        2,
        releaseResultCaps: true,
      );
      expect(resultExportIds, equals([10, 11]));
      expect(table.getResolvedAnswerFor(2), isNull);
      expect(table.isTracked(2), isFalse);
    });

    test('Finish with releaseResultCaps=false for an already-resolved qid '
        'reports nothing to release, but still clears the answer', () {
      final table = AnswerTable();
      table.handleAnswerReadyWithoutDispatch(
        2,
        resolved: _answer(),
        resultExportIds: [10, 11],
      );

      expect(
        table.handleRequesterFinishedAnswer(2, releaseResultCaps: false),
        isNull,
      );
      expect(table.isTracked(2), isFalse);
    });

    test('Finish for a failed-and-retained answer reports nothing to '
        'release (a failure never carries result capabilities) and drops '
        'the retained error', () {
      final table = AnswerTable();
      _installFailedAnswer(table, 2, const CapnpException('boom'));
      expect(table.getDispatchErrorFor(2), isNotNull);

      final resultExportIds = table.handleRequesterFinishedAnswer(
        2,
        releaseResultCaps: true,
      );
      expect(resultExportIds, isNull);
      expect(table.getDispatchErrorFor(2), isNull);
      expect(table.isTracked(2), isFalse);
    });

    test('duplicate question id: isTracked reports true for every distinct '
        'answer-lifecycle state a peer could illegally collide a fresh Call '
        'against', () {
      final table = AnswerTable();
      expect(table.isTracked(3), isFalse);

      final pending = Completer<ResolvedAnswer>();
      pending.future.ignore();
      table.handleDispatchStarted(
        3,
        pending.future,
        DispatchCancellationController(),
      );
      expect(table.isTracked(3), isTrue, reason: 'pending answer');
      expect(
        table.handleDispatchFailed(3, const CapnpException('boom')),
        isA<AnswerSettled>(),
      );
      expect(table.isTracked(3), isFalse);

      table.handleAnswerReadyWithoutDispatch(3, resolved: _answer());
      expect(table.isTracked(3), isTrue, reason: 'resolved, awaiting Finish');
      table.handleRequesterFinishedAnswer(3, releaseResultCaps: true);
      expect(table.isTracked(3), isFalse);

      _installFailedAnswer(table, 3, const CapnpException('boom'));
      expect(table.isTracked(3), isTrue, reason: 'failed answer retained');
    });

    test(
      'handleAnswerReadyWithoutDispatch with no resolved answer still tracks the qid '
      '(so duplicate reuse is rejected) but reports no pipelining data',
      () {
        final table = AnswerTable();
        table.handleAnswerReadyWithoutDispatch(4);
        expect(table.isTracked(4), isTrue);
        expect(table.getResolvedAnswerFor(4), isNull);
        expect(
          table.handleRequesterFinishedAnswer(4, releaseResultCaps: true),
          equals(const []),
        );
      },
    );

    test(
      'getResolvedAnswerFor/getDispatchResultFor/getDispatchErrorFor each only report data for '
      'their own state, null for every other state and for an unknown '
      'qid',
      () {
        final table = AnswerTable();
        final pending = Completer<ResolvedAnswer>();
        pending.future.ignore();

        table.handleDispatchStarted(
          1,
          pending.future,
          DispatchCancellationController(),
        );
        expect(table.getDispatchResultFor(1), same(pending.future));
        expect(table.getResolvedAnswerFor(1), isNull);
        expect(table.getDispatchErrorFor(1), isNull);

        final resolved = _answer();
        table.handleAnswerReadyWithoutDispatch(2, resolved: resolved);
        expect(table.getResolvedAnswerFor(2), same(resolved));
        expect(table.getDispatchResultFor(2), isNull);
        expect(table.getDispatchErrorFor(2), isNull);

        final error = const CapnpException('boom');
        _installFailedAnswer(table, 3, error);
        expect(table.getDispatchErrorFor(3), same(error));
        expect(table.getResolvedAnswerFor(3), isNull);
        expect(table.getDispatchResultFor(3), isNull);

        expect(table.getResolvedAnswerFor(999), isNull);
        expect(table.getDispatchResultFor(999), isNull);
        expect(table.getDispatchErrorFor(999), isNull);
      },
    );

    test('count is the number of distinct tracked qids, regardless of which '
        'state each one is in', () {
      final table = AnswerTable();
      table.handleAnswerReadyWithoutDispatch(1, resolved: _answer());
      _installFailedAnswer(table, 2, const CapnpException('x'));
      expect(table.count, equals(2));
    });

    test('cancellationCount reflects only live in-flight cancellation '
        'controllers, dropped once the dispatch settles', () {
      final table = AnswerTable();
      final pending = Completer<ResolvedAnswer>();
      pending.future.ignore();
      table.handleDispatchStarted(
        1,
        pending.future,
        DispatchCancellationController(),
      );
      expect(table.cancellationCount, equals(1));
      table.handleDispatchFailed(1, const CapnpException('x'));
      expect(table.cancellationCount, equals(0));
    });

    test('tearDown cancels every live dispatch, clears all tracked state, '
        'and makes a fresh failOnTearDown call fail with the given error '
        'immediately', () async {
      final table = AnswerTable();
      final pending = Completer<ResolvedAnswer>();
      pending.future.ignore();
      final cancellation = DispatchCancellationController();
      table.handleDispatchStarted(1, pending.future, cancellation);
      table.handleAnswerReadyWithoutDispatch(2, resolved: _answer());
      _installFailedAnswer(table, 3, const CapnpException('x'));

      final error = const CapnpException('connection torn down');
      table.tearDown(error);
      expect(cancellation.context.isCanceled, isTrue);
      expect(table.count, equals(0));
      expect(table.cancellationCount, equals(0));
      // tearDown() already ran, so failOnTearDown resolves the "already
      // torn down" branch immediately — safe to await directly here
      // (unlike a real caller racing a still-pending operation, which must
      // call failOnTearDown *before* whatever triggers tearDown runs).
      final neverCompletes = Completer<ResolvedAnswer>().future..ignore();
      await expectLater(
        table.failOnTearDown(neverCompletes),
        throwsA(same(error)),
      );
    });

    group('failOnTearDown registration lifecycle (issue #113 review)', () {
      test('resolves with the operation\'s own result when it wins the '
          'race, and does not leave a registration behind', () async {
        final table = AnswerTable();
        final operation = Completer<ResolvedAnswer>();
        final resolved = _answer();

        final raced = table.failOnTearDown(operation.future);
        expect(table.pendingTearDownRegistrationCount, equals(1));

        operation.complete(resolved);
        expect(await raced, same(resolved));
        expect(
          table.pendingTearDownRegistrationCount,
          equals(0),
          reason:
              'the registration must be cleaned up once the operation '
              'itself wins the race, not just when tearDown wins it',
        );
      });

      test('does not accumulate registrations across many successful '
          'races — the resource leak this API replaced a caller-managed '
          '"watch" handle to avoid', () async {
        final table = AnswerTable();
        for (var i = 0; i < 100; i++) {
          final operation = Completer<ResolvedAnswer>();
          final raced = table.failOnTearDown(operation.future);
          operation.complete(_answer());
          await raced;
        }
        expect(table.pendingTearDownRegistrationCount, equals(0));
      });

      test('fails with the teardown error when tearDown wins the race, and '
          'does not leave a registration behind either — the operation '
          'itself is left running, uninterrupted, in the background', () async {
        final table = AnswerTable();
        final operation = Completer<ResolvedAnswer>();
        operation.future.ignore();

        final raced = table.failOnTearDown(operation.future);
        expect(table.pendingTearDownRegistrationCount, equals(1));

        table.tearDown(_tornDownProbeError);
        expect(
          table.pendingTearDownRegistrationCount,
          equals(0),
          reason: 'tearDown itself clears every outstanding registration',
        );
        await expectLater(raced, throwsA(same(_tornDownProbeError)));

        // The operation was never touched by any of this — it can still
        // complete normally later, exactly as issue #99 requires.
        final resolved = _answer();
        operation.complete(resolved);
        expect(await operation.future, same(resolved));
      });
    });

    group('pipelined dependents (issue #109)', () {
      test('tryBeginPipelinedDependency resolves immediately for an '
          'already-answered qid, registers a dependent for a still-pending '
          'one, and returns null for anything else (unknown, failed, or an '
          'exception answer with no resolved payload of its own)', () {
        final table = AnswerTable();

        final resolved = _answer();
        table.handleAnswerReadyWithoutDispatch(1, resolved: resolved);
        final resolvedDependency = table.tryBeginPipelinedDependency(1);
        expect(
          resolvedDependency,
          isA<ResolvedPipelineDependency>().having(
            (d) => d.resolved,
            'resolved',
            same(resolved),
          ),
        );

        final pending = Completer<ResolvedAnswer>();
        pending.future.ignore();
        table.handleDispatchStarted(
          2,
          pending.future,
          DispatchCancellationController(),
        );
        final pendingDependency = table.tryBeginPipelinedDependency(2);
        expect(pendingDependency, isA<PendingPipelineDependency>());
        expect(
          (pendingDependency as PendingPipelineDependency).parentDispatchResult,
          same(pending.future),
        );

        table.handleAnswerReadyWithoutDispatch(
          3,
        ); // no resolved payload of its own
        expect(table.tryBeginPipelinedDependency(3), isNull);

        _installFailedAnswer(table, 4, const CapnpException('boom'));
        expect(table.tryBeginPipelinedDependency(4), isNull);

        expect(table.tryBeginPipelinedDependency(999), isNull);
      });

      test('Finish arriving while an outstanding pipelined dependent still '
          'depends on the pending answer does not cancel the dispatch, and '
          'rejects any further pipelined call registered after the Finish', () {
        final table = AnswerTable();
        final pending = Completer<ResolvedAnswer>();
        final cancellation = DispatchCancellationController();
        pending.future.ignore();
        table.handleDispatchStarted(1, pending.future, cancellation);

        expect(
          table.tryBeginPipelinedDependency(1),
          isA<PendingPipelineDependency>(),
        );

        final resultExportIds = table.handleRequesterFinishedAnswer(
          1,
          releaseResultCaps: true,
        );
        expect(resultExportIds, isNull);
        expect(cancellation.context.isCanceled, isFalse);
        expect(table.isTracked(1), isTrue);

        // No new pipelined call may target this answer once Finish arrived,
        // even though it still has a live dependent from before the Finish.
        expect(table.tryBeginPipelinedDependency(1), isNull);

        // A second Finish is a no-op — must not double up anything.
        expect(
          table.handleRequesterFinishedAnswer(1, releaseResultCaps: true),
          isNull,
        );
        expect(cancellation.context.isCanceled, isFalse);
      });

      test('Ordering A — every dependent drains only after the parent '
          'settles: export ids are released exactly once, on the last '
          'dependent to drain', () {
        final table = AnswerTable();
        final pending = Completer<ResolvedAnswer>();
        final cancellation = DispatchCancellationController();
        pending.future.ignore();
        table.handleDispatchStarted(1, pending.future, cancellation);

        final dep1 =
            table.tryBeginPipelinedDependency(1) as PendingPipelineDependency;
        final dep2 =
            table.tryBeginPipelinedDependency(1) as PendingPipelineDependency;

        expect(
          table.handleRequesterFinishedAnswer(1, releaseResultCaps: true),
          isNull,
        );
        expect(cancellation.context.isCanceled, isFalse);

        final recorded = table.handleDispatchSucceeded(
          1,
          resolved: _answer(),
          resultExportIds: [7],
        );
        expect(
          recorded,
          isA<AnswerSettled>().having(
            (r) => r.releaseResultExportsAfterSend,
            'releaseResultExportsAfterSend',
            isNull,
          ),
          reason: 'dependents are still outstanding',
        );
        expect(
          table.isTracked(1),
          isFalse,
          reason:
              'qid is freed immediately once the Return is sent — a '
              'second Finish will never arrive to do it',
        );

        expect(
          table.endPipelinedDependency(dep1.ticket),
          isNull,
          reason: 'not the last dependent',
        );
        expect(table.endPipelinedDependency(dep2.ticket), equals([7]));
      });

      test('Ordering B — every dependent drains before the parent settles: '
          'export ids are still released exactly once, once the parent '
          'finally records them (not a leak just because dependentCount '
          'reached 0 first)', () {
        final table = AnswerTable();
        final pending = Completer<ResolvedAnswer>();
        final cancellation = DispatchCancellationController();
        pending.future.ignore();
        table.handleDispatchStarted(1, pending.future, cancellation);

        final dep1 =
            table.tryBeginPipelinedDependency(1) as PendingPipelineDependency;
        final dep2 =
            table.tryBeginPipelinedDependency(1) as PendingPipelineDependency;

        expect(
          table.handleRequesterFinishedAnswer(1, releaseResultCaps: true),
          isNull,
        );

        expect(table.endPipelinedDependency(dep1.ticket), isNull);
        expect(
          table.endPipelinedDependency(dep2.ticket),
          isNull,
          reason: 'resultExportIds not known yet — not a leak',
        );

        final recorded = table.handleDispatchSucceeded(
          1,
          resolved: _answer(),
          resultExportIds: [7],
        );
        expect(
          recorded,
          isA<AnswerSettled>().having(
            (r) => r.releaseResultExportsAfterSend,
            'releaseResultExportsAfterSend',
            equals([7]),
          ),
        );
      });

      test('releaseResultCaps=false on the early Finish: export ids are '
          'never produced by either side of the rendezvous, regardless of '
          'drain order', () {
        for (final drainBeforeSettle in [false, true]) {
          final table = AnswerTable();
          final pending = Completer<ResolvedAnswer>();
          pending.future.ignore();
          table.handleDispatchStarted(
            1,
            pending.future,
            DispatchCancellationController(),
          );
          final dep =
              table.tryBeginPipelinedDependency(1) as PendingPipelineDependency;
          table.handleRequesterFinishedAnswer(1, releaseResultCaps: false);

          if (drainBeforeSettle) {
            expect(table.endPipelinedDependency(dep.ticket), isNull);
            final recorded =
                table.handleDispatchSucceeded(
                      1,
                      resolved: _answer(),
                      resultExportIds: [7],
                    )
                    as AnswerSettled;
            expect(recorded.releaseResultExportsAfterSend, isNull);
          } else {
            final recorded =
                table.handleDispatchSucceeded(
                      1,
                      resolved: _answer(),
                      resultExportIds: [7],
                    )
                    as AnswerSettled;
            expect(recorded.releaseResultExportsAfterSend, isNull);
            expect(table.endPipelinedDependency(dep.ticket), isNull);
          }
        }
      });

      test('endPipelinedDependency throws if the same ticket is ended '
          'twice — a caller bug that would otherwise silently corrupt the '
          'dependent count', () {
        final table = AnswerTable();
        final pending = Completer<ResolvedAnswer>();
        pending.future.ignore();
        table.handleDispatchStarted(
          1,
          pending.future,
          DispatchCancellationController(),
        );
        final dep =
            table.tryBeginPipelinedDependency(1) as PendingPipelineDependency;

        table.endPipelinedDependency(dep.ticket);
        expect(
          () => table.endPipelinedDependency(dep.ticket),
          throwsStateError,
        );
      });

      test('qid reuse / generation isolation: a dependent that drains after '
          'its original qid has been legally reused by a brand-new answer '
          'does not affect that new answer, and still reports the original '
          'export ids', () {
        final table = AnswerTable();
        final pending = Completer<ResolvedAnswer>();
        pending.future.ignore();
        table.handleDispatchStarted(
          1,
          pending.future,
          DispatchCancellationController(),
        );
        final dep =
            table.tryBeginPipelinedDependency(1) as PendingPipelineDependency;

        table.handleRequesterFinishedAnswer(1, releaseResultCaps: true);
        final recorded = table.handleDispatchSucceeded(
          1,
          resolved: _answer(),
          resultExportIds: [7],
        );
        expect(recorded, isA<AnswerSettled>());
        expect(table.isTracked(1), isFalse);

        // The peer legally reuses qid 1 for a brand-new, unrelated Call —
        // this vat's own Return for the original call already went out.
        final newResolved = _answer();
        table.handleAnswerReadyWithoutDispatch(
          1,
          resolved: newResolved,
          resultExportIds: [99],
        );
        expect(table.getResolvedAnswerFor(1), same(newResolved));

        // The OLD dependent finally drains — must not touch the new answer.
        expect(table.endPipelinedDependency(dep.ticket), equals([7]));
        expect(
          table.getResolvedAnswerFor(1),
          same(newResolved),
          reason: 'new answer under the reused qid is untouched',
        );
        expect(table.isTracked(1), isTrue);

        // The new answer's own Finish still works normally.
        expect(
          table.handleRequesterFinishedAnswer(1, releaseResultCaps: true),
          equals([99]),
        );
      });
    });

    group(
      'takeRedirectedResult (PR #117 review round 6: one-shot ownership '
      'transfer, mirroring capnp-rpc\'s Answer.redirected_results.take())',
      () {
        test('unknown qid reports "unknown question id"', () async {
          final table = AnswerTable();
          await expectLater(
            table.takeRedirectedResult(999),
            throwsA(
              isA<RpcException>().having(
                (e) => e.message,
                'message',
                contains('unknown question id'),
              ),
            ),
          );
        });

        test('a qid tracked but never started with sendResultsToYourself '
            'reports the same "nothing to take" error as an already-taken '
            'one — from the caller\'s perspective both are protocol '
            'violations', () async {
          final table = AnswerTable();
          table.handleAnswerReadyWithoutDispatch(1, resolved: _answer());
          await expectLater(
            table.takeRedirectedResult(1),
            throwsA(
              isA<RpcException>().having(
                (e) => e.message,
                'message',
                contains('did not use sendResultsTo.yourself'),
              ),
            ),
          );
        });

        test('taken while pending, then the dispatch succeeds: resolves to '
            'the real result, and a second take for the same generation '
            'fails instead of returning it again', () async {
          final table = AnswerTable();
          final pending = Completer<ResolvedAnswer>();
          table.handleDispatchStarted(
            1,
            pending.future,
            DispatchCancellationController(),
            sendResultsToYourself: true,
          );

          final taken = table.takeRedirectedResult(1);
          await expectLater(
            table.takeRedirectedResult(1),
            throwsA(isA<RpcException>()),
            reason:
                'already taken by the call above, even though the '
                'dispatch has not settled yet',
          );

          final resolved = _answer();
          pending.complete(resolved);
          expect(await taken, same(resolved));
        });

        test('taken while pending, then the dispatch fails: surfaces the '
            'original error', () async {
          final table = AnswerTable();
          final pending = Completer<ResolvedAnswer>();
          table.handleDispatchStarted(
            2,
            pending.future,
            DispatchCancellationController(),
            sendResultsToYourself: true,
          );

          final taken = table.takeRedirectedResult(2);
          final error = const CapnpException('boom');
          pending.completeError(error);
          await expectLater(taken, throwsA(same(error)));
        });

        test('taken after the dispatch already succeeded resolves '
            'immediately to the real result', () async {
          final table = AnswerTable();
          final pending = Completer<ResolvedAnswer>();
          pending.future.ignore();
          table.handleDispatchStarted(
            3,
            pending.future,
            DispatchCancellationController(),
            sendResultsToYourself: true,
          );
          final resolved = _answer();
          pending.complete(resolved);
          table.handleDispatchSucceeded(3, resolved: resolved);

          expect(await table.takeRedirectedResult(3), same(resolved));
        });

        test('taken after the dispatch already failed surfaces the '
            'retained error, and a second take for the same generation '
            'fails instead of surfacing it again', () async {
          final table = AnswerTable();
          final error = const CapnpException('boom');
          _installFailedAnswer(table, 4, error);

          await expectLater(
            table.takeRedirectedResult(4),
            throwsA(same(error)),
          );
          await expectLater(
            table.takeRedirectedResult(4),
            throwsA(
              isA<RpcException>().having(
                (e) => e.message,
                'message',
                contains('already taken'),
              ),
            ),
          );
        });

        test('a real Finish removes the whole entry once the dispatch has '
            'settled, regardless of whether the redirect was already '
            'taken, and the peer legally reusing the qid afterward starts '
            'a fresh, unrelated generation untouched by the old one', () async {
          final table = AnswerTable();
          final pending = Completer<ResolvedAnswer>();
          table.handleDispatchStarted(
            1,
            pending.future,
            DispatchCancellationController(),
            sendResultsToYourself: true,
          );
          final taken = table.takeRedirectedResult(1);
          final resolvedA = _answer();
          pending.complete(resolvedA);
          // Matches the real causal order: a redirected call's own Return
          // is only ever sent (and handleDispatchSucceeded called) once its
          // dispatch settles, and a real Finish can only arrive after that
          // Return reaches the peer — see AnswerTable.takeRedirectedResult's
          // own doc comment.
          table.handleDispatchSucceeded(1, resolved: resolvedA);
          expect(await taken, same(resolvedA));
          expect(
            table.isTracked(1),
            isTrue,
            reason:
                'a resultsSentElsewhere Return always still owes a '
                'real Finish',
          );

          table.handleRequesterFinishedAnswer(1, releaseResultCaps: true);
          expect(table.isTracked(1), isFalse);

          // The peer reuses qid 1 for a brand-new, unrelated generation.
          final resolvedB = _answer();
          table.handleAnswerReadyWithoutDispatch(1, resolved: resolvedB);
          await expectLater(
            table.takeRedirectedResult(1),
            throwsA(isA<RpcException>()),
            reason: 'generation B was never started with sendResultsToYourself',
          );
        });

        test('still-pending when connection teardown wins the race fails '
            'with the teardown error, not the eventual local result', () async {
          final table = AnswerTable();
          final pending = Completer<ResolvedAnswer>();
          pending.future.ignore();
          table.handleDispatchStarted(
            1,
            pending.future,
            DispatchCancellationController(),
            sendResultsToYourself: true,
          );

          final taken = table.takeRedirectedResult(1);
          final error = const CapnpException('connection torn down');
          table.tearDown(error);
          await expectLater(taken, throwsA(same(error)));

          // The dispatch itself keeps running, uninterrupted, in the
          // background (issue #99) — completing it afterward must not
          // resurrect anything or throw.
          pending.complete(_answer());
        });

        test('already resolved before teardown stays immune to it — only a '
            'still-pending take races teardown', () async {
          final table = AnswerTable();
          final pending = Completer<ResolvedAnswer>();
          pending.future.ignore();
          table.handleDispatchStarted(
            1,
            pending.future,
            DispatchCancellationController(),
            sendResultsToYourself: true,
          );
          final resolved = _answer();
          pending.complete(resolved);
          table.handleDispatchSucceeded(1, resolved: resolved);
          final taken = table.takeRedirectedResult(1);

          table.tearDown(const CapnpException('connection torn down'));
          expect(await taken, same(resolved));
        });

        test('a requester Finish arriving while the dispatch is still '
            'pending must not cancel it once a local consumer has already '
            'taken the redirected result — that consumer still needs the '
            'real result, not whatever a cooperative cancellation would '
            'produce instead; the dispatch settles normally afterward and '
            'the taken Future delivers the real success', () async {
          final table = AnswerTable();
          final pending = Completer<ResolvedAnswer>();
          final cancellation = DispatchCancellationController();
          table.handleDispatchStarted(
            1,
            pending.future,
            cancellation,
            sendResultsToYourself: true,
          );

          final taken = table.takeRedirectedResult(1);
          table.handleRequesterFinishedAnswer(1, releaseResultCaps: true);
          expect(
            cancellation.context.isCanceled,
            isFalse,
            reason:
                'the local consumer holding `taken` still needs this '
                'dispatch to actually finish',
          );

          final resolved = _answer();
          pending.complete(resolved);
          final settlement = table.handleDispatchSucceeded(
            1,
            resolved: resolved,
          );
          expect(
            settlement,
            isA<AnswerSettled>(),
            reason:
                'not AnswerAlreadyFinished — the requester Finish alone '
                'must not have turned this into a canceled answer',
          );
          expect(await taken, same(resolved));
        });

        test('the same guard applies to a failure: a requester Finish '
            'while pending must not cancel a dispatch whose redirected '
            'result a local consumer already took, and that consumer '
            'still gets the real failure once the dispatch settles', () async {
          final table = AnswerTable();
          final pending = Completer<ResolvedAnswer>();
          final cancellation = DispatchCancellationController();
          table.handleDispatchStarted(
            2,
            pending.future,
            cancellation,
            sendResultsToYourself: true,
          );

          final taken = table.takeRedirectedResult(2);
          table.handleRequesterFinishedAnswer(2, releaseResultCaps: true);
          expect(cancellation.context.isCanceled, isFalse);

          final error = const CapnpException('boom');
          pending.completeError(error);
          final settlement = table.handleDispatchFailed(2, error);
          expect(settlement, isA<AnswerSettled>());
          await expectLater(taken, throwsA(same(error)));
        });
      },
    );
  });
}
