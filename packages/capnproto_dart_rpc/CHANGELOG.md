## 0.2.0

### Breaking changes

The RPC capability API now uses names that describe dispatch, pipelining, and
reference ownership more explicitly:

- `CapCall` was renamed to `DispatchHandle`.
- `DispatchContext` was renamed to `DispatchCancellationContext`.
- `TailCall` was renamed to `TailCallRequest`.
- `vendCapabilityHandle()` was renamed to `acquireCapabilityLease()`.
- `Capability.beginDispatch()` was renamed to
  `Capability.dispatchForPipelining()`.
- `Capability.dispatchBuilding()` was renamed to
  `Capability.dispatchWithParamsBuilder()`.
- `CapCall.pipelineResult()` was renamed to
  `DispatchHandle.pipelinedCapability()`.
- `CapCall.pipelineResultPath()` was renamed to
  `DispatchHandle.pipelinedCapabilityFromResultPath()`.

For example:

```dart
final DispatchHandle handle = capability.dispatchForPipelining(
  interfaceId,
  methodId,
  params,
);
final Capability resultCapability = handle.pipelinedCapability(0);
```

- Added `WeakCapabilityRef`, a non-owning reference to a `Capability` for holding onto a peer-supplied capability (e.g. as a long-lived callback/observer) without keeping it reachable or needing to `dispose()` it yourself.
- `Release` messages for imports are now batched: several `dispose()` calls issued without an intervening `await` (e.g. disposing a whole observer list in one synchronous pass, or `Future.wait([...].map((c) => c.dispose()))`) coalesce into a single `Release` per import ID with `referenceCount > 1`, instead of one wire message each.
- Implemented `Return.releaseParamCaps`: when a dispatched call's params capabilities are all disposed by the time it settles, the Return now sets `releaseParamCaps: true` and skips the separate `Release` messages that would otherwise follow; the client applies the same local effect without waiting for one. Falls back to individual `Release`s when only some are disposed in time.
- Implemented `Return.noFinishNeeded`: a Return whose results carry no capabilities (or which is an exception) now tells the peer no `Finish` is required, and the client skips sending one.
- Fixed a resource leak: when a Call's params-capTable resolution created a new/reused export for a params capability and then failed before the Call itself reached the wire (e.g. a broken import discovered later in the same params list, or the target import breaking before send), the export's refcount bump was never rolled back.
- Fixed a protocol violation: a params capability referencing another in-flight call's still-unresolved result (wire-level promise pipelining) could be sent as a `receiverAnswer` capability descriptor before that parent Call itself reached the wire, causing a compliant peer (e.g. capnp-rust) to reject it as referencing an unknown question id.
- Fixed a bug where a capability received from a peer (import or wire-pipelined promise) and then passed back to that same peer as a call parameter — via any of the `acquireCapabilityLease`-wrapped accessors generated code normally uses — was encoded as a brand-new export instead of the cheap `receiverHosted` reference, causing the peer's normal params-release behavior to prematurely dispose the shared underlying capability out from under other live references to it.
- Fixed a bug (#99): when a tail-call optimization (`sendResultsTo=yourself`) forwarded a call back to a capability hosted on this same vat, and the connection was torn down while that forwarded dispatch was still running, the *original* call stayed pending instead of failing — if the forwarded dispatch legally ignored cooperative cancellation and finished later anyway, the original call would then succeed from purely local state, even though the connection that correlated it was long gone. It now fails with the same disconnection error every other still-pending call gets on teardown, without needing to abort the still-running forwarded dispatch itself.
- Fixed a protocol-correctness bug (#109): a `Finish` received from the peer before an incoming call's dispatch had completed unconditionally canceled that dispatch and dropped its eventual result — even when a pipelined call already queued behind it (`{promisedAnswer: {questionId: ...}}`) still depended on that result. Cancellation is now deferred until every such pipelined dependent has finished with the result; the dispatch is left to complete normally and answered with an ordinary `Return` in that case, exactly as Cap'n Proto's RPC spec permits. Also implemented `Return(canceled)`, which this vat never sent at all before: when cancellation is genuinely accepted (no pipelined dependents), it's now sent once the dispatch actually settles, with the correct `Return.releaseParamCaps`.
- Fixed a related race (#116): the same `Finish`-during-dispatch handling could also cancel a dispatch a peer had *not* asked to cancel — for a `sendResultsTo=yourself` forwarded call, once a correlating `Return.takeFromOtherQuestion` had already claimed this vat's own local reference to the eventual result, the *forwarder's* own `Finish` for that call no longer meant nothing else needed it. Cancellation is now withheld in that case too, symmetrically with the pipelined-dependent case above. Also fixed the same race's other half: a `Return.takeFromOtherQuestion` arriving while the correlated dispatch was still pending is now correlated to it synchronously, the instant the `Return` is processed, rather than in a later continuation — previously, a `Finish` for that same call arriving in between could turn a legitimate result into an "unknown question id" error.
- Fixed a reentrant-peer hazard: a peer that reacted to one of this vat's own `Return` messages synchronously (e.g. over an in-memory or `sync: true` transport) by immediately sending `Finish` and reusing the same question id for a new, unrelated `Call` could, in rare orderings, have that new call's state corrupted by leftover cleanup from the old one. Answer bookkeeping is now fully resolved before any Return is sent, closing the window entirely.
