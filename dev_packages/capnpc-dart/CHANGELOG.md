## 0.2.0

- Generated client methods now support capabilities nested inside a struct/group field or a `List(Interface)` field of a method's params, not just capabilities referenced directly — see the ["Capability parameters nested inside structs/lists"](https://angrymane.github.io/capnproto-dart/howto/schema-and-codegen) guide. Previously, sending such a capability required bypassing the generated client and calling `Capability.dispatch` directly.
  - **Breaking**: for a method whose params reach a capability this way, the generated `build` callback now takes a second parameter, a `CapabilityTableBuilder` (from `capnproto_dart_rpc`) — e.g. `client.exchange((b, capTable) { ...; b.someField.setXTyped(cap, capTable); })` instead of `client.exchange((b) { ... })`. Existing call sites for such methods need this second parameter added even if they don't set any capability (e.g. `(b, _) { ... }`). Methods whose params only ever reference a capability directly are unaffected — they keep the existing `required Capability x` named-parameter form.
  - Not supported: a capability reachable only through a generic struct instantiation (e.g. `Optional(SomeInterface)`, `Result(SomeInterface, ErrorInfo)`) is not detected and has no generated way to be sent — see the guide linked above for the exact limitation and workarounds.
  - The per-field `setXxxTyped(cap, ...)` builder helpers for an Interface/`List(Interface)` struct field also switched from a raw `List<Object?> capTable` parameter to `CapabilityTableBuilder`, for both the case above and direct top-level fields — a plain mutable list invited accidental corruption (clearing it, inserting something other than a capability, reordering already-assigned entries) that would silently desync a builder's wire-level capability-pointer indices from the accumulator's actual contents.
- **Breaking**: generated code now calls `capnproto_dart_rpc`'s renamed capability-dispatch API (`Capability.dispatchForPipelining`/`dispatchWithParamsBuilder`, `DispatchHandle`, ...) — requires `capnproto_dart_rpc: ^0.2.0`.

## 0.1.0

- Initial development release of the Dart code-generator plugin.
