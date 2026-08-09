# Schema and Code Generation

Corresponds to UC-1 ("Generate Dart Code from Schema") in the retired `usecase.md`.

## Writing a `.capnp` schema

`capnpc-dart` does not parse schema syntax itself — it is invoked by the official `capnp`
compiler as a code-generator plugin, which does the parsing and hands `capnpc-dart` a
`CodeGeneratorRequest` message. Schema syntax is therefore exactly the official Cap'n
Proto schema language; see the
[Cap'n Proto schema language docs](https://capnproto.org/language.html) for the full
reference (structs, interfaces, enums, unions, generics, imports, ...).

```capnp
@0xdeadbeefdeadbeef;

struct Person {
  name @0 :Text;
  age  @1 :UInt32;
  address @2 :Address;
}

struct Address {
  city @0 :Text;
}
```

## Generating Dart code

```sh
capnp compile -o dart:<output-dir> <schema.capnp...>
```

- **Input**: your `.capnp` file(s); `capnp` parses them and streams a
  `CodeGeneratorRequest` to `capnpc-dart` over stdin.
- **Output**: one `.dart` file per `.capnp` input file, written under `<output-dir>`.
- **Exit code**: `0` on success; non-zero if generation fails (e.g. malformed schema, as
  reported by `capnp` itself before `capnpc-dart` even runs).

See the [`capnproto_dart` API documentation](https://pub.dev/documentation/capnproto_dart/latest/)
for the Cap'n Proto → Dart type mapping used by the generated code, and the
[`capnpc_dart` README](https://pub.dev/packages/capnpc_dart) for the full CLI contract.

If the schema contains syntax errors, `capnp` reports them and exits without invoking code
generation at all.

## Cross-file references (`using`/`import`)

A schema can reference types declared in another `.capnp` file:

```capnp
# bar.capnp
struct Bar {
  value @0 :UInt32;
}
```

```capnp
# foo.capnp
using import "bar.capnp".Bar;

struct Foo {
  bar @0 :Bar;
}
```

Compile every file that needs Dart output explicitly — `capnp compile` (like every
other language's plugin) only generates code for files listed on the command line, not
their transitive imports:

```sh
capnp compile -o dart:<output-dir> foo.capnp bar.capnp
```

The generated `foo.capnp.dart` automatically imports `bar.capnp.dart` (a relative import,
mirroring the same relative path the two `.capnp` files have to each other) for any type
it references from another file.

## `const` declarations

```capnp
const maxSize :UInt32 = 100;
const greeting :Text = "hello";
const origin :Point = (x = 0, y = 0);
```

generates a top-level Dart declaration per const — `const int maxSize = 100;`,
`const String greeting = 'hello';`, `final PointReader origin = ...;` (struct/enum
consts can't be Dart `const` since building them involves a function call, so they're
`final` instead).

**List-typed consts are not supported** (e.g. `const primes :List(UInt32) = [2, 3, 5];`)
— they're skipped with a comment in the generated file rather than emitting broken code.
Every other const kind (scalars, `Text`, `Data`, enum, struct) is supported.

## Capability parameters nested inside structs/lists

A method's params struct doesn't have to reference a capability directly — the
capability can be nested inside another struct field, a `group`, or a
`List(SomeInterface)`:

```capnp
struct Bundle {
  primary @0 :SomeInterface;
  targets @1 :List(SomeInterface);
}

interface Foo {
  exchange @0 (bundle :Bundle) -> (bundle :Bundle);
}
```

The generated client method for `exchange` detects this and switches its
`build` callback to take a second parameter — a `CapabilityTableBuilder`
(from `capnproto_dart_rpc`) accumulator shared by every `setXxxTyped(cap,
capTable)` helper the callback calls, however deeply nested:

```dart
await client.exchange((b, capTable) {
  final bundle = b.initBundle();
  bundle.setPrimaryTyped(someCap, capTable);
  bundle.setTargetsTyped([someCap, otherCap], capTable);
});
```

Methods whose params only ever reference a capability directly (not nested)
keep the simpler `required Capability x` named-parameter form instead — the
two-parameter `build` callback is only generated when it's actually needed.

**Not supported: a capability nested inside a generic instantiation.**
Detection does not descend into generic struct instantiations (any
`StructRefType` with type arguments), so a capability reachable only through
one of these is not picked up by the accumulator mechanism above:

```capnp
struct Bundle {
  observer @0 :Optional(SomeInterface);          # not detected
  result   @1 :Result(SomeInterface, ErrorInfo);  # not detected
}
```

For a params struct like this, the generated client method has no `capTable`
parameter at all, and there is currently no supported way to send
`observer`/`result`'s capability through the generated API — sending one
requires either restructuring the schema to avoid the generic wrapper, or
building and dispatching the call by hand. Extending detection to walk
through generic type arguments (substituting `StructRefType.typeArgs` into
the referenced struct's `TypeParameterRefType` fields) is tracked as future
work.

## Checking backward/forward compatibility

When a schema evolves, you can ask `capnpc-dart` to diff the new schema against a
previous version before shipping the change. `capnp`'s `-o` plugin syntax has no
channel for freeform options, so this mode is invoked by dumping the request with
`-o-` and piping it into the plugin binary directly, rather than through
`capnp compile -o`:

```sh
capnp compile -o- <new.capnp> | capnpc-dart --check=<old.capnp>
```

- **Output**: a list of incompatible changes printed to stdout (empty if none).
- **Exit code**: `0` if compatible, `1` if incompatible changes were detected, `2` on
  error.

[`test/interop/schema-evolution/`](https://github.com/AngryMane/capnproto-dart/tree/main/test/interop/schema-evolution) — see
[`samples-and-testing.md`](samples-and-testing.md) — is a related but separate check:
it proves the *generated code* reads/writes old and new schema versions correctly at
runtime, not this static compatibility-check CLI mode.
