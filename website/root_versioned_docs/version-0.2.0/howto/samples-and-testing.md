# Samples and Testing

## `sample/greeter` — minimal Dart client + Rust server

A minimal RPC walkthrough: a Rust server exposes a `Greeter` bootstrap capability with a
`newSession` method returning a session capability, and a Dart client calls both the
top-level method and the session's methods.

```sh
# Terminal 1: start the Rust server
cargo run --manifest-path sample/greeter/server/Cargo.toml

# Terminal 2: run the Dart client
dart run sample/greeter/client/bin/main.dart
```

Read `sample/greeter/client/bin/main.dart` alongside [`rpc.md`](rpc.md) — it demonstrates
bootstrap, one-shot calls, obtaining a second capability (`newSession`) and calling
methods on it, then disposing it.

## Running the unit test suites

```sh
dart test dev_packages/capnpc-dart/
dart test packages/capnproto_dart/
dart test packages/capnproto_dart_rpc/
```

## Running everything, including cross-language interop

```sh
ci/run-tests.sh
```

This requires the `capnp` CLI and a Rust toolchain (both provided by the dev container).
It runs, in order: the three unit test suites above, the `sample/greeter` integration
check, and the three suites under `test/interop/`:

- **`test/interop/complex`** — a 29-section RPC interop suite (encoding, every field type,
  pipelining, bidirectional callbacks, Level 1 subset flows), driven in both directions:

  ```sh
  # Dart client against the Rust server
  cargo run --manifest-path test/interop/complex/server/Cargo.toml &
  dart run test/interop/complex/client/bin/main.dart

  # Or the Rust client against the Dart server
  dart run test/interop/complex/dart-server/bin/main.dart &
  cargo run --manifest-path test/interop/complex/rust-client/Cargo.toml
  ```

  If you're looking for a concrete, runnable example of a specific RPC feature discussed
  in [`rpc.md`](rpc.md) (promise pipelining, streaming, capability arguments, ...),
  `test/interop/complex/client/bin/main.dart` is organized into numbered sections (`_s1_`
  through `_s29_`) that each exercise one feature end to end.

- **`test/interop/schema-evolution`** — proves, across both languages, that a message
  written against an old schema version is readable by the other language's newer schema
  (and vice versa); see [`schema-and-codegen.md`](schema-and-codegen.md#checking-backwardforward-compatibility).

- **`test/interop/wire-format-golden`** — checks that this library's serialized bytes are
  byte-for-byte interchangeable with the official C++ reference implementation, using the
  `capnp decode`/`capnp encode` CLI as ground truth.

Each suite is also runnable by hand; see the comment header of each suite's client/server
entry point, or `ci/run-tests.sh` itself for the exact invocation.
