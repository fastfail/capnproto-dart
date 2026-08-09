# Getting Started

**Audience**: a Flutter/Dart application developer who wants to use Cap'n Proto for
serialization and/or RPC without an FFI dependency on the C++ reference implementation.

## Overall Flow

```
Define .capnp schema → Generate Dart code → Integrate into app → Serialize / Deserialize / RPC
```

## Prerequisites

- Dart SDK (see `.devcontainer/Dockerfile` for the pinned version used in CI)
- The official `capnp` compiler (`capnpc-dart` is a plugin for it, not a standalone
  compiler — see [`schema-and-codegen.md`](schema-and-codegen.md))
- Rust + `cargo`, only if you plan to run the cross-language interop suites under
  `test/interop/` or the `sample/greeter` server

A ready-to-use dev container (`.devcontainer/`) already has all of the above; see the
repository root [`README.md`](https://github.com/AngryMane/capnproto-dart/blob/main/README.md#development) for how to open it.

Most apps built on this library talk to another process over RPC — that's the path
below. If you only need to read/write Cap'n Proto messages yourself, with no RPC
involved, see [Not using RPC?](#not-using-rpc) at the end.

## 1. Install

Add the runtime and the code-generation builder to `pubspec.yaml`:

```yaml
dependencies:
  capnproto_dart_rpc: ^0.1.0

dev_dependencies:
  build_runner: ^2.4.13
  capnpc_dart_builder: ^0.1.0
```

```sh
dart pub get
```

The official `capnp` compiler must also be installed and on `PATH` —
`capnpc_dart_builder` shells out to it to parse your schemas.

## 2. Write a schema

`build_runner` only looks under `lib/`, so put schema files there:

```capnp
# lib/schema/hello.capnp
@0xdeadbeefdeadbeef;

interface Greeter {
  greet @0 (name :Text) -> (reply :Text);
}
```

## 3. Generate Dart code

```sh
dart run build_runner build
```

`capnpc_dart_builder` finds every `.capnp` file in your package, runs it through
`capnp`, and writes `lib/schema/hello.capnp.dart` next to it — with a typed client
(`GreeterClientFactory`) and a server base class (`GreeterServer`). Re-run this command
after editing a schema; add `--delete-conflicting-outputs` if build_runner complains
about stale output. See [`schema-and-codegen.md`](schema-and-codegen.md) for
schema-evolution / compatibility checking.

## 4. Implement a server

```dart
import 'package:capnproto_dart_rpc/capnproto_dart_rpc.dart';
import 'schema/hello.capnp.dart';

class MyGreeter extends GreeterServer {
  @override
  Future<DispatchResult> greet(
    GreeterGreetParamsReader params,
    List<Capability> paramsCapabilities,
  ) async {
    return buildGreetResults((out) => out.reply = 'Hello, ${params.name}!');
  }
}

Future<void> main() async {
  await RpcSystem.serve(Uri.parse('tcp://0.0.0.0:12345'), MyGreeter());
}
```

## 5. Call it from a client

```dart
import 'package:capnproto_dart_rpc/capnproto_dart_rpc.dart';
import 'schema/hello.capnp.dart';

Future<void> main() async {
  final conn = await RpcSystem.connect(Uri.parse('tcp://127.0.0.1:12345'));
  final greeter = conn.bootstrap(GreeterClientFactory());

  final result = await greeter.greet((b) => b.name = 'World');
  print(result.reply); // Hello, World!

  await greeter.dispose();
  await conn.close();
}
```

Neither side touches `MessageBuilder`/`MessageReader` directly — the generated client
stub and the `buildGreetResults` helper handle serialization for you. See
[`rpc.md`](rpc.md) for bootstrap, capabilities, promise pipelining, and streaming calls,
and [`samples-and-testing.md`](samples-and-testing.md) to run a working client/server
pair end to end.

## Not using RPC?

If you just need to read and write Cap'n Proto messages yourself — e.g. saving to a
file, or sending them over your own transport — depend on `capnproto_dart` directly
instead of `capnproto_dart_rpc`, add a plain struct to your schema, and use
`MessageBuilder`/`MessageReader`:

```capnp
struct Greeting {
  name @0 :Text;
  reply @1 :Text;
}
```

```dart
import 'package:capnproto_dart/capnproto_dart.dart';
import 'schema/hello.capnp.dart';

void main() {
  final builder = MessageBuilder();
  final greeting = builder.initRoot(greetingFactory);
  greeting.name = 'World';

  final bytes = builder.serialize();

  final reader = MessageReader.deserialize(bytes);
  final g = reader.getRoot(greetingFactory);
  print(g.name); // World
}
```

See [`serialization.md`](serialization.md) for packed encoding, streaming, and dynamic
(schema-less) access.
