# RPC

Corresponds to UC-4 ("Perform RPC over a Network") in the retired `usecase.md`. Assumes
your schema defines an `interface`, not just `struct`s — see
[`schema-and-codegen.md`](schema-and-codegen.md).

This library implements a **Cap'n Proto RPC Level 1 subset**; see the RPC support table in
the top-level [`README.md`](https://github.com/AngryMane/capnproto-dart/blob/main/README.md#rpc-support-status) for exactly which Level 1
features are and aren't implemented.

## Connecting and calling the bootstrap capability

```dart
import 'package:capnproto_dart_rpc/capnproto_dart_rpc.dart';
import 'src/generated/greeter.capnp.dart';

final conn = await RpcSystem.connect(Uri.parse('tcp://127.0.0.1:12345'));
final greeter = conn.bootstrap(GreeterClientFactory());

final result = await greeter.greet((b) => b.name = 'World');
print(result.reply);

await conn.close();
```

`RpcSystem.connect` only supports `tcp://` URIs today. `conn.bootstrap<T>(factory)` is
idempotent per connection — calling it again returns the same capability.

## Serving a bootstrap capability

```dart
class MyGreeterImpl extends GreeterServer {
  @override
  Future<DispatchResult> greet(
    GreeterGreetParamsReader params,
    List<Capability> paramsCapabilities,
  ) async {
    return buildGreetResults((out) => out.reply = 'Hello, ${params.name}!');
  }
}

final server = await RpcSystem.serve(
  Uri.parse('tcp://0.0.0.0:12345'),
  MyGreeterImpl(),
);
```

`buildGreetResults` — generated on `GreeterServer` alongside every method —
builds and serializes the results struct for you; server code never needs to
touch `MessageBuilder` directly.

Every `interface` in your schema generates a `<Name>Server` base class (dispatch
skeleton) alongside the `<Name>Client`/`<Name>ClientFactory` pair used on the calling
side.

## Capabilities as arguments and return values

Capabilities aren't limited to the bootstrap object — a call can take another capability
as a parameter (the server then calls back into it) or return one:

```dart
class MyObserverImpl extends ObserverServer {
  @override
  Future<void> onNext(ObserverOnNextParamsReader params,
      List<Capability> paramsCapabilities) async {
    print('event: ${params.name}');
  }

  @override
  Future<void> onComplete(ObserverOnCompleteParamsReader params,
      List<Capability> paramsCapabilities) async {}
}

await svc.callObserver((b) => ..., observer: MyObserverImpl());
```

Here `MyObserverImpl` is a Dart object the client exports to the server; the server calls
`onNext`/`onComplete` back on it as work progresses — this is how server-initiated "push"
notifications work in this library (there is no separate subscribe/callback channel, just
a capability passed as a normal argument). Note that this dispatch always runs on the
same isolate that called `RpcSystem.connect`/`serve` — if handling a call is expensive,
offload the work yourself (e.g. `Isolate.run(...)`) inside the overridden method.

## Promise pipelining

Dart's `Future<T>` naturally supports promise pipelining: generated client stubs return a
`Future` whose result can itself be a capability, so you can chain a follow-up call
without a round-trip in between:

```dart
// Without pipelining: 2 round-trips
final fooResult = await client.getFoo();
final barResult = await fooResult.getBar();

// With pipelining: 1 round-trip — both calls go out immediately
final barResult = await client.getFoo().then((foo) => foo.getBar());
```

Generated code also exposes a `<method>Pipeline(...)` variant for chaining onto a
capability returned by an in-flight call *before* awaiting it at all — see
`test/interop/complex/client/bin/main.dart` (search for `Pipeline(`) for worked examples.

## Streaming calls (`-> stream`)

Methods declared with `-> stream` in the schema (e.g. `write @0 (chunk :Data) -> stream;`)
get flow-controlled backpressure automatically:

```dart
await sink.write((b) => b.chunk = Uint8List.fromList([1, 2, 3]));
```

The returned `Future` only completes once the fixed-size flow-control window has room,
rather than after every chunk round-trips individually.

## Errors

Connection loss or a remote exception surfaces to the caller as an `RpcException` (a
`CapnpException` subclass):

```dart
try {
  await greeter.greet((b) => b.name = 'World');
} on RpcException catch (e) {
  print('RPC failed: ${e.message}');
}
```

See the [API documentation](https://pub.dev/documentation/capnproto_dart_rpc/latest/)
for the full type signatures, and [`samples-and-testing.md`](samples-and-testing.md) to
run a real client/server pair.
