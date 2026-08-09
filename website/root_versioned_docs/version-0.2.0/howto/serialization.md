# Serialization

Corresponds to UC-2 ("Serialize a Cap'n Proto Message") and UC-3 ("Deserialize a Cap'n
Proto Message") in the retired `usecase.md`. Assumes you've already generated Dart code
from a schema — see [`schema-and-codegen.md`](schema-and-codegen.md).

## Building and serializing a message

```dart
import 'package:capnproto_dart/capnproto_dart.dart';
import 'src/generated/hello.capnp.dart';

final builder = MessageBuilder();
final greeting = builder.initRoot(greetingFactory); // <name>Factory, generated per struct
greeting.name = 'World';

final bytes = builder.serialize();       // standard framing
final packed = builder.serializePacked(); // zero-byte-compressed framing
```

If a required field is missing or a value is out of range, the generated setter or
`serialize()` throws a `SchemaException`/`DecodeException` (both subclasses of
`CapnpException`) rather than producing malformed bytes.

## Reading a message back

```dart
final reader = MessageReader.deserialize(bytes);
// or: MessageReader.deserializePacked(packed);
final g = reader.getRoot(greetingFactory);
print(g.name);
```

`MessageReader.deserialize`/`deserializePacked` accept a `MessageReaderOptions` to tune
the traversal limit, nesting limit, and max segment count — the defaults guard against
amplification attacks and are usually fine to leave alone. If the bytes are malformed, or
exceed one of these limits, a `DecodeException` is thrown.

See the [API documentation](https://pub.dev/documentation/capnproto_dart/latest/)
for the full `MessageBuilder`/`MessageReader`/`MessageReaderOptions` contract and the
Cap'n Proto → Dart primitive type mapping.

## Streaming multiple messages over a byte stream

For framed sequences of messages over something like a `Socket`, use `MessageStream`
instead of manually deserializing one message at a time:

```dart
// Reading: one MessageReader per framed message on the wire
await for (final reader in MessageStream.deserializeStream(socket)) {
  final g = reader.getRoot(greetingFactory);
  print(g.name);
}

// Writing: turn a stream of MessageBuilders into framed bytes
final outBytes = MessageStream.serializeStream(builderStream);
```

This is the same mechanism the RPC Runtime uses under the hood to frame `Call`/`Return`/…
messages — see [`rpc.md`](rpc.md).

## Dynamic (schema-less) access

When the concrete struct type isn't known at compile time — e.g. inspecting an
`AnyPointer` field generically, or building a tool that walks arbitrary schemas — use the
reflection-based `AnyPointerReader`/`AnyPointerBuilder` and
`DynamicStructReader`/`DynamicListReader` types instead of a generated `StructFactory`:

```dart
final anyPtr = someStruct.getAnyPointerField(0); // AnyPointerReader
final dynStruct = anyPtr.asDynamicStruct();      // null if it's not a struct pointer
final field = dynStruct?.schema.fieldByName('name');
```

This is built on the `SchemaInfo` metadata `capnpc-dart` emits alongside every generated
struct/enum/interface — see the [API documentation](https://pub.dev/documentation/capnproto_dart/latest/)
for the full reflection contract.

## Text format

`encodeText`/`decodeText` convert to/from the human-readable representation used by the
reference `capnp` CLI's `encode`/`decode` subcommands (e.g. `capnp decode my.capnp Foo`) —
handy for debugging or hand-authoring test fixtures:

```dart
final registry = schemaRegistryOf([greetingSchema]); // every struct/enum reachable
                                                      // from Greeting's fields

print(encodeText(g, greetingSchema, registry));
// (name = "World")

final bytes = decodeText('(name = "World")', greetingSchema, registry);
final g2 = MessageReader.deserialize(bytes).getRoot(greetingFactory);
```

See the [API documentation](https://pub.dev/documentation/capnproto_dart/latest/)
for the full contract, including what isn't representable (capabilities, untyped
`AnyPointer`/generic fields).
