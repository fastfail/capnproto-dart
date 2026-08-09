// Cap'n Proto RPC wire message codec.
//
// Binary offsets are derived from the rpc.capnp specification using the
// standard field layout algorithm (size class ordering: 64-bit, 32-bit,
// 16-bit, bool).
//
// Payload.content is encoded as an AnyPointer struct, fully spec-compliant
// with the Cap'n Proto RPC standard (interoperable with C++/Rust peers).

import 'dart:async';
import 'dart:typed_data';

import 'package:capnproto_dart/capnproto_dart.dart';

import 'capabilities/wire_capability_reference.dart';

// ---------------------------------------------------------------------------
// Message discriminant values
// ---------------------------------------------------------------------------

// RPC envelope messages (Call/Return/Finish/Release/...) carry the fixed
// envelope fields plus, for Call/Return, a deep copy of the params/results
// content — typically small (a common rule of thumb for Cap'n Proto RPC's
// object-capability style is many fine-grained calls in the tens-of-bytes
// to ~1 KiB range) but not tiny like Finish/Release. MessageBuilder's own
// default (8 KiB) is sized for a caller that doesn't know its message's
// size ahead of time; every builder function below does, so it opts into a
// smaller first-segment allocation instead of paying for (and zeroing)
// 8 KiB per envelope. Sized with headroom above that ~1 KiB common case —
// a message that turns out larger still transparently grows into an extra
// segment (see [MessageBuilder.new]'s doc comment) rather than failing, but
// that growth (and the far-pointer indirection it costs) is itself not
// free, so this is deliberately not cut as close to the small end as
// possible; measured against the RPC UDS benchmark, a too-small value here
// made ~1 KiB payloads slightly *slower* than the unbounded 8 KiB default,
// even though it clearly won for smaller ones.
const int _initialEnvelopeCapacityWords = 192; // 1.5 KiB

const int _msgUnimplemented = 0;
const int _msgAbort = 1;
const int _msgCall = 2;
const int _msgReturn = 3;
const int _msgFinish = 4;
const int _msgResolve = 5;
const int _msgRelease = 6;
const int _msgBootstrap = 8;
const int _msgDisembargo = 13;

// ---------------------------------------------------------------------------
// Struct layout constants (byte offsets from data section start)
// ---------------------------------------------------------------------------

// Message (dw=1, pw=1)
//   bytes 0-1: discriminant (UInt16)
//   ptr 0: payload (union)
const int _msgDiscOff = 0;

// Bootstrap (dw=1, pw=1)
//   bytes 0-3: questionId (UInt32)
//   ptr 0: deprecatedObjectId (AnyPointer, ignored)
const int _bootstrapQid = 0;

// Call (dw=3, pw=3)
//   Fields allocated in ordinal order to smallest-fitting available slot:
//   bytes  0-3: questionId (UInt32, @0)
//   bytes  4-5: methodId (UInt16, @2)
//   bytes  6-7: sendResultsTo disc (UInt16, @5-@7 union disc)
//   bytes 8-15: interfaceId (UInt64, @1)
//   ptr 0: target (MessageTarget)
//   ptr 1: params (Payload)
//   ptr 2: sendResultsTo.thirdParty (AnyPointer, unused)
const int _callQid = 0;
const int _callMethodId = 4;
const int _callSendResultsDisc = 6;
const int _callIfaceId = 8;

// MessageTarget (dw=1, pw=1)
//   Slot assignment: UInt32 fields come before UInt16 (discriminant) in layout.
//   bytes 0-3: importedCap (UInt32, union data slot, @0)
//   bytes 4-5: discriminant (UInt16, discriminantOffset=2 → byte 4)
//              0=importedCap, 1=promisedAnswer
//   ptr 0: promisedAnswer (PromisedAnswer, when disc=1)
// Verified against Rust rpc_capnp.rs: get_data_field::<u32>(0) and get_data_field::<u16>(2).
const int _targetImportCap = 0;
const int _targetDisc = 4;
const int _targetPromisedAnswer = 1;

// PromisedAnswer (dw=1, pw=1)
//   bytes 0-3: questionId (UInt32, @0)
//   ptr 0: transform (List(Op), @1)
const int _paQid = 0;

// PromisedAnswer.Op (dw=1, pw=0)
//   bytes 0-1: discriminant (UInt16, discriminantOffset=0 → byte 0)
//              0=noop, 1=getPointerField
//   bytes 2-3: getPointerField value (UInt16, @1 → second UInt16 slot)
const int _opDisc = 0;
const int _opGetPtrField = 2;

// Payload (dw=0, pw=2)
//   ptr 0: content (AnyPointer → Data bytes in our convention)
//   ptr 1: capTable (List(CapDescriptor))
// (no data words)

// Return (dw=2, pw=1)
//   Fields allocated in ordinal order to smallest-fitting available slot:
//   bytes 0-3: answerId (UInt32, @0)
//   byte  4 bit 0: releaseParamCaps (Bool, @1)
//   byte  4 bit 1: noFinishNeeded (Bool, @8)
//   bytes 6-7: union discriminant (UInt16, @2-@7 union disc)
//              0=results, 1=exception, 2=canceled, 3=resultsSentElsewhere,
//              4=takeFromOtherQuestion, 5=acceptFromThirdParty
//   bytes 8-11: union data slot (UInt32, for takeFromOtherQuestion @6)
//   ptr 0: union ptr slot (Payload for results / Exception for exception)
const int _returnAnswerId = 0;
const int _returnReleaseParamCaps = 32; // bit index = byte 4 * 8 + bit 0
const int _returnNoFinishNeeded = 33; // bit index = byte 4 * 8 + bit 1
const int _returnDisc = 6;
const int _returnTakeFromOtherQuestionOff = 8;

const int _retResults = 0;
const int _retException = 1;
// Not implemented by this vat (see OutgoingCallCoordinator's internal
// _awaitAndProcessReturn, in outgoing_call_coordinator.dart).
const int _retCanceled = 2;
const int _retResultsSentElsewhere = 3;
const int _retTakeFromOtherQuestion = 4;
const int _retAcceptFromThirdParty = 5;

/// Maps a wire `Exception.type` value to [ErrorKind]. Out-of-range values
/// (e.g. a future peer-side enumerant this vat doesn't know about yet) fall
/// back to [ErrorKind.failed] rather than throwing — matches this file's
/// existing "never crash on a value from an untrusted peer" pattern (see
/// [describeReturnVariant]'s `unknown($disc)` fallback).
ErrorKind _errorKindFromWire(int wireType) =>
    wireType >= 0 && wireType < ErrorKind.values.length
        ? ErrorKind.values[wireType]
        : ErrorKind.failed;

/// Human-readable name for a [RpcMessage.returnDisc] value, for diagnostics
/// when a peer sends a `Return` variant this vat doesn't implement.
String describeReturnVariant(int disc) => switch (disc) {
  _retResults => 'results',
  _retException => 'exception',
  _retCanceled => 'canceled',
  _retResultsSentElsewhere => 'resultsSentElsewhere',
  _retTakeFromOtherQuestion => 'takeFromOtherQuestion',
  _retAcceptFromThirdParty => 'acceptFromThirdParty',
  _ => 'unknown($disc)',
};

// Resolve (dw=1, pw=1)
//   bytes 0-3: promiseId (UInt32, @0)
//   bytes 4-5: union discriminant (UInt16, @1-@2 union disc)
//              0=cap, 1=exception
//   ptr 0: CapDescriptor or Exception
const int _resolvePromiseId = 0;
const int _resolveDisc = 4;
const int _resolveCap = 0;
const int _resolveException = 1;

// Exception (dw=1, pw=2)
//   bytes 0-1: type (UInt16, 0=failed)
//   byte 2 bit 0: obsoleteIsCallersFault
//   ptr 0: reason (Text)
//   ptr 1: trace (Text, unused)
const int _excTypeOff = 0;

// Finish (dw=1, pw=0)
//   bytes 0-3: questionId (UInt32)
//   byte  4 bit 0: releaseResultCaps (Bool, default=true)
const int _finishQid = 0;
const int _finishRelease = 32; // bit index = byte 4 * 8 = 32

// Release (dw=1, pw=0)
//   bytes 0-3: id (UInt32)
//   bytes 4-7: referenceCount (UInt32)
const int _releaseId = 0;
const int _releaseRefCnt = 4;

// Disembargo (dw=1, pw=1)
//   bytes 0-3: context union data (EmbargoId / QuestionId)
//   bytes 4-5: context union discriminant
//   ptr 0: target (MessageTarget)
const int _disembargoContextData = 0;
const int _disembargoContextDisc = 4;

// CapDescriptor (dw=1, pw=1)
//   All-union struct → discriminant first, then union data:
//   bytes 0-1: union discriminant (UInt16)
//   bytes 4-7: union data (senderHosted/senderPromise/receiverHosted as UInt32)
//              0=none, 1=senderHosted, 2=senderPromise, 3=receiverHosted,
//              4=receiverAnswer, 5=thirdPartyHosted
const int _capDescDisc = 0;
const int _capDescData = 4;
const int _capDescNone = 0;
const int _capDescSenderHosted = 1;
const int _capDescSenderPromise = 2;
const int _capDescReceiverHosted = 3;
const int _capDescReceiverAnswer = 4;

// ---------------------------------------------------------------------------
// StructReader / StructBuilder subclasses (internal, rpc_message_codec only)
// ---------------------------------------------------------------------------

class _RpcMessageReader extends StructReader {
  _RpcMessageReader(super.raw);
  int get disc => getUint16Field(_msgDiscOff);
  _BootstrapReader? get asBootstrap =>
      getStructFieldWith(0, _BootstrapReader.new);
  _CallReader? get asCall => getStructFieldWith(0, _CallReader.new);
  _ReturnReader? get asReturn => getStructFieldWith(0, _ReturnReader.new);
  _ResolveReader? get asResolve => getStructFieldWith(0, _ResolveReader.new);
  _FinishReader? get asFinish => getStructFieldWith(0, _FinishReader.new);
  _ReleaseReader? get asRelease => getStructFieldWith(0, _ReleaseReader.new);
  _DisembargoReader? get asDisembargo =>
      getStructFieldWith(0, _DisembargoReader.new);
  _ExceptionReader? get asAbort => getStructFieldWith(0, _ExceptionReader.new);
}

class _RpcMessageBuilder extends StructBuilder {
  _RpcMessageBuilder(super.raw);
  @override
  StructReader asReader() => throw UnsupportedError('internal');
  void setDisc(int v) => setUint16Field(_msgDiscOff, v);
  _BootstrapBuilder initBootstrap() =>
      initStructFieldWith(0, _BootstrapBuilder.new, 1, 1);
  _CallBuilder initCall() => initStructFieldWith(0, _CallBuilder.new, 3, 3);
  _ReturnBuilder initReturn() =>
      initStructFieldWith(0, _ReturnBuilder.new, 2, 1);
  _ResolveBuilder initResolve() =>
      initStructFieldWith(0, _ResolveBuilder.new, 1, 1);
  _FinishBuilder initFinish() =>
      initStructFieldWith(0, _FinishBuilder.new, 1, 0);
  _ReleaseBuilder initRelease() =>
      initStructFieldWith(0, _ReleaseBuilder.new, 1, 0);
  _DisembargoBuilder initDisembargo() =>
      initStructFieldWith(0, _DisembargoBuilder.new, 1, 1);
  _ExceptionBuilder initAbort() =>
      initStructFieldWith(0, _ExceptionBuilder.new, 1, 2);
  // Embed the original message bytes as the 'unimplemented' payload (AnyPointer).
  void setUnimplementedPayload(Uint8List bytes) =>
      setAnyPointerFromMessage(0, bytes);
}

class _BootstrapReader extends StructReader {
  _BootstrapReader(super.raw);
  int get questionId => getUint32Field(_bootstrapQid);
}

class _BootstrapBuilder extends StructBuilder {
  _BootstrapBuilder(super.raw);
  @override
  StructReader asReader() => throw UnsupportedError('internal');
  void setQuestionId(int v) => setUint32Field(_bootstrapQid, v);
}

class _CallReader extends StructReader {
  _CallReader(super.raw);
  int get questionId => getUint32Field(_callQid);
  int get interfaceId => getUint64Field(_callIfaceId);
  int get methodId => getUint16Field(_callMethodId);
  _MessageTargetReader? get target =>
      getStructFieldWith(0, _MessageTargetReader.new);
  _PayloadReader? get params => getStructFieldWith(1, _PayloadReader.new);
  int get sendResultsToDisc => getUint16Field(_callSendResultsDisc);
}

class _CallBuilder extends StructBuilder {
  _CallBuilder(super.raw);
  @override
  StructReader asReader() => throw UnsupportedError('internal');
  void setQuestionId(int v) => setUint32Field(_callQid, v);
  void setInterfaceId(int v) => setUint64Field(_callIfaceId, v);
  void setMethodId(int v) => setUint16Field(_callMethodId, v);
  void setSendResultsToCaller() => setUint16Field(_callSendResultsDisc, 0);
  void setSendResultsToYourself() => setUint16Field(_callSendResultsDisc, 1);
  _MessageTargetBuilder initTarget() =>
      initStructFieldWith(0, _MessageTargetBuilder.new, 1, 1);
  _PayloadBuilder initParams() =>
      initStructFieldWith(1, _PayloadBuilder.new, 0, 2);
}

class _MessageTargetReader extends StructReader {
  _MessageTargetReader(super.raw);
  int get disc => getUint16Field(_targetDisc);
  int get importedCap => getUint32Field(_targetImportCap);
  _PromisedAnswerReader? get promisedAnswer =>
      getStructFieldWith(0, _PromisedAnswerReader.new);
}

class _MessageTargetBuilder extends StructBuilder {
  _MessageTargetBuilder(super.raw);
  @override
  StructReader asReader() => throw UnsupportedError('internal');
  void setImportedCap(int v) {
    setUint32Field(_targetImportCap, v);
    setUint16Field(_targetDisc, 0);
  }

  /// [transformPath] is the full sequence of `getPointerField` hops (see
  /// [ReceiverAnswerCapabilityReference.transformPath]) — not just a single
  /// index — so a capability nested more than one struct deep in the target
  /// answer round-trips correctly.
  void setPromisedAnswer(int questionId, List<int> transformPath) {
    setUint16Field(_targetDisc, _targetPromisedAnswer);
    final pa = initStructFieldWith(0, _PromisedAnswerBuilder.new, 1, 1);
    pa.setQuestionId(questionId);
    final transform = pa.initTransform(transformPath.length);
    for (var i = 0; i < transformPath.length; i++) {
      transform[i].setGetPointerField(transformPath[i]);
    }
  }
}

class _PromisedAnswerReader extends StructReader {
  _PromisedAnswerReader(super.raw);
  int get questionId => getUint32Field(_paQid);
  ListReader<_OpReader>? get transform =>
      getStructListFieldWith(0, _OpReader.new);
}

class _PromisedAnswerBuilder extends StructBuilder {
  _PromisedAnswerBuilder(super.raw);
  @override
  StructReader asReader() => throw UnsupportedError('internal');
  void setQuestionId(int v) => setUint32Field(_paQid, v);
  ListBuilder<_OpBuilder> initTransform(int count) =>
      initStructListFieldWith(0, count, _OpBuilder.new, 1, 0);
}

class _OpReader extends StructReader {
  _OpReader(super.raw);
  int get disc => getUint16Field(_opDisc);
  int get ptrIndex => getUint16Field(_opGetPtrField);
}

class _OpBuilder extends StructBuilder {
  _OpBuilder(super.raw);
  @override
  StructReader asReader() => throw UnsupportedError('internal');
  void setGetPointerField(int ptrIndex) {
    setUint16Field(_opDisc, 1);
    setUint16Field(_opGetPtrField, ptrIndex);
  }
}

class _PayloadReader extends StructReader {
  _PayloadReader(super.raw);
  // content (ptr 0): AnyPointer, read in place — no copy, no re-parse.
  AnyPointerReader? get content => getAnyPointerField(0);
  // capTable (ptr 1): composite list of CapDescriptor.
  ListReader<_CapDescriptorReader>? get capTable =>
      getStructListFieldWith(1, _CapDescriptorReader.new);
}

class _PayloadBuilder extends StructBuilder {
  _PayloadBuilder(super.raw);
  @override
  StructReader asReader() => throw UnsupportedError('internal');
  // content (ptr 0): embed [v] (a serialized Cap'n Proto message) as AnyPointer.
  void setContentBytes(Uint8List v) =>
      setAnyPointerFromMessage(0, v, preserveCapabilityPointers: true);
  // content (ptr 0): embed an already-resolved struct directly, skipping
  // the serialize-then-reparse round trip [setContentBytes] needs — see
  // [AnyPointerBuilder.setFromRawStruct].
  void setContentFromRawStruct(RawStructReader v) => initAnyPointerField(
    0,
  ).setFromRawStruct(v, preserveCapabilityPointers: true);
  // capTable built via initCapTable
  ListBuilder<_CapDescriptorBuilder> initCapTable(int count) =>
      initStructListFieldWith(1, count, _CapDescriptorBuilder.new, 1, 1);
}

class _CapDescriptorReader extends StructReader {
  _CapDescriptorReader(super.raw);
  int get disc => getUint16Field(_capDescDisc);
  int get id => getUint32Field(_capDescData);
  int get senderHostedId => id;
  _PromisedAnswerReader? get receiverAnswer =>
      getStructFieldWith(0, _PromisedAnswerReader.new);
}

class _CapDescriptorBuilder extends StructBuilder {
  _CapDescriptorBuilder(super.raw);
  @override
  StructReader asReader() => throw UnsupportedError('internal');
  void setSenderHosted(int exportId) {
    setUint32Field(_capDescData, exportId);
    setUint16Field(_capDescDisc, _capDescSenderHosted);
  }

  void setSenderPromise(int exportId) {
    setUint32Field(_capDescData, exportId);
    setUint16Field(_capDescDisc, _capDescSenderPromise);
  }

  void setReceiverHosted(int importId) {
    setUint32Field(_capDescData, importId);
    setUint16Field(_capDescDisc, _capDescReceiverHosted);
  }

  /// See [_MessageTargetBuilder.setPromisedAnswer] on [transformPath].
  void setReceiverAnswer(int questionId, List<int> transformPath) {
    setUint16Field(_capDescDisc, _capDescReceiverAnswer);
    final pa = initStructFieldWith(0, _PromisedAnswerBuilder.new, 1, 1);
    pa.setQuestionId(questionId);
    final transform = pa.initTransform(transformPath.length);
    for (var i = 0; i < transformPath.length; i++) {
      transform[i].setGetPointerField(transformPath[i]);
    }
  }
}

class _ReturnReader extends StructReader {
  _ReturnReader(super.raw);
  int get answerId => getUint32Field(_returnAnswerId);
  bool get releaseParamCaps =>
      getBoolField(_returnReleaseParamCaps, defaultValue: true);
  bool get noFinishNeeded =>
      getBoolField(_returnNoFinishNeeded, defaultValue: false);
  int get disc => getUint16Field(_returnDisc);
  _PayloadReader? get results => getStructFieldWith(0, _PayloadReader.new);
  _ExceptionReader? get exception =>
      getStructFieldWith(0, _ExceptionReader.new);
  int get takeFromOtherQuestion =>
      getUint32Field(_returnTakeFromOtherQuestionOff);
}

class _ReturnBuilder extends StructBuilder {
  _ReturnBuilder(super.raw);
  @override
  StructReader asReader() => throw UnsupportedError('internal');
  void setAnswerId(int v) => setUint32Field(_returnAnswerId, v);
  void setReleaseParamCaps({bool value = true}) =>
      setBoolField(_returnReleaseParamCaps, value, defaultValue: true);
  void setNoFinishNeeded({bool value = false}) =>
      setBoolField(_returnNoFinishNeeded, value, defaultValue: false);
  void setDiscResults() => setUint16Field(_returnDisc, _retResults);
  void setDiscException() => setUint16Field(_returnDisc, _retException);
  void setDiscRaw(int disc) => setUint16Field(_returnDisc, disc);
  void setTakeFromOtherQuestion(int questionId) {
    setDiscRaw(_retTakeFromOtherQuestion);
    setUint32Field(_returnTakeFromOtherQuestionOff, questionId);
  }

  _PayloadBuilder initResults() =>
      initStructFieldWith(0, _PayloadBuilder.new, 0, 2);
  _ExceptionBuilder initException() =>
      initStructFieldWith(0, _ExceptionBuilder.new, 1, 2);
}

class _ResolveReader extends StructReader {
  _ResolveReader(super.raw);
  int get promiseId => getUint32Field(_resolvePromiseId);
  int get disc => getUint16Field(_resolveDisc);
  _CapDescriptorReader? get cap =>
      getStructFieldWith(0, _CapDescriptorReader.new);
  _ExceptionReader? get exception =>
      getStructFieldWith(0, _ExceptionReader.new);
}

class _ResolveBuilder extends StructBuilder {
  _ResolveBuilder(super.raw);
  @override
  StructReader asReader() => throw UnsupportedError('internal');
  void setPromiseId(int v) => setUint32Field(_resolvePromiseId, v);
  void setDiscCap() => setUint16Field(_resolveDisc, _resolveCap);
  void setDiscException() => setUint16Field(_resolveDisc, _resolveException);
  _CapDescriptorBuilder initCap() =>
      initStructFieldWith(0, _CapDescriptorBuilder.new, 1, 1);
  _ExceptionBuilder initException() =>
      initStructFieldWith(0, _ExceptionBuilder.new, 1, 2);
}

class _FinishReader extends StructReader {
  _FinishReader(super.raw);
  int get questionId => getUint32Field(_finishQid);
  bool get releaseResultCaps =>
      getBoolField(_finishRelease, defaultValue: true);
}

class _FinishBuilder extends StructBuilder {
  _FinishBuilder(super.raw);
  @override
  StructReader asReader() => throw UnsupportedError('internal');
  void setQuestionId(int v) => setUint32Field(_finishQid, v);
  void setReleaseResultCaps({bool value = true}) =>
      setBoolField(_finishRelease, value, defaultValue: true);
}

class _ReleaseReader extends StructReader {
  _ReleaseReader(super.raw);
  int get id => getUint32Field(_releaseId);
  int get referenceCount => getUint32Field(_releaseRefCnt);
}

class _ReleaseBuilder extends StructBuilder {
  _ReleaseBuilder(super.raw);
  @override
  StructReader asReader() => throw UnsupportedError('internal');
  void setId(int v) => setUint32Field(_releaseId, v);
  void setReferenceCount(int v) => setUint32Field(_releaseRefCnt, v);
}

class _DisembargoReader extends StructReader {
  _DisembargoReader(super.raw);
  int get contextDisc => getUint16Field(_disembargoContextDisc);
  int get contextId => getUint32Field(_disembargoContextData);
  _MessageTargetReader? get target =>
      getStructFieldWith(0, _MessageTargetReader.new);
}

class _DisembargoBuilder extends StructBuilder {
  _DisembargoBuilder(super.raw);
  @override
  StructReader asReader() => throw UnsupportedError('internal');
  void setContext(int disc, int id) {
    setUint32Field(_disembargoContextData, id);
    setUint16Field(_disembargoContextDisc, disc);
  }

  _MessageTargetBuilder initTarget() =>
      initStructFieldWith(0, _MessageTargetBuilder.new, 1, 1);
}

class _ExceptionReader extends StructReader {
  _ExceptionReader(super.raw);
  int get type => getUint16Field(_excTypeOff);
  String? get reason => getTextField(0);
}

class _ExceptionBuilder extends StructBuilder {
  _ExceptionBuilder(super.raw);
  @override
  StructReader asReader() => throw UnsupportedError('internal');
  void setType(int v) => setUint16Field(_excTypeOff, v);
  void setReason(String v) => setTextField(0, v);
}

// ---------------------------------------------------------------------------
// Factories
// ---------------------------------------------------------------------------

final class _RpcMessageFactory
    extends StructFactory<_RpcMessageReader, _RpcMessageBuilder> {
  @override
  int get dataWords => 1;
  @override
  int get ptrWords => 1;
  @override
  _RpcMessageReader fromRawReader(RawStructReader r) => _RpcMessageReader(r);
  @override
  _RpcMessageBuilder fromRawBuilder(RawStructBuilder r) =>
      _RpcMessageBuilder(r);
}

final _rpcMessageFactory = _RpcMessageFactory();

// ---------------------------------------------------------------------------
// Parsed message type
// ---------------------------------------------------------------------------

/// Discriminant values from a decoded RPC message.
enum RpcMessageType {
  bootstrap,
  call,
  return_,
  resolve,
  finish,
  release,
  disembargo,
  abort,
  unimplemented, // disc=0: peer could not handle a message we sent
  other,
}

/// Decoded RPC message.
final class RpcMessage {
  final RpcMessageType type;

  // bootstrap / call
  final int questionId;

  // call
  final int interfaceId;
  final int methodId;
  // target: importedCap (targetIsPromisedAnswer=false) or promisedAnswer (true)
  final int targetImportId;
  final bool targetIsPromisedAnswer;
  final int targetPromisedAnswerQid;
  // Full getPointerField hop sequence (see
  // ReceiverAnswerCapabilityReference.transformPath) — not just the first hop.
  final List<int> targetTransformPath;
  final AnyPointerReader? paramsContent;
  // Call.sendResultsTo union disc (0=caller, 1=yourself, 2=thirdParty).
  // Only 0 (caller, the default for every normal call) and 1 (yourself, the
  // Level 1 tail-call mechanism) are meaningful here; thirdParty is Level 3
  // and out of scope.
  final int sendResultsToDisc;

  // return
  final int answerId;
  // Return.releaseParamCaps/noFinishNeeded — only meaningful (and only
  // decoded; see parseRpcMessageFromReader) for isReturnResults/
  // isReturnException, matching the two Return variants that can legally
  // carry param-capability/answer-bookkeeping optimizations. Other Return
  // variants (canceled/resultsSentElsewhere/takeFromOtherQuestion/
  // acceptFromThirdParty) always report the wire defaults (true/false).
  final bool returnReleaseParamCaps;
  final bool returnNoFinishNeeded;
  final bool isReturnResults;
  final bool isReturnException;
  // Raw Return.disc (0=results, 1=exception, 2=canceled,
  // 3=resultsSentElsewhere, 4=takeFromOtherQuestion, 5=acceptFromThirdParty).
  // Only results and exception are handled as first-class outcomes above;
  // callers that need to distinguish canceled/resultsSentElsewhere/etc. from
  // an actual empty-results success (neither isReturnResults nor
  // isReturnException is true for either) must check this.
  final int returnDisc;
  final bool isReturnTakeFromOtherQuestion;
  final int takeFromOtherQuestion;
  final AnyPointerReader? resultsContent;
  final String? exceptionReason;
  // Populated wherever exceptionReason is (Return-exception, Resolve-
  // exception, Abort) from the wire Exception.type field, which is defined
  // (rpc.capnp) with the same 4 values, in the same order, as ErrorKind —
  // see rpc_message_codec.dart's build*ExceptionMessage/buildAbortMessage.
  final ErrorKind exceptionKind;
  // Semantic decode of the return payload's capTable entries — see
  // WireCapabilityReference's own doc comment for why this is the
  // representation code above the codec boundary should use instead of a
  // raw (disc, id) pair.
  final List<WireCapabilityReference> capabilityTableReferences;

  // resolve
  final int promiseId;
  final bool isResolveCap;
  final bool isResolveException;
  // Semantic decode of the Resolve message's cap — see
  // [capabilityTableReferences]'s doc comment.
  final WireCapabilityReference? resolutionCapabilityReference;

  // disembargo
  final int disembargoContextDisc;
  final int disembargoContextId;
  final int disembargoTargetImportId;
  final bool disembargoTargetIsPromisedAnswer;
  final int disembargoTargetPromisedAnswerQid;
  final List<int> disembargoTargetTransformPath;

  // finish
  final bool releaseResultCaps;

  // release
  final int releaseId;
  final int referenceCount;

  const RpcMessage._({
    required this.type,
    this.questionId = 0,
    this.interfaceId = 0,
    this.methodId = 0,
    this.targetImportId = 0,
    this.targetIsPromisedAnswer = false,
    this.targetPromisedAnswerQid = 0,
    this.targetTransformPath = const [],
    this.paramsContent,
    this.sendResultsToDisc = 0,
    this.answerId = 0,
    this.returnReleaseParamCaps = true,
    this.returnNoFinishNeeded = false,
    this.isReturnResults = false,
    this.isReturnException = false,
    this.returnDisc = _retResults,
    this.isReturnTakeFromOtherQuestion = false,
    this.takeFromOtherQuestion = 0,
    this.resultsContent,
    this.exceptionReason,
    this.exceptionKind = ErrorKind.failed,
    this.capabilityTableReferences = const [],
    this.promiseId = 0,
    this.isResolveCap = false,
    this.isResolveException = false,
    this.resolutionCapabilityReference,
    this.disembargoContextDisc = 0,
    this.disembargoContextId = 0,
    this.disembargoTargetImportId = 0,
    this.disembargoTargetIsPromisedAnswer = false,
    this.disembargoTargetPromisedAnswerQid = 0,
    this.disembargoTargetTransformPath = const [],
    this.releaseResultCaps = true,
    this.releaseId = 0,
    this.referenceCount = 0,
  });
}

// ---------------------------------------------------------------------------
// Public encode helpers
// ---------------------------------------------------------------------------

/// Serializes a Bootstrap message.
Uint8List buildBootstrapMessage(int questionId) {
  final mb = MessageBuilder(
    initialCapacityWords: _initialEnvelopeCapacityWords,
  );
  final msg = mb.initRoot(_rpcMessageFactory);
  msg.setDisc(_msgBootstrap);
  msg.initBootstrap().setQuestionId(questionId);
  return mb.serialize();
}

/// Serializes a Call message. [paramsBytes] is a standalone serialized struct.
///
/// **Target**: either importedCap or promisedAnswer (wire-level pipelining).
/// - To target an already-imported cap: set [targetImportId] (default).
/// - To target a pending question result: set [targetPromisedAnswerQid] and
///   [targetTransformPath] (the getPointerField hop sequence inside the
///   result struct — see [ReceiverAnswerCapabilityReference.transformPath]).
Uint8List buildCallMessage({
  required int questionId,
  int targetImportId = 0,
  int? targetPromisedAnswerQid,
  List<int> targetTransformPath = const [],
  required int interfaceId,
  required int methodId,
  required Uint8List paramsBytes,
  List<WireCapabilityReference> capabilityTableReferences = const [],
  bool sendResultsToYourself = false,
}) => buildCallMessageWithParamsBuilderSync(
  questionId: questionId,
  targetImportId: targetImportId,
  targetPromisedAnswerQid: targetPromisedAnswerQid,
  targetTransformPath: targetTransformPath,
  interfaceId: interfaceId,
  methodId: methodId,
  buildParams:
      (anyPtr) =>
          anyPtr.setMessageBytes(paramsBytes, preserveCapabilityPointers: true),
  references: capabilityTableReferences,
  sendResultsToYourself: sendResultsToYourself,
);

/// Like [buildCallMessage], but instead of deep-copying pre-built
/// [Uint8List] params bytes into the Call's `Payload.content`, invokes
/// [buildParams] with an [AnyPointerBuilder] pointing directly at that slot
/// — so the caller can build params content in place, with no separate
/// standalone message and no copy.
///
/// [resolveCapTable] is called *after* [buildParams] returns (so it can
/// read a capability list [buildParams] only finished populating as a side
/// effect — e.g. generic/typed fields whose capabilities aren't known until
/// their encoder runs) and *before* the message is serialized, to resolve
/// the wire capTable descriptors. It's async because that resolution may
/// need to wait on an import ID (see callers in `two_party_connection.dart`).
Future<Uint8List> buildCallMessageWithParamsBuilder({
  required int questionId,
  int targetImportId = 0,
  int? targetPromisedAnswerQid,
  List<int> targetTransformPath = const [],
  required int interfaceId,
  required int methodId,
  required void Function(AnyPointerBuilder) buildParams,
  required Future<List<WireCapabilityReference>> Function() resolveCapTable,
  bool sendResultsToYourself = false,
}) async {
  final (mb, params) = _beginCallMessage(
    questionId: questionId,
    targetImportId: targetImportId,
    targetPromisedAnswerQid: targetPromisedAnswerQid,
    targetTransformPath: targetTransformPath,
    interfaceId: interfaceId,
    methodId: methodId,
    sendResultsToYourself: sendResultsToYourself,
  );
  buildParams(params.initAnyPointerField(0));
  final references = await resolveCapTable();
  return _finishCallMessage(mb, params, references);
}

/// Synchronous counterpart of [buildCallMessageWithParamsBuilder] for callers that
/// already have a fully-resolved capTable (no async resolution needed) —
/// used by [buildCallMessage] itself.
Uint8List buildCallMessageWithParamsBuilderSync({
  required int questionId,
  int targetImportId = 0,
  int? targetPromisedAnswerQid,
  List<int> targetTransformPath = const [],
  required int interfaceId,
  required int methodId,
  required void Function(AnyPointerBuilder) buildParams,
  required List<WireCapabilityReference> references,
  bool sendResultsToYourself = false,
}) {
  final (mb, params) = _beginCallMessage(
    questionId: questionId,
    targetImportId: targetImportId,
    targetPromisedAnswerQid: targetPromisedAnswerQid,
    targetTransformPath: targetTransformPath,
    interfaceId: interfaceId,
    methodId: methodId,
    sendResultsToYourself: sendResultsToYourself,
  );
  buildParams(params.initAnyPointerField(0));
  return _finishCallMessage(mb, params, references);
}

/// Like [buildCallMessageWithParamsBuilder], but for callers whose capTable
/// references might be resolvable with no `await` at all — the common
/// case for an already-resolved call target with capability params that
/// are all locally resolvable already. Returns the finished bytes
/// synchronously in that case, instead of forcing every caller through a
/// `Future`/microtask; falls back to actually awaiting [resolveReferences]
/// only when it returns a `Future` (some param genuinely isn't resolvable
/// yet). [buildParams] always runs synchronously either way, so any
/// capabilities it appends as a side effect (see [buildCallMessageWithParamsBuilder]'s
/// doc comment) are visible to [resolveReferences] exactly as they would
/// be with the fully-async version.
FutureOr<Uint8List> buildCallMessageWithParamsBuilderMaybeSync({
  required int questionId,
  int targetImportId = 0,
  int? targetPromisedAnswerQid,
  List<int> targetTransformPath = const [],
  required int interfaceId,
  required int methodId,
  required void Function(AnyPointerBuilder) buildParams,
  required FutureOr<List<WireCapabilityReference>> Function() resolveReferences,
  bool sendResultsToYourself = false,
}) {
  final (mb, params) = _beginCallMessage(
    questionId: questionId,
    targetImportId: targetImportId,
    targetPromisedAnswerQid: targetPromisedAnswerQid,
    targetTransformPath: targetTransformPath,
    interfaceId: interfaceId,
    methodId: methodId,
    sendResultsToYourself: sendResultsToYourself,
  );
  buildParams(params.initAnyPointerField(0));
  final referencesOrFuture = resolveReferences();
  if (referencesOrFuture is List<WireCapabilityReference>) {
    return _finishCallMessage(mb, params, referencesOrFuture);
  }
  return referencesOrFuture.then(
    (references) => _finishCallMessage(mb, params, references),
  );
}

(MessageBuilder, _PayloadBuilder) _beginCallMessage({
  required int questionId,
  int targetImportId = 0,
  int? targetPromisedAnswerQid,
  List<int> targetTransformPath = const [],
  required int interfaceId,
  required int methodId,
  required bool sendResultsToYourself,
}) {
  final mb = MessageBuilder(
    initialCapacityWords: _initialEnvelopeCapacityWords,
  );
  final msg = mb.initRoot(_rpcMessageFactory);
  msg.setDisc(_msgCall);
  final call = msg.initCall();
  call.setQuestionId(questionId);
  call.setInterfaceId(interfaceId);
  call.setMethodId(methodId);
  if (sendResultsToYourself) {
    call.setSendResultsToYourself();
  } else {
    call.setSendResultsToCaller();
  }
  if (targetPromisedAnswerQid != null) {
    call.initTarget().setPromisedAnswer(
      targetPromisedAnswerQid,
      targetTransformPath,
    );
  } else {
    call.initTarget().setImportedCap(targetImportId);
  }
  return (mb, call.initParams());
}

Uint8List _finishCallMessage(
  MessageBuilder mb,
  _PayloadBuilder params,
  List<WireCapabilityReference> references,
) {
  if (references.isNotEmpty) {
    final capTable = params.initCapTable(references.length);
    for (int i = 0; i < references.length; i++) {
      _writeCapabilityReference(capTable[i], references[i]);
    }
  }
  return mb.serialize();
}

/// Serializes a Return-results message.
Uint8List buildReturnResultsMessage({
  required int answerId,
  required Uint8List resultsBytes,
}) => buildReturnResultsMessageFromReader(
  answerId: answerId,
  resultsRoot: MessageReader.deserialize(resultsBytes).getRootRaw(),
);

/// Like [buildReturnResultsMessage]/[buildReturnResultsWithCapabilityReferencesMessage],
/// but instead of deep-copying pre-built [Uint8List] results bytes into the
/// Return's `Payload.content`, copies the already-resolved struct
/// [resultsRoot] directly — skipping the serialize-then-reparse round trip a
/// bytes-based copy would need (e.g. when [resultsRoot] came from
/// [StructBuilder.rawToReader]'s zero-copy view onto a server's own
/// in-progress results builder). The final copy into the Return's own arena
/// still happens — [resultsRoot] and the Return are different arenas.
Uint8List buildReturnResultsMessageFromReader({
  required int answerId,
  required RawStructReader resultsRoot,
  List<WireCapabilityReference> references = const [],
  bool releaseParamCaps = true,
  bool noFinishNeeded = false,
}) {
  final mb = MessageBuilder(
    initialCapacityWords: _initialEnvelopeCapacityWords,
  );
  final msg = mb.initRoot(_rpcMessageFactory);
  msg.setDisc(_msgReturn);
  final ret = msg.initReturn();
  ret.setAnswerId(answerId);
  ret.setReleaseParamCaps(value: releaseParamCaps);
  ret.setNoFinishNeeded(value: noFinishNeeded);
  ret.setDiscResults();
  final payload = ret.initResults();
  payload.setContentFromRawStruct(resultsRoot);
  if (references.isNotEmpty) {
    final capTable = payload.initCapTable(references.length);
    for (int i = 0; i < references.length; i++) {
      _writeCapabilityReference(capTable[i], references[i]);
    }
  }
  return mb.serialize();
}

/// Serializes a Return-results message that includes capabilities in the
/// capTable. [exportIds] are the server-side export IDs, in capTable order.
Uint8List buildReturnResultsWithCapsMessage({
  required int answerId,
  required Uint8List resultsBytes,
  required List<int> exportIds,
  bool releaseParamCaps = true,
  bool noFinishNeeded = false,
}) {
  return buildReturnResultsWithCapabilityReferencesMessage(
    answerId: answerId,
    resultsBytes: resultsBytes,
    references: exportIds
        .map(SenderHostedCapabilityReference.new)
        .toList(growable: false),
    releaseParamCaps: releaseParamCaps,
    noFinishNeeded: noFinishNeeded,
  );
}

/// Serializes a `Return` message with a raw disc value and no payload —
/// covers every variant that isn't built through its own dedicated
/// builder ([buildReturnCanceledMessage], [buildReturnResultsSentElsewhereMessage],
/// [buildReturnTakeFromOtherQuestionMessage]) plus `acceptFromThirdParty`,
/// which this vat doesn't implement at all (see [describeReturnVariant]).
/// Used to test how a vat reacts to receiving one of these from a peer.
Uint8List buildRawReturnVariantMessage({
  required int answerId,
  required int disc,
}) {
  final mb = MessageBuilder(
    initialCapacityWords: _initialEnvelopeCapacityWords,
  );
  final msg = mb.initRoot(_rpcMessageFactory);
  msg.setDisc(_msgReturn);
  final ret = msg.initReturn();
  ret.setAnswerId(answerId);
  ret.setDiscRaw(disc);
  return mb.serialize();
}

/// Serializes a Return-results message with explicit capTable references.
Uint8List buildReturnResultsWithCapabilityReferencesMessage({
  required int answerId,
  required Uint8List resultsBytes,
  required List<WireCapabilityReference> references,
  bool releaseParamCaps = true,
  bool noFinishNeeded = false,
}) => buildReturnResultsMessageFromReader(
  answerId: answerId,
  resultsRoot: MessageReader.deserialize(resultsBytes).getRootRaw(),
  references: references,
  releaseParamCaps: releaseParamCaps,
  noFinishNeeded: noFinishNeeded,
);

/// Serializes a Resolve message resolving [promiseId] to [reference].
Uint8List buildResolveCapMessage({
  required int promiseId,
  required WireCapabilityReference reference,
}) {
  final mb = MessageBuilder(
    initialCapacityWords: _initialEnvelopeCapacityWords,
  );
  final msg = mb.initRoot(_rpcMessageFactory);
  msg.setDisc(_msgResolve);
  final resolve = msg.initResolve();
  resolve.setPromiseId(promiseId);
  resolve.setDiscCap();
  _writeCapabilityReference(resolve.initCap(), reference);
  return mb.serialize();
}

/// Serializes a Resolve message resolving [promiseId] to an exception.
Uint8List buildResolveExceptionMessage({
  required int promiseId,
  required String reason,
  ErrorKind kind = ErrorKind.failed,
}) {
  final mb = MessageBuilder(
    initialCapacityWords: _initialEnvelopeCapacityWords,
  );
  final msg = mb.initRoot(_rpcMessageFactory);
  msg.setDisc(_msgResolve);
  final resolve = msg.initResolve();
  resolve.setPromiseId(promiseId);
  resolve.setDiscException();
  final exc = resolve.initException();
  exc.setType(kind.index);
  exc.setReason(reason);
  return mb.serialize();
}

/// Serializes a Disembargo message.
///
/// [contextDisc] is one of the rpc.capnp context union discriminants:
/// senderLoopback=0, receiverLoopback=1, accept=2, provide=3.
Uint8List buildDisembargoMessage({
  int targetImportId = 0,
  int? targetPromisedAnswerQid,
  List<int> targetTransformPath = const [],
  required int contextDisc,
  required int contextId,
}) {
  final mb = MessageBuilder(
    initialCapacityWords: _initialEnvelopeCapacityWords,
  );
  final msg = mb.initRoot(_rpcMessageFactory);
  msg.setDisc(_msgDisembargo);
  final disembargo = msg.initDisembargo();
  disembargo.setContext(contextDisc, contextId);
  if (targetPromisedAnswerQid != null) {
    disembargo.initTarget().setPromisedAnswer(
      targetPromisedAnswerQid,
      targetTransformPath,
    );
  } else {
    disembargo.initTarget().setImportedCap(targetImportId);
  }
  return mb.serialize();
}

/// Serializes a Return-results message for a Bootstrap call.
/// [exportId] is the server's export table ID for the capability.
Uint8List buildBootstrapReturnMessage({
  required int answerId,
  required int exportId,
}) {
  final mb = MessageBuilder(
    initialCapacityWords: _initialEnvelopeCapacityWords,
  );
  final msg = mb.initRoot(_rpcMessageFactory);
  msg.setDisc(_msgReturn);
  final ret = msg.initReturn();
  ret.setAnswerId(answerId);
  ret.setDiscResults();
  final payload = ret.initResults();
  // Empty content (no struct); capability is in capTable.
  final capTable = payload.initCapTable(1);
  capTable[0].setSenderHosted(exportId);
  return mb.serialize();
}

/// Serializes a Return-exception message.
Uint8List buildReturnExceptionMessage({
  required int answerId,
  required String reason,
  ErrorKind kind = ErrorKind.failed,
  bool releaseParamCaps = true,
  // Exceptions never carry a results payload/capTable, so it's always
  // *wire-format*-safe to set this — but the sender must also make sure it
  // doesn't need the peer's Finish for its own answer-table bookkeeping
  // (see incoming_call_coordinator.dart's _executeIncomingDispatch, the only call site that
  // passes true) before opting in. Defaults to false so every other call
  // site (unknown-export-id/pipelining-error/tail-call-error Returns, which
  // still rely on Finish to clear their `_answers` entry) keeps its
  // existing behavior unchanged.
  bool noFinishNeeded = false,
}) {
  final mb = MessageBuilder(
    initialCapacityWords: _initialEnvelopeCapacityWords,
  );
  final msg = mb.initRoot(_rpcMessageFactory);
  msg.setDisc(_msgReturn);
  final ret = msg.initReturn();
  ret.setAnswerId(answerId);
  ret.setReleaseParamCaps(value: releaseParamCaps);
  ret.setNoFinishNeeded(value: noFinishNeeded);
  ret.setDiscException();
  final exc = ret.initException();
  exc.setType(kind.index);
  exc.setReason(reason);
  return mb.serialize();
}

/// Serializes a Return with `takeFromOtherQuestion` set.
///
/// Used when this vat, after receiving a Call, forwards it as a tail call to
/// another capability on the SAME peer connection: the real answer will
/// arrive via that forwarded call's own answer, tracked under [questionId].
Uint8List buildReturnTakeFromOtherQuestionMessage({
  required int answerId,
  required int questionId,
}) {
  final mb = MessageBuilder(
    initialCapacityWords: _initialEnvelopeCapacityWords,
  );
  final msg = mb.initRoot(_rpcMessageFactory);
  msg.setDisc(_msgReturn);
  final ret = msg.initReturn();
  ret.setAnswerId(answerId);
  ret.setTakeFromOtherQuestion(questionId);
  return mb.serialize();
}

/// Serializes a Return with `resultsSentElsewhere` set.
///
/// Sent in response to an incoming Call whose `sendResultsTo` was
/// `yourself`: the real results were/are being delivered to whichever of
/// this vat's own outgoing calls the peer correlates via
/// `takeFromOtherQuestion`, not via this Return.
Uint8List buildReturnResultsSentElsewhereMessage({required int answerId}) {
  final mb = MessageBuilder(
    initialCapacityWords: _initialEnvelopeCapacityWords,
  );
  final msg = mb.initRoot(_rpcMessageFactory);
  msg.setDisc(_msgReturn);
  final ret = msg.initReturn();
  ret.setAnswerId(answerId);
  ret.setDiscRaw(_retResultsSentElsewhere);
  return mb.serialize();
}

/// Serializes a Return with `canceled` set.
///
/// Sent when this vat accepts a peer's premature Finish (received before
/// the call it names had completed) and, once that call's dispatch
/// actually settles, chooses not to answer it for real — see
/// `AnswerTable.handleRequesterFinishedAnswer`/`handleDispatchSucceeded`'s
/// `AnswerAlreadyFinished` case and `IncomingCallCoordinator._sendCanceledReturn`. Deliberately
/// not sent the moment Finish arrives: `releaseParamCaps` isn't knowable
/// until the dispatch's params-capability disposal tracking resolves, and
/// sending any Return earlier would let the peer legally reuse `answerId`
/// before this vat's own pending dispatch has actually finished with it.
Uint8List buildReturnCanceledMessage({
  required int answerId,
  bool releaseParamCaps = true,
}) {
  final mb = MessageBuilder(
    initialCapacityWords: _initialEnvelopeCapacityWords,
  );
  final msg = mb.initRoot(_rpcMessageFactory);
  msg.setDisc(_msgReturn);
  final ret = msg.initReturn();
  ret.setAnswerId(answerId);
  ret.setReleaseParamCaps(value: releaseParamCaps);
  ret.setDiscRaw(_retCanceled);
  return mb.serialize();
}

/// Serializes a Finish message.
Uint8List buildFinishMessage(int questionId, {bool releaseResultCaps = true}) {
  final mb = MessageBuilder(
    initialCapacityWords: _initialEnvelopeCapacityWords,
  );
  final msg = mb.initRoot(_rpcMessageFactory);
  msg.setDisc(_msgFinish);
  final finish = msg.initFinish();
  finish.setQuestionId(questionId);
  finish.setReleaseResultCaps(value: releaseResultCaps);
  return mb.serialize();
}

/// Serializes a Release message.
Uint8List buildReleaseMessage(int id, int referenceCount) {
  final mb = MessageBuilder(
    initialCapacityWords: _initialEnvelopeCapacityWords,
  );
  final msg = mb.initRoot(_rpcMessageFactory);
  msg.setDisc(_msgRelease);
  final rel = msg.initRelease();
  rel.setId(id);
  rel.setReferenceCount(referenceCount);
  return mb.serialize();
}

/// Serializes an Unimplemented message that echoes [originalMessageBytes].
///
/// Per the Cap'n Proto RPC spec, when a peer receives a message it cannot
/// handle, it should echo the original message back inside an Unimplemented
/// envelope (disc=0) so the sender knows which message was not understood.
Uint8List buildUnimplementedMessage(Uint8List originalMessageBytes) {
  final mb = MessageBuilder(
    initialCapacityWords: _initialEnvelopeCapacityWords,
  );
  final msg = mb.initRoot(_rpcMessageFactory);
  msg.setDisc(_msgUnimplemented);
  msg.setUnimplementedPayload(originalMessageBytes);
  return mb.serialize();
}

/// Serializes an Abort message.
Uint8List buildAbortMessage(
  String reason, {
  ErrorKind kind = ErrorKind.failed,
}) {
  final mb = MessageBuilder(
    initialCapacityWords: _initialEnvelopeCapacityWords,
  );
  final msg = mb.initRoot(_rpcMessageFactory);
  msg.setDisc(_msgAbort);
  final exc = msg.initAbort();
  exc.setType(kind.index);
  exc.setReason(reason);
  return mb.serialize();
}

/// Decodes [reader] into the semantic [WireCapabilityReference] the RPC
/// implementation above the codec boundary uses — see that sealed class's
/// own doc comment for why this is a distinct representation from the
/// schema-facing `_CapDescriptorReader`. Every discriminant this vat doesn't
/// decode further (a future/Level 2+ variant, or one simply not implemented yet)
/// becomes [UnsupportedCapabilityReference], preserving the raw
/// discriminant instead of losing it to a generic "none".
WireCapabilityReference _readCapabilityReference(_CapDescriptorReader reader) {
  return switch (reader.disc) {
    _capDescNone => const NoCapabilityReference(),
    _capDescSenderHosted => SenderHostedCapabilityReference(reader.id),
    _capDescSenderPromise => SenderPromiseCapabilityReference(reader.id),
    _capDescReceiverHosted => ReceiverHostedCapabilityReference(reader.id),
    _capDescReceiverAnswer => ReceiverAnswerCapabilityReference(
      reader.receiverAnswer?.questionId ?? 0,
      _readTransformPath(reader.receiverAnswer?.transform),
    ),
    final disc => UnsupportedCapabilityReference(disc),
  };
}

/// Encodes [reference] into [builder] — the inverse of
/// [_readCapabilityReference]. [UnsupportedCapabilityReference] can only
/// ever result from decoding an incoming message (see that variant's own
/// doc comment), so encoding one is a programming error, not a condition an
/// outgoing message could legitimately hit.
void _writeCapabilityReference(
  _CapDescriptorBuilder builder,
  WireCapabilityReference reference,
) {
  switch (reference) {
    case NoCapabilityReference():
      builder.setUint16Field(_capDescDisc, _capDescNone);
    case SenderHostedCapabilityReference(:final exportId):
      builder.setSenderHosted(exportId);
    case SenderPromiseCapabilityReference(:final exportId):
      builder.setSenderPromise(exportId);
    case ReceiverHostedCapabilityReference(:final importId):
      builder.setReceiverHosted(importId);
    case ReceiverAnswerCapabilityReference(
      :final questionId,
      :final transformPath,
    ):
      builder.setReceiverAnswer(questionId, transformPath);
    case UnsupportedCapabilityReference(:final discriminant):
      throw ArgumentError(
        'unsupported capability reference cannot be encoded (disc=$discriminant)',
      );
  }
}

/// Collects every `getPointerField` index from [transform], in order,
/// skipping `noop` entries (which per rpc.capnp's `PromisedAnswer.Op`
/// contribute no hop) — the full multi-hop path a transform describes, not
/// just its first operation. An empty/all-noop transform (or no transform
/// at all) yields an empty path; callers normalize that themselves (see
/// two_party_connection.dart) since what an empty path *means* depends on
/// context — for a real method's result it can never legitimately occur
/// (results are always a struct, so reaching a capability field always
/// takes at least one hop), but a Bootstrap answer's content genuinely *is*
/// the capability with no wrapping struct, so this vat's internal
/// single-capability wrapper representation for that case treats an empty
/// path as reaching pointer slot 0 of the wrapper.
List<int> _readTransformPath(ListReader<_OpReader>? transform) {
  if (transform == null) return const [];
  final path = <int>[];
  for (var i = 0; i < transform.length; i++) {
    final op = transform[i];
    if (op.disc == 1) path.add(op.ptrIndex);
  }
  return path;
}

// ---------------------------------------------------------------------------
// Public decode
// ---------------------------------------------------------------------------

/// Parses a raw RPC message from bytes.
RpcMessage parseRpcMessage(Uint8List bytes) =>
    parseRpcMessageFromReader(MessageReader.deserialize(bytes));

/// Parses an RPC message from an already-deserialized [MessageReader].
RpcMessage parseRpcMessageFromReader(MessageReader mr) {
  final msg = mr.getRoot(_rpcMessageFactory);

  switch (msg.disc) {
    case _msgBootstrap:
      return RpcMessage._(
        type: RpcMessageType.bootstrap,
        questionId: msg.asBootstrap?.questionId ?? 0,
      );

    case _msgCall:
      final call = msg.asCall;
      final target = call?.target;
      final params = call?.params;
      final callCapTable = params?.capTable;
      final capabilityTableReferences = <WireCapabilityReference>[];
      if (callCapTable != null) {
        for (int i = 0; i < callCapTable.length; i++) {
          capabilityTableReferences.add(
            _readCapabilityReference(callCapTable[i]),
          );
        }
      }
      final isPA = (target?.disc ?? 0) == _targetPromisedAnswer;
      int paQid = 0;
      var paPath = const <int>[];
      if (isPA) {
        final pa = target?.promisedAnswer;
        paQid = pa?.questionId ?? 0;
        paPath = _readTransformPath(pa?.transform);
      }
      return RpcMessage._(
        type: RpcMessageType.call,
        questionId: call?.questionId ?? 0,
        interfaceId: call?.interfaceId ?? 0,
        methodId: call?.methodId ?? 0,
        targetImportId: isPA ? 0 : (target?.importedCap ?? 0),
        targetIsPromisedAnswer: isPA,
        targetPromisedAnswerQid: paQid,
        targetTransformPath: paPath,
        paramsContent: params?.content,
        capabilityTableReferences: capabilityTableReferences,
        sendResultsToDisc: call?.sendResultsToDisc ?? 0,
      );

    case _msgReturn:
      final ret = msg.asReturn;
      final retDisc = ret?.disc ?? 0;
      if (retDisc == _retResults) {
        final payload = ret?.results;
        final capTable = payload?.capTable;
        final capabilityTableReferences = <WireCapabilityReference>[];
        if (capTable != null) {
          for (int i = 0; i < capTable.length; i++) {
            capabilityTableReferences.add(
              _readCapabilityReference(capTable[i]),
            );
          }
        }
        return RpcMessage._(
          type: RpcMessageType.return_,
          answerId: ret?.answerId ?? 0,
          returnReleaseParamCaps: ret?.releaseParamCaps ?? true,
          returnNoFinishNeeded: ret?.noFinishNeeded ?? false,
          isReturnResults: true,
          returnDisc: retDisc,
          resultsContent: payload?.content,
          capabilityTableReferences: capabilityTableReferences,
        );
      } else if (retDisc == _retException) {
        final exc = ret?.exception;
        return RpcMessage._(
          type: RpcMessageType.return_,
          answerId: ret?.answerId ?? 0,
          returnReleaseParamCaps: ret?.releaseParamCaps ?? true,
          returnNoFinishNeeded: ret?.noFinishNeeded ?? false,
          isReturnException: true,
          returnDisc: retDisc,
          exceptionReason: exc?.reason ?? 'unknown error',
          exceptionKind: _errorKindFromWire(exc?.type ?? 0),
        );
      } else if (retDisc == _retTakeFromOtherQuestion) {
        return RpcMessage._(
          type: RpcMessageType.return_,
          answerId: ret?.answerId ?? 0,
          returnDisc: retDisc,
          isReturnTakeFromOtherQuestion: true,
          takeFromOtherQuestion: ret?.takeFromOtherQuestion ?? 0,
        );
      } else {
        // canceled(2) / resultsSentElsewhere(3) / acceptFromThirdParty(5) —
        // canceled is the only one of these this vat itself ever sends (see
        // buildReturnCanceledMessage); none are otherwise interpreted as a
        // real outcome (see OutgoingCallCoordinator's internal
        // _awaitAndProcessReturn), but the disc is still preserved here
        // rather than silently discarded, so callers can report exactly
        // what happened. releaseParamCaps is a top-level Return field valid
        // for every variant, not just results/exception, so it's read the
        // same way here too.
        return RpcMessage._(
          type: RpcMessageType.return_,
          answerId: ret?.answerId ?? 0,
          returnReleaseParamCaps: ret?.releaseParamCaps ?? true,
          returnDisc: retDisc,
        );
      }

    case _msgResolve:
      final resolve = msg.asResolve;
      final resolveDisc = resolve?.disc ?? _resolveCap;
      if (resolveDisc == _resolveException) {
        final exc = resolve?.exception;
        return RpcMessage._(
          type: RpcMessageType.resolve,
          promiseId: resolve?.promiseId ?? 0,
          isResolveException: true,
          exceptionReason: exc?.reason ?? 'promise resolved to exception',
          exceptionKind: _errorKindFromWire(exc?.type ?? 0),
        );
      }
      final cap = resolve?.cap;
      return RpcMessage._(
        type: RpcMessageType.resolve,
        promiseId: resolve?.promiseId ?? 0,
        isResolveCap: true,
        resolutionCapabilityReference:
            cap == null ? null : _readCapabilityReference(cap),
      );

    case _msgFinish:
      final finish = msg.asFinish;
      return RpcMessage._(
        type: RpcMessageType.finish,
        questionId: finish?.questionId ?? 0,
        releaseResultCaps: finish?.releaseResultCaps ?? true,
      );

    case _msgRelease:
      final rel = msg.asRelease;
      return RpcMessage._(
        type: RpcMessageType.release,
        releaseId: rel?.id ?? 0,
        referenceCount: rel?.referenceCount ?? 0,
      );

    case _msgDisembargo:
      final disembargo = msg.asDisembargo;
      final target = disembargo?.target;
      final isPA = (target?.disc ?? 0) == _targetPromisedAnswer;
      int paQid = 0;
      var paPath = const <int>[];
      if (isPA) {
        final pa = target?.promisedAnswer;
        paQid = pa?.questionId ?? 0;
        paPath = _readTransformPath(pa?.transform);
      }
      return RpcMessage._(
        type: RpcMessageType.disembargo,
        disembargoContextDisc: disembargo?.contextDisc ?? 0,
        disembargoContextId: disembargo?.contextId ?? 0,
        disembargoTargetImportId: isPA ? 0 : (target?.importedCap ?? 0),
        disembargoTargetIsPromisedAnswer: isPA,
        disembargoTargetPromisedAnswerQid: paQid,
        disembargoTargetTransformPath: paPath,
      );

    case _msgAbort:
      return RpcMessage._(
        type: RpcMessageType.abort,
        exceptionReason: msg.asAbort?.reason ?? 'peer aborted',
        exceptionKind: _errorKindFromWire(msg.asAbort?.type ?? 0),
      );

    case _msgUnimplemented:
      return const RpcMessage._(type: RpcMessageType.unimplemented);

    default:
      return const RpcMessage._(type: RpcMessageType.other);
  }
}
