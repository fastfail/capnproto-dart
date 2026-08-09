# Purpose

## Problem

Developers who want to use Cap'n Proto in Flutter applications currently have no native Dart implementation available. The only option is to call C++ or Rust Cap'n Proto libraries via FFI, which introduces the following problems:

- **Complex software stack**: Bridging Dart and native code via FFI adds significant architectural complexity.
- **Difficult debugging**: Crossing the FFI boundary makes it hard to trace issues, inspect state, and use standard Dart debugging tools.
- **Thread constraint mismatch**: Dart's FFI threading model is incompatible with the threading constraints of existing Cap'n Proto libraries, leading to subtle and hard-to-fix concurrency issues.

## Solution

This repository provides a pure Dart implementation of Cap'n Proto, eliminating the need for FFI entirely.

## Benefits

- **Easier debugging**: Developers can use standard Dart/Flutter debugging tools without dealing with native code boundaries.
- **Simpler build and integration**: No need to compile or link native libraries; the package integrates like any other Dart package.
- **Improved performance**: Removing the FFI crossing overhead results in faster serialization and deserialization.
- **No thread constraint issues**: Avoids the incompatibility between Dart's FFI threading model and Cap'n Proto library threading constraints.
