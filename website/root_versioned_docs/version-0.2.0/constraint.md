# Constraints

## Technical Constraints

- **Language**: Dart — use the latest stable version as of 2026-07-14.
- **Framework**: Flutter — use the latest stable version as of 2026-07-14. The runtime library must be compatible with Flutter applications.
- **Cap'n Proto specification**: Conform to the latest stable Cap'n Proto specification as of 2026-07-14.
- **Pure Dart**: No FFI bindings to C++, Rust, or any other native language. The entire implementation must be written in Dart.
- **External dependencies**: Keep third-party package dependencies to a minimum to reduce maintenance burden and improve long-term stability.

## Business Constraints

- **Schedule**: No fixed deadlines or milestones. This is a personal project.
- **Team**: Single developer.
- **Quality**: The project is intended to be released as open-source software (OSS). A level of code quality, documentation, and test coverage appropriate for public OSS must be maintained throughout development.

## Non-Functional Requirements

### Performance
- Target the performance of the reference C++ Cap'n Proto implementation as a benchmark.
- Where the Dart language itself imposes unavoidable overhead compared to C++, a reasonable degradation is acceptable, provided it is measured and documented.

### Maintainability
- The design must be resilient to version upgrades of Dart, Flutter, and the Cap'n Proto specification. Avoid tight coupling to version-specific APIs or behaviors.
- Code must be readable and well-structured to support long-term maintenance by the author and potential future contributors.

### Testability
- All features must be covered by automated tests (unit and integration) to ensure correctness and prevent regressions.
