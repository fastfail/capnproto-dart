# AI Rules

This document defines the rules that AI must follow when working in this repository.

## 1. Code Changes

- Must not modify or delete code without explicit user approval.
- Must not add or modify implementation code without corresponding tests.
- Must not implement features outside the defined scope.

## 2. Documentation

- Must not modify documents under `docs/` without user approval.
- Must not leave documentation that diverges from the implementation.
- All documents must be written in English.

## 3. Git Operations

- Must not execute `git push` without explicit user instruction.
- Must not execute destructive Git operations such as `git reset --hard`.
- Must not determine commit messages without user confirmation.

## 4. External Services / APIs

- Must not make requests to external APIs without user approval.

## 5. Naming

- Variable, function, and class names must be descriptive enough to convey their purpose at a glance. Avoid generic names such as `tmp`, `data`, and `flag`.
- Boolean values must use prefixes such as `is_`, `has_`, or `can_` to make their truth values clear.
- Function names must start with a verb and clearly indicate what they do.

## 6. Comments

- Comments must explain *why*, not *what*.
- Must not write comments that merely restate what the code already makes clear.

## 7. Function and Class Design

- Functions must have a single responsibility (Single Responsibility Principle).
- Functions must have as few parameters as possible (guideline: 3 or fewer).
- Nesting must be kept shallow; use early returns where appropriate.
- Must not use magic numbers or magic strings; use named constants instead.

## 8. Architecture and Dependencies

- Business logic must not depend on external details such as frameworks, databases, or UI layers.
- Must depend on abstractions (interfaces), not on concrete implementations (Dependency Inversion Principle).
- Must not duplicate the same logic in multiple places (DRY Principle).
- Must not pre-implement features that are not currently needed (YAGNI Principle).

## 9. Error Handling

- Must not suppress errors; ignoring exceptions or error values is prohibited.
- Errors must be detected and reported as early as possible (Fail Fast).
