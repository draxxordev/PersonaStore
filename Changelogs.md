# Changelogs

All notable changes to PersonaStore will be documented here.

This project follows **Semantic Versioning** (`MAJOR.MINOR.PATCH`).

---

# v1.0.0
*Initial Release*

## Added

- Initial PersonaStore engine.
- OOP architecture consisting of `PersonaStore`, `Founder`, and `DataSession`.
- Session locking with automatic ownership validation.
- Automatic schema reconciliation.
- Deep observable proxy tree for nested table change detection.
- Automatic patch-based autosaving.
- Full profile saves via `Save()`.
- Selective patch saves via `SavePatch()`.
- Transaction system with rollback support.
- Cross-server global update system.
- Offline global update queue.
- MessagingService cluster synchronization.
- Runtime performance metadata.
- Cache freshness verification.
- Exponential retry layer with randomized backoff.
- Automatic BindToClose session draining.
- Multiple isolated DataStore support.
- Full API documentation.
- FAQ section.
- Examples and Getting Started guide.

---

## Changed

- N/A

---

## Fixed

- N/A

---

## Removed

- N/A
