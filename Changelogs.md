# Changelogs

All notable changes to PersonaStore will be documented here.
This project follows **Semantic Versioning** (`MAJOR.MINOR.PATCH`).

---

# v1.1.0
*EncodingService Integration & Advanced Features*

## Added

### Core Compression Features
- Native Roblox `EncodingService` integration for efficient data compression.
- `DataSession:SaveCompressed()` method for bandwidth-optimized saves.
- Support for multiple compression algorithms: `Deflate` and `ZSTD`.
- Configurable compression levels (1-22 depending on algorithm).
- Compression metadata tracking (algorithm, original size, compressed size).
- Automatic compression cache to avoid recompressing unchanged data.
- `PersonaStore:SetCompressionSettings(algorithm, level)` for global configuration.

### Data Integrity & Security
- Cryptographic hash verification using `EncodingService:ComputeStringHash()`.
- Support for multiple hash algorithms: `SHA256` (default), `SHA1`, `MD5`.
- `DataSession:VerifyDataIntegrity()` method to detect data corruption.
- Automatic hash computation on every `Save()` and `SaveCompressed()`.
- Configurable integrity checking via `PersonaStore.EnableDataIntegrityChecks`.

### Read-Only Access
- `Founder:LoadReadOnlySession(key)` for lock-free profile queries.
- Perfect for leaderboards, admin panels, and data aggregation.
- Zero lock conflict overhead.
- Full profile data access without ownership constraints.

### Atomic Operations
- `DataSession:IncrementCounter(fieldName, amount)` for thread-safe increments.
- Default increment of 1 if amount not specified.
- Automatic transaction handling and saving.
- Ideal for leaderboards, currency systems, and statistics.

### Batch Operations
- `Founder:BatchUpdate(keys, transformFn)` for mass profile updates.
- Atomic updates with automatic lock management per profile.
- Returns success/failure status per key.
- Useful for seasonal resets, migrations, and event-based updates.

### Data Export & Import
- `DataSession:ExportData(includeMetadata)` for profile backup as JSON.
- `DataSession:ImportData(jsonString, overwrite)` to restore from backup.
- Optional metadata inclusion for audit trails.
- Merge mode (append) or overwrite mode for imports.

### Profile Metadata Queries
- `Founder:GetKeyMetadata(key)` to query profile info without loading.
- Returns version, last update time, session token, JobId, hash, and compression info.
- Efficient diagnostics and monitoring without lock acquisition.

### Statistics & Monitoring
- `PersonaStore:GetStatistics()` for engine-wide metrics.
- Tracks total saves, loads, compressions, decompressions.
- DataStore request attempt/failure counts.
- Total bytes saved through compression.
- `Founder:GetCompressionStats()` for store-specific compression data.
- Real-time performance monitoring for infrastructure teams.

### Enhanced Session Metadata
- Expanded `GetPerformanceMetadata()` with compression statistics.
- Tracks last compressed/uncompressed sizes per session.
- More detailed session lifecycle information.

### Base64 Encoding Support
- Built-in Base64 encoding/decoding for compressed data.
- Seamless integration with JSON-safe DataStore format.
- Automatic handling via `CompressionHandler`.

## Changed
- Improved retry logic with better exponential backoff calculation.
- Enhanced error messages for clearer debugging.
- `DataSession` now invalidates compression cache on mutations.
- Profile structure now includes `DataHash` and `CompressionMetadata` fields.
- `PersonaStore` now tracks comprehensive statistics.

## Fixed
- Retry mechanism now properly tracks DataStore request failures.

## Removed
- N/A

### Migration Notes for v1.0.0 → v1.1.0
**100% Backward Compatible** - No breaking changes. Existing code continues to work without modification.

New features are opt-in and can be adopted gradually:
```lua
-- v1.0.0 code continues to work
session:SavePatch()

-- New v1.1.0 features available when needed
session:SaveCompressed()  -- New compression
session:VerifyDataIntegrity()  -- New integrity check
```

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
  - `BeginTransaction()` for snapshot creation.
  - `CommitTransaction()` to finalize changes.
  - `RollbackTransaction()` to revert to snapshot.
- Cross-server global update system.
- Offline global update queue.
- MessagingService cluster synchronization.
- Runtime performance metadata via `GetPerformanceMetadata()`.
- Cache freshness verification via `IsCacheStale()`.
- Exponential retry layer with randomized backoff.
- Automatic BindToClose session draining.
- Multiple isolated DataStore support via `CreateDataStore()`.
- Event listeners:
  - `ListenToFieldChange()` for data mutations.
  - `ListenToGlobalUpdate()` for cluster updates.
- Session lifecycle:
  - `LoadSession()` for immediate acquire.
  - `LoadSessionAsync()` for wait-based acquire.
  - `Release()` for manual cleanup.
  - `Destroy()` for save and cleanup.

---

## Changed
- N/A

---

## Fixed
- N/A

---

## Removed
- N/A

---

## Versioning Policy

- **MAJOR**: Breaking API changes or complete rewrites
- **MINOR**: New features that are backward compatible
- **PATCH**: Bug fixes and performance improvements

---
