# Changelogs

All notable changes to PersonaStore will be documented here.
This project follows **Semantic Versioning** (`MAJOR.MINOR.PATCH`).

---

# v1.2.0

## Added
*OrderedDataStore, MemoryStore, Version APIs, and configurable integrity hashing*

---

## Changed
- `withRetry()` no longer sleeps with exponential backoff after its *final* failed attempt (it used to wait needlessly before returning failure).
- Profile schema (`profileRaw`) now includes a `FieldHashes` table alongside the existing `DataHash`.
- `Founder:BatchUpdate()` no longer calls `SavePatch()` immediately before `Destroy()` — `Destroy()` already performs a full `Save()`, so every batched key was previously being written to the DataStore twice.

---

## Fixed
- **Require guard threw the wrong error / retried needlessly.** The server-only guard was calling `withRetry(task.spawn(function() ... end))`, which passes a *thread* into `withRetry`. `withRetry` does `pcall(thread)` internally, and a thread isn't callable, so every one of its 5 attempts failed immediately (burning through exponential backoff) before finally surfacing an unrelated pcall error instead of the intended "server only" message. Fixed to call `withRetry` with an actual function and `maxAttempts = 1`.
- **Decompression could use the wrong algorithm.** `CompressionHandler.decompress()` / `getDecompressedSize()` always decompressed using the *current global* `PersonaStore.CompressionAlgorithm` rather than whichever algorithm a given profile was actually compressed with. Calling `SetCompressionSettings()` to change the global algorithm made every previously-compressed profile fail to decompress correctly. Fixed by recording the algorithm's serialized name in `CompressionMetadata.Algorithm` at compress time and resolving it back to the correct `Enum.CompressionAlgorithm` at read time (`LoadSession`, `LoadReadOnlySnapshot`, and the new `GetVersionAsync` all go through this now).
- **`VerifyDataIntegrity()` almost always returned `false`.** It hashed the live observable proxy (`self.Data`) directly via `HttpService:JSONEncode`, but every `Save()` path hashes `deepCopy(self.Data)` — a plain table. `JSONEncode` doesn't respect the proxy's `__iter` metamethod, so the two hashes were computed over different representations and essentially never matched. Fixed to `deepCopy` before hashing, consistent with the save paths.
- **`BatchUpdate()` wrote every key twice.** See "Changed" above.
- **Every save that computed an integrity hash failed with `CantStoreValue`.** `CompressionHandler.hashData()` returned the raw digest from `EncodingService:ComputeStringHash()` as-is, and stored it directly in `DataHash`/`FieldHashes`. Hash digests are effectively random bytes, not guaranteed valid UTF-8, and Roblox DataStores reject any string that isn't valid UTF-8 — so `Save()`/`SavePatch()` would fail on `UpdateAsync` every single time `PersonaStore.EnableDataIntegrityChecks` was on (the default). Fixed by base64-encoding the digest before it's stored or compared, the same approach already used for compressed buffer data.

---

### Configurable Integrity Hashing
- New `PersonaStore.IntegrityMode` setting with three modes:
  - `"Full"` *(default, back-compat)* — rehashes the entire profile on every `SavePatch()`, same as v1.1.x.
  - `"HashOnlyOnFullSave"` — `SavePatch()` skips hashing entirely; `DataHash` only refreshes the next time `Save()` runs.
  - `"PerField"` — `SavePatch()` only rehashes the fields that were actually dirtied, tracked in a new `FieldHashes` table. A profile with a 50,000-item `Inventory` no longer pays to rehash it just because `Coins` changed.
- `PersonaStore:SetIntegrityMode(mode)` to switch modes at runtime.
- `DataSession:VerifyDataIntegrity()` is now integrity-mode-aware, checking `FieldHashes` in `"PerField"` mode instead of the whole-profile hash.
- `Founder:GetKeyMetadata()` now also returns `FieldHashes`.

---

### OrderedDataStore Support
- New `OrderedFounder` class wrapping `DataStoreService:GetOrderedDataStore()`.
- `PersonaStore:CreateOrderedDataStore(storeName)` factory, registered/cached like `CreateDataStore()`.
- `OrderedFounder:Set(key, value)`, `:Get(key)`, `:Increment(key, delta)`, `:Remove(key)`.
- `OrderedFounder:GetSortedPage(ascending, pageSize, minValue, maxValue)` for paginated leaderboard reads, returning both the current page and the native `DataStorePages` object for further pagination.

---

### MemoryStoreService Support
- New `MemoryQueueWrapper` over `MemoryStoreService:GetQueue()`.
  - `PersonaStore:CreateMemoryQueue(name)` factory.
  - `:AddItem(value, expirationSeconds, priority)`, `:ReadItems(count, allOrNothing, waitTimeoutSeconds)` (returns `items, receiptId`), `:RemoveItems(receiptId)`.
- New `MemorySortedMapWrapper` over `MemoryStoreService:GetSortedMap()`.
  - `PersonaStore:CreateMemorySortedMap(name)` factory.
  - `:Set(key, value, expirationSeconds, sortKey)`, `:Get(key)`, `:Remove(key)`, `:GetRange(direction, count, exclusiveLowerBound, exclusiveUpperBound)`, `:Update(key, transformFn, expirationSeconds)`.
- All MemoryStore calls go through the same `withRetry` backoff wrapper as DataStore calls.

---

### DataStore Version APIs
- `Founder:ListVersionsAsync(key, sortDirection, minDate, maxDate, pageSize)` — returns the native `DataStoreVersionPages` object.
- `Founder:GetVersionAsync(key, version)` — returns a deep copy of a historical version, automatically decompressed if that version was stored compressed.
- `Founder:RemoveVersionAsync(key, version)` — permanently deletes a historical version.

---

### DataStore Version API Notes

* `ListVersionsAsync()` relies on Roblox's DataStore version history index, which may be eventually consistent after writes.
* Newly created versions may not appear immediately after a successful `Save()` operation, especially in Studio.
* PersonaStore's internal revision tracking remains authoritative for save ordering and mutation tracking.
* Consumers using version enumeration for administrative tools or recovery workflows should retry `ListVersionsAsync()` when recently-created versions are expected.

---

### Migration Notes for v1.1.0 → v1.2.0
**100% Backward Compatible** — no breaking changes. `PersonaStore.IntegrityMode` defaults to `"Full"`, the same hashing behavior as v1.1.x, so nothing changes unless you opt in:

```lua
-- v1.1.0 code continues to work exactly as before
session:SavePatch()

-- Opt in to cheaper patch-hashing when you have large, mostly-static fields
PersonaStore:SetIntegrityMode("PerField")

-- New OrderedDataStore / MemoryStore / Version APIs are additive
local Leaderboard = PersonaStore:CreateOrderedDataStore("Leaderboard_v1")
local Queue = PersonaStore:CreateMemoryQueue("PurchaseQueue")
local versions = PlayerStore:ListVersionsAsync(tostring(userId))
```

If you were previously relying on `VerifyDataIntegrity()` returning `false` in normal operation (i.e. code paths that treated "always fails" as expected), double check those paths — it now correctly returns `true` when data hasn't been tampered with.

---

# v1.1.0

## Added
*EncodingService Integration & Advanced Features*

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

## Versioning Policy

- **MAJOR**: Breaking API changes or complete rewrites
- **MINOR**: New features that are backward compatible
- **PATCH**: Bug fixes and performance improvements

---
