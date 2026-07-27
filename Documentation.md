# PersonaStore Documentation

PersonaStore is a robust, production-ready DataStore persistence framework with native Roblox EncodingService integration, compression, data integrity verification, and advanced session management.

## Table of Contents

1. [Getting Started](#getting-started)
2. [Core API Reference](#core-api-reference)
3. [EncodingService Integration](#encodingservice-integration)
4. [Compression & Hashing](#compression--hashing)
5. [Read-Only Access](#read-only-access)
6. [Batch Operations](#batch-operations)
7. [Statistics & Monitoring](#statistics--monitoring)
8. [Advanced Features](#advanced-features)

---

## Getting Started

Before loading any player data, initialize the engine once:

```lua
local PersonaStore = require(ServerStorage.PersonaStore)

PersonaStore:Init()
```

Create isolated DataStores with optional compression configuration:

```lua
local PlayerStore = PersonaStore:CreateDataStore("PlayerData_v1", {
    Schema = PlayerTemplate,
    AutoSaveInterval = 30
})
```

Configure compression settings globally:

```lua
-- Use ZSTD compression (better ratio, slower) with level 9
PersonaStore:SetCompressionSettings(Enum.CompressionAlgorithm.ZSTD, 9)
```

Load and interact with sessions:

```lua
local session = PlayerStore:LoadSession(tostring(player.UserId))
if session then
    session.Data.Coins += 100
    session:SavePatch()
    session:Destroy()
end
```

---

## Core API Reference

### PersonaStore

The engine singleton that manages all DataStores, compression, and cluster communication.

#### PersonaStore:Init()

Starts the shared MessagingService subscription and registers the engine-wide shutdown handler.

```lua
PersonaStore:Init()
```

**Must be called once** before any `LoadSession()` calls.

---

#### PersonaStore:CreateDataStore(storeName, configOptions)

Creates or returns an existing `Founder` (isolated DataStore wrapper).

```lua
local PlayerStore = PersonaStore:CreateDataStore("PlayerData", {
    Schema = PlayerTemplate,
    AutoSaveInterval = 30
})
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `storeName` | `string` | Yes | Name of the Roblox DataStore |
| `configOptions` | `table` | No | Configuration table |

**Configuration Options:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `Schema` | `table` | `{}` | Template for new profiles |
| `AutoSaveInterval` | `number` | `30` | Seconds between autosaves |

---

#### PersonaStore:SetCompressionSettings(algorithm, level)

Configures compression algorithm and level for all future compression operations.

```lua
PersonaStore:SetCompressionSettings(Enum.CompressionAlgorithm.ZSTD, 9)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `algorithm` | `Enum.CompressionAlgorithm` | `Deflate` or `ZSTD` |
| `level` | `number` | 1-22 depending on algorithm (1=faster, 22=better compression) |

**Recommendations:**
- **Deflate, Level 6**: Good balance of speed and compression (default)
- **ZSTD, Level 9**: Best for data-heavy profiles (slower but ~20-30% better compression)
- **ZSTD, Level 22**: Maximum compression for archival/exports

---

#### PersonaStore:GetStatistics()

Returns engine-wide statistics since server start.

```lua
local stats = PersonaStore:GetStatistics()
print("Total saves:", stats.TotalSaves)
print("Bytes saved by compression:", stats.BytesSavedByCompression)
print("DataStore requests failed:", stats.DatastoreRequestsFailed)
```

**Returned Table:**

| Field | Type | Description |
|-------|------|-------------|
| `TotalSaves` | `number` | Count of successful `Save()` / `SavePatch()` calls |
| `TotalLoads` | `number` | Count of sessions loaded |
| `TotalCompressions` | `number` | Count of compression operations |
| `TotalDecompressions` | `number` | Count of decompression operations |
| `DatastoreRequestsAttempted` | `number` | Total DataStore API calls attempted |
| `DatastoreRequestsFailed` | `number` | Failed DataStore API calls |
| `BytesSavedByCompression` | `number` | Total bytes reduced through compression |

---

### Founder

Represents a single isolated DataStore with session management.

#### Founder:LoadSession(key)

Acquires ownership of a profile and returns a `DataSession`.

```lua
local session = Store:LoadSession(tostring(player.UserId))
if not session then
    player:Kick("Data is still loading. Please rejoin.")
    return
end
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `key` | `string` | Yes | Profile key (usually player UserID) |

**Returns:** `DataSession?` — `nil` if ownership could not be acquired

---

#### Founder:LoadSessionAsync(key, maxWaitSeconds)

Repeatedly attempts to acquire a session for up to `maxWaitSeconds`.

```lua
local session = Store:LoadSessionAsync(tostring(player.UserId), 8)
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `key` | `string` | Yes | Profile key |
| `maxWaitSeconds` | `number` | No | Timeout in seconds (default: 10) |

**Best for:** Player rejoin after crashes, where waiting 5-10 seconds is preferable to kicking.

---

#### Founder:LoadReadOnlySession(key)

**NEW** - Loads profile data without acquiring a lock (read-only, no mutations).

```lua
local readOnlyData = Store:LoadReadOnlySession(tostring(userId))
if readOnlyData then
    print("Coins:", readOnlyData.Data.Coins)
    print("Level:", readOnlyData.Data.Level)
end
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `key` | `string` | Yes | Profile key |

**Returns:** `table?` — The profile data, or `nil` if not found

**Use cases:**
- Leaderboard queries without locking the player
- Admin panels viewing player data
- Cross-server data aggregation

---

#### Founder:PublishGlobalUpdate(key, payload)

Sends an update to a profile regardless of whether the player is online or which server owns the session.

```lua
PlayerStore:PublishGlobalUpdate(tostring(userId), {
    Type = "Currency",
    Amount = 500,
    Reason = "DailyReward"
})
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `key` | `string` | Yes | Profile key |
| `payload` | `table` | Yes | Arbitrary data describing the update |

**Returns:** `boolean` — Whether the update was successfully queued

**Best for:**
- Granting items to offline players
- Delivering web store purchases
- Admin actions (bans, warnings, etc.)

---

#### Founder:BatchUpdate(keys, transformFn)

**NEW** - Atomically updates multiple profiles with a transformation function.

```lua
local results = PlayerStore:BatchUpdate({userId1, userId2, userId3}, function(data)
    data.SeasonPoints = 0  -- Reset season points
    data.SeasonRank = 1
end)

for key, success in pairs(results) do
    if success then
        print("Successfully reset", key)
    end
end
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `keys` | `{string}` | Yes | Array of profile keys |
| `transformFn` | `(data: table) -> ()` | Yes | Function to transform each profile |

**Returns:** `{[string]: boolean}` — Success status per key

**Important:** `BatchUpdate` will wait up to 5 seconds per key. Avoid with very large arrays.

---

#### Founder:GetKeyMetadata(key)

**NEW** - Retrieves metadata about a profile without loading it.

```lua
local meta = Store:GetKeyMetadata(tostring(userId))
if meta then
    print("Version:", meta.Version)
    print("Last updated:", os.date("%Y-%m-%d %H:%M:%S", meta.LastUpdate))
    print("Compression:", meta.CompressionMetadata.IsCompressed)
end
```

**Returns:** `table?` — Metadata, or `nil` if not found

**Returned Table:**

| Field | Type | Description |
|-------|------|-------------|
| `Version` | `number` | Profile version (incremented on each save) |
| `LastUpdate` | `number` | Unix timestamp of last save |
| `SessionToken` | `string?` | Current session token if locked |
| `JobId` | `string?` | Server JobId currently owning the lock |
| `DataHash` | `string?` | SHA256 hash of profile data (if enabled) |
| `CompressionMetadata` | `table?` | Compression info |

---

#### Founder:GetCompressionStats()

**NEW** - Returns compression statistics for this store.

```lua
local stats = Store:GetCompressionStats()
print("Bytes saved:", stats.BytesSavedByCompression)
```

---

#### Founder:GetActiveSessionCount()

Returns the number of currently loaded sessions on this server.

```lua
local count = Store:GetActiveSessionCount()
print("Active sessions:", count)
```

---

### DataSession

Represents an actively loaded, locked profile. This is the only object for reading/writing profile data.

#### DataSession.Data

The observable data table for the loaded profile.

Every write at any nesting depth marks the top-level field as dirty and fires `ListenToFieldChange` listeners.

```lua
session.Data.Coins += 500
session.Data.Inventory.Sword = true
session.Data.Stats.Level = 10
```

---

#### DataSession:Save()

Writes the entire profile back to the DataStore.

```lua
local success = session:Save()
```

**Returns:** `boolean` — Whether the save succeeded

**Notes:**
- Costs more bandwidth than `SavePatch()`
- Confirms lock token before writing
- Use for important checkpoints

---

#### DataSession:SavePatch()

Writes only the top-level fields that changed since the last save.

```lua
session:SavePatch()
```

**Returns:** `boolean` — Whether the save succeeded (returns `true` if nothing changed)

**Notes:**
- This is what the autosave heartbeat calls
- Dramatically reduces bandwidth on large profiles
- Prefer over `Save()` for routine autosaving

---

#### DataSession:SaveCompressed()

**NEW** - Saves the entire profile with EncodingService compression.

```lua
local success = session:SaveCompressed()
```

**Returns:** `boolean` — Whether the save succeeded

**Best for:**
- Large profiles with deep inventories
- End-of-session checkpoints
- Reducing storage costs

**Compression ratio:** Typically 40-70% depending on data structure and algorithm

---

#### DataSession:BeginTransaction()

Takes a deep-copy snapshot of the current `Data` for later rollback.

```lua
session:BeginTransaction()

session.Data.Coins -= 100
session.Data.Inventory.Sword = true

if not validatePurchase() then
    session:RollbackTransaction()
else
    session:CommitTransaction()
    session:SavePatch()
end
```

---

#### DataSession:CommitTransaction()

Discards the snapshot, keeping data as-is.

```lua
session:CommitTransaction()
session:SavePatch()
```

---

#### DataSession:RollbackTransaction()

Restores `Data` to the pre-transaction state.

```lua
local success = session:RollbackTransaction()
```

**Returns:** `boolean` — `false` if no transaction was active

---

#### DataSession:IncrementCounter(fieldName, amount)

**NEW** - Atomically increments a counter in the data.

```lua
session:IncrementCounter("Coins", 100)  -- Coins += 100
session:IncrementCounter("Level")       -- Level += 1
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `fieldName` | `string` | Yes | Field to increment |
| `amount` | `number` | No | Increment amount (default: 1) |

**Returns:** `boolean` — Whether the save succeeded

**Best for:**
- Leaderboard score increments
- Currency transactions
- Statistics counters

---

#### DataSession:ExportData(includeMetadata)

**NEW** - Exports profile data as JSON string for backup/migration.

```lua
local json = session:ExportData(true)
-- Save to file or send to backend...
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `includeMetadata` | `boolean` | No | Include session metadata in export |

**Returns:** `string` — JSON-encoded export

**Exported structure:**

```json
{
  "Data": { /* profile data */ },
  "ExportTime": 1234567890,
  "SessionToken": "...",  // if includeMetadata = true
  "Metadata": { /* session metadata */ }
}
```

---

#### DataSession:ImportData(jsonString, overwrite)

**NEW** - Imports profile data from a JSON export.

```lua
local success = session:ImportData(jsonString, false)  -- Merge mode
-- or
local success = session:ImportData(jsonString, true)   -- Overwrite mode
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `jsonString` | `string` | Yes | JSON export string |
| `overwrite` | `boolean` | No | `true` = replace all data, `false` = merge (default) |

**Returns:** `boolean` — Whether import succeeded

---

#### DataSession:VerifyDataIntegrity()

**NEW** - Verifies profile data integrity using stored hash.

```lua
if not session:VerifyDataIntegrity() then
    warn("Profile data was modified externally!")
    session:Reload()
end
```

**Returns:** `boolean` — Whether data hash matches

**Requires:** `PersonaStore.EnableDataIntegrityChecks = true` (enabled by default)

---

#### DataSession:IsCacheStale()

Compares in-memory version against cloud version without acquiring a lock.

```lua
if session:IsCacheStale() then
    warn("Profile was modified by another server!")
end
```

**Returns:** `boolean` — `true` if cloud version is newer

---

#### DataSession:StartAutoSave(intervalSeconds)

Starts autosave loop with given interval.

```lua
session:StartAutoSave(15)  -- Save every 15 seconds
```

**Called automatically by `LoadSession()`** using the store's `AutoSaveInterval` config.

---

#### DataSession:StopAutoSave()

Cancels the autosave loop.

```lua
session:StopAutoSave()
```

---

#### DataSession:ListenToFieldChange(callback)

Fires whenever any field in `Data` changes at any nesting depth.

```lua
session:ListenToFieldChange(function(key, value, rootKey)
    if rootKey == "Coins" then
        print("Coins changed to:", session.Data.Coins)
        player.leaderstats.Coins.Value = session.Data.Coins
    end
end)
```

| Argument | Meaning |
|----------|---------|
| `key` | The literal field name that was assigned |
| `value` | The new value |
| `rootKey` | The top-level field under `Data` that was modified |

---

#### DataSession:ListenToGlobalUpdate(callback)

Fires whenever a live global update arrives from another server.

```lua
session:ListenToGlobalUpdate(function(payload)
    if payload.Type == "Currency" then
        StarterGui:SetCore("SendNotification", {
            Title = "Gift Received",
            Text = "+" .. payload.Amount .. " coins"
        })
    end
end)
```

---

#### DataSession:ConsumeGlobalUpdates()

Returns all queued global updates and clears the queue.

```lua
for _, update in session:ConsumeGlobalUpdates() do
    if update.Type == "Currency" then
        session.Data.Coins += update.Amount
    elseif update.Type == "Item" then
        session.Data.Inventory[update.ItemId] = true
    end
end
```

**Returns:** `{table}` — Array of update payloads

---

#### DataSession:GetPerformanceMetadata()

Returns runtime metadata about the session.

```lua
local meta = session:GetPerformanceMetadata()
print("Last saved:", os.date("%c", meta.LastSaved))
print("Save cycles:", meta.WriteCycles)
print("Compression stats:", meta.CompressionStats)
```

**Returned Table:**

| Field | Type | Description |
|-------|------|-------------|
| `Created` | `number` | Unix timestamp when session was created |
| `LastSaved` | `number` | Unix timestamp of last successful save |
| `WriteCycles` | `number` | Count of `Save()` / `SavePatch()` calls |
| `LastModified` | `number?` | Unix timestamp of last data mutation |
| `CompressionStats` | `table` | Latest compression stats |

---

#### DataSession:Release()

Releases ownership without saving.

```lua
session:Release()
```

**Prefer `Destroy()` for normal cleanup.**

---

#### DataSession:Destroy()

Saves the full profile, then releases the session.

```lua
session:Destroy()
```

**Use for:**
- `Players.PlayerRemoving`
- Planned session closure
- Any graceful disconnect

---

## EncodingService Integration

PersonaStore uses Roblox's native `EncodingService` for efficient serialization.

### Compression Handler

The `CompressionHandler` is an internal utility managing all encoding operations:

**Compression Algorithms:**

| Algorithm | Speed | Compression Ratio | Best For |
|-----------|-------|-------------------|----------|
| `Deflate` | Fast | ~40% | General purpose, balanced |
| `ZSTD` | Slower | ~60-70% | Large profiles, storage optimization |

**Levels:**

- **1-3:** Fastest compression, lower ratio (use for real-time saves)
- **6:** Default, good balance
- **9+:** Better compression, slower (use for end-of-session saves)
- **20-22:** Maximum compression (use for archives/exports)

### Hash Verification

When `PersonaStore.EnableDataIntegrityChecks = true` (default):

- Every `Save()` computes a SHA256 hash of the profile data
- The hash is stored with the profile
- `session:VerifyDataIntegrity()` checks if cloud hash matches in-memory hash

**Hash Algorithms Supported:**

- `SHA256` (default, recommended)
- `SHA1` (faster, less secure)
- `MD5` (fastest, not recommended for security-critical data)

### Base64 Encoding

When compressed data is stored in DataStore (JSON-compatible format):

```lua
-- Internally:
local compressed = CompressionHandler.compress(profileData)
local base64 = CompressionHandler.encodeToBase64(compressed)
-- Stored as JSON-safe string in DataStore
```

---

## Read-Only Access

Use `LoadReadOnlySession()` to query profiles without locks:

```lua
-- Leaderboard query
local topPlayers = {}
for _, userId in ipairs(topUserIds) do
    local data = PlayerStore:LoadReadOnlySession(tostring(userId))
    if data then
        table.insert(topPlayers, {
            UserId = userId,
            Coins = data.Data.Coins,
            Level = data.Data.Level
        })
    end
end
```

**Advantages:**
- No lock acquisition overhead
- No risk of lock conflicts
- Instant reads
- Perfect for queries and reporting

---

## Batch Operations

Use `BatchUpdate()` for mass profile modifications:

```lua
-- Grant season pass bonus to multiple players
local playerIds = {1234, 5678, 9012}

local results = PlayerStore:BatchUpdate(playerIds, function(data)
    data.SeasonPoints += 500
    data.Cosmetics.SeasonPass = true
end)

print("Successfully updated:", #(results or {}))
```

Each profile is locked individually, modified, and saved. If a lock fails, that key's result is `false`.

---

## Statistics & Monitoring

Monitor server health and performance:

```lua
-- Create a monitoring loop
while true do
    task.wait(300)  -- Every 5 minutes
    
    local stats = PersonaStore:GetStatistics()
    local playerStore = PersonaStore.RegisteredStores["PlayerData"]
    
    print("=== Engine Statistics ===")
    print("Active sessions:", playerStore:GetActiveSessionCount())
    print("Total loads:", stats.TotalLoads)
    print("Total saves:", stats.TotalSaves)
    print("Failed DataStore requests:", stats.DatastoreRequestsFailed)
    print("Compression saved bytes:", stats.BytesSavedByCompression / 1024 .. " KB")
    print("Compression ratio:", math.round(
        100 * (1 - stats.BytesSavedByCompression / (stats.TotalCompressions * 1000))
    ) .. "%")
end
```

---

## Advanced Features

### Compression Caching

PersonaStore caches compressed data to avoid recompressing unchanged profiles:

```lua
-- First SaveCompressed() compresses the data
session:SaveCompressed()

-- Subsequent SaveCompressed() reuses cached result
session:SaveCompressed()
```

Cache is automatically invalidated when `Data` is mutated.

### Selective Compression

Use `SaveCompressed()` only when needed:

```lua
-- Autosave with regular SavePatch()
session:StartAutoSave(30)

-- Manual full compression save on important events
if playerWonTournament then
    session:SaveCompressed()  -- Compress for better storage ratio
end
```

### Data Validation

Combine transactions and integrity checks:

```lua
session:BeginTransaction()

local hadCoins = session.Data.Coins
session.Data.Coins -= 100

if not validateTransaction() or session.Data.Coins < 0 then
    session:RollbackTransaction()
else
    session:CommitTransaction()
    session:SavePatch()
    if not session:VerifyDataIntegrity() then
        warn("Transaction verification failed!")
    end
end
```

---

## Best Practices

1. **Always initialize:** Call `PersonaStore:Init()` once at server start
2. **Use `SavePatch()` by default:** Significantly reduces bandwidth for most use cases
3. **Enable integrity checks:** `PersonaStore.EnableDataIntegrityChecks = true` catches data corruption
4. **Use compression strategically:** `SaveCompressed()` for large profiles, important checkpoints
5. **Read-only for queries:** Use `LoadReadOnlySession()` for leaderboards, admin panels
6. **Clean up sessions:** Always call `Destroy()` on player leave
7. **Monitor statistics:** Track engine health with `GetStatistics()`
8. **Configure compression:** Tune compression algorithm and level per your data size

---

## Example: Complete Game Loop

```lua
local PersonaStore = require(ServerStorage.PersonaStore)

-- Configure engine
PersonaStore:SetCompressionSettings(Enum.CompressionAlgorithm.ZSTD, 6)
PersonaStore:Init()

-- Create stores
local PlayerStore = PersonaStore:CreateDataStore("PlayerData_v2", {
    Schema = {
        Coins = 0,
        Level = 1,
        Inventory = {},
        Stats = { Kills = 0, Deaths = 0 }
    },
    AutoSaveInterval = 30
})

-- On player join
Players.PlayerAdded:Connect(function(player)
    local session = PlayerStore:LoadSessionAsync(tostring(player.UserId), 10)
    
    if not session then
        player:Kick("Failed to load data. Please rejoin.")
        return
    end
    
    -- Listen for changes
    session:ListenToFieldChange(function(key, value, rootKey)
        if rootKey == "Coins" then
            player.leaderstats.Coins.Value = session.Data.Coins
        end
    end)
    
    -- Process queued global updates
    for _, update in session:ConsumeGlobalUpdates() do
        if update.Type == "Currency" then
            session.Data.Coins += update.Amount
        end
    end
    
    player:SetAttribute("Session", session)
end)

-- On player leave
Players.PlayerRemoving:Connect(function(player)
    local session = player:GetAttribute("Session")
    if session then
        session:Destroy()  -- Save and release
    end
end)

-- Periodic monitoring
while true do
    task.wait(300)
    
    local stats = PersonaStore:GetStatistics()
    print("Active sessions:", PlayerStore:GetActiveSessionCount())
    print("Total saves:", stats.TotalSaves)
    print("Bytes saved by compression:", stats.BytesSavedByCompression / (1024 * 1024) .. " MB")
end
```

---

## Configuration Reference

### PersonaStore Settings

```lua
PersonaStore.CompressionAlgorithm = Enum.CompressionAlgorithm.Deflate
PersonaStore.CompressionLevel = 6
PersonaStore.EnableDataIntegrityChecks = true
PersonaStore.HashAlgorithm = Enum.HashAlgorithm.SHA256
PersonaStore.GlobalLockTimeout = 120  -- seconds
PersonaStore.GlobalAutoSaveInterval = 30  -- seconds
```

All settings can be modified before `Init()` or via `:SetCompressionSettings()`.
