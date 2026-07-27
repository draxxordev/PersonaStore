# Getting Started

PersonaStore is designed to make persistent data management simple while remaining scalable for larger games.

Before loading any player data, the engine must be initialized once.

```lua
local PersonaStore = require(ServerStorage.PersonaStore)

PersonaStore:Init()
```

Initialization performs several engine-wide tasks:

- Starts the shared MessagingService router
- Enables cross-server synchronization
- Registers the automatic BindToClose cleanup
- Allows DataSessions to be loaded safely

> **Note**
>
> `:Init()` should only be called once during server startup. Calling it again has no effect beyond a warning; it will not create a second MessagingService subscription.

---

After initialization, isolated DataStores are created with `:CreateDataStore()`.

```lua
local PlayerStore = PersonaStore:CreateDataStore("PlayerData", {
    Schema = PlayerTemplate
})
```

Once a DataStore exists, sessions can be loaded for individual keys.

```lua
local session = PlayerStore:LoadSession(tostring(player.UserId))
```

From there, all data interaction happens through the returned `DataSession`.

```lua
session.Data.Coins += 100
session.Data.Level = 5
```

When finished with a session, always destroy it.

```lua
session:Destroy()
```

That's the entire lifecycle:

```
PersonaStore:Init()
        ↓
CreateDataStore()
        ↓
LoadSession()
        ↓
Read / Modify Data
        ↓
Destroy()
```

---

# API Reference

---

# PersonaStore

The engine singleton. Owns the single cluster-wide MessagingService subscription, routes incoming eviction and global-update signals to the correct `Founder`, and drains every open session on every registered store when the server shuts down.

---

## PersonaStore.new()

Constructs a new PersonaStore table without starting any live services.

Kept separate from `:Init()` so the module can be required for its class definitions alone (for example, in unit tests) without immediately subscribing to MessagingService or registering a BindToClose handler. Calling this directly is not required for normal usage, since the required module already returns a usable engine table.

```lua
local Engine = PersonaStore.new()
```

---

### Returns

| Type | Description |
|------|-------------|
| `PersonaStore` | A fresh, uninitialized engine table. |

---

## PersonaStore:Init()

Starts the shared MessagingService subscription and registers the engine-wide shutdown handler.

The subscription is what routes `Eject_Key` and `Global_Update` cluster messages to the correct `Founder`, which in turn routes them to the correct `DataSession`. The shutdown handler walks every registered store's active sessions on `BindToClose` and calls `:Destroy()` on each one, so individual scripts never need their own shutdown handling to avoid losing data on deploys.

```lua
PersonaStore:Init()
```

---

### Returns

Nothing.

---

### Notes

- Must run before any `Founder:LoadSession()` call. `LoadSession` checks whether the engine is active and refuses to load, returning `nil` with a warning, if `Init()` hasn't run yet.
- Calling `Init()` more than once only emits a warning. It will not create a second subscription or a second shutdown handler.
- The BindToClose drain does not run inside Studio test sessions, since stalling the editor to flush DataStore writes on every test stop isn't useful.

---

## PersonaStore:CreateDataStore()

Creates, or returns an existing, isolated `Founder` object bound to a Roblox DataStore.

Each `Founder` manages its own sessions, schema reconciliation, autosave configuration, and cross-server synchronization independently of every other `Founder`.

```lua
local PlayerStore = PersonaStore:CreateDataStore("PlayerData", {
    Schema = PlayerTemplate,
    AutoSaveInterval = 30
})
```

---

### Arguments

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `storeName` | `string` | Yes | Name of the Roblox DataStore. |
| `configOptions` | `table` | No | Optional configuration table. |

---

### Configuration

| Field | Type | Default | Description |
|------|------|---------|-------------|
| `Schema` | `table` | `{}` | Template used when reconciling newly loaded profiles. |
| `AutoSaveInterval` | `number` | `30` | Seconds between automatic patch saves. |

---

### Returns

| Type | Description |
|------|-------------|
| `Founder` | The DataStore wrapper used to manage sessions. |

---

### Best Uses

- Creating separate player databases.
- Separating inventories from statistics.
- Guild or clan persistence.
- World save data.
- Leaderboards.
- Seasonal save slots.

---

### Example

```lua
local PlayerStore = PersonaStore:CreateDataStore("PlayerData", {
    Schema = Template
})

local GuildStore = PersonaStore:CreateDataStore("Guilds", {
    Schema = GuildTemplate
})
```

---

### Notes

- Calling this multiple times with the same `storeName` returns the existing `Founder`; the `configOptions` passed on subsequent calls are ignored.
- Every `Founder` manages its own active sessions, independently of every other store.
- One `Founder` corresponds to one Roblox DataStore.

---

# Founder

Represents a single, isolated DataStore. Handles session locking, schema reconciliation, and both live and offline global updates for every key stored under it.

---

## Founder:LoadSession()

Attempts to acquire ownership of a profile and returns a `DataSession` wrapping it.

If this server already has the key loaded, the existing `DataSession` is returned directly instead of re-locking. Otherwise, an `Eject_Key` cluster message is published first so any other server currently holding the lock releases it, then an `UpdateAsync` call claims the lock, reconciles the profile against the store's `Schema`, and stands up a new `DataSession` with autosaving already running.

```lua
local session = Store:LoadSession(key)
```

---

### Arguments

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `key` | `string` | Yes | The profile key to load. |

---

### Returns

| Type | Description |
|------|-------------|
| `DataSession?` | `nil` if ownership could not be acquired. |

---

### Best Uses

- Loading a player's profile on join.
- Loading guild, world, or leaderboard data on demand.
- Any read or write that needs an exclusive lock on a key.

---

### Notes

- Returns `nil`, with a warning, if called before `PersonaStore:Init()`.
- Returns `nil` if another server holds a valid lock (younger than `GlobalLockTimeout`) under a different `JobId`, even after the `Eject_Key` broadcast, since eviction is asynchronous and may not land before this call's own `UpdateAsync` resolves. Use `LoadSessionAsync()` if a short wait is acceptable.
- Autosaving starts automatically using the store's `AutoSaveInterval` (default: 30 seconds).

---

## Founder:LoadSessionAsync()

Repeatedly attempts to acquire a session, once per second, until successful or the timeout expires.

This exists for cases where waiting a few seconds for a lock to free up is preferable to failing immediately, such as a player rejoining right after a crash, where the previous server's lock hasn't formally timed out yet but its eviction message may still be in flight.

```lua
local session = Store:LoadSessionAsync(key, maxWait)
```

---

### Arguments

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `key` | `string` | Yes | The profile key to load. |
| `maxWait` | `number` | No | Maximum seconds to keep retrying. Defaults to `10`. |

---

### Returns

| Type | Description |
|------|-------------|
| `DataSession?` | `nil` if the timeout elapses without acquiring the lock. |

---

### Best Uses

- Reconnects shortly after a crash or unexpected disconnect.
- Any load where a short delay is preferable to an outright failure.

---

## Founder:PublishGlobalUpdate()

Delivers a payload to a profile regardless of whether the target is online, or which server currently owns its session.

If the key is loaded on the calling server, the payload is delivered immediately through the session's global update listeners. It is also written into the profile's `GlobalUpdates` queue in the DataStore, so a server that loads the profile later still receives it, and broadcast over MessagingService so any other currently running server that owns the session can react without waiting for a reload.

```lua
Store:PublishGlobalUpdate(key, payload)
```

---

### Arguments

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `key` | `string` | Yes | The profile key to target. |
| `payload` | `table` | Yes | Arbitrary data describing the update. |

---

### Returns

| Type | Description |
|------|-------------|
| `boolean` | Whether the update was successfully persisted to the DataStore queue. |

---

### Best Uses

- Granting items, currency, or mail to a player regardless of which server they're on.
- Delivering purchases from an external system (e.g. a web store) to an offline player.
- Banning or flagging an account from an admin panel running elsewhere.

---

### Notes

- The `boolean` return reflects whether the queue write succeeded, independent of whether a locally loaded session also received the update live.
- A session that is offline when this is called consumes the update the next time it loads, via `ConsumeGlobalUpdates()`.

---

## Founder:ForceEjectKey()

Requests that every server release a specific profile, without loading it locally.

```lua
Store:ForceEjectKey(key)
```

---

### Arguments

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `key` | `string` | Yes | The profile key to evict. |

---

### Returns

Nothing.

---

### Best Uses

- Admin tooling that needs to guarantee no server holds a stale lock before a manual DataStore edit.
- Force-clearing a lock left behind by an abnormal server termination.

---

## Founder:GetActiveSessionCount()

Returns the number of currently loaded sessions on this server, for this store.

```lua
local count = Store:GetActiveSessionCount()
```

---

### Returns

| Type | Description |
|------|-------------|
| `number` | Count of active sessions. |

---

# DataSession

Represents an actively loaded, locked profile. This is the only object gameplay code should read or write through.

---

## DataSession.Data

The observable data table for the loaded profile.

Every read passes through to the underlying data. Every write, at any nesting depth, marks the corresponding top-level field as dirty for the next `SavePatch()` call and fires any callback registered with `ListenToFieldChange()`.

```lua
session.Data.Coins += 500

session.Data.Inventory.Sword = true
```

---

### Type

| Type | Description |
|------|-------------|
| `table` | Shaped by the store's `Schema`. |

---

## DataSession:Save()

Writes the entire profile back to the DataStore.

Confirms the session's lock token still matches before writing, and fails if the lock has since lapsed (for example, following a remote eviction). Clears the dirty-field tracker on success.

```lua
session:Save()
```

---

### Returns

| Type | Description |
|------|-------------|
| `boolean` | Whether the save succeeded. |

---

### Notes

- Costs more bandwidth than `SavePatch()`, since the full `Data` table is serialized regardless of what changed.
- Reserve for release-time checkpoints or important events; use `SavePatch()` for routine saving.

---

## DataSession:SavePatch()

Writes only the top-level fields that have changed since the previous successful save.

If nothing is dirty, this returns `true` immediately without making a DataStore request. Fields that aren't tracked as dirty are left untouched in the cloud, so writes made by another process, such as a global update consumed elsewhere, aren't overwritten.

```lua
session:SavePatch()
```

---

### Returns

| Type | Description |
|------|-------------|
| `boolean` | Whether the save succeeded. |

---

### Notes

- This is what the autosave heartbeat calls internally.
- Prefer this over `Save()` for periodic autosaving on profiles with large or deeply nested data.

---

## DataSession:BeginTransaction()

Takes a deep-copy snapshot of the current `Data` table for later rollback.

Has no effect on what's persisted to the DataStore. `Save()` or `SavePatch()` must still be called separately to persist anything.

```lua
session:BeginTransaction()
```

---

### Returns

| Type | Description |
|------|-------------|
| `boolean` | `false` if the session has already been released. |

---

## DataSession:CommitTransaction()

Discards the snapshot taken by `BeginTransaction()`, keeping the data as it currently stands in memory.

```lua
session:CommitTransaction()
```

---

### Returns

Nothing.

---

## DataSession:RollbackTransaction()

Restores `Data` to the state captured by the most recent `BeginTransaction()` call, then clears the snapshot.

```lua
session:RollbackTransaction()
```

---

### Returns

| Type | Description |
|------|-------------|
| `boolean` | `false` if no transaction was in progress. |

---

## DataSession:StartAutoSave()

Starts a cancellable autosave loop that calls `SavePatch()` every `intervalSeconds`.

Any autosave loop already running on this session is stopped first. Called automatically by `LoadSession()`, so most game code never needs to call this directly.

```lua
session:StartAutoSave(interval)
```

---

### Arguments

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `interval` | `number` | Yes | Seconds between automatic patch saves. |

---

### Returns

Nothing.

---

## DataSession:StopAutoSave()

Cancels the running autosave loop, if one exists.

Called automatically by `Release()`, and therefore by `Destroy()`.

```lua
session:StopAutoSave()
```

---

### Returns

Nothing.

---

## DataSession:IsCacheStale()

Compares this session's last-synced version stamp against whatever is currently stored in the cloud, without acquiring a lock or mutating anything.

Useful before trusting in-memory `Data` for something high-stakes, such as confirming a trade, since a mismatch should only occur after a forced eviction and steal by another server.

```lua
local stale = session:IsCacheStale()
```

---

### Returns

| Type | Description |
|------|-------------|
| `boolean` | `true` if the cloud version is newer than the locally synced version. |

---

### Notes

- Returns `false` if the check itself fails, so a transient read error doesn't falsely flag the cache as stale.

---

## DataSession:ConsumeGlobalUpdates()

Returns every global update currently queued in memory for this session and clears the queue.

Includes updates that were already waiting in the cloud when the profile loaded, as well as any that arrived live while the session has been open.

```lua
local updates = session:ConsumeGlobalUpdates()
```

---

### Returns

| Type | Description |
|------|-------------|
| `{table}` | The queued updates, in the order they were received. |

---

## DataSession:ListenToGlobalUpdate()

Registers a callback that fires whenever a live global update arrives from another server while this session is loaded.

```lua
session:ListenToGlobalUpdate(function(update)

end)
```

---

### Arguments

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `callback` | `(payload: table) -> ()` | Yes | Invoked once per incoming live update. |

---

### Returns

Nothing.

---

### Notes

- The update is also added to the queue read by `ConsumeGlobalUpdates()`; the two systems don't compete with each other.

---

## DataSession:ListenToFieldChange()

Fires whenever any field in `Data` changes, at any nesting depth.

```lua
session:ListenToFieldChange(function(key, value, rootKey)

end)
```

---

### Arguments

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `callback` | `(key: string, value: any, rootKey: string) -> ()` | Yes | Invoked on every mutation. `rootKey` is the top-level field the change is tracked under, regardless of how deep the actual write occurred. |

---

### Returns

Nothing.

---

## DataSession:GetPerformanceMetadata()

Returns a copy of the session's runtime metadata.

```lua
local metadata = session:GetPerformanceMetadata()
```

---

### Returns

| Type | Description |
|------|-------------|
| `table` | Contains `Created`, `LastSaved`, `WriteCycles`, and (once modified) `LastModified`. |

---

## DataSession:Release()

Releases ownership of the profile without saving first.

Removes the session from the store's active sessions and clears the lock in the DataStore, so another server can immediately acquire the profile.

```lua
session:Release()
```

---

### Returns

Nothing.

---

### Notes

- Prefer `Destroy()` for planned disconnects, such as `Players.PlayerRemoving`, so data loss only happens on genuine server crashes, never on ordinary leaves.

---

## DataSession:Destroy()

Saves the full profile, then releases the session.

```lua
session:Destroy()
```

---

### Returns

Nothing.

---

### Best Uses

- The standard way to end any session under normal circumstances.
- `Players.PlayerRemoving`.
- Manually closing out a guild, world, or leaderboard session once work on it is finished.
