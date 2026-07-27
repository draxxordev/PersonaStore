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
> `:Init()` should only be called **once** during server startup. Calling it multiple times has no effect other than emitting a warning.

After initialization, you can create isolated DataStores using `:CreateDataStore()`.

```lua
local PlayerStore = PersonaStore:CreateDataStore("PlayerData", {
    Schema = PlayerTemplate
})
```

Once a DataStore has been created, sessions can be loaded for individual keys.

```lua
local session = PlayerStore:LoadSession(tostring(player.UserId))
```

From there, all data interaction happens through the returned `DataSession`.

```lua
session.Data.Coins += 100
session.Data.Level = 5
```

When you're finished with a session, always destroy it.

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

You're now ready to explore the rest of PersonaStore's API.

---



# API Reference

---

# PersonaStore

## PersonaStore.new()

Creates a new PersonaStore engine instance.

```lua
local Engine = PersonaStore.new()
```

> In most cases, you can simply use the module directly rather than constructing a new engine manually.

**Returns**

- `PersonaStore`

---

## PersonaStore:Init()

Initializes the engine.

This starts the MessagingService router, enables session loading, and registers the automatic shutdown handler.

```lua
PersonaStore:Init()
```

**Returns**

Nothing.

---

## PersonaStore:CreateDataStore()

Creates or returns an isolated DataStore wrapper.

```lua
local Store = PersonaStore:CreateDataStore(storeName, config)
```

### Parameters

| Name | Type | Description |
|------|------|-------------|
| `storeName` | `string` | Name of the Roblox DataStore. |
| `config` | `table` | Configuration table. |

### Config

```lua
{
    Schema = PlayerTemplate,
    AutoSaveInterval = 30
}
```

### Returns

`Founder`

---

# Founder

Represents an isolated DataStore.

---

## Founder:LoadSession()

Attempts to acquire ownership of a profile.

```lua
local session = Store:LoadSession(key)
```

### Parameters

| Name | Type |
|------|------|
| key | string |

### Returns

`DataSession?`

Returns nil if ownership could not be acquired.

---

## Founder:LoadSessionAsync()

Repeatedly attempts to acquire a session until successful or the timeout expires.

```lua
local session = Store:LoadSessionAsync(key, maxWait)
```

### Parameters

| Name | Type |
|------|------|
| key | string |
| maxWait | number? |

### Returns

`DataSession?`

---

## Founder:PublishGlobalUpdate()

Queues or immediately dispatches a global update.

```lua
Store:PublishGlobalUpdate(key, payload)
```

### Parameters

| Name | Type |
|------|------|
| key | string |
| payload | table |

### Returns

`boolean`

---

## Founder:ForceEjectKey()

Requests every server to release a specific profile.

```lua
Store:ForceEjectKey(key)
```

---

## Founder:GetActiveSessionCount()

Returns the number of currently loaded sessions.

```lua
local count = Store:GetActiveSessionCount()
```

### Returns

`number`

---

# DataSession

Represents an actively loaded profile.

---

## DataSession.Data

Observable data table.

```lua
session.Data.Coins += 500

session.Data.Inventory.Sword = true
```

---

## DataSession:Save()

Performs a full profile save.

```lua
session:Save()
```

### Returns

`boolean`

---

## DataSession:SavePatch()

Saves only dirty top-level fields.

```lua
session:SavePatch()
```

### Returns

`boolean`

---

## DataSession:BeginTransaction()

Begins a rollback transaction.

```lua
session:BeginTransaction()
```

---

## DataSession:CommitTransaction()

Commits the current transaction.

```lua
session:CommitTransaction()
```

---

## DataSession:RollbackTransaction()

Restores data to the transaction snapshot.

```lua
session:RollbackTransaction()
```

Returns `boolean`.

---

## DataSession:StartAutoSave()

Starts the autosave loop.

```lua
session:StartAutoSave(interval)
```

---

## DataSession:StopAutoSave()

Stops the autosave loop.

```lua
session:StopAutoSave()
```

---

## DataSession:IsCacheStale()

Checks whether cloud data is newer than local memory.

```lua
local stale = session:IsCacheStale()
```

Returns `boolean`.

---

## DataSession:ConsumeGlobalUpdates()

Returns queued global updates.

```lua
local updates = session:ConsumeGlobalUpdates()
```

Returns `{table}`.

---

## DataSession:ListenToGlobalUpdate()

Listens for live global updates.

```lua
session:ListenToGlobalUpdate(function(update)

end)
```

---

## DataSession:ListenToFieldChange()

Fires whenever any field changes.

```lua
session:ListenToFieldChange(function(key, value, rootKey)

end)
```

---

## DataSession:GetPerformanceMetadata()

Returns runtime metadata.

```lua
local metadata = session:GetPerformanceMetadata()
```

Returns `table`.

---

## DataSession:Release()

Releases ownership without forcing a save.

```lua
session:Release()
```

---

## DataSession:Destroy()

Saves then releases the session.

```lua
session:Destroy()
```

---
