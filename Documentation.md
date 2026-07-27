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
