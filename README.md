# PersonaStore

> A ProfileStore-inspired persistence engine for Roblox, built from scratch with an object-oriented architecture, explicit data isolation, session locking, observable state tracking, transactional operations, and cross-server synchronization.

---

## Features

-  Session locking with automatic ownership validation
-  Automatic periodic patch saves
-  Selective data patching (only changed fields are written)
-  Deep observable proxy tree for nested table tracking
-  Cross-server global update system
-  MessagingService-powered cluster synchronization
-  Transaction support with rollback protection
-  Automatic schema reconciliation
-  Retry layer with exponential backoff + jitter
-  Runtime metadata & diagnostics
-  Automatic BindToClose cleanup
-  Multiple isolated DataStores through Founder objects

---

# Installation

Place `PersonaStore.lua` inside **ServerScriptService** or another server-only location.

```lua
local PersonaStore = require(path.To.PersonaStore)
```

Initialize the engine once during server startup.

```lua
local PersonaStore = require(ServerStorage.PersonaStore)

PersonaStore:Init()
```

---

# Quick Start

Create a schema.

```lua
local PlayerTemplate = {
    Cash = 0,
    Gems = 0,

    Inventory = {},

    Stats = {
        Level = 1,
        XP = 0
    }
}
```

Create a DataStore.

```lua
local PlayerStore = PersonaStore:CreateDataStore("PlayerData", {
    Schema = PlayerTemplate
})
```

Load a session.

```lua
local session = PlayerStore:LoadSession(tostring(player.UserId))

if not session then
    player:Kick("Failed to load data.")
    return
end
```

Access data normally.

```lua
session.Data.Cash += 100

session.Data.Stats.Level += 1
```

Destroy the session when finished.

```lua
session:Destroy()
```

---

# Architecture

PersonaStore is composed of three primary objects.

| Object | Purpose |
|---------|----------|
| **PersonaStore** | Global engine responsible for initialization, routing, shutdown handling, and DataStore creation. |
| **Founder** | Represents a single isolated DataStore and manages session ownership. |
| **DataSession** | Active profile wrapper used for reading, writing, saving, and listening to data changes. |

---

# Example

> Without Generic Types (No AutoComplete)
```lua
local ServerStorage = game:GetService("ServerStorage")
local Players = game:GetService("Players")

local PersonaStore = require(ServerStorage.PersonaStore)
PersonaStore:Init()

local PlayerStore = PersonaStore:CreateDataStore("PlayerData", {
    Schema = {
        Coins = 0,
        Inventory = {}
    }
})

Players.PlayerAdded:Connect(function(player)
    local session = PlayerStore:LoadSession(tostring(player.UserId))

    if not session then
        player:Kick("Unable to load your profile.")
        return
    end

    session.Data.Coins += 500

    player.AncestryChanged:Connect(function(_, parent)
        if not parent then
            session:Destroy()
        end
    end)
end)
```

> With Generic Types (AutoComplete For Data)
```lua
local ServerStorage = game:GetService("ServerStorage")
local Players = game:GetService("Players")

local PersonaStore = require(ServerStorage.PersonaStore)
PersonaStore:Init()

local DataTemplate = {
    Coins = 0,
    Inventory = {}
}

local PlayerStore: PersonaStore.Founder<typeof(DataTemplate)> = PersonaStore:CreateDataStore("PlayerData", {
    Schema = DataTemplate
}) -- You now have autocomplete for session.Data!

Players.PlayerAdded:Connect(function(player)
    local session = PlayerStore:LoadSession(tostring(player.UserId))

    if not session then
        player:Kick("Unable to load your profile.")
        return
    end

    session.Data.Coins += 500

    player.AncestryChanged:Connect(function(_, parent)
        if not parent then
            session:Destroy()
        end
    end)
end)
```

---

# Why PersonaStore?

Roblox's DataStoreService is intentionally low-level.

PersonaStore builds on top of it by providing:

- Session ownership
- Automatic saving
- Observable data
- Transactions
- Patch saving
- Global updates
- Retry handling
- Cross-server synchronization
- Schema reconciliation

so developers can spend more time building games rather than persistence infrastructure.

---

# License

# [MIT License]

Open-source for the Roblox Development community.

I've linked other really good DataStoreService modules as well in the sources folder.

Made with ❤️ by **@DraxxorDev**
