--// PersonaStore.lua \\--

--[[
    ========================================================================================
    PersonaStore - A ProfileStore-inspired persistence engine built
    entirely with an OOP architecture, explicit data isolation, session
    locking, observable state tracking, and cross-server synchronization.
    
    Also includes state profile session locking, change tracking, and auto-save.
    ========================================================================================
   By: @DraxxorDev
   Version: 1.0.0

    Description:
        A robust DataStore engine that manages player persistence profiles.
        It uses OOP, explicit data isolation, cross-server
        concurrency handlers, and transactional state protections.

    Blueprint & Components:
        1. PersonaStore (Main Global Engine Manager):
           Tracks database topologies, manages global server networks, and handles
           server shutdowns.
        2. Founder (Database Factory Class):
           Acts as an isolated interface mapping directly to a specific cloud DataStore.
           Allows separate schemas for items like PlayerData, GuildData, or GlobalLeaderboards.
        3. DataSession (Atomic Working Profile Class):
           An active profile wrapper that handles session locks, in-memory operations,
           rollback histories, change tracking proxies, and auto-saves.

    Advanced Features (Beyond ProfileStore i THINK):
        - Multi-Level Initializer Pipeline via .new() and :CreateDataStore() factories.
        - Atomic Memory Transactions allowing multi-step writes with hardware-level rollbacks.
        - High-Fidelity Deep Observable Proxy Tree to track nested mutation events.
        - Distributed Messaging Cluster Network to clear stale session locks instantly.
        - High-Efficiency Selective Data Patching to minimize network overhead.
        - On-Demand Cache Verification Layer to prevent stale reads.
        - Live Cross-Server Global Updates (online push + offline queue), so you can grant
          items / mail / bans to a player regardless of which server currently owns them.
        - Resilient Retry Layer with exponential backoff + jitter around every DataStore call.
        - Engine-wide BindToClose drain that saves & releases every open session on every
          registered store before the server is allowed to fully shut down.

    How the pieces connect (read this before touching internals):
        PersonaStore (engine singleton)
            -> owns ONE MessagingService subscription for the whole server (cheap: MessagingService
               has strict per-topic budgets, so every store/session shares this single pipe).
            -> routes incoming cluster messages to the correct Founder by name, which
               in turn routes to the correct `DataSession` by key. This fan-out is why
               PersonaStore.RegisteredStores exists: it's the lookup table the router walks.
            -> owns the single BindToClose hook that walks every store's _activeSessions and
               drains them, so individual scripts never have to remember to do this themselves.
        Founder (one per DataStore, e.g. "PlayerData")
            -> wraps a single DataStoreService:GetDataStore() handle.
            -> is the thing that actually calls UpdateAsync to acquire/steal/release locks.
            -> is also the thing that PUBLISHES cluster messages (both the "please die" eviction
               ping and "here's a live global update" ping), because it's the only object that
               knows both the store name (needed for routing) and the raw DataStore handle
               (needed to persist an offline queue when no server has the key loaded).
        DataSession (one per loaded profile)
            -> the only object gameplay code should ever read/write through.
            -> owns the observable proxy tree (Data) and the dirty-field tracker that makes
               patch saves possible, plus the autosave heartbeat loop.
               
               
    --// Q & A

	### Why not just use Roblox DataStores directly?
		You absolutely can. PersonaStore simply provides a higher-level
		abstraction with session locking, autosaving, change tracking,
		global updates, transactions, and other quality-of-life features
		that would otherwise need to be implemented manually.

	---

	### Is PersonaStore a replacement for ProfileStore?
		Not necessarily. PersonaStore is heavily inspired by ProfileStore,
		but it was built from scratch with its own architecture, APIs,
		and feature set.

	---

	### Does PersonaStore support multiple DataStores?
		Yes. Each `Founder` instance represents a completely isolated
		DataStore, allowing you to separate player data, guilds,
		leaderboards, or any other persistent systems.

	---

	### What happens if two servers load the same profile?
		PersonaStore uses session locking to prevent concurrent
		ownership. If another server owns the profile, loading will fail
		(or wait when using `LoadSessionAsync()`) unless the lock has
		expired or the owning server releases it.

	---

	### Are nested table changes detected automatically?
		Yes. PersonaStore wraps your data in an observable proxy tree,
		allowing mutations deep inside nested tables to automatically
		mark the appropriate top-level field as dirty for patch saves.

	---

	### What's the difference between `:Save()` and `:SavePatch()`?
		`Save()` writes the entire profile back to the DataStore.

		`SavePatch()` only writes the top-level fields that have changed
		since the previous successful save, reducing unnecessary
		serialization and network overhead.

	---

	### Do I need to call `:Save()` myself?
		Usually, no. PersonaStore automatically performs periodic patch
		saves and drains every active session during server shutdown.
		Manual saves are typically reserved for important checkpoints.

	---

	### Can I grant items to offline players?
		Yes. `PublishGlobalUpdate()` supports both live cross-server
		updates and offline update queues. If the player isn't currently
		loaded, the update will be delivered the next time their profile
		is opened.

	---

	### Does PersonaStore automatically reconcile new data fields?
		Yes. Any missing fields defined in your schema are automatically
		added when a session is loaded without overwriting existing data.

	---

	### Can I use this on the client?
		No. PersonaStore is a **server-only** persistence framework and
		will throw an error if required from the client.
	
	---
	
	
	Made with love and passion for other developers <3.
	
	Version 1.0.0
	
	By @Draxxor
]]--

--// Types
export type table =
	{ [string]: any } | { [number]: any } | { table }

--// Services
local DataStoreService = game:GetService("DataStoreService")
local MessagingService = game:GetService("MessagingService")
local HttpService = game:GetService("HttpService")
local RunService = game:GetService("RunService")
local Players = game:GetService("Players")

--// Interchangables

-- Global state tracking configuration
local PersonaStore = {
	EngineActive = false,
	RegisteredStores = {}, -- storeName -> Founder instance, used by the message router
	GlobalLockTimeout = 120,
	GlobalAutoSaveInterval = 30,
	ClusterTopic = "PersonaStore_Cluster_Signals",
}

--// Constants (DO NOT TOUCH UNLESS YOU KNOW WHAT YOU'RE DOING!)
PersonaStore.__index = PersonaStore
PersonaStore._messagingConnection = nil
PersonaStore._bindToCloseRegistered = false

--// Private Utility Functions (Internal Use Only)

-- Performs a deep memory clone of an associative or sequential tree to isolate templates
local function deepCopy(original: table): table
	local copy = {}
	
	for key, value in original do
		if type(value) == "table" then
			copy[key] = deepCopy(value)
		else
			copy[key] = value
		end
	end

	return copy
end

-- Safely fills missing keys in a target table using a structural template reference
local function reconcileData(target: table, template: table)
	for key, value in pairs(template) do
		if target[key] == nil then
			if type(value) == "table" then
				target[key] = deepCopy(value)
			else
				target[key] = value
			end
		elseif type(target[key]) == "table" and type(value) == "table" then
			reconcileData(target[key], value)
		end
	end
end

-- Generates a deeply nested proxy framework to intercept mutations and trigger listeners.
-- `rootKey` is threaded down through recursive calls so that no matter how deep a write
-- happens (e.g. Data.Inventory.Weapons.Sword.Enchants.Fire = true), the callback always
-- also knows which TOP-LEVEL field ("Inventory") was actually touched. That top-level name
-- is what `DataSession` uses as the patch key for selective saves further down.
local function createObservableTable(rawTable: table, changeCallback: (key: string, value: any, rootKey: string) -> (), rootKey: string?)
	local proxy = {}
	local internalShadow = {}

	for key, value in pairs(rawTable) do
		local effectiveRoot = rootKey or key -- only nil on the very first (top) call

		if type(value) == "table" then
			internalShadow[key] = createObservableTable(value, changeCallback, effectiveRoot)
		else
			internalShadow[key] = value
		end
	end

	setmetatable(proxy, {
		__index = internalShadow,
		__newindex = function(_, key, newValue)
			local effectiveRoot = rootKey or key
			
			if type(newValue) == "table" then
				internalShadow[key] = createObservableTable(newValue, changeCallback, effectiveRoot)
			else
				internalShadow[key] = newValue
			end
			
			changeCallback(key, newValue, effectiveRoot)
		end,
		__iter = function()
			return next, internalShadow, nil
		end,
	})

	return proxy
end

local function profileRaw(elapsed: number): table
	return {
		Data = {},
		SessionToken = nil,
		JobId = nil,
		LastUpdate = elapsed,
		Version = 0,
		GlobalUpdates = {}
	}
end

-- Resilient wrapper around DataStoreService calls. DataStore requests are throttled and can
-- transiently fail under load; retrying blindly in a tight loop just makes the throttling
-- worse, so this backs off exponentially and adds random jitter so many servers retrying at
-- once don't all hammer the API in lockstep.
local function withRetry(attemptFn: () -> any, maxAttempts: number?): (boolean, any)
	local attempts = maxAttempts or 5
	local lastError = nil

	for attemptIndex = 1, attempts do
		local success, result = pcall(attemptFn)
		if success then
			return true, result
		end

		lastError = result

		task.wait(
			math.min(2 ^ attemptIndex, 16) + (math.random() * 0.5) -- exponential backoff delay with jitter
		)
	end

	return false, lastError
end

--// Class DataSession
-- This is the main class for interacting with the persistent data store.

local DataSession = {}
DataSession.__index = DataSession

-- Factory function to create a runtime session wrapper
function DataSession.new<T>(key: string, rawData: table, token: string, parentStore): DataSession<T>
	-- The DataSession class is a wrapper around the raw data table that provides a convenient
	-- interface for listening to changes and saving data.
	
	local self = setmetatable({}, DataSession)
	self._key = key
	self._token = token
	self._storeRef = parentStore
	self._isReleased = false
	self._transactionBackup = nil
	self._metadata = {
		Created = os.time(),
		LastSaved = os.time(),
		WriteCycles = 0,
	}

	self._changeListeners = {}
	self._globalUpdateListeners = {}
	self._dirtyFields = {} -- set of top-level field names changed since the last successful save
	self._autoSaveThread = nil
	self._pendingGlobalUpdates = {}

	-- Instantiates the observable entry tree bound to the event dispatch channel. Every
	-- mutation (at any depth) marks its top-level field dirty AND fires any listeners the
	-- game code attached via :ListenToFieldChange, so UI/leaderstat syncing and save-patching
	-- both piggyback off the same single proxy tree instead of needing two systems.
	self.Data = createObservableTable(rawData.Data, function(mutatedKey, mutatedValue, rootKey)
		self._metadata.LastModified = os.time()
		self._dirtyFields[rootKey] = true

		for _, listener in ipairs(self._changeListeners) do
			task.spawn(listener, mutatedKey, mutatedValue, rootKey)
		end
	end)

	return self
end

-- Connects an execution callback to fire when any property is updated
function DataSession:ListenToFieldChange(callback: (key: string, value: any, rootKey: string) -> ())
	table.insert(self._changeListeners, callback)
end

-- Connects a callback that fires whenever a live global update arrives from another server
-- while this session is loaded (e.g. an admin panel running elsewhere granting an item).
function DataSession:ListenToGlobalUpdate(callback: (payload: table) -> ())
	table.insert(self._globalUpdateListeners, callback)
end

-- Feature: Atomic Transaction Protection (Rollback Setup)
function DataSession:BeginTransaction()
	if self._isReleased then return false end
	self._transactionBackup = deepCopy(self.Data)

	return true
end

-- Commits current memory allocations and discards the historical rollback stack
function DataSession:CommitTransaction()
	self._transactionBackup = nil
end

-- Instantly reverts the profile fields back to the pre-transaction state
function DataSession:RollbackTransaction()
	if not self._transactionBackup then return false end

	for key, value in self._transactionBackup do
		self.Data[key] = value
	end

	self._transactionBackup = nil

	return true
end

-- Forces an immediate FULL data synchronization write directly to the Roblox cloud.
-- This overwrites the entire Data blob and is the safest option (guarantees no field is
-- ever missed), but costs the most bandwidth. Use :SavePatch() for routine autosaves and
-- reserve :Save() for release-time / critical checkpoints.
function DataSession:Save(): boolean
	if self._isReleased then return false end

	local success, err = withRetry(function()
		self._storeRef._storageInstance:UpdateAsync(self._key, function(currentRaw)
			if not currentRaw or currentRaw.SessionToken ~= self._token then
				error("Ownership mismatch detected; session token has lapsed.")
			end

			currentRaw.Data = deepCopy(self.Data)
			currentRaw.LastUpdate = os.time()
			currentRaw.Version = (currentRaw.Version or 0) + 1

			return currentRaw
		end)
	end)

	if success then
		self._metadata.LastSaved = os.time()
		self._metadata.WriteCycles += 1
		self._dirtyFields = {}

		return true
	else
		warn("[PersonaStoreSession]: Save failure on key: " .. self._key .. " -> " .. tostring(err))

		return false
	end
end

-- Feature: High-Efficiency Selective Data Patching. Instead of re-serializing the WHOLE
-- profile every autosave tick, this only ships the top-level fields that were actually
-- mutated since the last save (tracked via _dirtyFields in the observable proxy callback).
-- On heavy profiles (deep inventories, quest logs, etc.), this dramatically cuts down the
-- payload size of routine autosaves. Falls back to a no-op if nothing changed, which saves
-- an entire DataStore request budget slot for genuinely idle players.
function DataSession:SavePatch(): boolean
	if self._isReleased then return false end

	local dirtyKeys = {}

	for fieldName in pairs(self._dirtyFields) do
		table.insert(dirtyKeys, fieldName)
	end

	if #dirtyKeys == 0 then
		return true -- nothing to do; treat as a trivially successful save
	end

	local exportedSnapshot = deepCopy(self.Data)

	local success, err = withRetry(function()
		self._storeRef._storageInstance:UpdateAsync(self._key, function(currentRaw)
			if not currentRaw or currentRaw.SessionToken ~= self._token then
				error("Ownership mismatch detected; session token has lapsed.")
			end

			currentRaw.Data = currentRaw.Data or {}

			-- Only merge in the fields we actually know changed, leaving anything a sibling
			-- process wrote (e.g. a queued global update consumed elsewhere) untouched.
			for _, fieldName in ipairs(dirtyKeys) do
				currentRaw.Data[fieldName] = exportedSnapshot[fieldName]
			end

			currentRaw.LastUpdate = os.time()
			currentRaw.Version = (currentRaw.Version or 0) + 1

			return currentRaw
		end)
	end)

	if success then
		self._metadata.LastSaved = os.time()
		self._metadata.WriteCycles += 1
		self._dirtyFields = {}

		return true
	else
		warn("[PersonaStoreSession]: Patch save failure on key: " .. self._key .. " -> " .. tostring(err))

		return false
	end
end

-- Starts the autosave heartbeat. Runs as a cancellable task.spawn loop rather than a
-- while-true in the constructor so :StopAutoSave() can cleanly kill it on release without
-- leaving an orphaned coroutine spinning after the session is gone which would cause throttling.
function DataSession:StartAutoSave(intervalSeconds: number)
	self:StopAutoSave()

	self._autoSaveThread = task.spawn(function()
		while not self._isReleased do
			task.wait(intervalSeconds)

			if self._isReleased then break end

			self:SavePatch()
		end
	end)
end

-- Cancels the autosave heartbeat thread, if one is running.
function DataSession:StopAutoSave()
	if self._autoSaveThread then
		task.cancel(self._autoSaveThread)

		self._autoSaveThread = nil -- clear the reference to the cancelled coroutine
	end
end

-- Feature: On-Demand Cache Verification Layer. This compares this session's in-memory version
-- stamp against whatever is currently sitting in the cloud WITHOUT taking a lock or
-- mutating anything. Useful before trusting the in-memory Data for something high-stakes
-- (e.g. a trade confirmation), since it flags if another process somehow wrote to this key
-- (this should only happen after a forced eviction/steal) so the calling code can decideto reload.
function DataSession:IsCacheStale(): boolean
	local success, remoteRaw = withRetry(function()
		return self._storeRef._storageInstance:GetAsync(self._key)
	end, 3)

	if not success or not remoteRaw then
		return false -- can't verify; assume fresh rather than falsely alarming callers
	end

	local localVersion = self._metadata.SyncedVersion or 0
	local remoteVersion = remoteRaw.Version or 0
	
	return remoteVersion > localVersion
end

-- Delivers any global updates that were queued for this key while it was offline (or owned
-- by a different server) and clears them from the in-memory queue. Call this once right
-- after loading, and again whenever ListenToGlobalUpdate fires for a live push.
function DataSession:ConsumeGlobalUpdates(): {table}
	local updates = self._pendingGlobalUpdates

	self._pendingGlobalUpdates = {} -- clear the queue

	return updates -- return the original
end

-- Internal: invoked by the PersonaStore message router when a live Global_Update arrives
-- for this exact key while the session is loaded on this server.
function DataSession:_ReceiveLiveGlobalUpdate(payload: table)
	table.insert(self._pendingGlobalUpdates, payload)

	for _, listener in ipairs(self._globalUpdateListeners) do
		task.spawn(listener, payload)
	end
end

-- Internal: invoked by the PersonaStore message router when another server broadcasts an
-- Eject_Key request for this exact key. Rather than just dying mid-write, this saves first
-- (best-effort) and only then releases, so the requesting server picks up fully fresh data
-- instead of waiting out the full GlobalLockTimeout for a stale lock to expire.
function DataSession:_HandleRemoteEviction()
	if self._isReleased then return end

	self:Save()
	self:Release()
end

-- Safely breaks cloud session locks so other cluster nodes can acquire ownership
function DataSession:Release()
	if self._isReleased then return end
	self._isReleased = true

	self:StopAutoSave()

	self._storeRef._activeSessions[self._key] = nil

	task.spawn(function()
		withRetry(function()
			self._storeRef._storageInstance:UpdateAsync(self._key, function(currentRaw)
				if not currentRaw or currentRaw.SessionToken ~= self._token then return nil end

				currentRaw.SessionToken = nil
				currentRaw.JobId = nil
				currentRaw.LastUpdate = os.time()

				return currentRaw
			end)
		end, 3)
	end)
end

-- Convenience alias: a full teardown that always saves first. Prefer this over a bare
-- :Release() for any planned disconnect (e.g. Players.PlayerRemoving) so data loss only
-- happens on genuine server crashes, never on ordinary leaves.
function DataSession:Destroy()
	self:Save()
	self:Release()
end

-- Returns an operational log of data transactions and lifecycle states
function DataSession:GetPerformanceMetadata(): table
	return deepCopy(self._metadata)
end

--// Class Founder
-- A singleton class for managing a single DataStore, including session ownership and
-- replication of global updates.

local Founder = {}
Founder.__index = Founder

-- Constructor for an isolated DataStore mapping object
function Founder.new<T>(storeName: string, configOptions: table): Founder<T>
	local self = setmetatable({}, Founder)

	self._storeName = storeName
	self._config = configOptions or {}
	self._storageInstance = DataStoreService:GetDataStore(storeName)
	self._activeSessions = {}
	self._templateSchema = self._config.Schema or {}

	PersonaStore.RegisteredStores[storeName] = self

	return self
end

-- Main interface handler to acquire, reconcile, and lock user profiles safely
function Founder:LoadSession<T>(key: string): DataSession<T>
	if self._activeSessions[key] then
		return self._activeSessions[key] -- Prevent session overlaps on this server
	end
	
	-- Safeguard: if the store is marked as disabled, return nil
	if not PersonaStore.EngineActive then
		warn("[PersonaStore]: LoadSession() called before Init()! Call PersonaStore:Init() first.")

		return nil
	end

	local uniqueToken = HttpService:GenerateGUID(false)
	local currentJobId = game.JobId ~= "" and game.JobId or "StudioInstance"
	local activeSessionObject = nil

	-- Feature 2: Broad-spectrum cluster broadcast to eject matching sessions residing in
	-- remote nodes. This is what lets PersonaStore steal a lock in near-real-time instead of
	-- waiting out GlobalLockTimeout: whichever server currently owns `key` receives this,
	-- saves, and releases itself (see `DataSession`:_HandleRemoteEviction) before we even
	-- finish our own UpdateAsync call below.
	MessagingService:PublishAsync(PersonaStore.ClusterTopic, {
		Action = "Eject_Key",
		TargetStore = self._storeName,
		TargetKey = key,
		OriginJobId = currentJobId,
	})

	local success, err = withRetry(function()
		self._storageInstance:UpdateAsync(key, function(currentRaw)
			local currentTime = os.time()

			if currentRaw then
				if currentRaw.SessionToken ~= nil then
					local elapsed = currentTime - (currentRaw.LastUpdate or 0)
					if elapsed < PersonaStore.GlobalLockTimeout and currentRaw.JobId ~= currentJobId then
						error("Deadlock validation active; slot held by instance: " .. tostring(currentRaw.JobId))
					end
				end
			else
				currentRaw = profileRaw(currentTime) -- Initialize a new profile
			end
			
			-- Set up the new session lock

			currentRaw.SessionToken = uniqueToken
			currentRaw.JobId = currentJobId
			currentRaw.LastUpdate = currentTime

			reconcileData(currentRaw.Data, self._templateSchema) -- Reconcile schema

			activeSessionObject = currentRaw

			return currentRaw -- Return the updated profile
		end)
	end)

	if not success then
		warn("[Founder]: Failed to acquire session lock for key: " .. key .. " -> " .. tostring(err))

		return nil
	end

	-- activeSessionObject was captured by reference inside the UpdateAsync closure above;
	-- it's the freshly reconciled + locked raw profile that now belongs to this server.
	local newSession = DataSession.new(key, activeSessionObject, uniqueToken, self)
	newSession._metadata.SyncedVersion = activeSessionObject.Version or 0

	self._activeSessions[key] = newSession

	-- Any global updates that were queued for this player while they were offline (or loaded
	-- on a different server) get delivered the moment the session comes online here.
	newSession._pendingGlobalUpdates = activeSessionObject.GlobalUpdates or {}

	-- Kick off the autosave heartbeat immediately so long sessions never rely purely on
	-- manual :Save() calls or the eventual :Release()/:Destroy().
	newSession:StartAutoSave(self._config.AutoSaveInterval or PersonaStore.GlobalAutoSaveInterval)

	return newSession
end

-- Queued variant of LoadSession for cases where you'd rather wait a few seconds for a lock
-- to free up than fail immediately (e.g. a player rejoining right after a crash, where the
-- old server's lock hasn't timed out yet but its eviction message may just be in flight).
-- ProfileStore-style engines typically only offer instant-fail or instant-steal; this adds
-- a real middle ground.
function Founder:LoadSessionAsync<T>(key: string, maxWaitSeconds: number?): DataSession<T>?
	local deadline = os.clock() + (maxWaitSeconds or 10)

	while os.clock() < deadline do
		local session = self:LoadSession(key)
		if session then
			return session
		end

		task.wait(1)
	end

	return nil
end

-- Feature: Live Cross-Server Global Updates. Lets ANY server hand data to a player's
-- profile even if that player isn't currently loaded here (or isn't even online). If the
-- key is loaded on this server, deliver it instantly through the observable session. If
-- not, persist it into the profile's GlobalUpdates queue in the cloud so it's picked up the
-- next time the profile loads anywhere (see LoadSession's activeSessionObject.GlobalUpdates
-- assignment above), and also fan the update out over MessagingService in case some other
-- currently-running server has it loaded right now.
function Founder:PublishGlobalUpdate(key: string, payload: table): boolean
	local locallyLoadedSession = self._activeSessions[key]
	if locallyLoadedSession then
		locallyLoadedSession:_ReceiveLiveGlobalUpdate(payload)
	end

	local success = withRetry(function()
		self._storageInstance:UpdateAsync(key, function(currentRaw)
			if not currentRaw then
				currentRaw = profileRaw(os.time())
			end

			currentRaw.GlobalUpdates = currentRaw.GlobalUpdates or {}

			table.insert(currentRaw.GlobalUpdates, payload)

			return currentRaw
		end)
	end)

	-- Also broadcast live so a server that currently owns the session (but is a DIFFERENT
	-- Luau VM than this one, since PublishGlobalUpdate can be called from any server) can
	-- react without waiting for its own next reconciliation/reload.
	MessagingService:PublishAsync(PersonaStore.ClusterTopic, {
		Action = "Global_Update",
		TargetStore = self._storeName,
		TargetKey = key,
		Payload = payload,
	})

	return (success == true)
end

-- Forcibly requests eviction of a key without loading it yourself. Useful for admin tooling
-- that just wants to make sure nobody has a stale lock before, say, a manual DataStore edit.
function Founder:ForceEjectKey(key: string)
	MessagingService:PublishAsync(PersonaStore.ClusterTopic, {
		Action = "Eject_Key",
		TargetStore = self._storeName,
		TargetKey = key,
		OriginJobId = game.JobId ~= "" and game.JobId or "StudioInstance",
	})
end

-- Simple diagnostic used by ops dashboards / console commands to see the load on this store.
function Founder:GetActiveSessionCount(): number
	local count = 0
	
	for _ in pairs(self._activeSessions) do
		count += 1
	end

	return count
end

-- Creates the engine singleton. Kept separate from Init() so the
-- module can be required (building the table, setting up class metatables) without
-- necessarily standing up live services yet — handy for unit testing individual classes.
-- Not essential to be called, as it returns another singleton metatable.
function PersonaStore.new()
	return setmetatable({}, PersonaStore) -- :D
end

-- Registers and returns a new isolated Founder. This is the
-- primary entry point game code should call, e.g.:
--   local PlayerStore = PersonaStore:CreateDataStore("PlayerData_v1", { Schema = Template })
-- If the store has already been registered, the existing store is returned to
-- store instances to share the same connection.
function PersonaStore:CreateDataStore<T>(storeName: string, configOptions: table): Founder<T>
	if self.RegisteredStores[storeName] then
		return self.RegisteredStores[storeName] :: T -- :D
	end
	
	return Founder.new(storeName, configOptions) :: T -- :D
end

-- Boots the shared MessagingService subscription and the BindToClose drain. Call this ONCE
-- from a server Script at game startup, before any :LoadSession calls happen, so eviction
-- and global-update messages aren't missed during the earliest joins.
function PersonaStore:Init()
	if PersonaStore.EngineActive then
		warn("[PersonaStore]: Init() called more than once!")
		
		return
	end

	PersonaStore.EngineActive = true

	-- Single shared subscription for the whole server. Every store/session piggybacks off
	-- this one connection and gets routed by TargetStore/TargetKey, which keeps us well
	-- under MessagingService's per-topic subscriber limits even with dozens of stores.
	local success, connection = pcall(function()
		return MessagingService:SubscribeAsync(PersonaStore.ClusterTopic, function(message)
			local data = message.Data
			if type(data) ~= "table" then return end

			local targetStore = PersonaStore.RegisteredStores[data.TargetStore]
			if not targetStore then return end

			local currentJobId = game.JobId ~= "" and game.JobId or "StudioInstance"

			if data.Action == "Eject_Key" then
				-- Ignore our own broadcasts; a server never needs to evict itself.
				if data.OriginJobId == currentJobId then return end

				local session = targetStore._activeSessions[data.TargetKey]
				if session then
					session:_HandleRemoteEviction() -- in case the session is still running
				end
			elseif data.Action == "Global_Update" then
				local session = targetStore._activeSessions[data.TargetKey]
				if session then
					session:_ReceiveLiveGlobalUpdate(data.Payload) -- self explanitory
				end
			end
		end)
	end)

	if success then
		PersonaStore._messagingConnection = connection
	else
		warn("[PersonaStore]: Failed to subscribe to cluster topic -> " .. tostring(connection))
	end

	-- Engine-wide server shutdown: drains every active session on every registered store.
	-- Centralizing this here means individual game scripts never need their own
	-- Players.PlayerRemoving/BindToClose gatekeeping just to avoid losing data on deploys.
	if not PersonaStore._bindToCloseRegistered then
		PersonaStore._bindToCloseRegistered = true

		game:BindToClose(function()
			if RunService:IsStudio() then
				return -- studio test sessions don't need the full drain, and it can stall the editor
			end

			for _, store in pairs(PersonaStore.RegisteredStores) do
				for _, session in pairs(store._activeSessions) do
					session:Destroy() -- auto-removes from _activeSessions
				end
			end

			-- Give in-flight UpdateAsync retries a moment to land before Roblox force-kills
			-- the server process.
			task.wait(2)

			-- If the server is shutting down, then the server is shutting down.
			-- No point in returning.
			-- So that means we're done!
		end)
	end
end

-- Client require guard
task.spawn(function()
	if not RunService:IsServer() then
		error("PersonaStore can only be required from the server")
	end

	return {}
end)

--// Final Exports for autocomplete
export type Founder<T> = typeof(Founder) & {
	LoadSession: (self: Founder<T>, key: string) -> DataSession<T>,
	LoadSessionAsync: (self: Founder<T>, key: string, maxWaitSeconds: number?) -> DataSession<T>?,
}

export type Session<T> = {
	Data: T,
	SessionToken: string?,
	JobId: string?,
	LastUpdate: number,
	Version: number,
	GlobalUpdates: table,
}

export type DataSession<T> = typeof(DataSession) & Session<T>

return PersonaStore
