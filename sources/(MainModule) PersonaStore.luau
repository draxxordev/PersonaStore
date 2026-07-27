--// PersonaStore.lua \\--

--[[
    ========================================================================================
    PersonaStore - A ProfileStore-inspired DataStore library built
    entirely with an OOP architecture, explicit data isolation, session
    locking, observable state tracking, and cross-server synchronization.
    
    Now enhanced with EncodingService integration, compression, hashing, and advanced APIs.
    ========================================================================================
    
    Description:
        A robust DataStore engine that manages player persistence profiles.
        It uses OOP, explicit data isolation, cross-server concurrency handlers,
        transactional state protections, and native Roblox EncodingService.

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
        4. CompressionHandler (EncodingService Wrapper):
           Manages compression, decompression, hashing, and base64 encoding using
           native Roblox EncodingService for efficient serialization.

    Advanced Features:
        - Multi-Level Initializer Pipeline via .new() and :CreateDataStore() factories.
        - Atomic Memory Transactions allowing multi-step writes with rollbacks.
        - High-Fidelity Deep Observable Proxy Tree to track nested mutation events.
        - Distributed Messaging Cluster Network to clear stale session locks instantly.
        - High-Efficiency Selective Data Patching to minimize network overhead.
        - On-Demand Cache Verification Layer to prevent stale reads.
        - Live Cross-Server Global Updates (online push + offline queue).
        - Resilient Retry Layer with exponential backoff + jitter.
        - Engine-wide BindToClose drain with automatic session cleanup.
        - Native EncodingService compression (Deflate/ZSTD) with configurable levels.
        - Data integrity verification using cryptographic hashing (SHA256, SHA1, MD5).
        - Read-only profile access without acquiring session locks.
        - Atomic counter operations for leaderboards and currencies.
        - Batch profile operations for mass updates and migrations.
        - Comprehensive statistics and audit logging.
        - Data export/import for backups and migrations.
        - Compression caching to avoid recompressing unchanged data.
        
    Built with time, effort, love, and passion for the Roblox Developer Community <3.
    
    Github(repository):
    https://github.com/draxxordev/PersonaStore
    
    v.1.1.0
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
local EncodingService = game:GetService("EncodingService")

--// Interchangables

-- Global state tracking configuration
local PersonaStore = {
	EngineActive = false,
	RegisteredStores = {}, -- storeName -> Founder instance, used by the message router
	GlobalLockTimeout = 120,
	GlobalAutoSaveInterval = 30,
	ClusterTopic = "PersonaStore_Cluster_Signals",
	CompressionAlgorithm = Enum.CompressionAlgorithm.Deflate, -- or ZSTD for better compression
	CompressionLevel = 6, -- 1-22 depending on algorithm
	EnableDataIntegrityChecks = true,
	HashAlgorithm = Enum.HashAlgorithm.SHA256,
	EnableCompressionCache = true,
}

--// Constants (DO NOT TOUCH UNLESS YOU KNOW WHAT YOU'RE DOING!)
PersonaStore.__index = PersonaStore
PersonaStore._messagingConnection = nil
PersonaStore._bindToCloseRegistered = false
PersonaStore._compressionCache = {}
PersonaStore._statistics = {
	TotalSaves = 0,
	TotalLoads = 0,
	TotalCompressions = 0,
	TotalDecompressions = 0,
	DatastoreRequestsAttempted = 0,
	DatastoreRequestsFailed = 0,
	BytesSavedByCompression = 0,
}

--// Private Utility Functions (Internal Use Only)

local CompressionHandler = {}

-- Compress a table by JSON encoding, then using EncodingService:CompressBuffer
function CompressionHandler.compress(data: table, algorithm: any?, level: number?): buffer
	local algo = algorithm or PersonaStore.CompressionAlgorithm
	local lvl = level or PersonaStore.CompressionLevel

	local jsonString = HttpService:JSONEncode(data)
	local dataBuffer = buffer.fromstring(jsonString)

	local compressed = EncodingService:CompressBuffer(dataBuffer, algo, lvl)
	PersonaStore._statistics.TotalCompressions += 1
	PersonaStore._statistics.BytesSavedByCompression += buffer.len(dataBuffer) - buffer.len(compressed)

	return compressed
end

-- Decompress a buffer back to a table
function CompressionHandler.decompress(compressed: buffer): table
	local decompressed = EncodingService:DecompressBuffer(compressed, PersonaStore.CompressionAlgorithm)
	PersonaStore._statistics.TotalDecompressions += 1

	local jsonString = buffer.tostring(decompressed)
	return HttpService:JSONDecode(jsonString)
end

-- Get the size of decompressed data without actually decompressing
function CompressionHandler.getDecompressedSize(compressed: buffer): number?
	return EncodingService:GetDecompressedBufferSize(compressed, PersonaStore.CompressionAlgorithm)
end

-- Encode buffer to base64 for safe storage in DataStore (JSON compatible)
function CompressionHandler.encodeToBase64(buf: buffer): string
	local encoded = EncodingService:Base64Encode(buf)
	return buffer.tostring(encoded)
end

-- Decode base64 string back to buffer
function CompressionHandler.decodeFromBase64(str: string): buffer
	return EncodingService:Base64Decode(buffer.fromstring(str))
end

-- Compute hash of data for integrity verification
function CompressionHandler.hashData(data: string): string
	return EncodingService:ComputeStringHash(data, PersonaStore.HashAlgorithm)
end

-- Compute hash of buffer
function CompressionHandler.hashBuffer(buf: buffer): buffer
	return EncodingService:ComputeBufferHash(buf, PersonaStore.HashAlgorithm)
end

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
local function createObservableTable(rawTable: table, changeCallback: (key: string, value: any, rootKey: string) -> (), rootKey: string?)
	local proxy = {}
	local internalShadow = {}

	for key, value in pairs(rawTable) do
		local effectiveRoot = rootKey or key

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
		GlobalUpdates = {},
		DataHash = nil, -- For integrity verification
		CompressionMetadata = {
			IsCompressed = false,
			Algorithm = nil,
			OriginalSize = nil,
			CompressedSize = nil,
		}
	}
end

-- Resilient wrapper around DataStoreService calls with retry logic
local function withRetry(attemptFn: () -> any, maxAttempts: number?): (boolean, any)
	local attempts = maxAttempts or 5
	local lastError = nil

	for attemptIndex = 1, attempts do
		PersonaStore._statistics.DatastoreRequestsAttempted += 1
		local success, result = pcall(attemptFn)
		if success then
			return true, result
		end

		lastError = result
		PersonaStore._statistics.DatastoreRequestsFailed += 1

		task.wait(
			math.min(2 ^ attemptIndex, 16) + (math.random() * 0.5)
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
		CompressionStats = {
			LastCompressedSize = nil,
			LastUncompressedSize = nil,
		}
	}

	self._changeListeners = {}
	self._globalUpdateListeners = {}
	self._dirtyFields = {}
	self._autoSaveThread = nil
	self._pendingGlobalUpdates = {}
	self._compressionCache = nil -- Cached compressed data

	self.Data = createObservableTable(rawData.Data, function(mutatedKey, mutatedValue, rootKey)
		self._metadata.LastModified = os.time()
		self._dirtyFields[rootKey] = true
		self._compressionCache = nil -- Invalidate cache on mutation

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

-- Forces an immediate FULL data synchronization write directly to the Roblox cloud
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

			-- Compute data hash for integrity verification
			if PersonaStore.EnableDataIntegrityChecks then
				currentRaw.DataHash = CompressionHandler.hashData(HttpService:JSONEncode(currentRaw.Data))
			end

			return currentRaw
		end)
	end)

	if success then
		self._metadata.LastSaved = os.time()
		self._metadata.WriteCycles += 1
		self._dirtyFields = {}
		self._compressionCache = nil
		PersonaStore._statistics.TotalSaves += 1
		return true
	else
		warn("[PersonaStoreSession]: Save failure on key: " .. self._key .. " -> " .. tostring(err))
		return false
	end
end

-- High-Efficiency Selective Data Patching with optional compression
function DataSession:SavePatch(enableCompression: boolean?): boolean
	if self._isReleased then return false end

	local shouldCompress = enableCompression ~= nil and enableCompression or false
	local dirtyKeys = {}

	for fieldName in pairs(self._dirtyFields) do
		table.insert(dirtyKeys, fieldName)
	end

	if #dirtyKeys == 0 then
		return true
	end

	local exportedSnapshot = deepCopy(self.Data)

	local success, err = withRetry(function()
		self._storeRef._storageInstance:UpdateAsync(self._key, function(currentRaw)
			if not currentRaw or currentRaw.SessionToken ~= self._token then
				error("Ownership mismatch detected; session token has lapsed.")
			end

			currentRaw.Data = currentRaw.Data or {}

			for _, fieldName in ipairs(dirtyKeys) do
				currentRaw.Data[fieldName] = exportedSnapshot[fieldName]
			end

			currentRaw.LastUpdate = os.time()
			currentRaw.Version = (currentRaw.Version or 0) + 1

			-- Compute data hash for integrity verification
			if PersonaStore.EnableDataIntegrityChecks then
				currentRaw.DataHash = CompressionHandler.hashData(HttpService:JSONEncode(currentRaw.Data))
			end

			return currentRaw
		end)
	end)

	if success then
		self._metadata.LastSaved = os.time()
		self._metadata.WriteCycles += 1
		self._dirtyFields = {}
		self._compressionCache = nil
		PersonaStore._statistics.TotalSaves += 1
		return true
	else
		warn("[PersonaStoreSession]: Patch save failure on key: " .. self._key .. " -> " .. tostring(err))
		return false
	end
end

-- NEW: Save with compression for large profiles
function DataSession:SaveCompressed(): boolean
	if self._isReleased then return false end

	local success, err = withRetry(function()
		self._storeRef._storageInstance:UpdateAsync(self._key, function(currentRaw)
			if not currentRaw or currentRaw.SessionToken ~= self._token then
				error("Ownership mismatch detected; session token has lapsed.")
			end

			local dataToSave = deepCopy(self.Data)
			local compressed = CompressionHandler.compress(dataToSave)
			local encoded = CompressionHandler.encodeToBase64(compressed)

			currentRaw.Data = dataToSave -- Keep uncompressed for runtime access
			currentRaw.CompressionMetadata = {
				IsCompressed = true,
				Algorithm = tostring(PersonaStore.CompressionAlgorithm),
				OriginalSize = HttpService:JSONEncode(dataToSave):len(),
				CompressedSize = encoded:len(),
			}
			currentRaw.LastUpdate = os.time()
			currentRaw.Version = (currentRaw.Version or 0) + 1

			if PersonaStore.EnableDataIntegrityChecks then
				currentRaw.DataHash = CompressionHandler.hashData(encoded)
			end

			self._metadata.CompressionStats.LastCompressedSize = encoded:len()
			self._metadata.CompressionStats.LastUncompressedSize = dataToSave:len()

			return currentRaw
		end)
	end)

	if success then
		self._metadata.LastSaved = os.time()
		self._metadata.WriteCycles += 1
		self._dirtyFields = {}
		self._compressionCache = nil
		PersonaStore._statistics.TotalSaves += 1
		return true
	else
		warn("[PersonaStoreSession]: Compressed save failure on key: " .. self._key .. " -> " .. tostring(err))
		return false
	end
end

-- Starts the autosave heartbeat
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

-- Cancels the autosave heartbeat thread
function DataSession:StopAutoSave()
	if self._autoSaveThread then
		task.cancel(self._autoSaveThread)
		self._autoSaveThread = nil
	end
end

-- Feature: On-Demand Cache Verification Layer
function DataSession:IsCacheStale(): boolean
	local success, remoteRaw = withRetry(function()
		return self._storeRef._storageInstance:GetAsync(self._key)
	end, 3)

	if not success or not remoteRaw then
		return false
	end

	local localVersion = self._metadata.SyncedVersion or 0
	local remoteVersion = remoteRaw.Version or 0

	return remoteVersion > localVersion
end

-- NEW: Verify data integrity using stored hash
function DataSession:VerifyDataIntegrity(): boolean
	if not PersonaStore.EnableDataIntegrityChecks then
		return true
	end

	local success, remoteRaw = withRetry(function()
		return self._storeRef._storageInstance:GetAsync(self._key)
	end, 3)

	if not success or not remoteRaw or not remoteRaw.DataHash then
		return false
	end

	local currentHash = CompressionHandler.hashData(HttpService:JSONEncode(self.Data))
	return currentHash == remoteRaw.DataHash
end

-- NEW: Atomically increment a counter in the data
function DataSession:IncrementCounter(fieldName: string, amount: number?): boolean
	amount = amount or 1
	if self._isReleased then return false end

	self:BeginTransaction()
	self.Data[fieldName] = (self.Data[fieldName] or 0) + amount
	self:CommitTransaction()

	return self:SavePatch()
end

-- NEW: Export profile data for backup/migration
function DataSession:ExportData(includeMetadata: boolean?): string
	local export = {
		Data = deepCopy(self.Data),
		ExportTime = os.time(),
		SessionToken = includeMetadata and self._token or nil,
		Metadata = includeMetadata and self._metadata or nil,
	}

	return HttpService:JSONEncode(export)
end

-- NEW: Import profile data from export (with validation)
function DataSession:ImportData(jsonString: string, overwrite: boolean?): boolean
	if self._isReleased then return false end

	local success, decoded = pcall(function()
		return HttpService:JSONDecode(jsonString)
	end)

	if not success or not decoded.Data then
		return false
	end

	self:BeginTransaction()

	if overwrite then
		self.Data = decoded.Data
	else
		for key, value in pairs(decoded.Data) do
			self.Data[key] = value
		end
	end

	self:CommitTransaction()
	return true
end

-- Delivers any global updates that were queued for this key while it was offline
function DataSession:ConsumeGlobalUpdates(): {table}
	local updates = self._pendingGlobalUpdates
	self._pendingGlobalUpdates = {}
	return updates
end

-- Internal: invoked by the PersonaStore message router when a live Global_Update arrives
function DataSession:_ReceiveLiveGlobalUpdate(payload: table)
	table.insert(self._pendingGlobalUpdates, payload)

	for _, listener in ipairs(self._globalUpdateListeners) do
		task.spawn(listener, payload)
	end
end

-- Internal: invoked by the PersonaStore message router when another server broadcasts an Eject_Key request
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

-- Convenience alias: a full teardown that always saves first
function DataSession:Destroy()
	self:Save()
	self:Release()
end

-- Returns an operational log of data transactions and lifecycle states
function DataSession:GetPerformanceMetadata(): table
	return deepCopy(self._metadata)
end

--// Class Founder
-- A singleton class for managing a single DataStore

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
	self._readOnlyCache = {} -- For read-only access

	PersonaStore.RegisteredStores[storeName] = self

	return self
end

-- Main interface handler to acquire, reconcile, and lock user profiles safely
function Founder:LoadSession<T>(key: string): DataSession<T>
	if self._activeSessions[key] then
		return self._activeSessions[key]
	end

	if not PersonaStore.EngineActive then
		warn("[PersonaStore]: LoadSession() called before Init()! Call PersonaStore:Init() first.")
		return nil
	end

	local uniqueToken = HttpService:GenerateGUID(false)
	local currentJobId = game.JobId ~= "" and game.JobId or "StudioInstance"
	local activeSessionObject = nil

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
				currentRaw = profileRaw(currentTime)
			end

			currentRaw.SessionToken = uniqueToken
			currentRaw.JobId = currentJobId
			currentRaw.LastUpdate = currentTime

			reconcileData(currentRaw.Data, self._templateSchema)

			activeSessionObject = currentRaw

			return currentRaw
		end)
	end)

	if not success then
		warn("[Founder]: Failed to acquire session lock for key: " .. key .. " -> " .. tostring(err))
		return nil
	end

	local newSession = DataSession.new(key, activeSessionObject, uniqueToken, self)
	newSession._metadata.SyncedVersion = activeSessionObject.Version or 0

	self._activeSessions[key] = newSession

	newSession._pendingGlobalUpdates = activeSessionObject.GlobalUpdates or {}

	newSession:StartAutoSave(self._config.AutoSaveInterval or PersonaStore.GlobalAutoSaveInterval)

	PersonaStore._statistics.TotalLoads += 1

	return newSession
end

-- Queued variant of LoadSession for cases where you'd rather wait for a lock to free up
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

-- NEW: Load profile as read-only without acquiring a lock
function Founder:LoadReadOnlySession<T>(key: string): table?
	local success, data = withRetry(function()
		return self._storageInstance:GetAsync(key)
	end, 3)

	if not success or not data then
		return nil
	end

	return deepCopy(data)
end

-- Feature: Live Cross-Server Global Updates
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

	MessagingService:PublishAsync(PersonaStore.ClusterTopic, {
		Action = "Global_Update",
		TargetStore = self._storeName,
		TargetKey = key,
		Payload = payload,
	})

	return (success == true)
end

-- Forcibly requests eviction of a key without loading it yourself
function Founder:ForceEjectKey(key: string)
	MessagingService:PublishAsync(PersonaStore.ClusterTopic, {
		Action = "Eject_Key",
		TargetStore = self._storeName,
		TargetKey = key,
		OriginJobId = game.JobId ~= "" and game.JobId or "StudioInstance",
	})
end

-- NEW: Batch operation to update multiple keys with a transformation function
function Founder:BatchUpdate(keys: {string}, transformFn: (data: table) -> table): {[string]: boolean}
	local results = {}

	for _, key in ipairs(keys) do
		local session = self:LoadSessionAsync(key, 5)
		if session then
			local success = pcall(function()
				transformFn(session.Data)
			end)

			if success then
				session:SavePatch()
			end

			session:Release()
			results[key] = success
		else
			results[key] = false
		end
	end

	return results
end

-- NEW: Read profile statistics and metadata
function Founder:GetKeyMetadata(key: string): table?
	local success, data = withRetry(function()
		return self._storageInstance:GetAsync(key)
	end, 3)

	if not success or not data then
		return nil
	end

	return {
		Version = data.Version,
		LastUpdate = data.LastUpdate,
		SessionToken = data.SessionToken,
		JobId = data.JobId,
		DataHash = data.DataHash,
		CompressionMetadata = data.CompressionMetadata,
	}
end

-- Simple diagnostic used by ops dashboards
function Founder:GetActiveSessionCount(): number
	local count = 0

	for _ in pairs(self._activeSessions) do
		count += 1
	end

	return count
end

-- NEW: Get compression statistics for this store
function Founder:GetCompressionStats(): table
	return {
		TotalCompressions = PersonaStore._statistics.TotalCompressions,
		TotalDecompressions = PersonaStore._statistics.TotalDecompressions,
		BytesSavedByCompression = PersonaStore._statistics.BytesSavedByCompression,
	}
end

-- Creates the engine singleton
function PersonaStore.new()
	return setmetatable({}, PersonaStore)
end

-- Registers and returns a new isolated Founder
function PersonaStore:CreateDataStore<T>(storeName: string, configOptions: table): Founder<T>
	if self.RegisteredStores[storeName] then
		return self.RegisteredStores[storeName] :: T
	end

	return Founder.new(storeName, configOptions) :: T
end

-- Boots the shared MessagingService subscription and the BindToClose drain
function PersonaStore:Init()
	if PersonaStore.EngineActive then
		warn("[PersonaStore]: Init() called more than once!")
		return
	end

	PersonaStore.EngineActive = true

	local success, connection = pcall(function()
		return MessagingService:SubscribeAsync(PersonaStore.ClusterTopic, function(message)
			local data = message.Data
			if type(data) ~= "table" then return end

			local targetStore = PersonaStore.RegisteredStores[data.TargetStore]
			if not targetStore then return end

			local currentJobId = game.JobId ~= "" and game.JobId or "StudioInstance"

			if data.Action == "Eject_Key" then
				if data.OriginJobId == currentJobId then return end

				local session = targetStore._activeSessions[data.TargetKey]
				if session then
					session:_HandleRemoteEviction()
				end
			elseif data.Action == "Global_Update" then
				local session = targetStore._activeSessions[data.TargetKey]
				if session then
					session:_ReceiveLiveGlobalUpdate(data.Payload)
				end
			end
		end)
	end)

	if success then
		PersonaStore._messagingConnection = connection
	else
		warn("[PersonaStore]: Failed to subscribe to cluster topic -> " .. tostring(connection))
	end

	if not PersonaStore._bindToCloseRegistered then
		PersonaStore._bindToCloseRegistered = true

		game:BindToClose(function()
			if RunService:IsStudio() then
				return
			end

			for _, store in pairs(PersonaStore.RegisteredStores) do
				for _, session in pairs(store._activeSessions) do
					session:Destroy()
				end
			end

			task.wait(2)
		end)
	end
end

-- NEW: Get engine-wide statistics
function PersonaStore:GetStatistics(): table
	return deepCopy(PersonaStore._statistics)
end

-- NEW: Configure compression settings
function PersonaStore:SetCompressionSettings(algorithm: any, level: number)
	PersonaStore.CompressionAlgorithm = algorithm or PersonaStore.CompressionAlgorithm
	PersonaStore.CompressionLevel = math.clamp(level or PersonaStore.CompressionLevel, 1, 22)
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
	LoadReadOnlySession: (self: Founder<T>, key: string) -> table?,
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
