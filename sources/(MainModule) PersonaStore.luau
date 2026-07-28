--// PersonaStore.lua \\--

--[[
    ========================================================================================
    PersonaStore - A ProfileStore-inspired DataStore library built
    entirely with an OOP architecture, explicit data isolation, session
    locking, observable state tracking, and cross-server synchronization.

    Now enhanced with EncodingService integration, compression, hashing, advanced APIs,
    OrderedDataStore + MemoryStore wrappers, and DataStore Version APIs.
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
        5. OrderedFounder (OrderedDataStore Wrapper):
           A thin, retry-wrapped interface over GetOrderedDataStore, intended for
           leaderboards and other strictly-numeric sorted keys.
        6. MemoryQueueWrapper / MemorySortedMapWrapper (MemoryStoreService Wrappers):
           Retry-wrapped interfaces over MemoryStoreService queues and sorted maps for
           high-throughput, short-lived, in-memory-tier data.

    Advanced Features:
        - Multi-Level Initializer Pipeline via .new() and :CreateDataStore() factories.
        - Atomic Memory Transactions allowing multi-step writes with rollbacks.
        - High-Fidelity Deep Observable Proxy Tree to track nested mutation events.
        - Distributed Messaging Cluster Network to clear stale session locks instantly.
        - High-Efficiency Selective Data Patching to minimize network overhead.
        - Configurable Integrity Modes (Full / HashOnlyOnFullSave / PerField) so patch
          saves on large profiles don't have to rehash the entire document every time.
        - On-Demand Cache Verification Layer to prevent stale reads.
        - Live Cross-Server Global Updates (online push + offline queue).
        - Resilient Retry Layer with exponential backoff + jitter.
        - Engine-wide BindToClose drain with automatic session cleanup.
        - Native EncodingService compression (Zstd) with configurable levels,
          with the algorithm used recorded per-profile so decompression is always
          correct even after global settings change.
        - Data integrity verification using cryptographic hashing (Sha256).
        - Read-only profile access without acquiring session locks.
        - Atomic counter operations for leaderboards and currencies.
        - Batch profile operations for mass updates and migrations.
        - OrderedDataStore wrapper for numeric leaderboards.
        - MemoryStoreService Queue + SortedMap wrappers.
        - DataStore Version APIs: ListVersionsAsync / GetVersionAsync / RemoveVersionAsync.
        - Comprehensive statistics and audit logging.
        - Data export/import for backups and migrations.
        - Compression caching to avoid recompressing unchanged data.

    Built with time, effort, love, and passion for the Roblox Developer Community <3.

    Github(repository):
    https://github.com/draxxordev/PersonaStore

    v.1.2.1 [7/27/2026]
    By @Draxxor
]]--

--// Types
export type table =
	{ [string]: any } | { [number]: any } | { table }

--// Services
local DataStoreService = game:GetService("DataStoreService")
local MessagingService = game:GetService("MessagingService")
local MemoryStoreService = game:GetService("MemoryStoreService")
local HttpService = game:GetService("HttpService")
local RunService = game:GetService("RunService")
local Players = game:GetService("Players")
local EncodingService = game:GetService("EncodingService")

--// Interchangables

-- Global state tracking configuration
local PersonaStore = {
	EngineActive = false,
	RegisteredStores = {}, -- storeName -> Founder instance, used by the message router
	RegisteredOrderedStores = {}, -- storeName -> OrderedFounder instance
	RegisteredMemoryQueues = {}, -- name -> MemoryQueueWrapper instance
	RegisteredMemorySortedMaps = {}, -- name -> MemorySortedMapWrapper instance
	GlobalLockTimeout = 120,
	GlobalAutoSaveInterval = 30,
	ClusterTopic = "PersonaStore_Cluster_Signals",
	CompressionAlgorithm = Enum.CompressionAlgorithm.Zstd,
	CompressionLevel = 6, -- 1-22 depending on algorithm
	EnableDataIntegrityChecks = true,
	HashAlgorithm = Enum.HashAlgorithm.Sha256,
	EnableCompressionCache = true,

	-- IntegrityMode controls how expensive a SavePatch() hash pass is:
	--   "Full"                -> (default/back-compat) rehash the ENTIRE profile every SavePatch.
	--   "HashOnlyOnFullSave"  -> SavePatch never hashes; DataHash only refreshes on a full Save().
	--   "PerField"            -> SavePatch only rehashes the fields that were actually mutated,
	--                            tracked in a FieldHashes table. Cheap even with huge sibling fields
	--                            (e.g. a 50,000-item Inventory) as long as those fields stay clean.
	IntegrityMode = "Full",
}

--// Constants (DO NOT TOUCH UNLESS YOU KNOW WHAT YOU'RE DOING!)
PersonaStore.__index = PersonaStore
PersonaStore._messagingConnection = nil
PersonaStore._bindToCloseRegistered = false
PersonaStore._statistics = {
	TotalSaves = 0,
	TotalLoads = 0,
	DatastoreRequestsAttempted = 0,
	DatastoreRequestsFailed = 0,
}

--// Private Utility Functions (Internal Use Only)

local CompressionHandler = {}

-- Resolve a serialized algorithm string (e.g. tostring(Enum.CompressionAlgorithm.Deflate))
-- back into the actual enum. Needed because profiles are decompressed with whatever
-- algorithm they were ORIGINALLY compressed with, not whatever the engine's current
-- global default happens to be.
local function resolveCompressionAlgorithm(serializedAlgorithm: string?): any?
	if not serializedAlgorithm then
		return nil
	end

	for _, algorithmEnum in ipairs(Enum.CompressionAlgorithm:GetEnumItems()) do
		if tostring(algorithmEnum) == serializedAlgorithm then
			return algorithmEnum
		end
	end

	return nil
end

-- Compress a table by JSON encoding, then using EncodingService:CompressBuffer
function CompressionHandler.compress(data: table): buffer
	local json = HttpService:JSONEncode(data)
	local buf = buffer.fromstring(json)

	return EncodingService:CompressBuffer(
		buf,
		PersonaStore.CompressionAlgorithm,
		PersonaStore.CompressionLevel
	)
end

-- Decompress a buffer back to a table. `algorithm` should be the SAME algorithm the
-- buffer was originally compressed with (BUG FIX: this used to silently default to
-- whatever PersonaStore.CompressionAlgorithm currently is, which breaks the moment
-- SetCompressionSettings() is called and old profiles are read back).
function CompressionHandler.decompress(compressed: buffer, algorithm: Enum.CompressionAlgorithm)
	local decompressed = EncodingService:DecompressBuffer(compressed, algorithm)
	return HttpService:JSONDecode(buffer.tostring(decompressed))
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

-- Compute hash of data for integrity verification (BEFORE compression for consistency).
-- BUG FIX: EncodingService:ComputeStringHash returns a raw hash digest, which is
-- effectively random bytes -- NOT guaranteed to be valid UTF-8. DataHash and
-- FieldHashes get written directly into the DataStore record, and Roblox
-- DataStores reject any string that isn't valid UTF-8 with
-- "CantStoreValue: Cannot store <...> in data store." on EVERY save that computes
-- a hash. Base64-encoding the digest (the same trick already used for compressed
-- buffer data below) guarantees the result is DataStore-safe ASCII.
function CompressionHandler.hashData(data: string): string
	local rawHash = EncodingService:ComputeStringHash(data, PersonaStore.HashAlgorithm)
	return CompressionHandler.encodeToBase64(buffer.fromstring(rawHash))
end

-- Compute hash of buffer. NOTE: returns a raw buffer, not a storage-safe string --
-- if you ever persist this directly, base64-encode it first the same way
-- hashData() does (see CompressionHandler.encodeToBase64).
function CompressionHandler.hashBuffer(buf: buffer): buffer
	return EncodingService:ComputeBufferHash(buf, PersonaStore.HashAlgorithm)
end

-- Given a raw stored profile record, decompress its Data in-place if needed, resolving
-- the algorithm it was actually compressed with rather than assuming the current global.
local function decompressRawIfNeeded(currentRaw)
	if currentRaw.CompressionMetadata
		and currentRaw.CompressionMetadata.IsCompressed
		and currentRaw.CompressedData then

		local decodedBuffer =
			CompressionHandler.decodeFromBase64(currentRaw.CompressedData)

		local algorithm =
			resolveCompressionAlgorithm(
				currentRaw.CompressionMetadata.Algorithm
			)
			or PersonaStore.CompressionAlgorithm

		currentRaw.Data =
			CompressionHandler.decompress(
				decodedBuffer,
				algorithm
			)
	end
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

	local mt = {}

	function mt.__index(_, key)
		return internalShadow[key]
	end

	function mt.__newindex(_, key, value)
		if type(value) == "table" then
			internalShadow[key] = createObservableTable(value, changeCallback, rootKey or key)
		else
			internalShadow[key] = value
		end

		changeCallback(key, value, rootKey or key)
	end

	function mt.__iter()
		return next, internalShadow
	end

	setmetatable(proxy, mt)

	return proxy
end

local function profileRaw(elapsed: number): table
	return {
		Data = {},
		SessionToken = nil,
		JobId = nil,
		LastUpdate = elapsed,
		Revision = 0,
		GlobalUpdates = {},
		DataHash = nil, -- Hash computed BEFORE compression for consistency (Full / HashOnlyOnFullSave modes)
		FieldHashes = {}, -- Per-field hashes (PerField mode)
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

		if attemptIndex < attempts then
			task.wait(
				math.min(2 ^ attemptIndex, 16) + (math.random() * 0.5)
			)
		end
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
	self._compressionCache = nil -- Cached compressed data (per-session, not global)

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
		print("UpdateAsync attempt", os.clock())

		self._storeRef._storageInstance:UpdateAsync(self._key, function(currentRaw)
			print("Inside UpdateAsync callback")

			if not currentRaw or currentRaw.SessionToken ~= self._token then
				error("Ownership mismatch detected; session token has lapsed.")
			end

			currentRaw.Data = deepCopy(self.Data)
			currentRaw.LastUpdate = os.time()
			currentRaw.Revision = (currentRaw.Revision or 0) + 1

			-- Update metadata after a successful save
			-- We actually DONT wanna increment this on patch save
			-- Suppose another server somehow advanced it to 250,
			-- +1 isn't necessarily correct.
			self._metadata.SyncedRevision = currentRaw.Revision

			-- Full saves always recompute BOTH the whole-profile hash and the per-field
			-- hash table, regardless of IntegrityMode, so PerField mode always has a
			-- consistent baseline to diff patches against.
			if PersonaStore.EnableDataIntegrityChecks then
				currentRaw.DataHash = CompressionHandler.hashData(HttpService:JSONEncode(currentRaw.Data))

				local hashes = {}

				for fieldName, fieldValue in pairs(currentRaw.Data) do
					hashes[fieldName] =
						CompressionHandler.hashData(HttpService:JSONEncode(fieldValue))
				end

				currentRaw.FieldHashes = hashes
			end

			-- Clear compression metadata when doing uncompressed save
			currentRaw.CompressionMetadata = {
				IsCompressed = false,
				Algorithm = nil,
				OriginalSize = nil,
				CompressedSize = nil,
			}

			-- Also clear compressed data when doing uncompressed save
			currentRaw.CompressedData = nil

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

-- High-Efficiency Selective Data Patching
-- Hashing cost now scales with IntegrityMode instead of always rehashing the whole profile:
--   "Full"               -> rehashes everything on every patch (old behavior, expensive).
--   "HashOnlyOnFullSave"  -> skips hashing entirely; DataHash only advances on Save().
--   "PerField"            -> only rehashes the fields that were actually dirtied.
function DataSession:SavePatch(): boolean
	if self._isReleased then return false end

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
			currentRaw.Revision = (currentRaw.Revision or 0) + 1

			if PersonaStore.EnableDataIntegrityChecks then
				local mode = PersonaStore.IntegrityMode

				if mode == "PerField" then
					currentRaw.FieldHashes = currentRaw.FieldHashes or {}
					for _, fieldName in ipairs(dirtyKeys) do
						local fieldJson = HttpService:JSONEncode(exportedSnapshot[fieldName])
						currentRaw.FieldHashes[fieldName] = CompressionHandler.hashData(fieldJson)
					end
					-- DataHash intentionally left stale here; PerField verification
					-- reads FieldHashes instead of the whole-profile hash.
				elseif mode == "HashOnlyOnFullSave" then
					-- Deliberately skip all hashing on patch saves. DataHash/FieldHashes
					-- only get refreshed the next time Save() runs a full write.
				else -- "Full" (default/back-compat)
					currentRaw.DataHash = CompressionHandler.hashData(HttpService:JSONEncode(currentRaw.Data))
				end
			end

			-- Clear compression metadata on patch save
			currentRaw.CompressionMetadata = {
				IsCompressed = false,
				Algorithm = nil,
				OriginalSize = nil,
				CompressedSize = nil,
			}

			-- Also clear compression metadata on patch save
			currentRaw.CompressedData = nil

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

-- Save with compression for large profiles
-- Now actually stores compressed data and uses compression cache
function DataSession:SaveCompressed(): boolean
	if self._isReleased then return false end

	local success, err = withRetry(function()
		self._storeRef._storageInstance:UpdateAsync(self._key, function(currentRaw)
			if not currentRaw or currentRaw.SessionToken ~= self._token then
				error("Ownership mismatch detected; session token has lapsed.")
			end

			local dataToSave = deepCopy(self.Data)
			local jsonData = HttpService:JSONEncode(dataToSave)
			local originalSize = #jsonData

			-- Compute hash BEFORE compression for consistency
			local dataHash = nil
			if PersonaStore.EnableDataIntegrityChecks then
				dataHash = CompressionHandler.hashData(jsonData)
			end

			-- Use cached compression if available
			local compressed
			if self._compressionCache then
				compressed = self._compressionCache
			else
				compressed = CompressionHandler.compress(dataToSave)
				if PersonaStore.EnableCompressionCache then
					self._compressionCache = compressed
				end
			end

			local encoded = CompressionHandler.encodeToBase64(compressed)
			local compressedSize = #encoded

			-- Store metadata and compressed data
			currentRaw.Data = nil  -- Clear uncompressed data
			currentRaw.CompressedData = encoded  -- Store compressed data
			currentRaw.CompressionMetadata = {
				IsCompressed = true,
				OriginalSize = originalSize,
				CompressedSize = compressedSize,
				Algorithm = tostring(PersonaStore.CompressionAlgorithm)
			}
			currentRaw.LastUpdate = os.time()
			currentRaw.Revision = (currentRaw.Revision or 0) + 1

			-- Store hash computed BEFORE compression
			if PersonaStore.EnableDataIntegrityChecks then
				currentRaw.DataHash = dataHash
			end

			-- Properly compute uncompressed size using JSON
			self._metadata.CompressionStats.LastCompressedSize = compressedSize
			self._metadata.CompressionStats.LastUncompressedSize = originalSize

			return currentRaw
		end)
	end)

	if success then
		self._metadata.LastSaved = os.time()
		self._metadata.WriteCycles += 1
		self._dirtyFields = {}
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

			-- To prevent firing every interval if nothing has changed
			if next(self._dirtyFields) then
				self:SavePatch()
			end
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

	local localVersion = self._metadata.SyncedRevision or 0
	local remoteVersion = remoteRaw.Revision or 0

	return remoteVersion > localVersion
end

-- Verify data integrity using stored hash(es).
-- BUG FIX: this used to hash the live observable proxy (self.Data) directly with
-- HttpService:JSONEncode, which doesn't respect the proxy's __iter metamethod and
-- therefore never matched the DataHash computed from deepCopy(self.Data) at save time.
-- It now deep-copies first, exactly like every Save path does, and is IntegrityMode-aware.
function DataSession:VerifyDataIntegrity(): boolean
	if not PersonaStore.EnableDataIntegrityChecks then
		return true
	end

	local success, remoteRaw = withRetry(function()
		return self._storeRef._storageInstance:GetAsync(self._key)
	end, 3)

	if not success or not remoteRaw then
		return false
	end

	local snapshot = deepCopy(self.Data)

	if PersonaStore.IntegrityMode == "PerField" then
		if not remoteRaw.FieldHashes then
			return false
		end

		for fieldName, fieldValue in pairs(snapshot) do
			local expectedHash = remoteRaw.FieldHashes[fieldName]
			if expectedHash then
				local actualHash = CompressionHandler.hashData(HttpService:JSONEncode(fieldValue))
				if actualHash ~= expectedHash then
					return false
				end
			end
		end

		return true
	end

	-- "Full" and "HashOnlyOnFullSave" both rely on the whole-profile DataHash; the
	-- difference between those two modes is only WHEN that hash gets refreshed.
	if not remoteRaw.DataHash then
		return false
	end

	local currentHash = CompressionHandler.hashData(HttpService:JSONEncode(snapshot))
	return currentHash == remoteRaw.DataHash
end

-- Atomically increment a counter with proper transaction handling
function DataSession:IncrementCounter(fieldName: string, amount: number?): boolean
	amount = amount or 1
	if self._isReleased then return false end

	self:BeginTransaction()
	self.Data[fieldName] = (self.Data[fieldName] or 0) + amount

	-- Save first, then commit. If save fails, rollback.
	local saveSuccess = self:SavePatch()

	if saveSuccess then
		self:CommitTransaction()
		return true
	else
		self:RollbackTransaction()
		return false
	end
end

-- Export profile data for backup/migration
function DataSession:ExportData(includeMetadata: boolean?): string
	local export = {
		Data = deepCopy(self.Data),
		ExportTime = os.time(),
		SessionToken = includeMetadata and self._token or nil,
		Metadata = includeMetadata and self._metadata or nil,
	}

	return HttpService:JSONEncode(export)
end

-- Import profile data from export (preserves observable proxy)
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
		-- Merge values instead of replacing proxy
		for key in pairs(self.Data) do
			self.Data[key] = nil
		end
		for key, value in pairs(decoded.Data) do
			self.Data[key] = value
		end

		reconcileData(self.Data, self._storeRef._templateSchema)
	else
		-- Merge mode: merge without replacing proxy
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

	--// NOTE
	--// You would usually want to make this asynchronous by wrapping
	-- it in a task.spawn() call, but for simplicity we'll keep it
	-- synchronous here.
	withRetry(function()
		self._storeRef._storageInstance:UpdateAsync(self._key, function(currentRaw)
			if not currentRaw or currentRaw.SessionToken ~= self._token then return nil end

			currentRaw.SessionToken = nil
			currentRaw.JobId = nil
			currentRaw.LastUpdate = os.time()

			return currentRaw
		end)
	end, 3)
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

	PersonaStore.RegisteredStores[storeName] = self

	return self
end

-- Main interface handler now supports decompression on load
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

	-- This publish is a best-effort hint so other servers release the lock a
	-- little sooner; UpdateAsync's own lock-timeout check below is the real
	-- correctness guarantee, so on failure (e.g. MessagingService throttling,
	-- which is easy to hit in Studio) we don't want to retry-storm over it.
	withRetry(function()
		MessagingService:PublishAsync(PersonaStore.ClusterTopic, {
			Action = "Eject_Key",
			TargetStore = self._storeName,
			TargetKey = key,
			OriginJobId = currentJobId,
		})
	end, 1)

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

			-- Decompress data if it was stored compressed (uses the algorithm it was
			-- actually compressed with, resolved from CompressionMetadata.Algorithm).
			decompressRawIfNeeded(currentRaw)

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
	newSession._metadata.SyncedRevision = activeSessionObject.Revision or 0

	self._activeSessions[key] = newSession

	newSession._pendingGlobalUpdates = activeSessionObject.GlobalUpdates or {}

	newSession:StartAutoSave(self._config.AutoSaveInterval or PersonaStore.GlobalAutoSaveInterval)

	PersonaStore._statistics.TotalLoads += 1

	return newSession
end

-- Queued variant of LoadSession for cases where you'd rather wait for a lock to free up
function Founder:LoadSessionAsync<T>(key: string, maxWaitSeconds: number?): DataSession<T>
	local deadline = os.clock() + (maxWaitSeconds or 10)

	while os.clock() < deadline do
		local session = self:LoadSession(key) :: DataSession<T>
		if session then
			return session
		end

		task.wait(1)
	end

	return nil
end

-- Load profile as read-only without acquiring a lock (returns deep copy)
function Founder:LoadReadOnlySnapshot<T>(key: string): table?
	local success, data = withRetry(function()
		return self._storageInstance:GetAsync(key)
	end, 3)

	if not success or not data then
		return nil
	end

	-- Decompress if needed before returning snapshot
	decompressRawIfNeeded(data)

	return deepCopy(data)
end

-- Alias for backwards compatibility
function Founder:LoadReadOnlySession<T>(key: string): table?
	return self:LoadReadOnlySnapshot(key)
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

	-- We wrap it in a real function, and keep it a single best-effort attempt.
	withRetry(function()
		MessagingService:PublishAsync(PersonaStore.ClusterTopic, {
			Action = "Global_Update",
			TargetStore = self._storeName,
			TargetKey = key,
			Payload = payload,
		})
	end, 1)

	return (success == true)
end

-- Forcibly requests eviction of a key without loading it yourself
function Founder:ForceEjectKey(key: string)
	-- Same bug as LoadSession's Eject_Key publish -- see the note there.
	withRetry(function()
		MessagingService:PublishAsync(PersonaStore.ClusterTopic, {
			Action = "Eject_Key",
			TargetStore = self._storeName,
			TargetKey = key,
			OriginJobId = game.JobId ~= "" and game.JobId or "StudioInstance",
		})
	end, 1)
end

-- Batch operation with improved cleanup
-- BUG FIX: this previously called session:SavePatch() and then session:Destroy()
-- (which itself calls Save(), a full write) back to back -- every key in the batch
-- was written to the DataStore twice. Destroy() already saves; the extra SavePatch()
-- is removed.
function Founder:BatchUpdate(keys: {string}, transformFn: (data: table) -> ()): {[string]: boolean}
	local results = {}

	for _, key in ipairs(keys) do
		local session = self:LoadSessionAsync(key, 5)
		if session then
			local transformSuccess = pcall(function()
				transformFn(session.Data)
			end)

			-- Destroy() performs a full Save() (which captures all fields, transformed
			-- or not) and then releases the lock. Do not also SavePatch() here.
			session:Destroy()
			results[key] = transformSuccess
		else
			results[key] = false
		end
	end

	return results
end

-- Read profile statistics and metadata
function Founder:GetKeyMetadata(key: string): table?
	local success, data = withRetry(function()
		return self._storageInstance:GetAsync(key)
	end, 3)

	if not success or not data then
		return nil
	end

	return {
		Revision = data.Revision,
		LastUpdate = data.LastUpdate,
		SessionToken = data.SessionToken,
		JobId = data.JobId,

		Integrity = {
			DataHash = data.DataHash,
			FieldHashes = data.FieldHashes,
		},

		Compression = data.CompressionMetadata,
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

--// DataStore Version APIs
-- Thin, retry-wrapped interfaces over the native DataStore versioning API.

-- Lists historical versions of a key. Returns the native DataStoreVersionPages object
-- (call :GetCurrentPage() / :AdvanceToNextPageAsync() on it as usual), or nil on failure.
function Founder:ListVersionsAsync(key: string, sortDirection: Enum.SortDirection?, minDate: number?, maxDate: number?, pageSize: number?)
	local success, pages = withRetry(function()
		return self._storageInstance:ListVersionsAsync(
			key,
			sortDirection or Enum.SortDirection.Descending,
			minDate,
			maxDate,
			pageSize or 50
		)
	end, 3)

	if not success then
		warn("[Founder]: ListVersionsAsync failed for key: " .. key .. " -> " .. tostring(pages))
		return nil
	end

	return pages
end

-- Fetches a specific historical version of a key's raw record, decompressing its
-- Data automatically if that version happened to be stored compressed.
function Founder:GetVersionAsync(key: string, version: string): table?
	local success, data = withRetry(function()
		return self._storageInstance:GetVersionAsync(key, version)
	end, 3)

	if not success or not data then
		warn("[Founder]: GetVersionAsync failed for key: " .. key .. " version: " .. tostring(version) .. " -> " .. tostring(data))
		return nil
	end

	decompressRawIfNeeded(data)

	return deepCopy(data)
end

-- Permanently removes a specific historical version of a key. This cannot be undone.
function Founder:RemoveVersionAsync(key: string, version: string): boolean
	local success, err = withRetry(function()
		self._storageInstance:RemoveVersionAsync(key, version)
		return true
	end, 3)

	if not success then
		warn("[Founder]: RemoveVersionAsync failed for key: " .. key .. " version: " .. tostring(version) .. " -> " .. tostring(err))
	end

	return success == true
end

--// Class OrderedFounder
-- A thin, retry-wrapped interface over GetOrderedDataStore, intended for leaderboards
-- and other strictly-numeric sorted keys. Kept separate from Founder since
-- OrderedDataStores only accept non-negative integer values, and have no concept of
-- session locks, structured schemas, or compression.

local OrderedFounder = {}
OrderedFounder.__index = OrderedFounder

function OrderedFounder.new<T>(storeName: string): OrderedFounder<T>
	local self = setmetatable({}, OrderedFounder)

	self._storeName = storeName
	self._storageInstance = DataStoreService:GetOrderedDataStore(storeName)

	PersonaStore.RegisteredOrderedStores[storeName] = self

	return self
end

-- Sets a key to an explicit non-negative integer value
function OrderedFounder:Set(key: string, value: number): boolean
	if type(value) ~= "number" or value < 0 or value % 1 ~= 0 then
		warn("[OrderedFounder]: value must be a non-negative integer for key: " .. key)
		return false
	end

	local success = withRetry(function()
		self._storageInstance:SetAsync(key, value)
		return true
	end)

	return success == true
end

-- Reads the current value for a key, or nil if it doesn't exist
function OrderedFounder:Get(key: string): number?
	local success, value = withRetry(function()
		return self._storageInstance:GetAsync(key)
	end, 3)

	if not success then
		return nil
	end

	return value
end

-- Atomically increments (or decrements, with a negative delta) a key's value
function OrderedFounder:Increment(key: string, delta: number?): number?
	local success, result = withRetry(function()
		return self._storageInstance:IncrementAsync(key, delta or 1)
	end)

	if not success then
		return nil
	end

	return result
end

-- Removes a key entirely
function OrderedFounder:Remove(key: string): boolean
	local success = withRetry(function()
		self._storageInstance:RemoveAsync(key)
		return true
	end, 3)

	return success == true
end

-- Fetches one sorted page of entries. Returns the current page ({key, value} entries)
-- plus the raw DataStorePages object (for pagination via :AdvanceToNextPageAsync()).
function OrderedFounder:GetSortedPage(ascending: boolean?, pageSize: number?, minValue: number?, maxValue: number?): DataStorePages?
	if pageSize and (pageSize < 1 or pageSize > 100) then
		warn("[OrderedFounder]: pageSize must be between 1 and 100 for store: " .. self._storeName)
		return nil, nil
	end

	local success, pages = withRetry(function()
		return self._storageInstance:GetSortedAsync(ascending or false, pageSize or 50, minValue, maxValue)
	end, 3)

	if not success then
		warn("[OrderedFounder]: GetSortedAsync failed for store: " .. self._storeName .. " -> " .. tostring(pages))
		return nil, nil
	end

	local currentPage = pages:GetCurrentPage()
	return currentPage, pages
end

--// Class MemoryQueueWrapper
-- Retry-wrapped interface over a MemoryStoreService queue. Intended for short-lived,
-- high-throughput, ephemeral job/event data -- NOT for anything that needs the
-- durability guarantees of a DataStore.

local MemoryQueueWrapper = {}
MemoryQueueWrapper.__index = MemoryQueueWrapper

function MemoryQueueWrapper.new(name: string): MemoryQueueWrapper
	local self = setmetatable({}, MemoryQueueWrapper)

	self._name = name
	self._queue = MemoryStoreService:GetQueue(name)

	PersonaStore.RegisteredMemoryQueues[name] = self

	return self
end

-- Adds an item to the queue. expirationSeconds defaults to 1 hour.
function MemoryQueueWrapper:AddItem(value: any, expirationSeconds: number?, priority: number?): boolean
	local success = withRetry(function()
		self._queue:AddAsync(value, expirationSeconds or 3600, priority)
		return true
	end, 3)

	return success == true
end

-- Reads up to `count` items. Returns (items, receiptId) so the caller can later
-- RemoveItem(receiptId) once the items have been successfully processed.
function MemoryQueueWrapper:ReadItems(count: number?, allOrNothing: boolean?, waitTimeoutSeconds: number?): ({any}?, string?)
	local success, result = withRetry(function()
		local items, receiptId = self._queue:ReadAsync(count or 1, allOrNothing or false, waitTimeoutSeconds or 5)
		return { Items = items, ReceiptId = receiptId }
	end, 3)

	if not success or not result then
		return nil, nil
	end

	return result.Items, result.ReceiptId
end

-- Removes a previously-read batch of items using the receiptId from ReadItems
function MemoryQueueWrapper:RemoveItems(receiptId: string): boolean
	local success = withRetry(function()
		self._queue:RemoveAsync(receiptId)
		return true
	end, 3)

	return success == true
end

--// Class MemorySortedMapWrapper
-- Retry-wrapped interface over a MemoryStoreService sorted map. Useful for
-- fast-changing leaderboard-style data that doesn't need DataStore durability, or as
-- a scratch layer in front of a real DataStore write.

local MemorySortedMapWrapper = {}
MemorySortedMapWrapper.__index = MemorySortedMapWrapper

function MemorySortedMapWrapper.new(name: string): MemorySortedMapWrapper
	local self = setmetatable({}, MemorySortedMapWrapper)

	self._name = name
	self._map = MemoryStoreService:GetSortedMap(name)

	PersonaStore.RegisteredMemorySortedMaps[name] = self

	return self
end

-- Sets a key's value. expirationSeconds defaults to 1 hour.
function MemorySortedMapWrapper:Set(key: string, value: any, expirationSeconds: number?, sortKey: any?): boolean
	local success = withRetry(function()
		self._map:SetAsync(key, value, expirationSeconds or 3600, sortKey)
		return true
	end, 3)

	return success == true
end

-- Gets a key's value, or nil if it doesn't exist / has expired
function MemorySortedMapWrapper:Get(key: string): any?
	local success, value = withRetry(function()
		return self._map:GetAsync(key)
	end, 3)

	if not success then
		return nil
	end

	return value
end

-- Removes a key
function MemorySortedMapWrapper:Remove(key: string): boolean
	local success = withRetry(function()
		self._map:RemoveAsync(key)
		return true
	end, 3)

	return success == true
end

-- Returns a sorted range of {key, value, sortKey} entries
function MemorySortedMapWrapper:GetRange(direction: Enum.SortDirection?, count: number?, exclusiveLowerBound: any?, exclusiveUpperBound: any?): {table}
	local success, items = withRetry(function()
		return self._map:GetRangeAsync(direction or Enum.SortDirection.Ascending, count or 50, exclusiveLowerBound, exclusiveUpperBound)
	end, 3)

	if not success then
		return {}
	end

	return items
end

-- Atomically transforms a key's value via `transformFn(oldValue) -> newValue`
function MemorySortedMapWrapper:Update(key: string, transformFn: (oldValue: any) -> any, expirationSeconds: number?): any?
	local success, result = withRetry(function()
		return self._map:UpdateAsync(key, transformFn, expirationSeconds or 3600)
	end, 3)

	if not success then
		return nil
	end

	return result
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

-- Registers and returns a new isolated OrderedFounder
function PersonaStore:CreateOrderedDataStore<T>(storeName: string): OrderedFounder<T>
	if self.RegisteredOrderedStores[storeName] then
		return self.RegisteredOrderedStores[storeName] :: T
	end

	return OrderedFounder.new(storeName) :: T
end

-- Registers and returns a new MemoryQueueWrapper
function PersonaStore:CreateMemoryQueue(name: string): MemoryQueueWrapper
	if self.RegisteredMemoryQueues[name] then
		return self.RegisteredMemoryQueues[name]
	end

	return MemoryQueueWrapper.new(name)
end

-- Registers and returns a new MemorySortedMapWrapper
function PersonaStore:CreateMemorySortedMap(name: string): MemorySortedMapWrapper
	if self.RegisteredMemorySortedMaps[name] then
		return self.RegisteredMemorySortedMaps[name]
	end

	return MemorySortedMapWrapper.new(name)
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

	print("[PersonaStore]: Engine started successfully!")
end

-- Get engine-wide statistics
function PersonaStore:GetStatistics(): table
	return deepCopy(PersonaStore._statistics)
end

-- Configure compression settings
function PersonaStore:SetCompressionSettings(algorithm: any, level: number)
	PersonaStore.CompressionAlgorithm = algorithm or PersonaStore.CompressionAlgorithm
	PersonaStore.CompressionLevel = math.clamp(level or PersonaStore.CompressionLevel, 1, 22)
end

-- Configure how expensive integrity hashing is on SavePatch(). See the IntegrityMode
-- comment near the top of this file for the tradeoffs of each mode.
function PersonaStore:SetIntegrityMode(mode: string)
	if mode ~= "Full" and mode ~= "HashOnlyOnFullSave" and mode ~= "PerField" then
		warn("[PersonaStore]: Unknown IntegrityMode '" .. tostring(mode) .. "'; expected 'Full', 'HashOnlyOnFullSave', or 'PerField'. Ignoring.")
		return
	end

	PersonaStore.IntegrityMode = mode
end

-- Client require guard.
-- BUG FIX: this used to be `withRetry(task.spawn(function() ... end))`, which passes a
-- THREAD (the return value of task.spawn) into withRetry. withRetry then does
-- `pcall(attemptFn)` on that thread, which is not callable, so pcall fails immediately
-- on every one of its 5 attempts (burning through exponential backoff) before finally
-- surfacing the WRONG error entirely. Fixed to call withRetry with an actual function,
-- and to use maxAttempts = 1 since "are we on the server" is not a transient condition
-- worth retrying/backing off on.
local guardSuccess, guardErr = withRetry(function()
	if not RunService:IsServer() then
		error("PersonaStore can only be required from the server")
	end
	return true
end, 1)

if not guardSuccess then
	error("[PersonaStore]: " .. tostring(guardErr))
end

--// Final Exports for autocomplete
export type MemoryQueueWrapper = typeof(MemoryQueueWrapper)
export type MemorySortedMapWrapper = typeof(MemorySortedMapWrapper)

export type Founder<T> = {
	_storeName: string,
	_config: table,
	_storageInstance: DataStore,
	_activeSessions: {[string]: DataSession<T>},
	_templateSchema: T,

	LoadSession: (self: Founder<T>, key: string) -> DataSession<T>?,
	LoadSessionAsync: (self: Founder<T>, key: string, maxWaitSeconds: number?) -> DataSession<T>?,
	LoadReadOnlySnapshot: (self: Founder<T>, key: string) -> table?,
	LoadReadOnlySession: (self: Founder<T>, key: string) -> table?,
	ForceEjectKey: (self: Founder<T>, key: string) -> (),
}

export type OrderedFounder<T> = typeof(OrderedFounder) & {
	Set: (self: OrderedFounder<T>, key: string, value: number) -> boolean,
	Get: (self: OrderedFounder<T>, key: string) -> number?,
	Increment: (self: OrderedFounder<T>, key: string, delta: number?) -> number?,
	Remove: (self: OrderedFounder<T>, key: string) -> boolean,
}

export type Session<T> = {
	Data: T,
	SessionToken: string?,
	JobId: string?,
	LastUpdate: number,
	Revision: number,
	GlobalUpdates: table,
}

export type DataSession<T> = typeof(DataSession) & Session<T>

return PersonaStore
