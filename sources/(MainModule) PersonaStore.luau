--// PersonaStore.lua \\--

--[[
    ========================================================================================
    PersonaStore - A ProfileStore-inspired DataStore library built
    entirely with an OOP architecture, explicit data isolation, session
    locking, observable state tracking, and cross-server synchronization.

    Now enhanced with EncodingService integration, compression, hashing, advanced APIs,
    OrderedDataStore + MemoryStore wrappers, DataStore Version APIs, and a built-in
    serialization engine for complex/custom types.
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
        7. BufferArray (Typed Array Utility):
           A thin wrapper over Luau `buffer` for compact, typed numeric arrays (u8/u16/
           u32/f32/f64/etc.), with base64 encode/decode helpers for DataStore storage.

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
        - Global serializer registry (Vector3 / Vector2 / CFrame / Color3 / UDim / UDim2
          built in, custom types via RegisterSerializer) with automatic (de)serialization
          on Save() / SavePatch() / SaveCompressed() / LoadSession(), driven by a
          per-store `SerializationManifest` (with per-session overrides available too).
        - BufferArray utility for compact typed numeric arrays.
        - Smart Rate Limiter: budget-aware priority queue over DataStore calls
          (Critical lock/release > High Save > Normal reads > Low auto-patches),
          live GetRequestBudgetForRequestType gating, per-key patch coalescing,
          and configurable concurrency / reserve / queue wait.

    Built with time, effort, love, and passion for the Roblox Developer Community <3.

    Github(repository):
    https://github.com/draxxordev/PersonaStore

    v.1.4.0 [7/28/2026]
    By @Draxxor
]]--

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

	-- Rate limiting (smart request budget management)
	-- Uses DataStoreService:GetRequestBudgetForRequestType when available,
	-- plus an internal priority queue so critical ops (lock acquire, release,
	-- full Save on leave/BindToClose) always run ahead of background patches.
	EnableRateLimiter = true,
	-- Keep this many budget tokens in reserve so a sudden player join/leave
	-- wave never starves. Background (Low) requests wait when budget is at
	-- or below this floor.
	RateLimiterBudgetReserve = 5,
	-- Max simultaneous in-flight DataStore calls from this server.
	RateLimiterMaxConcurrent = 8,
	-- Minimum seconds between SavePatch / auto-save attempts on the *same*
	-- session key (coalesces rapid dirty writes). Full Save / Release ignore this.
	RateLimiterMinPatchInterval = 8,
	-- Soft cap on how long a Low-priority request will wait in the queue
	-- before giving up (seconds). High/Critical never give up for budget alone.
	RateLimiterMaxQueueWait = 30,
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

--// Global Serializer Registry
-- Maps a type name (e.g. "Vector3", or any custom name you choose) to a table of
-- {Serialize = function(value) -> storageSafeData, Deserialize = function(data) -> value}.
-- Populated with built-ins below; extend with PersonaStore:RegisterSerializer().
PersonaStore._SerializerRegistry = {
	Vector3 = {
		Serialize = function(v)
			if not v or not v.X then return nil end
			return {X = v.X, Y = v.Y, Z = v.Z}
		end,
		Deserialize = function(data)
			if not data or not data.X then return nil end
			return Vector3.new(data.X, data.Y, data.Z)
		end,
	},

	Vector2 = {
		Serialize = function(v)
			if not v or not v.X then return nil end
			return {X = v.X, Y = v.Y}
		end,
		Deserialize = function(data)
			if not data or not data.X then return nil end
			return Vector2.new(data.X, data.Y)
		end,
	},

	CFrame = {
		Serialize = function(cf)
			if not cf then return nil end
			return {c = {cf:GetComponents()}}
		end,
		Deserialize = function(data)
			if not data or not data.c then return nil end
			return CFrame.fromMatrix(
				Vector3.new(data.c[1], data.c[2], data.c[3]),
				Vector3.new(data.c[4], data.c[5], data.c[6]),
				Vector3.new(data.c[7], data.c[8], data.c[9]),
				Vector3.new(data.c[10], data.c[11], data.c[12])
			)
		end,
	},

	Color3 = {
		Serialize = function(c)
			if not c or not c.R then return nil end
			return {R = c.R, G = c.G, B = c.B}
		end,
		Deserialize = function(data)
			if not data or not data.R then return nil end
			return Color3.new(data.R, data.G, data.B)
		end,
	},

	UDim = {
		Serialize = function(u)
			if not u or not u.Scale then return nil end
			return {Scale = u.Scale, Offset = u.Offset}
		end,
		Deserialize = function(data)
			if not data then return nil end
			return UDim.new(data.Scale or 0, data.Offset or 0)
		end,
	},

	UDim2 = {
		Serialize = function(ud)
			if not ud or not ud.X then return nil end
			return {
				X = {Scale = ud.X.Scale, Offset = ud.X.Offset},
				Y = {Scale = ud.Y.Scale, Offset = ud.Y.Offset},
			}
		end,
		Deserialize = function(data)
			if not data or not data.X then return nil end
			return UDim2.new(data.X.Scale or 0, data.X.Offset or 0, data.Y.Scale or 0, data.Y.Offset or 0)
		end,
	},
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

--// Serialization Engine
-- Converts complex/custom types (Vector3, CFrame, Color3, ...) into plain,
-- JSON/DataStore-safe tables so they survive Save()/SavePatch()/SaveCompressed(),
-- and converts them back on LoadSession(). Driven by a "manifest": a table of
-- {fieldName = typeName, ...} configured per-store (Founder config's
-- `SerializationManifest`) and/or per-session (DataSession:SetSerialize()).

local function getSerializer(typeName: string)
	return PersonaStore._SerializerRegistry[typeName]
end

-- Recursively serialize a value. If `typeName` is given, that registered serializer is
-- used directly on the value (this is how manifest-marked fields are handled). If not,
-- known Roblox types are auto-detected via typeof(), and plain tables are walked
-- recursively so nested/embedded typed values (e.g. an array of CFrames inside a
-- manifest-marked field) still get converted correctly.
local function serializeValue(value: any, typeName: string?): any
	if value == nil then
		return nil
	end

	if typeName then
		local serializer = getSerializer(typeName)
		if serializer then
			return {
				__serialized = true,
				__type = typeName,
				__data = serializer.Serialize(value),
			}
		end
		warn("[PersonaStore]: No serializer registered for type '" .. tostring(typeName) .. "'; storing raw")
	end

	local robloxType = typeof(value)
	local autoSerializer = getSerializer(robloxType)
	if autoSerializer then
		return {
			__serialized = true,
			__type = robloxType,
			__data = autoSerializer.Serialize(value),
		}
	end

	if type(value) == "table" then
		local serialized = {}
		for key, nestedValue in pairs(value) do
			serialized[key] = serializeValue(nestedValue)
		end
		return serialized
	end

	return value
end

-- Recursively deserialize a value, reversing serializeValue().
local function deserializeValue(value: any): any
	if value == nil or type(value) ~= "table" then
		return value
	end

	if value.__serialized == true and value.__type and value.__data ~= nil then
		local serializer = getSerializer(value.__type)
		if serializer then
			return serializer.Deserialize(value.__data)
		end
		warn("[PersonaStore]: No serializer registered for type '" .. tostring(value.__type) .. "'; leaving raw")
		return value.__data
	end

	local deserialized = {}
	for key, nestedValue in pairs(value) do
		deserialized[key] = deserializeValue(nestedValue)
	end
	return deserialized
end

-- Applies a manifest ({fieldName = typeName, ...}) to a raw Data table IN-PLACE,
-- converting the marked top-level fields into storage-safe form.
local function applyManifestSerialize(dataTable: table?, manifest: table?)
	if not dataTable or not manifest or next(manifest) == nil then
		return
	end

	for fieldName, typeName in pairs(manifest) do
		if dataTable[fieldName] ~= nil then
			dataTable[fieldName] = serializeValue(dataTable[fieldName], typeName)
		end
	end
end

-- Reverses applyManifestSerialize() on a raw Data table IN-PLACE.
local function applyManifestDeserialize(dataTable: table?, manifest: table?)
	if not dataTable or not manifest or next(manifest) == nil then
		return
	end

	for fieldName in pairs(manifest) do
		if dataTable[fieldName] ~= nil then
			dataTable[fieldName] = deserializeValue(dataTable[fieldName])
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

--// Rate Limiter
-- Priority levels (higher number = more important):
--   3 Critical  – lock acquire, Release, BindToClose full save
--   2 High      – explicit Save / SaveCompressed / IncrementCounter
--   1 Normal    – LoadReadOnly, GetKeyMetadata, Verify, versions
--   0 Low       – auto-save SavePatch, background coalesced writes
--
-- Request kinds map to DataStoreService budgets when available.
local RateLimiter = {
	_inFlight = 0,
	_queue = {}, -- array of {priority, kind, resolve, reject, enqueuedAt}
	_pumpRunning = false,
	_lastPatchAt = {}, -- key -> os.clock() for per-key patch coalescing
	_stats = {
		Queued = 0,
		BudgetWaits = 0,
		CoalescedPatches = 0,
		DroppedLowPriority = 0,
	},
}

local PRIORITY = {
	Critical = 3,
	High = 2,
	Normal = 1,
	Low = 0,
}

-- Best-effort mapping from our logical "kind" to Enum.DataStoreRequestType.
-- Falls back gracefully if an enum item is missing on older clients.
local function resolveBudgetEnum(kind: string): Enum.DataStoreRequestType?
	local RT = Enum.DataStoreRequestType
	if kind == "Update" then
		-- UpdateAsync burns both read and write; prefer StandardWrite if present,
		-- else UpdateAsync / SetIncrementAsync style names.
		return (RT :: any).StandardWrite
			or (RT :: any).UpdateAsync
			or (RT :: any).SetIncrementAsync
	elseif kind == "Read" then
		return (RT :: any).StandardRead
			or (RT :: any).GetAsync
	elseif kind == "OrderedWrite" then
		return (RT :: any).OrderedWrite
			or (RT :: any).SetIncrementSortedAsync
	elseif kind == "OrderedRead" then
		return (RT :: any).OrderedRead
			or (RT :: any).GetAsync
	elseif kind == "OrderedList" then
		return (RT :: any).OrderedList
			or (RT :: any).GetSortedAsync
	elseif kind == "List" or kind == "Version" then
		return (RT :: any).StandardList
			or (RT :: any).ListAsync
			or (RT :: any).GetVersionAsync
	end
	return nil
end

local function getLiveBudget(kind: string): number?
	local enumItem = resolveBudgetEnum(kind)
	if not enumItem then
		return nil
	end
	local ok, budget = pcall(function()
		return DataStoreService:GetRequestBudgetForRequestType(enumItem)
	end)
	if ok and type(budget) == "number" then
		return budget
	end
	return nil
end

local function budgetAllows(kind: string, priority: number): boolean
	if not PersonaStore.EnableRateLimiter then
		return true
	end
	local budget = getLiveBudget(kind)
	if budget == nil then
		-- Budget API unavailable (Studio edge cases, etc.) — allow through.
		return true
	end
	local reserve = PersonaStore.RateLimiterBudgetReserve or 5
	if priority >= PRIORITY.High then
		-- Critical / High may dip into the reserve.
		return budget > 0
	end
	return budget > reserve
end

local function insertByPriority(entry)
	local q = RateLimiter._queue
	local inserted = false
	for i = 1, #q do
		if entry.priority > q[i].priority then
			table.insert(q, i, entry)
			inserted = true
			break
		end
	end
	if not inserted then
		table.insert(q, entry)
	end
	RateLimiter._stats.Queued += 1
end

local function pumpQueue()
	if RateLimiter._pumpRunning then
		return
	end
	RateLimiter._pumpRunning = true

	task.spawn(function()
		while #RateLimiter._queue > 0 or RateLimiter._inFlight > 0 do
			local maxConcurrent = PersonaStore.RateLimiterMaxConcurrent or 8
			local advanced = false

			while #RateLimiter._queue > 0 and RateLimiter._inFlight < maxConcurrent do
				local entry = RateLimiter._queue[1]

				-- Drop Low-priority items that have waited too long.
				if entry.priority <= PRIORITY.Low then
					local waited = os.clock() - entry.enqueuedAt
					local maxWait = PersonaStore.RateLimiterMaxQueueWait or 30
					if waited > maxWait then
						table.remove(RateLimiter._queue, 1)
						RateLimiter._stats.DroppedLowPriority += 1
						entry.resolve(false, "RateLimiter: Low-priority request timed out in queue")
						advanced = true
						continue
					end
				end

				if not budgetAllows(entry.kind, entry.priority) then
					RateLimiter._stats.BudgetWaits += 1
					break -- Wait for budget to recover; don't starve higher items forever.
				end

				table.remove(RateLimiter._queue, 1)
				RateLimiter._inFlight += 1
				advanced = true

				task.spawn(function()
					local ok, result = entry.attemptFn()
					RateLimiter._inFlight -= 1
					entry.resolve(ok, result)
					-- Nudge the pump in case budget/concurrency freed a slot.
					pumpQueue()
				end)
			end

			if not advanced then
				task.wait(0.15)
			end
		end
		RateLimiter._pumpRunning = false
	end)
end

-- Enqueue a rate-limited operation. Returns (success, result) like withRetry.
-- `kind` is one of: "Update" | "Read" | "OrderedWrite" | "OrderedRead" | "OrderedList" | "List" | "Version"
-- `priority` is PRIORITY.Critical / High / Normal / Low
local function enqueueRateLimited(
	kind: string,
	priority: number,
	attemptFn: () -> (boolean, any),
	maxQueueWait: number?
): (boolean, any)
	if not PersonaStore.EnableRateLimiter then
		return attemptFn()
	end

	-- Fast path: capacity + budget available → run immediately under concurrency slot.
	local maxConcurrent = PersonaStore.RateLimiterMaxConcurrent or 8
	if RateLimiter._inFlight < maxConcurrent and budgetAllows(kind, priority) and #RateLimiter._queue == 0 then
		RateLimiter._inFlight += 1
		local ok, result = attemptFn()
		RateLimiter._inFlight -= 1
		pumpQueue()
		return ok, result
	end

	local bindable = Instance.new("BindableEvent")
	local storedOk, storedResult
	local entry = {
		priority = priority,
		kind = kind,
		enqueuedAt = os.clock(),
		attemptFn = attemptFn,
		resolve = function(ok, result)
			storedOk, storedResult = ok, result
			bindable:Fire()
		end,
	}
	insertByPriority(entry)
	pumpQueue()

	local waitCap = maxQueueWait or (priority <= PRIORITY.Low and (PersonaStore.RateLimiterMaxQueueWait or 30) or 120)
	bindable.Event:Wait()
	bindable:Destroy()
	-- If somehow never resolved (shouldn't happen), treat as failure.
	if storedOk == nil then
		return false, "RateLimiter: request never resolved"
	end
	return storedOk, storedResult
end

-- Per-key patch coalescing: returns true if this patch should be skipped
-- because a recent one already went out (or is in flight conceptually).
local function shouldCoalescePatch(sessionKey: string): boolean
	if not PersonaStore.EnableRateLimiter then
		return false
	end
	local minInterval = PersonaStore.RateLimiterMinPatchInterval or 8
	local last = RateLimiter._lastPatchAt[sessionKey]
	if last and (os.clock() - last) < minInterval then
		RateLimiter._stats.CoalescedPatches += 1
		return true
	end
	return false
end

local function markPatchSent(sessionKey: string)
	RateLimiter._lastPatchAt[sessionKey] = os.clock()
end

-- Resilient wrapper around DataStoreService calls with retry logic.
-- When `kind` + `priority` are supplied, the whole retry loop is scheduled
-- through the rate limiter (budget + concurrency + priority queue).
local function withRetry(
	attemptFn: () -> any,
	maxAttempts: number?,
	kind: string?,
	priority: number?
): (boolean, any)
	local attempts = maxAttempts or 5
	local lastError = nil

	local function runAttempts(): (boolean, any)
		for attemptIndex = 1, attempts do
			PersonaStore._statistics.DatastoreRequestsAttempted += 1
			local success, result = pcall(attemptFn)
			if success then
				return true, result
			end

			lastError = result
			PersonaStore._statistics.DatastoreRequestsFailed += 1

			-- Detect throttle-style errors and back off harder.
			local errStr = tostring(result)
			local isThrottle = string.find(errStr, "302")
				or string.find(errStr, "[Tt]hrottl")
				or string.find(errStr, "429")
				or string.find(errStr, "Budget")

			if attemptIndex < attempts then
				local base = isThrottle and (2 ^ (attemptIndex + 1)) or (2 ^ attemptIndex)
				task.wait(math.min(base, 20) + (math.random() * 0.5))
			end
		end
		return false, lastError
	end

	if kind and PersonaStore.EnableRateLimiter then
		return enqueueRateLimited(kind, priority or PRIORITY.Normal, runAttempts)
	end

	return runAttempts()
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

	-- Inherit the store-level SerializationManifest (deep-copied so per-session
	-- SetSerialize()/MarkFieldSerialized() calls don't mutate the Founder's copy).
	self._serializationManifest = deepCopy((parentStore and parentStore._serializationManifest) or {})

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

-- Marks multiple fields for automatic (de)serialization, e.g.
--   session:SetSerialize({Position = "Vector3", Rotation = "CFrame"})
-- This REPLACES whatever manifest the session currently has (including whatever it
-- inherited from the Founder's `SerializationManifest` config). Use
-- MarkFieldSerialized() instead if you just want to add one field.
function DataSession:SetSerialize(manifest: table): boolean
	if type(manifest) ~= "table" then
		warn("[DataSession]: SetSerialize() expects a table of {fieldName = typeName, ...}")
		return false
	end

	self._serializationManifest = manifest
	return true
end

-- Marks a single field for automatic (de)serialization without touching the rest of
-- the manifest.
function DataSession:MarkFieldSerialized(fieldName: string, typeName: string): boolean
	if not fieldName or not typeName then
		return false
	end

	self._serializationManifest = self._serializationManifest or {}
	self._serializationManifest[fieldName] = typeName
	return true
end

-- Manually serializes a value: uses the registered serializer for `typeName` if given,
-- otherwise auto-detects known Roblox types (and recurses into plain tables).
function DataSession:Serialize(value: any, typeName: string?): any
	return serializeValue(value, typeName)
end

-- Manually reverses Serialize().
function DataSession:Deserialize(value: any): any
	return deserializeValue(value)
end

-- Returns a storage-safe (serialized) deep copy of the whole profile, with every
-- manifest-marked field converted. Useful if you want to inspect/ship the exact bytes
-- that a Save() would write without actually saving.
function DataSession:_SerializeProfile(): table
	local serialized = deepCopy(self.Data)
	applyManifestSerialize(serialized, self._serializationManifest)
	return serialized
end

-- Deserializes manifest-marked fields of a raw profile table IN-PLACE, returning it.
function DataSession:_DeserializeProfile(rawData: table): table
	applyManifestDeserialize(rawData, self._serializationManifest)
	return rawData
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

			-- Convert manifest-marked fields (Vector3, CFrame, custom types, ...) into
			-- storage-safe form BEFORE hashing/writing, so both the persisted record
			-- and its integrity hashes reflect what's actually being stored.
			applyManifestSerialize(currentRaw.Data, self._serializationManifest)

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
	end, 5, "Update", PRIORITY.High)

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
-- Rate-limited as Low priority + per-key coalescing so rapid dirty writes don't
-- burn the DataStore budget.
function DataSession:SavePatch(): boolean
	if self._isReleased then return false end

	local dirtyKeys = {}

	for fieldName in pairs(self._dirtyFields) do
		table.insert(dirtyKeys, fieldName)
	end

	if #dirtyKeys == 0 then
		return true
	end

	if shouldCoalescePatch(self._key) then
		-- Still dirty; next auto-save / explicit Save will pick it up.
		return true
	end

	local exportedSnapshot = deepCopy(self.Data)

	-- Serialize manifest-marked fields BEFORE they're written/hashed, so a patch save
	-- persists the same storage-safe form a full Save() would.
	applyManifestSerialize(exportedSnapshot, self._serializationManifest)

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
	end, 5, "Update", PRIORITY.Low)

	if success then
		markPatchSent(self._key)
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

			-- Serialize manifest-marked fields BEFORE JSON-encoding/compressing, since
			-- JSONEncode can't handle Vector3/CFrame/etc. directly.
			applyManifestSerialize(dataToSave, self._serializationManifest)

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
	end, 5, "Update", PRIORITY.High)

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
	end, 3, "Read", PRIORITY.Normal)

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
	end, 3, "Read", PRIORITY.Normal)

	if not success or not remoteRaw then
		return false
	end

	local snapshot = deepCopy(self.Data)

	-- Serialize manifest-marked fields so the hash we compute here matches the hash
	-- that was computed from the (already-serialized) stored data at save time.
	applyManifestSerialize(snapshot, self._serializationManifest)

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
	local exportedData = deepCopy(self.Data)

	-- Serialize manifest-marked fields so the export is plain, JSON-safe data (and so
	-- ImportData() can round-trip it back into real Vector3/CFrame/etc. objects).
	applyManifestSerialize(exportedData, self._serializationManifest)

	local export = {
		Data = exportedData,
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

	-- Reverse the serialization ExportData() applied, so manifest-marked fields come
	-- back in as real Vector3/CFrame/etc. objects rather than raw {__serialized=...} tables.
	applyManifestDeserialize(decoded.Data, self._serializationManifest)

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
	end, 3, "Update", PRIORITY.Critical)
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
-- configOptions supports (among others):
--   Schema                 -> template used to reconcile missing fields on load
--   AutoSaveInterval        -> seconds between automatic SavePatch() calls
--   SerializationManifest   -> {fieldName = typeName, ...} applied to every session
--                               loaded from this store (Vector3/CFrame/Color3/UDim/
--                               UDim2/Vector2 are built in; register custom types via
--                               PersonaStore:RegisterSerializer() first)
function Founder.new<T>(storeName: string, configOptions: table): Founder<T>
	local self = setmetatable({}, Founder)

	self._storeName = storeName
	self._config = configOptions or {}
	self._storageInstance = DataStoreService:GetDataStore(storeName)
	self._activeSessions = {}
	self._templateSchema = self._config.Schema or {}
	self._serializationManifest = self._config.SerializationManifest or {}

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
	end, 5, "Update", PRIORITY.Critical)

	if not success then
		warn("[Founder]: Failed to acquire session lock for key: " .. key .. " -> " .. tostring(err))
		return nil
	end

	-- IMPORTANT: this runs AFTER UpdateAsync has already returned (i.e. the record
	-- that's currently sitting in the DataStore is still in its serialized/storage-safe
	-- form). We only mutate activeSessionObject.Data in-place here, turning
	-- manifest-marked fields back into real Vector3/CFrame/etc. objects for the live
	-- session -- deserializing INSIDE the UpdateAsync callback would instead try to
	-- persist raw Roblox types, which DataStores reject.
	applyManifestDeserialize(activeSessionObject.Data, self._serializationManifest)

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
	end, 3, "Read", PRIORITY.Normal)

	if not success or not data then
		return nil
	end

	-- Decompress if needed before returning snapshot
	decompressRawIfNeeded(data)

	local snapshot = deepCopy(data)

	-- Reverse manifest serialization so callers get real Vector3/CFrame/etc. objects,
	-- consistent with what LoadSession() hands back.
	applyManifestDeserialize(snapshot.Data, self._serializationManifest)

	return snapshot
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
	end, 5, "Update", PRIORITY.Normal)

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
	end, 3, "Read", PRIORITY.Normal)

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
	end, 3, "Version", PRIORITY.Normal)

	if not success then
		warn("[Founder]: ListVersionsAsync failed for key: " .. key .. " -> " .. tostring(pages))
		return nil
	end

	return pages
end

-- Fetches a specific historical version of a key's raw record, decompressing its
-- Data automatically if that version happened to be stored compressed, and reversing
-- any manifest serialization so callers get real Vector3/CFrame/etc. objects back.
function Founder:GetVersionAsync(key: string, version: string): table?
	local success, data = withRetry(function()
		return self._storageInstance:GetVersionAsync(key, version)
	end, 3, "Version", PRIORITY.Normal)

	if not success or not data then
		warn("[Founder]: GetVersionAsync failed for key: " .. key .. " version: " .. tostring(version) .. " -> " .. tostring(data))
		return nil
	end

	decompressRawIfNeeded(data)

	local snapshot = deepCopy(data)
	applyManifestDeserialize(snapshot.Data, self._serializationManifest)

	return snapshot
end

-- Permanently removes a specific historical version of a key. This cannot be undone.
function Founder:RemoveVersionAsync(key: string, version: string): boolean
	local success, err = withRetry(function()
		self._storageInstance:RemoveVersionAsync(key, version)
		return true
	end, 3, "Version", PRIORITY.High)

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
	end, 5, "OrderedWrite", PRIORITY.High)

	return success == true
end

-- Reads the current value for a key, or nil if it doesn't exist
function OrderedFounder:Get(key: string): number?
	local success, value = withRetry(function()
		return self._storageInstance:GetAsync(key)
	end, 3, "OrderedRead", PRIORITY.Normal)

	if not success then
		return nil
	end

	return value
end

-- Atomically increments (or decrements, with a negative delta) a key's value
function OrderedFounder:Increment(key: string, delta: number?): number?
	local success, result = withRetry(function()
		return self._storageInstance:IncrementAsync(key, delta or 1)
	end, 5, "OrderedWrite", PRIORITY.High)

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
	end, 3, "OrderedWrite", PRIORITY.High)

	return success == true
end

-- Fetches one sorted page of entries. Returns the current page ({key, value} entries)
-- plus the raw DataStorePages object (for pagination via :AdvanceToNextPageAsync()).
function OrderedFounder:GetSortedPage(ascending: boolean?, pageSize: number?, minValue: number?, maxValue: number?): (any, DataStorePages)
	if pageSize and (pageSize < 1 or pageSize > 100) then
		warn("[OrderedFounder]: pageSize must be between 1 and 100 for store: " .. self._storeName)
		return nil, nil
	end

	local success, pages = withRetry(function()
		return self._storageInstance:GetSortedAsync(ascending or false, pageSize or 50, minValue, maxValue)
	end, 3, "OrderedList", PRIORITY.Normal)

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

--// Class BufferArray

-- Type-safe typed array utility built on Luau `buffer`. Useful for compact numeric
-- storage (leaderboard snapshots, grids, particle-ish data, etc.) that you may then
-- base64-encode into a DataStore field.

local BufferArray = {}
BufferArray.__index = BufferArray

-- Size of each type in bytes
local typeSizes = {
	u8 = 1, i8 = 1,
	u16 = 2, i16 = 2,
	u32 = 4, i32 = 4,
	u64 = 8, i64 = 8,
	f32 = 4,
	f64 = 8,
}

-- Reader function names on the global `buffer` library (buffer.readXXX)
local bufferReadMethods = {
	u8 = "readu8", i8 = "readi8",
	u16 = "readu16", i16 = "readi16",
	u32 = "readu32", i32 = "readi32",
	u64 = "readu64", i64 = "readi64",
	f32 = "readf32",
	f64 = "readf64",
}

-- Writer function names on the global `buffer` library (buffer.writeXXX)
local bufferWriteMethods = {
	u8 = "writeu8", i8 = "writei8",
	u16 = "writeu16", i16 = "writei16",
	u32 = "writeu32", i32 = "writei32",
	u64 = "writeu64", i64 = "writei64",
	f32 = "writef32",
	f64 = "writef64",
}

-- Create a new typed array
function BufferArray.new(elementType: string, elementCount: number): BufferArray
	if not typeSizes[elementType] then
		error("[BufferArray]: Unknown element type '" .. tostring(elementType) .. "'")
	end

	if type(elementCount) ~= "number" or elementCount < 1 then
		error("[BufferArray]: elementCount must be a positive integer")
	end

	local elementSize = typeSizes[elementType]
	local totalSize = elementSize * elementCount

	local self = setmetatable({
		_buffer = buffer.create(totalSize),
		_elementType = elementType,
		_elementSize = elementSize,
		_elementCount = elementCount,
		_readMethod = bufferReadMethods[elementType],
		_writeMethod = bufferWriteMethods[elementType],
	}, BufferArray)

	return self
end

-- Get element at index (0-based)
-- BUG FIX: the original implementation indexed the buffer VALUE itself with a string
-- key (self._buffer[self._readMethod](...)) -- Luau buffers aren't tables, so that
-- isn't callable/indexable. The reader/writer functions live on the global `buffer`
-- library instead, e.g. buffer.readu8(self._buffer, offset).
function BufferArray:Get(index: number): number
	if index < 0 or index >= self._elementCount then
		error("[BufferArray]: Index out of bounds: " .. index)
	end

	local offset = index * self._elementSize
	return buffer[self._readMethod](self._buffer, offset)
end

-- Set element at index (0-based)
function BufferArray:Set(index: number, value: number): boolean
	if index < 0 or index >= self._elementCount then
		error("[BufferArray]: Index out of bounds: " .. index)
	end

	local offset = index * self._elementSize
	buffer[self._writeMethod](self._buffer, offset, value)
	return true
end

-- Get multiple elements as a table
function BufferArray:GetRange(startIndex: number, length: number): {number}
	if startIndex < 0 or startIndex + length > self._elementCount then
		error("[BufferArray]: Range out of bounds")
	end

	local result = {}
	for i = 0, length - 1 do
		table.insert(result, self:Get(startIndex + i))
	end

	return result
end

-- Set multiple elements from a table
function BufferArray:SetRange(startIndex: number, values: {number}): boolean
	if startIndex < 0 or startIndex + #values > self._elementCount then
		error("[BufferArray]: Range out of bounds")
	end

	for i, value in ipairs(values) do
		self:Set(startIndex + i - 1, value)
	end

	return true
end

-- Fill entire array with a value
function BufferArray:Fill(value: number): boolean
	for i = 0, self._elementCount - 1 do
		self:Set(i, value)
	end
	return true
end

-- Get the raw buffer for direct access or serialization
function BufferArray:GetBuffer(): buffer
	return self._buffer
end

-- Get array metadata
function BufferArray:GetMetadata(): table
	return {
		ElementType = self._elementType,
		ElementSize = self._elementSize,
		ElementCount = self._elementCount,
		TotalSize = buffer.len(self._buffer),
	}
end

-- Encode to base64 for storage
function BufferArray:EncodeToBase64(): string
	return CompressionHandler.encodeToBase64(self._buffer)
end

-- Static method: decode from base64
function BufferArray.DecodeFromBase64(elementType: string, encoded: string): BufferArray?
	if not typeSizes[elementType] then
		error("[BufferArray]: Unknown element type '" .. tostring(elementType) .. "'")
	end

	local decodedBuffer = CompressionHandler.decodeFromBase64(encoded)
	local elementSize = typeSizes[elementType]
	local elementCount = buffer.len(decodedBuffer) / elementSize

	if elementCount ~= math.floor(elementCount) then
		error("[BufferArray]: Decoded buffer size doesn't align with element size")
	end

	local self = setmetatable({
		_buffer = decodedBuffer,
		_elementType = elementType,
		_elementSize = elementSize,
		_elementCount = elementCount,
		_readMethod = bufferReadMethods[elementType],
		_writeMethod = bufferWriteMethods[elementType],
	}, BufferArray)

	return self
end

-- Creates the engine singleton
function PersonaStore.new()
	return setmetatable({}, PersonaStore)
end

-- Registers and returns a new isolated Founder
-- If a store with the same name already exists, it returns the existing one
function PersonaStore:CreateDataStore<T>(storeName: string, configOptions: table): Founder<T>
	if self.RegisteredStores[storeName] then
		return self.RegisteredStores[storeName] :: T
	end

	return Founder.new(storeName, configOptions) :: T
end

-- Registers and returns a new isolated OrderedFounder
-- If a store with the same name already exists, it returns the existing one
function PersonaStore:CreateOrderedDataStore<T>(storeName: string): OrderedFounder<T>
	if self.RegisteredOrderedStores[storeName] then
		return self.RegisteredOrderedStores[storeName] :: T
	end

	return OrderedFounder.new(storeName) :: T
end

-- Registers and returns a new MemoryQueueWrapper
-- If a queue with the same name already exists, it returns the existing one
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
-- Only call this once on the server
--[[
	CompressionAlgorithm: Enum.CompressionAlgorithm?, -- default Zstd
	CompressionLevel: number?, -- default 6
	EnableDataIntegrityChecks: boolean?, -- default true
	HashAlgorithm: Enum.HashAlgorithm?, -- default sha256
	IntegrityMode: "Full" | "HashOnlyOnFullSave" | "PerField"?, -- default Full
	GlobalLockTimeout: number?, -- default 120 (seconds)
	GlobalAutoSaveInterval: number?, -- default 30 (seconds) 
]]--
function PersonaStore:Init(settings: PersonaStoreSettings?)
	if PersonaStore.EngineActive then
		warn("[PersonaStore]: Init() called more than once!")
		return
	end

	if settings and settings ~= {} then
		for k, v in settings do
			-- Use ~= nil so boolean false settings (e.g. EnableRateLimiter = false) apply.
			if PersonaStore[k] ~= nil then
				PersonaStore[k] = v
			else
				error("[PersonaStore]: Invalid setting key '" .. tostring(k) .. "'")
			end
		end
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

-- Get engine-wide statistics (includes rate-limiter counters when enabled)
function PersonaStore:GetStatistics(): table
	local stats = deepCopy(PersonaStore._statistics)
	stats.RateLimiter = {
		InFlight = RateLimiter._inFlight,
		Queued = #RateLimiter._queue,
		QueuedTotal = RateLimiter._stats.Queued,
		BudgetWaits = RateLimiter._stats.BudgetWaits,
		CoalescedPatches = RateLimiter._stats.CoalescedPatches,
		DroppedLowPriority = RateLimiter._stats.DroppedLowPriority,
		EnableRateLimiter = PersonaStore.EnableRateLimiter,
	}
	return stats
end

-- Live snapshot of the rate limiter (queue depth, budgets, etc.)
function PersonaStore:GetRateLimiterStatus(): table
	local status = {
		Enabled = PersonaStore.EnableRateLimiter,
		InFlight = RateLimiter._inFlight,
		QueueDepth = #RateLimiter._queue,
		MaxConcurrent = PersonaStore.RateLimiterMaxConcurrent,
		BudgetReserve = PersonaStore.RateLimiterBudgetReserve,
		MinPatchInterval = PersonaStore.RateLimiterMinPatchInterval,
		Stats = deepCopy(RateLimiter._stats),
		Budgets = {},
	}
	for _, kind in ipairs({ "Update", "Read", "OrderedWrite", "OrderedRead", "OrderedList", "Version" }) do
		status.Budgets[kind] = getLiveBudget(kind)
	end
	return status
end

-- Toggle / retune the rate limiter at runtime (after Init)
function PersonaStore:SetRateLimiterSettings(settings: table)
	if type(settings) ~= "table" then
		return
	end
	if settings.EnableRateLimiter ~= nil then
		PersonaStore.EnableRateLimiter = settings.EnableRateLimiter
	end
	if settings.BudgetReserve ~= nil then
		PersonaStore.RateLimiterBudgetReserve = math.max(0, settings.BudgetReserve)
	end
	if settings.MaxConcurrent ~= nil then
		PersonaStore.RateLimiterMaxConcurrent = math.max(1, settings.MaxConcurrent)
	end
	if settings.MinPatchInterval ~= nil then
		PersonaStore.RateLimiterMinPatchInterval = math.max(0, settings.MinPatchInterval)
	end
	if settings.MaxQueueWait ~= nil then
		PersonaStore.RateLimiterMaxQueueWait = math.max(1, settings.MaxQueueWait)
	end
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

-- Registers a custom (de)serializer for a type name, so it can be used in a Founder's
-- `SerializationManifest`, a session's SetSerialize()/MarkFieldSerialized(), or passed
-- explicitly to DataSession:Serialize(value, "MyType"). `serializer` must be a table
-- with Serialize(value) -> storageSafeData and Deserialize(data) -> value.
--
-- Example:
--   PersonaStore:RegisterSerializer("Currency", {
--       Serialize = function(c) return {Amount = c.Amount, Kind = c.Kind} end,
--       Deserialize = function(d) return Currency.new(d.Amount, d.Kind) end,
--   })
function PersonaStore:RegisterSerializer(typeName: string, serializer: table): boolean
	if type(serializer) ~= "table" or type(serializer.Serialize) ~= "function" or type(serializer.Deserialize) ~= "function" then
		warn("[PersonaStore]: RegisterSerializer() requires a table with Serialize and Deserialize functions")
		return false
	end

	self._SerializerRegistry[typeName] = serializer
	return true
end

--// Expose the serialization engine and BufferArray on the module table itself so
-- they're usable without a live session, e.g. PersonaStore.SerializeValue(v, "Vector3")
-- or PersonaStore.BufferArray.new("u32", 1000).
PersonaStore.BufferArray = BufferArray
PersonaStore.SerializeValue = serializeValue
PersonaStore.DeserializeValue = deserializeValue

-- Client require guard.
local guardSuccess, guardErr = withRetry(function()
	if not RunService:IsServer() then
		error("PersonaStore can only be required from the server")
	end
	return true
end, 1)

if not guardSuccess then
	error("[PersonaStore]: " .. tostring(guardErr))
end

--// Type definitions for Luau type checking
export type table =
	{ [string]: any } | { [number]: any } | { table }

export type PersonaStoreSettings = {
	CompressionAlgorithm: Enum.CompressionAlgorithm?, -- default Zstd
	CompressionLevel: number?, -- default 6
	EnableDataIntegrityChecks: boolean?, -- default true
	HashAlgorithm: Enum.HashAlgorithm?, -- default sha256
	IntegrityMode: "Full" | "HashOnlyOnFullSave" | "PerField"?, -- default Full
	GlobalLockTimeout: number?, -- default 120 (seconds)
	GlobalAutoSaveInterval: number?, -- default 30 (seconds)
	-- Rate limiter
	EnableRateLimiter: boolean?, -- default true
	RateLimiterBudgetReserve: number?, -- default 5 (leave headroom for Critical/High)
	RateLimiterMaxConcurrent: number?, -- default 8
	RateLimiterMinPatchInterval: number?, -- default 8 seconds between patches per key
	RateLimiterMaxQueueWait: number?, -- default 30 seconds for Low priority
}

export type MemoryQueueWrapper = typeof(MemoryQueueWrapper)
export type MemorySortedMapWrapper = typeof(MemorySortedMapWrapper)

export type BufferArray = {
	Get: (self: BufferArray, index: number) -> number,
	Set: (self: BufferArray, index: number, value: number) -> boolean,
	GetRange: (self: BufferArray, startIndex: number, length: number) -> {number},
	SetRange: (self: BufferArray, startIndex: number, values: {number}) -> boolean,
	Fill: (self: BufferArray, value: number) -> boolean,
	GetBuffer: (self: BufferArray) -> buffer,
	GetMetadata: (self: BufferArray) -> table,
	EncodeToBase64: (self: BufferArray) -> string,
}

export type Founder<T> = {
	_storeName: string,
	_config: table,
	_storageInstance: DataStore,
	_activeSessions: {[string]: DataSession<T>},
	_templateSchema: T,
	_serializationManifest: table,

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
