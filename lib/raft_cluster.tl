--- Deterministic multi-node harness.
---
--- The cluster owns a simulated network and one RaftNode per member. A tick
--- advances the logical clock, ticks every node once, then delivers everything
--- that became due. There is no wall clock and no goroutine: partitions,
--- latency and crashes-in-place are fully reproducible.
---
--- `checkInvariants` turns Raft's safety properties into executable checks:
---   * at most one leader per term, ever;
---   * log matching (same index+term => same prefix and entry);
---   * committed entries agree across nodes;
---   * a leader's log contains every committed index.

import nodeMod from "./raft_node"
import kv from "./raft_kv"
import storageMod from "./raft_storage"
import transportMod from "./raft_transport"

local function copyIds(ids: string[]): string[]
  local out: string[] = {}
  for i: integer = 1, #ids do
    out[i] = ids[i]
  end
  return out
end

class Cluster implements ClusterApi {
  ids: string[] = {},
  nodes: map<string, RaftNodeApi> = {},
  machines: map<string, KVView> = {},
  transport: SimTransport,
  now: integer = 0,
  termLeaders: map<integer, string> = {},
  violations: string[] = {},
  violationSet: map<string, boolean> = {},

  constructor(size: integer, options: ClusterOptions)
    if size < 1 or size > 99 then
      error("Cluster: size must be between 1 and 99")
    end
    self.transport = transportMod.InMemoryTransport.new()
    local latency: integer = 0
    local latencyOpt: integer | nil = options.latency
    if latencyOpt ~= nil then
      latency = latencyOpt!
    end
    self.transport:setLatency(latency)

    local config: RaftConfig = {}
    local et: integer | nil = options.electionTimeout
    if et ~= nil then config.electionTimeout = et! end
    local jitter: integer | nil = options.electionJitter
    if jitter ~= nil then config.electionJitter = jitter! end
    local hb: integer | nil = options.heartbeatInterval
    if hb ~= nil then config.heartbeatInterval = hb! end
    local snap: integer | nil = options.snapshotThreshold
    if snap ~= nil then config.snapshotThreshold = snap! end
    local batch: integer | nil = options.maxBatch
    if batch ~= nil then config.maxBatch = batch! end
    local seed: integer = 1
    local seedOpt: integer | nil = options.seed
    if seedOpt ~= nil then seed = seedOpt! end

    local ids: string[] = {}
    for i: integer = 1, size do
      ids[#ids + 1] = "n" .. tostring(i)
    end
    self.ids = copyIds(ids)
    for i: integer = 1, #ids do
      local id: string = ids[i]
      local machine = kv.KVStore.new()
      self.machines[id] = machine
      self.nodes[id] = nodeMod.RaftNode.new(
        id,
        ids,
        storageMod.MemoryStorage.new(),
        self.transport,
        machine,
        config,
        seed + i * 7919
      )
    end
  end,

  -- -----------------------------------------------------------------------
  -- Clock and delivery
  -- -----------------------------------------------------------------------

  tick(): void
    self.now = self.now + 1
    self.transport:advance(1)
    for i: integer = 1, #self.ids do
      self.nodes[self.ids[i]]!:tick()
    end
    for i: integer = 1, #self.ids do
      local id: string = self.ids[i]
      local node: RaftNodeApi = self.nodes[id]!
      local ready: Envelope[] = self.transport:drain(id, self.now)
      for j: integer = 1, #ready do
        local envelope: Envelope = ready[j]
        node:handle(envelope.from, envelope.message)
      end
    end
    self:recordLeaders()
  end,

  run(ticks: integer): void
    for _: integer = 1, ticks do
      self:tick()
    end
  end,

  isStable(): boolean
    -- In-flight heartbeats are normal, so "no pending messages" is the wrong
    -- test. Stability means one leader, everything committed, and every
    -- follower has applied that prefix (as far as its own commit allows).
    if self.transport:blockedCount() ~= 0 then
      return false
    end
    local leaders: string[] = self:leaders()
    if #leaders ~= 1 then
      return false
    end
    local leader: RaftNodeApi = self.nodes[leaders[1]]!
    if leader.commitIndex ~= leader.log:lastIndex() then
      return false
    end
    for i: integer = 1, #self.ids do
      local node: RaftNodeApi = self.nodes[self.ids[i]]!
      if node.lastApplied < leader.commitIndex then
        return false
      end
      if node.log:lastIndex() < leader.commitIndex then
        return false
      end
    end
    return true
  end,

  stabilize(maxTicks: integer): boolean
    for _: integer = 1, maxTicks do
      if self:isStable() then
        return true
      end
      self:tick()
    end
    return self:isStable()
  end,

  -- -----------------------------------------------------------------------
  -- Observability
  -- -----------------------------------------------------------------------

  node(id: string): RaftNodeApi
    local found: RaftNodeApi | nil = self.nodes[id]
    if found == nil then
      error("Cluster: unknown node '" .. id .. "'")
    end
    return found!
  end,

  nodeIds(): string[]
    return copyIds(self.ids)
  end,

  leaders(): string[]
    local out: string[] = {}
    for i: integer = 1, #self.ids do
      local id: string = self.ids[i]
      local node: RaftNodeApi = self.nodes[id]!
      if node.role == "leader" then
        out[#out + 1] = id
      end
    end
    return out
  end,

  leaderId(): string | nil
    local best: string | nil = nil
    local bestTerm: integer = -1
    for i: integer = 1, #self.ids do
      local id: string = self.ids[i]
      local node: RaftNodeApi = self.nodes[id]!
      if node.role == "leader" and node.currentTerm > bestTerm then
        best = id
        bestTerm = node.currentTerm
      end
    end
    return best
  end,

  lookup(id: string, key: string): string | nil
    local machine: KVView | nil = self.machines[id]
    if machine == nil then
      error("Cluster: unknown node '" .. id .. "'")
    end
    return machine!:get(key)
  end,

  sentCount(kind: string): integer
    return self.transport:sentCount(kind)
  end,

  pendingMessages(): integer
    return self.transport:pending()
  end,

  -- -----------------------------------------------------------------------
  -- Client helpers
  -- -----------------------------------------------------------------------

  propose(command: string): ClusterProposal
    local leader: string | nil = self:leaderId()
    if leader == nil then
      return { ok = false, index = 0, leader = nil }
    end
    local result: ProposeResult = self.nodes[leader]!:propose(command)
    return { ok = result.ok, index = result.index, leader = leader }
  end,

  waitCommitted(index: integer, maxTicks: integer): boolean
    for _: integer = 1, maxTicks do
      if self:committedAtLeast(index) then
        return true
      end
      self:tick()
    end
    return self:committedAtLeast(index)
  end,

  committedAtLeast(index: integer): boolean
    for i: integer = 1, #self.ids do
      local node: RaftNodeApi = self.nodes[self.ids[i]]!
      if node.commitIndex >= index then
        return true
      end
    end
    return false
  end,

  waitCommand(index: integer, command: string, maxTicks: integer): boolean
    for _: integer = 1, maxTicks do
      if self:hasCommittedCommand(index, command) then
        return true
      end
      self:tick()
    end
    return self:hasCommittedCommand(index, command)
  end,

  hasCommittedCommand(index: integer, command: string): boolean
    for i: integer = 1, #self.ids do
      local node: RaftNodeApi = self.nodes[self.ids[i]]!
      if node.commitIndex >= index and node.log:contains(index) then
        if index > node.log:baseIndex() then
          local entry: LogEntry = node.log:entryAt(index)
          if entry.kind == "command" and entry.command == command then
            return true
          end
        end
      end
    end
    return false
  end,

  -- -----------------------------------------------------------------------
  -- Fault injection
  -- -----------------------------------------------------------------------

  heal(): void
    self.transport:clearBlocks()
  end,

  drop(from: string, to: string, blocked: boolean): void
    self.transport:setBlocked(from, to, blocked)
  end,

  setLatency(ticks: integer): void
    self.transport:setLatency(ticks)
  end,

  partition(groups: string[][]): void
    self:heal()
    if #groups < 2 then
      error("Cluster: partition needs at least two groups")
    end
    local groupOf: map<string, integer> = {}
    for g: integer = 1, #groups do
      local group: string[] = groups[g]
      if #group == 0 then
        error("Cluster: empty partition group")
      end
      for i: integer = 1, #group do
        local id: string = group[i]
        if self.nodes[id] == nil then
          error("Cluster: unknown node in partition: " .. id)
        end
        if groupOf[id] ~= nil then
          error("Cluster: node appears twice in partition: " .. id)
        end
        groupOf[id] = g
      end
    end
    for i: integer = 1, #self.ids do
      if groupOf[self.ids[i]] == nil then
        error("Cluster: partition must include every node")
      end
    end
    for i: integer = 1, #self.ids do
      for j: integer = 1, #self.ids do
        if i ~= j then
          local a: string = self.ids[i]
          local b: string = self.ids[j]
          if groupOf[a] ~= groupOf[b] then
            self.transport:setBlocked(a, b, true)
          end
        end
      end
    end
  end,

  -- -----------------------------------------------------------------------
  -- Safety checks
  -- -----------------------------------------------------------------------

  recordLeaders(): void
    for i: integer = 1, #self.ids do
      local node: RaftNodeApi = self.nodes[self.ids[i]]!
      if node.role == "leader" then
        local term: integer = node.currentTerm
        local existing: string | nil = self.termLeaders[term]
        if existing == nil then
          self.termLeaders[term] = node.id
        elseif existing != node.id then
          self:violation(
            "election:" .. tostring(term),
            "two leaders in term " .. tostring(term) .. ": " .. existing! .. " and " .. node.id
          )
        end
      end
    end
  end,

  violation(key: string, message: string): void
    if self.violationSet[key] == true then
      return
    end
    self.violationSet[key] = true
    self.violations[#self.violations + 1] = message
  end,

  checkInvariants(): string[]
    self:recordLeaders()
    for i: integer = 1, #self.ids do
      for j: integer = i + 1, #self.ids do
        local aId: string = self.ids[i]
        local bId: string = self.ids[j]
        local a: RaftNodeApi = self.nodes[aId]!
        local b: RaftNodeApi = self.nodes[bId]!
        self:checkLogMatching(aId, a, bId, b)
        self:checkCommitted(aId, a, bId, b)
      end
    end
    -- Only the newest leader is required to contain everything committed so
    -- far. An old leader in a minority partition may legitimately lag behind
    -- entries committed by a higher-term majority it cannot see.
    local topTerm: integer = -1
    for i: integer = 1, #self.ids do
      local node: RaftNodeApi = self.nodes[self.ids[i]]!
      if node.role == "leader" and node.currentTerm > topTerm then
        topTerm = node.currentTerm
      end
    end
    if topTerm >= 0 then
      for i: integer = 1, #self.ids do
        local node: RaftNodeApi = self.nodes[self.ids[i]]!
        if node.role == "leader" and node.currentTerm == topTerm then
          self:checkLeaderCompleteness(node)
        end
      end
    end
    local out: string[] = {}
    for i: integer = 1, #self.violations do
      out[i] = self.violations[i]
    end
    return out
  end,

  checkLogMatching(aId: string, a: RaftNodeApi, bId: string, b: RaftNodeApi): void
    local baseA: integer = a.log:baseIndex()
    local baseB: integer = b.log:baseIndex()
    local from: integer = baseA + 1
    if baseB > baseA then
      from = baseB + 1
    end
    local last: integer = a.log:lastIndex()
    if b.log:lastIndex() < last then
      last = b.log:lastIndex()
    end
    for index: integer = from, last do
      if a.log:termAt(index) == b.log:termAt(index) then
        local ea: LogEntry = a.log:entryAt(index)
        local eb: LogEntry = b.log:entryAt(index)
        if ea.kind ~= eb.kind or ea.command ~= eb.command then
          self:violation(
            "entry:" .. tostring(index),
            "same index/term, different entry on " .. aId .. " and " .. bId
              .. " at index " .. tostring(index)
          )
        end
        if index > from and a.log:termAt(index - 1) ~= b.log:termAt(index - 1) then
          self:violation(
            "prefix:" .. aId .. ":" .. bId .. ":" .. tostring(index),
            "log matching violated at index " .. tostring(index)
              .. " (" .. aId .. " vs " .. bId .. ")"
          )
        end
      end
    end
  end,

  checkCommitted(aId: string, a: RaftNodeApi, bId: string, b: RaftNodeApi): void
    local baseA: integer = a.log:baseIndex()
    local baseB: integer = b.log:baseIndex()
    local from: integer = baseA + 1
    if baseB > baseA then
      from = baseB + 1
    end
    local upto: integer = a.commitIndex
    if b.commitIndex < upto then
      upto = b.commitIndex
    end
    for index: integer = from, upto do
      if a.log:termAt(index) ~= b.log:termAt(index) then
        self:violation(
          "committed:" .. tostring(index),
          "committed entry differs at index " .. tostring(index)
            .. " (" .. aId .. " vs " .. bId .. ")"
        )
      else
        local ea: LogEntry = a.log:entryAt(index)
        local eb: LogEntry = b.log:entryAt(index)
        if ea.command ~= eb.command or ea.kind ~= eb.kind then
          self:violation(
            "committed-entry:" .. tostring(index),
            "committed command differs at index " .. tostring(index)
              .. " (" .. aId .. " vs " .. bId .. ")"
          )
        end
      end
    end
  end,

  checkLeaderCompleteness(leader: RaftNodeApi): void
    for i: integer = 1, #self.ids do
      local other: RaftNodeApi = self.nodes[self.ids[i]]!
      if other.currentTerm <= leader.currentTerm then
        self:checkLeaderAgainst(leader, other)
      end
    end
  end,

  checkLeaderAgainst(leader: RaftNodeApi, other: RaftNodeApi): void
      local baseL: integer = leader.log:baseIndex()
      local baseO: integer = other.log:baseIndex()
      local from: integer = baseL + 1
      if baseO > baseL then
        from = baseO + 1
      end
      local upto: integer = other.commitIndex
      if leader.log:lastIndex() < upto then
        self:violation(
          "short-leader:" .. leader.id,
          "leader " .. leader.id .. " lacks committed index " .. tostring(upto)
        )
      else
        for index: integer = from, upto do
          if leader.log:termAt(index) ~= other.log:termAt(index) then
            self:violation(
              "leader-missing:" .. leader.id .. ":" .. tostring(index),
              "leader " .. leader.id .. " disagrees on committed index "
                .. tostring(index) .. " with " .. other.id
            )
          end
        end
      end
  end,
}

return { Cluster = Cluster }
