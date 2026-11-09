import T from "tlua_test"
import nodeMod from "../lib/raft_node"
import kv from "../lib/raft_kv"
import kit from "../lib/raft_testkit"

type Rig = {
  transport: SimTransport,
  nodes: map<string, RaftNodeApi>,
  machines: map<string, KVView>,
}

local function fastConfig(election: integer, snapshotThreshold: integer): RaftConfig
  return {
    electionTimeout = election,
    electionJitter = 0,
    heartbeatInterval = 1,
    snapshotThreshold = snapshotThreshold,
    maxBatch = 8,
  }
end

local function rig(ids: string[], config: RaftConfig, seed: integer): Rig
  local transport = kit.InMemoryTransport.new()
  local nodes: map<string, RaftNodeApi> = {}
  local machines: map<string, KVView> = {}
  for i: integer = 1, #ids do
    local id: string = ids[i]
    local machine = kv.KVStore.new()
    machines[id] = machine
    nodes[id] = nodeMod.RaftNode.new(
      id, ids, kit.MemoryStorage.new(), transport, machine, config, seed + i * 131
    )
  end
  return { transport = transport, nodes = nodes, machines = machines }
end

local function nodeOf(r: Rig, id: string): RaftNodeApi
  return r.nodes[id]!
end

local function machineOf(r: Rig, id: string): KVView
  return r.machines[id]!
end

local function pumpAll(r: Rig, rounds: integer): void
  for _: integer = 1, rounds do
    r.transport:advance(1)
    kit.pump(r.transport, r.nodes, r.transport.now)
  end
end

local function pumpOnly(r: Rig, ids: string[], rounds: integer): void
  for _: integer = 1, rounds do
    r.transport:advance(1)
    for i: integer = 1, #ids do
      local id: string = ids[i]
      local node: RaftNodeApi = r.nodes[id]!
      local ready: Envelope[] = r.transport:drain(id, r.transport.now)
      for j: integer = 1, #ready do
        node:handle(ready[j].from, ready[j].message)
      end
    end
  end
end

local function elect(r: Rig, id: string): void
  nodeOf(r, id):startElection()
  pumpAll(r, 5)
  T.eq(nodeOf(r, id).role, "leader", "elected " .. id)
end

--- Ticks the leader (heartbeats carry leaderCommit) while delivering traffic.
local function settle(r: Rig, leaderId: string, rounds: integer): void
  for _: integer = 1, rounds do
    local leader: RaftNodeApi = nodeOf(r, leaderId)
    if leader:isLeader() then
      leader:tick()
    end
    r.transport:advance(1)
    kit.pump(r.transport, r.nodes, r.transport.now)
  end
end

local function settleOnly(r: Rig, leaderId: string, ids: string[], rounds: integer): void
  for _: integer = 1, rounds do
    local leader: RaftNodeApi = nodeOf(r, leaderId)
    if leader:isLeader() then
      leader:tick()
    end
    pumpOnly(r, ids, 1)
  end
end

local function hasCommand(entries: LogEntry[], command: string): boolean
  for i: integer = 1, #entries do
    if entries[i].command == command then
      return true
    end
  end
  return false
end

local function blockAll(r: Rig, isolates: string[], rest: string[]): void
  for i: integer = 1, #isolates do
    for j: integer = 1, #rest do
      local a: string = isolates[i]
      local b: string = rest[j]
      r.transport:setBlocked(a, b, true)
      r.transport:setBlocked(b, a, true)
    end
  end
end

local function unblockAll(r: Rig, ids: string[]): void
  for i: integer = 1, #ids do
    for j: integer = 1, #ids do
      if i ~= j then
        r.transport:setBlocked(ids[i], ids[j], false)
      end
    end
  end
end

return {
  ["old-term entries are not committed by replica count alone"] = function(): void
    local ids: string[] = { "n1", "n2", "n3" }
    local r: Rig = rig(ids, fastConfig(10, 0), 21)
    local storage1 = kit.MemoryStorage.new()
    storage1:saveHardState(1, nil)
    storage1:saveLog(0, 0, {
      { term = 1, kind = "command", command = kv.putCommand("old", "1") },
    })
    local machine1 = kv.KVStore.new()
    local n1 = nodeMod.RaftNode.new(
      "n1", ids, storage1, r.transport, machine1, fastConfig(10, 0), 21
    )
    r.nodes["n1"] = n1
    r.machines["n1"] = machine1

    n1:startElection() -- term 2 with a term-1 entry at index 1
    pumpOnly(r, { "n2", "n3" }, 1) -- vote requests
    pumpOnly(r, { "n1" }, 1) -- vote replies -> leader, appendEntries queued
    T.eq(n1.role, "leader", "leader")
    T.eq(n1.currentTerm, 2, "term")
    T.eq(n1.log:lastIndex(), 2, "old entry + no-op")
    T.eq(n1.commitIndex, 0, "nothing committed by count alone")

    -- Let only n2 receive the log. n1 + n2 is a majority; n3 stays in the
    -- dark, so its queue is never drained.
    for _: integer = 1, 8 do
      r.transport:advance(1)
      local toN2: Envelope[] = r.transport:drain("n2", r.transport.now)
      for i: integer = 1, #toN2 do
        local envelope: Envelope = toN2[i]
        r.nodes["n2"]!:handle(envelope.from, envelope.message)
      end
      local toN1: Envelope[] = r.transport:drain("n1", r.transport.now)
      for i: integer = 1, #toN1 do
        local envelope: Envelope = toN1[i]
        n1:handle(envelope.from, envelope.message)
      end
    end
    T.eq(n1.commitIndex, 2, "no-op on a majority commits the prefix")
    T.eq(machine1:get("old"), "1", "old-term entry applied")
  end,

  ["minority entries are truncated when the majority wins"] = function(): void
    local ids: string[] = { "n1", "n2", "n3" }
    local r: Rig = rig(ids, fastConfig(20, 0), 31)
    elect(r, "n1")
    local n1: RaftNodeApi = nodeOf(r, "n1")
    T.ok(n1:propose(kv.putCommand("a", "1")).ok, "a")
    settle(r, "n1", 4)
    T.ok(n1:propose(kv.putCommand("b", "2")).ok, "b")
    settle(r, "n1", 4)
    T.eq(machineOf(r, "n1"):get("a"), "1", "a committed")

    -- Isolate the old leader and elect n2 with the other two nodes.
    blockAll(r, { "n1" }, { "n2", "n3" })
    nodeOf(r, "n2"):startElection()
    pumpOnly(r, { "n2", "n3" }, 5)
    T.eq(nodeOf(r, "n2").role, "leader", "n2 leads term 2")

    local n2: RaftNodeApi = nodeOf(r, "n2")
    T.ok(n2:propose(kv.putCommand("c", "3")).ok, "c")
    settleOnly(r, "n2", { "n2", "n3" }, 5)
    T.eq(machineOf(r, "n2"):get("c"), "3", "c committed without n1")

    -- The stale leader still accepts a client command; it cannot commit it.
    local stale: ProposeResult = n1:propose(kv.putCommand("d", "4"))
    T.ok(stale.ok, "stale leader accepts locally")
    local before: integer = n1.commitIndex
    settleOnly(r, "n1", { "n1" }, 2)
    T.eq(n1.commitIndex, before, "stale entry not committed")

    unblockAll(r, ids)
    settle(r, "n2", 12)

    T.eq(n1.role, "follower", "old leader followed")
    T.eq(n1.currentTerm, 2, "term caught up")
    T.ok(not hasCommand(n1.log:allEntries(), kv.putCommand("d", "4")), "stale entry truncated")
    T.eq(machineOf(r, "n1"):get("c"), "3", "follower applied majority log")
    T.eq(machineOf(r, "n1"):get("d"), nil, "stale value absent")
    for i: integer = 1, 3 do
      T.eq(machineOf(r, ids[i]):get("a"), "1", "a on " .. ids[i])
      T.eq(machineOf(r, ids[i]):get("c"), "3", "c on " .. ids[i])
    end
  end,

  ["snapshot compacts and restores on restart"] = function(): void
    local shared = kit.MemoryStorage.new()
    local transport = kit.InMemoryTransport.new()
    local machine = kv.KVStore.new()
    local original = nodeMod.RaftNode.new(
      "n1", { "n1" }, shared, transport, machine, fastConfig(10, 0), 42
    )
    for _: integer = 1, 10 do
      original:tick()
    end
    T.eq(original.role, "leader", "leader")
    original:propose(kv.putCommand("k1", "v1"))
    original:propose(kv.putCommand("k2", "v2"))
    original:snapshotNow()
    T.ok(shared:loadSnapshot() ~= nil, "snapshot persisted")
    T.eq(original.log:baseIndex(), original.commitIndex, "log compacted")
    T.eq(original.log:lastIndex(), original.log:baseIndex(), "all entries covered")

    local restoredMachine = kv.KVStore.new()
    local restored = nodeMod.RaftNode.new(
      "n1", { "n1" }, shared, transport, restoredMachine, fastConfig(10, 0), 42
    )
    T.eq(restored.commitIndex, original.commitIndex, "commit index restored")
    T.eq(restored.lastApplied, original.lastApplied, "apply index restored")
    T.eq(restoredMachine:get("k1"), "v1", "k1 restored")
    T.eq(restoredMachine:get("k2"), "v2", "k2 restored")
    T.eq(restored.log:baseIndex(), original.log:baseIndex(), "base restored")
    T.eq(restored.log:lastIndex(), original.log:lastIndex(), "suffix restored")
    for _: integer = 1, 10 do
      restored:tick()
    end
    T.eq(restored.role, "leader", "restored node can lead")
  end,

  ["automatic snapshots fire at the configured threshold"] = function(): void
    local r: Rig = rig({ "n1" }, fastConfig(10, 3), 43)
    local n1: RaftNodeApi = nodeOf(r, "n1")
    for _: integer = 1, 10 do
      n1:tick()
    end
    T.eq(n1.log:baseIndex(), 0, "no-op alone is below the threshold")
    n1:propose(kv.putCommand("a", "1"))
    T.eq(n1.log:baseIndex(), 0, "two applied entries are below the threshold")
    n1:propose(kv.putCommand("b", "2"))
    T.ok(n1.log:baseIndex() >= 3, "threshold compacted the prefix")
    T.eq(machineOf(r, "n1"):get("b"), "2", "state kept")
  end,

  ["a lagging follower receives an installSnapshot"] = function(): void
    local ids: string[] = { "n1", "n2", "n3" }
    local r: Rig = rig(ids, fastConfig(20, 2), 51)
    elect(r, "n1")
    local n1: RaftNodeApi = nodeOf(r, "n1")
    T.ok(n1:propose(kv.putCommand("base", "0")).ok, "base")
    settle(r, "n1", 4)

    blockAll(r, { "n3" }, { "n1", "n2" })
    for i: integer = 1, 5 do
      T.ok(n1:propose(kv.putCommand("k" .. tostring(i), tostring(i))).ok, "k" .. tostring(i))
    end
    settleOnly(r, "n1", { "n1", "n2" }, 12)
    T.ok(n1.log:baseIndex() >= 2, "leader compacted")
    T.ok(r.transport:sentCount("installSnapshot") == 0, "no snapshot needed yet")

    unblockAll(r, ids)
    settle(r, "n1", 16)
    T.ok(r.transport:sentCount("installSnapshot") > 0, "snapshot installed")
    local n3: RaftNodeApi = nodeOf(r, "n3")
    T.ok(n3.log:baseIndex() >= 2, "follower base advanced")
    for i: integer = 1, 5 do
      local key: string = "k" .. tostring(i)
      T.eq(machineOf(r, "n3"):get(key), tostring(i), key)
    end
    T.eq(machineOf(r, "n3"):get("base"), "0", "base value")
  end,
}
