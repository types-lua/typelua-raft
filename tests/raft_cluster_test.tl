import T from "tlua_test"
import clusterMod from "../lib/raft_cluster"
import kv from "../lib/raft_kv"
import random from "../lib/raft_random"

local function newCluster(size: integer, seed: integer): ClusterApi
  return clusterMod.Cluster.new(size, {
    electionTimeout = 6,
    electionJitter = 4,
    heartbeatInterval = 1,
    snapshotThreshold = 0,
    maxBatch = 8,
    latency = 0,
    seed = seed,
  })
end

local function idsOf(c: ClusterApi): string[]
  return c:nodeIds()
end

local function isolate(c: ClusterApi, leader: string): string[][]
  local ids: string[] = c:nodeIds()
  local rest: string[] = {}
  for i: integer = 1, #ids do
    if ids[i] ~= leader then
      rest[#rest + 1] = ids[i]
    end
  end
  return { { leader }, rest }
end

local function randomGroups(c: ClusterApi, rng: RngApi): string[][]
  local ids: string[] = c:nodeIds()
  local groups: string[][] = { {}, {} }
  groups[1][1] = ids[1]
  for i: integer = 2, #ids - 1 do
    local side: integer = rng:int(2)
    groups[side][#groups[side] + 1] = ids[i]
  end
  groups[2][#groups[2] + 1] = ids[#ids]
  return groups
end

local function expectAll(c: ClusterApi, key: string, value: string, where: string): void
  local ids: string[] = c:nodeIds()
  for i: integer = 1, #ids do
    T.eq(c:lookup(ids[i], key), value, where .. " on " .. ids[i])
  end
end

local function anyValue(c: ClusterApi, key: string, value: string): boolean
  local ids: string[] = c:nodeIds()
  for i: integer = 1, #ids do
    if c:lookup(ids[i], key) == value then
      return true
    end
  end
  return false
end

local function hasCommand(entries: LogEntry[], command: string): boolean
  for i: integer = 1, #entries do
    if entries[i].command == command then
      return true
    end
  end
  return false
end

return {
  ["a healthy cluster elects, replicates and stays invariant"] = function(): void
    local c: ClusterApi = newCluster(3, 101)
    T.ok(c:stabilize(300), "stabilized")
    local leader: string | nil = c:leaderId()
    T.ok(leader ~= nil, "has leader")
    T.eq(#c:leaders(), 1, "one leader")

    local command: string = kv.putCommand("hello", "raft")
    local proposal: ClusterProposal = c:propose(command)
    T.ok(proposal.ok, "proposal accepted")
    T.ok(c:waitCommand(proposal.index, command, 100), "committed")
    c:run(10)
    expectAll(c, "hello", "raft", "value")
    T.eq(#c:checkInvariants(), 0, "invariants")
  end,

  ["a minority leader cannot commit and is truncated after healing"] = function(): void
    local c: ClusterApi = newCluster(3, 202)
    T.ok(c:stabilize(300), "stabilized")
    local baseline: string = kv.putCommand("base", "0")
    local first: ClusterProposal = c:propose(baseline)
    T.ok(c:waitCommand(first.index, baseline, 100), "baseline committed")
    c:run(10)

    local oldLeader: string = c:leaderId()!
    c:partition(isolate(c, oldLeader))
    c:run(20)

    local newLeader: string | nil = c:leaderId()
    local waited: integer = 0
    while (newLeader == nil or newLeader == oldLeader) and waited < 200 do
      c:run(1)
      waited = waited + 1
      newLeader = c:leaderId()
    end
    T.ok(newLeader ~= nil, "majority elected")
    T.ok(newLeader != oldLeader, "new leader in a higher term")

    local oldNode: RaftNodeApi = c:node(oldLeader)
    local staleCommand: string = kv.putCommand("stale", "x")
    local stale: ProposeResult = oldNode:propose(staleCommand)
    T.ok(stale.ok, "stale leader accepts locally")
    T.ok(not c:waitCommand(stale.index, staleCommand, 40), "minority cannot commit")

    local freshCommand: string = kv.putCommand("fresh", "y")
    local fresh: ClusterProposal = c:propose(freshCommand)
    T.ok(fresh.ok, "majority proposal")
    T.ok(c:waitCommand(fresh.index, freshCommand, 100), "majority commits")
    c:run(10)
    T.eq(c:lookup(newLeader!, "fresh"), "y", "new leader applied")

    c:heal()
    T.ok(c:stabilize(400), "healed and stabilized")
    c:run(20)
    expectAll(c, "fresh", "y", "fresh value")
    expectAll(c, "base", "0", "base value")
    for i: integer = 1, #idsOf(c) do
      local id: string = idsOf(c)[i]
      T.eq(c:lookup(id, "stale"), nil, "stale value absent on " .. id)
    end
    T.ok(not hasCommand(c:node(oldLeader).log:allEntries(), staleCommand), "stale entry truncated")
    T.eq(#c:checkInvariants(), 0, "invariants after healing")
  end,

  ["random partitions preserve election and log safety"] = function(): void
    local c: ClusterApi = newCluster(5, 303)
    T.ok(c:stabilize(400), "initial stabilization")
    local rng = random.Rng.new(2026)
    local expected: map<string, string> = {}

    for round: integer = 1, 12 do
      c:partition(randomGroups(c, rng))
      c:run(15)

      for attempt: integer = 1, 2 do
        local key: string = "r" .. tostring(round) .. "-" .. tostring(attempt)
        local value: string = "v" .. tostring(round) .. "-" .. tostring(attempt)
        local command: string = kv.putCommand(key, value)
        local proposal: ClusterProposal = c:propose(command)
        if not proposal.ok then
          c:run(10)
          proposal = c:propose(command)
        end
        if proposal.ok and c:waitCommand(proposal.index, command, 80) then
          expected[key] = value
        end
      end

      local violations: string[] = c:checkInvariants()
      T.eq(#violations, 0, "partition invariants round " .. tostring(round)
        .. ": " .. table.concat(violations, "; "))

      c:heal()
      c:run(20)
      violations = c:checkInvariants()
      T.eq(#violations, 0, "healed invariants round " .. tostring(round)
        .. ": " .. table.concat(violations, "; "))
    end

    T.ok(c:stabilize(600), "final stabilization")
    c:run(20)
    for key, value in pairs(expected) do
      expectAll(c, key, value, "committed " .. key)
    end
    T.eq(#c:checkInvariants(), 0, "final invariants")
  end,

  ["snapshots survive partitions and catch-up"] = function(): void
    local c: ClusterApi = clusterMod.Cluster.new(5, {
      electionTimeout = 6,
      electionJitter = 4,
      heartbeatInterval = 1,
      snapshotThreshold = 3,
      maxBatch = 4,
      latency = 0,
      seed = 404,
    })
    T.ok(c:stabilize(400), "initial stabilization")
    local rng = random.Rng.new(9090)
    local expected: map<string, string> = {}

    for round: integer = 1, 8 do
      c:partition(randomGroups(c, rng))
      c:run(15)
      for attempt: integer = 1, 2 do
        local key: string = "s" .. tostring(round) .. "-" .. tostring(attempt)
        local value: string = "w" .. tostring(round) .. "-" .. tostring(attempt)
        local command: string = kv.putCommand(key, value)
        local proposal: ClusterProposal = c:propose(command)
        if not proposal.ok then
          c:run(10)
          proposal = c:propose(command)
        end
        if proposal.ok then
          -- waitCommand cannot see compacted indexes, so observe the value in
          -- any state machine instead; a state machine only applies commits.
          for _: integer = 1, 30 do
            if anyValue(c, key, value) then
              break
            end
            c:tick()
          end
          if anyValue(c, key, value) then
            expected[key] = value
          end
        end
      end
      local violations: string[] = c:checkInvariants()
      T.eq(#violations, 0, "snapshot partition invariants round " .. tostring(round)
        .. ": " .. table.concat(violations, "; "))
      c:heal()
      c:run(25)
      violations = c:checkInvariants()
      T.eq(#violations, 0, "snapshot healed invariants round " .. tostring(round)
        .. ": " .. table.concat(violations, "; "))
    end

    T.ok(c:stabilize(800), "final stabilization with snapshots")
    c:run(30)
    for key, value in pairs(expected) do
      expectAll(c, key, value, "snapshot-committed " .. key)
    end
    T.eq(#c:checkInvariants(), 0, "snapshot final invariants")
    T.ok(c:sentCount("installSnapshot") > 0, "installSnapshot exercised")
  end,
}
