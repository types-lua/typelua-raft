--- End-to-end Raft demo: elect a leader, replicate a value, isolate the old
--- leader, show that a minority cannot commit, then heal and converge.
---
--- Run:
---   tlua run main.tl
--- or:
---   tlua build && lua dist/main.lua

import clusterMod from "./lib/raft_cluster"
import kv from "./lib/raft_kv"

local function fail(message: string): never
  error("demo: " .. message)
end

local function ids(c: ClusterApi): string[]
  return c:nodeIds()
end

local function show(c: ClusterApi, key: string): void
  local all: string[] = ids(c)
  local parts: string[] = {}
  for i: integer = 1, #all do
    local value: string | nil = c:lookup(all[i], key)
    parts[#parts + 1] = all[i] .. "=" .. tostring(value)
  end
  print("  " .. key .. ": " .. table.concat(parts, "  "))
end

print("== TypeLua Raft demo ==")
local c: ClusterApi = clusterMod.Cluster.new(3, {
  electionTimeout = 8,
  electionJitter = 5,
  heartbeatInterval = 2,
  snapshotThreshold = 8,
  maxBatch = 8,
  latency = 0,
  seed = 42,
})

if not c:stabilize(400) then
  fail("cluster did not elect a stable leader")
end
local leader: string | nil = c:leaderId()
if leader == nil then
  fail("no leader after stabilization")
end
print("leader: " .. leader! .. " (term " .. tostring(c:node(leader!).currentTerm) .. ")")

local hello: string = kv.putCommand("hello", "raft")
local proposal: ClusterProposal = c:propose(hello)
if not proposal.ok or not c:waitCommand(proposal.index, hello, 200) then
  fail("hello was not committed")
end
c:run(10)
print("committed:")
show(c, "hello")

-- Isolate the current leader. It keeps accepting client commands but can no
-- longer reach a majority, so nothing it appends becomes committed.
local rest: string[] = {}
local all: string[] = ids(c)
for i: integer = 1, #all do
  if all[i] ~= leader then
    rest[#rest + 1] = all[i]
  end
end
c:partition({ { leader! }, rest })
c:run(30)

local staleCommand: string = kv.putCommand("stale", "minority")
local stale: ProposeResult = c:node(leader!):propose(staleCommand)
print("minority leader " .. leader! .. " accepted stale write: " .. tostring(stale.ok)
  .. " (index " .. tostring(stale.index) .. ", committed="
  .. tostring(c:node(leader!).commitIndex >= stale.index) .. ")")

local newLeader: string | nil = c:leaderId()
local waited: integer = 0
while (newLeader == nil or newLeader == leader) and waited < 300 do
  c:run(1)
  waited = waited + 1
  newLeader = c:leaderId()
end
if newLeader == nil or newLeader == leader then
  fail("majority did not elect a new leader")
end
print("majority elected: " .. newLeader! .. " (term " .. tostring(c:node(newLeader!).currentTerm) .. ")")

local healthy: string = kv.putCommand("healthy", "majority")
local second: ClusterProposal = c:propose(healthy)
if not second.ok or not c:waitCommand(second.index, healthy, 200) then
  fail("majority write was not committed")
end
c:run(10)
print("committed during partition:")
show(c, "healthy")

c:heal()
if not c:stabilize(500) then
  fail("cluster did not converge after healing")
end
c:run(20)
print("after healing:")
show(c, "hello")
show(c, "healthy")
show(c, "stale")

local violations: string[] = c:checkInvariants()
if #violations ~= 0 then
  fail("invariants violated: " .. table.concat(violations, "; "))
end
print("invariants: OK")
print("== demo complete ==")
