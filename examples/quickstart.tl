--- Minimal Raft quickstart: elect a leader, commit a value, read it back.
--- Run with `tlua example quickstart`.

import clusterMod from "../lib/raft_cluster"
import kv from "../lib/raft_kv"

local c: ClusterApi = clusterMod.Cluster.new(3, {
  electionTimeout = 8,
  electionJitter = 5,
  heartbeatInterval = 2,
  snapshotThreshold = 16,
  seed = 7,
})

if not c:stabilize(400) then
  error("quickstart: no stable leader")
end
local leader: string | nil = c:leaderId()
if leader == nil then
  error("quickstart: no leader")
end
print("leader:", leader!)

local command: string = kv.putCommand("hello", "raft")
local proposal: ClusterProposal = c:propose(command)
if not proposal.ok or not c:waitCommand(proposal.index, command, 200) then
  error("quickstart: value was not committed")
end
c:run(10)

local ids: string[] = c:nodeIds()
for i: integer = 1, #ids do
  print(ids[i] .. " -> " .. tostring(c:lookup(ids[i], "hello")))
end
