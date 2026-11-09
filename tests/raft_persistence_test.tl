import T from "tlua_test"
import nodeMod from "../lib/raft_node"
import kv from "../lib/raft_kv"
import storageMod from "../lib/raft_storage"
import transportMod from "../lib/raft_transport"

local function freshPath(): string
  local path: string = os.tmpname()
  os.remove(path)
  return path
end

local function config(): RaftConfig
  return {
    electionTimeout = 5,
    electionJitter = 0,
    heartbeatInterval = 1,
    snapshotThreshold = 0,
    maxBatch = 8,
  }
end

return {
  ["a node restarts from FileStorage with its term, log and snapshot"] = function(): void
    local path: string = freshPath()
    local storage = storageMod.FileStorage.new(path)
    local transport = transportMod.InMemoryTransport.new()
    local machine = kv.KVStore.new()
    local n1 = nodeMod.RaftNode.new("n1", { "n1" }, storage, transport, machine, config(), 71)
    for _: integer = 1, 10 do
      n1:tick()
    end
    T.eq(n1.role, "leader", "leader")
    T.ok(n1:propose(kv.putCommand("a", "1")).ok, "a")
    T.ok(n1:propose(kv.putCommand("b", "2")).ok, "b")
    n1:snapshotNow()
    T.ok(storage:loadSnapshot() ~= nil, "snapshot on disk")
    T.ok(n1.log:baseIndex() > 0, "compacted")

    local file = io.open(path, "rb")
    T.ok(file ~= nil, "state file exists")
    if file ~= nil then
      local bytes: string | nil = file:read("*a")
      file:close()
      T.ok(bytes ~= nil and #bytes! > 0, "state file is not empty")
    end

    local reopened = storageMod.FileStorage.new(path)
    local restoredMachine = kv.KVStore.new()
    local restored = nodeMod.RaftNode.new(
      "n1", { "n1" }, reopened, transport, restoredMachine, config(), 71
    )
    T.eq(restored.currentTerm, n1.currentTerm, "term")
    T.eq(restored.votedFor, n1.votedFor, "vote")
    T.eq(restored.commitIndex, n1.commitIndex, "commit")
    T.eq(restored.lastApplied, n1.lastApplied, "applied")
    T.eq(restored.log:baseIndex(), n1.log:baseIndex(), "base")
    T.eq(restoredMachine:get("a"), "1", "a restored")
    T.eq(restoredMachine:get("b"), "2", "b restored")

    for _: integer = 1, 10 do
      restored:tick()
    end
    T.eq(restored.role, "leader", "restored node leads")
    T.ok(restored:propose(kv.putCommand("c", "3")).ok, "c after restart")
    T.eq(restoredMachine:get("c"), "3", "c applied")
    os.remove(path)
  end,
}
