import T from "tlua_test"
import nodeMod from "../lib/raft_node"
import kv from "../lib/raft_kv"
import kit from "../lib/raft_testkit"

type Rig = {
  transport: SimTransport,
  nodes: map<string, RaftNodeApi>,
  machines: map<string, KVView>,
}

local function rig(ids: string[], config: RaftConfig, seed: integer): Rig
  local transport = kit.InMemoryTransport.new()
  local nodes: map<string, RaftNodeApi> = {}
  local machines: map<string, KVView> = {}
  for i: integer = 1, #ids do
    local id: string = ids[i]
    local machine = kv.KVStore.new()
    machines[id] = machine
    nodes[id] = nodeMod.RaftNode.new(
      id,
      ids,
      kit.MemoryStorage.new(),
      transport,
      machine,
      config,
      seed + i * 101
    )
  end
  return { transport = transport, nodes = nodes, machines = machines }
end

local function fastConfig(election: integer): RaftConfig
  return {
    electionTimeout = election,
    electionJitter = 0,
    heartbeatInterval = 1,
    snapshotThreshold = 0,
    maxBatch = 8,
  }
end

local function pumpRound(r: Rig): void
  r.transport:advance(1)
  kit.pump(r.transport, r.nodes, r.transport.now)
end

return {
  ["single node elects itself and commits the no-op"] = function(): void
    local r: Rig = rig({ "n1" }, fastConfig(3), 1)
    local n: RaftNodeApi = r.nodes["n1"]!
    for _: integer = 1, 3 do
      n:tick()
    end
    T.eq(n.role, "leader", "leader")
    T.eq(n.currentTerm, 1, "term")
    T.eq(n.commitIndex, 1, "no-op committed")
    T.eq(n.lastApplied, 1, "no-op applied")

    local first: ProposeResult = n:propose(kv.putCommand("k", "v"))
    T.ok(first.ok, "proposal accepted")
    T.eq(first.index, 2, "index")
    T.eq(n.commitIndex, 2, "committed")
    local machine: KVView = r.machines["n1"]!
    T.eq(machine:get("k"), "v", "applied")
  end,

  ["three nodes elect one leader and replicate the no-op"] = function(): void
    local r: Rig = rig({ "n1", "n2", "n3" }, fastConfig(10), 7)
    local n1: RaftNodeApi = r.nodes["n1"]!
    n1:startElection()
    pumpRound(r) -- requests
    pumpRound(r) -- replies -> leader + appendEntries
    T.eq(n1.role, "leader", "elected")
    T.eq(n1.currentTerm, 1, "term")

    pumpRound(r) -- followers append no-op
    pumpRound(r) -- replies advance commit
    T.eq(n1.commitIndex, 1, "leader committed no-op")
    n1:tick() -- heartbeat carries leaderCommit
    pumpRound(r)
    for i: integer = 1, 3 do
      local id: string = r.nodes["n1"]!.members[i]!
      local n: RaftNodeApi = r.nodes[id]!
      T.eq(n.commitIndex, 1, "commit " .. id)
      T.eq(n.role == "leader", id == "n1", "role " .. id)
    end
  end,

  ["a node votes once per term"] = function(): void
    local transport = kit.InMemoryTransport.new()
    local node = nodeMod.RaftNode.new(
      "n3",
      { "n1", "n2", "n3" },
      kit.MemoryStorage.new(),
      transport,
      kv.KVStore.new(),
      fastConfig(10),
      3
    )
    node:handle("n1", {
      kind = "requestVote", term = 1, lastLogIndex = 0, lastLogTerm = 0,
    })
    node:handle("n2", {
      kind = "requestVote", term = 1, lastLogIndex = 0, lastLogTerm = 0,
    })
    T.eq(node.votedFor, "n1", "first vote sticks")
    transport:advance(1)
    local toN1: Envelope[] = transport:drain("n1", 1)
    local toN2: Envelope[] = transport:drain("n2", 1)
    T.eq(#toN1, 1, "reply to first")
    T.eq(#toN2, 1, "reply to second")
    local first: RequestVoteReply = toN1[1].message as RequestVoteReply
    local second: RequestVoteReply = toN2[1].message as RequestVoteReply
    T.ok(first.granted, "granted")
    T.ok(not second.granted, "rejected")
  end,

  ["stale logs are rejected"] = function(): void
    local storage = kit.MemoryStorage.new()
    storage:saveHardState(5, nil)
    storage:saveLog(0, 0, {
      { term = 5, kind = "command", command = "old" },
    })
    local transport = kit.InMemoryTransport.new()
    local node = nodeMod.RaftNode.new(
      "n2",
      { "n1", "n2" },
      storage,
      transport,
      kv.KVStore.new(),
      fastConfig(10),
      2
    )
    node:handle("n1", {
      kind = "requestVote", term = 6, lastLogIndex = 100, lastLogTerm = 4,
    })
    T.eq(node.currentTerm, 6, "term advanced")
    T.eq(node.votedFor, nil, "vote withheld")
    T.eq(storage:loadTerm(), 6, "term persisted")
    T.eq(storage:loadVotedFor(), nil, "nil vote persisted")

    -- Equal last term but a shorter log is still stale.
    local storage2 = kit.MemoryStorage.new()
    storage2:saveHardState(5, nil)
    storage2:saveLog(0, 0, {
      { term = 5, kind = "command", command = "old" },
    })
    local node2 = nodeMod.RaftNode.new(
      "n2",
      { "n1", "n2" },
      storage2,
      kit.InMemoryTransport.new(),
      kv.KVStore.new(),
      fastConfig(10),
      2
    )
    node2:handle("n1", {
      kind = "requestVote", term = 6, lastLogIndex = 0, lastLogTerm = 5,
    })
    T.eq(node2.votedFor, nil, "shorter log rejected")

    node2:handle("n1", {
      kind = "requestVote", term = 6, lastLogIndex = 1, lastLogTerm = 5,
    })
    T.eq(node2.votedFor, "n1", "up-to-date log granted")
  end,

  ["appendEntries returns a conflict hint and rejects forged duplicates"] = function(): void
    local storage = kit.MemoryStorage.new()
    storage:saveHardState(3, nil)
    storage:saveLog(0, 0, {
      { term = 1, kind = "command", command = "a" },
      { term = 1, kind = "command", command = "b" },
      { term = 2, kind = "command", command = "c" },
      { term = 3, kind = "command", command = "d" },
    })
    local transport = kit.InMemoryTransport.new()
    local node = nodeMod.RaftNode.new(
      "n2",
      { "n1", "n2" },
      storage,
      transport,
      kv.KVStore.new(),
      fastConfig(10),
      8
    )
    node:handle("n1", {
      kind = "appendEntries",
      term = 3,
      prevLogIndex = 3,
      prevLogTerm = 1,
      entries = {},
      leaderCommit = 0,
    })
    transport:advance(1)
    local ready: Envelope[] = transport:drain("n1", 1)
    T.eq(#ready, 1, "reply")
    local reply: AppendEntriesReply = ready[1].message as AppendEntriesReply
    T.ok(not reply.success, "rejected")
    T.eq(reply.conflictIndex, 3, "first index of the conflicting term")

    local err: string = T.raises(function(): void
      node:handle("n1", {
        kind = "appendEntries",
        term = 3,
        prevLogIndex = 0,
        prevLogTerm = 0,
        entries = {
          { term = 1, kind = "command", command = "forged" },
        },
        leaderCommit = 0,
      })
    end)
    T.ok(string.find(err, "same index/term", 1, true) ~= nil, "invariant error")
  end,

  ["higher term makes the leader step down"] = function(): void
    local r: Rig = rig({ "n1", "n2", "n3" }, fastConfig(10), 11)
    local n1: RaftNodeApi = r.nodes["n1"]!
    n1:startElection()
    pumpRound(r)
    pumpRound(r)
    T.eq(n1.role, "leader", "leader first")
    n1:handle("n2", {
      kind = "appendEntries",
      term = 2,
      prevLogIndex = 0,
      prevLogTerm = 0,
      entries = {},
      leaderCommit = 0,
    })
    T.eq(n1.role, "follower", "stepped down")
    T.eq(n1.currentTerm, 2, "term updated")
    T.eq(n1.leaderId, "n2", "leader id")
    T.eq(n1.votedFor, nil, "vote cleared")
  end,

  ["same-term appendEntries makes a candidate follow"] = function(): void
    local r: Rig = rig({ "n1", "n2", "n3" }, fastConfig(10), 12)
    local n1: RaftNodeApi = r.nodes["n1"]!
    n1:startElection()
    T.eq(n1.role, "candidate", "candidate")
    n1:handle("n2", {
      kind = "appendEntries",
      term = 1,
      prevLogIndex = 0,
      prevLogTerm = 0,
      entries = {},
      leaderCommit = 0,
    })
    T.eq(n1.role, "follower", "follower")
    T.eq(n1.currentTerm, 1, "term kept")
    T.eq(n1.leaderId, "n2", "leader id")
  end,

  ["term, vote and log survive a restart"] = function(): void
    local storage = kit.MemoryStorage.new()
    local transport = kit.InMemoryTransport.new()
    local first = nodeMod.RaftNode.new(
      "n1",
      { "n1", "n2" },
      storage,
      transport,
      kv.KVStore.new(),
      fastConfig(10),
      5
    )
    first:startElection()
    T.eq(first.currentTerm, 1, "term")
    T.eq(first.votedFor, "n1", "vote")

    local restarted = nodeMod.RaftNode.new(
      "n1",
      { "n1", "n2" },
      storage,
      transport,
      kv.KVStore.new(),
      fastConfig(10),
      5
    )
    T.eq(restarted.currentTerm, 1, "term reloaded")
    T.eq(restarted.votedFor, "n1", "vote reloaded")
    T.eq(restarted.role, "follower", "volatile role resets")

    restarted:handle("n2", {
      kind = "appendEntries",
      term = 1,
      prevLogIndex = 0,
      prevLogTerm = 0,
      entries = {
        { term = 1, kind = "command", command = "persisted" },
      },
      leaderCommit = 0,
    })
    local again = nodeMod.RaftNode.new(
      "n1",
      { "n1", "n2" },
      storage,
      transport,
      kv.KVStore.new(),
      fastConfig(10),
      5
    )
    T.eq(again.log:lastIndex(), 1, "entry reloaded")
    T.eq(again.log:entryAt(1).command, "persisted", "command reloaded")
  end,

  ["hard state and log are persisted before replies"] = function(): void
    local trace: string[] = {}
    local storage = kit.RecordingStorage.new(kit.MemoryStorage.new(), trace)
    local transport = kit.TraceTransport.new(kit.InMemoryTransport.new(), trace)
    local node = nodeMod.RaftNode.new(
      "n1",
      { "n1", "n2" },
      storage,
      transport,
      kv.KVStore.new(),
      fastConfig(10),
      6
    )

    node:handle("n2", {
      kind = "requestVote", term = 2, lastLogIndex = 0, lastLogTerm = 0,
    })
    local saveAt: integer = kit.traceIndex(trace, "saveHardState")
    local replyAt: integer = kit.traceIndex(trace, "send:requestVoteReply")
    T.ok(saveAt > 0, "hard state saved")
    T.ok(replyAt > 0, "reply sent")
    T.ok(saveAt < replyAt, "save before vote reply")

    node:handle("n2", {
      kind = "appendEntries",
      term = 2,
      prevLogIndex = 0,
      prevLogTerm = 0,
      entries = {
        { term = 2, kind = "command", command = "x" },
      },
      leaderCommit = 0,
    })
    local logAt: integer = kit.traceIndex(trace, "saveLog")
    local appendReplyAt: integer = kit.traceIndex(trace, "send:appendEntriesReply")
    T.ok(logAt > 0, "log saved")
    T.ok(appendReplyAt > 0, "append reply sent")
    T.ok(logAt < appendReplyAt, "save before append reply")
  end,
}
