--- The Raft consensus state machine.
---
--- This implementation follows the Raft paper (Ongaro & Ousterhout) with the
--- standard safety rules:
---
---   * one vote per term, granted only to an at-least-as-up-to-date log;
---   * a leader commits only entries from its own term by counting replicas
---     (Figure 8), then applies the whole prefix in order;
---   * term/vote and log mutations are persisted before any dependent reply;
---   * conflicting suffixes are truncated by the AppendEntries consistency
---     check, with conflict hints to avoid O(log length) retries;
---   * log compaction uses InstallSnapshot when a follower falls behind the
---     leader's base index.
---
--- The node is driven by explicit `tick()` calls and an injected Transport.
--- There is no wall clock and no hidden concurrency: a cluster simulator can
--- therefore reproduce every election and partition exactly.

import logmod from "./raft_log"
import randmod from "./raft_random"

local DEFAULT_ELECTION: integer = 10
local DEFAULT_JITTER: integer = 5
local DEFAULT_HEARTBEAT: integer = 2
local DEFAULT_SNAPSHOT: integer = 64
local DEFAULT_BATCH: integer = 8

--- floor(size / 2) + 1 without relying on `//` (Lua 5.1 has no floor div).
local function majorityOf(size: integer): integer
  local half: integer = 0
  while half * 2 + 1 < size do
    half = half + 1
  end
  return half + 1
end

local function copyMembers(members: string[]): string[]
  local out: string[] = {}
  for i: integer = 1, #members do
    out[i] = members[i]
  end
  return out
end

class RaftNode implements RaftNodeApi {
  id: string,
  members: string[] = {},
  memberSet: map<string, boolean> = {},
  majority: integer = 1,
  role: RaftRole = "follower",
  currentTerm: integer = 0,
  votedFor: string | nil = nil,
  commitIndex: integer = 0,
  lastApplied: integer = 0,
  leaderId: string | nil = nil,
  log: RaftLogView,
  machine: StateMachine,
  storage: Storage,
  transport: Transport,
  rng: RngApi,

  baseElection: integer = DEFAULT_ELECTION,
  electionJitter: integer = DEFAULT_JITTER,
  heartbeatInterval: integer = DEFAULT_HEARTBEAT,
  snapshotThreshold: integer = DEFAULT_SNAPSHOT,
  maxBatch: integer = DEFAULT_BATCH,

  electionElapsed: integer = 0,
  electionTimeout: integer = DEFAULT_ELECTION,
  heartbeatElapsed: integer = 0,

  votes: map<string, boolean> = {},
  nextIndex: map<string, integer> = {},
  matchIndex: map<string, integer> = {},

  constructor(
    id: string,
    members: string[],
    storage: Storage,
    transport: Transport,
    machine: StateMachine,
    config: RaftConfig,
    seed: integer
  )
    if id == "" then
      error("RaftNode: empty id")
    end
    if #members == 0 then
      error("RaftNode: empty membership")
    end
    self.id = id
    self.members = copyMembers(members)
    self.storage = storage
    self.transport = transport
    self.machine = machine
    self.rng = randmod.Rng.new(seed)

    local seen: map<string, boolean> = {}
    local found: boolean = false
    for i: integer = 1, #self.members do
      local member: string = self.members[i]
      if member == "" then
        error("RaftNode: empty member id")
      end
      if seen[member] == true then
        error("RaftNode: duplicate member '" .. member .. "'")
      end
      seen[member] = true
      self.memberSet[member] = true
      if member == id then
        found = true
      end
    end
    if not found then
      error("RaftNode: id '" .. id .. "' is not in members")
    end
    self.majority = majorityOf(#self.members)

    local et: integer | nil = config.electionTimeout
    if et ~= nil then
      self.baseElection = et!
    end
    local jitter: integer | nil = config.electionJitter
    if jitter ~= nil then
      self.electionJitter = jitter!
    end
    local hb: integer | nil = config.heartbeatInterval
    if hb ~= nil then
      self.heartbeatInterval = hb!
    end
    local snap: integer | nil = config.snapshotThreshold
    if snap ~= nil then
      self.snapshotThreshold = snap!
    end
    local batch: integer | nil = config.maxBatch
    if batch ~= nil then
      self.maxBatch = batch!
    end
    if self.baseElection < 1 then
      error("RaftNode: electionTimeout must be >= 1")
    end
    if self.electionJitter < 0 then
      error("RaftNode: electionJitter must be >= 0")
    end
    if self.heartbeatInterval < 1 then
      error("RaftNode: heartbeatInterval must be >= 1")
    end
    if self.snapshotThreshold < 0 then
      error("RaftNode: snapshotThreshold must be >= 0")
    end
    if self.maxBatch < 1 then
      error("RaftNode: maxBatch must be >= 1")
    end

    self.currentTerm = storage:loadTerm()
    self.votedFor = storage:loadVotedFor()
    self.log = logmod.RaftLog.new(
      storage:loadLogBase(),
      storage:loadLogBaseTerm(),
      storage:loadLogEntries()
    )
    if self.currentTerm < 0 then
      error("RaftNode: negative persisted term")
    end

    local image: Snapshot | nil = storage:loadSnapshot()
    if image ~= nil then
      if image.index < self.log:baseIndex() then
        error("RaftNode: snapshot is older than the compacted log base")
      end
      if image.index == self.log:baseIndex() then
        if image.term ~= self.log:baseTerm() then
          error("RaftNode: snapshot/base term mismatch at index " .. tostring(image.index))
        end
      elseif image.index > self.log:baseIndex() then
        if self.log:contains(image.index) and self.log:termAt(image.index) == image.term then
          self.log:compact(image.index)
        else
          self.log:reset(image.index, image.term)
        end
      end
      self.machine:restore(image.data)
      self.commitIndex = image.index
      self.lastApplied = image.index
    end

    self.role = "follower"
    self.leaderId = nil
    self:resetElectionTimer()
  end,

  -- -----------------------------------------------------------------------
  -- Timers and elections
  -- -----------------------------------------------------------------------

  resetElectionTimer(): void
    self.electionElapsed = 0
    self.electionTimeout = self.baseElection + self.rng:int(self.electionJitter)
  end,

  tick(): void
    if self.role == "leader" then
      self.heartbeatElapsed = self.heartbeatElapsed + 1
      if self.heartbeatElapsed >= self.heartbeatInterval then
        self.heartbeatElapsed = 0
        self:broadcastAppend()
      end
      return
    end
    self.electionElapsed = self.electionElapsed + 1
    if self.electionElapsed >= self.electionTimeout then
      self:startElection()
    end
  end,

  startElection(): void
    if self.role == "leader" then
      return
    end
    self.role = "candidate"
    self.currentTerm = self.currentTerm + 1
    self.votedFor = self.id
    self.leaderId = nil
    self.heartbeatElapsed = 0
    self.votes = {}
    self.votes[self.id] = true
    self:resetElectionTimer()
    self:persistHardState()
    if self:voteCount() >= self.majority then
      self:becomeLeader()
      return
    end
    local request: RequestVote = {
      kind = "requestVote",
      term = self.currentTerm,
      lastLogIndex = self.log:lastIndex(),
      lastLogTerm = self.log:lastTerm(),
    }
    for i: integer = 1, #self.members do
      local peer: string = self.members[i]
      if peer ~= self.id then
        self.transport:send(self.id, peer, request)
      end
    end
  end,

  isLogUpToDate(candidateIndex: integer, candidateTerm: integer): boolean
    local myTerm: integer = self.log:lastTerm()
    if candidateTerm ~= myTerm then
      return candidateTerm > myTerm
    end
    return candidateIndex >= self.log:lastIndex()
  end,

  becomeLeader(): void
    self.role = "leader"
    self.leaderId = self.id
    self.heartbeatElapsed = 0
    self.nextIndex = {}
    self.matchIndex = {}
    for i: integer = 1, #self.members do
      local peer: string = self.members[i]
      if peer ~= self.id then
        self.nextIndex[peer] = self.log:lastIndex() + 1
        self.matchIndex[peer] = 0
      end
    end
    -- Committing a no-op from the new term is what makes entries from older
    -- terms safe to commit: the current-term entry carries the prefix with it.
    self.log:append({ { term = self.currentTerm, kind = "noop", command = "" } })
    self:persistLog()
    self:broadcastAppend()
    self:advanceCommit()
  end,

  becomeFollower(term: integer, leader: string | nil): void
    local changed: boolean = false
    if term > self.currentTerm then
      self.currentTerm = term
      self.votedFor = nil
      changed = true
    end
    self.role = "follower"
    self.leaderId = leader
    self.electionElapsed = 0
    self:resetElectionTimer()
    if changed then
      self:persistHardState()
    end
  end,

  voteCount(): integer
    local count: integer = 0
    for _, granted in pairs(self.votes) do
      if granted then
        count = count + 1
      end
    end
    return count
  end,

  -- -----------------------------------------------------------------------
  -- Message dispatch
  -- -----------------------------------------------------------------------

  handle(from: string, message: RaftMessage): void
    if from == self.id or not self:isMember(from) then
      return
    end
    if message.kind == "requestVote" then
      self:onRequestVote(from, message as RequestVote)
    elseif message.kind == "requestVoteReply" then
      self:onRequestVoteReply(from, message as RequestVoteReply)
    elseif message.kind == "appendEntries" then
      self:onAppendEntries(from, message as AppendEntries)
    elseif message.kind == "appendEntriesReply" then
      self:onAppendEntriesReply(from, message as AppendEntriesReply)
    elseif message.kind == "installSnapshot" then
      self:onInstallSnapshot(from, message as InstallSnapshot)
    elseif message.kind == "installSnapshotReply" then
      self:onInstallSnapshotReply(from, message as InstallSnapshotReply)
    end
  end,

  isMember(candidate: string): boolean
    return self.memberSet[candidate] == true
  end,

  onRequestVote(from: string, message: RequestVote): void
    if message.term < self.currentTerm then
      self:replyVote(from, false)
      return
    end
    local termChanged: boolean = false
    if message.term > self.currentTerm then
      self.currentTerm = message.term
      self.votedFor = nil
      self.role = "follower"
      self.leaderId = nil
      termChanged = true
    end
    local granted: boolean = false
    if self.votedFor == nil or self.votedFor == from then
      if self:isLogUpToDate(message.lastLogIndex, message.lastLogTerm) then
        granted = true
        self.votedFor = from
        self.leaderId = nil
      end
    end
    if termChanged or granted then
      self:persistHardState()
    end
    if granted then
      self:resetElectionTimer()
    end
    self:replyVote(from, granted)
  end,

  replyVote(to: string, granted: boolean): void
    local reply: RequestVoteReply = {
      kind = "requestVoteReply",
      term = self.currentTerm,
      granted = granted,
    }
    self.transport:send(self.id, to, reply)
  end,

  onRequestVoteReply(from: string, message: RequestVoteReply): void
    if message.term > self.currentTerm then
      self:becomeFollower(message.term, nil)
      return
    end
    if message.term < self.currentTerm or self.role ~= "candidate" then
      return
    end
    if not message.granted then
      return
    end
    self.votes[from] = true
    if self:voteCount() >= self.majority then
      self:becomeLeader()
    end
  end,

  onAppendEntries(from: string, message: AppendEntries): void
    if message.term < self.currentTerm then
      self:replyAppend(from, false, 0, self.log:lastIndex() + 1)
      return
    end
    local termChanged: boolean = false
    if message.term > self.currentTerm then
      self.currentTerm = message.term
      self.votedFor = nil
      termChanged = true
    end
    self.role = "follower"
    self.leaderId = from
    self:resetElectionTimer()
    if termChanged then
      self:persistHardState()
    end

    if message.prevLogIndex > self.log:lastIndex() then
      self:replyAppend(from, false, 0, self.log:lastIndex() + 1)
      return
    end
    if message.prevLogIndex < self.log:baseIndex() then
      self:replyAppend(from, false, 0, self.log:baseIndex() + 1)
      return
    end
    if self.log:termAt(message.prevLogIndex) ~= message.prevLogTerm then
      local conflictTerm: integer = self.log:termAt(message.prevLogIndex)
      local conflictIndex: integer = message.prevLogIndex
      while conflictIndex > self.log:baseIndex() + 1 do
        if self.log:termAt(conflictIndex - 1) ~= conflictTerm then
          break
        end
        conflictIndex = conflictIndex - 1
      end
      self:replyAppend(from, false, 0, conflictIndex)
      return
    end

    local changed: boolean = false
    for i: integer = 1, #message.entries do
      local index: integer = message.prevLogIndex + i
      local incoming: LogEntry = message.entries[i]
      if index <= self.log:lastIndex() then
        local existingTerm: integer = self.log:termAt(index)
        if existingTerm ~= incoming.term then
          self.log:truncateFrom(index)
          self.log:append({ incoming })
          changed = true
        else
          local existing: LogEntry = self.log:entryAt(index)
          if existing.kind ~= incoming.kind or existing.command ~= incoming.command then
            error("RaftNode: same index/term but different entry")
          end
        end
      else
        self.log:append({ incoming })
        changed = true
      end
    end
    if changed then
      self:persistLog()
    end

    local lastNew: integer = message.prevLogIndex + #message.entries
    if message.leaderCommit > self.commitIndex then
      local target: integer = math.min(message.leaderCommit, lastNew)
      if target > self.commitIndex then
        self.commitIndex = target
        self:applyCommitted()
      end
    end
    self:replyAppend(from, true, lastNew, 0)
  end,

  replyAppend(to: string, success: boolean, matchIndex: integer, conflictIndex: integer): void
    local reply: AppendEntriesReply = {
      kind = "appendEntriesReply",
      term = self.currentTerm,
      success = success,
      matchIndex = matchIndex,
      conflictIndex = conflictIndex,
    }
    self.transport:send(self.id, to, reply)
  end,

  onAppendEntriesReply(from: string, message: AppendEntriesReply): void
    if message.term > self.currentTerm then
      self:becomeFollower(message.term, nil)
      return
    end
    if message.term < self.currentTerm or self.role ~= "leader" then
      return
    end
    if message.success then
      local matched: integer = math.min(message.matchIndex, self.log:lastIndex())
      if matched > self:matchOf(from) then
        self.matchIndex[from] = matched
        self.nextIndex[from] = matched + 1
      end
      self:advanceCommit()
      if self:nextOf(from) <= self.log:lastIndex() then
        self:sendAppend(from)
      end
      return
    end

    local next: integer = message.conflictIndex
    if next <= self.log:baseIndex() then
      self:sendSnapshot(from)
      return
    end
    if next < 1 then
      next = 1
    end
    local current: integer = self:nextOf(from)
    if next < current then
      self.nextIndex[from] = next
    elseif current > self.log:baseIndex() + 1 then
      self.nextIndex[from] = current - 1
    else
      -- A failure that would move nextIndex at or below the compacted base
      -- carries no useful backtrack (often it is a stale reply to a request
      -- sent before a successful append). Wait for the next heartbeat rather
      -- than resending the same probe forever.
      return
    end
    self:sendAppend(from)
  end,

  -- -----------------------------------------------------------------------
  -- Replication
  -- -----------------------------------------------------------------------

  broadcastAppend(): void
    for i: integer = 1, #self.members do
      local peer: string = self.members[i]
      if peer ~= self.id then
        self:sendAppend(peer)
      end
    end
  end,

  matchOf(peer: string): integer
    local value: integer | nil = self.matchIndex[peer]
    if value == nil then
      return 0
    end
    return value!
  end,

  nextOf(peer: string): integer
    local existing: integer | nil = self.nextIndex[peer]
    if existing ~= nil then
      return existing!
    end
    local created: integer = self.log:lastIndex() + 1
    self.nextIndex[peer] = created
    return created
  end,

  sendAppend(peer: string): void
    local next: integer = self:nextOf(peer)
    if next > self.log:lastIndex() + 1 then
      next = self.log:lastIndex() + 1
      self.nextIndex[peer] = next
    end
    if next <= self.log:baseIndex() then
      self:sendSnapshot(peer)
      return
    end
    local prev: integer = next - 1
    if prev < self.log:baseIndex() then
      self:sendSnapshot(peer)
      return
    end
    local message: AppendEntries = {
      kind = "appendEntries",
      term = self.currentTerm,
      prevLogIndex = prev,
      prevLogTerm = self.log:termAt(prev),
      entries = self.log:entriesFrom(next, self.maxBatch),
      leaderCommit = self.commitIndex,
    }
    self.transport:send(self.id, peer, message)
  end,

  sendSnapshot(peer: string): void
    local image: Snapshot | nil = self.storage:loadSnapshot()
    if image == nil then
      error("RaftNode: missing snapshot while peer " .. peer .. " is behind")
    end
    local message: InstallSnapshot = {
      kind = "installSnapshot",
      term = self.currentTerm,
      lastIncludedIndex = image!.index,
      lastIncludedTerm = image!.term,
      data = image!.data,
    }
    self.transport:send(self.id, peer, message)
  end,

  onInstallSnapshot(from: string, message: InstallSnapshot): void
    if message.term < self.currentTerm then
      self:replySnapshot(from, false, message.lastIncludedIndex)
      return
    end
    local termChanged: boolean = false
    if message.term > self.currentTerm then
      self.currentTerm = message.term
      self.votedFor = nil
      termChanged = true
    end
    self.role = "follower"
    self.leaderId = from
    self:resetElectionTimer()
    if termChanged then
      self:persistHardState()
    end

    if message.lastIncludedIndex <= self.commitIndex
      or message.lastIncludedIndex <= self.log:baseIndex()
    then
      self:replySnapshot(from, true, message.lastIncludedIndex)
      return
    end

    self.machine:restore(message.data)
    local image: Snapshot = {
      index = message.lastIncludedIndex,
      term = message.lastIncludedTerm,
      data = message.data,
    }
    self.storage:saveSnapshot(image)
    if self.log:contains(message.lastIncludedIndex)
      and self.log:termAt(message.lastIncludedIndex) == message.lastIncludedTerm
    then
      self.log:compact(message.lastIncludedIndex)
    else
      self.log:reset(message.lastIncludedIndex, message.lastIncludedTerm)
    end
    self:persistLog()
    self.commitIndex = message.lastIncludedIndex
    self.lastApplied = message.lastIncludedIndex
    self:replySnapshot(from, true, message.lastIncludedIndex)
  end,

  replySnapshot(to: string, success: boolean, lastIncludedIndex: integer): void
    local reply: InstallSnapshotReply = {
      kind = "installSnapshotReply",
      term = self.currentTerm,
      success = success,
      lastIncludedIndex = lastIncludedIndex,
    }
    self.transport:send(self.id, to, reply)
  end,

  onInstallSnapshotReply(from: string, message: InstallSnapshotReply): void
    if message.term > self.currentTerm then
      self:becomeFollower(message.term, nil)
      return
    end
    if message.term < self.currentTerm or self.role ~= "leader" then
      return
    end
    if not message.success then
      return
    end
    if message.lastIncludedIndex > self:matchOf(from) then
      self.matchIndex[from] = message.lastIncludedIndex
    end
    local next: integer = message.lastIncludedIndex + 1
    if next > self:nextOf(from) then
      self.nextIndex[from] = next
    end
    self:advanceCommit()
    if self:nextOf(from) <= self.log:lastIndex() then
      self:sendAppend(from)
    end
  end,

  -- -----------------------------------------------------------------------
  -- Commit, apply, snapshot
  -- -----------------------------------------------------------------------

  advanceCommit(): void
    if self.role ~= "leader" then
      return
    end
    local indexes: integer[] = { self.log:lastIndex() }
    for i: integer = 1, #self.members do
      local peer: string = self.members[i]
      if peer ~= self.id then
        indexes[#indexes + 1] = self:matchOf(peer)
      end
    end
    table.sort(indexes, function(a: integer, b: integer): boolean
      return a > b
    end)
    local candidate: integer = indexes[self.majority]
    if candidate <= self.commitIndex then
      return
    end
    if candidate < self.log:baseIndex() then
      return
    end
    if self.log:termAt(candidate) ~= self.currentTerm then
      return
    end
    self.commitIndex = candidate
    self:applyCommitted()
  end,

  applyCommitted(): void
    if self.commitIndex > self.log:lastIndex() then
      error("RaftNode: commit index " .. tostring(self.commitIndex)
        .. " is beyond the log " .. tostring(self.log:lastIndex()))
    end
    if self.lastApplied < self.log:baseIndex() then
      self.lastApplied = self.log:baseIndex()
    end
    while self.lastApplied < self.commitIndex do
      self.lastApplied = self.lastApplied + 1
      if self.lastApplied > self.log:baseIndex() then
        local entry: LogEntry = self.log:entryAt(self.lastApplied)
        self.machine:apply(self.lastApplied, entry)
      end
    end
    self:maybeSnapshot()
  end,

  maybeSnapshot(): void
    if self.snapshotThreshold == 0 then
      return
    end
    if self.lastApplied <= self.log:baseIndex() then
      return
    end
    if self.lastApplied - self.log:baseIndex() < self.snapshotThreshold then
      return
    end
    self:snapshotNow()
  end,

  snapshotNow(): void
    local index: integer = self.lastApplied
    if index <= self.log:baseIndex() then
      return
    end
    if index > self.log:lastIndex() then
      error("RaftNode: cannot snapshot uncommitted index " .. tostring(index))
    end
    local image: Snapshot = {
      index = index,
      term = self.log:termAt(index),
      data = self.machine:snapshot(),
    }
    -- Order matters: write the snapshot first, then the compacted log. If a
    -- crash lands between the two, the constructor reconciles the snapshot
    -- boundary instead of losing committed entries.
    self.storage:saveSnapshot(image)
    self.log:compact(index)
    self:persistLog()
  end,

  -- -----------------------------------------------------------------------
  -- Client API and persistence
  -- -----------------------------------------------------------------------

  propose(command: string): ProposeResult
    if self.role ~= "leader" then
      return { ok = false, index = 0 }
    end
    self.log:append({ {
      term = self.currentTerm,
      kind = "command",
      command = command,
    } })
    self:persistLog()
    local index: integer = self.log:lastIndex()
    self:advanceCommit()
    self:broadcastAppend()
    return { ok = true, index = index }
  end,

  isLeader(): boolean
    return self.role == "leader"
  end,

  status(): NodeStatus
    return {
      id = self.id,
      role = self.role,
      term = self.currentTerm,
      commitIndex = self.commitIndex,
      lastApplied = self.lastApplied,
      lastIndex = self.log:lastIndex(),
      leaderId = self.leaderId,
    }
  end,

  persistHardState(): void
    self.storage:saveHardState(self.currentTerm, self.votedFor)
  end,

  persistLog(): void
    self.storage:saveLog(self.log:baseIndex(), self.log:baseTerm(), self.log:allEntries())
  end,
}

return { RaftNode = RaftNode }
