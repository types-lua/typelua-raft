--- Public contracts shared by every Raft module.
---
--- This file is ambient: `tlua check` type-checks it, but it is never emitted
--- and has no runtime value. Keeping the ports here means implementations can
--- be swapped (memory storage vs. a file, simulated network vs. sockets)
--- without leaking concrete classes through the codebase.
---
--- TypeLua has no namespaced types, so these names are intentionally global.

declare type RaftRole = "follower" | "candidate" | "leader"
declare type EntryKind = "command" | "noop"

--- One replicated log record. Indexes are implicit: an entry at array slot
--- `i` (after the compaction base) has index `baseIndex + i`.
declare type LogEntry = {
  term: integer,
  kind: EntryKind,
  command: string,
}

--- A compacted prefix of the log plus a state-machine image at `index`.
declare type Snapshot = {
  index: integer,
  term: integer,
  data: string,
}

--- Binary decoders return records rather than tuples: TypeLua's module type
--- inference does not preserve tuple returns across `import`, so an explicit
--- `{ value, pos }` keeps callers fully typed.
declare type DecodedString = {
  value: string,
  pos: integer,
}

declare type DecodedInt = {
  value: integer,
  pos: integer,
}

declare type ProposeResult = {
  ok: boolean,
  index: integer,
}

declare type ClusterProposal = {
  ok: boolean,
  index: integer,
  leader: string | nil,
}

--- Read/append/truncate/compact operations used by the consensus core.
--- All returned entries are copies: callers may not mutate the log by aliasing.
declare interface RaftLogView {
  baseIndex(): integer,
  baseTerm(): integer,
  firstIndex(): integer,
  lastIndex(): integer,
  lastTerm(): integer,
  termAt(index: integer): integer,
  entryAt(index: integer): LogEntry,
  entriesFrom(index: integer, max: integer): LogEntry[],
  append(entries: LogEntry[]): void,
  truncateFrom(index: integer): void,
  compact(index: integer): void,
  reset(baseIndex: integer, baseTerm: integer): void,
  contains(index: integer): boolean,
  allEntries(): LogEntry[],
}

--- Durable state. Raft requires term/vote/log to be on stable storage before
--- any RPC reply that depends on them; every method here is synchronous.
declare interface Storage {
  saveHardState(term: integer, votedFor: string | nil): void,
  loadTerm(): integer,
  loadVotedFor(): string | nil,
  saveLog(baseIndex: integer, baseTerm: integer, entries: LogEntry[]): void,
  loadLogBase(): integer,
  loadLogBaseTerm(): integer,
  loadLogEntries(): LogEntry[],
  saveSnapshot(snapshot: Snapshot): void,
  loadSnapshot(): Snapshot | nil,
}

--- Deterministic materialized view of committed commands.
declare interface StateMachine {
  apply(index: integer, entry: LogEntry): void,
  snapshot(): string,
  restore(data: string): void,
}

--- Convenience view used by the demo/test harness for key/value machines.
declare interface KVView {
  get(key: string): string | nil,
  size(): integer,
}

-- ---------------------------------------------------------------------------
-- RPC payloads. The sender identity travels in the envelope so payloads stay
-- small; `from` is authoritative for candidate/leader ids.
-- ---------------------------------------------------------------------------

declare type RequestVote = {
  kind: "requestVote",
  term: integer,
  lastLogIndex: integer,
  lastLogTerm: integer,
}

declare type RequestVoteReply = {
  kind: "requestVoteReply",
  term: integer,
  granted: boolean,
}

declare type AppendEntries = {
  kind: "appendEntries",
  term: integer,
  prevLogIndex: integer,
  prevLogTerm: integer,
  entries: LogEntry[],
  leaderCommit: integer,
}

declare type AppendEntriesReply = {
  kind: "appendEntriesReply",
  term: integer,
  success: boolean,
  matchIndex: integer,
  conflictIndex: integer,
}

declare type InstallSnapshot = {
  kind: "installSnapshot",
  term: integer,
  lastIncludedIndex: integer,
  lastIncludedTerm: integer,
  data: string,
}

declare type InstallSnapshotReply = {
  kind: "installSnapshotReply",
  term: integer,
  success: boolean,
  lastIncludedIndex: integer,
}

declare type RaftMessage = RequestVote | RequestVoteReply | AppendEntries | AppendEntriesReply | InstallSnapshot | InstallSnapshotReply

declare type Envelope = {
  from: string,
  to: string,
  message: RaftMessage,
  due: integer,
}

declare interface Transport {
  send(from: string, to: string, message: RaftMessage): void,
  drain(to: string, now: integer): Envelope[],
  setBlocked(from: string, to: string, blocked: boolean): void,
  pending(): integer,
}

--- Extra surface of the deterministic simulator (tests and Cluster only).
declare interface SimTransport extends Transport {
  advance(ticks: integer): void,
  clearBlocks(): void,
  setLatency(ticks: integer): void,
  sentCount(kind: string): integer,
  blockedCount(): integer,
  now: integer,
  dropped: integer,
}

-- ---------------------------------------------------------------------------
-- Configuration and observable node/cluster API.
-- ---------------------------------------------------------------------------

--- All fields are optional so callers can override only what a test needs.
declare interface RngApi {
  next(): integer,
  int(maxInclusive: integer): integer,
}

declare type RaftConfig = {
  electionTimeout?: integer,
  electionJitter?: integer,
  heartbeatInterval?: integer,
  snapshotThreshold?: integer,
  maxBatch?: integer,
  seed?: integer,
}

declare type NodeStatus = {
  id: string,
  role: RaftRole,
  term: integer,
  commitIndex: integer,
  lastApplied: integer,
  lastIndex: integer,
  leaderId: string | nil,
}

declare interface RaftNodeApi {
  id: string,
  role: RaftRole,
  currentTerm: integer,
  votedFor: string | nil,
  commitIndex: integer,
  lastApplied: integer,
  leaderId: string | nil,
  members: string[],
  log: RaftLogView,
  machine: StateMachine,
  tick(): void,
  handle(from: string, message: RaftMessage): void,
  propose(command: string): ProposeResult,
  startElection(): void,
  isLeader(): boolean,
  snapshotNow(): void,
  status(): NodeStatus,
}

declare type ClusterOptions = {
  electionTimeout?: integer,
  electionJitter?: integer,
  heartbeatInterval?: integer,
  snapshotThreshold?: integer,
  maxBatch?: integer,
  latency?: integer,
  seed?: integer,
}

declare interface ClusterApi {
  tick(): void,
  run(ticks: integer): void,
  stabilize(maxTicks: integer): boolean,
  node(id: string): RaftNodeApi,
  nodeIds(): string[],
  leaders(): string[],
  leaderId(): string | nil,
  propose(command: string): ClusterProposal,
  waitCommitted(index: integer, maxTicks: integer): boolean,
  waitCommand(index: integer, command: string, maxTicks: integer): boolean,
  partition(groups: string[][]): void,
  heal(): void,
  drop(from: string, to: string, blocked: boolean): void,
  setLatency(ticks: integer): void,
  lookup(id: string, key: string): string | nil,
  sentCount(kind: string): integer,
  checkInvariants(): string[],
  pendingMessages(): integer,
}
