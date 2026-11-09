# typelua-raft

A from-scratch [Raft](https://raft.github.io/) implementation in
[TypeLua](https://types-lua.github.io/docs/) — leader election, log
replication, commitment, persistence, log compaction and snapshot install, with
a deterministic 5-node cluster simulator and executable safety invariants.

No sockets, no wall clock, no hidden coroutines: every election and partition is
reproducible from a seed, which makes the hard parts of Raft testable.

```bash
tlua check          # strict type checking (22 files)
tlua test           # 48 cases / 1049 assertions
tlua example quickstart
tlua run main.tl    # partition + heal demo
```

The same suite passes on PUC Lua 5.4 and LuaJIT/5.1:

```bash
tlua test
tlua test --target 5.1 --lua luajit
```

## What is implemented

- **Elections**: randomized election timeouts, one vote per term, log
  up-to-date check, higher-term step-down, split-vote recovery.
- **Replication**: `AppendEntries` with the log matching check, batch limits,
  conflict hints (`conflictIndex`) instead of one-index-at-a-time backtracking,
  and out-of-order follower catch-up.
- **Commit safety**: a leader only advances `commitIndex` for an entry from its
  own term that is present on a majority. Older terms are committed only via
  the current-term prefix (the Figure 8 rule).
- **Persistence**: term, vote and log are written before any reply that depends
  on them. `MemoryStorage` for simulation; `FileStorage` for a single
  atomically-replaced file with length-prefixed binary fields and a checksum.
- **Log compaction**: threshold-based snapshots, state-machine snapshot/restore,
  `InstallSnapshot` for followers behind the leader's base index, and crash
  reconciliation when a snapshot is newer than the log base.
- **State machine**: a binary-safe key/value store (`put`/`del`) with
  deterministic snapshots.
- **Simulator**: in-memory network with latency, one-way blocks, partitions,
  and message counters; `checkInvariants()` verifies election safety, log
  matching and committed-prefix agreement.
- **Tests**: unit tests for every module plus election, Figure 8, truncation,
  persistence, snapshot, partition and randomized fault-injection scenarios.

## Quick start

```lua
import clusterMod from "./lib/raft_cluster"
import kv from "./lib/raft_kv"

local c: ClusterApi = clusterMod.Cluster.new(3, {
  electionTimeout = 8,
  electionJitter = 5,
  heartbeatInterval = 2,
  snapshotThreshold = 16,
  seed = 7,
})

if not c:stabilize(400) then error("no leader") end

local command: string = kv.putCommand("hello", "raft")
local proposal: ClusterProposal = c:propose(command)
if not proposal.ok or not c:waitCommand(proposal.index, command, 200) then
  error("not committed")
end
c:run(10)

for _, id in pairs(c:nodeIds()) do
  print(id, c:lookup(id, "hello"))
end
```

Run it: `tlua example quickstart`.

`Cluster:waitCommand` verifies the entry at an index, so it cannot see an entry
that has already been compacted. With snapshots enabled, observe the state
machine instead (`cluster:lookup`); a state machine only applies committed
entries.

## Architecture

| File | Responsibility |
|---|---|
| `types/raft.d.tl` | Ambient ports and messages: `RaftLogView`, `Storage`, `Transport`, `StateMachine`, RPC union, config. Checked but never emitted. |
| `lib/raft_codec.tl` | Binary-safe netstrings, validated integers, checksum. |
| `lib/raft_log.tl` | Contiguous log with compaction base; all accessors copy. |
| `lib/raft_storage.tl` | `MemoryStorage` and checksummed, atomic `FileStorage`. |
| `lib/raft_transport.tl` | Deterministic in-memory network (latency, blocks, FIFO, counters). |
| `lib/raft_random.tl` | Park–Miller PRNG (identical on Lua 5.4 and LuaJIT). |
| `lib/raft_kv.tl` | Binary-safe key/value state machine and command builders. |
| `lib/raft_node.tl` | The Raft state machine: timers, elections, replication, commit, snapshots. |
| `lib/raft_cluster.tl` | Multi-node harness, partitions, client helpers, invariant checker. |
| `lib/raft_testkit.tl` | Recording storage/transport and manual message pump for tests. |

The node depends only on the four ports (`Storage`, `Transport`,
`StateMachine`, `RaftLogView`), so a socket transport or a WAL-backed storage can
replace the simulation without touching consensus logic.

## Safety model

`Cluster:checkInvariants()` checks:

1. **Election safety** — at most one leader per term, across the whole run.
2. **Log matching** — equal index+term implies the same entry and the same
   prefix.
3. **Committed agreement** — every committed index agrees across nodes.
4. **Leader completeness** — the newest leader contains every committed index
   (older minority leaders are exempt: they may lag entries committed by a
   higher-term majority they cannot see).

Randomized tests repeatedly partition a 5-node cluster, propose writes on both
sides, heal, and assert that every value that was ever observed committed is
still present on every node afterwards.

## Durability

`FileStorage` writes the complete state through a temporary file and
`os.rename`, with header/version, netstring fields and a checksum. It is a
reference adapter, not a production database:

- every save rewrites the file (no incremental WAL, no `fsync`);
- the parent directory must exist;
- one process per file.

`MemoryStorage` is the simulation adapter; it deep-copies on save/load so tests
cannot accidentally alias consensus state.

## Deliberate non-goals

- **No membership changes** (joint consensus / single-server changes). The
  membership is fixed at node construction.
- **No linearizable reads / ReadIndex / leases.** `Cluster:lookup` reads the
  local applied state; use it after a committed write, as the demo does.
- **No real network.** `InMemoryTransport` is the supported transport.
- **No pre-vote or check-quorum.** Elections follow the base paper.
- **No log truncation by time/size other than snapshots.**

These are isolated extensions, not cracks in the core: the ports and the
message union leave room for each without rewriting the node.

## Requirements

- A `tlua` compiler build (TypeLua `v0.9.2` or newer).
- Lua 5.4 (default) or LuaJIT/Lua 5.1 for the 5.1 target.
- No third-party Lua modules.

See [`docs/design.md`](docs/design.md) for the protocol walkthrough and the
reasoning behind each safety check.
