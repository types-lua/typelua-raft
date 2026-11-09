# Raft design notes

This document explains the protocol as implemented, the invariants the tests
enforce, and the places where a production system would plug in different
components.

## 1. Layers

```
             +---------------------------+
             | Cluster (simulator/tests) |
             +------+------+-------------+
                    |      |
          Transport |      | Storage / StateMachine
                    v      v
             +---------------------------+
             |        RaftNode           |
             |  election / replication   |
             |  commit / snapshots       |
             +-------------+-------------+
                           |
                           v
                      RaftLog (in memory,
                      persisted via Storage)
```

`RaftNode` knows only the ambient ports in `types/raft.d.tl`:

- `Storage`: durable term/vote/log/snapshot.
- `Transport`: fire-and-forget RPC with an envelope sender.
- `StateMachine`: apply committed entries, snapshot/restore.
- `RaftLogView`: the in-memory log operations.

The simulator is one implementation of those ports; a real deployment can
provide a socket transport and a WAL without changing the node.

## 2. Node state

Persistent (must survive a crash):

```
currentTerm : integer
votedFor    : string | nil
log         : entries[1..] with term, kind, command
snapshot    : { index, term, data }   // compacted prefix + machine image
```

Volatile:

```
role          : follower | candidate | leader
commitIndex   : integer
lastApplied   : integer
leaderId      : string | nil
electionTimer : elapsed / randomized timeout
votes         : map<node, boolean>          // candidate only
nextIndex     : map<peer, integer>          // leader only
matchIndex    : map<peer, integer>          // leader only
```

`commitIndex` is intentionally volatile. On restart it starts at the snapshot
boundary; a follower re-learns the commit index from the next leader. A state
machine that itself persists data must be snapshot-consistent (see §8).

## 3. Logical time

There is no wall clock. `RaftNode:tick()` increments one of two counters:

- Leader: a heartbeat counter; when it reaches `heartbeatInterval`, send
  `AppendEntries` to every peer.
- Follower/candidate: an election counter; when it reaches a randomized
  `electionTimeout` (`base + rng.int(jitter)`), start an election.

`InMemoryTransport` delivers a message at `now + latency + 1`, never in the same
tick. This one-tick floor prevents a node from recursively processing its own
reply inside `send()` and forces every test to exercise message ordering.

## 4. Election

```
candidate: term += 1; votedFor = self; persist; reset timer
           send RequestVote{term, lastLogIndex, lastLogTerm} to peers
```

On `RequestVote`:

1. Reject if `term < currentTerm`.
2. If `term > currentTerm`: adopt the term, clear the vote, become follower,
   persist.
3. Grant iff (`votedFor` is nil or already the candidate) **and** the
   candidate's log is at least as up-to-date:
   - higher `lastLogTerm` wins;
   - equal term requires `lastLogIndex >= our lastIndex`.
4. Persist the vote **before** replying.
5. Reset the election timer only when granting.

A candidate becomes leader on a majority. It immediately appends a **no-op**
entry in its term and broadcasts. The no-op is what allows older-term entries
to be committed safely (§6).

`AppendEntries` with `term >= currentTerm` makes a candidate or leader step
down; a higher `RequestVote` also steps a leader down (it cannot win against a
majority that has moved on).

## 5. Replication and the log matching check

Every `AppendEntries` carries `prevLogIndex`, `prevLogTerm`, a batch of
entries, and `leaderCommit`.

Follower checks, in order:

1. `term < currentTerm` → reject.
2. `prevLogIndex > lastIndex` → reject with `conflictIndex = lastIndex+1`.
3. `prevLogIndex < baseIndex` (compacted) → reject with
   `conflictIndex = baseIndex+1`, which drives the leader to `InstallSnapshot`.
4. Term mismatch at `prevLogIndex` → reject with the first index of that
   conflicting term.
5. Otherwise append, truncating only at the first index whose term differs.
6. Commit `min(leaderCommit, prevLogIndex + #entries)` and apply the prefix.

The leader keeps `nextIndex`/`matchIndex` per peer. On rejection it jumps back
using `conflictIndex` (bounded by its base index), instead of decrementing one
index at a time. On success it advances `matchIndex`, may send the next batch,
and recomputes the commit index.

Same index + same term **must** mean the same entry. `onAppendEntries` treats a
mismatched body as a local invariant violation and raises, rather than silently
choosing one version.

## 6. Commit rule (Figure 8)

A leader commits index `N` only when:

- `N` is replicated on a majority (including the leader), and
- `log[N].term == leader.currentTerm`.

The second condition is not an optimization. A leader that simply counted
replicas could commit an entry from an older term while a later leader (which
might overwrite it) is still possible. The current-term no-op acts as a barrier:
once it is on a majority, every earlier entry in the leader's log is safe.

`tests/raft_replication_test.tl` builds this exact scenario:

- n1 has a term-1 entry at index 1, wins term 2, appends a term-2 no-op at
  index 2;
- only n2 receives the log (n1+n2 is a majority, n3 is left in the dark);
- before n2's acknowledgement `commitIndex == 0`;
- after it, `commitIndex == 2` and the term-1 command is applied.

`applyCommitted` applies indices strictly in order up to `commitIndex`, then
may snapshot (§7). Applying a no-op only advances `lastApplied`; it does not
touch the state machine.

## 7. Snapshots

When `lastApplied - log.baseIndex() >= snapshotThreshold` (and the threshold is
non-zero), the node:

1. asks the state machine for a deterministic image;
2. writes `{index=lastApplied, term=log.termAt(index), data}` to `Storage`;
3. compacts the log at `index` and persists the new base.

The snapshot is written **before** the compacted log. If a crash lands between
the two writes, the constructor sees `snapshot.index > log.baseIndex()` and
reconciles:

- if the log still contains the snapshot boundary with the same term, compact
  to it;
- otherwise reset the log to the snapshot boundary.

A follower behind `baseIndex` receives `InstallSnapshot` with the image. It
restores the state machine, compacts or resets its log, sets
`commitIndex = lastApplied = snapshot.index`, persists snapshot then log, and
replies with the snapshot index. The leader treats that index as `matchIndex`
and continues with normal `AppendEntries`.

Snapshots are deterministic for the KV state machine: keys are sorted before
encoding, so two nodes with the same map produce byte-identical images.

## 8. Persistence ordering

The rule is simple and enforced by construction:

> No reply may be sent until the state it depends on is durable.

Concretely:

| Event | Before reply |
|---|---|
| term change / vote grant | `saveHardState(term, votedFor)` |
| append or truncate entries | `saveLog(base, baseTerm, entries)` |
| install snapshot | `saveSnapshot`, then `saveLog` |

`lib/raft_testkit.tl` records both storage writes and transport sends in one
trace; the election test asserts `saveHardState` appears before
`send:requestVoteReply`, and `saveLog` before `send:appendEntriesReply`.

`FileStorage` adds a header, length-prefixed fields (binary safe) and a
checksum, writes to `path.tmp`, then `os.rename`s it. Truncated or bit-rotten
files fail loudly instead of starting a node with a corrupt log.

## 9. Determinism and fault injection

`Cluster` owns:

- a deterministic `InMemoryTransport` (FIFO per destination, latency, blocks,
  sent/dropped counters);
- one `RaftNode`, `MemoryStorage` and `KVStore` per member;
- a PRNG seed per node, derived from the cluster seed.

`partition({{"n1"},{"n2","n3"}})` blocks every cross-group link in both
directions. `drop(a,b,true)` is one-way. `heal()` clears all blocks.

Because every node is ticked in id order and network delivery is deterministic,
a failing fuzz case can be reproduced from its seed.

## 10. Invariants as tests

`Cluster:checkInvariants()` returns a list of violations:

- **election safety** — `termLeaders[term]` is recorded forever; a second leader
  in the same term is a violation.
- **log matching** — for every pair of nodes and every shared index, equal
  index+term implies equal entry and equal preceding term.
- **committed agreement** — up to `min(commitIndex)`, terms and commands match.
- **leader completeness** — the newest leader's log covers every index another
  node has committed. Older leaders in a minority are exempt, because entries
  committed by a higher-term majority are not required to be in a stale leader's
  log.

The randomized tests partition 5 nodes, propose on both sides, heal, and repeat.
A value is considered expected only after it is observed in some node's state
machine (which only applies committed entries). At the end every expected value
must appear on every node, and the invariant list must be empty.

## 11. Complexity

- `AppendEntries` heartbeats: `O(peers)` messages per heartbeat interval.
- Log append/truncate: `O(batch)`.
- Commit advance: `O(peers log peers)` for the sort; peers are few.
- Compaction: `O(entries kept)` (copies the suffix).
- `FileStorage`: each save rewrites the whole state, `O(log + snapshot)`.

The implementation favors clarity and testability over throughput.

## 12. Where to extend

- **Membership changes**: add config entries and joint-consensus rules; the
  message union and `members` field are the entry points.
- **Linearizable reads**: implement ReadIndex (heartbeat quorum + wait for
  `lastApplied >= readIndex`).
- **Pre-vote / check-quorum**: add RPCs before incrementing the term, and make
  a leader step down when it cannot reach a quorum.
- **Real transport**: implement `Transport` over TCP or stdin/stdout; keep the
  envelope sender identity and the one-way `send` API.
- **Production storage**: implement `Storage` with a WAL plus incremental log
  appends and `fsync`; keep the persistence ordering from §8.
