# Changelog

## 0.1.0

- Paper-faithful Raft node: randomized elections, one vote per term, log
  up-to-date checks, higher-term step-down, AppendEntries log matching,
  conflict hints, current-term commit rule (Figure 8), no-op on election.
- Durable state: memory and checksummed atomic-file storage; term/vote/log
  persisted before dependent replies.
- Snapshots: threshold compaction, deterministic state-machine images,
  InstallSnapshot for lagging followers, crash reconciliation.
- Deterministic simulator: latency, one-way drops, partitions, message
  counters, and executable safety invariants.
- Binary-safe KV state machine with deterministic snapshots.
- 48 cases / 1049 assertions, passing on Lua 5.4 and LuaJIT/target 5.1.
- CI type-checks, runs both targets, builds the demo and runs the quickstart.
