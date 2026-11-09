--- Shared test scaffolding: recording adapters and a manual message pump.
---
--- The recording adapters share a trace array so a test can assert Raft's
--- persistence-before-reply rule (a hard-state/log save must appear before the
--- corresponding send), which is otherwise easy to regress silently.

import storageMod from "./raft_storage"
import transportMod from "./raft_transport"

class RecordingStorage implements Storage {
  inner: Storage,
  trace: string[],

  constructor(inner: Storage, trace: string[])
    self.inner = inner
    self.trace = trace
  end,

  saveHardState(term: integer, votedFor: string | nil): void
    self.trace[#self.trace + 1] = "saveHardState:" .. tostring(term) .. ":" .. tostring(votedFor)
    self.inner:saveHardState(term, votedFor)
  end,

  loadTerm(): integer
    return self.inner:loadTerm()
  end,

  loadVotedFor(): string | nil
    return self.inner:loadVotedFor()
  end,

  saveLog(baseIndex: integer, baseTerm: integer, entries: LogEntry[]): void
    self.trace[#self.trace + 1] = "saveLog:" .. tostring(baseIndex) .. ":" .. tostring(#entries)
    self.inner:saveLog(baseIndex, baseTerm, entries)
  end,

  loadLogBase(): integer
    return self.inner:loadLogBase()
  end,

  loadLogBaseTerm(): integer
    return self.inner:loadLogBaseTerm()
  end,

  loadLogEntries(): LogEntry[]
    return self.inner:loadLogEntries()
  end,

  saveSnapshot(snapshot: Snapshot): void
    self.trace[#self.trace + 1] = "saveSnapshot:" .. tostring(snapshot.index)
    self.inner:saveSnapshot(snapshot)
  end,

  loadSnapshot(): Snapshot | nil
    return self.inner:loadSnapshot()
  end,
}

class TraceTransport implements Transport {
  inner: Transport,
  trace: string[],

  constructor(inner: Transport, trace: string[])
    self.inner = inner
    self.trace = trace
  end,

  send(from: string, to: string, message: RaftMessage): void
    self.trace[#self.trace + 1] = "send:" .. message.kind .. ":" .. to
    self.inner:send(from, to, message)
  end,

  drain(to: string, now: integer): Envelope[]
    return self.inner:drain(to, now)
  end,

  setBlocked(from: string, to: string, blocked: boolean): void
    self.inner:setBlocked(from, to, blocked)
  end,

  pending(): integer
    return self.inner:pending()
  end,
}

--- Delivers all messages that are due at `now` to every node.
--- Must be called after the transport clock has been advanced.
local function pump(transport: Transport, nodes: map<string, RaftNodeApi>, now: integer): void
  for id, node in pairs(nodes) do
    local ready: Envelope[] = transport:drain(id, now)
    for i: integer = 1, #ready do
      local envelope: Envelope = ready[i]
      node:handle(envelope.from, envelope.message)
    end
  end
end

--- Position of the first trace entry containing `needle`, or 0.
local function traceIndex(trace: string[], needle: string): integer
  for i: integer = 1, #trace do
    if string.find(trace[i], needle, 1, true) ~= nil then
      return i
    end
  end
  return 0
end

return {
  RecordingStorage = RecordingStorage,
  TraceTransport = TraceTransport,
  pump = pump,
  traceIndex = traceIndex,
  MemoryStorage = storageMod.MemoryStorage,
  InMemoryTransport = transportMod.InMemoryTransport,
}
