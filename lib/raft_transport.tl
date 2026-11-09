--- Deterministic in-memory network.
---
--- Messages are delivered at `now + latency + 1`, so a send during tick T is
--- observed at T+1 or later. That one-tick floor is deliberate: it prevents a
--- node from recursively processing its own reply inside `send()`, which would
--- hide ordering bugs that a real network exposes.
---
--- Per-destination queues preserve FIFO order. `setBlocked` models partitions
--- and one-way link failures; blocked messages are dropped, not buffered.

class InMemoryTransport implements Transport {
  queues: map<string, Envelope[]> = {},
  blocked: map<string, boolean> = {},
  now: integer = 0,
  latency: integer = 0,
  sentKinds: map<string, integer> = {},
  blockedLinks: integer = 0,
  dropped: integer = 0,

  constructor() end,

  advance(ticks: integer): void
    if ticks < 0 then
      error("InMemoryTransport: negative advance")
    end
    self.now = self.now + ticks
  end,

  setLatency(ticks: integer): void
    if ticks < 0 then
      error("InMemoryTransport: negative latency")
    end
    self.latency = ticks
  end,

  send(from: string, to: string, message: RaftMessage): void
    if self:isBlocked(from, to) then
      self.dropped = self.dropped + 1
      return
    end
    local kind: string = message.kind
    local seen: integer | nil = self.sentKinds[kind]
    if seen == nil then
      self.sentKinds[kind] = 1
    else
      self.sentKinds[kind] = seen + 1
    end
    local queue: Envelope[] = self:queueFor(to)
    queue[#queue + 1] = {
      from = from,
      to = to,
      message = message,
      due = self.now + self.latency + 1,
    }
  end,

  queueFor(to: string): Envelope[]
    local existing: Envelope[] | nil = self.queues[to]
    if existing ~= nil then
      return existing!
    end
    local created: Envelope[] = {}
    self.queues[to] = created
    return created
  end,

  drain(to: string, now: integer): Envelope[]
    local queue: Envelope[] | nil = self.queues[to]
    if queue == nil then
      return {}
    end
    local ready: Envelope[] = {}
    local wait: Envelope[] = {}
    for i: integer = 1, #queue do
      local envelope: Envelope = queue[i]
      if envelope.due <= now then
        ready[#ready + 1] = envelope
      else
        wait[#wait + 1] = envelope
      end
    end
    if #wait == 0 then
      self.queues[to] = {}
    else
      self.queues[to] = wait
    end
    return ready
  end,

  setBlocked(from: string, to: string, blocked: boolean): void
    local key: string = self:key(from, to)
    local previous: boolean | nil = self.blocked[key]
    if previous == blocked then
      return
    end
    self.blocked[key] = blocked
    if blocked then
      self.blockedLinks = self.blockedLinks + 1
      -- A real partition also loses messages already in flight on that link;
      -- keeping them would let a heartbeat cross a partition retroactively.
      local queue: Envelope[] | nil = self.queues[to]
      if queue ~= nil then
        local kept: Envelope[] = {}
        for i: integer = 1, #queue! do
          local envelope: Envelope = queue![i]
          if envelope.from ~= from then
            kept[#kept + 1] = envelope
          end
        end
        self.queues[to] = kept
      end
    else
      self.blockedLinks = self.blockedLinks - 1
    end
  end,

  blockedCount(): integer
    return self.blockedLinks
  end,

  isBlocked(from: string, to: string): boolean
    local value: boolean | nil = self.blocked[self:key(from, to)]
    return value == true
  end,

  clearBlocks(): void
    self.blocked = {}
    self.blockedLinks = 0
  end,

  pending(): integer
    local total: integer = 0
    for _, queue in pairs(self.queues) do
      total = total + #queue
    end
    return total
  end,

  sentCount(kind: string): integer
    local n: integer | nil = self.sentKinds[kind]
    if n == nil then
      return 0
    end
    return n
  end,

  key(from: string, to: string): string
    return from .. "\0" .. to
  end,
}

return { InMemoryTransport = InMemoryTransport }
