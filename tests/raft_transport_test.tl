import T from "tlua_test"
import net from "../lib/raft_transport"

local function vote(term: integer): RaftMessage
  return { kind = "requestVote", term = term, lastLogIndex = 0, lastLogTerm = 0 }
end

return {
  ["messages are delivered no earlier than the next tick"] = function(): void
    local transport = net.InMemoryTransport.new()
    transport:send("a", "b", vote(1))
    T.eq(transport:pending(), 1, "queued")
    T.eq(#transport:drain("b", 0), 0, "not due at send tick")
    transport:advance(1)
    local ready: Envelope[] = transport:drain("b", 1)
    T.eq(#ready, 1, "due")
    T.eq(ready[1].from, "a", "from")
    T.eq(ready[1].to, "b", "to")
    T.eq(ready[1].message.kind, "requestVote", "kind")
    T.eq(transport:pending(), 0, "drained")
  end,

  ["latency delays delivery"] = function(): void
    local transport = net.InMemoryTransport.new()
    transport:setLatency(2)
    transport:send("a", "c", vote(1))
    transport:advance(2)
    T.eq(#transport:drain("c", 2), 0, "not yet")
    transport:advance(1)
    T.eq(#transport:drain("c", 3), 1, "arrived")
  end,

  ["per-destination FIFO"] = function(): void
    local transport = net.InMemoryTransport.new()
    transport:send("a", "b", vote(1))
    transport:send("a", "b", vote(2))
    transport:send("a", "b", vote(3))
    transport:advance(1)
    local ready: Envelope[] = transport:drain("b", 1)
    T.eq(#ready, 3, "all")
    local first = ready[1].message as RequestVote
    local second = ready[2].message as RequestVote
    local third = ready[3].message as RequestVote
    T.eq(first.term, 1, "first")
    T.eq(second.term, 2, "second")
    T.eq(third.term, 3, "third")
  end,

  ["blocked links drop messages"] = function(): void
    local transport = net.InMemoryTransport.new()
    transport:setBlocked("a", "b", true)
    transport:send("a", "b", vote(1))
    transport:send("b", "a", vote(2))
    T.eq(transport:pending(), 1, "only the allowed direction")
    T.eq(transport.dropped, 1, "dropped")
    T.ok(transport:isBlocked("a", "b"), "blocked")
    T.ok(not transport:isBlocked("b", "a"), "reverse open")
    T.eq(transport:blockedCount(), 1, "one blocked link")
    transport:setBlocked("a", "b", true)
    T.eq(transport:blockedCount(), 1, "idempotent")
    transport:setBlocked("a", "b", false)
    transport:send("a", "b", vote(3))
    T.eq(transport:pending(), 2, "healed")
  end,

  ["blocking drops messages already in flight"] = function(): void
    local transport = net.InMemoryTransport.new()
    transport:send("a", "b", vote(1))
    T.eq(transport:pending(), 1, "queued")
    transport:setBlocked("a", "b", true)
    T.eq(transport:pending(), 0, "in-flight message dropped")
    transport:advance(1)
    T.eq(#transport:drain("b", 1), 0, "nothing arrives")
  end,

  ["counters"] = function(): void
    local transport = net.InMemoryTransport.new()
    transport:send("a", "b", vote(1))
    transport:send("a", "c", vote(2))
    transport:send("a", "b", vote(3))
    T.eq(transport:sentCount("requestVote"), 3, "sent")
    T.eq(transport:sentCount("appendEntries"), 0, "none")
    transport:clearBlocks()
    T.eq(transport:blockedCount(), 0, "cleared")
    T.eq(transport:pending(), 3, "pending")
  end,
}
