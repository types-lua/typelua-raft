import T from "tlua_test"
import log from "../lib/raft_log"

local function entry(term: integer, command: string): LogEntry
  return { term = term, kind = "command", command = command }
end

return {
  ["empty log sentinel"] = function(): void
    local l = log.RaftLog.new(0, 0, {})
    T.eq(l:baseIndex(), 0, "base")
    T.eq(l:baseTerm(), 0, "base term")
    T.eq(l:firstIndex(), 1, "first")
    T.eq(l:lastIndex(), 0, "last")
    T.eq(l:lastTerm(), 0, "last term")
    T.eq(l:termAt(0), 0, "sentinel term")
    T.ok(l:contains(0), "contains sentinel")
    T.ok(not l:contains(1), "no entry")
  end,

  ["append keeps terms and indexes"] = function(): void
    local l = log.RaftLog.new(0, 0, {})
    l:append({ entry(1, "a"), entry(1, "b") })
    l:append({ entry(2, "c") })
    T.eq(l:lastIndex(), 3, "last index")
    T.eq(l:lastTerm(), 2, "last term")
    T.eq(l:termAt(1), 1, "term 1")
    T.eq(l:termAt(3), 2, "term 3")
    T.eq(l:entryAt(2).command, "b", "entry body")
    T.eq(#l:allEntries(), 3, "all")
  end,

  ["entriesFrom honours the batch limit"] = function(): void
    local l = log.RaftLog.new(0, 0, {})
    for i: integer = 1, 5 do
      l:append({ entry(1, "e" .. tostring(i)) })
    end
    local batch: LogEntry[] = l:entriesFrom(2, 2)
    T.eq(#batch, 2, "batch size")
    T.eq(batch[1].command, "e2", "first")
    T.eq(batch[2].command, "e3", "second")
    local all: LogEntry[] = l:entriesFrom(4, 0)
    T.eq(#all, 2, "rest")
    T.eq(#l:entriesFrom(9, 0), 0, "past end")
  end,

  ["accessors copy"] = function(): void
    local l = log.RaftLog.new(0, 0, {})
    l:append({ entry(1, "original") })
    local got: LogEntry = l:entryAt(1)
    got.command = "mutated"
    local all: LogEntry[] = l:allEntries()
    all[1].command = "mutated again"
    T.eq(l:entryAt(1).command, "original", "log is unchanged")
  end,

  ["truncate drops the suffix"] = function(): void
    local l = log.RaftLog.new(0, 0, {})
    for i: integer = 1, 5 do
      l:append({ entry(i, "e" .. tostring(i)) })
    end
    l:truncateFrom(3)
    T.eq(l:lastIndex(), 2, "truncated")
    T.eq(l:lastTerm(), 2, "term")
    l:truncateFrom(9)
    T.eq(l:lastIndex(), 2, "past end is a no-op")
    T.raises(function(): void l:truncateFrom(0) end)
  end,

  ["compact moves the base"] = function(): void
    local l = log.RaftLog.new(0, 0, {})
    for i: integer = 1, 5 do
      l:append({ entry(i, "e" .. tostring(i)) })
    end
    l:compact(3)
    T.eq(l:baseIndex(), 3, "base index")
    T.eq(l:baseTerm(), 3, "base term")
    T.eq(l:firstIndex(), 4, "first")
    T.eq(l:lastIndex(), 5, "last")
    T.eq(l:termAt(3), 3, "boundary term")
    T.ok(l:contains(3), "contains boundary")
    T.eq(#l:allEntries(), 2, "only suffix kept")
    T.eq(l:entryAt(4).command, "e4", "suffix body")
    T.raises(function(): void l:termAt(2) end)
    T.raises(function(): void l:entryAt(3) end)
    l:compact(3)
    T.eq(l:baseIndex(), 3, "idempotent")
    T.raises(function(): void l:compact(9) end)
  end,

  ["reset discards entries"] = function(): void
    local l = log.RaftLog.new(0, 0, {})
    l:append({ entry(1, "a"), entry(2, "b") })
    l:reset(7, 4)
    T.eq(l:baseIndex(), 7, "base")
    T.eq(l:baseTerm(), 4, "term")
    T.eq(l:lastIndex(), 7, "empty suffix")
    T.eq(l:lastTerm(), 4, "last term")
    T.eq(#l:allEntries(), 0, "no entries")
  end,
}
