--- Contiguous Raft log with a compaction base.
---
--- Index 0 is a sentinel with term 0. After `compact(i)`, index `i` is still
--- addressable via `termAt` (the snapshot boundary) but no longer has an
--- entry body; only indexes `i + 1 .. lastIndex()` can be applied or sent.
---
--- The class never hands out its internal tables: every accessor copies, so
--- a caller cannot corrupt consensus state through aliasing.

local function copyEntry(entry: LogEntry): LogEntry
  return {
    term = entry.term,
    kind = entry.kind,
    command = entry.command,
  }
end

class RaftLog implements RaftLogView {
  baseIdx: integer = 0,
  baseTrm: integer = 0,
  items: LogEntry[] = {},

  constructor(baseIndex: integer, baseTerm: integer, entries: LogEntry[])
    if baseIndex < 0 then
      error("RaftLog: negative base index " .. tostring(baseIndex))
    end
    if baseTerm < 0 then
      error("RaftLog: negative base term " .. tostring(baseTerm))
    end
    self.baseIdx = baseIndex
    self.baseTrm = baseTerm
    self.items = {}
    for i: integer = 1, #entries do
      self.items[i] = copyEntry(entries[i])
    end
  end,

  baseIndex(): integer
    return self.baseIdx
  end,

  baseTerm(): integer
    return self.baseTrm
  end,

  firstIndex(): integer
    return self.baseIdx + 1
  end,

  lastIndex(): integer
    return self.baseIdx + #self.items
  end,

  lastTerm(): integer
    if #self.items == 0 then
      return self.baseTrm
    end
    return self.items[#self.items].term
  end,

  termAt(index: integer): integer
    if index == self.baseIdx then
      return self.baseTrm
    end
    if index < self.baseIdx then
      error("RaftLog: index " .. tostring(index) .. " was compacted")
    end
    if index > self:lastIndex() then
      error("RaftLog: index " .. tostring(index) .. " is beyond the log")
    end
    return self.items[index - self.baseIdx].term
  end,

  entryAt(index: integer): LogEntry
    if index <= self.baseIdx then
      error("RaftLog: no entry body at or before base index " .. tostring(index))
    end
    if index > self:lastIndex() then
      error("RaftLog: index " .. tostring(index) .. " is beyond the log")
    end
    return copyEntry(self.items[index - self.baseIdx])
  end,

  entriesFrom(index: integer, max: integer): LogEntry[]
    if index < self:firstIndex() then
      error("RaftLog: index " .. tostring(index) .. " was compacted")
    end
    local out: LogEntry[] = {}
    if index > self:lastIndex() then
      return out
    end
    local last: integer = self:lastIndex()
    if max > 0 and index + max - 1 < last then
      last = index + max - 1
    end
    for i: integer = index, last do
      out[#out + 1] = copyEntry(self.items[i - self.baseIdx])
    end
    return out
  end,

  append(entries: LogEntry[]): void
    for i: integer = 1, #entries do
      self.items[#self.items + 1] = copyEntry(entries[i])
    end
  end,

  truncateFrom(index: integer): void
    if index <= self.baseIdx then
      error("RaftLog: cannot truncate compacted index " .. tostring(index))
    end
    local keep: integer = index - self.baseIdx - 1
    while #self.items > keep do
      table.remove(self.items)
    end
  end,

  compact(index: integer): void
    if index <= self.baseIdx then
      return
    end
    if index > self:lastIndex() then
      error("RaftLog: cannot compact beyond " .. tostring(self:lastIndex()))
    end
    local boundaryTerm: integer = self:termAt(index)
    local kept: LogEntry[] = {}
    for i: integer = index + 1, self:lastIndex() do
      kept[#kept + 1] = copyEntry(self.items[i - self.baseIdx])
    end
    self.baseIdx = index
    self.baseTrm = boundaryTerm
    self.items = kept
  end,

  reset(baseIndex: integer, baseTerm: integer): void
    if baseIndex < 0 or baseTerm < 0 then
      error("RaftLog: negative reset boundary")
    end
    self.baseIdx = baseIndex
    self.baseTrm = baseTerm
    self.items = {}
  end,

  contains(index: integer): boolean
    return index >= self.baseIdx and index <= self:lastIndex()
  end,

  allEntries(): LogEntry[]
    local out: LogEntry[] = {}
    for i: integer = 1, #self.items do
      out[i] = copyEntry(self.items[i])
    end
    return out
  end,
}

return { RaftLog = RaftLog }
