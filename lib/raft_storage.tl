--- Durable Raft state.
---
--- `MemoryStorage` is the simulation/test adapter. `FileStorage` persists the
--- same model to one atomically-replaced file, with a length-prefixed payload
--- and a checksum, so a torn write or a truncated file is detected instead of
--- silently starting a node with a corrupt log.
---
--- Neither adapter is a database: every save rewrites the full state. That is
--- the right trade-off for a reference implementation and for tests; a
--- production adapter can replace it with incremental WAL + fsync.

import codec from "./raft_codec"

local function copyEntry(entry: LogEntry): LogEntry
  return { term = entry.term, kind = entry.kind, command = entry.command }
end

local function copyEntries(entries: LogEntry[]): LogEntry[]
  local out: LogEntry[] = {}
  for i: integer = 1, #entries do
    out[i] = copyEntry(entries[i])
  end
  return out
end

local function copySnapshot(snapshot: Snapshot): Snapshot
  return { index = snapshot.index, term = snapshot.term, data = snapshot.data }
end

local function readFile(path: string): string | nil
  local file = io.open(path, "rb")
  if file == nil then
    return nil
  end
  local data: string | nil = file:read("*a")
  file:close()
  if data == nil then
    error("raft storage: cannot read " .. path)
  end
  return data
end

class MemoryStorage implements Storage {
  term: integer = 0,
  voted: string | nil = nil,
  base: integer = 0,
  baseTrm: integer = 0,
  entries: LogEntry[] = {},
  snap: Snapshot | nil = nil,

  constructor() end,

  saveHardState(term: integer, votedFor: string | nil): void
    self.term = term
    self.voted = votedFor
  end,

  loadTerm(): integer
    return self.term
  end,

  loadVotedFor(): string | nil
    return self.voted
  end,

  saveLog(baseIndex: integer, baseTerm: integer, entries: LogEntry[]): void
    self.base = baseIndex
    self.baseTrm = baseTerm
    self.entries = copyEntries(entries)
  end,

  loadLogBase(): integer
    return self.base
  end,

  loadLogBaseTerm(): integer
    return self.baseTrm
  end,

  loadLogEntries(): LogEntry[]
    return copyEntries(self.entries)
  end,

  saveSnapshot(snapshot: Snapshot): void
    self.snap = copySnapshot(snapshot)
  end,

  loadSnapshot(): Snapshot | nil
    local snapshot = self.snap
    if snapshot == nil then
      return nil
    end
    return copySnapshot(snapshot)
  end,
}

local HEADER: string = "TLRAFT1\n"

class FileStorage implements Storage {
  path: string,
  term: integer = 0,
  voted: string | nil = nil,
  base: integer = 0,
  baseTrm: integer = 0,
  entries: LogEntry[] = {},
  snap: Snapshot | nil = nil,

  constructor(path: string)
    if path == "" then
      error("FileStorage: empty path")
    end
    self.path = path
    local data: string | nil = readFile(path)
    if data ~= nil then
      self:decode(data)
    end
  end,

  saveHardState(term: integer, votedFor: string | nil): void
    self.term = term
    self.voted = votedFor
    self:writeAll()
  end,

  loadTerm(): integer
    return self.term
  end,

  loadVotedFor(): string | nil
    return self.voted
  end,

  saveLog(baseIndex: integer, baseTerm: integer, entries: LogEntry[]): void
    self.base = baseIndex
    self.baseTrm = baseTerm
    self.entries = copyEntries(entries)
    self:writeAll()
  end,

  loadLogBase(): integer
    return self.base
  end,

  loadLogBaseTerm(): integer
    return self.baseTrm
  end,

  loadLogEntries(): LogEntry[]
    return copyEntries(self.entries)
  end,

  saveSnapshot(snapshot: Snapshot): void
    self.snap = copySnapshot(snapshot)
    self:writeAll()
  end,

  loadSnapshot(): Snapshot | nil
    local snapshot = self.snap
    if snapshot == nil then
      return nil
    end
    return copySnapshot(snapshot)
  end,

  encode(): string
    local parts: string[] = { HEADER }
    parts[#parts + 1] = codec.encodeInt(self.term)
    if self.voted == nil then
      parts[#parts + 1] = codec.encodeInt(0)
    else
      parts[#parts + 1] = codec.encodeInt(1)
      parts[#parts + 1] = codec.encodeString(self.voted)
    end
    parts[#parts + 1] = codec.encodeInt(self.base)
    parts[#parts + 1] = codec.encodeInt(self.baseTrm)
    parts[#parts + 1] = codec.encodeInt(#self.entries)
    for i: integer = 1, #self.entries do
      local entry: LogEntry = self.entries[i]
      parts[#parts + 1] = codec.encodeInt(entry.term)
      if entry.kind == "noop" then
        parts[#parts + 1] = codec.encodeInt(1)
      else
        parts[#parts + 1] = codec.encodeInt(0)
      end
      parts[#parts + 1] = codec.encodeString(entry.command)
    end
    if self.snap == nil then
      parts[#parts + 1] = codec.encodeInt(0)
    else
      parts[#parts + 1] = codec.encodeInt(1)
      parts[#parts + 1] = codec.encodeInt(self.snap.index)
      parts[#parts + 1] = codec.encodeInt(self.snap.term)
      parts[#parts + 1] = codec.encodeString(self.snap.data)
    end
    local body: string = table.concat(parts)
    return body .. codec.encodeInt(codec.checksum(body))
  end,

  decode(data: string): void
    if string.sub(data, 1, #HEADER) ~= HEADER then
      error("FileStorage: bad header in " .. self.path)
    end
    local pos: integer = #HEADER + 1
    local termField: DecodedInt = codec.decodeInt(data, pos)
    local term: integer = termField.value
    pos = termField.pos
    local voteField: DecodedInt = codec.decodeInt(data, pos)
    local hasVote: integer = voteField.value
    pos = voteField.pos
    local voted: string | nil = nil
    if hasVote == 1 then
      local votedField: DecodedString = codec.decodeString(data, pos)
      voted = votedField.value
      pos = votedField.pos
    elseif hasVote ~= 0 then
      error("FileStorage: bad vote marker")
    end
    local baseField: DecodedInt = codec.decodeInt(data, pos)
    local base: integer = baseField.value
    pos = baseField.pos
    local baseTermField: DecodedInt = codec.decodeInt(data, pos)
    local baseTrm: integer = baseTermField.value
    pos = baseTermField.pos
    local countField: DecodedInt = codec.decodeInt(data, pos)
    local count: integer = countField.value
    pos = countField.pos
    if count < 0 or count > #data then
      error("FileStorage: bad entry count " .. tostring(count))
    end
    local entries: LogEntry[] = {}
    for i: integer = 1, count do
      local entryTermField: DecodedInt = codec.decodeInt(data, pos)
      local entryTerm: integer = entryTermField.value
      pos = entryTermField.pos
      local kindField: DecodedInt = codec.decodeInt(data, pos)
      local kindCode: integer = kindField.value
      pos = kindField.pos
      local commandField: DecodedString = codec.decodeString(data, pos)
      local command: string = commandField.value
      pos = commandField.pos
      local kind: EntryKind = "command"
      if kindCode == 1 then
        kind = "noop"
      elseif kindCode ~= 0 then
        error("FileStorage: bad entry kind " .. tostring(kindCode))
      end
      if entryTerm < 0 then
        error("FileStorage: negative entry term")
      end
      entries[i] = { term = entryTerm, kind = kind, command = command }
    end
    local snapField: DecodedInt = codec.decodeInt(data, pos)
    local hasSnap: integer = snapField.value
    pos = snapField.pos
    local snap: Snapshot | nil = nil
    if hasSnap == 1 then
      local indexField: DecodedInt = codec.decodeInt(data, pos)
      pos = indexField.pos
      local snapTermField: DecodedInt = codec.decodeInt(data, pos)
      pos = snapTermField.pos
      local snapDataField: DecodedString = codec.decodeString(data, pos)
      pos = snapDataField.pos
      snap = {
        index = indexField.value,
        term = snapTermField.value,
        data = snapDataField.value,
      }
    elseif hasSnap ~= 0 then
      error("FileStorage: bad snapshot marker")
    end
    local checksumStart: integer = pos
    local storedField: DecodedInt = codec.decodeInt(data, pos)
    local stored: integer = storedField.value
    pos = storedField.pos
    if pos ~= #data + 1 then
      error("FileStorage: trailing bytes in " .. self.path)
    end
    local body: string = string.sub(data, 1, checksumStart - 1)
    if stored ~= codec.checksum(body) then
      error("FileStorage: checksum mismatch in " .. self.path)
    end

    if term < 0 or base < 0 or baseTrm < 0 then
      error("FileStorage: negative term/index")
    end
    if snap ~= nil and (snap!.index < 0 or snap!.term < 0) then
      error("FileStorage: negative snapshot boundary")
    end
    self.term = term
    self.voted = voted
    self.base = base
    self.baseTrm = baseTrm
    self.entries = entries
    self.snap = snap
  end,

  writeAll(): void
    local payload: string = self:encode()
    local tmp: string = self.path .. ".tmp"
    local file = io.open(tmp, "wb")
    if file == nil then
      error("FileStorage: cannot write " .. tmp)
    end
    local written = file:write(payload)
    file:close()
    if written == nil then
      os.remove(tmp)
      error("FileStorage: short write " .. tmp)
    end
    local renamed, renameErr = os.rename(tmp, self.path)
    if renamed == nil then
      os.remove(tmp)
      error("FileStorage: rename failed: " .. tostring(renameErr))
    end
  end,
}

return {
  MemoryStorage = MemoryStorage,
  FileStorage = FileStorage,
  HEADER = HEADER,
}
