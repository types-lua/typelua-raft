import T from "tlua_test"
import storage from "../lib/raft_storage"

local function entry(term: integer, kind: EntryKind, command: string): LogEntry
  return { term = term, kind = kind, command = command }
end

local function writeRaw(path: string, data: string): void
  local file = io.open(path, "wb")
  if file == nil then
    error("test: cannot write " .. path)
  end
  file:write(data)
  file:close()
end

local function freshPath(): string
  local path: string = os.tmpname()
  os.remove(path)
  return path
end

return {
  ["memory storage copies in and out"] = function(): void
    local s = storage.MemoryStorage.new()
    s:saveHardState(9, "n2")
    local saved: LogEntry[] = { entry(9, "command", "a") }
    s:saveLog(4, 3, saved)
    s:saveSnapshot({ index = 4, term = 3, data = "image" })

    saved[1].command = "mutated"
    local loaded: LogEntry[] = s:loadLogEntries()
    T.eq(loaded[1].command, "a", "entry copied on save")
    loaded[1].command = "mutated again"
    T.eq(s:loadLogEntries()[1].command, "a", "entry copied on load")

    T.eq(s:loadTerm(), 9, "term")
    T.eq(s:loadVotedFor(), "n2", "vote")
    T.eq(s:loadLogBase(), 4, "base")
    T.eq(s:loadLogBaseTerm(), 3, "base term")
    local snap: Snapshot | nil = s:loadSnapshot()
    if snap == nil then
      error("missing snapshot")
    end
    T.eq(snap!.index, 4, "snapshot index")
    T.eq(snap!.data, "image", "snapshot data")
  end,

  ["file storage roundtrip including binary data"] = function(): void
    local path: string = freshPath()
    local first = storage.FileStorage.new(path)
    first:saveHardState(7, "node-b")
    first:saveLog(2, 1, {
      entry(1, "command", "put\0key"),
      entry(2, "noop", ""),
      entry(2, "command", "line1\nline2"),
    })
    first:saveSnapshot({ index = 2, term = 1, data = "\0\1snapshot\n" })

    local second = storage.FileStorage.new(path)
    T.eq(second:loadTerm(), 7, "term")
    T.eq(second:loadVotedFor(), "node-b", "vote")
    T.eq(second:loadLogBase(), 2, "base")
    T.eq(second:loadLogBaseTerm(), 1, "base term")
    local entries: LogEntry[] = second:loadLogEntries()
    T.eq(#entries, 3, "entries")
    T.eqBytes(entries[1].command, "put\0key", "binary command")
    T.eq(entries[2].kind, "noop", "kind")
    T.eqBytes(entries[3].command, "line1\nline2", "newline command")
    local snap: Snapshot | nil = second:loadSnapshot()
    if snap == nil then
      error("missing snapshot")
    end
    T.eqBytes(snap!.data, "\0\1snapshot\n", "snapshot bytes")

    -- Overwriting replaces the whole file, not just the tail.
    second:saveHardState(8, nil)
    local third = storage.FileStorage.new(path)
    T.eq(third:loadTerm(), 8, "updated term")
    T.eq(third:loadVotedFor(), nil, "cleared vote")
    T.eq(#third:loadLogEntries(), 3, "log kept")
    os.remove(path)
  end,

  ["file storage rejects corruption"] = function(): void
    local path: string = freshPath()
    writeRaw(path, "not a raft file")
    local headerErr: string = T.raises(function(): void
      storage.FileStorage.new(path)
    end)
    T.ok(string.find(headerErr, "bad header", 1, true) ~= nil, "header message")

    local validPath: string = freshPath()
    local writer = storage.FileStorage.new(validPath)
    writer:saveHardState(3, "n1")
    writer:saveLog(0, 0, { entry(1, "command", "abcdef") })
    local raw: string | nil = nil
    local file = io.open(validPath, "rb")
    if file ~= nil then
      raw = file:read("*a")
      file:close()
    end
    if raw == nil then
      error("test: could not reread storage")
    end

    -- Corrupt the stored checksum itself (the last digit before `;`) so the
    -- payload still parses and only the integrity check can reject it.
    local bytes: string = raw!
    local digitAt: integer = #bytes - 1
    local original: integer = string.byte(bytes, digitAt)!
    local replacement: string = "0"
    if original == 48 then
      replacement = "1"
    end
    local flipped: string = string.sub(bytes, 1, digitAt - 1)
      .. replacement
      .. string.sub(bytes, digitAt + 1)
    writeRaw(path, flipped)
    local sumErr: string = T.raises(function(): void
      storage.FileStorage.new(path)
    end)
    T.ok(string.find(sumErr, "checksum mismatch", 1, true) ~= nil, "checksum message")

    local truncated: string = raw!
    writeRaw(path, string.sub(truncated, 1, #truncated - 2))
    T.raises(function(): void
      storage.FileStorage.new(path)
    end)

    os.remove(path)
    os.remove(validPath)
  end,
}
