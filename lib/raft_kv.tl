--- A tiny deterministic key/value state machine for the Raft demo and tests.
---
--- Commands are length-prefixed binary strings, not ad-hoc `key=value` text,
--- so keys and values may contain any byte (including NUL and newlines).
--- Snapshots are deterministic: keys are sorted before encoding, which makes
--- equality checks and golden tests stable.

import codec from "./raft_codec"

local function putCommand(key: string, value: string): string
  return codec.encodeString("put") .. codec.encodeString(key) .. codec.encodeString(value)
end

local function deleteCommand(key: string): string
  return codec.encodeString("del") .. codec.encodeString(key)
end

class KVStore implements StateMachine {
  state: map<string, string | nil> = {},
  applied: integer = 0,

  constructor() end,

  apply(index: integer, entry: LogEntry): void
    if index <= self.applied then
      return
    end
    if entry.kind == "noop" then
      self.applied = index
      return
    end
    local head: DecodedString = codec.decodeString(entry.command, 1)
    local op: string = head.value
    local pos: integer = head.pos
    if op == "put" then
      local keyField: DecodedString = codec.decodeString(entry.command, pos)
      local valueField: DecodedString = codec.decodeString(entry.command, keyField.pos)
      if valueField.pos ~= #entry.command + 1 then
        error("KVStore: trailing bytes in put command")
      end
      self.state[keyField.value] = valueField.value
    elseif op == "del" then
      local keyField: DecodedString = codec.decodeString(entry.command, pos)
      if keyField.pos ~= #entry.command + 1 then
        error("KVStore: trailing bytes in del command")
      end
      self.state[keyField.value] = nil
    else
      error("KVStore: unknown command '" .. op .. "'")
    end
    self.applied = index
  end,

  get(key: string): string | nil
    return self.state[key]
  end,

  size(): integer
    local n: integer = 0
    for _key, _value in pairs(self.state) do
      n = n + 1
    end
    return n
  end,

  snapshot(): string
    local keys: string[] = {}
    for key, _ in pairs(self.state) do
      keys[#keys + 1] = key
    end
    table.sort(keys)
    local parts: string[] = { codec.encodeInt(#keys) }
    for i: integer = 1, #keys do
      local key: string = keys[i]
      local value: string | nil = self.state[key]
      if value == nil then
        error("KVStore: snapshot lost key '" .. key .. "'")
      end
      parts[#parts + 1] = codec.encodeString(key)
      parts[#parts + 1] = codec.encodeString(value!)
    end
    return table.concat(parts)
  end,

  restore(data: string): void
    local countField: DecodedInt = codec.decodeInt(data, 1)
    local count: integer = countField.value
    local pos: integer = countField.pos
    if count < 0 or count > #data then
      error("KVStore: bad snapshot count " .. tostring(count))
    end
    local restored: map<string, string | nil> = {}
    for i: integer = 1, count do
      local keyField: DecodedString = codec.decodeString(data, pos)
      local valueField: DecodedString = codec.decodeString(data, keyField.pos)
      pos = valueField.pos
      restored[keyField.value] = valueField.value
    end
    if pos ~= #data + 1 then
      error("KVStore: trailing bytes in snapshot")
    end
    self.state = restored
    self.applied = 0
  end,
}

return {
  KVStore = KVStore,
  putCommand = putCommand,
  deleteCommand = deleteCommand,
}
