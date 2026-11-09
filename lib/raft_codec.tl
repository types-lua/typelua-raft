--- Length-prefixed binary encoding used by storage and the KV state machine.
---
--- Strings are netstrings: `<byte length>:<raw bytes>`. They are binary safe
--- (embedded NUL/newlines included) and trivial to parse without a JSON
--- dependency. Integers are decimal followed by `;`, validated on read so a
--- corrupted file fails loudly instead of becoming a negative array index.
---
--- Decoders return `{ value, pos }` records instead of tuples: TypeLua's
--- module inference widens cross-module tuple returns, and an explicit record
--- keeps every caller under strict type checking.

local function encodeString(value: string): string
  return tostring(#value) .. ":" .. value
end

local function encodeInt(value: integer): string
  return tostring(value) .. ";"
end

--- Reads one netstring. `pos` is 1-based; the returned `pos` points after it.
local function decodeString(data: string, pos: integer): DecodedString
  if pos < 1 or pos > #data + 1 then
    error("decodeString: position out of range: " .. tostring(pos))
  end
  local colon: integer | nil = string.find(data, ":", pos)
  if colon == nil then
    error("decodeString: missing ':' at byte " .. tostring(pos))
  end
  local colonAt: integer = colon!
  local lengthText: string = string.sub(data, pos, colonAt - 1)
  if string.match(lengthText, "^%d+$") == nil then
    error("decodeString: bad length '" .. lengthText .. "'")
  end
  local parsed: number | nil = tonumber(lengthText)
  if parsed == nil then
    error("decodeString: bad length '" .. lengthText .. "'")
  end
  local length: integer = math.floor(parsed!)
  local first: integer = colonAt + 1
  local last: integer = first + length - 1
  if last > #data then
    error("decodeString: truncated payload (want " .. tostring(length) .. " bytes)")
  end
  return { value = string.sub(data, first, last), pos = last + 1 }
end

--- Reads one decimal integer terminated by `;`.
local function decodeInt(data: string, pos: integer): DecodedInt
  if pos < 1 or pos > #data + 1 then
    error("decodeInt: position out of range: " .. tostring(pos))
  end
  local semi: integer | nil = string.find(data, ";", pos)
  if semi == nil then
    error("decodeInt: missing ';' at byte " .. tostring(pos))
  end
  local semiAt: integer = semi!
  local text: string = string.sub(data, pos, semiAt - 1)
  if string.match(text, "^-?%d+$") == nil then
    error("decodeInt: not an integer: '" .. text .. "'")
  end
  local parsed: number | nil = tonumber(text)
  if parsed == nil then
    error("decodeInt: not an integer: '" .. text .. "'")
  end
  return { value = math.floor(parsed!), pos = semiAt + 1 }
end

--- Fast non-cryptographic checksum (mod 2^31-1, multiplier small enough that
--- the intermediate product stays below 2^53). Detects truncation and bit
--- rot; it is not a security primitive.
local function checksum(value: string): integer
  local hash: integer = 5381
  for i: integer = 1, #value do
    local byte: integer = string.byte(value, i)!
    hash = (hash * 131 + byte) % 2147483647
  end
  return hash
end

return {
  encodeString = encodeString,
  encodeInt = encodeInt,
  decodeString = decodeString,
  decodeInt = decodeInt,
  checksum = checksum,
}
