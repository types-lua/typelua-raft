import T from "tlua_test"
import codec from "../lib/raft_codec"

return {
  ["string roundtrip is binary safe"] = function(): void
    local samples: string[] = {
      "",
      "hello",
      "a\nb\r\nc",
      "nul\0byte",
      "\255\254\253",
      "ключ=значение",
    }
    for i: integer = 1, #samples do
      local encoded: string = codec.encodeString(samples[i])
      local decoded: DecodedString = codec.decodeString(encoded, 1)
      T.eqBytes(decoded.value, samples[i], "sample " .. tostring(i))
      T.eq(decoded.pos, #encoded + 1, "consumed " .. tostring(i))
    end
  end,

  ["integers roundtrip"] = function(): void
    local samples: integer[] = { 0, 1, 9, 42, 2147483647, -1, -99 }
    for i: integer = 1, #samples do
      local encoded: string = codec.encodeInt(samples[i])
      local decoded: DecodedInt = codec.decodeInt(encoded, 1)
      T.eq(decoded.value, samples[i], "int " .. tostring(samples[i]))
      T.eq(decoded.pos, #encoded + 1, "pos " .. tostring(samples[i]))
    end
  end,

  ["sequence decoding advances"] = function(): void
    local blob: string = codec.encodeString("a")
      .. codec.encodeInt(7)
      .. codec.encodeString("bb")
    local first: DecodedString = codec.decodeString(blob, 1)
    local second: DecodedInt = codec.decodeInt(blob, first.pos)
    local third: DecodedString = codec.decodeString(blob, second.pos)
    T.eq(first.value, "a", "first")
    T.eq(second.value, 7, "second")
    T.eq(third.value, "bb", "third")
    T.eq(third.pos, #blob + 1, "end")
  end,

  ["malformed input raises"] = function(): void
    local missingColon: string = T.raises(function(): void
      codec.decodeString("12", 1)
    end)
    T.ok(string.find(missingColon, "missing ':'", 1, true) ~= nil, "missing colon message")

    T.raises(function(): void
      codec.decodeString("x:abc", 1)
    end)
    T.raises(function(): void
      codec.decodeString("5:abc", 1)
    end)
    T.raises(function(): void
      codec.decodeInt("12x;", 1)
    end)
    T.raises(function(): void
      codec.decodeInt("12", 1)
    end)
    T.raises(function(): void
      codec.decodeString("0:a", 9)
    end)
  end,

  ["checksum detects changes"] = function(): void
    local a: integer = codec.checksum("raft")
    local b: integer = codec.checksum("raft")
    local c: integer = codec.checksum("Raft")
    T.eq(a, b, "stable")
    T.ok(a ~= c, "different input")
    T.ok(codec.checksum("") ~= 0, "empty")
  end,
}
