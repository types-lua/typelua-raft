import T from "tlua_test"
import random from "../lib/raft_random"

local function sequence(seed: integer, n: integer): string
  local rng = random.Rng.new(seed)
  local parts: string[] = {}
  for i: integer = 1, n do
    parts[#parts + 1] = tostring(rng:int(1000))
  end
  return table.concat(parts, ",")
end

return {
  ["same seed, same sequence"] = function(): void
    T.eq(sequence(42, 12), sequence(42, 12), "deterministic")
    T.ok(sequence(42, 12) ~= sequence(43, 12), "different seeds")
  end,

  ["int stays in range"] = function(): void
    local rng = random.Rng.new(7)
    for i: integer = 1, 500 do
      local v: integer = rng:int(9)
      T.ok(v >= 1 and v <= 9, "range")
    end
    T.eq(rng:int(0), 0, "empty range")
    T.eq(rng:int(-3), 0, "negative range")
  end,

  ["zero and negative seeds are usable"] = function(): void
    local a = random.Rng.new(0)
    local b = random.Rng.new(-1)
    T.ok(a:next() >= 1, "zero seed")
    T.ok(b:next() >= 1, "negative seed")
  end,
}
