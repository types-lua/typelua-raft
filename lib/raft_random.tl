--- Deterministic Park–Miller PRNG.
---
--- Tests and the cluster simulator must not depend on `math.random`, whose
--- sequence differs between PUC Lua and LuaJIT. The modulus is 2^31 - 1 and
--- every intermediate product stays below 2^53, so the recurrence is exact in
--- Lua's double-based arithmetic (and on Lua 5.1/LuaJIT).

local MODULUS: integer = 2147483647
local MULTIPLIER: integer = 48271

class Rng {
  state: integer,

  constructor(seed: integer)
    local s: integer = seed % MODULUS
    if s <= 0 then
      s = s + MODULUS - 1
    end
    self.state = s
  end,

  --- Uniform integer in [1, maxInclusive]; 0 when the range is empty.
  int(maxInclusive: integer): integer
    if maxInclusive <= 0 then
      return 0
    end
    return 1 + self:next() % maxInclusive
  end,

  next(): integer
    self.state = (self.state * MULTIPLIER) % MODULUS
    return self.state
  end,
}

return { Rng = Rng, MODULUS = MODULUS }
