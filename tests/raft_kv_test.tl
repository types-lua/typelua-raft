import T from "tlua_test"
import kv from "../lib/raft_kv"

local function command(index: integer, encoded: string): LogEntry
  return { term = 1, kind = "command", command = encoded }
end

return {
  ["put and delete apply in order"] = function(): void
    local store = kv.KVStore.new()
    store:apply(1, command(1, kv.putCommand("a", "1")))
    store:apply(2, command(1, kv.putCommand("b", "2")))
    T.eq(store:get("a"), "1", "a")
    T.eq(store:get("b"), "2", "b")
    T.eq(store:size(), 2, "size")
    store:apply(3, command(1, kv.deleteCommand("a")))
    T.eq(store:get("a"), nil, "deleted")
    T.eq(store:get("b"), "2", "kept")
    T.eq(store:size(), 1, "size after delete")
  end,

  ["noop advances applied without mutating state"] = function(): void
    local store = kv.KVStore.new()
    store:apply(1, { term = 2, kind = "noop", command = "" })
    T.eq(store.applied, 1, "applied")
    T.eq(store:size(), 0, "empty")
  end,

  ["replay is idempotent"] = function(): void
    local store = kv.KVStore.new()
    local put: LogEntry = command(1, kv.putCommand("k", "v"))
    store:apply(1, put)
    store:apply(1, put)
    T.eq(store:get("k"), "v", "value")
    T.eq(store:size(), 1, "no duplicate state")
  end,

  ["snapshot is deterministic and restores"] = function(): void
    local a = kv.KVStore.new()
    a:apply(1, command(1, kv.putCommand("b", "2")))
    a:apply(2, command(1, kv.putCommand("a", "1")))
    a:apply(3, command(1, kv.putCommand("c", "3")))

    local b = kv.KVStore.new()
    b:apply(1, command(1, kv.putCommand("c", "3")))
    b:apply(2, command(1, kv.putCommand("a", "1")))
    b:apply(3, command(1, kv.putCommand("b", "2")))

    local image: string = a:snapshot()
    T.eqBytes(image, b:snapshot(), "key order is stable")

    local restored = kv.KVStore.new()
    restored:restore(image)
    T.eq(restored:size(), 3, "size")
    T.eq(restored:get("a"), "1", "a")
    T.eq(restored:get("b"), "2", "b")
    T.eq(restored:get("c"), "3", "c")
    T.eqBytes(restored:snapshot(), image, "snapshot roundtrip")
  end,

  ["binary keys and values"] = function(): void
    local store = kv.KVStore.new()
    local key: string = "k\0\n\255"
    local value: string = "v\1\2\0"
    store:apply(1, command(1, kv.putCommand(key, value)))
    T.eqBytes(store:get(key)!, value, "binary value")
    local restored = kv.KVStore.new()
    restored:restore(store:snapshot())
    T.eqBytes(restored:get(key)!, value, "binary snapshot")
  end,

  ["invalid commands fail loudly"] = function(): void
    local store = kv.KVStore.new()
    T.raises(function(): void
      store:apply(1, command(1, "bogus"))
    end)
    T.raises(function(): void
      store:apply(1, command(1, "3:put2:ab"))
    end)
    T.raises(function(): void
      store:restore("1:2:ab")
    end)
  end,
}
