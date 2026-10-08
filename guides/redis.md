# Redis

`Shardex.Adapter.Redix` requires `:redix`.

```elixir
shards: [
  cache_1: {Shardex.Adapter.Redix, host: "redis-1", port: 6379},
  cache_2: {Shardex.Adapter.Redix, host: "redis-2", port: 6379, pool_size: 4}
]
```

All options except `:pool_size` go to `Redix.start_link/1`. With
`pool_size: 1` the ref is the connection name; with `pool_size > 1` the ref is
a `%Shardex.Adapter.Redix.Pool{}` and `run/3` passes one connection name chosen
at random:

```elixir
MyApp.Cache.run(key, &Redix.command(&1, ["GET", key]))

MyApp.Cache.run_batch(keys, & &1, fn conn, keys ->
  Redix.command(conn, ["MGET" | keys])
end)
```
