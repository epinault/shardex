# Maintenance & topology

## Maintenance

```elixir
MyApp.Shards.maintenance(:shard_2, :drain)                 # stop routing, keep pools
MyApp.Shards.maintenance(:shard_2, :stop)                  # stop routing, then stop pools
MyApp.Shards.maintenance(:shard_1, :drain, role: :replica) # one role only
MyApp.Shards.activate(:shard_2)                            # start pools, then route
MyApp.Shards.status(:shard_2)
#=> {:ok, %{status: :active, roles: %{primary: :active}}}
MyApp.Shards.shards()                                      # includes meta.pids
```

Calls are idempotent. Taking a shard out publishes the new status **before**
stopping pools; activating starts pools **before** publishing. Calls already
in flight are not tracked: `:stop` relies on the pool's own shutdown.

## Adding and removing shards

```elixir
MyApp.Shards.add_shard(:shard_9, primary: {Shardex.Adapter.Ecto, repo: MyApp.Repo, config: [database: "s9"]})
MyApp.Shards.add_shard(:shard_10, spec, status: :drain)    # warm up first
MyApp.Shards.remove_shard(:shard_9)
```

With `Hash`, adding or removing a shard remaps most keys (a warning is
logged). With `JumpHash`, appending moves ~1/n keys to the new shard. With a
directory strategy, nothing moves until you assign keys to the new shard.

## Persistence

Runtime changes live in memory. If the instance restarts, the boot config
wins. Keep your source of truth (database, config service) and re-apply it at
boot and when it changes:

```elixir
for %{shard: shard, mode: mode} <- MyApp.Metadata.shards_in_maintenance() do
  MyApp.Shards.maintenance(shard, mode)
end
```
