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
# warm up first: pools start, but the shard is not routed until activate/1
MyApp.Shards.add_shard(:shard_10, {Shardex.Adapter.Ecto, repo: MyApp.Repo, config: [database: "s10"]},
  status: :drain
)
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

The Coordinator can also restart on its own after a crash. It then rebuilds
from the boot config: drained or stopped shards come back `:active` and shards
added or removed at runtime are reverted. Shardex emits
`[:shardex, :coordinator, :init]` after every successful boot, so attach a
handler that re-applies your source of truth. The handler runs inside the
Coordinator while it boots, so do the work in another process:

```elixir
defmodule MyApp.ShardexReapply do
  def attach do
    :telemetry.attach("myapp-shardex-reapply", [:shardex, :coordinator, :init], &__MODULE__.handle/4, nil)
  end

  def handle(_event, _measurements, %{instance: MyApp.Shards}, _config) do
    Task.Supervisor.start_child(MyApp.TaskSupervisor, fn ->
      for %{shard: shard, mode: mode} <- MyApp.Metadata.shards_in_maintenance() do
        MyApp.Shards.maintenance(shard, mode)
      end
    end)
  end

  def handle(_event, _measurements, _metadata, _config), do: :ok
end
```

Call `MyApp.ShardexReapply.attach()` before the instance starts (e.g. in
`Application.start/2`) so it also covers the initial boot.

A pool that keeps failing to start also affects restarts: if the Coordinator
crashes and cannot rebuild (with `start_failure: :raise`, any boot-config pool
failing to start stops it), the instance supervisor keeps restarting it until
its restart intensity is exhausted, and then the whole instance goes down.
