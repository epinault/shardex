# Migrating from shardlib

| shardlib | shardex |
| --- | --- |
| `{Shardlib, name: :pg, shards: [...], strategy: &f/2}` | `use Shardex, otp_app: ...` + config |
| `shard_1: RepoShard1` | `shard_1: {Shardex.Adapter.Ecto, repo: RepoShard1}` |
| `{ES.Supervisor, ES.Cluster1, opts}` | `{Shardex.Adapter.Generic, child_spec: {ES.Supervisor, opts}, ref: ES.Cluster1}` |
| separate `*_replicas` instance | `shard_1: [primary: ..., replica: ...]` + `role: :replica` |
| `get_module(key, name: :pg)` | `MyApp.Shards.ref(key)` |
| `get_module!(key)` | `MyApp.Shards.ref!(key)` |
| `get_modules(keys)` + `Enum.group_by` | `MyApp.Shards.group(items, key_fun)` |
| `batch_strategy:` re-checking maintenance | implement `route_many/2`; Shardex checks maintenance |
| strategy returning `:not_found` | return `{:error, :unassigned}` |
| `enable_maintenance(s)` | `maintenance(s, :stop)` (or `:drain`) |
| `disable_maintenance(s)` | `activate(s)` |
| `in_maintenance?(shard)` | `match?({:ok, %{status: st}} when st != :active, status(shard))` |
| `list_shards_info()` | `shards()` |
| `test_mode: true` | `start_pools: false` |
| `[:shardlib, :query]` | `[:shardex, :run, :stop]` |

Strategies now receive `(key, ctx)` / `(keys, ctx)` and must return
`{:ok, shard}` or `{:error, reason}`.
