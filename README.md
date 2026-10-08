# Shardex

Route keys — and batches of items — to sharded pools: Ecto repos, Redis,
Elasticsearch, or anything with a child spec.

- **Backend-agnostic** adapters: `Shardex.Adapter.Ecto`, `Shardex.Adapter.Redix`,
  `Shardex.Adapter.Generic` (Elasticsearch, Finch, ...).
- **Batch-first routing**: group thousands of items by shard with one strategy call.
- **Roles** per shard (`:primary`, `:replica`, ...) with fallback.
- **Pluggable strategies**: hash (default), jump consistent hash, static maps, or
  your own (e.g. a database directory), with an optional cache.
- **Maintenance** (`:drain` / `:stop`) and **runtime** `add_shard` / `remove_shard`.
- **Telemetry** for routing, runs, batches and status changes.

## Installation

```elixir
def deps do
  [
    {:shardex, "~> 0.1"},
    # optional, depending on your pools:
    {:ecto_sql, "~> 3.12"},
    {:redix, "~> 1.5"}
  ]
end
```

## Quick start

```elixir
defmodule MyApp.Shards do
  use Shardex, otp_app: :my_app
end

# config/runtime.exs
config :my_app, MyApp.Shards,
  strategy: Shardex.Strategy.JumpHash,
  shards: [
    shard_1: [
      primary: {Shardex.Adapter.Ecto, repo: MyApp.Repo, config: [database: "app_s1"]},
      replica: {Shardex.Adapter.Ecto, repo: MyApp.Repo, config: [database: "app_s1", hostname: "replica-1"]}
    ],
    shard_2: {Shardex.Adapter.Ecto, repo: MyApp.Repo, config: [database: "app_s2"]}
  ]

# application.ex
children = [MyApp.Shards]
```

```elixir
# One key
{:ok, contacts} = MyApp.Shards.run(org_id, fn repo -> repo.all(Contact) end)
MyApp.Shards.run(org_id, &read_report/1, role: :replica, fallback: :primary)

# Many items, one routing pass, one call per shard
{:ok, results, errors} =
  MyApp.Shards.run_batch(messages, & &1.org_id, fn repo, msgs ->
    repo.insert_all(Event, Enum.map(msgs, &to_row/1))
  end)

# Maintenance
:ok = MyApp.Shards.maintenance(:shard_2, :drain)
:ok = MyApp.Shards.activate(:shard_2)
```

## Guides

- [Getting started](guides/getting-started.md)
- [Ecto](guides/ecto.md) · [Redis](guides/redis.md) · [Elasticsearch](guides/elasticsearch.md)
- [Strategies](guides/strategies.md) · [Batching](guides/batching.md)
- [Maintenance & topology](guides/maintenance.md) · [Telemetry](guides/telemetry.md)
- [Migrating from shardlib](guides/migrating-from-shardlib.md)

## License

MIT
