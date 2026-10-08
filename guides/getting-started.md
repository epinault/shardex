# Getting started

A Shardex **instance** owns a set of **shards**. Each shard has one **pool**
per **role** (`:primary` unless you name roles). A **strategy** maps keys to
shard names; an **adapter** knows how to start a pool and how to hand it to
your code.

## Define an instance

```elixir
defmodule MyApp.Shards do
  use Shardex, otp_app: :my_app
end
```

Configure it in `config/runtime.exs` (or pass the same options to
`start_link/1` / the child spec — they are merged over the app config):

```elixir
config :my_app, MyApp.Shards,
  shards: [
    shard_1: {Shardex.Adapter.Ecto, repo: MyApp.Repo, config: [database: "s1"]},
    shard_2: {Shardex.Adapter.Ecto, repo: MyApp.Repo, config: [database: "s2"]}
  ]
```

Options: `:shards` (required), `:strategy` (default `Shardex.Strategy.Hash`),
`:start_pools` (default `true`; set `false` in tests that don't need real
pools), `:start_failure` (`:raise` or `:stop_shard`).

Add `MyApp.Shards` to your supervision tree.

## Route

| Function | Returns |
| --- | --- |
| `lookup(key)` | `{:ok, %Shardex.Shard{}}` |
| `ref(key, role: r)` / `ref!` | the pool's ref (repo name, connection name, ...) |
| `run(key, fun)` / `run!` | `fun.(ref)` inside the adapter's context |
| `group(items, key_fun)` | `%Shardex.Batch{}` |
| `run_batch(items, key_fun, fun)` | `{:ok, %{shard => result}, errors}` |

Errors are `{:error, reason}` with `reason` one of `:unassigned`,
`:maintenance`, `:no_routable_shards`, `{:unknown_shard, name}`,
`{:unknown_role, role}`, `{:strategy_error, value}`, or an error your strategy
returned. Bang functions raise `Shardex.Error`.

## Without `use`

```elixir
children = [{Shardex, name: MyShards, shards: [...]}]
Shardex.run(MyShards, key, fun)
```

## Testing your app

- `start_pools: false` registers shards and refs without starting pools.
- `strategy: {Shardex.Strategy.Static, map: %{"org_1" => :shard_1}}` gives
  deterministic routing.
