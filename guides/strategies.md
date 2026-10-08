# Strategies

A strategy maps keys to shard names. Configure it with `strategy:`.

| Strategy | Use when |
| --- | --- |
| `Shardex.Strategy.Hash` (default) | fixed shard count |
| `Shardex.Strategy.JumpHash` | the shard count grows: appending a shard moves only ~1/n keys |
| `{Shardex.Strategy.Static, map: %{key => shard}, default: shard}` | small fixed maps, tests |
| your module or function | assignments live elsewhere (e.g. a database) |
| `{Shardex.Strategy.Cached, inner: ..., ttl: ms}` | cache an expensive strategy |

Hash strategies hash over the **full** shard list, so a shard in maintenance
returns `{:error, :maintenance}` for its keys instead of remapping them.

## Writing a strategy

Only `route_many/2` is required. Return `{:ok, shard}` or `{:error, reason}`
per key; keys you leave out are `{:error, :unassigned}`. Shardex validates the
shard names you return (unknown names, including strings, become
`{:error, {:unknown_shard, name}}`).

```elixir
defmodule MyApp.ShardDirectory do
  @behaviour Shardex.Strategy

  @impl true
  def route_many(org_ids, ctx) do
    known = Map.new(ctx.shards, &{Atom.to_string(&1), &1})

    MyApp.Metadata.shard_names_for(org_ids)   # %{org_id => "shard_1"}
    |> Map.new(fn {org_id, name} ->
      case Map.fetch(known, name) do
        {:ok, shard} -> {org_id, {:ok, shard}}
        :error -> {org_id, {:error, {:unknown_shard, name}}}
      end
    end)
  end
end

strategy: {Shardex.Strategy.Cached, inner: MyApp.ShardDirectory, ttl: :timer.minutes(10)}
```

Optional callbacks: `init/2` (build state, runs in the Coordinator),
`route/2` (a faster single-key path), `on_topology_change/2`, `trace?/0`.

A function works too: `strategy: fn key, ctx -> {:ok, hd(ctx.shards)} end`.

When an assignment changes, call `MyApp.Shards.invalidate(org_id)`.
