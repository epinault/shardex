# Ecto

`Shardex.Adapter.Ecto` requires `:ecto` (usually via `:ecto_sql`).

## Dynamic mode: one repo module, many databases

```elixir
shards: [
  shard_1: [
    primary: {Shardex.Adapter.Ecto, repo: MyApp.Repo, config: [database: "s1"]},
    replica: {Shardex.Adapter.Ecto, repo: MyApp.Repo, config: [database: "s1", hostname: "replica-1"]}
  ]
]
```

Shardex starts `MyApp.Repo` once per pool, named `MyApp.Shards.shard_1.primary`
(etc.), with `config` merged over the repo's application config. `run/3` sets
the dynamic repo, calls your function with `MyApp.Repo`, and restores the
previous dynamic repo afterwards — also when the function raises:

```elixir
MyApp.Shards.run(org_id, fn repo -> repo.get(Contact, id) end)
```

Dynamic repos are **per process**. Code that spawns processes inside `run`
must set the dynamic repo again there. `run_batch(..., max_concurrency: n)`
already does this for each task.

### In a Plug

If you prefer to set the dynamic repo for the whole request:

```elixir
def call(conn, _opts) do
  case MyApp.Shards.ref(conn.assigns.org_id) do
    {:ok, dynamic_name} ->
      MyApp.Repo.put_dynamic_repo(dynamic_name)
      conn

    {:error, :maintenance} ->
      conn |> send_resp(503, "maintenance") |> halt()

    {:error, _reason} ->
      conn |> send_resp(500, "no shard for this organization") |> halt()
  end
end
```

The `%Shardex.Pool{meta: %{repo: MyApp.Repo, dynamic_name: name}}` of a shard
(`lookup/1` returns the shard; its pools are in `shard.roles`) carries both
values if you need them.

## Module mode: one repo module per shard

```elixir
shards: [
  shard_1: {Shardex.Adapter.Ecto, repo: MyApp.RepoShard1},
  shard_2: {Shardex.Adapter.Ecto, repo: MyApp.RepoShard2}
]
```

The ref is the module, and `run/3` simply calls `fun.(MyApp.RepoShard1)`.

Shardex starts each repo module under its own supervisor. If your application
already starts the repo (e.g. it is in your supervision tree), pass
`start: false`. Otherwise Shardex finds the repo's name already registered,
treats that process as its pool and routes to it, but does not own it:
`maintenance(shard, :stop)` will not stop it.

## Repos started elsewhere

Add `start: false` to route to a repo you already start in your own tree.

## Migrations

Shardex does not run migrations. With dynamic mode, iterate over
`MyApp.Shards.shards()` and run `Ecto.Migrator` against each pool's
`dynamic_name`, or keep per-shard repo modules for migrations.
