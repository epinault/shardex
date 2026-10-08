if Code.ensure_loaded?(Ecto.Repo) do
  defmodule Shardex.Adapter.Ecto do
    @moduledoc """
    Adapter for Ecto repos. Requires the optional `:ecto` dependency.

    **Dynamic mode** (one repo module, many databases) — pass `:config`:

        shard_1: {Shardex.Adapter.Ecto, repo: MyApp.Repo, config: [database: "s1"]}

    Shardex starts `MyApp.Repo` with `name: MyApp.Shards.shard_1.primary` plus
    `config` (merged over the repo's app config). `run/2` sets that dynamic repo
    with `put_dynamic_repo/1`, calls `fun.(MyApp.Repo)` and restores the
    previous dynamic repo. `ref/3` returns the dynamic repo name, so a Plug can
    do `MyApp.Repo.put_dynamic_repo(ref)` itself.

    **Module mode** (one repo module per shard) — omit `:config`:

        shard_1: {Shardex.Adapter.Ecto, repo: MyApp.RepoShard1}

    Options:

      * `:repo` (required) - the `Ecto.Repo` module
      * `:config` - start options for dynamic mode
      * `:start` - set to `false` when the repo is started elsewhere (default `true`)
    """

    @behaviour Shardex.Adapter

    @impl true
    def init(opts, %{instance: instance, shard: shard, role: role}) do
      with {:ok, repo} <- fetch_repo(opts) do
        start? = Keyword.get(opts, :start, true)
        name = Shardex.Names.pool(instance, shard, role)
        {:ok, build(repo, Keyword.fetch(opts, :config), name, start?)}
      end
    end

    defp build(repo, {:ok, config}, name, start?) do
      spec = if start?, do: {repo, Keyword.put(config, :name, name)}
      meta = %{repo: repo, dynamic_name: name}
      %{state: {:dynamic, repo, name}, ref: name, child_spec: spec, meta: meta}
    end

    defp build(repo, :error, _name, start?) do
      spec = if start?, do: repo
      meta = %{repo: repo, dynamic_name: nil}
      %{state: {:module, repo}, ref: repo, child_spec: spec, meta: meta}
    end

    @impl true
    def run({:dynamic, repo, name}, fun) do
      previous = repo.get_dynamic_repo()
      repo.put_dynamic_repo(name)

      try do
        fun.(repo)
      after
        repo.put_dynamic_repo(previous)
      end
    end

    def run({:module, repo}, fun), do: fun.(repo)

    defp fetch_repo(opts) do
      case Keyword.fetch(opts, :repo) do
        {:ok, repo} when is_atom(repo) -> {:ok, repo}
        _missing -> {:error, {:missing_option, :repo}}
      end
    end
  end
end
