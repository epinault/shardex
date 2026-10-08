defmodule Shardex.Adapter.EctoTest do
  use ExUnit.Case, async: false

  import Shardex.TestHelpers

  alias Shardex.Adapter
  alias Shardex.Names
  alias Shardex.Test.ModuleRepo
  alias Shardex.Test.SqliteRepo

  @moduletag :tmp_dir

  defp start_dynamic(dir) do
    i = unique_instance()

    shards =
      for s <- [:s1, :s2] do
        {s,
         {Shardex.Adapter.Ecto, repo: SqliteRepo, config: [database: Path.join(dir, "#{s}.db"), pool_size: 1, log: false]}}
      end

    start_supervised!(
      {Shardex, name: i, shards: shards, strategy: {Shardex.Strategy.Static, map: %{"a" => :s1, "b" => :s2}}}
    )

    for key <- ["a", "b"], do: Shardex.run!(i, key, & &1.query!("CREATE TABLE items (name TEXT)"))
    i
  end

  test "dynamic mode routes queries to each shard's database", %{tmp_dir: dir} do
    i = start_dynamic(dir)
    Shardex.run!(i, "a", & &1.query!("INSERT INTO items VALUES ('x')"))
    assert %{rows: [[1]]} = Shardex.run!(i, "a", & &1.query!("SELECT count(*) FROM items"))
    assert %{rows: [[0]]} = Shardex.run!(i, "b", & &1.query!("SELECT count(*) FROM items"))
  end

  test "dynamic mode exposes the dynamic repo name as ref and the repo in meta", %{tmp_dir: dir} do
    i = start_dynamic(dir)
    name = Names.pool(i, :s1, :primary)
    assert Shardex.ref!(i, "a") == name
    {:ok, shard} = Shardex.lookup(i, "a")
    assert shard.roles.primary.meta == %{repo: SqliteRepo, dynamic_name: name}
  end

  # Review focus: the caller's dynamic repo is restored, even on exceptions.
  test "run restores the previous dynamic repo, even when fun raises", %{tmp_dir: dir} do
    i = start_dynamic(dir)
    name = Names.pool(i, :s1, :primary)
    SqliteRepo.put_dynamic_repo(:previous_repo)

    assert Shardex.run!(i, "a", fn repo -> repo.get_dynamic_repo() end) == name
    assert SqliteRepo.get_dynamic_repo() == :previous_repo

    assert_raise RuntimeError, fn -> Shardex.run!(i, "a", fn _repo -> raise "boom" end) end
    assert SqliteRepo.get_dynamic_repo() == :previous_repo
  end

  # Review focus: dynamic repo is process-local; it must be set inside each task.
  test "run_batch with max_concurrency sets the dynamic repo inside each task", %{tmp_dir: dir} do
    i = start_dynamic(dir)

    assert {:ok, results, []} =
             Shardex.run_batch(i, [%{k: "a"}, %{k: "b"}], & &1.k, fn repo, _items -> repo.get_dynamic_repo() end,
               max_concurrency: 2
             )

    assert results == %{s1: Names.pool(i, :s1, :primary), s2: Names.pool(i, :s2, :primary)}
  end

  test "module mode starts the repo and passes the module", %{tmp_dir: dir} do
    Application.put_env(:shardex, ModuleRepo, database: Path.join(dir, "module.db"), pool_size: 1, log: false)
    on_exit(fn -> Application.delete_env(:shardex, ModuleRepo) end)
    i = unique_instance()

    start_supervised!({Shardex, name: i, shards: [s1: {Shardex.Adapter.Ecto, repo: ModuleRepo}]})
    assert Shardex.ref!(i, "x") == ModuleRepo
    assert is_pid(Process.whereis(ModuleRepo))
    assert %{rows: [[1]]} = Shardex.run!(i, "x", & &1.query!("SELECT 1"))
  end

  test "start: false routes to a repo started elsewhere" do
    i = unique_instance()
    start_supervised!({Shardex, name: i, shards: [s1: {Shardex.Adapter.Ecto, repo: ModuleRepo, start: false}]})
    assert Supervisor.which_children(Names.pool_sup(i)) == []
    assert Shardex.ref!(i, "x") == ModuleRepo
  end

  test "a missing :repo is an init error" do
    assert Adapter.build_pool(:i, :s1, :primary, {Shardex.Adapter.Ecto, []}) == {:error, {:missing_option, :repo}}
  end
end
