defmodule Shardex.NamesTest do
  use ExUnit.Case, async: true

  alias Shardex.Names

  test "derives process and table names from the instance" do
    assert Names.coordinator(MyApp.Shards) == MyApp.Shards.Coordinator
    assert Names.pool_sup(MyApp.Shards) == MyApp.Shards.PoolSup
    assert Names.table(MyApp.Shards) == MyApp.Shards
  end

  test "derives a pool name from instance, shard and role" do
    assert Names.pool(MyApp.Shards, :shard_1, :replica) == :"Elixir.MyApp.Shards.shard_1.replica"
  end

  test "works with plain atom instance names" do
    assert Names.coordinator(:my_shards) == :"Elixir.my_shards.Coordinator"
  end
end
