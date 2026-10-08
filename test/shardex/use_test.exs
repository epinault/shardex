defmodule Shardex.UseTest do
  use ExUnit.Case, async: false

  import Shardex.TestHelpers

  alias Shardex.Shard
  alias Shardex.Strategy.Static
  alias Shardex.Test.MyShards

  setup do
    Application.put_env(:shardex, MyShards,
      shards: [s1: agent_pool(MyShards, :s1), s2: agent_pool(MyShards, :s2)],
      strategy: {Static, map: %{"a" => :s1, "b" => :s2}}
    )

    on_exit(fn -> Application.delete_env(:shardex, MyShards) end)
  end

  test "reads config from the otp_app and exposes the API without the instance argument" do
    start_supervised!(MyShards)

    assert MyShards.run!("a", &Agent.get(&1, fn s -> s end)) == {:s1, :primary}
    assert {:ok, {:s1, :primary}} = MyShards.run("a", &Agent.get(&1, fn s -> s end))
    assert {:ok, %Shard{name: :s2}} = MyShards.lookup("b")
    assert {:ok, ref} = MyShards.ref("b")
    assert MyShards.ref!("b") == ref
    assert ref == agent_name(MyShards, :s2)
    assert %Shardex.Batch{} = MyShards.group([], & &1)
    assert {:ok, %{s1: [%{k: "a"}]}, []} = MyShards.run_batch([%{k: "a"}], & &1.k, fn _ref, items -> items end)
    assert :ok = MyShards.maintenance(:s2, :drain)
    assert {:ok, %{status: :drain}} = MyShards.status(:s2)
    assert :ok = MyShards.activate(:s2)
    assert length(MyShards.shards()) == 2
    assert :ok = MyShards.add_shard(:s3, agent_pool(MyShards, :s3))
    assert :ok = MyShards.remove_shard(:s3)
    assert MyShards.invalidate("a") == {:error, :not_cached}
  end

  test "start options override application config" do
    start_supervised!({MyShards, strategy: {Static, map: %{"a" => :s2}}})
    assert {:ok, %Shard{name: :s2}} = MyShards.lookup("a")
  end
end
