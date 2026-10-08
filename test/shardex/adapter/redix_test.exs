defmodule Shardex.Adapter.RedixTest do
  use ExUnit.Case, async: true

  import Shardex.TestHelpers

  alias Shardex.Adapter
  alias Shardex.Adapter.Redix.Pool
  alias Shardex.Names

  # Redix connects asynchronously, so these tests run without a Redis server
  # (port 1 is never open); connection errors are logged and captured.
  @moduletag :capture_log

  setup do
    {:ok, instance: unique_instance()}
  end

  test "pool_size 1 starts one named connection", %{instance: i} do
    start_supervised!({Shardex, name: i, shards: [s1: {Shardex.Adapter.Redix, host: "localhost", port: 1}]})
    name = Names.pool(i, :s1, :primary)
    assert Shardex.ref!(i, "k") == name
    assert is_pid(Process.whereis(name))
    assert Shardex.run!(i, "k", & &1) == name
  end

  test "pool_size n starts n connections and run picks one", %{instance: i} do
    start_supervised!({Shardex, name: i, shards: [s1: {Shardex.Adapter.Redix, host: "localhost", port: 1, pool_size: 3}]})
    assert %Pool{names: names} = Shardex.ref!(i, "k")
    assert tuple_size(names) == 3
    assert Enum.all?(Tuple.to_list(names), &is_pid(Process.whereis(&1)))

    for _attempt <- 1..20 do
      assert Shardex.run!(i, "k", & &1) in Tuple.to_list(names)
    end
  end

  test "an invalid pool size is an init error" do
    assert Adapter.build_pool(:i, :s1, :primary, {Shardex.Adapter.Redix, pool_size: 0}) ==
             {:error, {:invalid_option, :pool_size, 0}}
  end

  @tag :redis
  test "talks to a real Redis server", %{instance: i} do
    uri = URI.parse(System.fetch_env!("REDIS_URL"))
    start_supervised!({Shardex, name: i, shards: [s1: {Shardex.Adapter.Redix, host: uri.host, port: uri.port}]})
    assert Shardex.run!(i, "k", &Redix.command!(&1, ["PING"])) == "PONG"
  end
end
