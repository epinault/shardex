defmodule Shardex.CachedTest do
  use ExUnit.Case, async: true

  import Shardex.TestHelpers

  alias Shardex.Strategy.Cached
  alias Shardex.Test.RecordingStrategy

  setup do
    {:ok, instance: unique_instance()}
  end

  defp start(i, cached_opts \\ []) do
    inner = {RecordingStrategy, pid: self(), routes: %{"err" => {:error, :unassigned}}}

    start_supervised!(
      {Shardex,
       name: i,
       shards: [s1: agent_pool(i, :s1), s2: agent_pool(i, :s2)],
       strategy: {Cached, [inner: inner] ++ cached_opts}}
    )

    i
  end

  test "only cache misses reach the inner strategy", %{instance: i} do
    start(i)
    Shardex.group(i, ["a", "b"], & &1)
    assert_received {:route_many, ["a", "b"]}

    Shardex.group(i, ["a", "b", "c"], & &1)
    assert_received {:route_many, ["c"]}

    assert {:ok, _shard} = Shardex.lookup(i, "a")
    refute_received {:route_many, _keys}
  end

  test "entries expire after ttl", %{instance: i} do
    start(i, ttl: 30)
    Shardex.lookup(i, "a")
    assert_received {:route_many, ["a"]}
    Process.sleep(50)
    Shardex.lookup(i, "a")
    assert_received {:route_many, ["a"]}
  end

  test "errors are not cached by default", %{instance: i} do
    start(i)
    assert Shardex.lookup(i, "err") == {:error, :unassigned}
    assert Shardex.lookup(i, "err") == {:error, :unassigned}
    assert_received {:route_many, ["err"]}
    assert_received {:route_many, ["err"]}
  end

  test "errors are cached with cache_errors: true", %{instance: i} do
    start(i, cache_errors: true)
    Shardex.lookup(i, "err")
    Shardex.lookup(i, "err")
    assert_received {:route_many, ["err"]}
    refute_received {:route_many, ["err"]}
  end

  test "invalidate/2 drops one key or everything", %{instance: i} do
    start(i)
    Shardex.group(i, ["a", "b"], & &1)
    assert_received {:route_many, ["a", "b"]}

    assert :ok = Shardex.invalidate(i, "a")
    Shardex.group(i, ["a", "b"], & &1)
    assert_received {:route_many, ["a"]}

    assert :ok = Shardex.invalidate(i, :all)
    Shardex.group(i, ["a", "b"], & &1)
    assert_received {:route_many, ["a", "b"]}
  end

  test "invalidate/2 requires the Cached strategy", %{instance: i} do
    start_supervised!({Shardex, name: i, shards: [s1: agent_pool(i, :s1)]})
    assert Shardex.invalidate(i, "a") == {:error, :not_cached}
  end

  test "topology changes clear the cache", %{instance: i} do
    start(i)
    Shardex.lookup(i, "a")
    assert_received {:route_many, ["a"]}

    assert :ok = Shardex.add_shard(i, :s3, agent_pool(i, :s3))
    Shardex.lookup(i, "a")
    assert_received {:route_many, ["a"]}
  end

  @tag :capture_log
  test "an invalid inner strategy fails boot", %{instance: i} do
    assert {:error, _reason} =
             start_supervised({Shardex, name: i, shards: [s1: agent_pool(i, :s1)], strategy: {Cached, inner: String}})
  end
end
