defmodule Shardex.AdapterTest do
  use ExUnit.Case, async: true

  alias Shardex.Adapter
  alias Shardex.Adapter.Generic
  alias Shardex.Pool

  defmodule Wrapper do
    @moduledoc false
    def run(ref, fun, tag), do: {tag, fun.(ref)}
  end

  test "build_pool/4 builds a pool with a child spec whose id is {shard, role}" do
    spec = {Generic, child_spec: {Agent, fn -> :ok end}, ref: :my_ref}
    assert {:ok, %Pool{} = pool} = Adapter.build_pool(:inst, :s1, :replica, spec)
    assert pool.role == :replica
    assert pool.adapter == Generic
    assert pool.ref == :my_ref
    assert pool.status == :active
    assert pool.child_spec.id == {:s1, :replica}
    assert {Agent, :start_link, [_fun]} = pool.child_spec.start
  end

  test "build_pool/4 keeps a nil child spec (externally managed pool)" do
    assert {:ok, %Pool{child_spec: nil, ref: :external}} =
             Adapter.build_pool(:inst, :s1, :primary, {Generic, ref: :external})
  end

  test "build_pool/4 returns adapter errors and rejects non-adapters" do
    assert {:error, {:missing_option, :ref}} = Adapter.build_pool(:inst, :s1, :primary, {Generic, []})
    assert {:error, {:invalid_adapter, String}} = Adapter.build_pool(:inst, :s1, :primary, {String, []})
  end

  test "run/2 defaults to calling fun with the ref" do
    {:ok, pool} = Adapter.build_pool(:inst, :s1, :primary, {Generic, ref: :r})
    assert Adapter.run(pool, &{:got, &1}) == {:got, :r}
  end

  test "Generic supports a run function and an MFA" do
    {:ok, pool} =
      Adapter.build_pool(:inst, :s1, :primary, {Generic, ref: :r, run: fn ref, fun -> {:wrapped, fun.(ref)} end})

    assert Adapter.run(pool, & &1) == {:wrapped, :r}

    {:ok, pool} = Adapter.build_pool(:inst, :s1, :primary, {Generic, ref: :r, run: {Wrapper, :run, [:tag]}})
    assert Adapter.run(pool, & &1) == {:tag, :r}
  end
end
