defmodule Shardex.TestHelpers do
  @moduledoc false

  import ExUnit.Callbacks, only: [on_exit: 1]

  alias Shardex.Adapter.Generic

  def unique_instance, do: :"shardex_test_#{System.unique_integer([:positive])}"

  @doc "A Generic pool backed by a named Agent whose state is `{shard, role}`."
  def agent_pool(instance, shard, role \\ :primary) do
    name = agent_name(instance, shard, role)

    {Generic, child_spec: %{id: name, start: {Agent, :start_link, [fn -> {shard, role} end, [name: name]]}}, ref: name}
  end

  def agent_name(instance, shard, role \\ :primary), do: Module.concat([instance, shard, role, Agent])

  @doc "A Generic pool whose start function returns `{:error, :boom}`."
  def failing_pool(ref) do
    {Generic, child_spec: %{id: ref, start: {Kernel, :apply, [fn -> {:error, :boom} end, []]}}, ref: ref}
  end

  @doc "Forwards the given telemetry events to the calling process as `{:telemetry, event, measurements, metadata}`."
  def attach_telemetry(events) do
    id = make_ref()
    :ok = :telemetry.attach_many(id, events, &__MODULE__.forward_event/4, self())
    on_exit(fn -> :telemetry.detach(id) end)
    :ok
  end

  def forward_event(event, measurements, metadata, pid), do: send(pid, {:telemetry, event, measurements, metadata})

  @doc "Polls `fun` until it returns a truthy value (up to ~1s)."
  def eventually(fun, attempts \\ 50) do
    case fun.() do
      value when value not in [nil, false] ->
        value

      _falsy when attempts > 0 ->
        Process.sleep(20)
        eventually(fun, attempts - 1)

      _falsy ->
        raise ExUnit.AssertionError, message: "condition not met in time"
    end
  end
end
