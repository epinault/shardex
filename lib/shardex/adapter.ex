defmodule Shardex.Adapter do
  @moduledoc """
  Behaviour for pool adapters.

  `init/2` runs once per `{shard, role}` pool (at boot and on `add_shard`),
  never on the routing path. It returns:

    * `:state` - adapter state passed to `run/2`
    * `:ref` - the value handed to callers (`Shardex.ref/3`)
    * `:child_spec` - how to start the pool (any `Supervisor.child_spec/2`
      input), or `nil` when the pool is managed elsewhere
    * `:meta` - adapter-specific info exposed on `%Shardex.Pool{}`

  `run/2` wraps a call in adapter context (e.g. Ecto's `put_dynamic_repo`).
  Default: `fun.(ref)`.

  Pool process names should be built with `Module.concat([instance, shard, role])`.
  """

  alias Shardex.Pool

  @type ctx :: %{instance: atom(), shard: atom(), role: atom()}

  @type init_result :: %{
          required(:state) => term(),
          required(:ref) => term(),
          optional(:child_spec) => Supervisor.module_spec() | Supervisor.child_spec() | nil,
          optional(:meta) => map()
        }

  @callback init(opts :: keyword(), ctx()) :: {:ok, init_result()} | {:error, term()}
  @callback run(state :: term(), (term() -> result)) :: result when result: term()

  @optional_callbacks run: 2

  @doc false
  @spec build_pool(atom(), atom(), atom(), {module(), keyword()}) :: {:ok, Pool.t()} | {:error, term()}
  def build_pool(instance, shard, role, {adapter, opts}) do
    ctx = %{instance: instance, shard: shard, role: role}

    with :ok <- ensure_adapter(adapter),
         {:ok, %{state: state, ref: ref} = result} <- adapter.init(opts, ctx) do
      {:ok,
       %Pool{
         role: role,
         adapter: adapter,
         adapter_state: state,
         ref: ref,
         child_spec: child_spec(Map.get(result, :child_spec), shard, role),
         meta: Map.get(result, :meta, %{})
       }}
    end
  end

  @doc false
  @spec run(Pool.t(), (term() -> result)) :: result when result: term()
  def run(%Pool{adapter: adapter, adapter_state: state, ref: ref}, fun) do
    if function_exported?(adapter, :run, 2), do: adapter.run(state, fun), else: fun.(ref)
  end

  defp ensure_adapter(adapter) do
    if Code.ensure_loaded?(adapter) and function_exported?(adapter, :init, 2),
      do: :ok,
      else: {:error, {:invalid_adapter, adapter}}
  end

  defp child_spec(nil, _shard, _role), do: nil
  defp child_spec(spec, shard, role), do: Supervisor.child_spec(spec, id: {shard, role})
end
