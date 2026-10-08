defmodule Shardex.Adapter.Generic do
  @moduledoc """
  Adapter for any pool: you provide the child spec and the ref.

      es_1: {Shardex.Adapter.Generic,
             child_spec: {MyApp.ES.Cluster1, []},
             ref: MyApp.ES.Cluster1}

  Options:

    * `:ref` (required) - value passed to callers
    * `:child_spec` - any `Supervisor.child_spec/2` input; omit for pools started elsewhere
    * `:run` - optional `(ref, fun -> result)` function, or `{m, f, a}` called as
      `apply(m, f, [ref, fun | a])`, to wrap calls in context
  """

  @behaviour Shardex.Adapter

  @impl true
  def init(opts, _ctx) do
    case Keyword.fetch(opts, :ref) do
      {:ok, ref} ->
        state = %{ref: ref, run: Keyword.get(opts, :run)}
        {:ok, %{state: state, ref: ref, child_spec: Keyword.get(opts, :child_spec), meta: %{}}}

      :error ->
        {:error, {:missing_option, :ref}}
    end
  end

  @impl true
  def run(%{ref: ref, run: nil}, fun), do: fun.(ref)
  def run(%{ref: ref, run: run}, fun) when is_function(run, 2), do: run.(ref, fun)
  def run(%{ref: ref, run: {m, f, a}}, fun), do: apply(m, f, [ref, fun | a])
end
