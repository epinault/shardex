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
    with {:ok, ref} <- fetch_ref(opts),
         {:ok, run} <- validate_run(Keyword.get(opts, :run)) do
      state = %{ref: ref, run: run}
      {:ok, %{state: state, ref: ref, child_spec: Keyword.get(opts, :child_spec), meta: %{}}}
    end
  end

  defp fetch_ref(opts) do
    case Keyword.fetch(opts, :ref) do
      {:ok, ref} -> {:ok, ref}
      :error -> {:error, {:missing_option, :ref}}
    end
  end

  defp validate_run(nil), do: {:ok, nil}
  defp validate_run(run) when is_function(run, 2), do: {:ok, run}
  defp validate_run({m, f, a} = run) when is_atom(m) and is_atom(f) and is_list(a), do: {:ok, run}
  defp validate_run(_run), do: {:error, {:invalid_option, :run}}

  @impl true
  def run(%{ref: ref, run: nil}, fun), do: fun.(ref)
  def run(%{ref: ref, run: run}, fun) when is_function(run, 2), do: run.(ref, fun)
  def run(%{ref: ref, run: {m, f, a}}, fun), do: apply(m, f, [ref, fun | a])
end
