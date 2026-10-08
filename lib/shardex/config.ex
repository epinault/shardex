defmodule Shardex.Config do
  @moduledoc false

  @schema NimbleOptions.new!(
            name: [
              type: {:custom, __MODULE__, :validate_name, []},
              required: true,
              doc: "Instance name, a non-nil atom (set automatically by `use Shardex`)."
            ],
            shards: [
              type: {:custom, __MODULE__, :validate_shards, []},
              required: true,
              doc: "Keyword list of `shard_name => pool_spec | [role => pool_spec]`."
            ],
            strategy: [
              type: {:custom, __MODULE__, :validate_strategy, []},
              default: Shardex.Strategy.Hash,
              doc: "Strategy module, `{module, opts}` or a `(key, ctx -> result)` function."
            ],
            start_pools: [type: :boolean, default: true, doc: "Set to `false` in tests to skip starting pools."],
            start_failure: [
              type: {:in, [:raise, :stop_shard]},
              default: :raise,
              doc: "What to do when a pool fails to start at boot."
            ]
          )

  @doc false
  def schema, do: @schema

  @spec validate!(keyword()) :: map()
  def validate!(opts), do: opts |> NimbleOptions.validate!(@schema) |> Map.new()

  @doc false
  def validate_name(name) when is_atom(name) and name not in [nil, true, false], do: {:ok, name}
  def validate_name(other), do: {:error, "expected :name to be a non-nil atom, got: #{inspect(other)}"}

  @doc false
  def validate_shards([_ | _] = shards) do
    with :ok <- check_keyword(shards),
         :ok <- check_unique(Keyword.keys(shards)) do
      normalize_all(shards)
    end
  end

  def validate_shards(other), do: {:error, "expected :shards to be a non-empty keyword list, got: #{inspect(other)}"}

  @doc false
  def validate_strategy(strategy), do: Shardex.Strategy.validate(strategy)

  @spec normalize_roles(atom(), term()) :: {:ok, [{atom(), {module(), keyword()}}]} | {:error, String.t()}
  def normalize_roles(_shard, {adapter, opts} = spec) when is_atom(adapter) and is_list(opts), do: {:ok, [primary: spec]}

  def normalize_roles(shard, [_ | _] = roles) do
    with :ok <- check_role_specs(shard, roles),
         [] <- duplicates(Keyword.keys(roles)) do
      {:ok, roles}
    else
      {:error, _message} = error -> error
      dups -> {:error, "duplicate role names for shard #{inspect(shard)}: #{inspect(dups)}"}
    end
  end

  def normalize_roles(shard, other), do: {:error, invalid_spec_message(shard, other)}

  defp normalize_all(shards) do
    shards
    |> Enum.reduce_while({:ok, []}, fn {name, spec}, {:ok, acc} ->
      case normalize_roles(name, spec) do
        {:ok, roles} -> {:cont, {:ok, [{name, roles} | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      error -> error
    end
  end

  defp check_role_specs(shard, roles) do
    if Keyword.keyword?(roles) and Enum.all?(roles, fn {_role, spec} -> pool_spec?(spec) end),
      do: :ok,
      else: {:error, invalid_spec_message(shard, roles)}
  end

  defp check_keyword(shards) do
    if Keyword.keyword?(shards), do: :ok, else: {:error, "expected :shards to be a keyword list, got: #{inspect(shards)}"}
  end

  defp check_unique(names) do
    case duplicates(names) do
      [] -> :ok
      dups -> {:error, "duplicate shard names in :shards: #{inspect(dups)}"}
    end
  end

  defp duplicates(names), do: Enum.uniq(names -- Enum.uniq(names))

  defp pool_spec?({adapter, opts}) when is_atom(adapter) and is_list(opts), do: true
  defp pool_spec?(_other), do: false

  defp invalid_spec_message(shard, spec) do
    "invalid spec for shard #{inspect(shard)}: expected {adapter, opts} or a non-empty keyword list of " <>
      "role => {adapter, opts}, got: #{inspect(spec)}"
  end
end
