defmodule Shardex.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/epinault/shardex"

  def project do
    [
      app: :shardex,
      version: @version,
      elixir: "~> 1.20",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Route keys and batches to sharded pools: Ecto, Redis, Elasticsearch or anything with a child spec.",
      package: package(),
      docs: docs(),
      source_url: @source_url,
      dialyzer: [
        plt_add_apps: [:ecto, :redix],
        plt_local_path: "priv/plts"
      ]
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp deps do
    [
      {:telemetry, "~> 1.3"},
      {:nimble_options, "~> 1.1"},
      {:ecto, "~> 3.12", optional: true},
      {:redix, "~> 1.5", optional: true},
      {:ecto_sql, "~> 3.14", only: :test},
      {:ecto_sqlite3, "~> 0.25", only: :test},
      {:stream_data, "~> 1.4", only: [:dev, :test]},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:styler, "~> 1.12", only: [:dev, :test], runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url, "Changelog" => "#{@source_url}/blob/main/CHANGELOG.md"},
      files: ~w(lib guides mix.exs README.md CHANGELOG.md LICENSE)
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      extras: [
        "README.md",
        "guides/getting-started.md",
        "guides/ecto.md",
        "guides/redis.md",
        "guides/elasticsearch.md",
        "guides/strategies.md",
        "guides/batching.md",
        "guides/maintenance.md",
        "guides/telemetry.md",
        "guides/migrating-from-shardlib.md",
        "CHANGELOG.md"
      ],
      groups_for_extras: [Guides: ~r/guides\//],
      groups_for_modules: [
        Strategies: [~r/Shardex\.Strategy/],
        Adapters: [~r/Shardex\.Adapter/]
      ]
    ]
  end
end
