defmodule Shardex.Test.SqliteRepo do
  @moduledoc false
  use Ecto.Repo, otp_app: :shardex, adapter: Ecto.Adapters.SQLite3
end

defmodule Shardex.Test.ModuleRepo do
  @moduledoc false
  use Ecto.Repo, otp_app: :shardex, adapter: Ecto.Adapters.SQLite3
end
