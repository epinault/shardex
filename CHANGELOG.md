# Changelog

All notable changes to this project are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `use Shardex` instance modules and the functional `Shardex` API.
- Strategies: `Hash` (default), `JumpHash`, `Static`, function strategies, `Cached` wrapper.
- Adapters: `Ecto` (dynamic and module modes), `Redix` (optional pooling), `Generic`.
- Roles per shard with fallback.
- Batching with `group/4` and `run_batch/5`.
- Maintenance (`:drain`, `:stop`), `activate`, `status`, `shards`.
- Runtime `add_shard` / `remove_shard`.
- Telemetry events under `[:shardex, ...]`, including `[:shardex, :coordinator, :init]` to
  re-apply runtime state after a Coordinator restart.
