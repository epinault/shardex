# Telemetry

Shardex emits these events (all metadata includes `:instance`); it attaches no
handlers itself.

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:shardex, :route, :start \| :stop \| :exception]` | `duration`, `key_count` (stop) | `strategy`, `batch?` |
| `[:shardex, :run, :start \| :stop \| :exception]` | `duration` | `shard`, `role` |
| `[:shardex, :batch, :stop]` | `duration`, `items`, `groups`, `errors` | |
| `[:shardex, :shard, :status_changed]` | | `shard`, `role` (`nil` = whole shard), `from`, `to` |
| `[:shardex, :topology, :changed]` | | `action`, `shard`, `version` |

Route spans are emitted for every batch and, for single keys, only when the
strategy's `trace?/0` is `true` (`Hash`, `JumpHash` and `Static` opt out).

```elixir
Telemetry.Metrics.distribution("shardex.run.stop.duration", tags: [:instance, :shard, :role], unit: {:native, :millisecond})
Telemetry.Metrics.counter("shardex.shard.status_changed", tags: [:instance, :shard, :to])
```
