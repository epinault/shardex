# Batching

`group/3` routes many items with **one** strategy call (keys are
deduplicated) and returns a `%Shardex.Batch{}`:

```elixir
%Shardex.Batch{
  groups: %{shard_1: %{shard: %Shardex.Shard{}, pool: %Shardex.Pool{}, items: [...]}},
  errors: [{item, :unassigned}, ...]
} = MyApp.Shards.group(messages, & &1.org_id)
```

Every item appears exactly once, either in a group (in input order) or in
`errors`. Use `role:` / `fallback:` to pick pools.

`run_batch/4` groups, then calls your function once per shard inside the
adapter context:

```elixir
{:ok, results, errors} =
  MyApp.Shards.run_batch(messages, & &1.org_id, fn repo, msgs ->
    repo.insert_all(Event, rows(msgs))
  end, max_concurrency: 4)
```

- `on_error: :raise` raises `Shardex.BatchError` before running anything if
  any item cannot be routed (e.g. a Kafka consumer that must not skip
  messages while a shard is in maintenance).
- `max_concurrency: n` runs shard groups in parallel tasks; `timeout:` bounds
  each one.
