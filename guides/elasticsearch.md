# Elasticsearch (and any other pool)

Elasticsearch clients differ, so Shardex uses `Shardex.Adapter.Generic`: you
give it the child spec to start and the ref to hand to callers.

## elasticsearch-elixir

```elixir
shards: [
  cluster_1: {Shardex.Adapter.Generic, child_spec: MyApp.ES.Cluster1, ref: MyApp.ES.Cluster1},
  cluster_2: {Shardex.Adapter.Generic, child_spec: MyApp.ES.Cluster2, ref: MyApp.ES.Cluster2}
]

MyApp.Search.run(org_id, fn cluster -> Elasticsearch.post(cluster, "/contacts/_search", query) end)
```

## Snap

```elixir
cluster_1: {Shardex.Adapter.Generic, child_spec: MyApp.Snap1, ref: MyApp.Snap1}

MyApp.Search.run(org_id, fn cluster -> Snap.Search.search(cluster, "contacts", query) end)
```

## Wrapping calls

`run:` lets you add context around every call, as a 2-arity function or an MFA
called as `apply(m, f, [ref, fun | args])`:

```elixir
{Shardex.Adapter.Generic,
 child_spec: {Finch, name: MyApp.Finch1},
 ref: MyApp.Finch1,
 run: fn ref, fun -> Logger.metadata(pool: ref); fun.(ref) end}
```

Omit `child_spec` for pools started elsewhere.
