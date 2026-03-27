# LithicDB

[![CI](https://github.com/deadsec07/lithicdb/actions/workflows/ci.yml/badge.svg)](https://github.com/deadsec07/lithicdb/actions/workflows/ci.yml)
[![Pages](https://github.com/deadsec07/lithicdb/actions/workflows/pages.yml/badge.svg)](https://github.com/deadsec07/lithicdb/actions/workflows/pages.yml)
[![Release](https://github.com/deadsec07/lithicdb/actions/workflows/release.yml/badge.svg)](https://github.com/deadsec07/lithicdb/actions/workflows/release.yml)
[![Site](https://img.shields.io/badge/site-live-0a7f5a)](https://deadsec07.github.io/lithicdb/)

LithicDB is a disk-first vector database MVP built around a hybrid cluster-graph plus quantized payload design. It targets retrieval teams that want lower memory pressure than fully in-memory HNSW systems while still supporting online writes, deletes, metadata filtering, and benchmarkable ANN search on a single node.

The current build also includes two product-hardening features beyond the initial MVP:

- Per-collection WAL replay for crash recovery between snapshots
- Online compaction to reclaim deleted payload space and rebuild clusters
- Periodic background maintenance for tombstone-heavy collections
- Checksummed WAL frames and atomic snapshot rewrites
- Manifest-published segment generations for safer compaction cutovers

## Product thesis

- Who it is for: teams building RAG, semantic search, or recommendation systems that need predictable single-node cost and do not want to keep the full vector payload in RAM.
- Why they would choose it: the in-memory footprint stays bounded because only routing centroids, metadata postings, and document handles live in memory while normalized vectors stay on disk.
- Technical edge: ANN search navigates a graph of coarse clusters, scans only a small set of candidate blocks, scores candidates using `q8` compressed vectors, then reranks the finalists with exact cosine from disk-resident `f32` vectors.
- Tradeoffs: peak recall can trail mature all-memory graph indexes, and append-heavy workloads will eventually need background compaction and smarter rebalancing to maintain ideal latency.

## Differentiation

LithicDB commits to direction `C`: hybrid graph + quantization with lower memory usage than standard HNSW.

The key design choice is to separate routing from payload:

- Routing layer: a compact graph over cluster centroids kept in memory.
- Payload layer: normalized vectors stored on disk in two append-only files:
  - `vectors.f32` for exact reranking and brute-force benchmarking
  - `vectors.q8` for low-memory approximate scoring
- Filtering layer: in-memory metadata posting lists for exact `key=value` constraints plus a numeric metadata index for range predicates.

This gives a useful MVP behavior envelope:

- Disk-first and low-memory
- Online inserts and deletes
- Metadata-aware ANN search
- Brute-force cosine comparison from the same collection state

## Release flow

LithicDB now has a GitHub release workflow that builds installable binaries for Linux, macOS, and Windows whenever you push a semantic version tag.

Standard release path:

```bash
git tag v0.1.0
git push origin v0.1.0
```

Manual GitHub CLI path:

```bash
gh release create v0.1.0 --generate-notes
```

The workflow publishes packaged artifacts containing:

- `lithicdb` server binary
- `benchmark` binary
- `README.md`

You can also rerun the workflow manually from GitHub Actions with `workflow_dispatch` and pass an existing tag.

## Architecture

Repo structure:

- `src/main.rs`: server entrypoint
- `src/api/routes.rs`: REST routes and handlers
- `src/engine/db.rs`: multi-collection database orchestration
- `src/engine/collection.rs`: collection lifecycle, insert/delete/search, persistence
- `src/index/graph.rs`: centroid graph traversal
- `src/index/quantizer.rs`: normalization and `q8` quantization math
- `src/storage/files.rs`: append/read primitives and snapshot persistence
- `src/models/`: API and persisted data models
- `src/bin/benchmark.rs`: ANN vs brute-force benchmark runner
- `tests/engine_tests.rs`: integration-style engine test

Collection layout on disk:

- `data/<collection>/CURRENT`: active segment manifest pointing to the current state/vector files
- `data/<collection>/GENERATIONS`: catalog of active and retired generations
- `data/<collection>/wal.bin`: write-ahead log of inserts and deletes since the last snapshot
- `data/<collection>/state-<generation>.bin`: serialized snapshot for an active generation
- `data/<collection>/vectors-<generation>.f32`: active generation exact vectors
- `data/<collection>/vectors-<generation>.q8`: active generation quantized vectors
- `data/<collection>/state.bin`, `vectors.f32`, `vectors.q8`: legacy base-generation names used for the initial generation

Core data structures:

- `DocumentRecord`: external id, metadata, append offsets, deletion flag
- `ClusterNode`: centroid vector, member doc ids, neighbor clusters
- `filter_index`: `key -> value -> set(doc_id)` for metadata filtering

## Search algorithm

LithicDB uses a three-stage search pipeline:

1. Normalize the query to unit length.
2. Traverse the cluster graph from an entry centroid using greedy best-first expansion to select the most promising clusters.
3. Score members of those clusters with quantized cosine using the `q8` payload, then rerank the best candidates against exact normalized `f32` vectors from disk.

Why this works:

- The graph narrows the search space without storing all vectors in memory.
- Quantized candidate scoring reduces disk bandwidth and CPU cost for the first pass.
- Exact reranking recovers quality on the final shortlist.

Update path:

1. Append a logical insert to `wal.bin`.
2. Append normalized `f32` vector to `vectors.f32`.
3. Append `q8` vector to `vectors.q8`.
4. Add metadata terms to posting lists.
5. Add numeric metadata values to the range index when parsable.
6. Assign the vector to the nearest cluster.
7. Split an oversized cluster with a lightweight two-seed k-means style partition.
8. Rewire graph neighbors for affected clusters.
9. Periodically snapshot state, truncate the WAL, and roll over to a fresh write generation.

Delete path:

1. Append a logical delete to `wal.bin`.
2. Mark the document deleted in memory.
3. Remove metadata postings.
4. Remove the doc id from any cluster membership list.
5. Recompute centroids for affected clusters and drop empty clusters.
6. Snapshot and truncate the WAL on the configured interval.

Recovery path:

1. Load `state.bin`.
2. Replay `wal.bin` entries in order.
3. Persist a fresh snapshot.
4. Truncate `wal.bin`.

Integrity guards:

- Each WAL frame stores `length + checksum + payload`
- Replay aborts if a checksum does not match
- Snapshot writes go to a temporary file and then atomically rename into place
- Compaction writes a new generation and publishes it by atomically rewriting `CURRENT`
- Retired generations are tracked in `GENERATIONS` and cleaned after publish

## REST API

### Health

`GET /healthz`

### Create collection

`POST /collections`

```json
{
  "name": "docs",
  "dimension": 128,
  "max_cluster_size": 256,
  "graph_degree": 8
}
```

### Insert vectors

`POST /collections/docs/vectors`

```json
{
  "records": [
    {
      "id": "doc-1",
      "vector": [0.1, 0.2, 0.3],
      "metadata": {
        "category": "finance"
      }
    }
  ]
}
```

### Delete vector

`DELETE /collections/docs/vectors/doc-1`

### Search nearest neighbors

`POST /collections/docs/search`

```json
{
  "vector": [0.1, 0.2, 0.3],
  "k": 5,
  "filter": {
    "category": "finance"
  },
  "entry_points": 4,
  "ef_search": 24,
  "probe_clusters": 12
}
```

Structured filter form with exact plus numeric range:

```json
{
  "vector": [0.1, 0.2, 0.3],
  "k": 5,
  "filter": {
    "must": [
      { "op": "eq", "field": "category", "value": "finance" },
      { "op": "range", "field": "price", "gte": 20.0, "lte": 30.0 }
    ]
  }
}
```

### Fetch by id

`GET /collections/docs/vectors/doc-1`

### Collection stats

`GET /collections/docs/stats`

### Collection diagnostics

`GET /collections/docs/diagnostics`

### Compact collection

`POST /collections/docs/compact`

### Backup collection

`POST /admin/collections/docs/backup`

### Restore collection

`POST /admin/collections/restore`

## Exact run instructions

Prerequisites:

- Rust toolchain with `cargo` and `rustc`

Build:

```bash
cargo build --release
```

Run the server:

```bash
cargo run --release -- --data-dir ./data --bind 127.0.0.1:8080 --maintenance-interval-secs 30
```

Require an API key for mutating and admin endpoints:

```bash
LITHICDB_API_KEY=secret \
cargo run --release -- --data-dir ./data --bind 127.0.0.1:8080
```

Disable background maintenance:

```bash
cargo run --release -- --data-dir ./data --bind 127.0.0.1:8080 --maintenance-interval-secs 0
```

Create a collection:

```bash
curl -X POST http://127.0.0.1:8080/collections \
  -H 'content-type: application/json' \
  -d '{
    "name":"docs",
    "dimension":3,
    "max_cluster_size":128,
    "graph_degree":8
  }'
```

Insert vectors:

```bash
curl -X POST http://127.0.0.1:8080/collections/docs/vectors \
  -H 'content-type: application/json' \
  -d '{
    "records":[
      {"id":"a","vector":[1.0,0.0,0.0],"metadata":{"category":"finance"}},
      {"id":"b","vector":[0.9,0.1,0.0],"metadata":{"category":"finance"}},
      {"id":"c","vector":[0.0,1.0,0.0],"metadata":{"category":"legal"}}
    ]
  }'
```

Search with filtering:

```bash
curl -X POST http://127.0.0.1:8080/collections/docs/search \
  -H 'content-type: application/json' \
  -d '{
    "vector":[1.0,0.0,0.0],
    "k":2,
    "filter":{"category":"finance"},
    "ef_search":24,
    "probe_clusters":12
  }'
```

Fetch by id:

```bash
curl http://127.0.0.1:8080/collections/docs/vectors/a
```

Delete by id:

```bash
curl -X DELETE http://127.0.0.1:8080/collections/docs/vectors/a
```

Read collection stats:

```bash
curl http://127.0.0.1:8080/collections/docs/stats
```

Read collection diagnostics:

```bash
curl http://127.0.0.1:8080/collections/docs/diagnostics
```

Run compaction:

```bash
curl -X POST http://127.0.0.1:8080/collections/docs/compact
```

Create a backup:

```bash
curl -X POST http://127.0.0.1:8080/admin/collections/docs/backup \
  -H 'x-api-key: secret'
```

Restore a backup:

```bash
curl -X POST http://127.0.0.1:8080/admin/collections/restore \
  -H 'content-type: application/json' \
  -H 'x-api-key: secret' \
  -d '{
    "backup_name":"docs-1712345678",
    "target_name":"docs-restore"
  }'
```

Run tests:

```bash
cargo test
```

Run the benchmark at 100k vectors:

```bash
cargo run --release --bin benchmark -- \
  --data-dir ./data/bench \
  --vectors 100000 \
  --dimension 128 \
  --queries 200 \
  --k 10
```

Benchmark output includes:

- ANN average latency
- brute-force average latency
- recall@k
- disk footprint
- simple memory estimate for routing structures

## What works in this MVP

- Collection creation with fixed vector dimension
- Vector insert with id and metadata
- Disk persistence for vectors and index state
- WAL-backed crash recovery between snapshots
- Online delete
- Online compaction to reclaim deleted logical state
- Background compaction when tombstones cross a threshold
- Approximate cosine search with exact reranking
- Exact `key=value` metadata filtering
- Structured metadata filters with exact match and numeric range predicates
- Numeric metadata indexing for range-filter pruning
- Fetch by id
- Collection stats for observability
- Collection diagnostics for generation and maintenance visibility
- Generation rollover for incremental persistence
- Multi-entry ANN routing via `entry_points`
- Optional API-key protection for mutating/admin endpoints
- Local backup and restore
- Brute-force baseline over the same stored vectors
- 100k vector benchmark path

## Engineering notes

- Vectors are normalized on write so cosine similarity becomes a dot product.
- ANN quality depends on `max_cluster_size`, `graph_degree`, `ef_search`, and `probe_clusters`.
- Persistence uses snapshots plus a WAL. This gives basic crash recovery, but a production system should also add checksums, segment versioning, and stronger fsync policy controls.
- Persistence uses snapshots plus a checksummed WAL. Snapshot commits are temp-file writes followed by rename.
- Deletes are logical until compaction rebuilds the payload files.
- Background maintenance compacts collections when deleted vectors exceed either 20% of docs or 64 tombstones.

## Roadmap to a marketable product

1. Add versioned WAL segments with truncation-safe tail handling and recovery tooling.
2. Move from full snapshots to segment-based incremental persistence.
3. Add online compaction without full collection rewrite pauses.
4. Introduce adaptive product quantization instead of scalar `q8`.
5. Add concurrent ingest pipelines with per-collection write workers.
6. Improve graph routing with multi-entry search and better split heuristics.
7. Add richer metadata filtering with numeric ranges and boolean expressions.
8. Add collection diagnostics, graph introspection, and tunable search profiles.
9. Add gRPC and bulk import/export tools.
10. Extend to replication and object-store backed cold segments.

## Current limitations

- Numeric filtering is currently evaluated against stored metadata values at query time rather than through a dedicated numeric index.
- Snapshot persistence is periodic, but still collection-wide.
- Compaction still rewrites the whole collection and runs inline when triggered, but now publishes a new generation through a single active-manifest update and explicit generation catalog.
- No authentication, TLS, or replication.
- Benchmark memory is estimated from structure sizes rather than sampled from the OS.
