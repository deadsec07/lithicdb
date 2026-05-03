# LithicDB

[![CI](https://github.com/deadsec07/lithicdb/actions/workflows/ci.yml/badge.svg)](https://github.com/deadsec07/lithicdb/actions/workflows/ci.yml)
[![Pages](https://github.com/deadsec07/lithicdb/actions/workflows/pages.yml/badge.svg)](https://github.com/deadsec07/lithicdb/actions/workflows/pages.yml)
[![Release](https://github.com/deadsec07/lithicdb/actions/workflows/release.yml/badge.svg)](https://github.com/deadsec07/lithicdb/actions/workflows/release.yml)
[![Site](https://img.shields.io/badge/site-live-0a7f5a)](https://deadsec07.github.io/lithicdb/)

LithicDB is a disk-first vector database in Rust by A A Hasnat. It is designed for retrieval, semantic search, and recommendation workloads that want lower memory pressure than fully in-memory ANN systems while keeping online writes, metadata filtering, and benchmarkable search quality.

Links:
- Live site: https://deadsec07.github.io/lithicdb/
- Main site: https://hnetechnologies.com/
- Creator profile: https://deadsec07.github.io/
- LinkedIn: https://www.linkedin.com/in/a-a-hasnat/

## Product thesis

- Who it is for: teams building RAG, semantic search, or recommendation systems that need predictable single-node cost
- Why they would choose it: memory stays bounded because routing and filters stay in memory while payload vectors stay on disk
- Technical edge: graph-guided ANN search, quantized candidate scoring, exact reranking, and disk-first storage

## Core capabilities

- Disk-first and low-memory vector storage
- Online inserts and deletes
- Metadata-aware ANN search
- WAL-backed crash recovery
- Background maintenance and compaction
- Brute-force cosine comparison from the same collection state

## Architecture

Repo structure:

- `src/main.rs` - server entrypoint
- `src/api/routes.rs` - REST routes and handlers
- `src/engine/collection.rs` - collection lifecycle and persistence
- `src/index/graph.rs` - centroid graph traversal
- `src/index/quantizer.rs` - normalization and `q8` quantization math
- `src/bin/benchmark.rs` - ANN vs brute-force benchmark runner
- `tests/engine_tests.rs` - integration tests

## Release flow

Standard release path:

```bash
git tag v0.1.0
git push origin v0.1.0
```

Manual GitHub CLI path:

```bash
gh release create v0.1.0 --generate-notes
```

## REST API

- `GET /healthz`
- `POST /collections`
- `POST /collections/<name>/vectors`
- `DELETE /collections/<name>/vectors/<id>`
- `POST /collections/<name>/search`

See the live documentation site and repository examples for the full request and response payloads.
