use lithicdb::engine::db::Database;
use lithicdb::models::api::{CreateCollectionRequest, SearchRequest, VectorRecord};
use lithicdb::models::types::{MetadataFilter, MetadataPredicate, StructuredMetadataFilter};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> std::path::PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let seq = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
    path.push(format!("lithicdb-test-{}-{nanos}-{seq}", std::process::id()));
    path
}

#[test]
fn insert_search_fetch_delete_roundtrip() {
    let dir = temp_dir();
    let db = Database::open(&dir).unwrap();
    db.create_collection(CreateCollectionRequest {
        name: "docs".to_string(),
        dimension: 3,
        max_cluster_size: Some(2),
        graph_degree: Some(4),
    })
    .unwrap();

    db.insert_vectors(
        "docs",
        vec![
            VectorRecord {
                id: "a".to_string(),
                vector: vec![1.0, 0.0, 0.0],
                metadata: HashMap::from([("category".to_string(), "finance".to_string())]),
            },
            VectorRecord {
                id: "b".to_string(),
                vector: vec![0.9, 0.1, 0.0],
                metadata: HashMap::from([("category".to_string(), "finance".to_string())]),
            },
            VectorRecord {
                id: "c".to_string(),
                vector: vec![0.0, 1.0, 0.0],
                metadata: HashMap::from([("category".to_string(), "legal".to_string())]),
            },
        ],
    )
    .unwrap();

    let result = db
        .search(
            "docs",
            SearchRequest {
                vector: vec![1.0, 0.0, 0.0],
                k: 2,
                filter: MetadataFilter::ExactMap(HashMap::from([(
                    "category".to_string(),
                    "finance".to_string(),
                )])),
                entry_points: Some(2),
                ef_search: Some(10),
                probe_clusters: Some(4),
            },
        )
        .unwrap();
    assert_eq!(result.hits[0].id, "a");

    let fetched = db.fetch_vector("docs", "b").unwrap().unwrap();
    assert_eq!(fetched.1.get("category").unwrap(), "finance");

    let deleted = db.delete_vector("docs", "a").unwrap();
    assert!(deleted);

    let result = db
        .search(
            "docs",
            SearchRequest {
                vector: vec![1.0, 0.0, 0.0],
                k: 2,
                filter: MetadataFilter::Empty,
                entry_points: Some(2),
                ef_search: Some(10),
                probe_clusters: Some(4),
            },
        )
        .unwrap();
    assert!(result.hits.iter().all(|hit| hit.id != "a"));
}

#[test]
fn wal_replays_insert_and_delete_after_restart() {
    let dir = temp_dir();
    {
        let db = Database::open(&dir).unwrap();
        db.create_collection(CreateCollectionRequest {
            name: "docs".to_string(),
            dimension: 3,
            max_cluster_size: Some(4),
            graph_degree: Some(4),
        })
        .unwrap();

        db.insert_vectors(
            "docs",
            vec![VectorRecord {
                id: "persist-me".to_string(),
                vector: vec![1.0, 0.0, 0.0],
                metadata: HashMap::from([("category".to_string(), "ops".to_string())]),
            }],
        )
        .unwrap();
    }

    {
        let db = Database::open(&dir).unwrap();
        let fetched = db.fetch_vector("docs", "persist-me").unwrap();
        assert!(fetched.is_some());
        db.delete_vector("docs", "persist-me").unwrap();
    }

    {
        let db = Database::open(&dir).unwrap();
        let fetched = db.fetch_vector("docs", "persist-me").unwrap();
        assert!(fetched.is_none());
    }
}

#[test]
fn compaction_reclaims_deleted_docs_from_logical_state() {
    let dir = temp_dir();
    let db = Database::open(&dir).unwrap();
    db.create_collection(CreateCollectionRequest {
        name: "docs".to_string(),
        dimension: 3,
        max_cluster_size: Some(2),
        graph_degree: Some(4),
    })
    .unwrap();

    db.insert_vectors(
        "docs",
        vec![
            VectorRecord {
                id: "a".to_string(),
                vector: vec![1.0, 0.0, 0.0],
                metadata: HashMap::from([("category".to_string(), "finance".to_string())]),
            },
            VectorRecord {
                id: "b".to_string(),
                vector: vec![0.0, 1.0, 0.0],
                metadata: HashMap::from([("category".to_string(), "legal".to_string())]),
            },
            VectorRecord {
                id: "c".to_string(),
                vector: vec![0.0, 0.0, 1.0],
                metadata: HashMap::from([("category".to_string(), "ops".to_string())]),
            },
        ],
    )
    .unwrap();

    db.delete_vector("docs", "b").unwrap();
    let before = db.collection_stats("docs").unwrap();
    assert_eq!(before.live_docs, 2);
    assert_eq!(before.deleted_docs, 1);

    let after = db.compact_collection("docs").unwrap();
    assert_eq!(after.live_docs, 2);
    assert_eq!(after.deleted_docs, 0);
    assert_eq!(after.wal_bytes, 0);
    assert!(db.fetch_vector("docs", "b").unwrap().is_none());
}

#[test]
fn maintenance_compacts_tombstone_heavy_collection() {
    let dir = temp_dir();
    let db = Database::open(&dir).unwrap();
    db.create_collection(CreateCollectionRequest {
        name: "docs".to_string(),
        dimension: 3,
        max_cluster_size: Some(4),
        graph_degree: Some(4),
    })
    .unwrap();

    db.insert_vectors(
        "docs",
        vec![
            VectorRecord {
                id: "a".to_string(),
                vector: vec![1.0, 0.0, 0.0],
                metadata: HashMap::new(),
            },
            VectorRecord {
                id: "b".to_string(),
                vector: vec![0.0, 1.0, 0.0],
                metadata: HashMap::new(),
            },
            VectorRecord {
                id: "c".to_string(),
                vector: vec![0.0, 0.0, 1.0],
                metadata: HashMap::new(),
            },
            VectorRecord {
                id: "d".to_string(),
                vector: vec![0.6, 0.4, 0.0],
                metadata: HashMap::new(),
            },
            VectorRecord {
                id: "e".to_string(),
                vector: vec![0.0, 0.6, 0.4],
                metadata: HashMap::new(),
            },
        ],
    )
    .unwrap();

    db.delete_vector("docs", "a").unwrap();
    db.delete_vector("docs", "b").unwrap();

    let before = db.collection_stats("docs").unwrap();
    assert_eq!(before.deleted_docs, 2);
    assert!(before.wal_bytes > 0);

    let compacted = db.run_maintenance().unwrap();
    assert_eq!(compacted, vec!["docs".to_string()]);

    let after = db.collection_stats("docs").unwrap();
    assert_eq!(after.deleted_docs, 0);
    assert_eq!(after.wal_bytes, 0);
}

#[test]
fn wal_checksum_detects_corruption() {
    let dir = temp_dir();
    {
        let db = Database::open(&dir).unwrap();
        db.create_collection(CreateCollectionRequest {
            name: "docs".to_string(),
            dimension: 3,
            max_cluster_size: Some(4),
            graph_degree: Some(4),
        })
        .unwrap();
        db.insert_vectors(
            "docs",
            vec![VectorRecord {
                id: "persist-me".to_string(),
                vector: vec![1.0, 0.0, 0.0],
                metadata: HashMap::new(),
            }],
        )
        .unwrap();
    }

    let wal_path = dir.join("docs").join("wal.bin");
    let mut wal = OpenOptions::new().read(true).write(true).open(wal_path).unwrap();
    wal.seek(SeekFrom::Start(12)).unwrap();
    wal.write_all(&[0xFF]).unwrap();
    wal.flush().unwrap();

    let reopened = Database::open(&dir);
    assert!(reopened.is_err());
    let error = reopened.err().unwrap().to_string();
    assert!(error.contains("wal checksum mismatch"));
}

#[test]
fn compaction_publishes_new_generation_manifest() {
    let dir = temp_dir();
    let db = Database::open(&dir).unwrap();
    db.create_collection(CreateCollectionRequest {
        name: "docs".to_string(),
        dimension: 3,
        max_cluster_size: Some(2),
        graph_degree: Some(4),
    })
    .unwrap();

    db.insert_vectors(
        "docs",
        vec![
            VectorRecord {
                id: "a".to_string(),
                vector: vec![1.0, 0.0, 0.0],
                metadata: HashMap::new(),
            },
            VectorRecord {
                id: "b".to_string(),
                vector: vec![0.0, 1.0, 0.0],
                metadata: HashMap::new(),
            },
            VectorRecord {
                id: "c".to_string(),
                vector: vec![0.0, 0.0, 1.0],
                metadata: HashMap::new(),
            },
        ],
    )
    .unwrap();
    db.delete_vector("docs", "b").unwrap();

    let stats = db.compact_collection("docs").unwrap();
    assert_eq!(stats.deleted_docs, 0);
    assert!(db.fetch_vector("docs", "a").unwrap().is_some());
    assert!(db.fetch_vector("docs", "b").unwrap().is_none());

    let current_path = dir.join("docs").join("CURRENT");
    let current_bytes = std::fs::read(current_path).unwrap();
    let current: lithicdb::models::types::ActiveSegmentManifest =
        bincode::deserialize(&current_bytes).unwrap();
    assert_ne!(current.generation, "base");
    assert!(dir.join("docs").join(current.state_file).exists());
    assert!(dir.join("docs").join(current.raw_vectors_file).exists());
    assert!(dir.join("docs").join(current.quant_vectors_file).exists());

    let generations_path = dir.join("docs").join("GENERATIONS");
    let generations_bytes = std::fs::read(generations_path).unwrap();
    let catalog: lithicdb::models::types::GenerationCatalog =
        bincode::deserialize(&generations_bytes).unwrap();
    assert_eq!(catalog.write.generation, current.generation);
    assert!(catalog.readable.iter().any(|manifest| manifest.generation == current.generation));
    assert!(catalog.retired.is_empty());
}

#[test]
fn structured_filter_supports_exact_and_numeric_range() {
    let dir = temp_dir();
    let db = Database::open(&dir).unwrap();
    db.create_collection(CreateCollectionRequest {
        name: "docs".to_string(),
        dimension: 3,
        max_cluster_size: Some(4),
        graph_degree: Some(4),
    })
    .unwrap();

    db.insert_vectors(
        "docs",
        vec![
            VectorRecord {
                id: "low".to_string(),
                vector: vec![1.0, 0.0, 0.0],
                metadata: HashMap::from([
                    ("category".to_string(), "finance".to_string()),
                    ("price".to_string(), "12.5".to_string()),
                ]),
            },
            VectorRecord {
                id: "mid".to_string(),
                vector: vec![0.95, 0.05, 0.0],
                metadata: HashMap::from([
                    ("category".to_string(), "finance".to_string()),
                    ("price".to_string(), "25.0".to_string()),
                ]),
            },
            VectorRecord {
                id: "high".to_string(),
                vector: vec![0.90, 0.10, 0.0],
                metadata: HashMap::from([
                    ("category".to_string(), "finance".to_string()),
                    ("price".to_string(), "40.0".to_string()),
                ]),
            },
            VectorRecord {
                id: "other".to_string(),
                vector: vec![0.0, 1.0, 0.0],
                metadata: HashMap::from([
                    ("category".to_string(), "legal".to_string()),
                    ("price".to_string(), "30.0".to_string()),
                ]),
            },
        ],
    )
    .unwrap();

    let result = db
        .search(
            "docs",
            SearchRequest {
                vector: vec![1.0, 0.0, 0.0],
                k: 5,
                filter: MetadataFilter::Structured(StructuredMetadataFilter {
                    must: vec![
                        MetadataPredicate::Eq {
                            field: "category".to_string(),
                            value: "finance".to_string(),
                        },
                        MetadataPredicate::Range {
                            field: "price".to_string(),
                            gt: None,
                            gte: Some(20.0),
                            lt: None,
                            lte: Some(30.0),
                        },
                    ],
                }),
                entry_points: Some(2),
                ef_search: Some(10),
                probe_clusters: Some(4),
            },
        )
        .unwrap();

    let ids: Vec<_> = result.hits.into_iter().map(|hit| hit.id).collect();
    assert_eq!(ids, vec!["mid".to_string()]);
}

#[test]
fn diagnostics_expose_generation_and_compaction_state() {
    let dir = temp_dir();
    let db = Database::open(&dir).unwrap();
    db.create_collection(CreateCollectionRequest {
        name: "docs".to_string(),
        dimension: 3,
        max_cluster_size: Some(4),
        graph_degree: Some(4),
    })
    .unwrap();

    db.insert_vectors(
        "docs",
        vec![
            VectorRecord {
                id: "a".to_string(),
                vector: vec![1.0, 0.0, 0.0],
                metadata: HashMap::new(),
            },
            VectorRecord {
                id: "b".to_string(),
                vector: vec![0.0, 1.0, 0.0],
                metadata: HashMap::new(),
            },
        ],
    )
    .unwrap();
    db.delete_vector("docs", "a").unwrap();

    let diagnostics = db.collection_diagnostics("docs").unwrap();
    assert_eq!(diagnostics.collection, "docs");
    assert_eq!(diagnostics.current_generation, "base");
    assert!(diagnostics.compaction_recommended);
    assert!(diagnostics.pending_cleanup_generations.is_empty());
}

#[test]
fn backup_and_restore_collection_roundtrip() {
    let dir = temp_dir();
    let db = Database::open(&dir).unwrap();
    db.create_collection(CreateCollectionRequest {
        name: "docs".to_string(),
        dimension: 3,
        max_cluster_size: Some(4),
        graph_degree: Some(4),
    })
    .unwrap();
    db.insert_vectors(
        "docs",
        vec![VectorRecord {
            id: "a".to_string(),
            vector: vec![1.0, 0.0, 0.0],
            metadata: HashMap::from([("category".to_string(), "finance".to_string())]),
        }],
    )
    .unwrap();

    let backup_name = db.backup_collection("docs").unwrap();
    db.restore_collection(&backup_name, "docs-restore").unwrap();

    let fetched = db.fetch_vector("docs-restore", "a").unwrap();
    assert!(fetched.is_some());
}
