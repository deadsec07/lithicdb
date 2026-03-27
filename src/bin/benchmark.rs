use anyhow::Result;
use clap::Parser;
use lithicdb::engine::db::Database;
use lithicdb::models::api::{CreateCollectionRequest, SearchRequest, VectorRecord};
use lithicdb::models::types::MetadataFilter;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "./data/bench")]
    data_dir: String,
    #[arg(long, default_value_t = 100_000)]
    vectors: usize,
    #[arg(long, default_value_t = 128)]
    dimension: usize,
    #[arg(long, default_value_t = 200)]
    queries: usize,
    #[arg(long, default_value_t = 10)]
    k: usize,
    #[arg(long, default_value_t = 42)]
    seed: u64,
    #[arg(long, default_value = "bench")]
    collection: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let data_dir = PathBuf::from(&args.data_dir);
    if data_dir.exists() {
        std::fs::remove_dir_all(&data_dir)?;
    }
    std::fs::create_dir_all(&data_dir)?;

    let db = Database::open(&data_dir)?;
    db.create_collection(CreateCollectionRequest {
        name: args.collection.clone(),
        dimension: args.dimension,
        max_cluster_size: Some(256),
        graph_degree: Some(8),
    })?;

    let mut rng = StdRng::seed_from_u64(args.seed);
    let categories = ["finance", "legal", "health", "ops"];
    let mut batch = Vec::with_capacity(1000);
    let start_build = Instant::now();
    for i in 0..args.vectors {
        let vector = random_vector(&mut rng, args.dimension);
        let mut metadata = HashMap::new();
        metadata.insert(
            "category".to_string(),
            categories[i % categories.len()].to_string(),
        );
        batch.push(VectorRecord {
            id: format!("doc-{i}"),
            vector,
            metadata,
        });
        if batch.len() == 1000 {
            db.insert_vectors(&args.collection, std::mem::take(&mut batch))?;
        }
    }
    if !batch.is_empty() {
        db.insert_vectors(&args.collection, batch)?;
    }
    let build_elapsed = start_build.elapsed();

    let mut ann_latency_ms = 0.0;
    let mut brute_latency_ms = 0.0;
    let mut recall_sum = 0.0;

    for i in 0..args.queries {
        let query = random_vector(&mut rng, args.dimension);
        let category = categories[i % categories.len()].to_string();
        let filter = MetadataFilter::ExactMap(HashMap::from([("category".to_string(), category)]));

        let ann_start = Instant::now();
        let ann = db.search(
            &args.collection,
            SearchRequest {
                vector: query.clone(),
                k: args.k,
                filter: filter.clone(),
                entry_points: Some(4),
                ef_search: Some(24),
                probe_clusters: Some(12),
            },
        )?;
        ann_latency_ms += ann_start.elapsed().as_secs_f64() * 1000.0;

        let brute_start = Instant::now();
        let brute = db.brute_force_search(
            &args.collection,
            SearchRequest {
                vector: query,
                k: args.k,
                filter,
                entry_points: None,
                ef_search: None,
                probe_clusters: None,
            },
        )?;
        brute_latency_ms += brute_start.elapsed().as_secs_f64() * 1000.0;

        let brute_ids: std::collections::HashSet<_> =
            brute.hits.iter().map(|hit| hit.id.clone()).collect();
        let overlap = ann
            .hits
            .iter()
            .filter(|hit| brute_ids.contains(&hit.id))
            .count();
        recall_sum += overlap as f64 / args.k as f64;
    }

    let disk_bytes = dir_size(&data_dir)?;
    println!("LithicDB benchmark");
    println!("vectors: {}", args.vectors);
    println!("dimension: {}", args.dimension);
    println!("queries: {}", args.queries);
    println!("build_seconds: {:.3}", build_elapsed.as_secs_f64());
    println!(
        "ann_avg_latency_ms: {:.3}",
        ann_latency_ms / args.queries as f64
    );
    println!(
        "brute_avg_latency_ms: {:.3}",
        brute_latency_ms / args.queries as f64
    );
    println!(
        "avg_recall_at_{}: {:.4}",
        args.k,
        recall_sum / args.queries as f64
    );
    println!("disk_bytes: {}", disk_bytes);
    println!(
        "memory_estimate_bytes: {}",
        estimate_memory_bytes(args.vectors, args.dimension)
    );
    Ok(())
}

fn random_vector(rng: &mut StdRng, dimension: usize) -> Vec<f32> {
    (0..dimension).map(|_| rng.gen_range(-1.0..1.0)).collect()
}

fn estimate_memory_bytes(vectors: usize, dimension: usize) -> usize {
    let graph_bytes = vectors * (8 + 8);
    let centroid_bytes = (vectors / 256 + 1) * dimension * 4;
    graph_bytes + centroid_bytes
}

fn dir_size(path: &PathBuf) -> Result<u64> {
    let mut total = 0;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        if meta.is_dir() {
            total += dir_size(&entry.path())?;
        } else {
            total += meta.len();
        }
    }
    Ok(total)
}
