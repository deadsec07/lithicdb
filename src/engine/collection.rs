use crate::index::graph::top_n_centroids;
use crate::index::quantizer::{cosine, cosine_quantized, normalize, quantize_normalized};
use crate::models::api::VectorRecord;
use crate::models::types::{
    ActiveSegmentManifest, ClusterNode, CollectionDiagnostics, CollectionManifest,
    CollectionSnapshot, CollectionStats, DocumentRecord, FetchedRecord, GenerationCatalog,
    MetadataFilter, SearchHit, SearchResult, SearchStats, WalOp,
};
use crate::storage::files::{
    append_f32_vector, append_i8_vector, append_log_entry, default_active_manifest, file_len,
    generation_manifest, open_rw, read_f32_vector, read_generation_catalog, read_i8_vector,
    read_log_entries, read_state, read_storage_manifest, truncate_file, write_generation_catalog,
    write_state, write_storage_manifest, ActiveSegmentPaths, CollectionPaths,
};
use anyhow::{anyhow, bail, Context, Result};
use parking_lot::Mutex;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const SNAPSHOT_INTERVAL: usize = 64;
const COMPACTION_DELETE_THRESHOLD: usize = 64;
const COMPACTION_DELETE_RATIO: f32 = 0.20;

#[derive(Debug)]
struct ShadowInstall {
    manifest: CollectionManifest,
    docs: HashMap<u64, DocumentRecord>,
    external_to_doc: HashMap<String, u64>,
    filter_index: HashMap<String, HashMap<String, HashSet<u64>>>,
    clusters: HashMap<u32, ClusterNode>,
    entry_cluster: Option<u32>,
    segment_manifest: ActiveSegmentManifest,
    full_rewrite: bool,
}

#[derive(Debug, Default)]
struct LeaseState {
    active: HashMap<String, usize>,
    pending_cleanup: Vec<ActiveSegmentManifest>,
}

#[derive(Debug)]
struct ReadLease<'a> {
    generation: String,
    lease_state: &'a Mutex<LeaseState>,
}

impl Drop for ReadLease<'_> {
    fn drop(&mut self) {
        let mut lease_state = self.lease_state.lock();
        if let Some(count) = lease_state.active.get_mut(&self.generation) {
            *count -= 1;
            if *count == 0 {
                lease_state.active.remove(&self.generation);
            }
        }
    }
}

#[derive(Debug)]
pub struct Collection {
    pub manifest: CollectionManifest,
    pub docs: HashMap<u64, DocumentRecord>,
    pub external_to_doc: HashMap<String, u64>,
    pub filter_index: HashMap<String, HashMap<String, HashSet<u64>>>,
    pub numeric_index: HashMap<String, Vec<(f64, u64)>>,
    pub clusters: HashMap<u32, ClusterNode>,
    pub entry_cluster: Option<u32>,
    paths: CollectionPaths,
    generation_catalog: GenerationCatalog,
    active_manifest: ActiveSegmentManifest,
    active_paths: ActiveSegmentPaths,
    raw_file: File,
    quant_file: File,
    wal_file: File,
    dirty_ops: usize,
    lease_state: Mutex<LeaseState>,
}

impl Collection {
    pub fn create(
        base_path: impl AsRef<Path>,
        name: &str,
        dimension: usize,
        max_cluster_size: usize,
        graph_degree: usize,
    ) -> Result<Self> {
        let paths = CollectionPaths::new(base_path, name)?;
        let active_manifest = default_active_manifest();
        let active_paths = paths.active_paths(&active_manifest);
        let generation_catalog = GenerationCatalog {
            write: active_manifest.clone(),
            readable: vec![active_manifest.clone()],
            retired: Vec::new(),
        };
        let raw_file = open_rw(&active_paths.raw_vectors_file)?;
        let quant_file = open_rw(&active_paths.quant_vectors_file)?;
        let mut wal_file = open_rw(&paths.wal_file)?;
        truncate_file(&mut wal_file)?;
        write_storage_manifest(&paths.current_file, &active_manifest)?;
        write_generation_catalog(&paths.generations_file, &generation_catalog)?;

        let manifest = CollectionManifest {
            name: name.to_string(),
            dimension,
            next_doc_id: 0,
            next_cluster_id: 0,
            max_cluster_size,
            graph_degree,
        };
        let collection = Self {
            manifest,
            docs: HashMap::new(),
            external_to_doc: HashMap::new(),
            filter_index: HashMap::new(),
            numeric_index: HashMap::new(),
            clusters: HashMap::new(),
            entry_cluster: None,
            paths,
            generation_catalog,
            active_manifest,
            active_paths,
            raw_file,
            quant_file,
            wal_file,
            dirty_ops: 0,
            lease_state: Mutex::new(LeaseState::default()),
        };
        collection.persist()?;
        Ok(collection)
    }

    pub fn load(base_path: impl AsRef<Path>, name: &str) -> Result<Self> {
        let paths = CollectionPaths::new(base_path, name)?;
        let generation_catalog = read_generation_catalog(&paths.generations_file)?;
        let active_manifest = if paths.current_file.exists() {
            read_storage_manifest(&paths.current_file)?
        } else {
            generation_catalog.write.clone()
        };
        let active_paths = paths.active_paths(&active_manifest);
        let snapshot: CollectionSnapshot = read_state(&active_paths.state_file)?;
        let wal_entries: Vec<WalOp> = read_log_entries(&paths.wal_file)?;
        let mut collection = Self {
            manifest: snapshot.manifest,
            docs: snapshot.docs,
            external_to_doc: snapshot.external_to_doc,
            filter_index: snapshot.filter_index,
            numeric_index: HashMap::new(),
            clusters: snapshot.clusters,
            entry_cluster: snapshot.entry_cluster,
            generation_catalog,
            active_manifest,
            active_paths: active_paths.clone(),
            raw_file: open_rw(&active_paths.raw_vectors_file)?,
            quant_file: open_rw(&active_paths.quant_vectors_file)?,
            wal_file: open_rw(&paths.wal_file)?,
            paths,
            dirty_ops: 0,
            lease_state: Mutex::new(LeaseState::default()),
        };
        collection.rebuild_numeric_index();
        collection.replay_wal(wal_entries)?;
        Ok(collection)
    }

    pub fn persist(&self) -> Result<()> {
        let snapshot = CollectionSnapshot {
            manifest: self.manifest.clone(),
            docs: self.docs.clone(),
            external_to_doc: self.external_to_doc.clone(),
            filter_index: self.filter_index.clone(),
            clusters: self.clusters.clone(),
            entry_cluster: self.entry_cluster,
        };
        write_state(&self.active_paths.state_file, &snapshot)
    }

    pub fn insert_batch(&mut self, records: Vec<VectorRecord>) -> Result<usize> {
        let inserted = records.len();
        for record in records {
            self.record_wal(&WalOp::Insert {
                id: record.id.clone(),
                vector: record.vector.clone(),
                metadata: record.metadata.clone(),
            })?;
            self.insert_one(record)?;
            self.mark_dirty()?;
        }
        Ok(inserted)
    }

    pub fn delete(&mut self, external_id: &str) -> Result<bool> {
        if !self.external_to_doc.contains_key(external_id) {
            return Ok(false);
        }
        self.record_wal(&WalOp::Delete {
            id: external_id.to_string(),
        })?;
        let deleted = self.apply_delete(external_id)?;
        if deleted {
            self.mark_dirty()?;
        }
        Ok(deleted)
    }

    pub fn fetch(&self, external_id: &str) -> Result<Option<FetchedRecord>> {
        let _lease = self.begin_read();
        let Some(doc_id) = self.external_to_doc.get(external_id).copied() else {
            return Ok(None);
        };
        let doc = self.docs.get(&doc_id).context("document missing")?;
        if doc.deleted {
            return Ok(None);
        }
        let manifest = self
            .manifest_for_generation(&doc.generation)
            .context("document generation not found")?;
        let mut raw_file = open_rw(&self.paths.active_paths(manifest).raw_vectors_file)?;
        let vector = read_f32_vector(&mut raw_file, doc.raw_offset, self.manifest.dimension)?;
        Ok(Some((vector, doc.metadata.clone())))
    }

    pub fn search(
        &self,
        query: Vec<f32>,
        k: usize,
        filter: MetadataFilter,
        entry_points: usize,
        ef_search: usize,
        probe_clusters: usize,
    ) -> Result<SearchResult> {
        if query.len() != self.manifest.dimension {
            bail!("dimension mismatch for search");
        }

        let _lease = self.begin_read();
        let normalized = normalize(&query);
        let allowed = self.resolve_filter_candidates(&filter);
        let selected_clusters = top_n_centroids(
            &self.clusters,
            self.entry_cluster,
            &normalized,
            entry_points,
            ef_search,
            probe_clusters,
        );
        let mut quant_files: HashMap<String, File> = HashMap::new();
        let mut raw_files: HashMap<String, File> = HashMap::new();

        let mut candidates = Vec::new();
        for cluster_id in &selected_clusters {
            if let Some(cluster) = self.clusters.get(cluster_id) {
                for doc_id in &cluster.members {
                    if !self.doc_matches(*doc_id, allowed.as_ref(), &filter) {
                        continue;
                    }
                    let doc = self.docs.get(doc_id).context("candidate missing")?;
                    let quant_file = match quant_files.entry(doc.generation.clone()) {
                        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            let manifest = self
                                .manifest_for_generation(&doc.generation)
                                .context("candidate generation not found")?;
                            entry.insert(open_rw(
                                &self.paths.active_paths(manifest).quant_vectors_file,
                            )?)
                        }
                    };
                    let encoded =
                        read_i8_vector(quant_file, doc.quant_offset, self.manifest.dimension)?;
                    let approx_score = cosine_quantized(&normalized, &encoded);
                    candidates.push((*doc_id, approx_score));
                }
            }
        }

        if candidates.is_empty() && !filter.is_empty() {
            for doc_id in allowed.unwrap_or_default() {
                if !self.doc_matches(doc_id, None, &filter) {
                    continue;
                }
                let doc = self.docs.get(&doc_id).context("filtered doc missing")?;
                let quant_file = match quant_files.entry(doc.generation.clone()) {
                    std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let manifest = self
                            .manifest_for_generation(&doc.generation)
                            .context("filtered generation not found")?;
                        entry.insert(open_rw(
                            &self.paths.active_paths(manifest).quant_vectors_file,
                        )?)
                    }
                };
                let encoded =
                    read_i8_vector(quant_file, doc.quant_offset, self.manifest.dimension)?;
                let approx_score = cosine_quantized(&normalized, &encoded);
                candidates.push((doc_id, approx_score));
            }
        }

        let candidate_count = candidates.len();
        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        candidates.truncate((k * 4).max(k));

        let mut reranked = Vec::new();
        for (doc_id, _) in candidates {
            let doc = self.docs.get(&doc_id).context("rerank doc missing")?;
            let raw_file = match raw_files.entry(doc.generation.clone()) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let manifest = self
                        .manifest_for_generation(&doc.generation)
                        .context("rerank generation not found")?;
                    entry.insert(open_rw(
                        &self.paths.active_paths(manifest).raw_vectors_file,
                    )?)
                }
            };
            let vector = read_f32_vector(raw_file, doc.raw_offset, self.manifest.dimension)?;
            reranked.push((doc_id, cosine(&normalized, &vector)));
        }

        reranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        let hits = reranked
            .into_iter()
            .take(k)
            .map(|(doc_id, score)| {
                let doc = self.docs.get(&doc_id).unwrap();
                SearchHit {
                    id: doc.external_id.clone(),
                    score,
                    metadata: doc.metadata.clone(),
                }
            })
            .collect();

        Ok(SearchResult {
            hits,
            stats: SearchStats {
                visited_clusters: selected_clusters.len(),
                candidate_count,
            },
        })
    }

    pub fn brute_force_search(
        &self,
        query: Vec<f32>,
        k: usize,
        filter: MetadataFilter,
    ) -> Result<SearchResult> {
        let _lease = self.begin_read();
        let normalized = normalize(&query);
        let allowed = self.resolve_filter_candidates(&filter);
        let mut raw_files: HashMap<String, File> = HashMap::new();
        let mut hits = Vec::new();
        for (doc_id, doc) in &self.docs {
            if doc.deleted || !self.doc_matches(*doc_id, allowed.as_ref(), &filter) {
                continue;
            }
            let raw_file = match raw_files.entry(doc.generation.clone()) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let manifest = self
                        .manifest_for_generation(&doc.generation)
                        .context("brute force generation not found")?;
                    entry.insert(open_rw(
                        &self.paths.active_paths(manifest).raw_vectors_file,
                    )?)
                }
            };
            let vector = read_f32_vector(raw_file, doc.raw_offset, self.manifest.dimension)?;
            hits.push((*doc_id, cosine(&normalized, &vector)));
        }
        hits.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        let results = hits
            .into_iter()
            .take(k)
            .map(|(doc_id, score)| {
                let doc = self.docs.get(&doc_id).unwrap();
                SearchHit {
                    id: doc.external_id.clone(),
                    score,
                    metadata: doc.metadata.clone(),
                }
            })
            .collect();
        Ok(SearchResult {
            hits: results,
            stats: SearchStats {
                visited_clusters: self.clusters.len(),
                candidate_count: self.docs.values().filter(|doc| !doc.deleted).count(),
            },
        })
    }

    pub fn compact(&mut self) -> Result<()> {
        let (live_records, full_rewrite) = self.live_records()?;
        if live_records.is_empty() {
            return Ok(());
        }
        let shadow = self.build_shadow_compaction(live_records, full_rewrite)?;
        self.install_shadow_compaction(shadow)?;
        truncate_file(&mut self.wal_file)?;
        self.dirty_ops = 0;
        Ok(())
    }

    pub fn stats(&self) -> Result<CollectionStats> {
        let live_docs = self.docs.values().filter(|doc| !doc.deleted).count();
        let deleted_docs = self.docs.values().filter(|doc| doc.deleted).count();
        let disk_bytes = file_len(&self.paths.current_file)?
            + file_len(&self.active_paths.state_file)?
            + file_len(&self.active_paths.raw_vectors_file)?
            + file_len(&self.active_paths.quant_vectors_file)?
            + file_len(&self.paths.wal_file)?;
        Ok(CollectionStats {
            collection: self.manifest.name.clone(),
            dimension: self.manifest.dimension,
            current_generation: self.active_manifest.generation.clone(),
            readable_generations: self.generation_catalog.readable.len(),
            retired_generations: self.generation_catalog.retired.len(),
            live_docs,
            deleted_docs,
            cluster_count: self.clusters.len(),
            disk_bytes,
            wal_bytes: file_len(&self.paths.wal_file)?,
        })
    }

    pub fn diagnostics(&self) -> Result<CollectionDiagnostics> {
        let lease_state = self.lease_state.lock();
        Ok(CollectionDiagnostics {
            collection: self.manifest.name.clone(),
            current_generation: self.active_manifest.generation.clone(),
            readable_generations: self
                .generation_catalog
                .readable
                .iter()
                .map(|manifest| manifest.generation.clone())
                .collect(),
            retired_generations: self
                .generation_catalog
                .retired
                .iter()
                .map(|manifest| manifest.generation.clone())
                .collect(),
            active_reader_leases: lease_state.active.clone(),
            pending_cleanup_generations: lease_state
                .pending_cleanup
                .iter()
                .map(|manifest| manifest.generation.clone())
                .collect(),
            compaction_recommended: self.needs_compaction(),
            wal_bytes: file_len(&self.paths.wal_file)?,
        })
    }

    pub fn root_path(&self) -> &PathBuf {
        &self.paths.root
    }

    pub fn needs_compaction(&self) -> bool {
        let live_docs = self.docs.values().filter(|doc| !doc.deleted).count();
        let deleted_docs = self.docs.values().filter(|doc| doc.deleted).count();
        if deleted_docs == 0 {
            return false;
        }
        deleted_docs >= COMPACTION_DELETE_THRESHOLD
            || (deleted_docs as f32 / (live_docs + deleted_docs) as f32) >= COMPACTION_DELETE_RATIO
    }

    fn replay_wal(&mut self, wal_entries: Vec<WalOp>) -> Result<()> {
        if wal_entries.is_empty() {
            return Ok(());
        }
        for entry in wal_entries {
            match entry {
                WalOp::Insert {
                    id,
                    vector,
                    metadata,
                } => {
                    if self.external_to_doc.contains_key(&id) {
                        continue;
                    }
                    self.insert_one(VectorRecord {
                        id,
                        vector,
                        metadata,
                    })?;
                }
                WalOp::Delete { id } => {
                    let _ = self.apply_delete(&id)?;
                }
            }
        }
        self.persist()?;
        truncate_file(&mut self.wal_file)?;
        self.dirty_ops = 0;
        Ok(())
    }

    fn record_wal(&mut self, op: &WalOp) -> Result<()> {
        append_log_entry(&mut self.wal_file, op)
    }

    fn mark_dirty(&mut self) -> Result<()> {
        self.dirty_ops += 1;
        if self.dirty_ops >= SNAPSHOT_INTERVAL {
            self.persist()?;
            truncate_file(&mut self.wal_file)?;
            self.rollover_write_generation()?;
            self.dirty_ops = 0;
        }
        Ok(())
    }

    fn build_shadow_compaction(
        &mut self,
        live_records: Vec<VectorRecord>,
        full_rewrite: bool,
    ) -> Result<ShadowInstall> {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock before unix epoch")?
            .as_nanos();
        let segment_manifest = generation_manifest(format!("g{}-{nonce}", std::process::id()));
        let active_paths = self.paths.active_paths(&segment_manifest);
        let mut shadow = Self {
            manifest: CollectionManifest {
                name: self.manifest.name.clone(),
                dimension: self.manifest.dimension,
                next_doc_id: 0,
                next_cluster_id: 0,
                max_cluster_size: self.manifest.max_cluster_size,
                graph_degree: self.manifest.graph_degree,
            },
            docs: HashMap::new(),
            external_to_doc: HashMap::new(),
            filter_index: HashMap::new(),
            numeric_index: HashMap::new(),
            clusters: HashMap::new(),
            entry_cluster: None,
            paths: self.paths.clone(),
            generation_catalog: GenerationCatalog {
                write: segment_manifest.clone(),
                readable: vec![segment_manifest.clone()],
                retired: Vec::new(),
            },
            active_manifest: segment_manifest.clone(),
            active_paths: active_paths.clone(),
            raw_file: open_rw(&active_paths.raw_vectors_file)?,
            quant_file: open_rw(&active_paths.quant_vectors_file)?,
            wal_file: open_rw(&self.paths.wal_file)?,
            dirty_ops: 0,
            lease_state: Mutex::new(LeaseState::default()),
        };
        for record in live_records {
            shadow.insert_one(record)?;
        }
        shadow.persist()?;

        Ok(ShadowInstall {
            manifest: shadow.manifest.clone(),
            docs: shadow.docs.clone(),
            external_to_doc: shadow.external_to_doc.clone(),
            filter_index: shadow.filter_index.clone(),
            clusters: shadow.clusters.clone(),
            entry_cluster: shadow.entry_cluster,
            segment_manifest,
            full_rewrite,
        })
    }

    fn install_shadow_compaction(&mut self, shadow: ShadowInstall) -> Result<()> {
        let compacted_generations: Vec<ActiveSegmentManifest> = if shadow.full_rewrite {
            self.generation_catalog.readable.clone()
        } else {
            self.generation_catalog
                .readable
                .iter()
                .filter(|manifest| manifest.generation != self.active_manifest.generation)
                .cloned()
                .collect()
        };
        let mut next_catalog = self.generation_catalog.clone();
        next_catalog.retired.extend(compacted_generations.clone());
        let compacted_set: HashSet<String> = compacted_generations
            .iter()
            .map(|manifest| manifest.generation.clone())
            .collect();
        next_catalog
            .readable
            .retain(|manifest| !compacted_set.contains(&manifest.generation));
        next_catalog.readable.push(shadow.segment_manifest.clone());
        if shadow.full_rewrite {
            next_catalog.write = shadow.segment_manifest.clone();
            write_storage_manifest(&self.paths.current_file, &shadow.segment_manifest)?;
            self.active_manifest = shadow.segment_manifest.clone();
            self.active_paths = self.paths.active_paths(&shadow.segment_manifest);
            self.raw_file = open_rw(&self.active_paths.raw_vectors_file)?;
            self.quant_file = open_rw(&self.active_paths.quant_vectors_file)?;
        }
        write_generation_catalog(&self.paths.generations_file, &next_catalog)?;

        self.generation_catalog = next_catalog;
        self.manifest = shadow.manifest;
        self.docs = shadow.docs;
        self.external_to_doc = shadow.external_to_doc;
        self.filter_index = shadow.filter_index;
        self.rebuild_numeric_index();
        self.clusters = shadow.clusters;
        self.entry_cluster = shadow.entry_cluster;

        self.cleanup_retired_generations()?;
        Ok(())
    }

    fn cleanup_retired_generations(&mut self) -> Result<()> {
        let mut kept = Vec::new();
        let mut lease_state = self.lease_state.lock();
        for retired in &self.generation_catalog.retired {
            if retired.generation == self.active_manifest.generation {
                continue;
            }
            if lease_state
                .active
                .get(&retired.generation)
                .copied()
                .unwrap_or(0)
                > 0
            {
                if !lease_state
                    .pending_cleanup
                    .iter()
                    .any(|manifest| manifest.generation == retired.generation)
                {
                    lease_state.pending_cleanup.push(retired.clone());
                }
                kept.push(retired.clone());
                continue;
            }
            let retired_paths = self.paths.active_paths(retired);
            std::fs::remove_file(&retired_paths.state_file).ok();
            std::fs::remove_file(&retired_paths.raw_vectors_file).ok();
            std::fs::remove_file(&retired_paths.quant_vectors_file).ok();
            lease_state
                .pending_cleanup
                .retain(|manifest| manifest.generation != retired.generation);
        }
        drop(lease_state);
        self.generation_catalog.retired = kept;
        write_generation_catalog(&self.paths.generations_file, &self.generation_catalog)?;
        Ok(())
    }

    fn rollover_write_generation(&mut self) -> Result<()> {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock before unix epoch")?
            .as_nanos();
        let next_manifest = generation_manifest(format!("g{}-{nonce}", std::process::id()));
        let next_paths = self.paths.active_paths(&next_manifest);
        self.raw_file = open_rw(&next_paths.raw_vectors_file)?;
        self.quant_file = open_rw(&next_paths.quant_vectors_file)?;
        self.active_manifest = next_manifest.clone();
        self.active_paths = next_paths;
        self.generation_catalog.write = next_manifest.clone();
        self.generation_catalog.readable.push(next_manifest.clone());
        write_storage_manifest(&self.paths.current_file, &next_manifest)?;
        write_generation_catalog(&self.paths.generations_file, &self.generation_catalog)?;
        Ok(())
    }

    fn manifest_for_generation(&self, generation: &str) -> Option<&ActiveSegmentManifest> {
        self.generation_catalog
            .readable
            .iter()
            .find(|manifest| manifest.generation == generation)
            .or_else(|| {
                (self.generation_catalog.write.generation == generation)
                    .then_some(&self.generation_catalog.write)
            })
            .or_else(|| {
                self.generation_catalog
                    .retired
                    .iter()
                    .find(|manifest| manifest.generation == generation)
            })
    }

    fn begin_read(&self) -> ReadLease<'_> {
        let generation = self.active_manifest.generation.clone();
        let mut lease_state = self.lease_state.lock();
        *lease_state.active.entry(generation.clone()).or_insert(0) += 1;
        ReadLease {
            generation,
            lease_state: &self.lease_state,
        }
    }

    fn live_records(&mut self) -> Result<(Vec<VectorRecord>, bool)> {
        let full_rewrite = true;
        let mut doc_ids: Vec<u64> = self
            .docs
            .iter()
            .filter_map(|(doc_id, doc)| (!doc.deleted && full_rewrite).then_some(*doc_id))
            .collect();
        doc_ids.sort_unstable();

        let mut live = Vec::with_capacity(doc_ids.len());
        for doc_id in doc_ids {
            let doc = self.docs.get(&doc_id).context("document missing")?;
            let manifest = self
                .manifest_for_generation(&doc.generation)
                .context("cold generation not found")?;
            let mut raw_file = open_rw(&self.paths.active_paths(manifest).raw_vectors_file)?;
            let vector = read_f32_vector(&mut raw_file, doc.raw_offset, self.manifest.dimension)?;
            live.push(VectorRecord {
                id: doc.external_id.clone(),
                vector,
                metadata: doc.metadata.clone(),
            });
        }
        Ok((live, full_rewrite))
    }

    fn insert_one(&mut self, record: VectorRecord) -> Result<()> {
        if record.vector.len() != self.manifest.dimension {
            bail!(
                "dimension mismatch: expected {}, got {}",
                self.manifest.dimension,
                record.vector.len()
            );
        }
        if self.external_to_doc.contains_key(&record.id) {
            bail!("vector id '{}' already exists", record.id);
        }

        let normalized = normalize(&record.vector);
        let quantized = quantize_normalized(&normalized);
        let raw_offset = append_f32_vector(&mut self.raw_file, &normalized)?;
        let quant_offset = append_i8_vector(&mut self.quant_file, &quantized)?;

        let doc_id = self.manifest.next_doc_id;
        self.manifest.next_doc_id += 1;

        let doc = DocumentRecord {
            doc_id,
            external_id: record.id.clone(),
            generation: self.active_manifest.generation.clone(),
            raw_offset,
            quant_offset,
            metadata: record.metadata.clone(),
            deleted: false,
        };
        self.docs.insert(doc_id, doc.clone());
        self.external_to_doc.insert(record.id, doc_id);
        self.index_metadata(doc_id, &doc.metadata);
        self.index_numeric_metadata(doc_id, &doc.metadata);
        self.assign_to_cluster(doc_id, &normalized)?;
        Ok(())
    }

    fn apply_delete(&mut self, external_id: &str) -> Result<bool> {
        let Some(doc_id) = self.external_to_doc.get(external_id).copied() else {
            return Ok(false);
        };
        let metadata = {
            let doc = self.docs.get_mut(&doc_id).context("document missing")?;
            if doc.deleted {
                return Ok(false);
            }
            doc.deleted = true;
            doc.metadata.clone()
        };
        self.remove_metadata(doc_id, &metadata);
        self.remove_numeric_metadata(doc_id, &metadata);
        for cluster in self.clusters.values_mut() {
            cluster.members.retain(|member| *member != doc_id);
        }
        self.rebuild_clusters()?;
        Ok(true)
    }

    fn rebuild_clusters(&mut self) -> Result<()> {
        let cluster_ids: Vec<u32> = self.clusters.keys().copied().collect();
        for cluster_id in cluster_ids {
            let empty = self
                .clusters
                .get(&cluster_id)
                .map(|cluster| cluster.members.is_empty())
                .unwrap_or(false);
            if empty {
                self.clusters.remove(&cluster_id);
            } else if let Some(cluster) = self.clusters.get_mut(&cluster_id) {
                recalc_centroid(
                    cluster,
                    self.manifest.dimension,
                    &self.docs,
                    &mut self.raw_file,
                )?;
            }
        }
        let cluster_ids: Vec<u32> = self.clusters.keys().copied().collect();
        for cluster_id in cluster_ids {
            self.rewire_cluster(cluster_id);
        }
        if self
            .entry_cluster
            .is_some_and(|entry| !self.clusters.contains_key(&entry))
        {
            self.entry_cluster = self.clusters.keys().next().copied();
        }
        Ok(())
    }

    fn index_metadata(&mut self, doc_id: u64, metadata: &HashMap<String, String>) {
        for (key, value) in metadata {
            self.filter_index
                .entry(key.clone())
                .or_default()
                .entry(value.clone())
                .or_default()
                .insert(doc_id);
        }
    }

    fn index_numeric_metadata(&mut self, doc_id: u64, metadata: &HashMap<String, String>) {
        for (key, value) in metadata {
            let Ok(number) = value.parse::<f64>() else {
                continue;
            };
            let entries = self.numeric_index.entry(key.clone()).or_default();
            let position = entries
                .binary_search_by(|(existing, _)| existing.total_cmp(&number))
                .unwrap_or_else(|index| index);
            entries.insert(position, (number, doc_id));
        }
    }

    fn remove_numeric_metadata(&mut self, doc_id: u64, metadata: &HashMap<String, String>) {
        for (key, value) in metadata {
            let Ok(number) = value.parse::<f64>() else {
                continue;
            };
            if let Some(entries) = self.numeric_index.get_mut(key) {
                entries.retain(|(existing, existing_doc)| {
                    !(*existing_doc == doc_id && existing.total_cmp(&number).is_eq())
                });
            }
        }
    }

    fn rebuild_numeric_index(&mut self) {
        self.numeric_index.clear();
        let docs: Vec<(u64, HashMap<String, String>)> = self
            .docs
            .iter()
            .filter_map(|(doc_id, doc)| (!doc.deleted).then_some((*doc_id, doc.metadata.clone())))
            .collect();
        for (doc_id, metadata) in docs {
            self.index_numeric_metadata(doc_id, &metadata);
        }
    }

    fn remove_metadata(&mut self, doc_id: u64, metadata: &HashMap<String, String>) {
        for (key, value) in metadata {
            if let Some(values) = self.filter_index.get_mut(key) {
                if let Some(ids) = values.get_mut(value) {
                    ids.remove(&doc_id);
                }
            }
        }
    }

    fn assign_to_cluster(&mut self, doc_id: u64, normalized: &[f32]) -> Result<()> {
        if self.clusters.is_empty() {
            let cluster_id = self.new_cluster(vec![doc_id], normalized.to_vec());
            self.entry_cluster = Some(cluster_id);
            return Ok(());
        }

        let mut best_cluster = None;
        let mut best_score = f32::MIN;
        for (cluster_id, cluster) in &self.clusters {
            let score = cosine(normalized, &cluster.centroid);
            if score > best_score {
                best_score = score;
                best_cluster = Some(*cluster_id);
            }
        }

        let cluster_id = best_cluster.context("no cluster selected")?;
        let should_split = {
            let cluster = self.clusters.get_mut(&cluster_id).unwrap();
            cluster.members.push(doc_id);
            recalc_centroid(
                cluster,
                self.manifest.dimension,
                &self.docs,
                &mut self.raw_file,
            )?;
            cluster.members.len() > self.manifest.max_cluster_size
        };

        if should_split {
            self.split_cluster(cluster_id)?;
        } else {
            self.rewire_cluster(cluster_id);
        }
        Ok(())
    }

    fn new_cluster(&mut self, members: Vec<u64>, centroid: Vec<f32>) -> u32 {
        let cluster_id = self.manifest.next_cluster_id;
        self.manifest.next_cluster_id += 1;
        self.clusters.insert(
            cluster_id,
            ClusterNode {
                cluster_id,
                centroid,
                members,
                neighbors: Vec::new(),
            },
        );
        self.rewire_cluster(cluster_id);
        cluster_id
    }

    fn split_cluster(&mut self, cluster_id: u32) -> Result<()> {
        let members = self
            .clusters
            .get(&cluster_id)
            .map(|cluster| cluster.members.clone())
            .ok_or_else(|| anyhow!("cluster not found"))?;

        if members.len() < 2 {
            return Ok(());
        }

        let seed_a = members[0];
        let seed_b = members[members.len() / 2];
        let mut group_a = Vec::new();
        let mut group_b = Vec::new();
        let mut centroid_a = self.read_raw(seed_a)?;
        let mut centroid_b = self.read_raw(seed_b)?;

        for _ in 0..4 {
            group_a.clear();
            group_b.clear();
            for doc_id in &members {
                let vector = self.read_raw(*doc_id)?;
                let score_a = cosine(&vector, &centroid_a);
                let score_b = cosine(&vector, &centroid_b);
                if score_a >= score_b {
                    group_a.push(*doc_id);
                } else {
                    group_b.push(*doc_id);
                }
            }
            if group_a.is_empty() || group_b.is_empty() {
                break;
            }
            centroid_a = average_centroid(
                &group_a,
                self.manifest.dimension,
                &self.docs,
                &mut self.raw_file,
            )?;
            centroid_b = average_centroid(
                &group_b,
                self.manifest.dimension,
                &self.docs,
                &mut self.raw_file,
            )?;
        }

        if group_a.is_empty() || group_b.is_empty() {
            return Ok(());
        }

        if let Some(cluster) = self.clusters.get_mut(&cluster_id) {
            cluster.members = group_a.clone();
            cluster.centroid = centroid_a.clone();
        }
        let new_cluster_id = self.new_cluster(group_b, centroid_b);
        self.rewire_cluster(cluster_id);
        self.rewire_cluster(new_cluster_id);
        Ok(())
    }

    fn rewire_cluster(&mut self, cluster_id: u32) {
        let Some(target_centroid) = self.clusters.get(&cluster_id).map(|c| c.centroid.clone())
        else {
            return;
        };

        let mut neighbors: Vec<(u32, f32)> = self
            .clusters
            .iter()
            .filter_map(|(other_id, other)| {
                if *other_id == cluster_id {
                    None
                } else {
                    Some((*other_id, cosine(&target_centroid, &other.centroid)))
                }
            })
            .collect();

        neighbors.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
        let top: Vec<u32> = neighbors
            .into_iter()
            .take(self.manifest.graph_degree)
            .map(|(id, _)| id)
            .collect();

        if let Some(cluster) = self.clusters.get_mut(&cluster_id) {
            cluster.neighbors = top.clone();
        }
        for neighbor in top {
            if let Some(cluster) = self.clusters.get_mut(&neighbor) {
                if !cluster.neighbors.contains(&cluster_id) {
                    cluster.neighbors.push(cluster_id);
                    cluster.neighbors.sort_unstable();
                    cluster.neighbors.dedup();
                    if cluster.neighbors.len() > self.manifest.graph_degree {
                        cluster.neighbors.truncate(self.manifest.graph_degree);
                    }
                }
            }
        }
    }

    fn resolve_filter_candidates(&self, filter: &MetadataFilter) -> Option<HashSet<u64>> {
        if filter.is_empty() {
            return None;
        }
        let mut sets = Vec::new();
        for (key, value) in filter.exact_terms() {
            let set = self
                .filter_index
                .get(key)
                .and_then(|values| values.get(value))
                .cloned()
                .unwrap_or_default();
            sets.push(set);
        }
        for (field, gt, gte, lt, lte) in filter.range_predicates() {
            let mut set = HashSet::new();
            if let Some(entries) = self.numeric_index.get(field) {
                for (value, doc_id) in entries {
                    if gt.is_none_or(|bound| *value > bound)
                        && gte.is_none_or(|bound| *value >= bound)
                        && lt.is_none_or(|bound| *value < bound)
                        && lte.is_none_or(|bound| *value <= bound)
                    {
                        set.insert(*doc_id);
                    }
                }
            }
            sets.push(set);
        }
        let mut iter = sets.into_iter();
        let first = iter.next()?;
        Some(iter.fold(first, |acc, set| acc.intersection(&set).copied().collect()))
    }

    fn doc_matches(
        &self,
        doc_id: u64,
        allowed: Option<&HashSet<u64>>,
        filter: &MetadataFilter,
    ) -> bool {
        let Some(doc) = self.docs.get(&doc_id) else {
            return false;
        };
        if doc.deleted {
            return false;
        }
        allowed.map(|set| set.contains(&doc_id)).unwrap_or(true) && filter.matches(&doc.metadata)
    }

    fn read_raw(&mut self, doc_id: u64) -> Result<Vec<f32>> {
        let doc = self.docs.get(&doc_id).context("document missing")?;
        read_f32_vector(&mut self.raw_file, doc.raw_offset, self.manifest.dimension)
    }
}

fn average_centroid(
    members: &[u64],
    dimension: usize,
    docs: &HashMap<u64, DocumentRecord>,
    raw_file: &mut File,
) -> Result<Vec<f32>> {
    let mut centroid = vec![0.0; dimension];
    for doc_id in members {
        let doc = docs.get(doc_id).context("member missing")?;
        let vector = read_f32_vector(raw_file, doc.raw_offset, dimension)?;
        for (idx, value) in vector.iter().enumerate() {
            centroid[idx] += value;
        }
    }
    let count = members.len() as f32;
    for value in &mut centroid {
        *value /= count.max(1.0);
    }
    Ok(normalize(&centroid))
}

fn recalc_centroid(
    cluster: &mut ClusterNode,
    dimension: usize,
    docs: &HashMap<u64, DocumentRecord>,
    raw_file: &mut File,
) -> Result<()> {
    cluster.centroid = average_centroid(&cluster.members, dimension, docs, raw_file)?;
    Ok(())
}
