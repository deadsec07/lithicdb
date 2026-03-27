use crate::engine::collection::Collection;
use crate::models::api::{CreateCollectionRequest, SearchRequest, VectorRecord};
use crate::models::types::{CollectionDiagnostics, CollectionStats, SearchResult};
use anyhow::{bail, Context, Result};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Database {
    root_path: PathBuf,
    collections: Arc<RwLock<HashMap<String, Arc<RwLock<Collection>>>>>,
}

impl Database {
    pub fn open(root_path: impl AsRef<Path>) -> Result<Self> {
        let root_path = root_path.as_ref().to_path_buf();
        std::fs::create_dir_all(&root_path)?;

        let mut collections = HashMap::new();
        for entry in std::fs::read_dir(&root_path)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            let collection = Collection::load(&root_path, &name)?;
            collections.insert(name, Arc::new(RwLock::new(collection)));
        }

        Ok(Self {
            root_path,
            collections: Arc::new(RwLock::new(collections)),
        })
    }

    pub fn create_collection(&self, request: CreateCollectionRequest) -> Result<()> {
        if request.dimension == 0 {
            bail!("dimension must be greater than zero");
        }
        let mut guard = self.collections.write();
        if guard.contains_key(&request.name) {
            bail!("collection '{}' already exists", request.name);
        }
        let collection = Collection::create(
            &self.root_path,
            &request.name,
            request.dimension,
            request.max_cluster_size.unwrap_or(256),
            request.graph_degree.unwrap_or(8),
        )?;
        guard.insert(request.name, Arc::new(RwLock::new(collection)));
        Ok(())
    }

    fn get_collection(&self, name: &str) -> Result<Arc<RwLock<Collection>>> {
        self.collections
            .read()
            .get(name)
            .cloned()
            .with_context(|| format!("collection '{}' not found", name))
    }

    pub fn insert_vectors(&self, collection: &str, records: Vec<VectorRecord>) -> Result<usize> {
        let collection = self.get_collection(collection)?;
        let mut guard = collection.write();
        guard.insert_batch(records)
    }

    pub fn delete_vector(&self, collection: &str, id: &str) -> Result<bool> {
        let collection = self.get_collection(collection)?;
        let mut guard = collection.write();
        guard.delete(id)
    }

    pub fn fetch_vector(
        &self,
        collection: &str,
        id: &str,
    ) -> Result<Option<(Vec<f32>, HashMap<String, String>)>> {
        let collection = self.get_collection(collection)?;
        let guard = collection.read();
        guard.fetch(id)
    }

    pub fn search(&self, collection: &str, request: SearchRequest) -> Result<SearchResult> {
        let collection = self.get_collection(collection)?;
        let guard = collection.read();
        guard.search(
            request.vector,
            request.k,
            request.filter,
            request.entry_points.unwrap_or(4),
            request.ef_search.unwrap_or(24),
            request.probe_clusters.unwrap_or(12),
        )
    }

    pub fn brute_force_search(
        &self,
        collection: &str,
        request: SearchRequest,
    ) -> Result<SearchResult> {
        let collection = self.get_collection(collection)?;
        let guard = collection.read();
        guard.brute_force_search(request.vector, request.k, request.filter)
    }

    pub fn compact_collection(&self, collection: &str) -> Result<CollectionStats> {
        let collection = self.get_collection(collection)?;
        let mut guard = collection.write();
        guard.compact()?;
        guard.stats()
    }

    pub fn collection_stats(&self, collection: &str) -> Result<CollectionStats> {
        let collection = self.get_collection(collection)?;
        let guard = collection.read();
        guard.stats()
    }

    pub fn collection_diagnostics(&self, collection: &str) -> Result<CollectionDiagnostics> {
        let collection = self.get_collection(collection)?;
        let guard = collection.read();
        guard.diagnostics()
    }

    pub fn run_maintenance(&self) -> Result<Vec<String>> {
        let collections: Vec<(String, Arc<RwLock<Collection>>)> = self
            .collections
            .read()
            .iter()
            .map(|(name, collection)| (name.clone(), Arc::clone(collection)))
            .collect();

        let mut compacted = Vec::new();
        for (name, collection) in collections {
            let needs_compaction = {
                let guard = collection.read();
                guard.needs_compaction()
            };
            if needs_compaction {
                let mut guard = collection.write();
                if guard.needs_compaction() {
                    guard.compact()?;
                    compacted.push(name);
                }
            }
        }
        Ok(compacted)
    }

    pub fn backup_collection(&self, collection: &str) -> Result<String> {
        let source = self.root_path.join(collection);
        if !source.exists() {
            bail!("collection '{}' not found", collection);
        }
        let backup_name = format!(
            "{}-{}",
            collection,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs()
        );
        let target = self.root_path.join("_backups").join(&backup_name);
        copy_dir_all(&source, &target)?;
        Ok(backup_name)
    }

    pub fn restore_collection(&self, backup_name: &str, target_name: &str) -> Result<()> {
        let source = self.root_path.join("_backups").join(backup_name);
        if !source.exists() {
            bail!("backup '{}' not found", backup_name);
        }
        let target = self.root_path.join(target_name);
        if target.exists() {
            bail!("collection '{}' already exists", target_name);
        }
        copy_dir_all(&source, &target)?;
        let collection = Collection::load(&self.root_path, target_name)?;
        self.collections
            .write()
            .insert(target_name.to_string(), Arc::new(RwLock::new(collection)));
        Ok(())
    }
}

fn copy_dir_all(source: &Path, target: &Path) -> Result<()> {
    fs::create_dir_all(target)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let entry_type = entry.file_type()?;
        let destination = target.join(entry.file_name());
        if entry_type.is_dir() {
            copy_dir_all(&entry.path(), &destination)?;
        } else {
            fs::copy(entry.path(), destination)?;
        }
    }
    Ok(())
}
