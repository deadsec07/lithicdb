use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

pub type Metadata = HashMap<String, String>;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MetadataFilter {
    #[default]
    Empty,
    ExactMap(HashMap<String, String>),
    Structured(StructuredMetadataFilter),
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StructuredMetadataFilter {
    #[serde(default)]
    pub must: Vec<MetadataPredicate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum MetadataPredicate {
    Eq { field: String, value: String },
    Range {
        field: String,
        #[serde(default)]
        gt: Option<f64>,
        #[serde(default)]
        gte: Option<f64>,
        #[serde(default)]
        lt: Option<f64>,
        #[serde(default)]
        lte: Option<f64>,
    },
}

impl MetadataFilter {
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Empty => true,
            Self::ExactMap(map) => map.is_empty(),
            Self::Structured(filter) => filter.must.is_empty(),
        }
    }

    pub fn exact_terms(&self) -> Vec<(&str, &str)> {
        match self {
            Self::Empty => Vec::new(),
            Self::ExactMap(map) => map.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect(),
            Self::Structured(filter) => filter
                .must
                .iter()
                .filter_map(|predicate| match predicate {
                    MetadataPredicate::Eq { field, value } => Some((field.as_str(), value.as_str())),
                    MetadataPredicate::Range { .. } => None,
                })
                .collect(),
        }
    }

    pub fn range_predicates(&self) -> Vec<(&str, Option<f64>, Option<f64>, Option<f64>, Option<f64>)> {
        match self {
            Self::Empty | Self::ExactMap(_) => Vec::new(),
            Self::Structured(filter) => filter
                .must
                .iter()
                .filter_map(|predicate| match predicate {
                    MetadataPredicate::Range {
                        field,
                        gt,
                        gte,
                        lt,
                        lte,
                    } => Some((field.as_str(), *gt, *gte, *lt, *lte)),
                    MetadataPredicate::Eq { .. } => None,
                })
                .collect(),
        }
    }

    pub fn matches(&self, metadata: &Metadata) -> bool {
        match self {
            Self::Empty => true,
            Self::ExactMap(map) => map.iter().all(|(key, value)| metadata.get(key) == Some(value)),
            Self::Structured(filter) => filter.must.iter().all(|predicate| predicate.matches(metadata)),
        }
    }
}

impl MetadataPredicate {
    fn matches(&self, metadata: &Metadata) -> bool {
        match self {
            MetadataPredicate::Eq { field, value } => metadata.get(field) == Some(value),
            MetadataPredicate::Range {
                field,
                gt,
                gte,
                lt,
                lte,
            } => {
                let Some(raw) = metadata.get(field) else {
                    return false;
                };
                let Ok(number) = raw.parse::<f64>() else {
                    return false;
                };
                gt.is_none_or(|bound| number > bound)
                    && gte.is_none_or(|bound| number >= bound)
                    && lt.is_none_or(|bound| number < bound)
                    && lte.is_none_or(|bound| number <= bound)
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionManifest {
    pub name: String,
    pub dimension: usize,
    pub next_doc_id: u64,
    pub next_cluster_id: u32,
    pub max_cluster_size: usize,
    pub graph_degree: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentRecord {
    pub doc_id: u64,
    pub external_id: String,
    pub generation: String,
    pub raw_offset: u64,
    pub quant_offset: u64,
    pub metadata: Metadata,
    pub deleted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNode {
    pub cluster_id: u32,
    pub centroid: Vec<f32>,
    pub members: Vec<u64>,
    pub neighbors: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionSnapshot {
    pub manifest: CollectionManifest,
    pub docs: HashMap<u64, DocumentRecord>,
    pub external_to_doc: HashMap<String, u64>,
    pub filter_index: HashMap<String, HashMap<String, HashSet<u64>>>,
    pub clusters: HashMap<u32, ClusterNode>,
    pub entry_cluster: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveSegmentManifest {
    pub generation: String,
    pub state_file: String,
    pub raw_vectors_file: String,
    pub quant_vectors_file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerationCatalog {
    pub write: ActiveSegmentManifest,
    pub readable: Vec<ActiveSegmentManifest>,
    pub retired: Vec<ActiveSegmentManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchHit {
    pub id: String,
    pub score: f32,
    pub metadata: Metadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchStats {
    pub visited_clusters: usize,
    pub candidate_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub hits: Vec<SearchHit>,
    pub stats: SearchStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalOp {
    Insert {
        id: String,
        vector: Vec<f32>,
        metadata: Metadata,
    },
    Delete {
        id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionStats {
    pub collection: String,
    pub dimension: usize,
    pub current_generation: String,
    pub readable_generations: usize,
    pub retired_generations: usize,
    pub live_docs: usize,
    pub deleted_docs: usize,
    pub cluster_count: usize,
    pub disk_bytes: u64,
    pub wal_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionDiagnostics {
    pub collection: String,
    pub current_generation: String,
    pub readable_generations: Vec<String>,
    pub retired_generations: Vec<String>,
    pub active_reader_leases: HashMap<String, usize>,
    pub pending_cleanup_generations: Vec<String>,
    pub compaction_recommended: bool,
    pub wal_bytes: u64,
}
