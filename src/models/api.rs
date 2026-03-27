use super::types::{
    CollectionDiagnostics, CollectionStats, Metadata, MetadataFilter, SearchHit, SearchStats,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
pub struct CreateCollectionRequest {
    pub name: String,
    pub dimension: usize,
    pub max_cluster_size: Option<usize>,
    pub graph_degree: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct CreateCollectionResponse {
    pub name: String,
    pub dimension: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorRecord {
    pub id: String,
    pub vector: Vec<f32>,
    #[serde(default)]
    pub metadata: Metadata,
}

#[derive(Debug, Deserialize)]
pub struct InsertVectorsRequest {
    pub records: Vec<VectorRecord>,
}

#[derive(Debug, Serialize)]
pub struct InsertVectorsResponse {
    pub inserted: usize,
}

#[derive(Debug, Deserialize)]
pub struct SearchRequest {
    pub vector: Vec<f32>,
    pub k: usize,
    #[serde(default)]
    pub filter: MetadataFilter,
    pub ef_search: Option<usize>,
    pub probe_clusters: Option<usize>,
    pub entry_points: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct SearchResponse {
    pub hits: Vec<SearchHit>,
    pub stats: SearchStats,
}

#[derive(Debug, Serialize)]
pub struct FetchVectorResponse {
    pub id: String,
    pub vector: Vec<f32>,
    pub metadata: Metadata,
}

#[derive(Debug, Serialize)]
pub struct DeleteVectorResponse {
    pub deleted: bool,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub ok: bool,
    pub product: &'static str,
}

#[derive(Debug, Serialize)]
pub struct CompactCollectionResponse {
    pub compacted: bool,
    pub stats: CollectionStats,
}

#[derive(Debug, Serialize)]
pub struct CollectionStatsResponse {
    pub stats: CollectionStats,
}

#[derive(Debug, Serialize)]
pub struct CollectionDiagnosticsResponse {
    pub diagnostics: CollectionDiagnostics,
}

#[derive(Debug, Deserialize)]
pub struct RestoreCollectionRequest {
    pub backup_name: String,
    pub target_name: String,
}

#[derive(Debug, Serialize)]
pub struct BackupCollectionResponse {
    pub backup_name: String,
}

#[derive(Debug, Serialize)]
pub struct RestoreCollectionResponse {
    pub restored: bool,
    pub collection: String,
}
