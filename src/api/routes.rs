use crate::engine::db::Database;
use crate::models::api::{
    BackupCollectionResponse, CollectionDiagnosticsResponse, CollectionStatsResponse,
    CompactCollectionResponse, CreateCollectionRequest, CreateCollectionResponse,
    DeleteVectorResponse, FetchVectorResponse, HealthResponse, InsertVectorsRequest,
    InsertVectorsResponse, RestoreCollectionRequest, RestoreCollectionResponse, SearchRequest,
    SearchResponse,
};
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{
    routing::{get, post},
    Json, Router,
};
use serde_json::json;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<Database>,
    pub api_key: Option<String>,
}

pub fn router(db: Arc<Database>) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/collections", post(create_collection))
        .route("/collections/:name/vectors", post(insert_vectors))
        .route(
            "/collections/:name/vectors/:id",
            get(fetch_vector).delete(delete_vector),
        )
        .route("/collections/:name/search", post(search_vectors))
        .route("/collections/:name/compact", post(compact_collection))
        .route("/collections/:name/stats", get(collection_stats))
        .route(
            "/collections/:name/diagnostics",
            get(collection_diagnostics),
        )
        .route("/admin/collections/:name/backup", post(backup_collection))
        .route("/admin/collections/restore", post(restore_collection))
        .with_state(AppState {
            db,
            api_key: std::env::var("LITHICDB_API_KEY").ok(),
        })
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        ok: true,
        product: "LithicDB",
    })
}

async fn create_collection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateCollectionRequest>,
) -> ApiResult<Json<CreateCollectionResponse>> {
    require_api_key(&state, &headers)?;
    state.db.create_collection(request.clone())?;
    Ok(Json(CreateCollectionResponse {
        name: request.name,
        dimension: request.dimension,
    }))
}

async fn insert_vectors(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
    Json(request): Json<InsertVectorsRequest>,
) -> ApiResult<Json<InsertVectorsResponse>> {
    require_api_key(&state, &headers)?;
    let inserted = state.db.insert_vectors(&name, request.records)?;
    Ok(Json(InsertVectorsResponse { inserted }))
}

async fn delete_vector(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((name, id)): Path<(String, String)>,
) -> ApiResult<Json<DeleteVectorResponse>> {
    require_api_key(&state, &headers)?;
    let deleted = state.db.delete_vector(&name, &id)?;
    Ok(Json(DeleteVectorResponse { deleted }))
}

async fn fetch_vector(
    State(state): State<AppState>,
    Path((name, id)): Path<(String, String)>,
) -> ApiResult<Json<FetchVectorResponse>> {
    let Some((vector, metadata)) = state.db.fetch_vector(&name, &id)? else {
        return Err(ApiError::not_found("vector not found"));
    };
    Ok(Json(FetchVectorResponse {
        id,
        vector,
        metadata,
    }))
}

async fn search_vectors(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(request): Json<SearchRequest>,
) -> ApiResult<Json<SearchResponse>> {
    let result = state.db.search(&name, request)?;
    Ok(Json(SearchResponse {
        hits: result.hits,
        stats: result.stats,
    }))
}

async fn compact_collection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<Json<CompactCollectionResponse>> {
    require_api_key(&state, &headers)?;
    let stats = state.db.compact_collection(&name)?;
    Ok(Json(CompactCollectionResponse {
        compacted: true,
        stats,
    }))
}

async fn collection_stats(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Json<CollectionStatsResponse>> {
    let stats = state.db.collection_stats(&name)?;
    Ok(Json(CollectionStatsResponse { stats }))
}

async fn collection_diagnostics(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Json<CollectionDiagnosticsResponse>> {
    let diagnostics = state.db.collection_diagnostics(&name)?;
    Ok(Json(CollectionDiagnosticsResponse { diagnostics }))
}

async fn backup_collection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> ApiResult<Json<BackupCollectionResponse>> {
    require_api_key(&state, &headers)?;
    let backup_name = state.db.backup_collection(&name)?;
    Ok(Json(BackupCollectionResponse { backup_name }))
}

async fn restore_collection(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<RestoreCollectionRequest>,
) -> ApiResult<Json<RestoreCollectionResponse>> {
    require_api_key(&state, &headers)?;
    state
        .db
        .restore_collection(&request.backup_name, &request.target_name)?;
    Ok(Json(RestoreCollectionResponse {
        restored: true,
        collection: request.target_name,
    }))
}

pub type ApiResult<T> = std::result::Result<T, ApiError>;

pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(value: anyhow::Error) -> Self {
        let message = value.to_string();
        if message.contains("not found") {
            Self::not_found(message)
        } else {
            Self::bad_request(message)
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({
                "error": self.message,
            })),
        )
            .into_response()
    }
}

fn require_api_key(state: &AppState, headers: &HeaderMap) -> ApiResult<()> {
    let Some(expected) = &state.api_key else {
        return Ok(());
    };
    let supplied = headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if supplied == expected {
        Ok(())
    } else {
        Err(ApiError::unauthorized("missing or invalid api key"))
    }
}
