use anyhow::Result;
use clap::Parser;
use lithicdb::api::routes::router;
use lithicdb::engine::db::Database;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "./data")]
    data_dir: String,
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: String,
    #[arg(long, default_value_t = 30)]
    maintenance_interval_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let db = Arc::new(Database::open(&args.data_dir)?);
    if args.maintenance_interval_secs > 0 {
        let maintenance_db = Arc::clone(&db);
        let interval = Duration::from_secs(args.maintenance_interval_secs);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                if let Err(err) = maintenance_db.run_maintenance() {
                    tracing::warn!("maintenance run failed: {err}");
                }
            }
        });
    }
    let addr: SocketAddr = args.bind.parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;

    axum::serve(listener, router(db)).await?;
    Ok(())
}
