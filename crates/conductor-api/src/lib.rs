use std::io;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use axum::Router;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;

/// Build the HTTP application served by the combined agent.
pub fn router() -> Router {
    Router::new().route("/healthz", get(healthz))
}

async fn healthz() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        r#"{"status":"ok"}"#,
    )
}

/// Serve the HTTP application until the process requests a bounded graceful shutdown.
pub async fn serve(listener: tokio::net::TcpListener, shutdown: Arc<AtomicBool>) -> io::Result<()> {
    let server = axum::serve(listener, router()).with_graceful_shutdown(async move {
        while !shutdown.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
    match tokio::time::timeout(Duration::from_secs(5), server).await {
        Ok(result) => result,
        Err(_) => {
            eprintln!("{{\"event\":\"shutdown_deadline_elapsed\",\"timeout_seconds\":5}}");
            Ok(())
        }
    }
}
