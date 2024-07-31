use actix_web::{web, App, HttpResponse, HttpServer, ResponseError};
use env_logger;
use log::{debug, error, info};
use reqwest::Client;
use rmp_serde::Deserializer;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::time::sleep;

#[derive(Debug, Deserialize)]
struct DDTrace {
    span_id: u64,
    parent_id: u64,
    trace_id: u64,
    name: String,
    start: u64,
    duration: u64,
    service: String,
    resource: String,
    meta: std::collections::HashMap<String, String>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct ZipkinSpan {
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_id: Option<String>,
    trace_id: String,
    name: String,
    timestamp: u64,
    duration: u64,
    local_endpoint: LocalEndpoint,
    tags: std::collections::HashMap<String, String>,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct LocalEndpoint {
    service_name: String,
}

struct AppState {
    spans: Arc<Mutex<Vec<ZipkinSpan>>>,
}

#[derive(Error, Debug)]
enum AppError {
    #[error("Failed to deserialize MessagePack: {0}")]
    DeserializationError(#[from] rmp_serde::decode::Error),
    #[error("Failed to send spans: {0}")]
    SendError(#[from] reqwest::Error),
    #[error("Zipkin API error: Status {0}, Body: {1}")]
    ZipkinApiError(u16, String),
}

impl ResponseError for AppError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::InternalServerError().json(format!("Internal Server Error: {}", self))
    }
}

fn dd2zipkin(dd_trace: &DDTrace) -> ZipkinSpan {
    let mut tags = dd_trace.meta.clone();
    tags.insert("resource".to_string(), dd_trace.resource.clone());

    ZipkinSpan {
        id: format!("{:016x}", dd_trace.span_id),
        parent_id: if dd_trace.parent_id != 0 {
            Some(format!("{:016x}", dd_trace.parent_id))
        } else {
            None
        },
        trace_id: format!("{:016x}", dd_trace.trace_id),
        name: dd_trace.name.clone(),
        timestamp: dd_trace.start / 1000,
        duration: dd_trace.duration / 1000,
        local_endpoint: LocalEndpoint {
            service_name: dd_trace.service.clone(),
        },
        tags,
    }
}

async fn send_spans(client: &Client, spans: Arc<Mutex<Vec<ZipkinSpan>>>) {
    loop {
        let to_send: Vec<ZipkinSpan> = {
            let mut spans = spans.lock().await;
            let count = std::cmp::min(10, spans.len());
            spans.drain(..count).collect()
        };

        if !to_send.is_empty() {
            debug!("Attempting to send spans: {:#?}", &to_send);
            match send_batch(&client, &to_send).await {
                Ok(_) => {
                    info!("Successfully sent {} spans", to_send.len());
                }
                Err(e) => {
                    error!("Failed to send spans: {}", e);
                    // Return unsent spans to the queue
                    let mut spans = spans.lock().await;
                    spans.extend(to_send);
                }
            }
            debug!("waiting a bit before next batch");
            sleep(Duration::from_millis(100)).await;
        } else {
            debug!("No spans to send, waiting for next batch");
            sleep(Duration::from_secs(1)).await;
        }
    }
}

async fn send_batch(client: &Client, spans: &[ZipkinSpan]) -> Result<(), AppError> {
    let response = client
        .post("http://127.0.0.1:9411/api/v2/spans")
        .json(spans)
        .send()
        .await?;
    let status = response.status();
    if status.is_success() {
        Ok(())
    } else {
        let body = response.text().await?;
        error!("Zipkin API error: Status {}, Body: {}", status, body);
        Err(AppError::ZipkinApiError(status.as_u16(), body))
    }
}

async fn handle_traces(
    payload: web::Bytes,
    data: web::Data<AppState>,
) -> Result<HttpResponse, AppError> {
    debug!("Processing incoming traces");
    let mut deserializer = Deserializer::new(&payload[..]);
    let trace_groups: Vec<Vec<DDTrace>> = Vec::deserialize(&mut deserializer)?;

    let trace_groups_count = trace_groups.len();
    let zipkin_spans: Vec<ZipkinSpan> = trace_groups
        .into_iter()
        .flat_map(|group| group.into_iter().map(|trace| dd2zipkin(&trace)))
        .collect();

    let mut spans = data.spans.lock().await;
    spans.extend(zipkin_spans);

    info!(
        "Processed and converted {} trace groups",
        trace_groups_count
    );
    Ok(HttpResponse::Ok().finish())
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    let app_state = web::Data::new(AppState {
        spans: Arc::new(Mutex::new(Vec::new())),
    });

    let client = Client::new();
    let spans_clone = app_state.spans.clone();

    tokio::spawn(async move {
        send_spans(&client, spans_clone).await;
    });

    info!("Starting server on 127.0.0.1:8126");
    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .route("/v0.4/traces", web::post().to(handle_traces))
            .default_service(web::to(|| async {
                Ok::<_, AppError>(HttpResponse::Ok().finish())
            }))
    })
    .bind("127.0.0.1:8126")?
    .run()
    .await
}
