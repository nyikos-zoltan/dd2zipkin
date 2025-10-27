use actix_web::dev::ServiceResponse;
use actix_web::middleware::{ErrorHandlerResponse, Logger};
use actix_web::web::PayloadConfig;
use actix_web::{web, App, HttpResponse, HttpServer, ResponseError};
use env_logger;
use log::{debug, error, info};
use reqwest::Client;
use rmp_serde::Deserializer;
use serde::{Deserialize, Serialize};
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio::time::sleep;

// Avoid musl's default allocator due to lackluster performance
// https://nickb.dev/blog/default-musl-allocator-considered-harmful-to-performance
#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

lazy_static::lazy_static!(
    static ref SYNC_HOST: String = format!(
    "http://{}/v1/traces", std::env::var("ZIPKIN_COLLECTOR_HOST_PORT").unwrap_or_else(|_| "127.0.0.1:4318".to_owned()));
    static ref BIND_TO: String = std::env::var("BIND_TO").unwrap_or_else(|_| "127.0.0.1:8126".to_owned());
);

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
    error: u64,
}

#[derive(Debug, Serialize, Clone)]
struct OTelSpan {
    trace_id: String,
    span_id: String,
    parent_span_id: Option<String>,
    name: String,
    kind: i32, // 1 for INTERNAL
    start_time_unix_nano: u64,
    end_time_unix_nano: u64,
    attributes: Vec<OTelKeyValue>,
}

#[derive(Debug, Serialize, Clone)]
struct OTelKeyValue {
    key: String,
    value: OTelAnyValue,
}

#[derive(Debug, Serialize, Clone)]
#[serde(untagged)]
enum OTelAnyValue {
    String { string_value: String },
    Bool { bool_value: bool },
}
impl OTelAnyValue {
    fn string_value(&self) -> Option<&String> {
        if let OTelAnyValue::String { string_value } = self {
            Some(string_value)
        } else {
            None
        }
    }
}

#[derive(Debug, Serialize)]
struct OTelRequest {
    resource_spans: Vec<OTelResourceSpans>,
}

#[derive(Debug, Serialize)]
struct OTelResourceSpans {
    resource: OTelResource,
    scope_spans: Vec<OTelScopeSpans>,
}

#[derive(Debug, Serialize)]
struct OTelResource {
    attributes: Vec<OTelKeyValue>,
}

#[derive(Debug, Serialize)]
struct OTelScopeSpans {
    scope: OTelInstrumentationScope,
    spans: Vec<OTelSpan>,
}

#[derive(Debug, Serialize)]
struct OTelInstrumentationScope {
    name: String,
    version: String,
}

struct AppState {
    spans: Arc<Mutex<Vec<OTelSpan>>>,
}

#[derive(Error, Debug)]
enum AppError {
    #[error("Failed to deserialize MessagePack: {0}")]
    DeserializationError(#[from] rmp_serde::decode::Error),
    #[error("Failed to send spans: {0}")]
    SendError(#[from] reqwest::Error),
    #[error("OpenTelemetry API error: Status {0}, Body: {1}")]
    OTelApiError(u16, String),
}

impl ResponseError for AppError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::InternalServerError().json(format!("Internal Server Error: {}", self))
    }
}

fn dd2otel(dd_trace: &DDTrace) -> OTelSpan {
    let mut attributes = Vec::new();
    for (key, value) in &dd_trace.meta {
        attributes.push(OTelKeyValue {
            key: key.clone(),
            value: OTelAnyValue::String {
                string_value: value.clone(),
            },
        });
    }
    attributes.push(OTelKeyValue {
        key: "resource".to_string(),
        value: OTelAnyValue::String {
            string_value: dd_trace.resource.clone(),
        },
    });
    attributes.push(OTelKeyValue {
        key: "service.name".to_string(),
        value: OTelAnyValue::String {
            string_value: dd_trace.service.clone(),
        },
    });
    attributes.push(OTelKeyValue {
        key: "error".to_string(),
        value: OTelAnyValue::Bool {
            bool_value: dd_trace.error == 1,
        },
    });

    let conv = OTelSpan {
        trace_id: format!("{:032x}", dd_trace.trace_id),
        span_id: format!("{:016x}", dd_trace.span_id),
        parent_span_id: if dd_trace.parent_id != 0 {
            Some(format!("{:016x}", dd_trace.parent_id))
        } else {
            None
        },
        name: dd_trace.name.clone(),
        kind: 1, // INTERNAL
        start_time_unix_nano: (dd_trace.start),
        end_time_unix_nano: (dd_trace.start + dd_trace.duration),
        attributes,
    };
    debug!("{:#?} -> {:#?}", dd_trace, &conv);
    conv
}

#[test]
fn test_dd2otel() {
    let otel = dd2otel(&DDTrace {
        span_id: 1111u64,
        parent_id: 1110u64,
        trace_id: 1112u64,
        name: "name".to_owned(),
        start: 1,
        duration: 2,
        service: "svc".to_owned(),
        resource: "res".to_owned(),
        meta: Default::default(),
        error: 0,
    });

    let res = serde_json::ser::to_string(&otel).unwrap();
    let snap = std::fs::read_to_string("snapshots/dd2otel.json").unwrap();
    assert_eq!(res.trim(), snap.trim());
}

async fn send_spans(client: &Client, spans: Arc<Mutex<Vec<OTelSpan>>>) {
    loop {
        let to_send: Vec<OTelSpan> = {
            let mut spans = spans.lock().await;
            let count = std::cmp::min(10, spans.len());
            spans.drain(..count).collect()
        };

        if !to_send.is_empty() {
            // debug!("Attempting to send spans: {:#?}", &to_send);
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

async fn send_batch(client: &Client, spans: &[OTelSpan]) -> Result<(), AppError> {
    let service_name = spans
        .first()
        .and_then(|span| {
            span.attributes
                .iter()
                .find(|attr| attr.key == "service.name")
        })
        .and_then(|attr| attr.value.string_value().map(Clone::clone))
        .unwrap_or_else(|| "unknown-service".to_string());

    let otel_request = OTelRequest {
        resource_spans: vec![OTelResourceSpans {
            resource: OTelResource {
                attributes: vec![OTelKeyValue {
                    key: "service.name".to_string(),
                    value: OTelAnyValue::String {
                        string_value: service_name,
                    },
                }],
            },
            scope_spans: vec![OTelScopeSpans {
                scope: OTelInstrumentationScope {
                    name: "dd-to-otel-converter".to_string(),
                    version: "0.1.0".to_string(),
                },
                spans: spans.to_vec(),
            }],
        }],
    };

    let response = client
        .post(SYNC_HOST.deref())
        .json(&otel_request)
        .send()
        .await?;
    let status = response.status();
    if status.is_success() {
        Ok(())
    } else {
        let body = response.text().await?;
        error!("OpenTelemetry API error: Status {}, Body: {}", status, body);
        Err(AppError::OTelApiError(status.as_u16(), body))
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
    let otel_spans: Vec<OTelSpan> = trace_groups
        .into_iter()
        .flat_map(|group| group.into_iter().map(|trace| dd2otel(&trace)))
        .collect();

    let mut spans = data.spans.lock().await;
    spans.extend(otel_spans);

    info!(
        "Processed and converted {} trace groups",
        trace_groups_count
    );
    Ok(HttpResponse::Ok().finish())
}
fn add_error_header<B>(res: ServiceResponse<B>) -> actix_web::Result<ErrorHandlerResponse<B>> {
    Ok(ErrorHandlerResponse::Response(res.map_into_left_body()))
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

    info!("Starting server on {} sending to {}", *BIND_TO, *SYNC_HOST);
    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .wrap(actix_web::middleware::ErrorHandlers::new().default_handler(add_error_header))
            .app_data(PayloadConfig::default().limit(10_000_000))
            .app_data(app_state.clone())
            .route("/v0.4/traces", web::post().to(handle_traces))
            .default_service(web::to(|| async {
                debug!("???");
                Ok::<_, AppError>(HttpResponse::Ok().finish())
            }))
    })
    .bind(BIND_TO.deref())?
    .run()
    .await
}
