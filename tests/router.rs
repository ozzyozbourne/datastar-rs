use {
    axum::{
        Router,
        body::{Body, to_bytes},
        http::{
            Request, StatusCode,
            header::{CACHE_CONTROL, CONNECTION, CONTENT_TYPE},
        },
        routing::{get, post},
    },
    datastar_axum::{DatastarSse, PatchElements, ReadSignals},
    serde::Deserialize,
    tower::ServiceExt,
};

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
struct Signals {
    message: String,
    count: u8,
}

async fn read_signals(ReadSignals(signals): ReadSignals<Signals>) -> String {
    format!("{}:{}", signals.message, signals.count)
}

async fn optional_signals(signals: Option<ReadSignals<Signals>>) -> String {
    match signals {
        Some(ReadSignals(signals)) => format!("{}:{}", signals.message, signals.count),
        None => "none".to_owned(),
    }
}

async fn datastar_sse() -> DatastarSse {
    DatastarSse::events([PatchElements::new("<div id=\"x\">x</div>").into()])
}

#[tokio::test]
async fn router_get_with_datastar_query_extracts_signals() {
    let app = Router::new().route("/test", get(read_signals));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/test?datastar=%7B%22message%22%3A%22ok%22%2C%22count%22%3A2%7D")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_string(response).await, "ok:2");
}

#[tokio::test]
async fn router_get_without_datastar_query_returns_default_signals() {
    let app = Router::new().route("/test", get(read_signals));
    let response = app
        .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_string(response).await, ":0");
}

#[tokio::test]
async fn router_post_with_json_content_type_extracts_signals() {
    let app = Router::new().route("/test", post(read_signals));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"message":"ok","count":2}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_string(response).await, "ok:2");
}

#[tokio::test]
async fn router_post_without_content_type_extracts_signals() {
    let app = Router::new().route("/test", post(read_signals));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test")
                .body(Body::from(r#"{"message":"ok","count":2}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_string(response).await, "ok:2");
}

#[tokio::test]
async fn router_optional_extractor_is_none_without_datastar_request_header() {
    let app = Router::new().route("/test", get(optional_signals));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/test?datastar=%7B%22message%22%3A%22ok%22%2C%22count%22%3A2%7D")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(body_string(response).await, "none");
}

#[tokio::test]
async fn router_handler_returning_datastar_sse_streams_headers_and_body() {
    let app = Router::new().route("/events", get(datastar_sse));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "text/event-stream"
    );
    assert_eq!(response.headers().get(CACHE_CONTROL).unwrap(), "no-cache");
    assert!(response.headers().get(CONNECTION).is_none());

    let body = body_string(response).await;
    assert!(body.contains("event: datastar-patch-elements"));
    assert!(body.contains("data: elements <div id=\"x\">x</div>"));
}

async fn body_string(response: axum::response::Response) -> String {
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    String::from_utf8(body.to_vec()).unwrap()
}
