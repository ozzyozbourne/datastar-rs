use {
    axum::{
        body::Body,
        extract::{FromRequest, OptionalFromRequest},
        http::{Request, StatusCode},
    },
    datastar_axum::ReadSignals,
    serde::Deserialize,
};

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
struct Signals {
    message: String,
    count: u8,
}

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
struct RequiredSignals {
    count: u8,
}

#[tokio::test]
async fn get_without_datastar_query_defaults() {
    let request = Request::builder().uri("/test").body(Body::empty()).unwrap();

    let ReadSignals(signals) =
        <ReadSignals<Signals> as FromRequest<()>>::from_request(request, &())
            .await
            .unwrap();

    assert_eq!(signals, Signals::default());
}

#[tokio::test]
async fn get_with_empty_datastar_query_defaults() {
    let request = Request::builder()
        .uri("/test?datastar=")
        .body(Body::empty())
        .unwrap();

    let ReadSignals(signals) =
        <ReadSignals<Signals> as FromRequest<()>>::from_request(request, &())
            .await
            .unwrap();

    assert_eq!(signals, Signals::default());
}

#[tokio::test]
async fn get_with_datastar_query_parses_signals() {
    let request = Request::builder()
        .uri("/test?datastar=%7B%22message%22%3A%22ok%22%2C%22count%22%3A2%7D")
        .body(Body::empty())
        .unwrap();

    let ReadSignals(signals) =
        <ReadSignals<Signals> as FromRequest<()>>::from_request(request, &())
            .await
            .unwrap();

    assert_eq!(
        signals,
        Signals {
            message: "ok".to_owned(),
            count: 2,
        }
    );
}

#[tokio::test]
async fn mandatory_get_accepts_signals_with_default_type() {
    let request = Request::builder()
        .uri("/test?datastar=%7B%22count%22%3A2%7D")
        .body(Body::empty())
        .unwrap();

    let ReadSignals(signals) =
        <ReadSignals<RequiredSignals> as FromRequest<()>>::from_request(request, &())
            .await
            .unwrap();

    assert_eq!(signals, RequiredSignals { count: 2 });
}

#[tokio::test]
async fn malformed_datastar_query_rejects() {
    let request = Request::builder()
        .uri("/test?datastar=%7B")
        .body(Body::empty())
        .unwrap();

    let response = <ReadSignals<Signals> as FromRequest<()>>::from_request(request, &())
        .await
        .unwrap_err();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn malformed_post_body_rejects() {
    let request = Request::builder()
        .method("POST")
        .uri("/test")
        .body(Body::from("{"))
        .unwrap();

    let response = <ReadSignals<Signals> as FromRequest<()>>::from_request(request, &())
        .await
        .unwrap_err();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn optional_without_datastar_request_header_is_none() {
    let request = Request::builder()
        .uri("/test?datastar=%7B%22message%22%3A%22ok%22%2C%22count%22%3A2%7D")
        .body(Body::empty())
        .unwrap();

    let signals = <ReadSignals<Signals> as OptionalFromRequest<()>>::from_request(request, &())
        .await
        .unwrap();

    assert!(signals.is_none());
}

#[tokio::test]
async fn optional_with_datastar_request_header_parses() {
    let request = Request::builder()
        .uri("/test?datastar=%7B%22message%22%3A%22ok%22%2C%22count%22%3A2%7D")
        .header("datastar-request", "true")
        .body(Body::empty())
        .unwrap();

    let signals = <ReadSignals<Signals> as OptionalFromRequest<()>>::from_request(request, &())
        .await
        .unwrap();

    assert_eq!(signals.unwrap().0.message, "ok");
}

#[tokio::test]
async fn optional_with_datastar_request_header_and_missing_query_defaults() {
    let request = Request::builder()
        .uri("/test")
        .header("datastar-request", "true")
        .body(Body::empty())
        .unwrap();

    let signals = <ReadSignals<Signals> as OptionalFromRequest<()>>::from_request(request, &())
        .await
        .unwrap();

    assert_eq!(signals.unwrap().0, Signals::default());
}

#[tokio::test]
async fn optional_with_datastar_request_header_accepts_signals_with_default_type() {
    let request = Request::builder()
        .uri("/test?datastar=%7B%22count%22%3A2%7D")
        .header("datastar-request", "true")
        .body(Body::empty())
        .unwrap();

    let signals =
        <ReadSignals<RequiredSignals> as OptionalFromRequest<()>>::from_request(request, &())
            .await
            .unwrap();

    assert_eq!(signals.unwrap().0, RequiredSignals { count: 2 });
}
