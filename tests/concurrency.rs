use datastar_axum::{DatastarEvent, EventType, PatchElements, SseError};

#[tokio::test]
async fn cloned_datastar_senders_stream_each_successful_event_once() {
    let (first, sse) = datastar_axum::DatastarSse::builder()
        .channel_capacity(2)
        .channel();
    let second = first.clone();

    let first_send = tokio::spawn(async move {
        first
            .send(DatastarEvent::new(
                EventType::PatchElements,
                vec!["elements <div id=\"one\"></div>".to_owned()],
            ))
            .await
    });
    let second_send = tokio::spawn(async move {
        second
            .send(DatastarEvent::new(
                EventType::PatchElements,
                vec!["elements <div id=\"two\"></div>".to_owned()],
            ))
            .await
    });

    first_send.await.unwrap().unwrap();
    second_send.await.unwrap().unwrap();

    let response = axum::response::IntoResponse::into_response(sse);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();

    assert_eq!(body.matches("event: datastar-patch-elements").count(), 2);
    assert!(body.contains("data: elements <div id=\"one\"></div>"));
    assert!(body.contains("data: elements <div id=\"two\"></div>"));
}

#[tokio::test]
async fn datastar_sender_returns_closed_after_receiver_is_dropped() {
    let (sender, sse) = datastar_axum::DatastarSse::builder().channel();
    drop(sse);

    let err = sender
        .send(PatchElements::new("<div></div>"))
        .await
        .unwrap_err();

    assert!(matches!(err, SseError::Closed));
}

#[tokio::test]
async fn zero_channel_capacity_is_clamped_to_one() {
    let (sender, sse) = datastar_axum::DatastarSse::builder()
        .channel_capacity(0)
        .channel();

    sender
        .send(PatchElements::new("<div id=\"one\"></div>"))
        .await
        .unwrap();
    drop(sender);

    let response = axum::response::IntoResponse::into_response(sse);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();

    assert!(body.contains("data: elements <div id=\"one\"></div>"));
}
