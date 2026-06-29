use {
    datastar_axum::{
        DatastarEvent, DispatchCustomEventOptions, ElementPatchMode, ExecuteScript, Namespace,
        PatchElements, PatchSignals, console_log, dispatch_custom_event_with_options, redirect,
        remove_element_by_id,
    },
    serde::Serialize,
    std::time::Duration,
};

#[cfg(feature = "compression")]
use {
    datastar_axum::{Compression, CompressionAlgorithm, CompressionStrategy},
    http::header::{CONNECTION, CONTENT_ENCODING, CONTENT_TYPE},
    tokio::io::AsyncReadExt,
};

#[test]
fn patch_elements_supports_go_protocol_fields() {
    let event: DatastarEvent = PatchElements::new("<svg id=\"icon\"></svg>")
        .selector("#icon")
        .namespace(Namespace::Svg)
        .use_view_transition(true)
        .view_transition_selector("#icon")
        .event_id("e1")
        .retry(Duration::from_millis(2500))
        .into();

    assert_eq!(
        event.to_sse_string(),
        concat!(
            "event: datastar-patch-elements\n",
            "id: e1\n",
            "retry: 2500\n",
            "data: selector #icon\n",
            "data: namespace svg\n",
            "data: useViewTransition true\n",
            "data: viewTransitionSelector #icon\n",
            "data: elements <svg id=\"icon\"></svg>\n",
            "\n",
        )
    );
}

#[test]
fn remove_element_uses_remove_mode_without_elements() {
    let event: DatastarEvent = remove_element_by_id("toast").into();

    assert_eq!(
        event.to_sse_string(),
        concat!(
            "event: datastar-patch-elements\n",
            "data: selector #toast\n",
            "data: mode remove\n",
            "\n",
        )
    );
}

#[test]
fn empty_patch_elements_omits_elements_dataline() {
    let event: DatastarEvent = PatchElements::new("").into();

    assert_eq!(
        event.to_sse_string(),
        concat!("event: datastar-patch-elements\n", "\n",)
    );
}

#[test]
fn zero_retry_is_suppressed() {
    let event = DatastarEvent::new(
        datastar_axum::EventType::PatchElements,
        vec!["elements <div></div>".to_owned()],
    )
    .retry(Duration::ZERO);

    assert_eq!(
        event.to_sse_string(),
        concat!(
            "event: datastar-patch-elements\n",
            "data: elements <div></div>\n",
            "\n",
        )
    );
}

#[test]
fn positive_non_default_retry_is_emitted() {
    let event = DatastarEvent::new(
        datastar_axum::EventType::PatchElements,
        vec!["elements <div></div>".to_owned()],
    )
    .retry(Duration::from_millis(2500));

    assert!(event.to_sse_string().contains("retry: 2500\n"));
}

#[test]
fn patch_signals_can_serialize_json() {
    #[derive(Serialize)]
    struct Store {
        message: &'static str,
        count: u8,
    }

    let event: DatastarEvent = PatchSignals::json(&Store {
        message: "ok",
        count: 2,
    })
    .unwrap()
    .only_if_missing(true)
    .into();

    assert_eq!(
        event.to_sse_string(),
        concat!(
            "event: datastar-patch-signals\n",
            "data: onlyIfMissing true\n",
            "data: signals {\"message\":\"ok\",\"count\":2}\n",
            "\n",
        )
    );
}

#[test]
fn dispatch_custom_event_options_are_configurable() {
    #[derive(Serialize)]
    struct Detail {
        saved: bool,
    }

    let event: DatastarEvent = dispatch_custom_event_with_options(
        "saved",
        &Detail { saved: true },
        DispatchCustomEventOptions::default()
            .selector("#target")
            .bubbles(false)
            .cancelable(false)
            .composed(false),
    )
    .unwrap()
    .into();
    let body = event.to_sse_string();

    assert!(body.contains(r##"document.querySelectorAll("#target")"##));
    assert!(body.contains("bubbles: false"));
    assert!(body.contains("cancelable: false"));
    assert!(body.contains("composed: false"));
}

#[test]
fn execute_script_and_helpers_emit_patch_elements() {
    let event: DatastarEvent = ExecuteScript::new("console.log('x')")
        .raw_attribute(r#"type="module""#)
        .into();

    assert_eq!(
        event.to_sse_string(),
        concat!(
            "event: datastar-patch-elements\n",
            "data: selector body\n",
            "data: mode append\n",
            "data: elements <script type=\"module\" data-effect=\"el.remove()\">console.log('x')</script>\n",
            "\n",
        )
    );

    let event: DatastarEvent = console_log("saved").into();
    assert!(event.to_sse_string().contains("console.log(\"saved\")"));

    let event: DatastarEvent = redirect("/next").into();
    assert!(
        event
            .to_sse_string()
            .contains("window.location.href = \"/next\"")
    );
}

#[test]
fn all_patch_modes_have_expected_wire_values() {
    let modes = [
        (ElementPatchMode::Outer, "outer"),
        (ElementPatchMode::Inner, "inner"),
        (ElementPatchMode::Remove, "remove"),
        (ElementPatchMode::Replace, "replace"),
        (ElementPatchMode::Prepend, "prepend"),
        (ElementPatchMode::Append, "append"),
        (ElementPatchMode::Before, "before"),
        (ElementPatchMode::After, "after"),
    ];

    for (mode, expected) in modes {
        assert_eq!(mode.as_str(), expected);
    }
}

#[tokio::test]
#[cfg(feature = "compression")]
async fn axum_response_sets_sse_and_compression_headers() {
    let sse = datastar_axum::DatastarSse::builder()
        .compression(
            Compression::default()
                .strategy(CompressionStrategy::Forced)
                .algorithms([CompressionAlgorithm::Gzip]),
        )
        .events([PatchElements::new("<div id=\"x\"></div>").into()]);

    let response = axum::response::IntoResponse::into_response(sse);
    assert_eq!(
        response.headers().get(CONTENT_TYPE).unwrap(),
        "text/event-stream"
    );
    assert_eq!(response.headers().get(CONNECTION).unwrap(), "keep-alive");
    assert_eq!(response.headers().get(CONTENT_ENCODING).unwrap(), "gzip");

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let decompressed = decode_body(CompressionAlgorithm::Gzip, body.as_ref()).await;
    assert!(decompressed.contains("event: datastar-patch-elements"));
}

#[tokio::test]
#[cfg(feature = "compression")]
async fn compression_stream_contains_multiple_events() {
    for algorithm in [
        CompressionAlgorithm::Brotli,
        CompressionAlgorithm::Zstd,
        CompressionAlgorithm::Gzip,
        CompressionAlgorithm::Deflate,
    ] {
        let sse = datastar_axum::DatastarSse::builder()
            .compression(
                Compression::default()
                    .strategy(CompressionStrategy::Forced)
                    .algorithms([algorithm]),
            )
            .events([
                PatchElements::new("<div id=\"one\"></div>").into(),
                PatchElements::new("<div id=\"two\"></div>").into(),
            ]);

        let response = axum::response::IntoResponse::into_response(sse);
        assert_eq!(
            response.headers().get(CONTENT_ENCODING).unwrap(),
            algorithm.encoding()
        );

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let decompressed = decode_body(algorithm, body.as_ref()).await;
        assert!(decompressed.contains("data: elements <div id=\"one\"></div>"));
        assert!(decompressed.contains("data: elements <div id=\"two\"></div>"));
    }
}

#[cfg(feature = "compression")]
async fn decode_body(algorithm: CompressionAlgorithm, body: &[u8]) -> String {
    let mut decompressed = String::new();
    match algorithm {
        CompressionAlgorithm::Brotli => {
            let mut decoder = async_compression::tokio::bufread::BrotliDecoder::new(body);
            decoder.read_to_string(&mut decompressed).await.unwrap();
        }
        CompressionAlgorithm::Zstd => {
            let mut decoder = async_compression::tokio::bufread::ZstdDecoder::new(body);
            decoder.read_to_string(&mut decompressed).await.unwrap();
        }
        CompressionAlgorithm::Gzip => {
            let mut decoder = async_compression::tokio::bufread::GzipDecoder::new(body);
            decoder.read_to_string(&mut decompressed).await.unwrap();
        }
        CompressionAlgorithm::Deflate => {
            let mut decoder = async_compression::tokio::bufread::ZlibDecoder::new(body);
            decoder.read_to_string(&mut decompressed).await.unwrap();
        }
    }
    decompressed
}

#[tokio::test]
async fn channel_sender_streams_events() {
    let (mut sender, sse) = datastar_axum::DatastarSse::builder().channel();
    sender
        .patch_elements("<div id=\"x\">x</div>")
        .await
        .unwrap();
    drop(sender);

    let response = axum::response::IntoResponse::into_response(sse);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();

    assert!(body.contains("data: elements <div id=\"x\">x</div>"));
}
