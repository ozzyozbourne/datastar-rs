use {
    datastar_axum::{
        DatastarEvent, DispatchCustomEventOptions, ElementPatchMode, ExecuteScript, Namespace,
        PatchElements, PatchSignals, SseError, console_log, dispatch_custom_event_with_options,
        redirect, remove_element_by_id,
    },
    proptest::prelude::*,
    serde::Serialize,
    std::{error::Error, fmt, time::Duration},
};

#[cfg(feature = "compression")]
use {
    axum::http::header::{CACHE_CONTROL, CONNECTION, CONTENT_ENCODING, CONTENT_TYPE},
    datastar_axum::{Compression, CompressionAlgorithm, CompressionStrategy},
    std::{
        io::Write,
        sync::{Arc, Mutex},
    },
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
fn empty_patch_signals_omits_signals_dataline() {
    let event: DatastarEvent = PatchSignals::new("").into();

    assert_eq!(
        event.to_sse_string(),
        concat!("event: datastar-patch-signals\n", "\n",)
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
    assert_eq!(response.headers().get(CACHE_CONTROL).unwrap(), "no-cache");
    assert!(response.headers().get(CONNECTION).is_none());
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

#[tokio::test]
#[cfg(feature = "compression")]
async fn compressed_source_stream_errors_are_reported_as_source_errors() {
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_writer(logs.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let stream = futures_util::stream::iter([Err::<DatastarEvent, _>(TestSourceError)]);
    let sse = datastar_axum::DatastarSse::builder()
        .compression(
            Compression::default()
                .strategy(CompressionStrategy::Forced)
                .algorithms([CompressionAlgorithm::Gzip]),
        )
        .stream(stream);
    let response = axum::response::IntoResponse::into_response(sse);

    let err = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("SSE source stream failed"));

    let logs = logs.as_string();
    assert!(logs.contains("Datastar SSE source stream failed"));
    assert!(!logs.contains("Datastar SSE compression stream failed"));
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
    let (sender, sse) = datastar_axum::DatastarSse::builder().channel();
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

#[tokio::test]
async fn source_stream_errors_are_reported_as_source_errors() {
    let stream = futures_util::stream::iter([Err::<DatastarEvent, _>(TestSourceError)]);
    let response =
        axum::response::IntoResponse::into_response(datastar_axum::DatastarSse::new(stream));

    let err = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap_err();

    assert!(err.to_string().contains("SSE source stream failed"));
}

#[test]
fn source_error_display_is_distinct_from_compression_error() {
    let err = SseError::Source(std::io::Error::other("database failed"));

    assert_eq!(err.to_string(), "SSE source stream failed: database failed");
}

proptest! {
    #[test]
    fn patch_signals_serializes_go_sdk_dataline_shape(
        lines in payload_lines(),
        only_if_missing in any::<bool>(),
    ) {
        let signals = lines.join("\n");
        let event = PatchSignals::new(signals.as_str()).only_if_missing(only_if_missing);
        let body = DatastarEvent::from(event).to_sse_string();
        let data = data_lines(&body);

        let mut expected = Vec::new();
        if only_if_missing {
            expected.push("onlyIfMissing true".to_owned());
        }
        expected.extend(payload_datalines("signals", &signals));

        prop_assert_eq!(data, expected);
        prop_assert!(body.starts_with("event: datastar-patch-signals\n"));
        prop_assert!(body.ends_with("\n\n"));
    }

    #[test]
    fn patch_elements_serializes_go_sdk_dataline_shape(
        lines in payload_lines(),
        selector in prop::option::of(non_empty_payload_line()),
        mode in element_patch_mode(),
        namespace in namespace(),
        use_view_transition in any::<bool>(),
        view_transition_selector in prop::option::of(non_empty_payload_line()),
    ) {
        let elements = lines.join("\n");
        let mut event = PatchElements::new(elements.as_str())
            .mode(mode)
            .namespace(namespace)
            .use_view_transition(use_view_transition);

        if let Some(selector) = &selector {
            event = event.selector(selector);
        }
        if let Some(view_transition_selector) = &view_transition_selector {
            event = event.view_transition_selector(view_transition_selector);
        }

        let body = DatastarEvent::from(event).to_sse_string();
        let data = data_lines(&body);

        let mut expected = Vec::new();
        if let Some(selector) = selector {
            expected.push(format!("selector {selector}"));
        }
        if mode != ElementPatchMode::Outer {
            expected.push(format!("mode {}", mode.as_str()));
        }
        if namespace != Namespace::Html {
            expected.push(format!("namespace {}", namespace.as_str()));
        }
        if use_view_transition {
            expected.push("useViewTransition true".to_owned());
            if let Some(view_transition_selector) = view_transition_selector {
                expected.push(format!("viewTransitionSelector {view_transition_selector}"));
            }
        }
        expected.extend(payload_datalines("elements", &elements));

        prop_assert_eq!(data, expected);
        prop_assert!(body.starts_with("event: datastar-patch-elements\n"));
        prop_assert!(body.ends_with("\n\n"));
    }

    #[test]
    fn datastar_event_retry_matches_sse_and_go_sdk_defaults(
        retry_ms in 0_u64..5_000,
    ) {
        let event = DatastarEvent::new(
            datastar_axum::EventType::PatchElements,
            vec!["elements <div></div>".to_owned()],
        )
        .retry(Duration::from_millis(retry_ms));
        let body = event.to_sse_string();

        if retry_ms == 0 || retry_ms == 1_000 {
            prop_assert!(!body.contains("\nretry: "));
        } else {
            let expected = format!("\nretry: {retry_ms}\n");
            prop_assert!(body.contains(&expected));
        }
    }
}

#[derive(Debug)]
struct TestSourceError;

impl fmt::Display for TestSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("source failed")
    }
}

impl Error for TestSourceError {}

#[derive(Clone, Default)]
#[cfg(feature = "compression")]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

#[cfg(feature = "compression")]
impl CapturedLogs {
    fn as_string(&self) -> String {
        let bytes = self.0.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }
}

#[cfg(feature = "compression")]
struct CapturedLogWriter(Arc<Mutex<Vec<u8>>>);

#[cfg(feature = "compression")]
impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for CapturedLogs {
    type Writer = CapturedLogWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        CapturedLogWriter(Arc::clone(&self.0))
    }
}

#[cfg(feature = "compression")]
impl Write for CapturedLogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn payload_lines() -> impl Strategy<Value = Vec<String>> {
    prop_oneof![
        Just(Vec::new()),
        prop::collection::vec(payload_line(), 1..8),
    ]
}

fn payload_line() -> impl Strategy<Value = String> {
    "[ -~]{0,32}"
}

fn non_empty_payload_line() -> impl Strategy<Value = String> {
    "[ -~]{1,32}"
}

fn element_patch_mode() -> impl Strategy<Value = ElementPatchMode> {
    prop_oneof![
        Just(ElementPatchMode::Outer),
        Just(ElementPatchMode::Inner),
        Just(ElementPatchMode::Remove),
        Just(ElementPatchMode::Replace),
        Just(ElementPatchMode::Prepend),
        Just(ElementPatchMode::Append),
        Just(ElementPatchMode::Before),
        Just(ElementPatchMode::After),
    ]
}

fn namespace() -> impl Strategy<Value = Namespace> {
    prop_oneof![
        Just(Namespace::Html),
        Just(Namespace::Svg),
        Just(Namespace::Mathml),
    ]
}

fn data_lines(body: &str) -> Vec<String> {
    body.lines()
        .filter_map(|line| line.strip_prefix("data: ").map(ToOwned::to_owned))
        .collect()
}

fn payload_datalines(prefix: &str, payload: &str) -> Vec<String> {
    if payload.is_empty() {
        return Vec::new();
    }

    payload
        .split('\n')
        .map(|line| format!("{prefix} {line}"))
        .collect()
}
