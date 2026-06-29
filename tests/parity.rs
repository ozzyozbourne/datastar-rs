use {
    datastar_axum::{DatastarEvent, Namespace, PatchElements, PatchSignals},
    std::{
        collections::BTreeMap,
        env, fs, io,
        path::{Path, PathBuf},
        process::{Command, Stdio},
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
};

#[test]
fn representative_events_match_go_sdk_sse_fields() {
    let Some(go_cases) = go_sdk_cases() else {
        eprintln!("skipping Go SDK parity test: go binary or ../datastar-go is unavailable");
        return;
    };

    let rust_cases = BTreeMap::from([
        ("elements_full", rust_elements_full()),
        ("remove_element", rust_remove_element()),
        ("signals_multiline", rust_signals_multiline()),
    ]);

    for (name, rust_body) in rust_cases {
        let go_body = go_cases
            .get(name)
            .unwrap_or_else(|| panic!("Go SDK parity output is missing case {name}"));

        assert_eq!(
            parse_sse(go_body),
            parse_sse(&rust_body),
            "SSE field mismatch for Go SDK parity case {name}"
        );
    }
}

fn rust_elements_full() -> String {
    DatastarEvent::from(
        PatchElements::new("<svg id=\"icon\"></svg>\n<circle></circle>")
            .selector("#icon")
            .namespace(Namespace::Svg)
            .use_view_transition(true)
            .view_transition_selector("#icon")
            .event_id("e1")
            .retry(Duration::from_millis(2500)),
    )
    .to_sse_string()
}

fn rust_remove_element() -> String {
    DatastarEvent::from(PatchElements::remove("#toast")).to_sse_string()
}

fn rust_signals_multiline() -> String {
    DatastarEvent::from(
        PatchSignals::new("{\"message\":\"ok\"}\n{\"count\":2}")
            .only_if_missing(true)
            .event_id("s1")
            .retry(Duration::from_millis(2500)),
    )
    .to_sse_string()
}

fn go_sdk_cases() -> Option<BTreeMap<String, String>> {
    let go = find_go_binary()?;
    let sdk_path = workspace_parent().join("datastar-go");
    if !sdk_path.join("go.mod").is_file() {
        return None;
    }

    let temp_dir = create_temp_dir().ok()?;
    let result = run_go_parity_program(&go, &sdk_path, &temp_dir);
    let _ = fs::remove_dir_all(&temp_dir);

    match result {
        Ok(cases) => Some(cases),
        Err(err) => panic!("failed to run Go SDK parity program: {err}"),
    }
}

fn run_go_parity_program(
    go: &Path,
    sdk_path: &Path,
    temp_dir: &Path,
) -> io::Result<BTreeMap<String, String>> {
    fs::write(
        temp_dir.join("go.mod"),
        format!(
            "module datastar_parity\n\ngo 1.24\n\nrequire github.com/starfederation/datastar-go v0.0.0\nreplace github.com/starfederation/datastar-go => {}\n",
            sdk_path.display()
        ),
    )?;
    fs::write(temp_dir.join("main.go"), GO_PARITY_PROGRAM)?;

    let output = Command::new(go)
        .arg("run")
        .arg(".")
        .current_dir(temp_dir)
        .env("GOFLAGS", "-mod=mod")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()?;

    if !output.status.success() {
        return Err(io::Error::other(format!(
            "go run failed with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    parse_go_output(&String::from_utf8_lossy(&output.stdout))
}

fn parse_go_output(output: &str) -> io::Result<BTreeMap<String, String>> {
    serde_json::from_str(output).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}

fn parse_sse(body: &str) -> Vec<Vec<(String, String)>> {
    let mut events = Vec::new();
    let mut event = Vec::new();

    for line in body.lines() {
        if line.is_empty() {
            if !event.is_empty() {
                events.push(std::mem::take(&mut event));
            }
            continue;
        }

        if let Some((field, value)) = line.split_once(": ") {
            event.push((field.to_owned(), value.to_owned()));
        }
    }

    if !event.is_empty() {
        events.push(event);
    }

    events
}

fn find_go_binary() -> Option<PathBuf> {
    let path = env::var_os("PATH")?;
    env::split_paths(&path)
        .map(|dir| dir.join("go"))
        .find(|candidate| candidate.is_file())
}

fn workspace_parent() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate should have a parent directory")
        .to_owned()
}

fn create_temp_dir() -> io::Result<PathBuf> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_nanos();
    let dir = env::temp_dir().join(format!(
        "datastar-axum-go-parity-{}-{nanos}",
        std::process::id()
    ));
    fs::create_dir(&dir)?;
    Ok(dir)
}

const GO_PARITY_PROGRAM: &str = r##"
package main

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"time"

	ds "github.com/starfederation/datastar-go/datastar"
)

func emit(cases map[string]string, name string, fn func(*ds.ServerSentEventGenerator) error) {
	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	sse := ds.NewSSE(w, req)
	if err := fn(sse); err != nil {
		panic(err)
	}
	cases[name] = w.Body.String()
}

func main() {
	cases := map[string]string{}
	emit(cases, "elements_full", func(sse *ds.ServerSentEventGenerator) error {
		return sse.PatchElements(
			"<svg id=\"icon\"></svg>\n<circle></circle>",
			ds.WithSelector("#icon"),
			ds.WithNamespaceSVG(),
			ds.WithViewTransitions(),
			ds.WithViewTransitionSelector("#icon"),
			ds.WithPatchElementsEventID("e1"),
			ds.WithRetryDuration(2500*time.Millisecond),
		)
	})
	emit(cases, "remove_element", func(sse *ds.ServerSentEventGenerator) error {
		return sse.RemoveElementByID("toast")
	})
	emit(cases, "signals_multiline", func(sse *ds.ServerSentEventGenerator) error {
		return sse.PatchSignals(
			[]byte("{\"message\":\"ok\"}\n{\"count\":2}"),
			ds.WithOnlyIfMissing(true),
			ds.WithPatchSignalsEventID("s1"),
			ds.WithPatchSignalsRetryDuration(2500*time.Millisecond),
		)
	})
	encoded, err := json.Marshal(cases)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(encoded))
}
"##;
