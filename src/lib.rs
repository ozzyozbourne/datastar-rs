#![forbid(unsafe_code)]

mod compression;
mod consts;
mod elements;
mod event;
mod extract;
mod script;
mod signals;
mod sse;

pub use compression::{Compression, CompressionAlgorithm, CompressionStrategy};
pub use consts::{ElementPatchMode, EventType, Namespace};
pub use elements::{PatchElements, remove_element, remove_element_by_id};
pub use event::DatastarEvent;
pub use extract::ReadSignals;
pub use script::{
    ExecuteScript, ScriptError, console_error, console_log, dispatch_custom_event,
    dispatch_custom_event_to, prefetch, redirect, replace_url,
};
pub use signals::{PatchSignals, SignalError};
pub use sse::{DatastarSender, DatastarSse, DatastarSseBuilder, SseError};

/// Re-exports for applications that wire Datastar SDK spans into OpenTelemetry.
///
/// The SDK emits [`tracing`] spans/events. Applications should install a
/// `tracing_subscriber` with `tracing_opentelemetry::OpenTelemetryLayer` and
/// their exporter of choice.
pub mod telemetry {
    pub use opentelemetry;
    pub use tracing;
    pub use tracing_opentelemetry;
}

pub mod prelude {
    pub use crate::{
        Compression, CompressionAlgorithm, CompressionStrategy, DatastarEvent, DatastarSender,
        DatastarSse, ElementPatchMode, ExecuteScript, Namespace, PatchElements, PatchSignals,
        ReadSignals,
    };
}
