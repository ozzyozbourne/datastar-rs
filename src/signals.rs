use {
    crate::{
        DatastarEvent,
        consts::{self, EventType, ONLY_IF_MISSING_DATALINE_LITERAL, SIGNALS_DATALINE_LITERAL},
    },
    core::time::Duration,
    serde::Serialize,
};

#[derive(Debug, thiserror::Error)]
pub enum SignalError {
    #[error("failed to serialize signals: {0}")]
    Serialize(#[from] serde_json::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatchSignals {
    pub id: Option<String>,
    pub retry: Duration,
    pub signals: String,
    pub only_if_missing: bool,
}

impl PatchSignals {
    pub fn new(signals: impl Into<String>) -> Self {
        Self {
            id: None,
            retry: consts::DEFAULT_SSE_RETRY_DURATION,
            signals: signals.into(),
            only_if_missing: consts::DEFAULT_PATCH_SIGNALS_ONLY_IF_MISSING,
        }
    }

    pub fn json<T: Serialize>(signals: &T) -> Result<Self, SignalError> {
        Ok(Self::new(serde_json::to_string(signals)?))
    }

    pub fn event_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    pub fn retry(mut self, retry: Duration) -> Self {
        self.retry = retry;
        self
    }

    pub fn only_if_missing(mut self, only_if_missing: bool) -> Self {
        self.only_if_missing = only_if_missing;
        self
    }

    pub fn into_datastar_event(self) -> DatastarEvent {
        let mut data = Vec::new();

        if self.only_if_missing != consts::DEFAULT_PATCH_SIGNALS_ONLY_IF_MISSING {
            data.push(format!(
                "{ONLY_IF_MISSING_DATALINE_LITERAL} {}",
                self.only_if_missing
            ));
        }

        if !self.signals.is_empty() {
            for line in self.signals.split('\n') {
                data.push(format!("{SIGNALS_DATALINE_LITERAL} {line}"));
            }
        }

        DatastarEvent {
            event: EventType::PatchSignals,
            id: self.id,
            retry: self.retry,
            data,
        }
    }
}

impl From<PatchSignals> for DatastarEvent {
    fn from(value: PatchSignals) -> Self {
        value.into_datastar_event()
    }
}
