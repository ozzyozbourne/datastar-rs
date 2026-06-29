use {crate::consts, core::time::Duration};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatastarEvent {
    pub event: consts::EventType,
    pub id: Option<String>,
    pub retry: Duration,
    pub data: Vec<String>,
}

impl DatastarEvent {
    pub fn new(event: consts::EventType, data: Vec<String>) -> Self {
        Self {
            event,
            id: None,
            retry: consts::DEFAULT_SSE_RETRY_DURATION,
            data,
        }
    }

    pub fn event_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    pub fn retry(mut self, retry: Duration) -> Self {
        self.retry = retry;
        self
    }

    pub fn write_sse(&self, out: &mut impl core::fmt::Write) -> core::fmt::Result {
        writeln!(out, "event: {}", self.event.as_str())?;

        if let Some(id) = &self.id {
            writeln!(out, "id: {id}")?;
        }

        if self.retry > Duration::ZERO && self.retry != consts::DEFAULT_SSE_RETRY_DURATION {
            writeln!(out, "retry: {}", self.retry.as_millis())?;
        }

        for line in &self.data {
            writeln!(out, "data: {line}")?;
        }

        writeln!(out)
    }

    pub fn to_sse_string(&self) -> String {
        let mut out = String::new();
        self.write_sse(&mut out)
            .expect("writing to String should not fail");
        out
    }
}
