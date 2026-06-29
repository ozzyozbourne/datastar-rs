use {
    crate::{DatastarEvent, ElementPatchMode, PatchElements},
    core::time::Duration,
    serde::Serialize,
};

#[derive(Debug, thiserror::Error)]
pub enum ScriptError {
    #[error("event name is required")]
    EmptyEventName,
    #[error("failed to serialize detail: {0}")]
    Serialize(#[from] serde_json::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteScript {
    pub id: Option<String>,
    pub retry: Duration,
    pub script: String,
    pub auto_remove: Option<bool>,
    pub attributes: Vec<String>,
}

impl ExecuteScript {
    pub fn new(script: impl Into<String>) -> Self {
        Self {
            id: None,
            retry: crate::consts::DEFAULT_SSE_RETRY_DURATION,
            script: script.into(),
            auto_remove: None,
            attributes: Vec::new(),
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

    pub fn auto_remove(mut self, auto_remove: bool) -> Self {
        self.auto_remove = Some(auto_remove);
        self
    }

    pub fn attribute(mut self, key: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        self.attributes
            .push(format!(r#"{}="{}""#, key.as_ref(), value.as_ref()));
        self
    }

    pub fn raw_attribute(mut self, attribute: impl Into<String>) -> Self {
        self.attributes.push(attribute.into());
        self
    }

    pub fn into_datastar_event(self) -> DatastarEvent {
        let mut script = String::from("<script");

        for attribute in &self.attributes {
            script.push(' ');
            script.push_str(attribute);
        }

        if self.auto_remove.unwrap_or(true) {
            script.push_str(r#" data-effect="el.remove()""#);
        }

        script.push('>');
        script.push_str(&self.script);
        script.push_str("</script>");

        let mut patch = PatchElements::new(script)
            .selector("body")
            .mode(ElementPatchMode::Append)
            .retry(self.retry);

        if let Some(id) = self.id {
            patch = patch.event_id(id);
        }

        patch.into_datastar_event()
    }
}

impl From<ExecuteScript> for DatastarEvent {
    fn from(value: ExecuteScript) -> Self {
        value.into_datastar_event()
    }
}

pub fn console_log(message: impl AsRef<str>) -> ExecuteScript {
    ExecuteScript::new(format!("console.log({:?})", message.as_ref()))
}

pub fn console_error(message: impl AsRef<str>) -> ExecuteScript {
    ExecuteScript::new(format!("console.error({:?})", message.as_ref()))
}

pub fn redirect(url: impl AsRef<str>) -> ExecuteScript {
    ExecuteScript::new(format!(
        "setTimeout(() => window.location.href = {:?})",
        url.as_ref()
    ))
}

pub fn replace_url(url: impl AsRef<str>) -> ExecuteScript {
    ExecuteScript::new(format!(
        "window.history.replaceState({{}}, \"\", {:?})",
        url.as_ref()
    ))
}

pub fn prefetch(urls: impl IntoIterator<Item = impl AsRef<str>>) -> ExecuteScript {
    let urls = urls
        .into_iter()
        .map(|url| format!("{:?}", url.as_ref()))
        .collect::<Vec<_>>()
        .join(",\n");
    ExecuteScript::new(format!(
        r#"{{
  "prefetch": [
    {{
      "source": "list",
      "urls": [
        {urls}
      ]
    }}
  ]
}}"#
    ))
    .auto_remove(false)
    .raw_attribute(r#"type="speculationrules""#)
}

pub fn dispatch_custom_event<T: Serialize>(
    event_name: impl AsRef<str>,
    detail: &T,
) -> Result<ExecuteScript, ScriptError> {
    dispatch_custom_event_with_options(event_name, detail, DispatchCustomEventOptions::default())
}

pub fn dispatch_custom_event_to<T: Serialize>(
    event_name: impl AsRef<str>,
    detail: &T,
    selector: Option<&str>,
) -> Result<ExecuteScript, ScriptError> {
    dispatch_custom_event_with_options(
        event_name,
        detail,
        DispatchCustomEventOptions::default().selector_opt(selector),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchCustomEventOptions {
    pub selector: Option<String>,
    pub bubbles: bool,
    pub cancelable: bool,
    pub composed: bool,
}

impl Default for DispatchCustomEventOptions {
    fn default() -> Self {
        Self {
            selector: None,
            bubbles: true,
            cancelable: true,
            composed: true,
        }
    }
}

impl DispatchCustomEventOptions {
    pub fn selector(mut self, selector: impl Into<String>) -> Self {
        self.selector = Some(selector.into());
        self
    }

    pub fn selector_opt(mut self, selector: Option<&str>) -> Self {
        self.selector = selector.map(ToOwned::to_owned);
        self
    }

    pub fn bubbles(mut self, bubbles: bool) -> Self {
        self.bubbles = bubbles;
        self
    }

    pub fn cancelable(mut self, cancelable: bool) -> Self {
        self.cancelable = cancelable;
        self
    }

    pub fn composed(mut self, composed: bool) -> Self {
        self.composed = composed;
        self
    }
}

pub fn dispatch_custom_event_with_options<T: Serialize>(
    event_name: impl AsRef<str>,
    detail: &T,
    options: DispatchCustomEventOptions,
) -> Result<ExecuteScript, ScriptError> {
    let event_name = event_name.as_ref();
    if event_name.is_empty() {
        return Err(ScriptError::EmptyEventName);
    }

    let detail = serde_json::to_string(detail)?;
    let DispatchCustomEventOptions {
        selector,
        bubbles,
        cancelable,
        composed,
    } = options;
    let elements = selector.as_deref().map_or_else(
        || "[document]".to_owned(),
        |selector| format!("document.querySelectorAll({selector:?})"),
    );

    Ok(ExecuteScript::new(format!(
        r"{{
  const elements = {elements};
  const event = new CustomEvent({event_name:?}, {{
    bubbles: {bubbles},
    cancelable: {cancelable},
    composed: {composed},
    detail: {detail},
  }});
  elements.forEach((element) => {{
    element.dispatchEvent(event);
  }});
}}"
    )))
}
