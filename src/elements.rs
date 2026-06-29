use {
    crate::{
        DatastarEvent,
        consts::{
            self, ELEMENTS_DATALINE_LITERAL, ElementPatchMode, EventType, MODE_DATALINE_LITERAL,
            NAMESPACE_DATALINE_LITERAL, Namespace, SELECTOR_DATALINE_LITERAL,
            USE_VIEW_TRANSITION_DATALINE_LITERAL, VIEW_TRANSITION_SELECTOR_DATALINE_LITERAL,
        },
    },
    core::time::Duration,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatchElements {
    pub id: Option<String>,
    pub retry: Duration,
    pub elements: Option<String>,
    pub selector: Option<String>,
    pub mode: ElementPatchMode,
    pub namespace: Namespace,
    pub use_view_transition: bool,
    pub view_transition_selector: Option<String>,
}

impl PatchElements {
    pub fn new(elements: impl Into<String>) -> Self {
        Self {
            id: None,
            retry: consts::DEFAULT_SSE_RETRY_DURATION,
            elements: Some(elements.into()),
            selector: None,
            mode: ElementPatchMode::Outer,
            namespace: Namespace::Html,
            use_view_transition: consts::DEFAULT_ELEMENTS_USE_VIEW_TRANSITIONS,
            view_transition_selector: None,
        }
    }

    pub fn remove(selector: impl Into<String>) -> Self {
        Self::new("")
            .selector(selector)
            .mode(ElementPatchMode::Remove)
            .without_elements()
    }

    pub fn event_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    pub fn retry(mut self, retry: Duration) -> Self {
        self.retry = retry;
        self
    }

    pub fn selector(mut self, selector: impl Into<String>) -> Self {
        self.selector = Some(selector.into());
        self
    }

    pub fn selector_id(self, id: impl AsRef<str>) -> Self {
        self.selector(format!("#{}", id.as_ref()))
    }

    pub fn mode(mut self, mode: ElementPatchMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn namespace(mut self, namespace: Namespace) -> Self {
        self.namespace = namespace;
        self
    }

    pub fn use_view_transition(mut self, use_view_transition: bool) -> Self {
        self.use_view_transition = use_view_transition;
        self
    }

    pub fn view_transition_selector(mut self, selector: impl Into<String>) -> Self {
        self.view_transition_selector = Some(selector.into());
        self
    }

    pub fn without_elements(mut self) -> Self {
        self.elements = None;
        self
    }

    pub fn into_datastar_event(self) -> DatastarEvent {
        let mut data = Vec::new();

        if let Some(selector) = &self.selector {
            data.push(format!("{SELECTOR_DATALINE_LITERAL} {selector}"));
        }

        if self.mode != ElementPatchMode::Outer {
            data.push(format!("{MODE_DATALINE_LITERAL} {}", self.mode.as_str()));
        }

        if self.namespace != Namespace::Html {
            data.push(format!(
                "{NAMESPACE_DATALINE_LITERAL} {}",
                self.namespace.as_str()
            ));
        }

        if self.use_view_transition != consts::DEFAULT_ELEMENTS_USE_VIEW_TRANSITIONS {
            data.push(format!(
                "{USE_VIEW_TRANSITION_DATALINE_LITERAL} {}",
                self.use_view_transition
            ));
        }

        if self.use_view_transition {
            if let Some(selector) = &self.view_transition_selector {
                data.push(format!(
                    "{VIEW_TRANSITION_SELECTOR_DATALINE_LITERAL} {selector}"
                ));
            }
        }

        if let Some(elements) = &self.elements
            && !elements.is_empty()
        {
            for line in elements.split('\n') {
                data.push(format!("{ELEMENTS_DATALINE_LITERAL} {line}"));
            }
        }

        DatastarEvent {
            event: EventType::PatchElements,
            id: self.id,
            retry: self.retry,
            data,
        }
    }
}

impl From<PatchElements> for DatastarEvent {
    fn from(value: PatchElements) -> Self {
        value.into_datastar_event()
    }
}

pub fn remove_element(selector: impl Into<String>) -> PatchElements {
    PatchElements::remove(selector)
}

pub fn remove_element_by_id(id: impl AsRef<str>) -> PatchElements {
    PatchElements::remove(format!("#{}", id.as_ref()))
}
