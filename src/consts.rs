use core::time::Duration;

pub const DATASTAR_KEY: &str = "datastar";
pub const DATASTAR_REQ_HEADER: &str = "datastar-request";

pub const DEFAULT_SSE_RETRY_DURATION: Duration = Duration::from_millis(1000);

pub(crate) const SELECTOR_DATALINE_LITERAL: &str = "selector";
pub(crate) const MODE_DATALINE_LITERAL: &str = "mode";
pub(crate) const NAMESPACE_DATALINE_LITERAL: &str = "namespace";
pub(crate) const USE_VIEW_TRANSITION_DATALINE_LITERAL: &str = "useViewTransition";
pub(crate) const VIEW_TRANSITION_SELECTOR_DATALINE_LITERAL: &str = "viewTransitionSelector";
pub(crate) const ELEMENTS_DATALINE_LITERAL: &str = "elements";
pub(crate) const SIGNALS_DATALINE_LITERAL: &str = "signals";
pub(crate) const ONLY_IF_MISSING_DATALINE_LITERAL: &str = "onlyIfMissing";

pub(crate) const DEFAULT_ELEMENTS_USE_VIEW_TRANSITIONS: bool = false;
pub(crate) const DEFAULT_PATCH_SIGNALS_ONLY_IF_MISSING: bool = false;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ElementPatchMode {
    #[default]
    Outer,
    Inner,
    Remove,
    Replace,
    Prepend,
    Append,
    Before,
    After,
}

impl ElementPatchMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Outer => "outer",
            Self::Inner => "inner",
            Self::Remove => "remove",
            Self::Replace => "replace",
            Self::Prepend => "prepend",
            Self::Append => "append",
            Self::Before => "before",
            Self::After => "after",
        }
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Namespace {
    #[default]
    Html,
    Svg,
    Mathml,
}

impl Namespace {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Html => "html",
            Self::Svg => "svg",
            Self::Mathml => "mathml",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventType {
    PatchElements,
    PatchSignals,
}

impl EventType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PatchElements => "datastar-patch-elements",
            Self::PatchSignals => "datastar-patch-signals",
        }
    }
}
