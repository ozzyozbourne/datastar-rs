use {
    crate::consts::{DATASTAR_KEY, DATASTAR_REQ_HEADER},
    axum::{
        Json,
        body::Bytes,
        extract::{FromRequest, OptionalFromRequest, Query, Request},
        http::{Method, StatusCode},
        response::{IntoResponse, Response},
    },
    serde::{Deserialize, de::DeserializeOwned},
    tracing::{debug, trace},
};

#[derive(Deserialize)]
struct DatastarParam {
    datastar: serde_json::Value,
}

#[derive(Debug)]
pub struct ReadSignals<T: DeserializeOwned>(pub T);

impl<T: DeserializeOwned, S: Send + Sync> OptionalFromRequest<S> for ReadSignals<T>
where
    Bytes: FromRequest<S>,
{
    type Rejection = Response;

    async fn from_request(req: Request, state: &S) -> Result<Option<Self>, Self::Rejection> {
        if req.headers().get(DATASTAR_REQ_HEADER).is_none() {
            trace!("request is missing datastar-request header");
            return Ok(None);
        }
        trace!("request has datastar-request header");
        Ok(Some(
            <Self as FromRequest<S>>::from_request(req, state).await?,
        ))
    }
}

impl<T: DeserializeOwned, S: Send + Sync> FromRequest<S> for ReadSignals<T>
where
    Bytes: FromRequest<S>,
{
    type Rejection = Response;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        match *req.method() {
            Method::GET | Method::DELETE => {
                debug!(method = %req.method(), "reading Datastar signals from query");
                let query = Query::<DatastarParam>::from_request(req, state)
                    .await
                    .map_err(IntoResponse::into_response)?;

                let signals = query.0.datastar.as_str().ok_or_else(|| {
                    (
                        StatusCode::BAD_REQUEST,
                        format!("{DATASTAR_KEY} query parameter must be a JSON string"),
                    )
                        .into_response()
                })?;

                serde_json::from_str(signals).map(Self).map_err(|_| {
                    (
                        StatusCode::BAD_REQUEST,
                        "failed to parse Datastar signals from query",
                    )
                        .into_response()
                })
            }
            _ => {
                debug!(method = %req.method(), "reading Datastar signals from JSON body");
                let Json(value) = <Json<T> as FromRequest<S>>::from_request(req, state)
                    .await
                    .map_err(|_| {
                        (
                            StatusCode::BAD_REQUEST,
                            "failed to parse Datastar signals from body",
                        )
                            .into_response()
                    })?;
                Ok(Self(value))
            }
        }
    }
}
