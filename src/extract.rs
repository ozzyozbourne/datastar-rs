use {
    crate::consts::{DATASTAR_KEY, DATASTAR_REQ_HEADER},
    axum::{
        Json,
        body::Bytes,
        extract::{FromRequest, OptionalFromRequest, Request},
        http::{Method, StatusCode},
        response::{IntoResponse, Response},
    },
    serde::de::DeserializeOwned,
    tracing::{debug, trace},
};

#[derive(Debug)]
pub struct ReadSignals<T: DeserializeOwned + Default>(pub T);

impl<T: DeserializeOwned + Default, S: Send + Sync> OptionalFromRequest<S> for ReadSignals<T>
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

impl<T: DeserializeOwned + Default, S: Send + Sync> FromRequest<S> for ReadSignals<T>
where
    Bytes: FromRequest<S>,
{
    type Rejection = Response;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        match *req.method() {
            Method::GET | Method::DELETE => {
                debug!(method = %req.method(), "reading Datastar signals from query");
                let Some(signals) =
                    datastar_query_param(req.uri().query()).map_err(IntoResponse::into_response)?
                else {
                    return Ok(Self(T::default()));
                };

                serde_json::from_str(&signals).map(Self).map_err(|_| {
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

fn datastar_query_param(query: Option<&str>) -> Result<Option<String>, (StatusCode, &'static str)> {
    let Some(query) = query else {
        return Ok(None);
    };
    let params = serde_urlencoded::from_str::<Vec<(String, String)>>(query).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            "failed to parse Datastar signals from query",
        )
    })?;

    Ok(params
        .into_iter()
        .find_map(|(key, value)| (key == DATASTAR_KEY && !value.is_empty()).then_some(value)))
}
