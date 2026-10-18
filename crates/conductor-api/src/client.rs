//! Bounded, typed JSON client for the Conductor management API.

use std::fmt;
use std::net::IpAddr;
use std::time::Duration;

use reqwest::{Certificate, Client as HttpClient, Method, StatusCode, Url};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// Default cap applied to both successful and error response bodies.
pub const DEFAULT_MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

/// Settings used to construct a [`Client`].
#[derive(Clone)]
pub struct ClientConfig {
    pub base_url: String,
    pub bearer_token: Option<String>,
    pub ca_certificate_pem: Option<String>,
    pub timeout: Duration,
    pub max_response_bytes: usize,
}

impl ClientConfig {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            bearer_token: None,
            ca_certificate_pem: None,
            timeout: Duration::from_secs(10),
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
        }
    }
}

/// Safe, structured failures from the API client. Display output never includes
/// credentials, request contents, response contents, or the full URL.
pub enum ClientError {
    InvalidBaseUrl,
    InvalidConfiguration,
    InvalidPath,
    Request(reqwest::Error),
    ResponseTooLarge {
        limit: usize,
    },
    Api {
        status: StatusCode,
        code: Option<String>,
    },
    Decode,
}

impl fmt::Debug for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBaseUrl => f.write_str("InvalidBaseUrl"),
            Self::InvalidConfiguration => f.write_str("InvalidConfiguration"),
            Self::InvalidPath => f.write_str("InvalidPath"),
            Self::Request(_) => f.write_str("Request(<redacted>)"),
            Self::ResponseTooLarge { limit } => f
                .debug_struct("ResponseTooLarge")
                .field("limit", limit)
                .finish(),
            Self::Api { status, code } => f
                .debug_struct("Api")
                .field("status", status)
                .field("code", code)
                .finish(),
            Self::Decode => f.write_str("Decode"),
        }
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBaseUrl => f.write_str("invalid API base URL"),
            Self::InvalidConfiguration => f.write_str("invalid API client configuration"),
            Self::InvalidPath => f.write_str("invalid API path"),
            Self::Request(error) => write!(
                f,
                "API transport error ({})",
                error
                    .status()
                    .map_or("no status".to_owned(), |s| s.to_string())
            ),
            Self::ResponseTooLarge { limit } => {
                write!(f, "API response exceeded the {limit}-byte limit")
            }
            Self::Api { status, code } => match code {
                Some(code) => write!(f, "API returned {status} ({code})"),
                None => write!(f, "API returned {status}"),
            },
            Self::Decode => f.write_str("API response could not be decoded"),
        }
    }
}

impl std::error::Error for ClientError {}

/// Reusable HTTP client. Paths are relative to the base URL and must not be absolute.
#[derive(Clone)]
pub struct Client {
    http: HttpClient,
    base_url: Url,
    bearer_token: Option<String>,
    max_response_bytes: usize,
}

/// A decoded response with the HTTP status and optional ETag retained.
#[derive(Clone, Debug)]
pub struct ClientResponse<T> {
    pub status: StatusCode,
    pub etag: Option<String>,
    pub body: T,
}

impl Client {
    pub fn new(config: ClientConfig) -> Result<Self, ClientError> {
        if config.timeout.is_zero() || config.max_response_bytes == 0 {
            return Err(ClientError::InvalidConfiguration);
        }
        let mut base_url = Url::parse(&config.base_url).map_err(|_| ClientError::InvalidBaseUrl)?;
        if !matches!(base_url.scheme(), "http" | "https")
            || base_url.host_str().is_none()
            || base_url.query().is_some()
            || base_url.fragment().is_some()
            || !base_url.username().is_empty()
            || base_url.password().is_some()
        {
            return Err(ClientError::InvalidBaseUrl);
        }
        if base_url.scheme() != "https"
            && config.bearer_token.is_some()
            && !is_loopback_host(base_url.host_str().expect("validated host"))
        {
            return Err(ClientError::InvalidBaseUrl);
        }
        if !base_url.path().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }
        let mut builder = HttpClient::builder()
            .timeout(config.timeout)
            .redirect(reqwest::redirect::Policy::none());
        if let Some(pem) = config.ca_certificate_pem {
            let certificate = Certificate::from_pem(pem.as_bytes())
                .map_err(|_| ClientError::InvalidConfiguration)?;
            builder = builder.add_root_certificate(certificate);
        }
        let http = builder
            .build()
            .map_err(|_| ClientError::InvalidConfiguration)?;
        Ok(Self {
            http,
            base_url,
            bearer_token: config.bearer_token,
            max_response_bytes: config.max_response_bytes,
        })
    }

    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, ClientError> {
        self.request(Method::GET, path, None::<&()>, None, None, None)
            .await
    }

    /// Read a JSON response while preserving non-success status bodies such as
    /// `/readyz`'s typed `draining` or `degraded` state.
    pub async fn get_response<T: DeserializeOwned>(
        &self,
        path: &str,
    ) -> Result<ClientResponse<T>, ClientError> {
        let (status, etag, bytes) = self
            .request_raw(Method::GET, path, None::<&()>, None, None, None)
            .await?;
        let body = serde_json::from_slice(&bytes).map_err(|_| ClientError::Decode)?;
        Ok(ClientResponse { status, etag, body })
    }

    /// Read a bounded non-JSON response such as the Prometheus metrics endpoint.
    pub async fn get_bytes(&self, path: &str) -> Result<Vec<u8>, ClientError> {
        self.request_bytes(Method::GET, path, None::<&()>, None, None, None)
            .await
    }

    pub async fn get_text(&self, path: &str) -> Result<String, ClientError> {
        String::from_utf8(self.get_bytes(path).await?).map_err(|_| ClientError::Decode)
    }

    pub async fn post<B: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
        idempotency_key: Option<&str>,
    ) -> Result<T, ClientError> {
        self.request(Method::POST, path, Some(body), idempotency_key, None, None)
            .await
    }

    /// POST a mutation with an optional ETag precondition.
    pub async fn post_conditional<B: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
        idempotency_key: Option<&str>,
        if_match: Option<&str>,
    ) -> Result<T, ClientError> {
        self.request(
            Method::POST,
            path,
            Some(body),
            idempotency_key,
            if_match,
            None,
        )
        .await
    }

    /// POST a create-only operation using `If-None-Match: *`.
    pub async fn post_create<B: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
        idempotency_key: Option<&str>,
    ) -> Result<T, ClientError> {
        self.request(
            Method::POST,
            path,
            Some(body),
            idempotency_key,
            None,
            Some("*"),
        )
        .await
    }

    pub async fn put<B: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
        idempotency_key: Option<&str>,
        if_match: Option<&str>,
    ) -> Result<T, ClientError> {
        self.request(
            Method::PUT,
            path,
            Some(body),
            idempotency_key,
            if_match,
            None,
        )
        .await
    }

    /// PUT a create-only resource using `If-None-Match: *`.
    pub async fn put_create<B: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
        idempotency_key: Option<&str>,
    ) -> Result<T, ClientError> {
        self.request(
            Method::PUT,
            path,
            Some(body),
            idempotency_key,
            None,
            Some("*"),
        )
        .await
    }

    pub async fn delete<T: DeserializeOwned>(
        &self,
        path: &str,
        idempotency_key: Option<&str>,
        if_match: Option<&str>,
    ) -> Result<T, ClientError> {
        self.request(
            Method::DELETE,
            path,
            None::<&()>,
            idempotency_key,
            if_match,
            None,
        )
        .await
    }

    pub async fn delete_empty(
        &self,
        path: &str,
        idempotency_key: Option<&str>,
        if_match: Option<&str>,
    ) -> Result<(), ClientError> {
        let bytes = self
            .request_bytes(
                Method::DELETE,
                path,
                None::<&()>,
                idempotency_key,
                if_match,
                None,
            )
            .await?;
        if bytes.is_empty() {
            Ok(())
        } else {
            Err(ClientError::Decode)
        }
    }

    async fn request<B: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
        idempotency_key: Option<&str>,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
    ) -> Result<T, ClientError> {
        let bytes = self
            .request_bytes(method, path, body, idempotency_key, if_match, if_none_match)
            .await?;
        serde_json::from_slice(&bytes).map_err(|_| ClientError::Decode)
    }

    async fn request_bytes<B: Serialize + ?Sized>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
        idempotency_key: Option<&str>,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
    ) -> Result<Vec<u8>, ClientError> {
        let (status, _etag, bytes) = self
            .request_raw(method, path, body, idempotency_key, if_match, if_none_match)
            .await?;
        if !status.is_success() {
            let code = serde_json::from_slice::<ErrorBody>(&bytes)
                .ok()
                .map(|body| body.error.code);
            return Err(ClientError::Api { status, code });
        }
        Ok(bytes)
    }

    async fn request_raw<B: Serialize + ?Sized>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
        idempotency_key: Option<&str>,
        if_match: Option<&str>,
        if_none_match: Option<&str>,
    ) -> Result<(StatusCode, Option<String>, Vec<u8>), ClientError> {
        let route_path = path.split_once('?').map_or(path, |(route, _)| route);
        if route_path.starts_with('/')
            || route_path
                .split('/')
                .any(|segment| matches!(segment, "." | ".."))
            || path.contains('#')
        {
            return Err(ClientError::InvalidPath);
        }
        let url = self
            .base_url
            .join(path)
            .map_err(|_| ClientError::InvalidPath)?;
        if url.origin() != self.base_url.origin() {
            return Err(ClientError::InvalidPath);
        }
        let mut builder = self.http.request(method.clone(), url);
        if let Some(token) = &self.bearer_token {
            builder = builder.bearer_auth(token);
        }
        if let Some(key) = idempotency_key {
            builder = builder.header("idempotency-key", key);
        }
        if let Some(etag) = if_match {
            builder = builder.header(reqwest::header::IF_MATCH, etag);
        }
        if let Some(etag) = if_none_match {
            builder = builder.header(reqwest::header::IF_NONE_MATCH, etag);
        }
        if let Some(body) = body {
            builder = builder.json(body);
        }
        let response = builder.send().await.map_err(ClientError::Request)?;
        let status = response.status();
        let etag = response
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        if response
            .content_length()
            .is_some_and(|length| length > self.max_response_bytes as u64)
        {
            return Err(ClientError::ResponseTooLarge {
                limit: self.max_response_bytes,
            });
        }
        let mut stream = response;
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.chunk().await.map_err(ClientError::Request)? {
            if chunk.len() > self.max_response_bytes.saturating_sub(bytes.len()) {
                return Err(ClientError::ResponseTooLarge {
                    limit: self.max_response_bytes,
                });
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok((status, etag, bytes))
    }
}

fn is_loopback_host(host: &str) -> bool {
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

#[derive(serde::Deserialize)]
struct ErrorBody {
    error: ErrorDetail,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    fn serve_once(response: &'static [u8]) -> (String, thread::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral listener");
        let address = listener.local_addr().expect("listener address");
        let handle = thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("accept client");
            let mut request = vec![0; 8192];
            let read = socket.read(&mut request).expect("read request");
            let request = String::from_utf8_lossy(&request[..read]).into_owned();
            socket.write_all(response).expect("write response");
            request
        });
        (format!("http://{address}/"), handle)
    }

    fn client(url: String, limit: usize) -> Client {
        let mut config = ClientConfig::new(url);
        config.bearer_token = Some("test-secret-token".to_owned());
        config.max_response_bytes = limit;
        Client::new(config).expect("valid test client")
    }

    #[tokio::test]
    async fn decodes_bounded_json_and_sends_auth_and_mutation_metadata() {
        let response =
            b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}";
        let (url, server) = serve_once(response);
        let client = client(url, 11);
        let payload = serde_json::json!({"value": "safe"});
        let result: serde_json::Value = client
            .post_conditional(
                "v1/jobs/example/deployment/suspend",
                &payload,
                Some("operation-1"),
                Some("\"generation-4\""),
            )
            .await
            .expect("bounded response decodes");
        let request = server.join().expect("server thread");
        assert_eq!(result, serde_json::json!({"ok": true}));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("authorization: bearer test-secret-token")
        );
        assert!(
            request
                .to_ascii_lowercase()
                .contains("idempotency-key: operation-1")
        );
        assert!(
            request
                .to_ascii_lowercase()
                .contains("if-match: \"generation-4\"")
        );
        assert!(request.starts_with("POST /v1/jobs/example/deployment/suspend "));
    }

    #[tokio::test]
    async fn rejects_oversized_chunked_success_without_buffering_to_completion() {
        let response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n6\r\n123456\r\n6\r\nabcdef\r\n0\r\n\r\n";
        let (url, server) = serve_once(response);
        let client = client(url, 10);
        let error = client
            .get::<serde_json::Value>("v1/readyz")
            .await
            .expect_err("oversized body rejected");
        assert!(matches!(error, ClientError::ResponseTooLarge { limit: 10 }));
        let _ = server.join().expect("server thread");
    }

    #[tokio::test]
    async fn rejects_oversized_chunked_error_bodies() {
        let response = b"HTTP/1.1 503 Service Unavailable\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n6\r\nsecret\r\n7\r\npayload\r\n0\r\n\r\n";
        let (url, server) = serve_once(response);
        let error = client(url, 10)
            .get::<serde_json::Value>("v1/jobs")
            .await
            .expect_err("oversized error body rejected");
        assert!(matches!(error, ClientError::ResponseTooLarge { limit: 10 }));
        let _ = server.join().expect("server thread");
    }

    #[tokio::test]
    async fn reports_a_short_body_with_an_inaccurate_content_length_as_transport_error() {
        let response =
            b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{\"ok\":true}";
        let (url, server) = serve_once(response);
        let error = client(url, 128)
            .get::<serde_json::Value>("v1/readyz")
            .await
            .expect_err("short framed body is a transport error");
        assert!(matches!(error, ClientError::Request(_)));
        let _ = server.join().expect("server thread");
    }

    #[tokio::test]
    async fn returns_bounded_metrics_text_without_json_decoding() {
        let response =
            b"HTTP/1.1 200 OK\r\nContent-Length: 15\r\nConnection: close\r\n\r\nconductor_up 1\n";
        let (url, server) = serve_once(response);
        let text = client(url, 15)
            .get_text("v1/metrics")
            .await
            .expect("metrics are text, not JSON");
        assert_eq!(text, "conductor_up 1\n");
        let _ = server.join().expect("server thread");
    }

    #[tokio::test]
    async fn permits_pagination_query_parameters() {
        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 29\r\nConnection: close\r\n\r\n{\"jobs\":[],\"nextCursor\":null}";
        let (url, server) = serve_once(response);
        let page: serde_json::Value = client(url, 29)
            .get("v1/jobs?limit=25&cursor=next")
            .await
            .expect("pagination response decodes");
        assert_eq!(page["nextCursor"], serde_json::Value::Null);
        let request = server.join().expect("server thread");
        assert!(request.starts_with("GET /v1/jobs?limit=25&cursor=next "));
    }

    #[tokio::test]
    async fn create_put_sends_if_none_match_star_and_ipv6_loopback_allows_http() {
        let response =
            b"HTTP/1.1 201 Created\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}";
        let (url, server) = serve_once(response);
        let result: serde_json::Value = client(url, 11)
            .put_create(
                "v1/jobs/example/deployment",
                &serde_json::json!({"revision":1,"replicas":1}),
                Some("create-1"),
            )
            .await
            .expect("create response");
        assert_eq!(result["ok"], true);
        let request = server.join().expect("server thread").to_ascii_lowercase();
        assert!(request.starts_with("put /v1/jobs/example/deployment "));
        assert!(request.contains("if-none-match: *"));
        let mut config = ClientConfig::new("http://[::1]:8080/");
        config.bearer_token = Some("local-token".to_owned());
        assert!(Client::new(config).is_ok());
    }

    #[tokio::test]
    async fn typed_response_preserves_readiness_state_on_service_unavailable() {
        let response = b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 81\r\nETag: \"ready-7\"\r\nConnection: close\r\n\r\n{\"status\":\"draining\",\"role\":\"manager\",\"capabilities\":[\"remote_worker_transport\"]}";
        let (url, server) = serve_once(response);
        let response: ClientResponse<crate::contracts::ReadinessResponse> = client(url, 128)
            .get_response("readyz")
            .await
            .expect("typed readiness status response");
        assert_eq!(response.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.etag.as_deref(), Some("\"ready-7\""));
        assert_eq!(response.body.status, "draining");
        let _ = server.join().expect("server thread");
    }

    #[tokio::test]
    async fn maps_api_errors_without_exposing_response_contents() {
        let response = b"HTTP/1.1 409 Conflict\r\nContent-Length: 88\r\nConnection: close\r\n\r\n{\"error\":{\"code\":\"generation_conflict\",\"message\":\"secret request payload\",\"details\":[]}}";
        let (url, server) = serve_once(response);
        let error = client(url, 256)
            .get::<serde_json::Value>("v1/jobs/missing")
            .await
            .expect_err("API conflict maps to a typed error");
        let display = error.to_string();
        assert!(display.contains("409 Conflict"));
        assert!(display.contains("generation_conflict"));
        assert!(!display.contains("secret request payload"));
        let _ = server.join().expect("server thread");
    }

    #[tokio::test]
    async fn rejects_unsafe_paths_and_invalid_urls() {
        assert!(matches!(
            Client::new(ClientConfig::new("file:///tmp/api")),
            Err(ClientError::InvalidBaseUrl)
        ));
        let client = client("http://127.0.0.1/".to_owned(), 128);
        assert!(matches!(
            client
                .request::<(), serde_json::Value>(Method::GET, "../admin", None, None, None, None)
                .await,
            Err(ClientError::InvalidPath)
        ));
        assert!(matches!(
            client
                .request::<(), serde_json::Value>(
                    Method::GET,
                    "https://attacker.example/v1/jobs",
                    None,
                    None,
                    None,
                    None,
                )
                .await,
            Err(ClientError::InvalidPath)
        ));
        let mut remote_http_config = ClientConfig::new("http://api.example.test/");
        remote_http_config.bearer_token = Some("secret".to_owned());
        assert!(matches!(
            Client::new(remote_http_config),
            Err(ClientError::InvalidBaseUrl)
        ));
    }
}
#[derive(serde::Deserialize)]
struct ErrorDetail {
    code: String,
}
