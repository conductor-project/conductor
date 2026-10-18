//! HTTP API contracts and optional client/server implementations.
//!
//! Enable `client` for a reusable HTTP client or `server` for the Axum API.

#[cfg(feature = "client")]
pub mod client;

/// Serde request/response contracts shared by the HTTP server and client.
pub mod contracts;

#[cfg(feature = "server")]
mod server;

#[cfg(feature = "server")]
pub use server::*;
