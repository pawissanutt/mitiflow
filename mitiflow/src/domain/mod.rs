//! Domain primitives for logical isolation.
//!
//! Provides [`DomainId`] and [`Namespace`] types for scoping Zenoh key
//! expressions and transport configuration.

#[allow(clippy::module_inception)]
mod domain;
mod domain_id;
mod namespace;
mod runtime_config;
mod transport;

pub use domain::{MitiflowDomain, MitiflowDomainBuilder};
pub use domain_id::DomainId;
pub use namespace::Namespace;
pub use runtime_config::{DomainRuntimeConfig, DomainYamlConfig, TransportYamlConfig};
pub use transport::TransportProfile;

use thiserror::Error;

/// Domain-specific errors.
#[derive(Debug, Error)]
pub enum DomainError {
    #[error("invalid domain value: {0}")]
    Invalid(String),

    #[error("transport profile '{profile}' requires at least one endpoint")]
    EmptyEndpoints { profile: String },
}
