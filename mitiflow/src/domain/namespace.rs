//! Domain namespace — Zenoh key-prefix root scoped to a domain.

use std::fmt;

use crate::domain::DomainId;
use crate::error::{Error, Result};

/// A domain namespace, providing a validated Zenoh key-prefix root.
///
/// The `root` field holds a validated string suitable for use as a Zenoh
/// key expression prefix. It is derived from a [`DomainId`] and optionally
/// a suffix, following the pattern `{prefix}/{suffix}`.
///
/// # Validation rules
/// The root must be Zenoh key-prefix compatible:
/// - No leading slash (`/foo`)
/// - No trailing slash (`foo/`)
/// - No empty segments (`foo//bar`)
/// - No `*` or `$` characters
/// - No leading `_` segment (reserved for internal use)
/// - No whitespace
///
/// Default root is `mitiflow/{domain_id}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Namespace {
    root: String,
}

impl Namespace {
    /// Default namespace root template.
    const DEFAULT_ROOT_TEMPLATE: &'static str = "mitiflow/{domain_id}";

    /// Create a namespace with the default root derived from `domain_id`.
    ///
    /// Default root format: `mitiflow/{domain_id}`
    pub fn new(domain_id: &DomainId) -> Result<Self> {
        let root = Self::DEFAULT_ROOT_TEMPLATE.replace("{domain_id}", domain_id.as_str());
        Self::from_root(root)
    }

    /// Create a namespace from an explicit root string.
    ///
    /// Returns [`Error::Domain`] if the root fails validation.
    pub fn from_root(root: impl Into<String>) -> Result<Self> {
        let s = root.into();
        validate_namespace_root(&s)?;
        Ok(Self { root: s })
    }

    /// Derive a namespace-derived key prefix string for use with
    /// [`EventBusConfig`][crate::config::EventBusConfig].
    ///
    /// This combines the namespace root with a topic suffix:
    /// `"{root}/{suffix}"`.
    ///
    /// Returns [`Error::Domain`] if the suffix is invalid (empty, contains
    /// wildcard/reserved characters, or would produce an invalid key prefix).
    /// Internal control suffixes like `_store`, `_workers` are allowed.
    pub fn derive(&self, suffix: &str) -> Result<String> {
        validate_suffix(suffix)?;
        if suffix.starts_with('/') {
            return Err(Error::Domain(crate::domain::DomainError::Invalid(
                "suffix must not start with '/'".into(),
            )));
        }
        if suffix.ends_with('/') {
            return Err(Error::Domain(crate::domain::DomainError::Invalid(
                "suffix must not end with '/'".into(),
            )));
        }
        Ok(format!("{}/{}", self.root, suffix))
    }

    /// Get the namespace root string.
    pub fn root(&self) -> &str {
        &self.root
    }
}

impl fmt::Display for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.root)
    }
}

/// Validate a namespace root string.
///
/// Returns `Ok(())` if valid, or `Error::Domain(DomainError::Invalid)` otherwise.
fn validate_namespace_root(s: &str) -> Result<()> {
    if s.is_empty() {
        return Err(Error::Domain(crate::domain::DomainError::Invalid(
            "namespace root must not be empty".into(),
        )));
    }
    if s.starts_with('/') {
        return Err(Error::Domain(crate::domain::DomainError::Invalid(
            "namespace root must not have leading slash".into(),
        )));
    }
    if s.ends_with('/') {
        return Err(Error::Domain(crate::domain::DomainError::Invalid(
            "namespace root must not have trailing slash".into(),
        )));
    }
    if s.contains("//") {
        return Err(Error::Domain(crate::domain::DomainError::Invalid(
            "namespace root must not contain empty segments".into(),
        )));
    }
    if s.contains('*') {
        return Err(Error::Domain(crate::domain::DomainError::Invalid(
            "namespace root must not contain '*'".into(),
        )));
    }
    if s.contains('$') {
        return Err(Error::Domain(crate::domain::DomainError::Invalid(
            "namespace root must not contain '$'".into(),
        )));
    }
    if s.starts_with('_') {
        return Err(Error::Domain(crate::domain::DomainError::Invalid(
            "namespace root must not start with '_'".into(),
        )));
    }
    if s.contains(char::is_whitespace) {
        return Err(Error::Domain(crate::domain::DomainError::Invalid(
            "namespace root must not contain whitespace".into(),
        )));
    }
    Ok(())
}

/// Validate a namespace suffix string.
///
/// Returns `Ok(())` if valid, or `Error::Domain(DomainError::Invalid)` otherwise.
fn validate_suffix(s: &str) -> Result<()> {
    if s.is_empty() {
        return Err(Error::Domain(crate::domain::DomainError::Invalid(
            "suffix must not be empty".into(),
        )));
    }
    if s.contains("//") {
        return Err(Error::Domain(crate::domain::DomainError::Invalid(
            "suffix must not contain empty segments".into(),
        )));
    }
    if s.contains('*') {
        return Err(Error::Domain(crate::domain::DomainError::Invalid(
            "suffix must not contain '*'".into(),
        )));
    }
    if s.contains('$') {
        return Err(Error::Domain(crate::domain::DomainError::Invalid(
            "suffix must not contain '$'".into(),
        )));
    }
    if s.contains(char::is_whitespace) {
        return Err(Error::Domain(crate::domain::DomainError::Invalid(
            "suffix must not contain whitespace".into(),
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_default_root() {
        let domain_id: DomainId = "prod".parse().unwrap();
        let ns = Namespace::new(&domain_id).unwrap();
        assert_eq!(ns.root(), "mitiflow/prod");
    }

    #[test]
    fn namespace_derive() {
        let domain_id: DomainId = "prod".parse().unwrap();
        let ns = Namespace::new(&domain_id).unwrap();
        let prefix = ns.derive("events").unwrap();
        assert_eq!(prefix, "mitiflow/prod/events");
    }

    #[test]
    fn namespace_display() {
        let domain_id: DomainId = "test".parse().unwrap();
        let ns = Namespace::new(&domain_id).unwrap();
        assert_eq!(ns.to_string(), "mitiflow/test");
    }

    #[test]
    fn namespace_explicit_root() {
        let ns = Namespace::from_root("myapp/ns").unwrap();
        assert_eq!(ns.root(), "myapp/ns");
    }

    #[test]
    fn namespace_rejects_invalid_empty() {
        assert!(Namespace::from_root("").is_err());
    }

    #[test]
    fn namespace_rejects_empty_segment() {
        assert!(Namespace::from_root("a//b").is_err());
    }

    #[test]
    fn namespace_rejects_star() {
        assert!(Namespace::from_root("foo*bar").is_err());
    }

    #[test]
    fn namespace_rejects_dollar() {
        assert!(Namespace::from_root("foo$bar").is_err());
    }

    #[test]
    fn namespace_rejects_leading_underscore() {
        assert!(Namespace::from_root("_foo/bar").is_err());
    }

    #[test]
    fn namespace_rejects_whitespace() {
        assert!(Namespace::from_root("foo bar").is_err());
    }

    #[test]
    fn namespace_rejects_invalid() {
        assert!(Namespace::from_root("").is_err());
        assert!(Namespace::from_root("a//b").is_err());
        assert!(Namespace::from_root("foo*bar").is_err());
        assert!(Namespace::from_root("_foo/bar").is_err());
        assert!(Namespace::from_root("with space").is_err());
    }

    #[test]
    fn namespace_rejects_leading_slash() {
        assert!(Namespace::from_root("/foo").is_err());
    }

    #[test]
    fn namespace_rejects_trailing_slash() {
        assert!(Namespace::from_root("foo/").is_err());
    }

    #[test]
    fn derive_rejects_empty_suffix() {
        let domain_id: DomainId = "prod".parse().unwrap();
        let ns = Namespace::new(&domain_id).unwrap();
        assert!(ns.derive("").is_err());
    }

    #[test]
    fn derive_rejects_star_suffix() {
        let domain_id: DomainId = "prod".parse().unwrap();
        let ns = Namespace::new(&domain_id).unwrap();
        assert!(ns.derive("foo*bar").is_err());
    }

    #[test]
    fn derive_rejects_dollar_suffix() {
        let domain_id: DomainId = "prod".parse().unwrap();
        let ns = Namespace::new(&domain_id).unwrap();
        assert!(ns.derive("foo$bar").is_err());
    }

    #[test]
    fn derive_rejects_whitespace_suffix() {
        let domain_id: DomainId = "prod".parse().unwrap();
        let ns = Namespace::new(&domain_id).unwrap();
        assert!(ns.derive("foo bar").is_err());
    }

    #[test]
    fn derive_rejects_leading_slash_suffix() {
        let domain_id: DomainId = "prod".parse().unwrap();
        let ns = Namespace::new(&domain_id).unwrap();
        assert!(ns.derive("/events").is_err());
    }

    #[test]
    fn derive_rejects_trailing_slash_suffix() {
        let domain_id: DomainId = "prod".parse().unwrap();
        let ns = Namespace::new(&domain_id).unwrap();
        assert!(ns.derive("events/").is_err());
    }

    #[test]
    fn derive_rejects_double_slash_suffix() {
        let domain_id: DomainId = "prod".parse().unwrap();
        let ns = Namespace::new(&domain_id).unwrap();
        assert!(ns.derive("a//b").is_err());
    }

    #[test]
    fn derive_allows_internal_control_suffix() {
        let domain_id: DomainId = "prod".parse().unwrap();
        let ns = Namespace::new(&domain_id).unwrap();
        let prefix = ns.derive("_store").unwrap();
        assert_eq!(prefix, "mitiflow/prod/_store");
        let prefix2 = ns.derive("_workers").unwrap();
        assert_eq!(prefix2, "mitiflow/prod/_workers");
    }
}
