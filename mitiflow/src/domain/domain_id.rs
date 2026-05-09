//! Domain identifier — validated, type-safe namespace key component.

use std::fmt;
use std::str::FromStr;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{Error, Result};

/// Maximum length for a [`DomainId`] string representation.
const MAX_LEN: usize = 64;

/// A validated domain identifier.
///
/// Wraps a UTF-8 string that names a logical domain. Used as the primary
/// key component when constructing Zenoh key expressions and scoping
/// liveliness/admin keys.
///
/// # Validation rules
/// - Non-empty
/// - Maximum 64 Unicode scalar characters
/// - No `*` character (Zenoh wildcard)
/// - No `$` character (Zenoh reserved)
/// - No leading `_` character (reserved for internal use)
/// - No whitespace characters
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DomainId(String);

impl DomainId {
    pub fn new(id: impl Into<String>) -> Result<Self> {
        let s = id.into();
        validate_domain_id(&s)?;
        Ok(Self(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DomainId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for DomainId {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl AsRef<str> for DomainId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Serialize for DomainId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for DomainId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        DomainId::new(&s).map_err(D::Error::custom)
    }
}

/// Validate a domain identifier string.
///
/// Returns `Ok(())` if valid, or `Error::Domain(DomainError::Invalid)` otherwise.
fn validate_domain_id(s: &str) -> Result<()> {
    if s.is_empty() {
        return Err(Error::Domain(crate::domain::DomainError::Invalid(
            "domain id must not be empty".into(),
        )));
    }
    if s.chars().count() > MAX_LEN {
        return Err(Error::Domain(crate::domain::DomainError::Invalid(format!(
            "domain id must not exceed {} characters",
            MAX_LEN
        ))));
    }
    if s.contains('*') {
        return Err(Error::Domain(crate::domain::DomainError::Invalid(
            "domain id must not contain '*'".into(),
        )));
    }
    if s.contains('$') {
        return Err(Error::Domain(crate::domain::DomainError::Invalid(
            "domain id must not contain '$'".into(),
        )));
    }
    if s.starts_with('_') {
        return Err(Error::Domain(crate::domain::DomainError::Invalid(
            "domain id must not start with '_'".into(),
        )));
    }
    if s.contains(char::is_whitespace) {
        return Err(Error::Domain(crate::domain::DomainError::Invalid(
            "domain id must not contain whitespace".into(),
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_id_roundtrip() {
        let id: DomainId = "production".parse().unwrap();
        let s = id.to_string();
        let id2: DomainId = s.parse().unwrap();
        assert_eq!(id, id2);
    }

    #[test]
    fn domain_id_display() {
        let id: DomainId = "test-domain".parse().unwrap();
        assert_eq!(id.to_string(), "test-domain");
    }

    #[test]
    fn domain_id_validation() {
        DomainId::new("production").unwrap();
        DomainId::new("dev").unwrap();
        DomainId::new("域".to_string()).unwrap();
        DomainId::new("my-app-42").unwrap();
    }

    #[test]
    fn domain_id_64_chars_accepted() {
        let id: DomainId = "a".repeat(64).parse().unwrap();
        assert_eq!(id.as_str().len(), 64);
    }

    #[test]
    fn domain_id_65_chars_rejected() {
        let result = DomainId::new("a".repeat(65));
        assert!(result.is_err());
    }

    #[test]
    fn domain_id_unicode_64_chars_accepted() {
        let id: DomainId = "域".repeat(64).parse().unwrap();
        assert_eq!(id.as_str().chars().count(), 64);
    }

    #[test]
    fn domain_id_rejects_empty() {
        let result = DomainId::new("");
        assert!(result.is_err());
    }

    #[test]
    fn domain_id_rejects_star() {
        let result = DomainId::new("foo*bar");
        assert!(result.is_err());
    }

    #[test]
    fn domain_id_rejects_dollar() {
        let result = DomainId::new("foo$bar");
        assert!(result.is_err());
    }

    #[test]
    fn domain_id_rejects_leading_underscore() {
        let result = DomainId::new("_leading");
        assert!(result.is_err());
    }

    #[test]
    fn domain_id_rejects_whitespace() {
        let result = DomainId::new("with space");
        assert!(result.is_err());
    }

    #[test]
    fn domain_id_rejects_too_long() {
        let result = DomainId::new("a".repeat(65));
        assert!(result.is_err());
    }

    #[test]
    fn rejects_invalid() {
        assert!(DomainId::new("foo*bar").is_err());
        assert!(DomainId::new("foo$bar").is_err());
        assert!(DomainId::new("").is_err());
        assert!(DomainId::new("_leading").is_err());
        assert!(DomainId::new("with space").is_err());
    }

    #[test]
    fn serde_rejects_invalid() {
        let invalid_cases = ["foo*bar", "_leading", ""];
        for case in invalid_cases {
            let json = format!("\"{}\"", case);
            let result: std::result::Result<DomainId, _> = serde_json::from_str(&json);
            assert!(result.is_err(), "expected '{}' to be rejected", case);
        }
    }

    #[test]
    fn serde_roundtrip() {
        let id: DomainId = "production".parse().unwrap();
        let json = serde_json::to_string(&id).unwrap();
        let id2: DomainId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, id2);
    }
}
