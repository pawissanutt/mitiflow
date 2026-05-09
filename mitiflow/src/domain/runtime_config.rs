use serde::{Deserialize, Serialize};

use crate::domain::{DomainError, MitiflowDomain, Namespace, TransportProfile};
use crate::error::Result;

/// Optional domain block accepted by binary YAML configs.
///
/// ```yaml
/// domain:
///   id: my-domain
///   namespace: optional/override
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainYamlConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Optional transport block accepted by binary YAML configs.
///
/// ```yaml
/// transport:
///   profile: local-isolated
///   connect: ["tcp/router:7447"]
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransportYamlConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub connect: Vec<String>,
}

/// Domain settings resolved from defaults, YAML, environment, and CLI overrides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainRuntimeConfig {
    pub id: String,
    pub namespace: Option<String>,
    pub transport: TransportProfile,
}

impl DomainRuntimeConfig {
    /// Resolve domain settings with precedence: env → YAML → provided defaults.
    pub fn from_sources(
        default_id: &str,
        domain: Option<&DomainYamlConfig>,
        transport: Option<&TransportYamlConfig>,
    ) -> Result<Self> {
        Self::from_sources_with_transport(default_id, domain, transport, None)
    }

    /// Resolve domain settings, letting an explicit transport override env/YAML.
    pub fn from_sources_with_transport(
        default_id: &str,
        domain: Option<&DomainYamlConfig>,
        transport: Option<&TransportYamlConfig>,
        transport_override: Option<TransportProfile>,
    ) -> Result<Self> {
        let id = env_non_empty("MITIFLOW_DOMAIN_ID")
            .or_else(|| {
                domain
                    .and_then(|domain| domain.id.as_deref())
                    .map(trim_owned)
            })
            .unwrap_or_else(|| default_id.to_string());

        let namespace = env_non_empty("MITIFLOW_DOMAIN_NAMESPACE").or_else(|| {
            domain
                .and_then(|domain| domain.namespace.as_deref())
                .map(trim_owned)
        });

        let resolved_transport = match transport_override {
            Some(transport) => transport,
            None => resolve_transport_profile(transport)?,
        };

        Ok(Self {
            id,
            namespace,
            transport: resolved_transport,
        })
    }

    /// Open a [`MitiflowDomain`] from the resolved settings.
    pub async fn open(self) -> Result<MitiflowDomain> {
        let mut builder = MitiflowDomain::builder(self.id).transport(self.transport);
        if let Some(namespace) = self.namespace {
            builder = builder.namespace(Namespace::from_root(namespace)?);
        }
        builder.open().await
    }
}

fn resolve_transport_profile(transport: Option<&TransportYamlConfig>) -> Result<TransportProfile> {
    let profile = env_non_empty("MITIFLOW_TRANSPORT_PROFILE")
        .or_else(|| {
            transport
                .and_then(|transport| transport.profile.as_deref())
                .map(trim_owned)
        })
        .unwrap_or_else(|| "local-isolated".to_string());

    let connect = match std::env::var("MITIFLOW_TRANSPORT_CONNECT") {
        Ok(connect) => split_connect(&connect),
        Err(_) => transport
            .map(|transport| normalize_connect(&transport.connect))
            .unwrap_or_default(),
    };

    match normalize_profile(&profile).as_str() {
        "localisolated" => Ok(TransportProfile::LocalIsolated),
        "client" => Ok(TransportProfile::Client {
            connect: require_connect("client", connect)?,
        }),
        "peermesh" => Ok(TransportProfile::PeerMesh {
            connect: require_connect("peer-mesh", connect)?,
        }),
        "ambient" => Ok(TransportProfile::Ambient),
        other => Err(DomainError::Invalid(format!(
            "unknown transport profile '{profile}' (normalized '{other}'; expected local-isolated, client, peer-mesh, or ambient)"
        ))
        .into()),
    }
}

fn require_connect(profile: &str, connect: Vec<String>) -> Result<Vec<String>> {
    if connect.is_empty() {
        return Err(DomainError::EmptyEndpoints {
            profile: profile.into(),
        }
        .into());
    }
    Ok(connect)
}

fn normalize_profile(profile: &str) -> String {
    profile
        .chars()
        .filter(|ch| *ch != '-' && *ch != '_')
        .flat_map(char::to_lowercase)
        .collect()
}

fn split_connect(connect: &str) -> Vec<String> {
    connect
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn normalize_connect(connect: &[String]) -> Vec<String> {
    connect
        .iter()
        .map(|endpoint| endpoint.trim())
        .filter(|endpoint| !endpoint.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn env_non_empty(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn trim_owned(value: &str) -> String {
    value.trim().to_owned()
}
