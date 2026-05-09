//! Topic and partition configuration management.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use mitiflow::codec::CodecFormat;
use mitiflow::schema::KeyFormat;
use mitiflow::{DomainYamlConfig, TransportYamlConfig};

use serde::{Deserialize, Serialize};

// ── Orchestrator runtime YAML ──────────────────────────────────────────────

/// YAML config for orchestrator binaries.
///
/// ```yaml
/// domain:
///   id: my-domain
///   namespace: optional/override
/// transport:
///   profile: local-isolated
///   connect: ["tcp/router:7447"]
/// key_prefix: mitiflow
/// data_dir: ./orchestrator_data
/// lag_interval_ms: 1000
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct OrchestratorYamlConfig {
    #[serde(default)]
    pub domain: DomainYamlConfig,
    #[serde(default)]
    pub transport: TransportYamlConfig,
    #[serde(default)]
    pub key_prefix: Option<String>,
    #[serde(default = "default_orch_data_dir")]
    pub data_dir: PathBuf,
    #[serde(default = "default_lag_interval_ms")]
    pub lag_interval_ms: u64,
    #[serde(default)]
    pub admin_prefix: Option<String>,
    /// HTTP API bind address (e.g. "0.0.0.0:8080"). Also settable via `MITIFLOW_HTTP_BIND`.
    #[serde(default)]
    pub http_bind: Option<String>,
    /// Bearer token for HTTP API auth. Also settable via `MITIFLOW_AUTH_TOKEN`.
    #[serde(default)]
    pub auth_token: Option<String>,
    /// Path to a YAML file with `topics` to bootstrap on startup.
    /// Also settable via `MITIFLOW_BOOTSTRAP_TOPICS_FROM`.
    #[serde(default)]
    pub bootstrap_topics_from: Option<PathBuf>,
}

impl OrchestratorYamlConfig {
    pub fn from_yaml(yaml: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(serde_yaml::from_str(yaml)?)
    }

    pub fn from_file(
        path: impl AsRef<Path>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let content = std::fs::read_to_string(path)?;
        Self::from_yaml(&content)
    }

    pub fn into_orch_config(
        self,
        default_key_prefix: impl Into<String>,
    ) -> Result<crate::orchestrator::OrchestratorConfig, Box<dyn std::error::Error + Send + Sync>>
    {
        let key_prefix = std::env::var("MITIFLOW_KEY_PREFIX")
            .ok()
            .or(self.key_prefix)
            .unwrap_or_else(|| default_key_prefix.into());

        let data_dir = std::env::var("MITIFLOW_DATA_DIR")
            .ok()
            .map(PathBuf::from)
            .unwrap_or(self.data_dir);

        let lag_interval_ms = std::env::var("MITIFLOW_LAG_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(self.lag_interval_ms);

        let http_bind = std::env::var("MITIFLOW_HTTP_BIND")
            .ok()
            .or(self.http_bind)
            .and_then(|value| value.parse().ok());

        let bootstrap_topics_from = std::env::var("MITIFLOW_BOOTSTRAP_TOPICS_FROM")
            .ok()
            .map(PathBuf::from)
            .or(self.bootstrap_topics_from);

        let auth_token = auth_token_from_primary_env()?
            .or(normalize_auth_token(
                self.auth_token,
                "auth_token in orchestrator YAML config",
            )?)
            .or(auth_token_from_legacy_env()?);

        Ok(crate::orchestrator::OrchestratorConfig {
            key_prefix,
            data_dir,
            lag_interval: Duration::from_millis(lag_interval_ms),
            admin_prefix: self.admin_prefix,
            http_bind,
            auth_token,
            bootstrap_topics_from,
        })
    }
}

fn default_orch_data_dir() -> PathBuf {
    PathBuf::from("./orchestrator_data")
}

fn default_lag_interval_ms() -> u64 {
    1000
}

fn auth_token_from_primary_env() -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>>
{
    if let Ok(token) = std::env::var("MITIFLOW_AUTH_TOKEN") {
        return normalize_auth_token(Some(token), "MITIFLOW_AUTH_TOKEN");
    }
    Ok(None)
}

fn auth_token_from_legacy_env() -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>>
{
    if let Ok(token) = std::env::var("MITIFLOW_UI_TOKEN") {
        return normalize_auth_token(Some(token), "MITIFLOW_UI_TOKEN");
    }
    Ok(None)
}

fn normalize_auth_token(
    token: Option<String>,
    source: &str,
) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
    match token {
        Some(token) if token.trim().is_empty() => Err(format!("{source} must not be empty").into()),
        Some(token) => Ok(Some(token)),
        None => Ok(None),
    }
}

/// Topic configuration managed by the orchestrator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    pub name: String,
    /// Per-topic Zenoh key prefix. When set, the orchestrator creates a
    /// dedicated [`ClusterView`] for this prefix.  Defaults to `""` for
    /// backwards-compatible configs that share the orchestrator-level prefix.
    #[serde(default)]
    pub key_prefix: String,
    pub num_partitions: u32,
    pub replication_factor: u32,
    pub retention: RetentionPolicy,
    pub compaction: CompactionPolicy,
    /// Labels that an agent **must** have to serve this topic.
    /// An agent serves this topic only if its own labels contain all
    /// of these key-value pairs.
    #[serde(default)]
    pub required_labels: HashMap<String, String>,
    /// Labels that **exclude** an agent from serving this topic.
    /// If an agent's labels match any of these key-value pairs, it
    /// will not serve this topic.
    #[serde(default)]
    pub excluded_labels: HashMap<String, String>,
    /// Wire codec for events on this topic.
    #[serde(default)]
    pub codec: CodecFormat,
    /// Key format: unkeyed or keyed events.
    #[serde(default)]
    pub key_format: KeyFormat,
    /// Monotonically increasing schema version.
    #[serde(default)]
    pub schema_version: u32,
}

/// Retention policy for events.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RetentionPolicy {
    /// Max age of events before GC.
    pub max_age: Option<Duration>,
    /// Max total size in bytes per partition.
    pub max_bytes: Option<u64>,
    /// Max number of events per partition.
    pub max_events: Option<u64>,
}

/// Compaction policy.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CompactionPolicy {
    pub enabled: bool,
    /// Compaction interval.
    pub interval: Option<Duration>,
}

/// Persistent config store backed by fjall.
pub struct ConfigStore {
    #[allow(dead_code)]
    db: fjall::Database,
    topics: fjall::Keyspace,
}

impl ConfigStore {
    /// Open or create a config store at the given directory.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, fjall::Error> {
        let db = fjall::Database::builder(dir).open()?;
        let topics = db.keyspace("topics", fjall::KeyspaceCreateOptions::default)?;
        Ok(Self { db, topics })
    }

    /// Store a topic configuration.
    pub fn put_topic(
        &self,
        config: &TopicConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let value = serde_json::to_vec(config)?;
        self.topics.insert(&config.name, value)?;
        Ok(())
    }

    /// Get a topic configuration by name.
    pub fn get_topic(
        &self,
        name: &str,
    ) -> Result<Option<TopicConfig>, Box<dyn std::error::Error + Send + Sync>> {
        match self.topics.get(name)? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    /// List all topic configurations.
    pub fn list_topics(
        &self,
    ) -> Result<Vec<TopicConfig>, Box<dyn std::error::Error + Send + Sync>> {
        let mut topics = Vec::new();
        for guard in self.topics.iter() {
            let kv =
                guard
                    .into_inner()
                    .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                        Box::new(std::io::Error::other(format!("iter error: {e:?}")))
                    })?;
            let config: TopicConfig = serde_json::from_slice(&kv.1)?;
            topics.push(config);
        }
        Ok(topics)
    }

    /// Delete a topic configuration.
    pub fn delete_topic(&self, name: &str) -> Result<bool, fjall::Error> {
        if self.topics.get(name)?.is_some() {
            self.topics.remove(name)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

// ── Bootstrap from YAML ──────────────────────────────────────────────────

/// Minimal type that parses the `topics` array from any YAML file,
/// silently ignoring unknown top-level fields (`node`, `cluster`, etc.).
/// This allows the orchestrator to reuse the storage agent's YAML directly.
#[derive(Debug, Deserialize)]
pub struct BootstrapConfig {
    #[serde(default)]
    pub topics: Vec<BootstrapTopicEntry>,
}

/// A topic entry in a bootstrap YAML file.
/// All fields except `name` have sensible defaults.
#[derive(Debug, Deserialize)]
pub struct BootstrapTopicEntry {
    pub name: String,
    #[serde(default)]
    pub key_prefix: String,
    #[serde(default = "default_num_partitions")]
    pub num_partitions: u32,
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u32,
    #[serde(default)]
    pub codec: CodecFormat,
    #[serde(default)]
    pub key_format: KeyFormat,
    #[serde(default)]
    pub schema_version: u32,
}

fn default_num_partitions() -> u32 {
    16
}

fn default_replication_factor() -> u32 {
    1
}

impl BootstrapConfig {
    /// Read and parse a bootstrap config from a YAML file.
    pub fn from_file(
        path: impl AsRef<std::path::Path>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let content = std::fs::read_to_string(path)?;
        let config: BootstrapConfig = serde_yaml::from_str(&content)?;
        Ok(config)
    }
}

impl BootstrapTopicEntry {
    /// Convert into a full [`TopicConfig`] with default retention/compaction.
    pub fn into_topic_config(self) -> TopicConfig {
        TopicConfig {
            name: self.name,
            key_prefix: self.key_prefix,
            num_partitions: self.num_partitions,
            replication_factor: self.replication_factor,
            retention: RetentionPolicy::default(),
            compaction: CompactionPolicy::default(),
            required_labels: HashMap::new(),
            excluded_labels: HashMap::new(),
            codec: self.codec,
            key_format: self.key_format,
            schema_version: self.schema_version,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mitiflow::{DomainRuntimeConfig, TransportProfile};

    #[test]
    fn orchestrator_yaml_accepts_domain_transport_and_legacy_fields() {
        let _guard = EnvGuard::set(&[
            ("MITIFLOW_DOMAIN_ID", None),
            ("MITIFLOW_DOMAIN_NAMESPACE", None),
            ("MITIFLOW_TRANSPORT_PROFILE", None),
            ("MITIFLOW_TRANSPORT_CONNECT", None),
            ("MITIFLOW_KEY_PREFIX", None),
            ("MITIFLOW_DATA_DIR", None),
            ("MITIFLOW_LAG_INTERVAL_MS", None),
            ("MITIFLOW_HTTP_BIND", None),
            ("MITIFLOW_BOOTSTRAP_TOPICS_FROM", None),
        ]);

        let yaml = r#"
domain:
  id: orch-domain
  namespace: orch/ns
transport:
  profile: client
  connect: ["tcp/router:7447"]
key_prefix: legacy-prefix
data_dir: /tmp/orch-yaml
lag_interval_ms: 2500
admin_prefix: legacy-prefix/_admin
http_bind: 127.0.0.1:18080
bootstrap_topics_from: /tmp/topics.yaml
"#;

        let cfg = OrchestratorYamlConfig::from_yaml(yaml).unwrap();
        assert_eq!(cfg.domain.id.as_deref(), Some("orch-domain"));
        assert_eq!(cfg.domain.namespace.as_deref(), Some("orch/ns"));
        assert_eq!(cfg.transport.profile.as_deref(), Some("client"));
        assert_eq!(cfg.transport.connect, vec!["tcp/router:7447"]);

        let runtime = DomainRuntimeConfig::from_sources(
            "orchestrator-default",
            Some(&cfg.domain),
            Some(&cfg.transport),
        )
        .unwrap();
        assert_eq!(runtime.id, "orch-domain");
        assert_eq!(runtime.namespace.as_deref(), Some("orch/ns"));
        assert_eq!(
            runtime.transport,
            TransportProfile::Client {
                connect: vec!["tcp/router:7447".into()]
            }
        );

        let orch = cfg
            .into_orch_config("mitiflow/orchestrator-default")
            .unwrap();
        assert_eq!(orch.key_prefix, "legacy-prefix");
        assert_eq!(orch.data_dir, PathBuf::from("/tmp/orch-yaml"));
        assert_eq!(orch.lag_interval, Duration::from_millis(2500));
        assert_eq!(orch.admin_prefix.as_deref(), Some("legacy-prefix/_admin"));
        assert_eq!(
            orch.http_bind.map(|addr| addr.to_string()).as_deref(),
            Some("127.0.0.1:18080")
        );
        assert_eq!(
            orch.bootstrap_topics_from,
            Some(PathBuf::from("/tmp/topics.yaml"))
        );
    }

    #[test]
    fn orchestrator_yaml_legacy_minimal_remains_valid() {
        let _guard = EnvGuard::set(&[
            ("MITIFLOW_DOMAIN_ID", None),
            ("MITIFLOW_DOMAIN_NAMESPACE", None),
            ("MITIFLOW_TRANSPORT_PROFILE", None),
            ("MITIFLOW_TRANSPORT_CONNECT", None),
            ("MITIFLOW_KEY_PREFIX", None),
            ("MITIFLOW_DATA_DIR", None),
            ("MITIFLOW_LAG_INTERVAL_MS", None),
            ("MITIFLOW_HTTP_BIND", None),
            ("MITIFLOW_BOOTSTRAP_TOPICS_FROM", None),
        ]);

        let yaml = r#"
data_dir: /tmp/legacy-orch
lag_interval_ms: 500
"#;

        let cfg = OrchestratorYamlConfig::from_yaml(yaml).unwrap();
        assert_eq!(cfg.domain, DomainYamlConfig::default());
        assert_eq!(cfg.transport, TransportYamlConfig::default());
        let runtime = DomainRuntimeConfig::from_sources(
            "orchestrator-default",
            Some(&cfg.domain),
            Some(&cfg.transport),
        )
        .unwrap();
        assert_eq!(runtime.id, "orchestrator-default");
        assert_eq!(runtime.namespace, None);
        assert_eq!(runtime.transport, TransportProfile::LocalIsolated);

        let orch = cfg
            .into_orch_config("mitiflow/orchestrator-default")
            .unwrap();
        assert_eq!(orch.key_prefix, "mitiflow/orchestrator-default");
        assert_eq!(orch.data_dir, PathBuf::from("/tmp/legacy-orch"));
        assert_eq!(orch.lag_interval, Duration::from_millis(500));
    }

    #[test]
    fn orchestrator_env_overrides_yaml_domain_transport_and_config() {
        let _guard = EnvGuard::set(&[
            ("MITIFLOW_DOMAIN_ID", Some("env-orch")),
            ("MITIFLOW_DOMAIN_NAMESPACE", Some("env/orch")),
            ("MITIFLOW_TRANSPORT_PROFILE", Some("peer_mesh")),
            ("MITIFLOW_TRANSPORT_CONNECT", Some("tcp/env-router:7447")),
            ("MITIFLOW_KEY_PREFIX", Some("env-prefix")),
            ("MITIFLOW_DATA_DIR", Some("/tmp/env-orch-data")),
            ("MITIFLOW_LAG_INTERVAL_MS", Some("750")),
            ("MITIFLOW_HTTP_BIND", Some("127.0.0.1:19090")),
            (
                "MITIFLOW_BOOTSTRAP_TOPICS_FROM",
                Some("/tmp/env-topics.yaml"),
            ),
        ]);

        let yaml = r#"
domain:
  id: yaml-orch
  namespace: yaml/orch
transport:
  profile: local-isolated
key_prefix: yaml-prefix
data_dir: /tmp/yaml-orch-data
lag_interval_ms: 1000
http_bind: 127.0.0.1:18080
bootstrap_topics_from: /tmp/yaml-topics.yaml
"#;

        let cfg = OrchestratorYamlConfig::from_yaml(yaml).unwrap();
        let runtime = DomainRuntimeConfig::from_sources(
            "orchestrator-default",
            Some(&cfg.domain),
            Some(&cfg.transport),
        )
        .unwrap();
        assert_eq!(runtime.id, "env-orch");
        assert_eq!(runtime.namespace.as_deref(), Some("env/orch"));
        assert_eq!(
            runtime.transport,
            TransportProfile::PeerMesh {
                connect: vec!["tcp/env-router:7447".into()]
            }
        );

        let orch = cfg
            .into_orch_config("mitiflow/orchestrator-default")
            .unwrap();
        assert_eq!(orch.key_prefix, "env-prefix");
        assert_eq!(orch.data_dir, PathBuf::from("/tmp/env-orch-data"));
        assert_eq!(orch.lag_interval, Duration::from_millis(750));
        assert_eq!(
            orch.http_bind.map(|addr| addr.to_string()).as_deref(),
            Some("127.0.0.1:19090")
        );
        assert_eq!(
            orch.bootstrap_topics_from,
            Some(PathBuf::from("/tmp/env-topics.yaml"))
        );
    }

    #[test]
    fn orchestrator_peer_mesh_requires_connect() {
        let _guard = EnvGuard::set(&[
            ("MITIFLOW_TRANSPORT_PROFILE", None),
            ("MITIFLOW_TRANSPORT_CONNECT", None),
        ]);

        let yaml = r#"
transport:
  profile: peer-mesh
  connect: []
"#;

        let cfg = OrchestratorYamlConfig::from_yaml(yaml).unwrap();
        let err = DomainRuntimeConfig::from_sources(
            "orchestrator-default",
            Some(&cfg.domain),
            Some(&cfg.transport),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("peer-mesh") && msg.contains("requires at least one endpoint"),
            "error: {msg}"
        );
    }

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct EnvGuard {
        saved: Vec<(&'static str, Option<String>)>,
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl EnvGuard {
        fn set(vars: &[(&'static str, Option<&str>)]) -> Self {
            let lock = ENV_LOCK.lock().unwrap();
            let saved = vars
                .iter()
                .map(|(name, _)| (*name, std::env::var(name).ok()))
                .collect();
            for (name, value) in vars {
                match value {
                    Some(value) => unsafe { std::env::set_var(name, value) },
                    None => unsafe { std::env::remove_var(name) },
                }
            }
            Self { saved, _lock: lock }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (name, value) in self.saved.drain(..) {
                match value {
                    Some(value) => unsafe { std::env::set_var(name, value) },
                    None => unsafe { std::env::remove_var(name) },
                }
            }
        }
    }
}
