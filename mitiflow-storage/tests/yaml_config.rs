//! Tests for YAML configuration parsing.

use std::collections::HashMap;
use std::time::Duration;

use mitiflow::{DomainRuntimeConfig, DomainYamlConfig, TransportProfile, TransportYamlConfig};
use mitiflow_storage::config::{AgentYamlConfig, ClusterYamlConfig, NodeYamlConfig};

#[test]
fn parse_agent_yaml_full() {
    let yaml = r#"
node:
  id: node-42
  data_dir: /var/lib/mitiflow
  capacity: 200
  health_interval: 15s
  drain_grace_period: 1m
  labels:
    rack: us-east-1a
    tier: ssd

cluster:
  global_prefix: myapp
  auto_discover_topics: true

topics:
  - name: events
    key_prefix: myapp/events
    num_partitions: 16
    replication_factor: 2
  - name: logs
    key_prefix: myapp/logs
    num_partitions: 8
    replication_factor: 1
"#;

    let cfg = AgentYamlConfig::from_yaml(yaml).unwrap();
    assert_eq!(cfg.domain, DomainYamlConfig::default());
    assert_eq!(cfg.transport, TransportYamlConfig::default());
    assert_eq!(cfg.node.id, "node-42");
    assert_eq!(cfg.node.data_dir.to_str().unwrap(), "/var/lib/mitiflow");
    assert_eq!(cfg.node.capacity, 200);
    assert_eq!(cfg.node.health_interval, Duration::from_secs(15));
    assert_eq!(cfg.node.drain_grace_period, Duration::from_secs(60));
    assert_eq!(cfg.node.labels.get("rack").unwrap(), "us-east-1a");
    assert_eq!(cfg.node.labels.get("tier").unwrap(), "ssd");

    assert_eq!(cfg.cluster.global_prefix, "myapp");
    assert!(cfg.cluster.auto_discover_topics);

    assert_eq!(cfg.topics.len(), 2);
    assert_eq!(cfg.topics[0].name, "events");
    assert_eq!(cfg.topics[0].key_prefix, "myapp/events");
    assert_eq!(cfg.topics[0].num_partitions, 16);
    assert_eq!(cfg.topics[0].replication_factor, 2);
    assert_eq!(cfg.topics[1].name, "logs");
    assert_eq!(cfg.topics[1].num_partitions, 8);
}

#[test]
fn parse_agent_yaml_minimal() {
    let yaml = r#"
cluster:
  auto_discover_topics: true
"#;

    let cfg = AgentYamlConfig::from_yaml(yaml).unwrap();
    assert_eq!(cfg.domain, DomainYamlConfig::default());
    assert_eq!(cfg.transport, TransportYamlConfig::default());
    // Default values
    assert_eq!(cfg.node.id, "auto");
    assert_eq!(cfg.node.data_dir.to_str().unwrap(), "/tmp/mitiflow-storage");
    assert_eq!(cfg.node.capacity, 100);
    assert_eq!(cfg.node.health_interval, Duration::from_secs(10));
    assert_eq!(cfg.node.drain_grace_period, Duration::from_secs(30));
    assert!(cfg.node.labels.is_empty());

    assert_eq!(cfg.cluster.global_prefix, "mitiflow");
    assert!(cfg.cluster.auto_discover_topics);
    assert!(cfg.topics.is_empty());
}

#[test]
fn parse_agent_yaml_static_topics() {
    let yaml = r#"
topics:
  - name: orders
    key_prefix: shop/orders
    num_partitions: 32
    replication_factor: 3
  - name: inventory
    key_prefix: shop/inventory
"#;

    let cfg = AgentYamlConfig::from_yaml(yaml).unwrap();
    assert_eq!(cfg.topics.len(), 2);
    assert_eq!(cfg.topics[0].name, "orders");
    assert_eq!(cfg.topics[0].num_partitions, 32);
    assert_eq!(cfg.topics[0].replication_factor, 3);
    assert_eq!(cfg.topics[1].name, "inventory");
    // Defaults
    assert_eq!(cfg.topics[1].num_partitions, 16);
    assert_eq!(cfg.topics[1].replication_factor, 1);
}

#[test]
fn parse_agent_yaml_auto_discover_only() {
    let yaml = r#"
cluster:
  auto_discover_topics: true
"#;

    let cfg = AgentYamlConfig::from_yaml(yaml).unwrap();
    let agent_config = cfg.into_agent_config().unwrap();
    assert!(agent_config.auto_discover_topics);
    assert!(agent_config.topics.is_empty());
}

#[test]
fn yaml_to_agent_config_converts_correctly() {
    let yaml = r#"
node:
  id: test-node
  data_dir: /data
  capacity: 50
  labels:
    zone: a

cluster:
  global_prefix: acme

topics:
  - name: events
    key_prefix: acme/events
    num_partitions: 4
    replication_factor: 2
"#;

    let cfg = AgentYamlConfig::from_yaml(yaml).unwrap();
    let agent_config = cfg.into_agent_config().unwrap();

    assert_eq!(agent_config.node_id, "test-node");
    assert_eq!(agent_config.data_dir.to_str().unwrap(), "/data");
    assert_eq!(agent_config.capacity, 50);
    assert_eq!(agent_config.global_prefix, "acme");
    assert_eq!(agent_config.labels.get("zone").unwrap(), "a");
    assert_eq!(agent_config.topics.len(), 1);
    assert_eq!(agent_config.topics[0].name, "events");
    assert_eq!(agent_config.topics[0].num_partitions, 4);
}

#[test]
fn yaml_auto_node_id_generates_uuid() {
    let yaml = r#"
cluster:
  auto_discover_topics: true
"#;

    let cfg = AgentYamlConfig::from_yaml(yaml).unwrap();
    let agent_config = cfg.into_agent_config().unwrap();
    // When id == "auto", it should generate a UUID
    assert!(!agent_config.node_id.is_empty());
    assert_ne!(agent_config.node_id, "auto");
}

#[test]
fn yaml_rejects_no_topics_and_no_discover() {
    let yaml = r#"
node:
  id: lonely-node
"#;

    let cfg = AgentYamlConfig::from_yaml(yaml).unwrap();
    let err = cfg.into_agent_config().unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("at least one topic"), "error: {msg}");
}

#[test]
fn yaml_roundtrip_serialization() {
    let cfg = AgentYamlConfig {
        domain: DomainYamlConfig {
            id: Some("roundtrip-domain".into()),
            namespace: Some("roundtrip/ns".into()),
        },
        transport: TransportYamlConfig {
            profile: Some("client".into()),
            connect: vec!["tcp/router:7447".into()],
        },
        node: NodeYamlConfig {
            id: "my-node".into(),
            data_dir: "/data".into(),
            capacity: 100,
            health_interval: Duration::from_secs(10),
            drain_grace_period: Duration::from_secs(30),
            labels: {
                let mut m = HashMap::new();
                m.insert("tier".into(), "ssd".into());
                m
            },
        },
        cluster: ClusterYamlConfig {
            global_prefix: "mitiflow".into(),
            auto_discover_topics: true,
        },
        topics: vec![],
    };

    let yaml_str = serde_yaml::to_string(&cfg).unwrap();
    let parsed: AgentYamlConfig = serde_yaml::from_str(&yaml_str).unwrap();
    assert_eq!(parsed.domain.id.as_deref(), Some("roundtrip-domain"));
    assert_eq!(parsed.domain.namespace.as_deref(), Some("roundtrip/ns"));
    assert_eq!(parsed.transport.profile.as_deref(), Some("client"));
    assert_eq!(parsed.transport.connect, vec!["tcp/router:7447"]);
    assert_eq!(parsed.node.id, "my-node");
    assert_eq!(parsed.node.labels.get("tier").unwrap(), "ssd");
    assert!(parsed.cluster.auto_discover_topics);
}

#[test]
fn parse_agent_yaml_domain_and_transport_blocks() {
    let _guard = EnvGuard::set(&[
        ("MITIFLOW_DOMAIN_ID", None),
        ("MITIFLOW_DOMAIN_NAMESPACE", None),
        ("MITIFLOW_TRANSPORT_PROFILE", None),
        ("MITIFLOW_TRANSPORT_CONNECT", None),
    ]);

    let yaml = r#"
domain:
  id: my-domain
  namespace: optional/override

transport:
  profile: peer-mesh
  connect: ["tcp/router:7447", "tcp/router2:7447"]

cluster:
  auto_discover_topics: true
"#;

    let cfg = AgentYamlConfig::from_yaml(yaml).unwrap();
    assert_eq!(cfg.domain.id.as_deref(), Some("my-domain"));
    assert_eq!(cfg.domain.namespace.as_deref(), Some("optional/override"));
    assert_eq!(cfg.transport.profile.as_deref(), Some("peer-mesh"));
    assert_eq!(cfg.transport.connect.len(), 2);

    let runtime = DomainRuntimeConfig::from_sources(
        "storage-default",
        Some(&cfg.domain),
        Some(&cfg.transport),
    )
    .unwrap();
    assert_eq!(runtime.id, "my-domain");
    assert_eq!(runtime.namespace.as_deref(), Some("optional/override"));
    assert_eq!(
        runtime.transport,
        TransportProfile::PeerMesh {
            connect: vec!["tcp/router:7447".into(), "tcp/router2:7447".into()]
        }
    );
}

#[test]
fn transport_client_requires_non_empty_connect() {
    let _guard = EnvGuard::set(&[
        ("MITIFLOW_TRANSPORT_PROFILE", None),
        ("MITIFLOW_TRANSPORT_CONNECT", None),
    ]);

    let yaml = r#"
domain:
  id: bad-client
transport:
  profile: client
  connect: []
cluster:
  auto_discover_topics: true
"#;

    let cfg = AgentYamlConfig::from_yaml(yaml).unwrap();
    let err = DomainRuntimeConfig::from_sources(
        "storage-default",
        Some(&cfg.domain),
        Some(&cfg.transport),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("client") && msg.contains("requires at least one endpoint"),
        "error: {msg}"
    );
}

#[test]
fn transport_env_overrides_yaml() {
    let _guard = EnvGuard::set(&[
        ("MITIFLOW_DOMAIN_ID", Some("env-domain")),
        ("MITIFLOW_DOMAIN_NAMESPACE", Some("env/ns")),
        ("MITIFLOW_TRANSPORT_PROFILE", Some("client")),
        (
            "MITIFLOW_TRANSPORT_CONNECT",
            Some("tcp/env-router:7447,tcp/env-router2:7447"),
        ),
    ]);

    let yaml = r#"
domain:
  id: yaml-domain
  namespace: yaml/ns
transport:
  profile: local-isolated
  connect: ["tcp/yaml-router:7447"]
cluster:
  auto_discover_topics: true
"#;

    let cfg = AgentYamlConfig::from_yaml(yaml).unwrap();
    let runtime = DomainRuntimeConfig::from_sources(
        "storage-default",
        Some(&cfg.domain),
        Some(&cfg.transport),
    )
    .unwrap();

    assert_eq!(runtime.id, "env-domain");
    assert_eq!(runtime.namespace.as_deref(), Some("env/ns"));
    assert_eq!(
        runtime.transport,
        TransportProfile::Client {
            connect: vec!["tcp/env-router:7447".into(), "tcp/env-router2:7447".into()]
        }
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

#[test]
fn parse_agent_yaml_humantime_durations() {
    let yaml = r#"
node:
  health_interval: 500ms
  drain_grace_period: 2m 30s

cluster:
  auto_discover_topics: true
"#;

    let cfg = AgentYamlConfig::from_yaml(yaml).unwrap();
    assert_eq!(cfg.node.health_interval, Duration::from_millis(500));
    assert_eq!(cfg.node.drain_grace_period, Duration::from_secs(150));
}
