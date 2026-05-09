use std::net::TcpListener;

use crate::Result;
use crate::domain::DomainError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportProfile {
    LocalIsolated,
    Client { connect: Vec<String> },
    PeerMesh { connect: Vec<String> },
    Ambient,
}

impl TransportProfile {
    pub fn to_zenoh_config(&self) -> Result<zenoh::Config> {
        match self {
            TransportProfile::LocalIsolated => local_isolated_config(),
            TransportProfile::Client { connect } => client_config(connect),
            TransportProfile::PeerMesh { connect } => peer_mesh_config(connect),
            TransportProfile::Ambient => ambient_config(),
        }
    }
}

fn local_isolated_config() -> Result<zenoh::Config> {
    let mut config = zenoh::Config::default();
    let endpoint = local_ephemeral_tcp_endpoint()?;
    config.insert_json5("mode", r#""peer""#)?;
    config.insert_json5(r#"listen/endpoints"#, &serde_json::to_string(&[endpoint])?)?;
    config.insert_json5("scouting/multicast/enabled", "false")?;
    config.insert_json5("scouting/gossip/enabled", "false")?;
    config.insert_json5("timestamping/enabled", "true")?;
    Ok(config)
}

fn local_ephemeral_tcp_endpoint() -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|e| DomainError::Invalid(format!("failed to reserve local endpoint: {e}")))?;
    let addr = listener
        .local_addr()
        .map_err(|e| DomainError::Invalid(format!("failed to read local endpoint: {e}")))?;
    Ok(format!("tcp/{}", addr))
}

fn client_config(connect: &[String]) -> Result<zenoh::Config> {
    if connect.is_empty() {
        return Err(DomainError::EmptyEndpoints {
            profile: "Client".into(),
        }
        .into());
    }
    let mut config = zenoh::Config::default();
    config.insert_json5("mode", r#""client""#)?;
    let endpoints_json = serde_json::to_string(connect)?;
    config.insert_json5("connect/endpoints", &endpoints_json)?;
    config.insert_json5("scouting/multicast/enabled", "false")?;
    config.insert_json5("scouting/gossip/enabled", "false")?;
    config.insert_json5("timestamping/enabled", "true")?;
    Ok(config)
}

fn peer_mesh_config(connect: &[String]) -> Result<zenoh::Config> {
    if connect.is_empty() {
        return Err(DomainError::EmptyEndpoints {
            profile: "PeerMesh".into(),
        }
        .into());
    }
    let mut config = zenoh::Config::default();
    let endpoint = local_ephemeral_tcp_endpoint()?;
    config.insert_json5("mode", r#""peer""#)?;
    config.insert_json5(r#"listen/endpoints"#, &serde_json::to_string(&[endpoint])?)?;
    let endpoints_json = serde_json::to_string(connect)?;
    config.insert_json5("connect/endpoints", &endpoints_json)?;
    config.insert_json5("scouting/multicast/enabled", "false")?;
    config.insert_json5("scouting/gossip/enabled", "false")?;
    config.insert_json5("timestamping/enabled", "true")?;
    Ok(config)
}

fn ambient_config() -> Result<zenoh::Config> {
    let mut config = zenoh::Config::default();
    config.insert_json5("timestamping/enabled", "true")?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::DomainError;

    fn config_to_json(config: &zenoh::Config) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        let keys = [
            "mode",
            "listen/endpoints",
            "connect/endpoints",
            "scouting/multicast/enabled",
            "scouting/gossip/enabled",
            "timestamping/enabled",
        ];
        for key in keys {
            let Ok(val) = config.get_json(key) else {
                continue;
            };
            if let Ok(v) = serde_json::from_str(&val) {
                map.insert(key.to_string(), v);
            }
        }
        serde_json::Value::Object(map)
    }

    fn keys_in_config(config: &zenoh::Config) -> Vec<String> {
        let mut result = Vec::new();
        let keys = [
            "mode",
            "listen/endpoints",
            "connect/endpoints",
            "scouting/multicast/enabled",
            "scouting/gossip/enabled",
            "timestamping/enabled",
        ];
        for key in keys {
            if config.get_json(key).is_ok() {
                result.push(key.to_string());
            }
        }
        result
    }

    #[test]
    fn local_isolated_disables_scouting() {
        let config = TransportProfile::LocalIsolated
            .to_zenoh_config()
            .expect("LocalIsolated config must succeed");
        let json = config_to_json(&config);

        let mode = json.get("mode").and_then(|v| v.as_str()).unwrap();
        assert_eq!(mode, "peer", "mode must be 'peer'");

        let listen = json
            .get("listen/endpoints")
            .expect("listen/endpoints must exist");
        let arr = listen
            .as_array()
            .expect("listen/endpoints must be an array");
        assert!(!arr.is_empty(), "listen/endpoints must be non-empty");

        let multicast_disabled = json
            .get("scouting/multicast/enabled")
            .and_then(|v| v.as_bool())
            .unwrap();
        assert!(
            !multicast_disabled,
            "scouting/multicast/enabled must be false"
        );

        let gossip_disabled = json
            .get("scouting/gossip/enabled")
            .and_then(|v| v.as_bool())
            .unwrap();
        assert!(!gossip_disabled, "scouting/gossip/enabled must be false");

        let ts_enabled = json
            .get("timestamping/enabled")
            .and_then(|v| v.as_bool())
            .unwrap();
        assert!(ts_enabled, "timestamping/enabled must be true");
    }

    #[test]
    fn client_disables_scouting() {
        let config = TransportProfile::Client {
            connect: vec!["tcp/127.0.0.1:7447".into()],
        }
        .to_zenoh_config()
        .expect("Client config must succeed");
        let json = config_to_json(&config);

        let mode = json.get("mode").and_then(|v| v.as_str()).unwrap();
        assert_eq!(mode, "client", "mode must be 'client'");

        let endpoints = json
            .get("connect/endpoints")
            .expect("connect/endpoints must exist");
        let arr = endpoints
            .as_array()
            .expect("connect/endpoints must be an array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0].as_str().unwrap(), "tcp/127.0.0.1:7447");

        let multicast_disabled = json
            .get("scouting/multicast/enabled")
            .and_then(|v| v.as_bool())
            .unwrap();
        assert!(
            !multicast_disabled,
            "scouting/multicast/enabled must be false"
        );

        let gossip_disabled = json
            .get("scouting/gossip/enabled")
            .and_then(|v| v.as_bool())
            .unwrap();
        assert!(!gossip_disabled, "scouting/gossip/enabled must be false");

        let ts_enabled = json
            .get("timestamping/enabled")
            .and_then(|v| v.as_bool())
            .unwrap();
        assert!(ts_enabled, "timestamping/enabled must be true");
    }

    #[test]
    fn peer_mesh_disables_scouting() {
        let config = TransportProfile::PeerMesh {
            connect: vec!["tcp/10.0.0.1:7447".into(), "tcp/10.0.0.2:7447".into()],
        }
        .to_zenoh_config()
        .expect("PeerMesh config must succeed");
        let json = config_to_json(&config);

        let mode = json.get("mode").and_then(|v| v.as_str()).unwrap();
        assert_eq!(mode, "peer", "mode must be 'peer'");

        let endpoints = json
            .get("connect/endpoints")
            .expect("connect/endpoints must exist");
        let arr = endpoints
            .as_array()
            .expect("connect/endpoints must be an array");
        assert_eq!(arr.len(), 2);

        let multicast_disabled = json
            .get("scouting/multicast/enabled")
            .and_then(|v| v.as_bool())
            .unwrap();
        assert!(
            !multicast_disabled,
            "scouting/multicast/enabled must be false"
        );

        let gossip_disabled = json
            .get("scouting/gossip/enabled")
            .and_then(|v| v.as_bool())
            .unwrap();
        assert!(!gossip_disabled, "scouting/gossip/enabled must be false");

        let ts_enabled = json
            .get("timestamping/enabled")
            .and_then(|v| v.as_bool())
            .unwrap();
        assert!(ts_enabled, "timestamping/enabled must be true");
    }

    #[test]
    fn ambient_preserves_defaults() {
        let config = TransportProfile::Ambient
            .to_zenoh_config()
            .expect("Ambient config must succeed");
        let json = config_to_json(&config);

        let ts_enabled = json
            .get("timestamping/enabled")
            .and_then(|v| v.as_bool())
            .unwrap();
        assert!(ts_enabled, "timestamping/enabled must be true");

        let default = zenoh::Config::default();
        let default_keys = keys_in_config(&default);
        let config_keys = keys_in_config(&config);

        for key in ["scouting/multicast/enabled", "scouting/gossip/enabled"] {
            let in_default = default_keys.contains(&key.to_string());
            let in_config = config_keys.contains(&key.to_string());
            assert_eq!(
                in_default, in_config,
                "{key} must not be mutated by Ambient (default={in_default}, config={in_config})"
            );
        }

        let ambient_mode = json.get("mode").and_then(|v| v.as_str());
        let default_json = config_to_json(&default);
        let default_mode = default_json.get("mode").and_then(|v| v.as_str());
        assert_eq!(
            ambient_mode, default_mode,
            "Ambient mode must match Config::default() mode"
        );
    }

    #[test]
    fn client_empty_connect_errors() {
        let result = TransportProfile::Client { connect: vec![] }.to_zenoh_config();
        assert!(
            result.is_err(),
            "Client with empty connect must return error"
        );
        let err = result.unwrap_err();
        let domain_err = match err {
            crate::Error::Domain(e) => e,
            other => panic!("expected Error::Domain, got {other}"),
        };
        match domain_err {
            DomainError::EmptyEndpoints { profile } => {
                assert_eq!(profile, "Client");
            }
            other => panic!("expected DomainError::EmptyEndpoints, got {other:?}"),
        }
    }

    #[test]
    fn peer_mesh_empty_connect_errors() {
        let result = TransportProfile::PeerMesh { connect: vec![] }.to_zenoh_config();
        assert!(
            result.is_err(),
            "PeerMesh with empty connect must return error"
        );
        let err = result.unwrap_err();
        let domain_err = match err {
            crate::Error::Domain(e) => e,
            other => panic!("expected Error::Domain, got {other}"),
        };
        match domain_err {
            DomainError::EmptyEndpoints { profile } => {
                assert_eq!(profile, "PeerMesh");
            }
            other => panic!("expected DomainError::EmptyEndpoints, got {other:?}"),
        }
    }

    #[test]
    fn local_isolated_no_network_required() {
        let config = TransportProfile::LocalIsolated
            .to_zenoh_config()
            .expect("LocalIsolated must succeed without any network");
        let json = config_to_json(&config);
        assert!(json.get("mode").is_some());
        assert!(json.get("listen/endpoints").is_some());
    }

    #[test]
    fn ambient_no_network_required() {
        let config = TransportProfile::Ambient
            .to_zenoh_config()
            .expect("Ambient must succeed");
        let json = config_to_json(&config);
        assert!(json.get("timestamping/enabled").is_some());
    }

    #[test]
    fn client_multiple_endpoints() {
        let config = TransportProfile::Client {
            connect: vec![
                "tcp/10.0.0.1:7447".into(),
                "tcp/10.0.0.2:7447".into(),
                "tcp/10.0.0.3:7447".into(),
            ],
        }
        .to_zenoh_config()
        .expect("Client with multiple endpoints must succeed");
        let json = config_to_json(&config);

        let endpoints = json
            .get("connect/endpoints")
            .expect("connect/endpoints must exist");
        let arr = endpoints
            .as_array()
            .expect("connect/endpoints must be an array");
        assert_eq!(arr.len(), 3);
    }
}
