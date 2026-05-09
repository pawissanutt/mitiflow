//! E2E chaos test for `EventReplayer`.
//!
//! Phase 1: Run the emulator binary with a producer + storage_agent topology
//!          and a scheduled chaos event (kill + restart store-1 at t=4s).
//!          The producer writes a JSONL published-events manifest.
//!
//! Phase 2: Spawn a fresh `mitiflow-emulator-storage-agent` process pointing
//!          at the same on-disk data directory so `EventReplayer` has a live
//!          store to query against.
//!
//! Phase 3: Open an in-process Zenoh client session and run `EventReplayer`
//!          with `ReplayScope::All` + `ReplayEnd::Bounded` to drain every
//!          stored event.
//!
//! Phase 4: Assert ≥ 70% of published events were replayed and that no
//!          duplicate event IDs appear in the replay output.

use std::collections::HashSet;
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use mitiflow::codec::CodecFormat;
use mitiflow::config::EventBusConfig;
use mitiflow::{Error, EventReplayer, MitiflowDomain, ReplayEnd, TransportProfile};
use mitiflow_emulator::config::{CodecConfig, RecoveryModeConfig};
use mitiflow_emulator::metrics::{EventManifestEntry, ManifestRole};
use mitiflow_emulator::role_config::{StorageAgentRoleConfig, ZenohRoleConfig, encode_config};

const NUM_PARTITIONS: u32 = 4;

/// Topology template — `{DATA_DIR}` is replaced at runtime with an absolute path.
const TOPOLOGY_TEMPLATE: &str = r#"
zenoh:
  mode: client
  connect:
    - "{CONNECT_ENDPOINT}"
  timestamping_enabled: true

manifest:
  enabled: true
  directory: "./manifest"

defaults:
  codec: json
  cache_size: 512
  heartbeat_ms: 500
  recovery_mode: both

topics:
  - name: events
    key_prefix: "{KEY_PREFIX}"
    num_partitions: 4

components:
  - name: store-1
    kind: storage_agent
    topic: events
    capacity: 50
    data_dir: "{DATA_DIR}"

  - name: prod-1
    kind: producer
    topic: events
    rate: 15.0
    payload:
      generator: counter
      prefix: rce2e

chaos:
  enabled: true
  schedule:
    - at: 4s
      action: kill
      target: store-1
      restart_after: 2s
"#;

fn topology_for(key_prefix: &str, connect_endpoint: &str, data_dir: &Path) -> String {
    TOPOLOGY_TEMPLATE
        .replace("{KEY_PREFIX}", key_prefix)
        .replace("{CONNECT_ENDPOINT}", connect_endpoint)
        .replace("{DATA_DIR}", data_dir.to_str().expect("utf8 path"))
}

fn emulator_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_mitiflow-emulator"))
}

fn storage_agent_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_mitiflow-emulator-storage-agent"))
}

fn local_ephemeral_tcp_endpoint() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve local router endpoint");
    let addr = listener.local_addr().expect("read local router endpoint");
    format!("tcp/{addr}")
}

async fn open_local_router(endpoint: &str) -> zenoh::Session {
    let mut config = zenoh::Config::default();
    config
        .insert_json5("mode", r#""router""#)
        .expect("router mode");
    config
        .insert_json5(
            "listen/endpoints",
            &serde_json::to_string(&[endpoint]).expect("serialize router endpoint"),
        )
        .expect("router listen endpoint");
    config
        .insert_json5("scouting/multicast/enabled", "false")
        .expect("disable multicast scouting");
    config
        .insert_json5("scouting/gossip/enabled", "false")
        .expect("disable gossip scouting");
    config
        .insert_json5("timestamping/enabled", "true")
        .expect("enable timestamping");

    zenoh::open(config).await.expect("open local Zenoh router")
}

fn read_published_manifest(manifest_dir: &Path) -> Vec<EventManifestEntry> {
    let path = manifest_dir.join("prod-1-0-published.jsonl");
    let content = fs::read_to_string(&path).unwrap_or_default();
    content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<EventManifestEntry>(l).ok())
        .filter(|e| e.role == ManifestRole::Published)
        .collect()
}

async fn collect_replayed_ids(
    session: &zenoh::Session,
    bus_config: &EventBusConfig,
) -> mitiflow::Result<Vec<String>> {
    let mut replayer = EventReplayer::builder(session, bus_config.clone())
        .all()
        .end(ReplayEnd::Bounded { limit: usize::MAX })
        .query_timeout(Duration::from_secs(10))
        .build()
        .await?;

    let mut replayed_ids = Vec::new();
    loop {
        match replayer.recv_raw().await {
            Ok(ev) => replayed_ids.push(ev.id.to_string()),
            Err(Error::EndOfReplay) => return Ok(replayed_ids),
            Err(e) => return Err(e),
        }
    }
}

async fn wait_for_replayed_ids(
    session: &zenoh::Session,
    bus_config: &EventBusConfig,
    min_expected: usize,
    timeout: Duration,
) -> (Vec<String>, Option<String>) {
    let deadline = Instant::now() + timeout;
    let mut best_ids = Vec::new();
    let mut last_error = None;

    while Instant::now() < deadline {
        match collect_replayed_ids(session, bus_config).await {
            Ok(ids) => {
                if ids.len() >= min_expected {
                    return (ids, None);
                }
                if ids.len() > best_ids.len() {
                    best_ids = ids;
                }
            }
            Err(e) => last_error = Some(e.to_string()),
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    (best_ids, last_error)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replayer_survives_storage_agent_chaos() {
    let namespace_domain = MitiflowDomain::isolated_for_test("replayer_chaos_e2e")
        .await
        .expect("open namespace domain");
    let connect_endpoint = local_ephemeral_tcp_endpoint();
    let router = open_local_router(&connect_endpoint).await;
    let replay_domain = MitiflowDomain::builder("replayer-chaos-client")
        .namespace(namespace_domain.namespace().clone())
        .transport(TransportProfile::Client {
            connect: vec![connect_endpoint.clone()],
        })
        .open()
        .await
        .expect("open replay domain");
    let bus_config = replay_domain
        .event_bus_config("events")
        .expect("event bus config")
        .codec(CodecFormat::Json)
        .cache_size(512)
        .num_partitions(NUM_PARTITIONS)
        .build()
        .expect("build EventBusConfig");
    let key_prefix = bus_config.key_prefix.clone();

    let tmp = tempfile::tempdir().expect("create temp dir");
    let work_dir = tmp.path().to_path_buf();
    let manifest_dir = work_dir.join("manifest");
    let data_dir = work_dir.join("store");
    fs::create_dir_all(&manifest_dir).expect("create manifest dir");
    fs::create_dir_all(&data_dir).expect("create data dir");

    // --- Phase 1: emulator run with producer + storage_agent + chaos ---
    let topology = topology_for(&key_prefix, &connect_endpoint, &data_dir);
    let topo_path = work_dir.join("topology.yaml");
    fs::write(&topo_path, &topology).expect("write topology");

    let output = std::process::Command::new(emulator_bin())
        .args([
            "run",
            topo_path.to_str().expect("utf8 path"),
            "--seed",
            "42",
            "--duration",
            "14",
        ])
        .current_dir(&work_dir)
        .output()
        .expect("run emulator binary");

    assert!(
        output.status.success(),
        "emulator exited non-zero\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let published = read_published_manifest(&manifest_dir);
    assert!(
        !published.is_empty(),
        "producer published no events — check producer binary and topology"
    );

    // --- Phase 2: spawn a fresh storage agent against the persisted data dir ---
    // Instance data dir follows supervisor convention: {data_dir}/{name}-{instance_index}
    let instance_data_dir = data_dir.join("store-1-0");
    assert!(
        instance_data_dir.exists(),
        "instance data dir not found at {instance_data_dir:?} — storage agent may not have written data"
    );

    let zenoh_b64 = encode_config(&ZenohRoleConfig {
        mode: "client".to_string(),
        listen: vec![],
        connect: vec![connect_endpoint],
        timestamping_enabled: true,
    })
    .expect("encode ZenohRoleConfig");

    let store_b64 = encode_config(&StorageAgentRoleConfig {
        key_prefix,
        data_dir: instance_data_dir,
        num_partitions: NUM_PARTITIONS,
        replication_factor: 1,
        capacity: 50,
        node_id: Some("store-1-0".to_string()),
        codec: CodecConfig::Json,
        cache_size: 512,
        heartbeat_ms: 500,
        recovery_mode: RecoveryModeConfig::Both,
        log_level: None,
    })
    .expect("encode StorageAgentRoleConfig");

    let mut storage_agent = std::process::Command::new(storage_agent_bin())
        .env("MITIFLOW_ZENOH_CONFIG", &zenoh_b64)
        .env("MITIFLOW_EMU_CONFIG", &store_b64)
        .env("RUST_LOG", "warn")
        .spawn()
        .expect("spawn fresh storage agent");

    // Give the storage agent time to open its Zenoh session and register queryables.
    tokio::time::sleep(Duration::from_millis(900)).await;

    // --- Phase 3: in-process EventReplayer ---
    let published_count = published.len();
    let min_expected = published_count * 7 / 10;
    let (replayed_ids, last_replay_error) = wait_for_replayed_ids(
        replay_domain.session(),
        &bus_config,
        min_expected,
        Duration::from_secs(30),
    )
    .await;

    // --- Phase 4: cleanup and assertions ---
    storage_agent.kill().ok();
    storage_agent.wait().ok();
    replay_domain.shutdown().await.expect("close replay domain");
    namespace_domain
        .shutdown()
        .await
        .expect("close namespace domain");
    router.close().await.expect("close local router");

    let replayed_count = replayed_ids.len();

    assert!(
        replayed_count >= min_expected,
        "replayed too few events: got {replayed_count} of {published_count} published (need ≥ {min_expected} = 70%); last replay error: {}",
        last_replay_error.as_deref().unwrap_or("none")
    );

    let unique: HashSet<&String> = replayed_ids.iter().collect();
    assert_eq!(
        unique.len(),
        replayed_count,
        "replay contained duplicates: {replayed_count} total, {} unique",
        unique.len()
    );
}
