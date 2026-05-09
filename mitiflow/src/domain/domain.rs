//! MitiflowDomain — top-level domain container tying id, namespace, transport, and Zenoh session.

use std::time::Duration;

use tracing::instrument::WithSubscriber;
use zenoh::Session;

use crate::config::{EventBusConfig, EventBusConfigBuilder};
use crate::domain::domain_id::DomainId;
use crate::domain::namespace::Namespace;
use crate::domain::transport::TransportProfile;
use crate::error::{Error, Result};

pub struct MitiflowDomain {
    id: DomainId,
    namespace: Namespace,
    transport: TransportProfile,
    session: Session,
    local_endpoint: Option<String>,
}

impl MitiflowDomain {
    // ------------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------------

    /// Start building a `MitiflowDomain` with the given domain id.
    pub fn builder(id: impl Into<String>) -> MitiflowDomainBuilder {
        MitiflowDomainBuilder::new(id)
    }

    /// Create a domain isolated for testing — unique id, LocalIsolated transport,
    /// no router required.
    pub async fn isolated_for_test(test_name: &str) -> Result<Self> {
        let sanitized = sanitize_test_name(test_name);
        let unique_id = format!("{}-{}", sanitized, uuid::Uuid::new_v4().simple());
        MitiflowDomainBuilder::new(&unique_id)
            .transport(TransportProfile::LocalIsolated)
            .open()
            .await
    }

    pub async fn join_isolated(&self) -> Result<MitiflowDomain> {
        let endpoint = self.local_endpoint.as_ref().ok_or_else(|| {
            Error::Domain(crate::domain::DomainError::Invalid(
                "parent domain has no local endpoint (Client/Ambient transport)".into(),
            ))
        })?;

        let parent_component = bounded_domain_component(self.id.as_str(), 24);
        let child_id = format!(
            "{}-child-{}",
            parent_component,
            uuid::Uuid::new_v4().simple()
        );

        MitiflowDomainBuilder::new(&child_id)
            .namespace(self.namespace.clone())
            .transport(TransportProfile::Client {
                connect: vec![endpoint.clone()],
            })
            .open()
            .await
    }

    // ------------------------------------------------------------------------
    // Accessors
    // ------------------------------------------------------------------------

    /// Reference to the underlying Zenoh session.
    pub fn session(&self) -> &Session {
        &self.session
    }

    /// Domain identifier.
    pub fn id(&self) -> &DomainId {
        &self.id
    }

    /// Domain namespace.
    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    /// Transport profile used to open this domain.
    pub fn transport(&self) -> &TransportProfile {
        &self.transport
    }

    /// Build an [`EventBusConfig`] rooted at `topic` within this domain's namespace.
    ///
    /// The derived key expression is `"{namespace_root}/topics/{topic}"`.
    ///
    /// Returns a builder; validation errors are returned at `build()` time.
    /// Returns `Err` immediately if `topic` contains invalid Zenoh characters.
    pub fn event_bus_config(&self, topic: &str) -> Result<EventBusConfigBuilder> {
        // Validate via namespace.derive() — returns Result<String>
        let prefix = self.namespace.derive(&format!("topics/{}", topic))?;
        Ok(EventBusConfig::builder(prefix))
    }

    /// Returns the resolved local listen endpoint for this domain, if available.
    ///
    /// Returns `Some` for `LocalIsolated` and `PeerMesh` transports after the
    /// session has been opened with a port-assignment listen address.
    /// Returns `None` for `Client` and `Ambient` transports (no local bind).
    pub fn local_endpoint(&self) -> Option<String> {
        self.local_endpoint.clone()
    }

    // ------------------------------------------------------------------------
    // Lifecycle
    // ------------------------------------------------------------------------

    /// Gracefully close the Zenoh session with a 5-second timeout.
    ///
    /// If the session does not close within the timeout, returns an error
    /// rather than panicking.
    pub async fn shutdown(self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), self.session.close())
            .await
            .map_err(|_| {
                Error::Domain(crate::domain::DomainError::Invalid(
                    "shutdown timed out after 5s".into(),
                ))
            })?
            .map_err(Error::from)
    }
}

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

/// Builder for [`MitiflowDomain`].
#[derive(Debug)]
pub struct MitiflowDomainBuilder {
    id: Result<DomainId>,
    namespace: Option<Namespace>,
    transport: TransportProfile,
}

impl MitiflowDomainBuilder {
    fn new(id: impl Into<String>) -> Self {
        Self {
            id: DomainId::new(id.into()),
            namespace: None,
            transport: TransportProfile::LocalIsolated,
        }
    }

    /// Set the domain namespace (default: `Namespace::new(&id)`).
    pub fn namespace(mut self, ns: Namespace) -> Self {
        self.namespace = Some(ns);
        self
    }

    /// Set the transport profile (default: `LocalIsolated`).
    pub fn transport(mut self, transport: TransportProfile) -> Self {
        self.transport = transport;
        self
    }

    /// Open the Zenoh session and resolve the domain.
    ///
    /// Default: `LocalIsolated` transport, `Namespace::new(&id)` namespace.
    pub async fn open(self) -> Result<MitiflowDomain> {
        let id = self.id?;
        let ns = match self.namespace {
            Some(namespace) => namespace,
            None => Namespace::new(&id)?,
        };

        let config = self.transport.to_zenoh_config()?;
        let local_endpoint = resolve_local_endpoint(&config, &self.transport);
        let observability = StartupObservability::from_config(&config);
        let session = zenoh::open(config).await?;
        log_successful_open(&id, &ns, &self.transport, &observability);
        if matches!(self.transport, TransportProfile::Ambient) {
            tracing::warn!(
                domain_id = %id.as_str(),
                namespace.root = %ns.root(),
                transport.profile = %transport_profile_name(&self.transport),
                "Ambient transport enabled: transport.profile=Ambient domain_id={} namespace.root={} ambient discovery may join existing Zenoh routers/peers",
                id.as_str(),
                ns.root(),
            );
            spawn_ambient_discovery_log(
                session.clone(),
                id.as_str().to_string(),
                ns.root().to_string(),
            );
        }

        Ok(MitiflowDomain {
            id,
            namespace: ns,
            transport: self.transport,
            session,
            local_endpoint,
        })
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

struct StartupObservability {
    listen_endpoints: Vec<String>,
    connect_endpoints: Vec<String>,
    scouting_multicast: String,
    scouting_gossip: String,
}

impl StartupObservability {
    fn from_config(config: &zenoh::Config) -> Self {
        Self {
            listen_endpoints: config_string_array(config, "listen/endpoints"),
            connect_endpoints: config_string_array(config, "connect/endpoints"),
            scouting_multicast: config_json_or_default(config, "scouting/multicast/enabled"),
            scouting_gossip: config_json_or_default(config, "scouting/gossip/enabled"),
        }
    }
}

fn log_successful_open(
    id: &DomainId,
    ns: &Namespace,
    transport: &TransportProfile,
    observability: &StartupObservability,
) {
    let profile = transport_profile_name(transport);
    let listen = format_string_list(&observability.listen_endpoints);
    let connect = format_string_list(&observability.connect_endpoints);

    tracing::info!(
        domain_id = %id.as_str(),
        namespace.root = %ns.root(),
        transport.profile = %profile,
        listen.endpoints = %listen,
        connect.endpoints = %connect,
        scouting.multicast = %observability.scouting_multicast,
        scouting.gossip = %observability.scouting_gossip,
        "MitiflowDomain opened domain_id={} namespace.root={} transport.profile={} listen.endpoints={} connect.endpoints={} scouting.multicast={} scouting.gossip={}",
        id.as_str(),
        ns.root(),
        profile,
        listen,
        connect,
        observability.scouting_multicast,
        observability.scouting_gossip,
    );
}

fn spawn_ambient_discovery_log(session: Session, domain_id: String, namespace_root: String) {
    let task = async move {
        tokio::time::sleep(Duration::from_millis(100)).await;

        let info = session.info();
        let zid = info.zid().await.to_string();
        let routers: Vec<String> = info
            .routers_zid()
            .await
            .map(|zid| zid.to_string())
            .collect();
        let peers: Vec<String> = info.peers_zid().await.map(|zid| zid.to_string()).collect();
        let routers = format_string_list(&routers);
        let peers = format_string_list(&peers);

        tracing::info!(
            domain_id = %domain_id,
            namespace.root = %namespace_root,
            transport.profile = %"Ambient",
            zenoh.zid = %zid,
            zenoh.routers = %routers,
            zenoh.peers = %peers,
            "Ambient discovery state domain_id={} namespace.root={} transport.profile=Ambient zid={} routers={} peers={}",
            domain_id,
            namespace_root,
            zid,
            routers,
            peers,
        );
    };

    tokio::spawn(task.with_current_subscriber());
}

fn transport_profile_name(transport: &TransportProfile) -> &'static str {
    match transport {
        TransportProfile::LocalIsolated => "LocalIsolated",
        TransportProfile::Client { .. } => "Client",
        TransportProfile::PeerMesh { .. } => "PeerMesh",
        TransportProfile::Ambient => "Ambient",
    }
}

fn config_string_array(config: &zenoh::Config, key: &str) -> Vec<String> {
    config
        .get_json(key)
        .ok()
        .and_then(|value| serde_json::from_str(&value).ok())
        .unwrap_or_default()
}

fn config_json_or_default(config: &zenoh::Config, key: &str) -> String {
    config
        .get_json(key)
        .unwrap_or_else(|_| "default".to_string())
}

fn format_string_list(values: &[String]) -> String {
    if values.is_empty() {
        "[]".to_string()
    } else {
        format!("[{}]", values.join(","))
    }
}

/// Returns the first resolved listen endpoint for listener transports.
fn resolve_local_endpoint(config: &zenoh::Config, transport: &TransportProfile) -> Option<String> {
    match transport {
        TransportProfile::LocalIsolated | TransportProfile::PeerMesh { .. } => {
            // Zenoh's stable 1.9 API does not expose resolved listen locators.
            // LocalIsolated therefore reserves a concrete ephemeral localhost
            // port before open and records that configured listen endpoint.
            let ep_json = config.get_json("listen/endpoints").ok()?;
            let eps: Vec<String> = serde_json::from_str(&ep_json).ok()?;
            eps.into_iter().next()
        }
        TransportProfile::Client { .. } | TransportProfile::Ambient => None,
    }
}

/// Sanitize a test name into a valid domain-id component.
///
/// Converts invalid characters to `-`, trims separators, and returns `"test"`
/// if the result is empty.
fn sanitize_test_name(name: &str) -> String {
    let mut result = String::new();
    let mut last_dash = false;

    for c in name.chars() {
        if c.is_alphanumeric() {
            result.push(c);
            last_dash = false;
        } else if !last_dash && !result.is_empty() {
            result.push('-');
            last_dash = true;
        }

        if result.chars().count() >= 31 {
            break;
        }
    }

    bounded_domain_component(&result, 31)
}

fn bounded_domain_component(value: &str, max_chars: usize) -> String {
    let mut result = String::new();
    let mut last_dash = false;

    for c in value.chars() {
        if c.is_alphanumeric() {
            result.push(c);
            last_dash = false;
        } else if !last_dash && !result.is_empty() {
            result.push('-');
            last_dash = true;
        }

        if result.chars().count() >= max_chars {
            break;
        }
    }

    while result.ends_with('-') {
        result.pop();
    }

    if result.is_empty() {
        "test".into()
    } else {
        result
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::io;
    use std::sync::{Arc, Mutex};

    use crate::{Event, EventPublisher, EventSubscriber};
    use tracing::instrument::WithSubscriber;

    use super::*;

    fn config_for(domain: &MitiflowDomain, topic: &str) -> crate::Result<EventBusConfig> {
        domain
            .event_bus_config(topic)?
            .cache_size(100)
            .history_on_subscribe(false)
            .build()
    }

    #[derive(Clone, Default)]
    struct LogBuffer {
        bytes: Arc<Mutex<Vec<u8>>>,
    }

    impl LogBuffer {
        fn contents(&self) -> String {
            let bytes = self.bytes.lock().expect("log buffer lock poisoned");
            String::from_utf8_lossy(&bytes).into_owned()
        }
    }

    struct LogWriter {
        bytes: Arc<Mutex<Vec<u8>>>,
    }

    impl io::Write for LogWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let mut bytes = self.bytes.lock().expect("log buffer lock poisoned");
            bytes.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuffer {
        type Writer = LogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            LogWriter {
                bytes: Arc::clone(&self.bytes),
            }
        }
    }

    async fn capture_logs<F, T>(future: F) -> (T, String)
    where
        F: Future<Output = T>,
    {
        let buffer = LogBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_ansi(false)
            .without_time()
            .with_writer(buffer.clone())
            .finish();

        let output = future.with_subscriber(subscriber).await;
        (output, buffer.contents())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn open_emits_info_log() {
        let ((), logs) = capture_logs(async {
            let domain = MitiflowDomain::builder("log-observe-local")
                .open()
                .await
                .expect("open should succeed");
            domain.shutdown().await.expect("shutdown should succeed");
        })
        .await;

        assert!(logs.contains("INFO"), "expected info log, got: {logs}");
        assert!(
            logs.contains("domain_id=log-observe-local"),
            "expected domain_id field, got: {logs}"
        );
        assert!(
            logs.contains("namespace.root=mitiflow/log-observe-local"),
            "expected namespace.root field, got: {logs}"
        );
        assert!(
            logs.contains("transport.profile=LocalIsolated"),
            "expected LocalIsolated transport profile, got: {logs}"
        );
        assert!(
            logs.contains("listen.endpoints=[tcp/"),
            "expected resolved listen endpoint, got: {logs}"
        );
        assert!(
            logs.contains("connect.endpoints=[]"),
            "expected resolved connect endpoints, got: {logs}"
        );
        assert!(
            logs.contains("scouting.multicast=false") && logs.contains("scouting.gossip=false"),
            "expected scouting state, got: {logs}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ambient_emits_warning() {
        let ((), logs) = capture_logs(async {
            let domain = MitiflowDomain::builder("ambient-warn-observe")
                .transport(TransportProfile::Ambient)
                .open()
                .await
                .expect("ambient open should succeed");

            tokio::time::sleep(Duration::from_millis(300)).await;

            domain.shutdown().await.expect("shutdown should succeed");
        })
        .await;

        assert!(logs.contains("WARN"), "expected warning log, got: {logs}");
        assert!(
            logs.contains("transport.profile=Ambient"),
            "expected exact Ambient profile token, got: {logs}"
        );
        assert!(
            logs.contains("domain_id=ambient-warn-observe"),
            "expected domain_id field, got: {logs}"
        );
        assert!(
            logs.contains("namespace.root=mitiflow/ambient-warn-observe"),
            "expected namespace.root field, got: {logs}"
        );
        assert!(
            logs.contains("Ambient discovery state")
                && logs.contains("routers=")
                && logs.contains("peers="),
            "expected delayed Ambient discovery state, got: {logs}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn builder_open_default_local_isolated() {
        let domain = MitiflowDomain::builder("test-default-local")
            .open()
            .await
            .expect("open should succeed");

        assert_eq!(domain.id().as_str(), "test-default-local");
        assert_eq!(domain.transport(), &TransportProfile::LocalIsolated);

        // Namespace should default to mitiflow/{id}
        assert_eq!(domain.namespace().root(), "mitiflow/test-default-local");

        // Local endpoint should be Some for LocalIsolated
        let ep = domain.local_endpoint();
        assert!(ep.is_some(), "LocalIsolated should have a local endpoint");

        domain.shutdown().await.expect("shutdown should succeed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn isolated_for_test_unique_ids() {
        let d1 = MitiflowDomain::isolated_for_test("unique-test")
            .await
            .expect("first isolated open should succeed");
        let d2 = MitiflowDomain::isolated_for_test("unique-test")
            .await
            .expect("second isolated open should succeed");

        // Ids should be different due to UUID suffix
        assert_ne!(d1.id().as_str(), d2.id().as_str());
        assert!(d1.id().as_str().starts_with("unique-test-"));
        assert!(d2.id().as_str().starts_with("unique-test-"));
        assert!(d1.id().as_str().chars().count() <= 64);
        assert!(d2.id().as_str().chars().count() <= 64);

        // Both should use LocalIsolated
        assert_eq!(d1.transport(), &TransportProfile::LocalIsolated);
        assert_eq!(d2.transport(), &TransportProfile::LocalIsolated);

        // Both should have local endpoints
        assert!(d1.local_endpoint().is_some());
        assert!(d2.local_endpoint().is_some());

        d1.shutdown().await.expect("first shutdown should succeed");
        d2.shutdown().await.expect("second shutdown should succeed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn open_publish_receive_shutdown() {
        let domain = MitiflowDomain::isolated_for_test("roundtrip-test")
            .await
            .expect("isolated open should succeed");

        let config = config_for(&domain, "events").expect("config should build");

        let subscriber = EventSubscriber::new(domain.session(), config.clone())
            .await
            .expect("subscriber should be created");

        let publisher = EventPublisher::new(domain.session(), config)
            .await
            .expect("publisher should be created");

        // Give things time to settle
        tokio::time::sleep(Duration::from_millis(50)).await;

        publisher
            .publish(&Event::new("hello".to_string()))
            .await
            .expect("publish should succeed");

        let received: Event<String> =
            tokio::time::timeout(Duration::from_secs(3), subscriber.recv())
                .await
                .expect("recv should not timeout")
                .expect("recv should succeed");

        assert_eq!(received.payload, "hello");

        drop(publisher);
        drop(subscriber);
        domain.shutdown().await.expect("shutdown should succeed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_isolated_domains_dont_cross() {
        let shared_namespace = Namespace::from_root("mitiflow/shared-isolation")
            .expect("shared namespace should be valid");

        let domain_a = MitiflowDomain::builder("domain-a-isolated")
            .namespace(shared_namespace.clone())
            .transport(TransportProfile::LocalIsolated)
            .open()
            .await
            .expect("domain A open should succeed");
        let domain_b = MitiflowDomain::builder("domain-b-isolated")
            .namespace(shared_namespace)
            .transport(TransportProfile::LocalIsolated)
            .open()
            .await
            .expect("domain B open should succeed");

        let config_a = config_for(&domain_a, "events").expect("config A should build");
        let config_b = config_for(&domain_b, "events").expect("config B should build");
        assert_eq!(domain_a.namespace().root(), domain_b.namespace().root());
        assert_eq!(config_a.key_prefix, config_b.key_prefix);
        assert_ne!(domain_a.local_endpoint(), domain_b.local_endpoint());

        let pub_a = EventPublisher::new(domain_a.session(), config_a.clone())
            .await
            .expect("pub A created");

        let sub_b = EventSubscriber::new(domain_b.session(), config_b.clone())
            .await
            .expect("sub B created");

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Publish on domain A
        pub_a
            .publish(&Event::new("from-a".to_string()))
            .await
            .expect("pub A publish should succeed");

        // Try to receive on domain B with short timeout — should get nothing
        let result = tokio::time::timeout(Duration::from_millis(500), sub_b.recv::<String>()).await;

        assert!(
            result.is_err() || result.as_ref().is_err(),
            "domain B should NOT receive events from domain A"
        );

        drop(pub_a);
        drop(sub_b);
        domain_a.shutdown().await.expect("domain A shutdown");
        domain_b.shutdown().await.expect("domain B shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn join_isolated_communicates() {
        let parent = MitiflowDomain::isolated_for_test("parent-comm")
            .await
            .expect("parent open should succeed");

        let child = parent
            .join_isolated()
            .await
            .expect("child join should succeed");

        // Child should use Client transport
        assert!(
            matches!(child.transport(), TransportProfile::Client { .. }),
            "child should use Client transport"
        );

        // Namespaces should match
        assert_eq!(child.namespace().root(), parent.namespace().root());

        let config = config_for(&parent, "events").expect("config should build");

        let child_sub = EventSubscriber::new(child.session(), config.clone())
            .await
            .expect("child subscriber created");

        let parent_pub = EventPublisher::new(parent.session(), config.clone())
            .await
            .expect("parent publisher created");

        tokio::time::sleep(Duration::from_millis(50)).await;

        parent_pub
            .publish(&Event::new("from-parent".to_string()))
            .await
            .expect("parent publish should succeed");

        let received: Event<String> =
            tokio::time::timeout(Duration::from_secs(3), child_sub.recv())
                .await
                .expect("recv should not timeout")
                .expect("recv should succeed");

        assert_eq!(received.payload, "from-parent");

        drop(parent_pub);
        drop(child_sub);
        parent
            .shutdown()
            .await
            .expect("parent shutdown should succeed");
        child
            .shutdown()
            .await
            .expect("child shutdown should succeed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_endpoint_profile_behavior() {
        // LocalIsolated — should have endpoint
        let isolated = MitiflowDomain::isolated_for_test("local-ep-isolated")
            .await
            .expect("isolated open should succeed");
        assert!(
            isolated.local_endpoint().is_some(),
            "LocalIsolated should have local endpoint"
        );

        // Client to isolated endpoint — local_endpoint returns None
        let isolated_ep = isolated.local_endpoint().unwrap();
        let client = MitiflowDomainBuilder::new("client-ep-test")
            .transport(TransportProfile::Client {
                connect: vec![isolated_ep],
            })
            .open()
            .await
            .expect("client open should succeed");
        assert!(
            client.local_endpoint().is_none(),
            "Client transport should not have local endpoint"
        );
        client.shutdown().await.expect("client shutdown");

        // PeerMesh is routerless peer mode with explicit peer connect endpoints,
        // and it also listens locally so peers can join this domain.
        let peer_mesh = MitiflowDomainBuilder::new("peer-mesh-ep-test")
            .transport(TransportProfile::PeerMesh {
                connect: vec![isolated.local_endpoint().unwrap()],
            })
            .open()
            .await
            .expect("peer mesh open should succeed");
        assert!(
            peer_mesh.local_endpoint().is_some(),
            "PeerMesh transport should have local endpoint"
        );
        peer_mesh.shutdown().await.expect("peer mesh shutdown");
        isolated.shutdown().await.expect("isolated shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_returns_ok_within_timeout() {
        let domain = MitiflowDomain::isolated_for_test("shutdown-ok")
            .await
            .expect("open should succeed");

        let result = domain.shutdown().await;
        result.expect("shutdown should return Ok within timeout");
    }

    #[test]
    fn sanitize_test_name_valid_cases() {
        assert_eq!(sanitize_test_name("my-test"), "my-test");
        assert_eq!(sanitize_test_name("test-123"), "test-123");
        assert_eq!(sanitize_test_name("域".to_string().as_str()), "域");
    }

    #[test]
    fn sanitize_test_name_strips_invalid() {
        assert_eq!(sanitize_test_name("foo*bar"), "foo-bar");
        assert_eq!(sanitize_test_name("foo$bar"), "foo-bar");
        assert_eq!(sanitize_test_name("_leading"), "leading");
        assert_eq!(sanitize_test_name("with space"), "with-space");
        assert_eq!(sanitize_test_name("foo*bar$baz"), "foo-bar-baz");
    }

    #[test]
    fn sanitize_test_name_collapse_underscores() {
        assert_eq!(sanitize_test_name("foo__bar"), "foo-bar");
        assert_eq!(sanitize_test_name("__leading__"), "leading");
    }

    #[test]
    fn sanitize_test_name_fallback() {
        assert_eq!(sanitize_test_name("***"), "test");
        assert_eq!(sanitize_test_name(""), "test");
        assert_eq!(sanitize_test_name("   "), "test");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn event_bus_config_derive() {
        let domain = MitiflowDomain::builder("config-derive")
            .open()
            .await
            .expect("domain should open");
        let builder = domain
            .event_bus_config("my-topic")
            .expect("topic should be valid");
        let config = builder.build().expect("build should succeed");
        assert_eq!(config.key_prefix, "mitiflow/config-derive/topics/my-topic");
        domain.shutdown().await.expect("shutdown should succeed");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn event_bus_config_rejects_invalid_topic() {
        let domain = MitiflowDomain::builder("config-reject")
            .open()
            .await
            .expect("domain should open");
        let result = domain.event_bus_config("foo*bar");
        assert!(result.is_err(), "topic with '*' should return error");
        domain.shutdown().await.expect("shutdown should succeed");
    }
}
