//! Shared test helpers for mitiflow integration tests.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use mitiflow::{
    Event, EventBusConfig, EventPublisher, EventSubscriber, HeartbeatMode, MitiflowDomain,
};

/// Common test payload.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TestPayload {
    pub value: u64,
}

#[allow(dead_code)]
/// Open a `MitiflowDomain` isolated for the given test name.
///
/// Each call returns a domain with a unique id (UUID-suffixed) using the
/// `LocalIsolated` transport profile, so concurrent tests cannot reach each
/// other over Zenoh even when they share key prefixes.
pub async fn isolated_domain(test_name: &str) -> MitiflowDomain {
    MitiflowDomain::isolated_for_test(test_name)
        .await
        .expect("isolated domain should open")
}

#[allow(dead_code)]
/// Build an `EventBusConfig` rooted in the given domain's namespace.
///
/// The derived key prefix is `"{namespace_root}/topics/events"`.
pub fn test_config(domain: &MitiflowDomain) -> EventBusConfig {
    domain
        .event_bus_config("events")
        .expect("topic 'events' is valid")
        .cache_size(1000)
        .heartbeat(HeartbeatMode::Periodic(Duration::from_millis(200)))
        .history_on_subscribe(false)
        .build()
        .expect("valid config")
}

#[allow(dead_code)]
/// Open an isolated `MitiflowDomain` and create a connected publisher +
/// subscriber pair on its session.
///
/// Returns `(domain, publisher, subscriber)`. Includes a 100ms settle delay.
///
/// Callers must `drop` the publisher and subscriber **before** calling
/// `domain.shutdown().await` to avoid Zenoh close timeouts.
pub async fn setup_pubsub(test_name: &str) -> (MitiflowDomain, EventPublisher, EventSubscriber) {
    let domain = isolated_domain(test_name).await;
    let config = test_config(&domain);
    let subscriber = EventSubscriber::new(domain.session(), config.clone())
        .await
        .unwrap();
    let publisher = EventPublisher::new(domain.session(), config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    (domain, publisher, subscriber)
}

#[allow(dead_code)]
/// Publish `count` `TestPayload` events with sequential values `0..count`.
pub async fn publish_n(publisher: &EventPublisher, count: u64) {
    for i in 0..count {
        publisher
            .publish(&Event::new(TestPayload { value: i }))
            .await
            .unwrap();
    }
}

#[allow(dead_code)]
/// Receive `count` typed events with a 5-second per-event timeout.
pub async fn recv_n(subscriber: &EventSubscriber, count: u64) -> Vec<Event<TestPayload>> {
    let mut events = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let event: Event<TestPayload> =
            tokio::time::timeout(Duration::from_secs(5), subscriber.recv())
                .await
                .expect("timed out waiting for event")
                .expect("recv failed");
        events.push(event);
    }
    events
}

#[allow(dead_code)]
/// Create a temporary directory with a prefix based on the test name.
pub fn temp_dir(name: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("mitiflow_test_{name}_"))
        .tempdir()
        .expect("failed to create temp dir")
}
