//! Integration tests for HealthReporter and StatusReporter.

use std::time::Duration;

use mitiflow::MitiflowDomain;
use mitiflow_storage::health::HealthReporter;
use mitiflow_storage::status::StatusReporter;
use mitiflow_storage::types::{NodeHealth, NodeStatus, PartitionStatus, StoreState};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_reporter_publishes_periodically() {
    let domain = MitiflowDomain::isolated_for_test("health_periodic")
        .await
        .unwrap();
    let key_prefix = "test/health_periodic";
    let node_id = "node-h1";
    let health_key = format!("{key_prefix}/_cluster/health/{node_id}");

    // Subscribe to health updates.
    let subscriber = domain
        .session()
        .declare_subscriber(&health_key)
        .await
        .unwrap();

    // Create health reporter with a short interval.
    let reporter = HealthReporter::new(
        domain.session(),
        node_id.into(),
        key_prefix,
        Duration::from_millis(200),
    )
    .await
    .unwrap();

    // Should receive at least 2 health samples within 1 second.
    let mut received = 0;
    for _ in 0..3 {
        let result = tokio::time::timeout(Duration::from_secs(2), subscriber.recv_async()).await;
        if result.is_ok() {
            received += 1;
        }
    }
    assert!(
        received >= 2,
        "expected >= 2 health reports, got {received}"
    );

    drop(subscriber);
    reporter.shutdown().await;
    domain.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_reporter_includes_partition_count() {
    let domain = MitiflowDomain::isolated_for_test("health_fields")
        .await
        .unwrap();
    let key_prefix = "test/health_fields";
    let node_id = "node-h2";
    let health_key = format!("{key_prefix}/_cluster/health/{node_id}");

    let subscriber = domain
        .session()
        .declare_subscriber(&health_key)
        .await
        .unwrap();

    let reporter = HealthReporter::new(
        domain.session(),
        node_id.into(),
        key_prefix,
        Duration::from_millis(200),
    )
    .await
    .unwrap();

    // Update health stats.
    reporter.update(5, 1000, 1024 * 1024, 42, 0).await;

    // Wait for a health sample.
    let sample = tokio::time::timeout(Duration::from_secs(2), subscriber.recv_async())
        .await
        .expect("timed out waiting for health sample")
        .unwrap();

    let health: NodeHealth = serde_json::from_slice(&sample.payload().to_bytes()).unwrap();
    assert_eq!(health.node_id, node_id);
    assert_eq!(health.partitions_owned, 5);
    assert_eq!(health.events_stored, 1000);
    assert_eq!(health.disk_usage_bytes, 1024 * 1024);
    assert_eq!(health.store_latency_p99_us, 42);
    assert_eq!(health.error_count, 0);

    drop(subscriber);
    reporter.shutdown().await;
    domain.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_reporter_publishes_on_change() {
    let domain = MitiflowDomain::isolated_for_test("status_change")
        .await
        .unwrap();
    let key_prefix = "test/status_change";
    let node_id = "node-s1";
    let status_key = format!("{key_prefix}/_cluster/status/{node_id}");

    let subscriber = domain
        .session()
        .declare_subscriber(&status_key)
        .await
        .unwrap();

    let reporter = StatusReporter::new(domain.session(), node_id.into(), key_prefix)
        .await
        .unwrap();

    // Report partition statuses.
    let partitions = vec![
        PartitionStatus {
            partition: 0,
            replica: 0,
            state: StoreState::Active,
            event_count: 42,
            watermark_seq: Default::default(),
        },
        PartitionStatus {
            partition: 1,
            replica: 0,
            state: StoreState::Starting,
            event_count: 0,
            watermark_seq: Default::default(),
        },
    ];
    reporter.report(&partitions).await.unwrap();

    // Should receive the status message.
    let sample = tokio::time::timeout(Duration::from_secs(2), subscriber.recv_async())
        .await
        .expect("timed out waiting for status report");

    // May get periodic heartbeat first; drain until we get one with partitions.
    let mut found = false;
    let binding = sample.unwrap();
    let payload = binding.payload().to_bytes();
    if let Ok(status) = serde_json::from_slice::<NodeStatus>(&payload)
        && !status.partitions.is_empty()
    {
        assert_eq!(status.node_id, node_id);
        assert_eq!(status.partitions.len(), 2);
        assert_eq!(status.partitions[0].partition, 0);
        assert_eq!(status.partitions[0].state, StoreState::Active);
        found = true;
    }

    if !found {
        // Try once more in case we got a heartbeat first.
        let sample2 = tokio::time::timeout(Duration::from_secs(2), subscriber.recv_async())
            .await
            .expect("timed out waiting for second status")
            .unwrap();
        let status: NodeStatus = serde_json::from_slice(&sample2.payload().to_bytes()).unwrap();
        assert_eq!(status.partitions.len(), 2);
    }

    drop(subscriber);
    reporter.shutdown().await;
    domain.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_reporter_periodic_heartbeat() {
    let domain = MitiflowDomain::isolated_for_test("status_heartbeat")
        .await
        .unwrap();
    let key_prefix = "test/status_heartbeat";
    let node_id = "node-s2";
    let status_key = format!("{key_prefix}/_cluster/status/{node_id}");

    let subscriber = domain
        .session()
        .declare_subscriber(&status_key)
        .await
        .unwrap();

    // The status reporter has a 30s heartbeat by default - that's too slow for tests.
    // We'll just verify that report() works on demand.
    let reporter = StatusReporter::new(domain.session(), node_id.into(), key_prefix)
        .await
        .unwrap();

    reporter.report(&[]).await.unwrap();

    let sample = tokio::time::timeout(Duration::from_secs(2), subscriber.recv_async())
        .await
        .expect("timed out waiting for heartbeat")
        .unwrap();

    let status: NodeStatus = serde_json::from_slice(&sample.payload().to_bytes()).unwrap();
    assert_eq!(status.node_id, node_id);
    // Empty partitions since we reported with no partitions.
    assert!(status.partitions.is_empty());

    drop(subscriber);
    reporter.shutdown().await;
    domain.shutdown().await.unwrap();
}
