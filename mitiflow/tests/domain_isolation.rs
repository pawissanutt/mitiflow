//! Integration tests for Mitiflow domain transport isolation semantics.

mod common;

mod domain_isolation {
    use std::time::Duration;

    use mitiflow::{
        Event, EventBusConfig, EventPublisher, EventSubscriber, HeartbeatMode, MitiflowDomain,
        Namespace, TransportProfile, partition::PartitionManager,
    };
    use tokio::time::{Instant, timeout};

    use super::common::TestPayload;

    const OPEN_TIMEOUT: Duration = Duration::from_secs(5);
    const INIT_TIMEOUT: Duration = Duration::from_secs(5);
    const PUBLISH_TIMEOUT: Duration = Duration::from_secs(3);
    const RECEIVE_TIMEOUT: Duration = Duration::from_secs(3);
    const NEGATIVE_RECEIVE_TIMEOUT: Duration = Duration::from_millis(500);

    fn domain_config(domain: &MitiflowDomain) -> EventBusConfig {
        domain
            .event_bus_config("events")
            .expect("topic should be valid")
            .cache_size(32)
            .heartbeat(HeartbeatMode::Disabled)
            .history_on_subscribe(false)
            .build()
            .expect("domain event config should build")
    }

    async fn open_domain(
        id: &str,
        namespace: Namespace,
        transport: TransportProfile,
    ) -> MitiflowDomain {
        timeout(
            OPEN_TIMEOUT,
            MitiflowDomain::builder(id)
                .namespace(namespace)
                .transport(transport)
                .open(),
        )
        .await
        .expect("domain open timed out")
        .expect("domain should open")
    }

    async fn new_subscriber(domain: &MitiflowDomain, config: EventBusConfig) -> EventSubscriber {
        timeout(INIT_TIMEOUT, EventSubscriber::new(domain.session(), config))
            .await
            .expect("subscriber init timed out")
            .expect("subscriber should initialize")
    }

    async fn new_publisher(domain: &MitiflowDomain, config: EventBusConfig) -> EventPublisher {
        timeout(INIT_TIMEOUT, EventPublisher::new(domain.session(), config))
            .await
            .expect("publisher init timed out")
            .expect("publisher should initialize")
    }

    pub mod tests {
        use super::*;

        async fn assert_worker_view_stays_isolated(
            manager: &PartitionManager,
            own_worker: &str,
            foreign_worker: &str,
        ) {
            let deadline = Instant::now() + Duration::from_millis(500);

            loop {
                let workers = manager.known_workers().await;
                assert!(
                    workers.iter().any(|worker| worker == own_worker),
                    "partition manager should retain its own worker {own_worker}; got {workers:?}"
                );
                assert!(
                    !workers.iter().any(|worker| worker == foreign_worker),
                    "partition manager for {own_worker} must not see foreign worker {foreign_worker}; got {workers:?}"
                );
                assert_eq!(
                    workers.len(),
                    1,
                    "partition manager for {own_worker} should only know itself; got {workers:?}"
                );

                if Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn local_isolated_domains_with_same_key_prefix_do_not_cross() {
            let shared_namespace = Namespace::from_root("mitiflow/domain-isolation-shared")
                .expect("shared namespace should be valid");

            let domain_a = open_domain(
                "domain-isolation-a",
                shared_namespace.clone(),
                TransportProfile::LocalIsolated,
            )
            .await;
            let domain_b = open_domain(
                "domain-isolation-b",
                shared_namespace,
                TransportProfile::LocalIsolated,
            )
            .await;

            let config_a = domain_config(&domain_a);
            let config_b = domain_config(&domain_b);
            assert_eq!(
                config_a.key_prefix, config_b.key_prefix,
                "the logical event-bus key prefix must match exactly"
            );
            assert_ne!(
                domain_a.local_endpoint(),
                domain_b.local_endpoint(),
                "LocalIsolated domains should bind distinct local endpoints"
            );

            let sub_a = new_subscriber(&domain_a, config_a.clone()).await;
            let sub_b = new_subscriber(&domain_b, config_b).await;
            let pub_a = new_publisher(&domain_a, config_a).await;
            tokio::time::sleep(Duration::from_millis(100)).await;

            timeout(
                PUBLISH_TIMEOUT,
                pub_a.publish(&Event::new(TestPayload { value: 42 })),
            )
            .await
            .expect("publish on domain A timed out")
            .expect("publish on domain A should succeed");

            let received_a: Event<TestPayload> = timeout(RECEIVE_TIMEOUT, sub_a.recv())
                .await
                .expect("domain A subscriber should receive its own event")
                .expect("domain A receive should succeed");
            assert_eq!(received_a.payload.value, 42);

            let received_b = timeout(NEGATIVE_RECEIVE_TIMEOUT, sub_b.recv::<TestPayload>()).await;
            assert!(
                !matches!(received_b, Ok(Ok(_))),
                "domain B must not receive events published inside domain A"
            );

            drop(pub_a);
            drop(sub_b);
            drop(sub_a);
            domain_a.shutdown().await.unwrap();
            domain_b.shutdown().await.unwrap();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn client_dead_endpoint_fails_fast() {
            let open_result = timeout(
                Duration::from_secs(5),
                MitiflowDomain::builder("client-dead-endpoint")
                    .transport(TransportProfile::Client {
                        connect: vec!["tcp/127.0.0.1:1".into()],
                    })
                    .open(),
            )
            .await;

            match open_result {
                Ok(Ok(domain)) => {
                    let _ = domain.shutdown().await;
                    panic!("client domain unexpectedly opened against tcp/127.0.0.1:1");
                }
                Ok(Err(err)) => {
                    let message = err.to_string();
                    assert!(
                        !message.trim().is_empty(),
                        "dead endpoint error should include a clear message"
                    );
                }
                Err(_) => {
                    panic!("client dead endpoint timed out instead of returning Err");
                }
            }
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn ambient_domain_query_smoke_is_bounded_without_router() {
            let domain = timeout(
                OPEN_TIMEOUT,
                MitiflowDomain::builder("ambient-query-smoke")
                    .transport(TransportProfile::Ambient)
                    .open(),
            )
            .await
            .expect("ambient domain open timed out")
            .expect("ambient domain should open without requiring a local router");

            assert_eq!(domain.transport(), &TransportProfile::Ambient);
            assert!(
                domain.local_endpoint().is_none(),
                "Ambient should not expose a synthetic local endpoint"
            );

            let probe_key = format!("{}/ambient-query-smoke/probe", domain.namespace().root());
            let get_result = timeout(Duration::from_secs(2), async {
                domain
                    .session()
                    .get(&probe_key)
                    .timeout(Duration::from_millis(300))
                    .await
            })
            .await;

            if let Ok(Ok(replies)) = get_result {
                let replies_seen = timeout(Duration::from_secs(2), async move {
                    let mut count = 0usize;
                    while let Ok(_reply) = replies.recv_async().await {
                        count += 1;
                        if count >= 8 {
                            break;
                        }
                    }
                    count
                })
                .await
                .expect("ambient query reply drain timed out");

                assert!(
                    replies_seen <= 8,
                    "ambient smoke query should stay within its bounded drain cap"
                );
            } else if let Ok(Err(err)) = get_result {
                assert!(
                    !err.to_string().trim().is_empty(),
                    "bounded ambient query error should include a message"
                );
            }

            domain.shutdown().await.unwrap();
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn workers_not_shared() {
            let domain_a = timeout(
                OPEN_TIMEOUT,
                MitiflowDomain::isolated_for_test("workers_not_shared_a"),
            )
            .await
            .expect("domain A open timed out")
            .expect("domain A should open");
            let domain_b = timeout(
                OPEN_TIMEOUT,
                MitiflowDomain::isolated_for_test("workers_not_shared_b"),
            )
            .await
            .expect("domain B open timed out")
            .expect("domain B should open");

            let config_a = domain_a
                .event_bus_config("orders")
                .expect("orders topic should be valid")
                .num_partitions(8)
                .worker_id("worker-a")
                .heartbeat(HeartbeatMode::Disabled)
                .history_on_subscribe(false)
                .build()
                .expect("partition config A should build");
            let config_b = domain_b
                .event_bus_config("orders")
                .expect("orders topic should be valid")
                .num_partitions(8)
                .worker_id("worker-b")
                .heartbeat(HeartbeatMode::Disabled)
                .history_on_subscribe(false)
                .build()
                .expect("partition config B should build");

            assert_ne!(
                config_a.key_prefix, config_b.key_prefix,
                "unrelated isolated_for_test domains must derive distinct topic namespaces"
            );
            assert_ne!(
                domain_a.local_endpoint(),
                domain_b.local_endpoint(),
                "unrelated LocalIsolated domains should bind distinct endpoints"
            );

            let manager_a = timeout(
                INIT_TIMEOUT,
                PartitionManager::new(domain_a.session(), config_a),
            )
            .await
            .expect("partition manager A init timed out")
            .expect("partition manager A should initialize");
            let manager_b = timeout(
                INIT_TIMEOUT,
                PartitionManager::new(domain_b.session(), config_b),
            )
            .await
            .expect("partition manager B init timed out")
            .expect("partition manager B should initialize");

            let ((), ()) = tokio::join!(
                assert_worker_view_stays_isolated(&manager_a, "worker-a", "worker-b"),
                assert_worker_view_stays_isolated(&manager_b, "worker-b", "worker-a"),
            );

            drop(manager_b);
            drop(manager_a);
            domain_b.shutdown().await.unwrap();
            domain_a.shutdown().await.unwrap();
        }
    }
}
