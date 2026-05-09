//! Integration tests for partitioned consumer groups (Phase 3).

mod common;

use mitiflow::partition::hash_ring;

#[test]
fn hrw_deterministic_assignment() {
    let workers = vec!["w1".into(), "w2".into(), "w3".into()];
    let a1 = hash_ring::assignments(&workers, 32);
    let a2 = hash_ring::assignments(&workers, 32);
    assert_eq!(a1, a2);
}

#[test]
fn hrw_all_partitions_assigned() {
    let workers = vec![
        "alpha".into(),
        "beta".into(),
        "gamma".into(),
        "delta".into(),
    ];
    let map = hash_ring::assignments(&workers, 128);
    let total: usize = map.values().map(|v| v.len()).sum();
    assert_eq!(total, 128);
}

#[test]
fn hrw_single_worker_gets_all() {
    let workers = vec!["solo".into()];
    let map = hash_ring::assignments(&workers, 16);
    assert_eq!(map["solo"].len(), 16);
}

#[test]
fn partition_for_in_range() {
    for i in 0..1000 {
        let p = hash_ring::partition_for(&format!("order/{i}"), 64);
        assert!(p < 64);
    }
}

#[test]
fn hrw_leave_only_redistributes_departed() {
    let before_workers: Vec<String> = vec!["a".into(), "b".into(), "c".into(), "d".into()];
    let after_workers: Vec<String> = vec!["a".into(), "c".into(), "d".into()]; // b left

    let before = hash_ring::assignments(&before_workers, 64);
    let after = hash_ring::assignments(&after_workers, 64);

    let b_partitions = &before["b"];
    for p in 0..64u32 {
        let owner_before = before.iter().find(|(_, ps)| ps.contains(&p)).unwrap().0;
        let owner_after = after.iter().find(|(_, ps)| ps.contains(&p)).unwrap().0;
        if owner_before != owner_after {
            assert!(
                b_partitions.contains(&p),
                "partition {p} moved but was not owned by departed worker b"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_manager_basic() {
    let domain = common::isolated_domain("partition_basic").await;
    let config = domain
        .event_bus_config("events")
        .unwrap()
        .worker_id("worker-1")
        .num_partitions(16)
        .build()
        .unwrap();

    let pm = mitiflow::PartitionManager::new(domain.session(), config)
        .await
        .unwrap();
    let parts = pm.my_partitions().await;

    // Single worker should own all 16 partitions.
    assert_eq!(parts.len(), 16);
    assert_eq!(pm.worker_id(), "worker-1");
    assert_eq!(pm.num_partitions(), 16);

    drop(pm);
    domain.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_manager_two_workers_rebalance() {
    // Two workers must share Zenoh liveliness — child joins parent's listen
    // endpoint as a Client so the two sessions can communicate without
    // multicast scouting (which LocalIsolated disables).
    let domain1 = common::isolated_domain("partition_rebal").await;
    let domain2 = domain1.join_isolated().await.unwrap();

    let config1 = domain1
        .event_bus_config("events")
        .unwrap()
        .worker_id("w1")
        .num_partitions(16)
        .build()
        .unwrap();

    let pm1 = mitiflow::PartitionManager::new(domain1.session(), config1)
        .await
        .unwrap();

    // w1 starts alone — should own all 16.
    let initial = pm1.my_partitions().await;
    assert_eq!(initial.len(), 16);

    // Now w2 joins.
    let config2 = domain2
        .event_bus_config("events")
        .unwrap()
        .worker_id("w2")
        .num_partitions(16)
        .build()
        .unwrap();

    let pm2 = mitiflow::PartitionManager::new(domain2.session(), config2)
        .await
        .unwrap();

    // Give rebalance time to propagate.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let p1 = pm1.my_partitions().await;
    let p2 = pm2.my_partitions().await;

    // Together they should own all 16.
    let total = p1.len() + p2.len();
    assert_eq!(total, 16, "p1={p1:?} p2={p2:?}");

    // Each should own some partitions (not all to one).
    assert!(!p1.is_empty(), "w1 should have partitions");
    assert!(!p2.is_empty(), "w2 should have partitions");

    // No overlap.
    for p in &p1 {
        assert!(!p2.contains(p), "partition {p} assigned to both workers");
    }

    // Drop child partition manager + shut down child domain before the parent
    // so the Client transport closes cleanly while the parent listener is up.
    drop(pm2);
    domain2.shutdown().await.unwrap();
    drop(pm1);
    domain1.shutdown().await.unwrap();
}
