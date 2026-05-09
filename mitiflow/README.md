# mitiflow

Production-grade event streaming for [Zenoh](https://zenoh.io).

Layers sequencing, gap detection, recovery, single-store durable publishing, and
consumer groups on top of Zenoh pub/sub using stable APIs. No mandatory brokers
and no external coordinator. Quorum durability and Kafka protocol compatibility
are planned but not implemented yet.

## Features

- **Publisher** — standard, keyed (`publish_keyed`), and durable (`publish_durable`) variants
- **Subscriber** — gap detection, tiered recovery (store → cache → backoff), processing shards
- **Key-based publishing** — automatic partition routing via `{prefix}/p/{partition}/k/{key}/{seq}`
- **Consumer groups** — `ConsumerGroupSubscriber` with offset commits, zombie fencing, auto-commit
- **Event store** — fjall LSM backend with HLC-ordered replay, key index, log compaction, GC
- **Slow consumer offload** — automatic switchover to store-based catch-up
- **Dead letter queue** — configurable backoff (fixed, exponential)
- **Codecs** — JSON, MessagePack, Postcard via `CodecFormat`
- **Zero-copy routing** — 50-byte metadata in Zenoh attachments

## Quick Start

```rust,no_run
use mitiflow::{Event, EventPublisher, EventSubscriber, MitiflowDomain};

#[tokio::main]
async fn main() -> mitiflow::Result<()> {
    let domain = MitiflowDomain::builder("demo").open().await?;
    let config = domain.event_bus_config("sensors")?.build()?;

    let subscriber = EventSubscriber::new(domain.session(), config.clone()).await?;
    let publisher = EventPublisher::new(domain.session(), config).await?;

    let event = Event::new(serde_json::json!({"temp": 22.5}));
    publisher.publish(&event).await?;

    let received: Event<serde_json::Value> = subscriber.recv().await?;
    println!("Got: {:?}", received.payload);

    drop(publisher);
    drop(subscriber);
    domain.shutdown().await?;
    Ok(())
}
```

## Domains

`MitiflowDomain` combines namespace and transport isolation: derived event-bus
configs are rooted in a domain namespace, while the domain opens a Zenoh session
using its selected `TransportProfile`.

```rust,no_run
use mitiflow::{MitiflowDomain, TransportProfile};

#[tokio::main]
async fn main() -> mitiflow::Result<()> {
    let domain = MitiflowDomain::builder("orders-dev")
        .transport(TransportProfile::LocalIsolated)
        .open()
        .await?;

    let config = domain.event_bus_config("events")?.build()?;
    assert_eq!(config.key_prefix, "mitiflow/orders-dev/topics/events");

    domain.shutdown().await?;
    Ok(())
}
```

## Feature Flags

| Flag | Default | Description |
|------|---------|-------------|
| `store` | Yes | EventStore + storage backend trait |
| `fjall-backend` | No | Concrete fjall LSM-tree backend |
| `wal` | No | Placeholder for future publisher WAL support; no code path currently uses it |
| `full` | No | Store + fjall backend + currently placeholder WAL flag |

## License

Apache-2.0 — see [LICENSE](../LICENSE).
