# Domains, Namespaces, and Transport Profiles

How `MitiflowDomain` scopes event traffic, isolates namespaces, and selects
Zenoh transport without a mandatory router.

**Status:** Implemented.

**Related docs:**
[02_architecture.md](02_architecture.md),
[15_key_based_publishing.md](15_key_based_publishing.md),
[13_distributed_storage.md](13_distributed_storage.md),
[16_dx_and_multi_topic.md](16_dx_and_multi_topic.md)

---

## 1. What a Domain Is

A `MitiflowDomain` is a single unit of deployment isolation. It owns three
things:

1. A [`DomainId`](#domainid) that names the domain.
2. A [`Namespace`](#namespace) that prefixes every Zenoh key expression the
domain uses.
3. A [`TransportProfile`](#transport-profiles) that decides how the underlying
Zenoh session connects (or doesn't connect) to the network.

Opening a domain gives you a `zenoh::Session`. All publishers, subscribers,
stores, and partition managers created from that session share the same
namespace root and the same transport semantics. Two domains with different
transports can coexist on the same host without interfering, because
`LocalIsolated` binds to a private localhost port and disables all scouting.

---

## 2. IDs: DomainId vs ZenohId vs node_id vs PublisherId

Mitiflow uses four different identifiers. They sound similar but do different
jobs.

| ID | Type | Scope | Purpose |
|----|------|-------|---------|
| `DomainId` | Validated UTF-8 string (max 64 chars) | Logical domain | Names the namespace root (`mitiflow/{domain_id}`). Shared by every component in that domain. |
| `ZenohId` (zid) | 128-bit Zenoh runtime ID | Zenoh session | Unique to the Zenoh runtime process. Changes every restart. Not exposed by Mitiflow's public API. |
| `node_id` | Caller-defined string (usually hostname or pod name) | Partition manager / storage agent | Identifies a physical node in the hash ring for partition assignment. Independent of `DomainId`. |
| `PublisherId` | UUID v7 | Per publisher instance | Identifies a single `EventPublisher` for sequencing, gap detection, and cache recovery. |

### DomainId

`DomainId` is the human-chosen name you pass to `MitiflowDomain::builder(...)`.
Validation rejects empty strings, `*`, `$`, leading `_`, and whitespace. The
default namespace root becomes `mitiflow/{domain_id}`, so every event key
expression starts with that prefix.

```rust
let domain = MitiflowDomain::builder("orders-prod").open().await?;
// namespace root = "mitiflow/orders-prod"
```

### ZenohId

Zenoh assigns every session a `ZenohId` (zid) automatically. You do not set it
and you rarely need to read it. The only place it surfaces inside Mitiflow is
in the delayed discovery log for `Ambient` transport, where it is printed for
diagnostics.

### node_id

`node_id` appears in partition-manager and storage-agent code. It is the name
of the physical machine or container in the cluster, used for rendezvous
hashing. It has nothing to do with `DomainId`. One host can run multiple
domains, each with its own `DomainId`, while the host still carries a single
`node_id`.

### PublisherId

Every `EventPublisher` generates a fresh `PublisherId` (UUID v7) on creation.
It tags every event so subscribers can detect gaps and query the publisher's
cache for recovery. It is ephemeral and lasts only as long as the publisher
instance.

---

## 3. Namespace

A `Namespace` is a validated Zenoh key-prefix root. The default is
`mitiflow/{domain_id}`, but you can override it:

```rust
let ns = Namespace::from_root("myapp/production")?;
let domain = MitiflowDomain::builder("prod")
    .namespace(ns)
    .open()
    .await?;
```

`Namespace::derive(suffix)` appends a suffix to the root. `event_bus_config`
calls this internally so that `domain.event_bus_config("events")` produces the
key prefix `mitiflow/prod/topics/events`.

Validation rules for roots and suffixes:

- No leading or trailing slash
- No empty segments (`//`)
- No `*` or `$`
- No leading `_` on the root (suffixes may use `_` for internal channels)
- No whitespace

---

## 4. Transport Profiles

`TransportProfile` maps a high-level connectivity intent to a concrete Zenoh
configuration. There are four profiles.

| Profile | Zenoh mode | Scouting | Requires router | Use case |
|---------|-----------|----------|-----------------|----------|
| `LocalIsolated` | `peer` | multicast = false, gossip = false | No | Single-process or local-only testing. Binds to a localhost ephemeral port. No external discovery. |
| `Client { connect }` | `client` | multicast = false, gossip = false | Yes (connect list) | Production client: connects to known Zenoh router(s). |
| `PeerMesh { connect }` | `peer` | multicast = false, gossip = false | No (peer list) | Brokerless mesh: peers connect to each other directly. No central router required. |
| `Ambient` | default | unchanged | Maybe | Uses Zenoh defaults, including any existing routers or multicast peers on the local network. |

### Profile semantics

**LocalIsolated**

- Binds to `tcp/127.0.0.1:0` (OS-assigned ephemeral port).
- Disables multicast and gossip scouting.
- Enables timestamping.
- Other `LocalIsolated` domains on the same machine cannot see each other,
because each one uses a different port and no discovery.
- This is the default for `MitiflowDomain::builder(...).open()` and for
`isolated_for_test`.

**Client**

- Connects to one or more explicit endpoints (e.g., `tcp/zenoh-router:7447`).
- Disables scouting.
- Requires a non-empty `connect` list. Empty connect returns an error at open
time.
- The binary exits non-zero if the config specifies `client` without
endpoints.

**PeerMesh**

- Connects to a list of peer endpoints directly.
- Disables scouting.
- Requires a non-empty `connect` list.
- No router is mandatory. As long as the peer graph is connected, events flow
peer-to-peer. This preserves brokerless operation.

**Ambient**

- Leaves Zenoh's default configuration untouched, except enabling
`timestamping/enabled`.
- May discover existing routers or peers via multicast or gossip.
- Useful for development on a shared LAN, or when you want Zenoh to find
infrastructure automatically.
- Because it can join unexpected peers, `Ambient` logs a warning at startup
with the exact token `transport.profile=Ambient`.

---

## 5. Choosing a Profile

| Situation | Recommended profile | Why |
|-----------|--------------------|-----|
| Unit tests, CI, single-process demo | `LocalIsolated` | No network required, no cross-talk between concurrent tests. |
| Local multi-process dev on one host | `LocalIsolated` parent + `join_isolated()` children | Parent listens on localhost; children connect as `Client`. |
| Production with a Zenoh router fleet | `Client` | Stable, explicit router list. No surprise discovery. |
| Production brokerless mesh (no router) | `PeerMesh` | Direct peer links, no single point of failure. |
| Shared LAN, quick experiments | `Ambient` (opt-in) | Automatic discovery. Accept the warning. |

`LocalIsolated` and `PeerMesh` both preserve brokerless operation. `Client`
still operates without a message broker in the traditional sense, but it does
require a Zenoh router for session establishment. `Ambient` is the only profile
that can silently attach to existing infrastructure.

---

## 6. Brokerless Guarantees

Mitiflow remains brokerless at the product semantics layer: no Mitiflow
broker owns routing policy, offsets, or storage. The transport profile only
controls how Zenoh moves traffic:

- `LocalIsolated` and `PeerMesh` use private or explicit peer links and do not
need a router.
- `Client` connects to a Zenoh router for session establishment and may route
traffic through it. Mitiflow does not add Kafka-style broker semantics,
offsets, or storage to that router — events still flow publisher → subscriber
via Zenoh's native path.
- `Ambient` may use a router if one is present, but it is not required.

Switching from `LocalIsolated` to `PeerMesh` does not change any Mitiflow
semantics. Sequencing, gap detection, store durability, and consumer groups
work identically.

---

## 7. Migration from Key-Prefix-Only Setups

Before domains existed, every test and example called `zenoh::open` directly
and built `EventBusConfig` with a literal key prefix:

```rust
let session = zenoh::open(zenoh::Config::default()).await.unwrap();
let config = EventBusConfig::builder("myapp/events").build()?;
```

The domain-based equivalent:

```rust
let domain = MitiflowDomain::builder("myapp")
    .transport(TransportProfile::LocalIsolated)
    .open()
    .await?;
let config = domain.event_bus_config("events")?.build()?;
// key_prefix = "mitiflow/myapp/topics/events"
```

Migration steps:

1. Replace `zenoh::open(...)` with `MitiflowDomain::builder(id)`.
2. Pick a `TransportProfile` (usually `LocalIsolated` for tests, `Client` or
`PeerMesh` for production).
3. Replace literal `EventBusConfig::builder("...")` with
`domain.event_bus_config("topic")`.
4. Replace `session.close()` with `domain.shutdown().await`.
5. If you previously used `myapp/events` as a shared prefix across multiple
sessions, give them the same `DomainId` and namespace so the derived prefixes
match.

---

## 8. Observability and Log Warnings

Every `MitiflowDomain` emits a single structured `INFO` log at startup:

```
INFO MitiflowDomain opened
  domain_id=<id>
  namespace.root=<root>
  transport.profile=<profile>
  listen.endpoints=[...]
  connect.endpoints=[...]
  scouting.multicast=<true|false>
  scouting.gossip=<true|false>
```

This log is stable and safe to scrape. It tells an operator exactly which
transport the process is using, whether it is listening or connecting, and
whether discovery is enabled.

### Ambient warning

If you select `Ambient`, an additional `WARN` log is emitted:

```
WARN Ambient transport enabled
  transport.profile=Ambient
  domain_id=<id>
  namespace.root=<root>
```

A delayed `INFO` log (100ms after open) prints the discovered Zenoh routers
and peers:

```
INFO Ambient discovery state
  transport.profile=Ambient
  zenoh.zid=<zid>
  zenoh.routers=[...]
  zenoh.peers=[...]
```

If you see the Ambient warning in production and did not intend to use it,
check that `MITIFLOW_TRANSPORT_PROFILE` or `transport.profile` in YAML was not
explicitly set to `ambient`. The default is `local-isolated`; you must opt in
to `Ambient`.

---

## 9. Configuration: YAML and Environment

Binary configs accept an optional `domain` and `transport` block. Environment
variables override YAML; YAML overrides defaults.

### YAML schema

```yaml
domain:
  id: my-domain          # optional; default = binary-specific fallback
  namespace: myapp/prod  # optional; default = mitiflow/{id}
transport:
  profile: local-isolated   # optional; default = local-isolated
  connect:                  # required for client and peer-mesh
    - "tcp/router-1:7447"
    - "tcp/router-2:7447"
```

Supported `profile` strings (case-insensitive, hyphens and underscores ignored):

- `local-isolated`, `localisolated`, `LocalIsolated`
- `client`, `Client`
- `peer-mesh`, `peermesh`, `PeerMesh`
- `ambient`, `Ambient`

### Environment overrides

| Variable | Purpose |
|----------|---------|
| `MITIFLOW_DOMAIN_ID` | Overrides `domain.id` |
| `MITIFLOW_DOMAIN_NAMESPACE` | Overrides `domain.namespace` |
| `MITIFLOW_TRANSPORT_PROFILE` | Overrides `transport.profile` |
| `MITIFLOW_TRANSPORT_CONNECT` | Comma-separated endpoints; overrides `transport.connect` |

Example:

```bash
MITIFLOW_DOMAIN_ID=qa \
MITIFLOW_TRANSPORT_PROFILE=client \
MITIFLOW_TRANSPORT_CONNECT=tcp/zenoh-qa:7447 \
  mitiflow storage
```

### Resolution precedence

1. Explicit CLI transport override (highest)
2. Environment variables
3. YAML file values
4. Built-in defaults (lowest)

`client` and `peer-mesh` profiles exit non-zero at startup if `connect` is
empty after all sources are resolved.

---

## 10. Quick-Start Example

```rust,no_run
use mitiflow::{
    Event, EventPublisher, EventSubscriber,
    MitiflowDomain, TransportProfile,
};

#[tokio::main]
async fn main() -> mitiflow::Result<()> {
    // Open a local-isolated domain (no router needed)
    let domain = MitiflowDomain::builder("demo")
        .transport(TransportProfile::LocalIsolated)
        .open()
        .await?;

    // Derive an event-bus config from the domain namespace
    let config = domain.event_bus_config("sensors")?.build()?;

    // Create publisher and subscriber on the same domain session
    let subscriber = EventSubscriber::new(domain.session(), config.clone()).await?;
    let publisher = EventPublisher::new(domain.session(), config).await?;

    // Publish
    let event = Event::new(serde_json::json!({"temp": 22.5}));
    publisher.publish(&event).await?;

    // Receive
    let received = subscriber.recv::<serde_json::Value>().await?;
    println!("Got: {:?}", received.payload);

    // Graceful shutdown
    drop(publisher);
    drop(subscriber);
    domain.shutdown().await?;
    Ok(())
}
```

For a multi-process mesh, open a parent with `LocalIsolated` and have child
processes join via `join_isolated`:

```rust,no_run
let parent = MitiflowDomain::builder("mesh")
    .transport(TransportProfile::LocalIsolated)
    .open()
    .await?;

let child = parent.join_isolated().await?;
// child.session() can now pub/sub with parent.session()
```

---

## 11. Summary

- `DomainId` names the domain. `Namespace` turns it into a Zenoh key prefix.
- `TransportProfile` decides how the Zenoh session connects. Default is
`LocalIsolated`.
- `LocalIsolated` and `PeerMesh` need no router. `Ambient` is opt-in and logs a
warning.
- Configuration flows: env → YAML → defaults. `client` and `peer-mesh` require
non-empty `connect`.
- Startup logs expose transport, endpoints, and scouting state for every domain.

