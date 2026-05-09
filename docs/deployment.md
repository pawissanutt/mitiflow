# Deployment Guide

This guide covers deploying Mitiflow — from single-node development to multi-node container-based production setups.

> **Prerequisite:** Familiar with Mitiflow concepts? If not, start with the [Getting Started](getting_started.md) guide.

---

## Table of Contents

1. [Deployment Modes](#1-deployment-modes)
2. [Dev Mode (Single Process)](#2-dev-mode-single-process)
3. [Production with Containers](#3-production-with-containers)
4. [Domain & Transport Configuration](#4-domain--transport-configuration)
5. [Environment Variables](#5-environment-variables)
6. [Building Container Images](#6-building-container-images)
7. [Compose Stack](#7-compose-stack)
8. [Without Containers](#8-without-containers)
9. [Zenoh Network Topology](#9-zenoh-network-topology)
10. [Monitoring & Observability](#10-monitoring--observability)
11. [Troubleshooting](#11-troubleshooting)

---

## 1. Deployment Modes

Mitiflow is brokerless at the product semantics layer — no Mitiflow broker owns
routing policy, offsets, or storage. You choose how many operational components
to run based on your needs:

| Mode | Components | Use case |
|------|-----------|----------|
| **Library only** | Your app + Mitiflow crate | Embedded streaming, no separate processes |
| **Dev mode** | `mitiflow dev` (all-in-one) | Local development and testing |
| **Production** | Storage agent(s) + Orchestrator (optional) + optional explicit Zenoh routing | Multi-node deployments |

**Key insight:** The orchestrator and storage agent are *operational utilities*,
not message brokers. Publishers and subscribers exchange events over Zenoh's
native path. `LocalIsolated` and `PeerMesh` need no router; `Client` may route
traffic through a Zenoh router for session establishment without adding Mitiflow
broker semantics. The system works without any Mitiflow infrastructure
processes.

> **See also:** [Architecture](02_architecture.md) for component roles, [Distributed Storage](13_distributed_storage.md) for multi-node storage design.

---

## 2. Dev Mode (Single Process)

The fastest way to run the full stack locally:

```bash
# Install the CLI
cargo install --features full --path mitiflow-cli/
# Or: just install-cli

# Start everything in one process
mitiflow dev --topics "my-topic:8:1"
#                       name:partitions:replication_factor
```

This starts:
- An embedded Zenoh session (peer mode, no router)
- An Event Store with fjall backend
- The orchestrator HTTP API on `http://localhost:8080`
- Storage agent for partition management

You can interact immediately:
```bash
# Check cluster status
mitiflow ctl cluster status

# List topics
mitiflow ctl topics list

# Run diagnostics
mitiflow ctl diagnose
```

---

## 3. Production with Containers

### Architecture

```
┌──────────────────┐         Zenoh path         ┌──────────────┐
│   Orchestrator    │◄────────────────────────►│    Storage    │
│  (HTTP API :8080) │                          │    Agent      │
│  + embedded UI    │                          │  (fjall LSM)  │
└─────────┬────────┘                          └──────┬───────┘
          │                                          │
          │              Zenoh path                  │
          ▼                                          ▼
┌──────────────────┐                          ┌──────────────┐
│  Publisher       │◄────────────────────────►│  Subscriber  │
│  (your app)      │                          │  (your app)  │
└──────────────────┘                          └──────────────┘
```

The checked-in compose stack runs each component with its own `MitiflowDomain`.
By default, unconfigured binaries use `LocalIsolated` (no external discovery).
For multi-process communication across containers, configure an explicit profile
such as `Client` or `PeerMesh` with endpoints, or opt into `Ambient` discovery.
Events flow publisher → subscriber over Zenoh's native path.

---

## 4. Domain & Transport Configuration

Every Mitiflow binary and library entry point opens a `MitiflowDomain` that bundles the Zenoh session, namespace, and transport profile into one unit. You configure it through YAML, environment variables, or programmatically.

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

- `local-isolated` — single-process or local-only testing; no router needed
- `client` — connects to known Zenoh router(s); requires `connect` endpoints
- `peer-mesh` — direct peer links; no central router; requires `connect` endpoints
- `ambient` — uses Zenoh defaults, including multicast discovery; opt-in and logs a warning

`LocalIsolated` and `PeerMesh` preserve brokerless operation. `Client` may route through a Zenoh router for session establishment, but Mitiflow does not add Kafka-style broker semantics. See [Domains & Transport](21_domains.md) for full profile semantics and migration from raw `zenoh::open` setups.

### Resolution precedence

1. Explicit CLI transport override (highest)
2. Environment variables
3. YAML file values
4. Built-in defaults (lowest)

`client` and `peer-mesh` profiles exit non-zero at startup if `connect` is empty after all sources are resolved.

---

## 5. Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MITIFLOW_DOMAIN_ID` | — | Overrides `domain.id` in YAML |
| `MITIFLOW_DOMAIN_NAMESPACE` | — | Overrides `domain.namespace` in YAML |
| `MITIFLOW_TRANSPORT_PROFILE` | `local-isolated` | Overrides `transport.profile` in YAML |
| `MITIFLOW_TRANSPORT_CONNECT` | — | Comma-separated endpoints; overrides `transport.connect` in YAML |
| `MITIFLOW_KEY_PREFIX` | `mitiflow` | Zenoh key expression prefix for all topics (legacy; defaults to domain namespace when absent) |
| `MITIFLOW_DATA_DIR` | `/data` | Directory for persistent storage (fjall LSM) |
| `MITIFLOW_HTTP_BIND` | `0.0.0.0:8080` | Orchestrator HTTP bind address |
| `MITIFLOW_AUTH_TOKEN` | — | Bearer token required for orchestrator HTTP API and UI when set |
| `MITIFLOW_NUM_PARTITIONS` | `16` | Default partition count per topic |
| `RUST_LOG` | `info` | Logging filter (e.g., `mitiflow_storage=debug`) |

---

## 6. Building Container Images

The [`Containerfile`](../Containerfile) uses a multi-stage build optimized for caching:

```bash
# Build the storage agent image
podman build --build-arg PACKAGE=mitiflow-storage -t mitiflow-storage .

# Build the orchestrator (with embedded web UI)
podman build \
    --build-arg PACKAGE=mitiflow-orchestrator \
    --build-arg BUILD_UI=true \
    -t mitiflow-orchestrator .

# Or use the justfile shortcuts:
just container-storage
just container-orchestrator
just container-all          # Both images
```

**Build stages:** cargo-chef recipe → dependency cache → Svelte UI (if `BUILD_UI=true`) → Rust binary → Debian slim runtime with `tini` for signal handling.

---

## 7. Compose Stack

The [`docker-compose.yml`](../docker-compose.yml) defines a ready-to-use two-service stack:

```bash
# Start all services
podman compose up -d

# Start only storage
podman compose up -d storage

# View logs
podman compose logs -f

# Stop and clean up
podman compose down -v
```

### Services

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| `orchestrator` | `mitiflow-orchestrator` | 8080 | HTTP admin API + embedded web UI |
| `storage` | `mitiflow-storage` | — | Multi-topic storage agent |

### Persistent volumes

- `orchestrator_data` → `/data` in orchestrator container
- `storage_data` → `/data` in storage container

---

## 8. Without Containers

Run each component as a standalone binary:

```bash
# Terminal 1: Zenoh router (optional)
zenohd

# Terminal 2: Storage agent
mitiflow storage --config storage.yaml

# Terminal 3: Orchestrator
mitiflow orchestrator --config orchestrator.yaml

# Your application links against the mitiflow crate directly.
```

### Minimal storage config (`storage.yaml`)

```yaml
key_prefix: myapp
data_dir: /var/lib/mitiflow/storage
num_partitions: 16
```

### Minimal orchestrator config (`orchestrator.yaml`)

```yaml
key_prefix: myapp
data_dir: /var/lib/mitiflow/orchestrator
http_bind: 0.0.0.0:8080
auth_token: change-me-in-production
```

The HTTP API is unauthenticated unless `auth_token`, `MITIFLOW_AUTH_TOKEN`, or
legacy `MITIFLOW_UI_TOKEN` is set. Precedence is `MITIFLOW_AUTH_TOKEN` → YAML
`auth_token` → `MITIFLOW_UI_TOKEN`; blank tokens are rejected at startup. Use
bearer auth for any non-local deployment:

```bash
MITIFLOW_AUTH_TOKEN=change-me mitiflow orchestrator --config orchestrator.yaml
curl -H 'Authorization: Bearer change-me' http://localhost:8080/api/v1/topics
```

Bearer tokens are sent in cleartext over plain HTTP. Expose the orchestrator API
only on localhost/private networks or behind TLS termination in production.

---

## 9. Zenoh Network Topology

Mitiflow uses four transport profiles. The profile you choose determines the
network topology:

### LocalIsolated (default)

Binds to a localhost ephemeral port. Disables multicast and gossip scouting.
No external discovery. Perfect for single-process apps and local dev.

```
┌─────────────┐    localhost     ┌─────────────┐
│  Process A  │◄───tcp/127...──►│  Process B  │
│ (same host) │    (same domain) │ (same host) │
└─────────────┘                  └─────────────┘
```

### PeerMesh

Peers connect to each other directly via explicit endpoints. No router required.
Preserves brokerless operation across hosts.

```
┌─────────┐     tcp/...      ┌─────────┐
│  Node A │◄───────────────►│  Node B │
└─────────┘                  └─────────┘
```

### Client

Connects to known Zenoh router(s) for session establishment. Traffic may route
through the router, but Mitiflow does not add broker semantics.

```
┌─────────┐     tcp/7447     ┌──────────┐     tcp/7447     ┌─────────┐
│  Node A │────────────────►│  Router  │◄────────────────│  Node B │
└─────────┘                  └──────────┘                  └─────────┘
```

### Ambient

Uses Zenoh defaults, including multicast scouting and gossip discovery. May find
routers or peers automatically on the local network. Opt-in; logs a warning at
startup.

```
┌─────────┐     multicast     ┌─────────┐
│  Node A │◄────scouting────►│  Node B │
└─────────┘                   └─────────┘
```

Production binaries open a `MitiflowDomain` with the transport profile selected
via YAML or environment variables. See [Domain & Transport
Configuration](#4-domain--transport-configuration) and [Domains &
Transport](21_domains.md) for profile selection and explicit endpoint setup.

> **See also:** [Zenoh Capabilities](01_zenoh_capabilities.md) for the stable Zenoh APIs Mitiflow relies on.

---

## 10. Monitoring & Observability

### Orchestrator HTTP API

When the orchestrator is running, it exposes REST endpoints on the configured bind address:

```bash
# Cluster overview
curl http://localhost:8080/api/v1/cluster/status

# List topics
curl http://localhost:8080/api/v1/topics

# Topic details
curl http://localhost:8080/api/v1/topics/my-topic

# Consumer group details and lag
curl http://localhost:8080/api/v1/consumer-groups/my-group
```

### Web UI

Build the orchestrator with `BUILD_UI=true` to embed the Svelte dashboard at `http://localhost:8080/`. It provides:
- Cluster status and node health
- Topic management
- Consumer group lag monitoring

### Logging

All components use `tracing` with `RUST_LOG` filter:

```bash
# Storage agent debug logging
RUST_LOG=mitiflow_storage=debug mitiflow storage --config storage.yaml

# Verbose Zenoh protocol tracing
RUST_LOG=mitiflow=debug,zenoh=trace mitiflow dev --topics "test:4:1"
```

### CLI Diagnostics

```bash
# Health check — tests Zenoh connectivity and store responsiveness
mitiflow ctl diagnose --timeout 10
```

---

## 11. Troubleshooting

### "Unable to push non droppable network message — Closing transport!"

**Cause:** When using `Ambient` transport or explicit multicast scouting, stale
Zenoh peers from previous runs may remain in the discovery mesh, causing
transport buffer overflow. This does not occur with `LocalIsolated` (default) or
`Client`/`PeerMesh` with scouting disabled.

**Fix:** Kill orphaned processes before restarting:
```bash
pkill -9 -f mitiflow
```

### Store not responding to queries

**Check:**
1. Is the storage agent running? `mitiflow ctl cluster status`
2. Is the Zenoh key prefix matching? Both publisher and store must use the same `key_prefix`.
3. Are partitions assigned? `mitiflow ctl topics get <topic>`

### Consumer group not rebalancing

**Cause:** Liveliness tokens require a shared Zenoh network to propagate leave
events. With `LocalIsolated` (default), members must be in the same domain
session or joined via `join_isolated()`. With `Client` or `PeerMesh`, ensure all
members connect to the same router or peer endpoints. With `Ambient`, ensure
scouting can discover all members on the network.

**Fix:** Ensure all members use the same `worker_liveliness_prefix` and are on
the same Zenoh network. For multi-process setups, prefer an explicit profile
(`Client` or `PeerMesh`) over `Ambient`.

> **See also:** [Consumer Group Commits](11_consumer_group_commits.md) for the rebalancing protocol, [Graceful Termination](10_graceful_termination.md) for clean shutdown.

### Durable publish timeouts

**Cause:** No Event Store is running, or the store hasn't subscribed to the publisher's key prefix.

**Fix:**
1. Start an Event Store on the same key prefix.
2. Check `watermark_interval` — smaller values reduce latency but increase overhead.
3. Increase `durable_timeout` if the store is under heavy load.

> **See also:** [Durability](03_durability.md) for the watermark protocol and tuning.

### Slow subscriber performance degradation

If CPU is fine but throughput is low, check:
1. **Channel capacity** — increase `event_channel_capacity` (default: 1024).
2. **Processing shards** — set `num_processing_shards > 1` for multi-publisher streams.
3. **Offload** — enable slow consumer offload to avoid backpressure-induced drops. See [Slow Consumer Offload](17_slow_consumer_offload.md).
