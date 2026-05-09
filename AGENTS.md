# PROJECT KNOWLEDGE BASE

**Generated:** 2026-05-09
**Commit:** `7a5f4f9`
**Branch:** `feat/mitiflow-domains`

## OVERVIEW

Mitiflow is brokerless event streaming on Zenoh: Rust 2024 workspace, sidecar storage/orchestrator, emulator/bench crates, and a standalone Svelte 5 dashboard. Current durability is single-store watermark confirmation; quorum durability, publisher WAL, and Kafka gateway protocol handling remain planned/stubbed.

## STRUCTURE

```text
mitiflow/
├── mitiflow/                 # core library: domains, publisher/subscriber, store, partitions
├── mitiflow-storage/         # per-node storage daemon; topic supervisor/reconciler/watcher
├── mitiflow-orchestrator/    # optional control plane; config, lag, HTTP/SSE, admin queryables
├── mitiflow-cli/             # single `mitiflow` binary: storage/orchestrator/ctl/dev
├── mitiflow-emulator/        # YAML topology runner; many role binaries and chaos tests
├── mitiflow-bench/           # feature-gated transport benchmarks; not in CI clippy/test
├── mitiflow-gateway/         # Kafka gateway stub; `publish = false`
├── mitiflow-ui/              # standalone Svelte 5 + Tailwind 4 SPA, not Cargo workspace
└── docs/                     # flat numbered design/status docs
```

Ignore `target/`, `mitiflow-ui/node_modules/`, `mitiflow-ui/build/`, `mitiflow-ui/dist/`, `manifests/`, `mitiflow-bench/results/`, and `.tmp-chaos-*`.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Public API | `mitiflow/src/lib.rs` | Reexports are feature-gated; verify `store` / `fjall-backend`. |
| Domain/session setup | `mitiflow/src/domain/` | `MitiflowDomain` owns namespace + transport isolation. |
| Publisher path | `mitiflow/src/publisher/mod.rs` | Sequencing, cache queryable, heartbeat, durable watermark wait. |
| Subscriber path | `mitiflow/src/subscriber/` | Forwarder/pipeline/recovery/offload/replay/group modules. |
| Store path | `mitiflow/src/store/` | Backend contract, fjall impl, HLC replay, watermarks, offsets. |
| Storage agent | `mitiflow-storage/src/` | `TopicSupervisor` + `TopicWorker` + `Reconciler` are core. |
| Orchestrator API | `mitiflow-orchestrator/src/http/` | REST/SSE route families; `mod.rs` owns router/auth. |
| Unified CLI | `mitiflow-cli/src/main.rs` | Delegates most work; preserve env/config fallbacks. |
| Emulator | `mitiflow-emulator/src/`, `topologies/` | Numbered YAML fixtures and deterministic chaos. |
| UI | `mitiflow-ui/src/App.svelte`, `src/lib/api.ts`, `src/stores/` | Routes, typed REST client, SSE stores. |
| Status docs | `docs/implementation_plan.md`, `docs/ROADMAP.md` | Current behavior vs future work. |

## CODE MAP

| Symbol | Type | Location | Role |
|--------|------|----------|------|
| `MitiflowDomain` | struct | `mitiflow/src/domain/domain.rs` | Opens isolated/client/peer/ambient Zenoh sessions. |
| `EventBusConfig` | struct+builder | `mitiflow/src/config.rs` | Shared bus config; wire fields vs local tuning split. |
| `EventPublisher` | struct | `mitiflow/src/publisher/mod.rs` | Publishes standard/keyed/durable events. |
| `EventSubscriber` | struct | `mitiflow/src/subscriber/mod.rs` | Live receive, gap recovery, optional offload. |
| `ConsumerGroupSubscriber` | struct | `mitiflow/src/subscriber/consumer_group.rs` | Offset commits, fencing, partition membership. |
| `EventStore` | struct | `mitiflow/src/store/runner.rs` | Store queryable and watermark publisher. |
| `StorageAgent` | struct | `mitiflow-storage/src/agent.rs` | Multi-topic storage daemon lifecycle. |
| `Orchestrator` | struct | `mitiflow-orchestrator/src/orchestrator.rs` | Config CRUD, lag/cluster tracking, HTTP/SSE. |

## CONVENTIONS

- Rust toolchain is pinned by `rust-toolchain.toml` to `1.94.1`; workspace package minimum is `1.93`, edition `2024`.
- Root `justfile` exists; use it for shortcuts. `mitiflow-emulator/justfile` is local to emulator workflows.
- CI excludes `mitiflow-bench` from workspace clippy/test; docs also exclude `mitiflow-cli`.
- CI creates `mitiflow-ui/build` before Rust clippy/test/doc because orchestrator UI embedding expects the directory.
- Cargo-deny runs from `deny.toml`; advisories ignored there are Zenoh transitives awaiting upstream fixes.
- Container builds rewrite workspace members to production crates only: `mitiflow`, `mitiflow-storage`, `mitiflow-orchestrator`.

## ANTI-PATTERNS (THIS PROJECT)

- Do not use raw `zenoh::open(zenoh::Config::default())` in tests; use `MitiflowDomain::isolated_for_test("unique_name")`.
- Do not use `#[tokio::test(flavor = "current_thread")]`; Zenoh tests need multi-thread runtime.
- Drop publisher/subscriber/store handles before domain/session shutdown; graceful shutdown must await tasks.
- Do not claim quorum durability, publisher WAL durability, or Kafka protocol compatibility as implemented.
- Do not put application keys containing empty string, `*`, or `$`; `$*` is Zenoh-reserved syntax.
- Do not treat storage agents or orchestrator as data-path brokers; events are publisher -> subscriber via Zenoh.
- Do not use `.unwrap()` in library code; tests/examples may use it when failure context is obvious.

## COMMANDS

```bash
cargo fmt --all
cargo clippy --workspace --all-targets --features full --exclude mitiflow-bench -- -D warnings
cargo nextest run --workspace --features full --exclude mitiflow-bench --no-fail-fast
cargo doc --workspace --no-deps --features full --exclude mitiflow-cli --exclude mitiflow-bench
cargo deny check
just check
cd mitiflow-ui && pnpm install --frozen-lockfile && pnpm check && pnpm test && pnpm build
```

## NOTES

- Use `--features full` for normal Rust verification; `wal` is currently placeholder-only.
- `mitiflow-ui` runs with Node 24 + pnpm 10 in CI; build output is `mitiflow-ui/build/`.
- Tailwind 4 source detection is cwd-based; run UI tooling from `mitiflow-ui/` unless CSS `source(...)` is added.
- Zenoh 1.9 docs/examples are authoritative; old README snippets pinning earlier Zenoh versions are stale.
