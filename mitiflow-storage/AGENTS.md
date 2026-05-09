# AGENTS.md — storage agent

## OVERVIEW

Per-node sidecar storage daemon: discovers topics, starts/stops per-topic workers, reconciles partition stores, publishes status/health, and recovers from peers.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Binary boot | `src/main.rs` | YAML/env config and signal handling. |
| Agent lifecycle | `src/agent.rs` | Owns supervisor and optional topic watcher. |
| Config | `src/config.rs` | YAML types, env overrides, node identity, labels. |
| Topic supervisor | `src/topic_supervisor.rs` | Multi-topic worker registry. |
| Topic worker | `src/topic_worker.rs` | Per-topic store/reconciler/status bundle. |
| Discovery | `src/topic_watcher.rs` | Orchestrator config subscription and label filters. |
| Reconcile | `src/reconciler.rs` | Start/drain/stop stores for assignments. |
| Status/health | `src/status.rs`, `src/health.rs` | Queryable/status events and metrics. |
| Tests | `tests/e2e/`, `tests/smoke/` | In-process vs subprocess harnesses. |

## CONVENTIONS

- `auto_discover_topics` means orchestrator config drives topic worker creation.
- Label filters must honor both `required_labels` and `excluded_labels`.
- Persist generated node identity; do not churn IDs across restarts.
- Reconciler actions are state transitions: start, recover, active, drain, stop.
- Peer recovery queries must preserve partition/replica ownership semantics.

## TESTING

```bash
cargo nextest run -p mitiflow-storage --features full --no-fail-fast
cargo test -p mitiflow-storage --test e2e_tests --features full
```

- Smoke tests spawn subprocess agents.
- E2E tests use joined isolated sessions; shut child sessions down before `shutdown_all`.

## ANTI-PATTERNS

- Do not put storage agents in the live event routing path.
- Do not ignore drain grace periods; stores may still accept in-flight events while draining.
- Do not bypass topic watcher filtering when labels are configured.
