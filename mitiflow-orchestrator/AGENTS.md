# AGENTS.md — orchestrator

## OVERVIEW

Optional control plane: topic config CRUD, lag monitoring, cluster/status aggregation, overrides/drain, schema queryables, and HTTP/SSE API.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Service boot | `src/main.rs` | YAML/env config, domain open, HTTP bind. |
| Core wiring | `src/orchestrator.rs` | Starts config store, monitors, views, queryables, tasks. |
| Config model | `src/config.rs` | TopicConfig, retention/compaction, bootstrap YAML. |
| HTTP API | `src/http/` | `mod.rs` router/auth; route families by file. |
| Lag | `src/lag.rs` | Watermark/offset aggregation and SSE reports. |
| Cluster view | `src/cluster_view.rs` | Node status/health/liveliness aggregation. |
| Overrides/drain | `src/override_manager.rs`, `src/drain.rs` | Maintenance placement overrides. |
| Tests | `tests/` | Large E2E and HTTP suites. |

## CONVENTIONS

- Orchestrator is optional; do not place it in the event data path.
- HTTP paths live under `/api/v1`; UI types mirror `src/http/types.rs`.
- Preserve auth token fallback: `MITIFLOW_AUTH_TOKEN`, then legacy `MITIFLOW_UI_TOKEN` where implemented.
- Use `_admin`, `_config`, and `_schema` key families consistently with storage/CLI.
- Bootstrap topic files are idempotent; do not recreate existing topics.

## TESTING

```bash
cargo nextest run -p mitiflow-orchestrator --features full --no-fail-fast
cargo test -p mitiflow-orchestrator --test http_api --features full
```

- E2E tests use isolated parent domains and child joined sessions.
- HTTP tests may need random local ports; avoid fixed ports.

## ANTI-PATTERNS

- Do not synthesize unstable schema timestamps for existing versions.
- Do not make storage agents/orchestrator authoritative writers for user event payloads.
- Do not let missing schema/config for one topic affect other topics.
