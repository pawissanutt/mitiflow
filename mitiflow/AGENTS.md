# AGENTS.md — core crate

## OVERVIEW

Core library: domain setup, event config, publisher/subscriber, partitions, DLQ, schema, and feature-gated store APIs.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Public exports | `src/lib.rs` | `store`, `fjall-backend`, and subscriber extras are `cfg`-gated. |
| Domain/session | `src/domain/` | `MitiflowDomain` composes namespace + `TransportProfile`. |
| Config surface | `src/config.rs` | Builder-first API; many store/offload fields are gated. |
| Publisher | `src/publisher/mod.rs` | Sequencing, partition routing, cache queryable, watermark wait. |
| Subscriber | `src/subscriber/` | Live receive plus recovery/offload/replay/group internals. |
| Store | `src/store/` | Backend trait, fjall backend, query/replay, watermarks, offsets. |
| Partitions | `src/partition/` | HRW assignment and liveliness-driven rebalance. |
| Test helpers | `tests/common/mod.rs` | Canonical isolated-domain fixtures. |

## CONVENTIONS

- Feature flags: `store` default-on, `fjall-backend` concrete backend, `wal` placeholder, `full = store + fjall-backend + wal`.
- Config changes should preserve builder chaining and derived-key helpers.
- Keep wire-contract fields in `schema.rs` / `EventBusConfig`; keep local tuning fields local.
- Store APIs should remain behind `#[cfg(feature = "store")]`; fjall-only types behind `fjall-backend`.
- Use `crate::` imports inside the crate; tests can use `super::*` in unit modules.

## TESTING

```bash
cargo nextest run -p mitiflow --features full --no-fail-fast
cargo test -p mitiflow --features full -- test_name
cargo bench -p mitiflow --bench codec
```

- Integration tests use `MitiflowDomain::isolated_for_test("unique_test_name")` and multi-thread Tokio.
- Use `tests/common::setup_pubsub`, `publish_n`, `recv_n`, and `temp_dir` instead of ad hoc fixtures.

## ANTI-PATTERNS

- No raw Zenoh session opens in tests.
- No current-thread Tokio tests.
- No un-gated references to `EventStore`, `FjallBackend`, `KeyedConsumer`, replay, offload, or consumer groups.
- No library `.unwrap()`; return crate `Result<T>` with `Error` variants.
