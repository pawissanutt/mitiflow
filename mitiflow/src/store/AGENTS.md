# AGENTS.md — store internals

## OVERVIEW

Store code is feature-gated durability/replay infrastructure: backend contract, fjall implementation, query parsing, watermarks, offsets, and lifecycle state.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Reexports | `mod.rs` | Store feature boundary. |
| Backend API | `backend.rs` | Trait contract plus fjall backend implementation. |
| Store runner | `runner.rs` | Zenoh queryable, worker threads, watermark publishing. |
| Query parsing | `query.rs` | Replay selectors, keyed filters, HLC/time windows. |
| Watermarks | `watermark.rs` | Per-publisher persistence confirmation. |
| Offsets | `offset.rs` | Consumer group commit storage and replay offset lookup. |
| Lifecycle | `lifecycle.rs` | Publisher active/suspected/draining/archived state. |

## CONVENTIONS

- Keep synchronous fjall work off Tokio workers; use the store worker model.
- Replay ordering uses HLC metadata, not raw filesystem/key ordering.
- Watermarks are per publisher and partition-sensitive; avoid global-only shortcuts.
- Tombstone/compaction behavior must not break keyed replay or offset queries.
- Preserve queryable response metadata; subscribers depend on attachment fields.

## TESTING

```bash
cargo nextest run -p mitiflow --features full -E 'test(store)'
cargo test -p mitiflow --features full -- store
```

- Store tests normally require `fjall-backend` via `full`.
- Use temp dirs from `mitiflow/tests/common/mod.rs` or `tempfile` helpers.

## ANTI-PATTERNS

- Do not expose fjall-only types without `#[cfg(feature = "fjall-backend")]`.
- Do not treat `wal` as implemented publisher durability.
- Do not change query key formats without updating subscribers, orchestrator, and docs.
