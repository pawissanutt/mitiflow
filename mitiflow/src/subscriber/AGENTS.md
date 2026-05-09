# AGENTS.md — subscriber internals

## OVERVIEW

Subscriber logic is split between live forwarding, gap recovery, consumer groups, replay, keyed consumption, and slow-consumer offload.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Public subscriber API | `mod.rs` | Constructs forwarder/pipeline/offload tasks. |
| Gap tracking | `gap_detector.rs` | Pure sequence logic; keep deterministic and unit-testable. |
| Forward control | `forwarder.rs` | Pause/resume live Zenoh path during offload. |
| Sharded delivery | `pipeline.rs` | Publisher-based sharding and delivery channel handling. |
| Recovery | `recovery.rs` | Store -> publisher cache -> retry/backoff behavior. |
| Consumer groups | `consumer_group.rs` | Offset commits, fencing, partition assignment. |
| Offload | `offload.rs` | Live -> draining -> catch-up -> live state machine. |
| Replay | `replay.rs` | Store query API and bounded/tailing modes. |

## CONVENTIONS

- Keep `gap_detector.rs` independent of Zenoh/runtime state.
- Store-dependent modules stay `#[cfg(feature = "store")]` through callers and reexports.
- Offload is live-path sensitive: do not block the Tokio runtime with storage work.
- Sequence tracking is per `(PublisherId, partition)`; do not collapse by partition only.
- Keyed subscribers deduplicate by event id; preserve dedup capacity behavior.

## TESTS

- Add pure unit tests next to `gap_detector`, `event_id_dedup`, and pipeline helpers.
- Add integration tests under `mitiflow/tests/` for recovery/offload/replay behavior.
- Use unique domain names and multi-thread Tokio for all Zenoh-facing tests.

## ANTI-PATTERNS

- Do not deliver duplicate or out-of-order recovered samples just to close a gap.
- Do not remove the quiet/drain transition in offload; it protects live-buffer cursors.
- Do not make consumer-group offload multi-shard without revisiting ordering/fencing.
