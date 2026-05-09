# AGENTS.md — docs

## OVERVIEW

Flat documentation set: tutorials, operations guides, design notes, status plan, and roadmap. Use it to distinguish shipped behavior from proposals.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Docs index | `index.md` | Map of all guides/design docs. |
| Current status | `implementation_plan.md` | Best source for implemented vs stub/deferred work. |
| Future work | `ROADMAP.md` | Planned features only. |
| Architecture | `02_architecture.md` | System shape; cross-check status claims. |
| Durability | `03_durability.md`, `05_replication.md` | Current single-store vs planned quorum. |
| Domains | `21_domains.md` | Authoritative namespace/transport/test isolation rules. |
| Config/deploy | `configuration.md`, `deployment.md` | YAML/env/runtime guidance. |
| Keyed events | `15_key_based_publishing.md` | Implemented key format and validation. |
| Multi-topic/DX | `16_dx_and_multi_topic.md` | Implemented with noted deferred items. |
| Schema registry | `18_topic_schema_registry.md` | Phases 1-2 implemented; phase 3 planned. |

## CONVENTIONS

- State whether content is current implementation, design proposal, or roadmap.
- Prefer `implementation_plan.md` over old design docs for feature status.
- Keep planned quorum durability separate from current `publish_durable()` semantics.
- Link back to root README for user-facing quick start and crate inventory.
- Keep docs flat unless a real sub-domain emerges; current tree has no nested docs dirs.

## ANTI-PATTERNS

- Do not describe Kafka gateway, quorum durability, or publisher WAL as shipped.
- Do not copy optimistic checkmarks from `02_architecture.md` without checking current status.
- Do not bury status caveats inside prose; call them out near the feature description.
