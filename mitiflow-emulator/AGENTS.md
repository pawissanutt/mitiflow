# AGENTS.md — emulator

## OVERVIEW

YAML-driven topology runner and chaos testbed; spawns Mitiflow roles as processes or containers and validates deterministic scenarios.

## STRUCTURE

```text
mitiflow-emulator/
├── src/main.rs          # CLI: run / validate
├── src/bin/             # role binaries: producer, consumer, processor, agent, checker, etc.
├── src/config.rs        # large YAML schema and defaults
├── src/validation.rs    # topology validation rules
├── src/chaos.rs         # fixed/random chaos scheduling
├── src/supervisor.rs    # process/container lifecycle orchestration
├── topologies/          # numbered fixture scenarios
└── tests/               # config, validation, determinism, chaos/e2e tests
```

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Topology schema | `src/config.rs` | Component, generator, storage, chaos definitions. |
| Validation | `src/validation.rs` | DAG, storage coverage, warnings/errors. |
| Role payloads | `src/role_config.rs` | Base64/env contract for spawned role binaries. |
| Process backend | `src/process_backend.rs` | Local process spawn/control. |
| Container backend | `src/container_backend.rs` | Container isolation mode. |
| Fixtures | `topologies/*.yaml` | Numbered examples `01_...` through `15_...`. |

## CONVENTIONS

- Topology files are numbered fixtures; keep names stable because tests reference them.
- Random chaos must be seedable and deterministic for regression tests.
- Role binaries receive config through encoded role payloads; keep CLI/env contracts compatible.
- Manifests/log aggregation are test artifacts; do not commit generated `manifests/` data.
- Use the local `justfile` for emulator validation/run workflows.

## COMMANDS

```bash
cd mitiflow-emulator && just validate-all
cd mitiflow-emulator && just dry-run 01_minimal
cargo test -p mitiflow-emulator
cargo run -p mitiflow-emulator --bin mitiflow-emulator --features full -- validate mitiflow-emulator/topologies/01_minimal.yaml
```

## ANTI-PATTERNS

- Do not make chaos non-deterministic by default.
- Do not change topology schema without updating validation tests and fixture examples.
- Do not require containers for scenarios that should work in process isolation.
