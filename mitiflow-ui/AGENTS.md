# AGENTS.md — UI

## OVERVIEW

Standalone Svelte 5 + TypeScript dashboard for the orchestrator HTTP/SSE API. It is not a Cargo workspace member.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| App routes | `src/App.svelte` | `svelte-spa-router` route table. |
| Shell/nav | `src/components/Layout.svelte`, `Sidebar.svelte` | Shared layout and navigation. |
| Pages | `src/pages/` | Route-level data loading and actions. |
| API client | `src/lib/api.ts` | `/api/v1` typed fetch wrapper. |
| API types | `src/lib/types.ts` | TS mirror of Rust HTTP/SSE DTOs. |
| SSE helper | `src/lib/sse.ts` | EventSource wrapper used by stores. |
| Live state | `src/stores/*.svelte.ts` | Svelte 5 rune stores for cluster/lag/events. |
| Tests | `src/**/*.test.ts` | Vitest specs; current coverage is lib utilities/API. |

## CONVENTIONS

- Use Svelte 5 runes: `$state`, `$derived`, `$effect`, `$props`, snippets/bindables where appropriate.
- Keep REST calls centralized in `src/lib/api.ts`; pages should not hand-roll `/api/v1` fetch logic.
- Keep Rust/HTTP DTO mirrors in `src/lib/types.ts`; update with orchestrator `src/http/types.rs`.
- Vite aliases `$lib` to `/src/lib`; TypeScript path maps `$lib/*` to `src/lib/*`.
- Tailwind 4 is wired through `@tailwindcss/vite`; CSS entry is `@import "tailwindcss"`.
- Run UI tooling from `mitiflow-ui/` so Tailwind source detection sees the right tree.

## COMMANDS

```bash
cd mitiflow-ui && pnpm install --frozen-lockfile
cd mitiflow-ui && pnpm check
cd mitiflow-ui && pnpm test
cd mitiflow-ui && pnpm build
```

## TESTING

- Vitest config uses `jsdom`, globals, `$lib` alias, and `src/**/*.test.ts` includes.
- Use `vitest run`; avoid watch-mode assumptions in CI.
- Component tests should use `@testing-library/svelte` and keep DOM setup in Vitest config.

## ANTI-PATTERNS

- Do not add API types in pages; update `src/lib/types.ts` and `api.ts` together.
- Do not bypass the `/api` Vite proxy in development.
- Do not move build output away from `build/` without updating orchestrator embedding and CI.
