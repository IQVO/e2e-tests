# HARNESS.md — e2e-tests

What agent harness this repo has, what each piece runs, and when. Model (from
`warehouse-harness-template`, after Böckeler, "Harness engineering for coding agent users"):
**guides** steer an agent before it acts (feedforward), **sensors** report after it acts (feedback);
each is **computational** (deterministic) or **inferential** (LLM). This repo is a black-box godog
harness over the whole fleet, not a bounded-context service, so it carries none of the Go-service
sensors (coverage gate, arch-test, mutation, api-lint, helm). Only what is listed below exists here.

harness-template: v4

## Guides (feedforward)

| File | Type | Purpose |
|---|---|---|
| `CLAUDE.md` (`AGENTS.md` is a symlink to it) | computational | Entry point: layout, the three non-negotiables, commands, what CI runs. |
| `.claude/rules/idempotent-suite.md` | computational | Scoped to `features/**` and the step files: the `<run>` token, no table purges, no `>=` assertions. |
| `.claude/rules/harness-infra-and-ci.md` | computational | Scoped to `scripts/*.sh`, `env.sh`, compose, Makefile: per-run consumer groups, the shared broker, CI topology. |
| `.claude/rules/fleet/readme-standard.md` | computational | Fleet README standard (copied from the template). |
| `.claude/skills/*/SKILL.md` | inferential | Add a scenario or service, run the suite locally vs CI, write assertions that can fail. |
| `.claude/agents/*.md` | inferential | `code-review`, `architecture-review`, `domain-review` subagents (template-managed). |
| `.claude/memory/MEMORY.md` | computational | Durable repo heuristics that are not incidents. |
| `INCIDENTS.md` | computational | Fleet incident log: every escape with its fix and the sensor that now guards it. |

## Sensors (feedback)

| Sensor | Where | Lifecycle | Blocking? |
|---|---|---|---|
| `guide-lint` job: `guide_lint.py` (skills load, cited paths and `make` targets exist, context budget), `repo_lint.py` (workflow paths, `needs:`, Dependabot entries, stale owner, placeholders), `test_hook.py`, `test_red_issue.py`, `test_repo_lint.py` | `.github/workflows/ci.yml` | PR + push to develop/main | yes |
| `build-and-vet` job: gofmt check, `go vet ./...`, `go test -c` (compile-check of the godog suite; it is not run) | ci.yml | PR + push | yes |
| `seed-products-tests` job: offline tests of the seeders (`cmd/seed-*`), `cmd/warehouse-day`, `internal/...` against contract fakes; `scripts/test_harvest_fulfilment_products.py` | ci.yml | PR + push | yes |
| `scripts-sanity` job: `bash -n scripts/*.sh`, `docker compose config` | ci.yml | PR + push | yes |
| E2E Behaviour: `bootstrap-scenario`, `inter-warehouse-transfer-scenario`, `inbound-receiving-scenario`, `slotting-optimization-scenario`, `returns-exceptions-scenario` (real multi-repo checkout, real Kafka and Postgres) | `.github/workflows/e2e-behaviour.yml` | weekly (Mon 07:00 UTC) + manual | scheduled signal; `bootstrap-scenario` opens/closes a `harness:red` issue via `red_issue.py` |
| CodeQL (Go) | `.github/workflows/codeql.yml` | PR, push, weekly | yes |
| AI review (advisory) | `.github/workflows/ai-review.yml` | PR into develop touching `internal/domain`, `apis`, `charts` paths (none exist here, so it does not fire) | no |
| The full godog suite (`bash scripts/04-run-tests.sh`, `make run`) | local only | before merging a scenario change | by hand |

Local gates: `make check-fast` (fmt-check + vet; the agent Stop hook runs it), `make check` (adds
`go test -c`, script syntax, compose config), `make check-all` (= check), `make seed-tests`,
`make guide-lint`, `make harness-test`.

## Agent runtime harness

| Piece | Files | Lifecycle |
|---|---|---|
| Guard + feedback hooks | `scripts/harness/hook.py` (`pre` / `post` / `stop`) with adapters `.claude/settings.json`, `.codex/hooks.json`, `.opencode/plugins/harness.ts` | every tool call; end of turn |
| Skills for Codex | `.agents/skills` symlink to `.claude/skills` | on task match |
| Red-sensor loop | `scripts/harness/red_issue.py` in the scheduled bootstrap job | weekly |

The hooks block pushes to develop/main, `--no-verify`, force-push, bare `rm -rf` on non-scratch
paths, destructive git, and edits to generated files; after a Go edit they feed gofmt/vet findings
back; the Stop hook runs `make check-fast` when code changed. `HARNESS_OFF=1` disables them.
The `scripts/harness/*.py` files and the three adapters are template-managed copies: change them in
`warehouse-harness-template`, never here.

## Incident protocol

Every failure that was not caught before CI ends with the fix, a sensor or guide that would have
caught it, that sensor rolled out fleet-wide, and a row in [INCIDENTS.md](INCIDENTS.md). A row with
no guard is an open gap and says so.
