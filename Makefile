# Makefile — the local quality gate for e2e-tests.
#
# Every target below mirrors a sensor in .github/workflows/ci.yml (or, for
# the full-suite targets, the documented local workflow in README.md) so
# an agent or human has one consistent `make check`/`make check-all`
# vocabulary across every repo in the warehouse-systems fleet.
#
# This repo is a pure godog/Gherkin black-box harness (package main,
# TestMain, no buildable non-test package) that drives 11 already-running
# binaries built from FIVE sibling bounded-context repos plus this repo's
# own warehouse-ops-agent build. `go build ./...` is not a meaningful
# check here (always "no packages to build") — `go test -c` is the real
# compile-check equivalent. The full suite (`04-run-tests.sh`) needs the
# whole fleet up (scripts/01-03) and is therefore NOT part of check/
# check-all — see the `up`/`run`/`down` targets for that local workflow.

GO ?= go

.PHONY: help fmt fmt-check vet test-compile scripts-sanity compose-config check check-all up run soak down

help:
	@echo "e2e-tests — local quality gate (targets mirror .github/workflows/ci.yml)"
	@echo ""
	@echo "  help            Print this list of targets (default target)"
	@echo "  fmt             gofmt -w . — format the tree in place"
	@echo "  fmt-check       Fail if gofmt -l . is non-empty (the CI-style check)"
	@echo "  vet             go vet ./..."
	@echo "  test-compile    go test -c -o /dev/null . — compile-check the godog suite (no run)"
	@echo "  scripts-sanity  bash -n every scripts/*.sh (shell syntax check)"
	@echo "  compose-config  docker compose -f docker-compose.yml config"
	@echo ""
	@echo "  check           FAST bundle: fmt-check vet test-compile scripts-sanity compose-config"
	@echo "  check-all       alias for check — this repo has no local unit-test layer beyond compile-check"
	@echo ""
	@echo "  Full black-box suite against a live fleet (see README.md):"
	@echo "  up              scripts/02-up-infra.sh + 01-build.sh + 03-up-services.sh"
	@echo "  run             scripts/04-run-tests.sh — the default godog suite (excludes @soak)"
	@echo "  soak            scripts/06-run-soak.sh — the long-running @soak backlog-ramp scenario"
	@echo "  down            scripts/05-down-services.sh — stops only what 'make up' started"

fmt:
	gofmt -w .

fmt-check:
	@files=$$(gofmt -l .); \
	if [ -n "$$files" ]; then \
		echo "gofmt: the following files are not formatted:"; \
		echo "$$files" | sed 's/^/  /'; \
		echo "run 'make fmt' to fix them"; \
		exit 1; \
	fi; \
	echo "gofmt: clean"

vet:
	$(GO) vet ./...

test-compile:
	$(GO) test -c -o /dev/null .

scripts-sanity:
	@for f in scripts/*.sh; do \
		echo "checking $$f"; \
		bash -n "$$f" || exit 1; \
	done

compose-config:
	docker compose -f docker-compose.yml config

# The fast self-correction loop: run this after every change, before committing.
check: fmt-check vet test-compile scripts-sanity compose-config

# No separate deeper local gate here (no DB/broker-backed unit tests, and
# the full godog suite needs the whole fleet up — see 'make run').
check-all: check

up:
	bash scripts/02-up-infra.sh
	bash scripts/01-build.sh
	bash scripts/03-up-services.sh

run:
	bash scripts/04-run-tests.sh

soak:
	bash scripts/06-run-soak.sh

down:
	bash scripts/05-down-services.sh
