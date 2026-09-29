Feature: Sustained backlog-ramp soak across pickers and packers
  A dedicated, long-running (@soak) scenario: ramps injected backlog into
  BOTH the PICK and PACK process paths over a configurable duration
  (default 1 hour — see scripts/06-run-soak.sh) while a configurable pool
  of picker and packer stations continuously claims and completes the
  resulting tasks. This proves the estate stays up and keeps processing
  under sustained, ramping load, not just a single deterministic unit of
  work like bootstrap.feature.

  This scenario is EXCLUDED from the default `go test`/scripts/04-run-
  tests.sh run: e2e_test.go's TestMain filters out `@soak` unless
  GODOG_TAGS explicitly includes it, because it can legitimately run for
  an hour and would make every ordinary local/CI-adjacent run that long.

  It is deliberately OBSERVATIONAL, not assertive on backlog/queue depth:
  once picker/packer throughput approaches the ramped injection rate,
  wes-work-planning's `/release` legitimately returns 409 (WIP limit
  reached / pool empty) and fulfillment-execution's `/claim-next`
  legitimately returns 409 (no claimable task) — both are EXPECTED under
  ramping load, not failures. soak_test.go's runRamp prints a metrics
  summary at the end and only fails the scenario on a hard
  connectivity/setup problem (e.g. it never managed to enqueue a single
  work unit in the whole run).

  Background:
    Given all warehouse-systems services are healthy

  @soak @e2e
  Scenario: Ramp backlog into PICK and PACK while pickers and packers continuously process it
    Given wes-work-planning has release-fed work pools for soak process paths "pick-soak" and "pack-soak" with the configured WIP limit
    And the configured picker and packer stations are registered in fulfillment-execution for soak
    When backlog is ramped into process paths "pick-soak" and "pack-soak" for the configured soak duration with pickers and packers continuously processing
    Then the soak run summary is printed
