# E2E Test Plan — how to use

Catalogue: [`e2e-test-cases.csv`](e2e-test-cases.csv) (167 cases). Tracking: epic **#40** and child issues **#27–#39** (one per group).

## Columns
| Column | Meaning |
|---|---|
| TC_ID | Unique ID, prefix = group (DEP, ING, AUTH, LOG, NODE, RST, WH, SEC, RES, PERF, K8S, UI/CI, CMP) |
| Group / Feature / Title | What is being tested |
| Type | Functional, Negative, Security, Resilience, Performance, Exploratory |
| Priority | P0 release blocker · P1 must pass or be waived · P2 should · P3 nice |
| Claim_Link | Public claim (CL-1…CL-6) this case provides evidence for |
| Preconditions / Test_Steps / Expected_Result | The test itself (`$BASE`=`https://localhost`, `$KEY`=API key, `$ADMIN`=admin secret) |
| Automation | Manual · Automatable (script) · Automated (unit) |
| Existing_Automated_Coverage | Existing test/CI job that already covers all or part of it |
| GitHub_Issue | Child issue that tracks the case |
| Status … Evidence_Link | **Fill during execution**: `Not Run`/`Passed`/`Failed`/`Blocked`/`Skipped`, actual result, defect ref, who, when, commit SHA, evidence |

## Environment
- Linux host, Docker + Compose v2, ≥ 8 GB RAM free, `curl`, `jq`, `openssl`, Python 3.11, Go 1.22 (for `go test`).
- A webhook receiver: <https://webhook.site> (public https) or a local server with `WEBHOOK_ALLOW_PRIVATE_TARGETS=true`.
- `export BASE=https://localhost KEY=<issued key> ADMIN=<ADMIN_SECRET>` and use `curl -k`.
- Kubernetes cases: kind/minikube (+ metrics-server for HPA).

## Suggested order
DEP → ING → AUTH → LOG → NODE → RST → WH → SEC → CI → UI → RES → PERF → K8S → CMP (CMP last: it reuses evidence from earlier groups to back the claims).

## Defects
Title `[TC-ID] summary`, label `bug`, link the TC and attach logs. Put the issue number in `Defect_Ref`.

## Positioning (what the evidence must support)
Kibana tells you what happened. DLS notices, fixes it and tells you — over a multi-tenant API, with zero application changes. DLS complements Kibana (it is included in the compose stack); it does not replace Kibana's analytics. See CL-1…CL-6 in epic #40 and the "non-goals" case CMP-009.

## Automated baseline
`pytest tests -q` and `cd log_pipeline && go test ./...` (also run in CI) cover the unit-level parts referenced in the *Existing_Automated_Coverage* column. They do not replace E2E runs against a live stack.
