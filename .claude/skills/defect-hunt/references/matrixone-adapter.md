# MatrixOne Adapter

Corresponds to the core method in `defect-hunt/SKILL.md`. This file is allowed to rot: conclusions carry dates and sources; re-verify before retesting.

## Instance

```bash
cd <repo> && ./mo-service -launch /tmp/<dir>/launch.toml     # 1 CN / 1 TN / 1 LOG standalone
mysql -h127.0.0.1 -P16001 -udump -p111 -D <db>
```

Config: `cn.toml` / `tn.toml` / `log.toml`; `[cn.frontend] port` sets the connection port; `data-dir` isolates instances. Toggle `enable-auto-id-cache` for counter-style A/B tests.

## Where the five-question trace lands

| Layer | Path |
|---|---|
| Syntax | `pkg/sql/parsers/dialect/mysql/mysql_sql.y` (productions are authoritative) |
| AST | `pkg/sql/parsers/tree/*.go` |
| Plan/catalog structure | `pkg/sql/plan/build_*.go` -> `pkg/pb/plan/plan.pb.go` |
| Execution | `pkg/sql/colexec/*`, `pkg/sql/compile/*`, `pkg/frontend/*` |
| Introspection | `information_schema.*`, `SHOW CREATE TABLE/VIEW/INDEX` |

Reader counting:

```bash
grep -rn "<ProtoField>" pkg/ --include=*.go | grep -v _test | grep -v pb/plan
```

## Battery skeleton

```python
import os, subprocess
BASE = ['mysql','-h'+os.environ.get('MO_HOST','127.0.0.1'),
        '-P'+os.environ.get('MO_PORT','16001'),
        '-u'+os.environ.get('MO_USER','dump'), '-p'+os.environ.get('MO_PASS','111'),
        '-N','--raw','-D', os.environ.get('MO_DB','probe')]
def show(name, sql, timeout=120):
    p = subprocess.run(BASE+['-e',sql], capture_output=True, text=True, timeout=timeout)
    out = ' // '.join(p.stdout.strip().splitlines())[:400]
    err = ' '.join(p.stderr.replace('mysql: [Warning] Using a password on the command line interface can be insecure.','').splitlines())[:250]
    print(f"CASE {name}\n  rc={p.returncode} out=[{out}] err=[{err}]", flush=True)
```

`--raw` is mandatory (otherwise batch output escapes newlines as `\n`); one process per case; reset with `DROP DATABASE IF EXISTS` first.

## Dedup

```bash
gh issue list --repo matrixorigin/matrixone --state all --search "<phrase>" --limit 5 --json number,title,state
gh issue view <n> --repo matrixorigin/matrixone --json state,stateReason,labels
```

Metadata convention: `kind/bug`, assignee `matrix-meow` (details in `mo-bug-triage`).

## Ledger

`references/matrixone-ledger.md`, append-only; read before probing. Dates in `Asia/Shanghai (UTC+8)`.

## Known "by design" (with sources)

| Behavior | Ruling | Source | Date recorded (UTC+8) |
|---|---|---|---|
| `INVISIBLE` indexes still participate in path selection (metadata marker only) | Intentional | Maintainer reply; related issue closed as NOT_PLANNED | 2026-10-23 |

Partitioned-table behaviors are out of probing scope per review request (not "verified" — "agreed not to test").

## Worked examples (showing where the breakpoint lands)

| Issue | Class / breakpoint form | Breakpoint |
|---|---|---|
| #30898 | Metadata/behavior divergence | rename path has no viewdef rewrite |
| #30900 | Accepted but no-op | statement classified, no executor |
| #30905 | Silently dropped | index structure has column names only, no direction field |
| #30906 | Partially effective | not-null check ignores the IGNORE mode parameter |
| #30909 | Produced but never consumed | read-only txn marker has zero consumers |
| #30915 | Silent relaxation (fail-open) | privilege structure has no column list field -> widened to whole table |

This table only illustrates breakpoint shapes, **not a covered list**; re-confirm status per §6 each round.
