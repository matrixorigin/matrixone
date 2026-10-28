# MatrixOne Adapter

Use only the sections needed for the current probe. This adapter supplies project
entry points and durable decisions; it does not prescribe a full test campaign.

## Instance and connection, when executing SQL

Reuse a known test instance when suitable. Record its actual build/version,
configuration, and topology; the checkout's HEAD alone does not identify a running
binary. If startup is needed, use an isolated launch configuration based on
`etc/launch/launch.toml` and its CN/TN/log configs, with test-owned storage paths
and nonconflicting ports. Follow the existing
[build guidance](../../mo-dev/references/cgo-build-test.md) if a build is needed.

```bash
# MO_LAUNCH_CONFIG is the prepared, isolated launch config, not a generated default.
./mo-service -launch "$MO_LAUNCH_CONFIG"
```

Use the frontend port from that configuration as `MO_PORT`. Run a no-database
`SELECT VERSION(), 1` connection check before selecting or creating a probe
schema. Reuse existing readiness evidence when it is still valid. Change
`enable-auto-id-cache` only for a probe that actually needs that comparison.

## Source entry points, when tracing

| Layer | Path |
|---|---|
| Syntax | `pkg/sql/parsers/dialect/mysql/mysql_sql.y` |
| AST | `pkg/sql/parsers/tree/*.go` |
| Plan/catalog structure | `pkg/sql/plan/build_*.go` -> `pkg/pb/plan/plan.pb.go` |
| Execution | `pkg/sql/colexec/*`, `pkg/sql/compile/*`, `pkg/frontend/*` |
| Introspection | `information_schema.*`, `SHOW CREATE TABLE/VIEW/INDEX` |

Search a field and its accessors with `rg -n '<field-or-accessor>' pkg/`, then
follow transformations and consumers. Generated serialization/getter code may
be part of that path; a reader count alone is not a behavioral verdict.

## Optional SQL probe helper

For independent SQL cases, this example stores complete raw streams on disk and
prints their locations. SQL that depends on session state belongs in one call;
use persistent connections for interactive or multi-session experiments.
Set `MO_PORT` to the chosen instance's port. The helper uses TCP and ignores
client option/login files so implicit databases, init commands, or local sockets
cannot redirect the probe. The user/password defaults below are for a local
test instance; pass any additional connection options explicitly. Keep the
resulting evidence directory local.

```python
import json
import os
from pathlib import Path
import subprocess
import tempfile

BASE = ['mysql', '--no-login-paths', '--protocol=TCP',
        '-h', os.environ.get('MO_HOST', '127.0.0.1'),
        '-P', os.environ['MO_PORT'],
        '-u', os.environ.get('MO_USER', 'dump'),
        '-N', '--raw']
EVIDENCE = Path(tempfile.mkdtemp(prefix='mo-defect-hunt-'))

def show(name, sql, db=None, timeout=120):
    case = Path(tempfile.mkdtemp(prefix='case-', dir=EVIDENCE))
    (case / 'query.sql').write_text(sql, encoding='utf-8')
    result = {'case': name, 'database': db, 'timeout_seconds': timeout,
              'status': 'exited', 'rc': None, 'evidence': str(case)}
    password = os.environ.get('MO_PASS', '111')
    if any(char in password for char in ('\r', '\n', '\x00')):
        raise ValueError('MO_PASS contains unsupported control characters')
    try:
        # MySQL requires --defaults-file before ordinary client options.
        # Use an owner-only, per-call file instead of exposing the secret in argv.
        with tempfile.TemporaryDirectory(prefix='mo-probe-auth-') as auth_dir:
            auth_file = Path(auth_dir) / 'client.cnf'
            fd = os.open(auth_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, 'w', encoding='utf-8') as config:
                config.write('[client]\npassword=' + json.dumps(password, ensure_ascii=False) + '\n')
            command = [BASE[0], f'--defaults-file={auth_file}', *BASE[1:]]
            command += (['-D', db] if db is not None else []) + ['-e', sql]
            with (case / 'stdout').open('wb') as out, (case / 'stderr').open('wb') as err:
                p = subprocess.run(command, stdout=out, stderr=err, timeout=timeout)
                result['rc'] = p.returncode
    except subprocess.TimeoutExpired:
        result['status'] = 'timeout'
    except OSError as exc:
        result.update(status='launch-error', error=str(exc))
    (case / 'result.json').write_text(json.dumps(result) + '\n', encoding='utf-8')
    print(json.dumps(result), flush=True)
    return result
```

`show("ready", "SELECT VERSION(), 1")` does not select a possibly nonexistent
database. For stateful cases, create and clean only the schema owned by the
probe. `status=exited` describes client completion, not whether an assertion
passed; inspect `rc` and the raw streams. Timeout preserves partial output and
terminates the local client; it does not establish server cancellation. Inspect
server work before retrying. Preserve binary/text boundaries in comparisons;
choose explicit SQL predicates when client rendering is ambiguous.

## Issue search and policy, when preparing an issue

```bash
gh issue list --repo matrixorigin/matrixone --state all --search "<phrase>" --limit 100 --json number,title,state,url
gh issue view <n> --repo matrixorigin/matrixone --json state,stateReason,labels,body,comments,closedByPullRequestsReferences
```

Narrow or paginate if results hit the limit. Read the discussion and linked fix
before classifying a closed issue. Use the repository's
`.github/ISSUE_TEMPLATE/bug_report.yml` and
[mo-bug-triage](../../mo-bug-triage/SKILL.md) for intake/label policy; the template
currently specifies `kind/bug`, `needs-triage`, and assignee `matrix-meow`.
Only perform writes covered by the user's authorization.

## Prior evidence and decisions

The [probe ledger](matrixone-ledger.md) supports cross-round reuse; append only
when there is useful evidence to retain. Dates include timezone, normally
`Asia/Shanghai (UTC+8)`. Record per-task exclusions in the campaign record. No
subsystem, including partitioned tables, is excluded by this adapter.

| Behavior | Ruling | Source | Date recorded (UTC+8) |
|---|---|---|---|
| `INVISIBLE` indexes still participate in path selection | Intentional metadata-only behavior; verify the ruling's scope if the contract changes | [Maintainer clarification on #26862](https://github.com/matrixorigin/matrixone/issues/26862#issuecomment-6356340741) | 2026-10-23 |
