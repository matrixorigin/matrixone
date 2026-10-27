# Recorded SDK compatibility execution

[sdk-quality-results.json](sdk-quality-results.json) records five executed local
runs with Python/dependency versions, test selections, commands, terminal counts,
observed server identities and one shared SHA256 input manifest. It is execution
evidence, not an assertion that GitHub online CI ran.

| Target | Result | Additional subtests |
| --- | --- | --- |
| SQLAlchemy 1.4.54, offline | 1954 passed, zero skips | 95 passed |
| SQLAlchemy 2.0.43, offline | 1950 passed, zero skips | 95 passed |
| SQLAlchemy 2.1.6, offline | 1958 passed, zero skips | 95 passed |
| MatrixOne 4.2.5, git `1a44f3d` | 70 passed, zero skips | — |
| Source banner v20261026, git `305be60de8` | 70 passed, zero skips | — |

The native JUnit files retain each testcase and the observed server properties:
[sdk-quality-release.junit.xml](sdk-quality-release.junit.xml) and
[sdk-quality-source.junit.xml](sdk-quality-source.junit.xml). Reports omit local
hostnames, absolute workspace paths and credentials. The source binary was built
from main `83c1e23526`; later merged kernel changes were not rebuilt or claimed
as tested. These local Python 3.13.5 results complement GitHub's separately
reported Python 3.10/3.11/3.12 offline matrix.

The recorded Git revision preceded committing the tested changes. The manifest
identifies the actual dirty content: every production Python file, all test and
fixture Python files, the runner and packaging/pytest configuration. Each run
verified the manifest stayed unchanged during execution. Generated reports and
prose are excluded, so publishing this evidence does not invalidate it.

To reconcile a checkout with the tested inputs, run from `clients/python`:

```python
import hashlib
import json
from pathlib import Path

report = json.loads(Path('docs/validation/sdk-quality-results.json').read_text())
for name, expected in report['source_sha256'].items():
    assert hashlib.sha256(Path(name).read_bytes()).hexdigest() == expected, name
for run in report['runs']:
    if 'junit' in run:
        path = Path('docs/validation') / run['junit']
        assert hashlib.sha256(path.read_bytes()).hexdigest() == run['junit_sha256']
```

See [TESTING.md](../../TESTING.md) for runner setup and required target variables.
Reproduce with `python scripts/run_contracts.py --output contract-results` or
add `--server 4.2.5` / `--server current-source` against an isolated target. The
server tests create and remove uniquely named databases, tables and files.
The new cases assert legacy execution, mapped schema identity, alias DML,
rejected joined mutations, recursive results, async client/session database
isolation, exact export contents and connection reuse after errors. Existing
contracts also prove native mapped-file completion, EOF/precommit failure,
rollback, one-connection-pool reuse and fulltext membership.

Decimal qualification: both servers preserve the large DECIMAL(38,18) and small
DECIMAL(38,27) adjacent values used for exact read/update/delete scope. On 4.2.5,
a separate direct native INSERT control for some 38-fractional-place values
rounds a last digit even without SDK binding. The SDK does not bypass that native
limitation; fixed-point literal unit tests still cover all 38 fractional places.
