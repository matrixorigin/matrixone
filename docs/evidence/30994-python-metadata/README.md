# Python metadata row-count validation

Retained live validation for [PR #30994](https://github.com/matrixorigin/matrixone/pull/30994), addressing its request to execute the complete online metadata suite and prove all four exact-50 assertions. This is fresh validation on the frozen PR source, not a restatement of the earlier author-reported Linux release run.

## Source and frontend

Tested SDK head: `ea208d535537e6adb5307500e2d1b606fee39aae`. The evidence-only follow-up preserves its SDK/test trees and three changed Python file hashes; see [source-identity.json](source-identity.json.md). The SDK was imported from the isolated PR checkout, as recorded by [python-environment.json](python-environment.json.md).

Client interpreter: Python 3.12.8, x86_64 under macOS; SQLAlchemy 2.1.6. The independent native frontend is darwin/arm64, clean main `94faf0209b9ac3254d31906f92e34405f1988203`. It is a reused server binary, **not a build of the PR head**. Its binary SHA256, original clean build command/source/exit, and Go build metadata are retained in [frontend-receipt.json](frontend-receipt.json.md), [server-build-receipt.json](server-build-receipt.json.md) and [server-buildinfo.txt](server-buildinfo.txt.md). Both metadata producer/wire source files are byte-identical between the SDK merge base and this server commit; their Git blobs and checked compatibility boundary are recorded in [source-identity.json](source-identity.json.md).

The launcher creates a private local frontend with fresh task-owned data, UUIDs and loopback ports. Before starting it, the selected ports must be unused. It creates the initial test database, executes the tests, then SIGTERMs and waits for its owned process in a finally block. [frontend-receipt.json](frontend-receipt.json.md) records the process exit 0. These are local fixture configurations; password 111 is the public test root password, not an external-service credential.

## Executed checks

| Requirement | Retained proof |
| --- | --- |
| Complete owning offline metadata tests | [offline.log.txt](offline.log.txt.md), [offline.xml.txt](offline.xml.txt.md) and [checks-receipt.json](checks-receipt.json.md): 26 passed |
| Entire online metadata file | [online.log.txt](online.log.txt.md), [online.xml.txt](online.xml.txt.md) and [online-receipts.json](online-receipts.json.md): 31 passed, zero skipped |
| Synchronous Client exact 50 | TestMetadataOnline.test_table_brief_stats PASSED |
| Asynchronous Client exact 50 | TestAsyncMetadataOnline.test_async_table_brief_stats PASSED |
| Synchronous Session exact 50 | TestMetadataSession.test_session_brief_stats PASSED |
| Asynchronous Session exact 50 | TestAsyncMetadataSession.test_async_session_brief_stats PASSED |
| Nonempty selection/individual assertions | [exact-50-tests.json](exact-50-tests.json.md) records all four named cases; assertion source is bound by [source-identity.json](source-identity.json.md) |
| Overlapping async execution | [async_challenge.py.txt](async_challenge.py.txt.md), [async-challenge.log.txt](async-challenge.log.txt.md) and [async-challenge-results.json](async-challenge-results.json.md): eight released-together tasks, four Client calls and four independent Session calls, all row_cnt=50 |
| Unhappy path/reuse/cleanup | Two missing-table errors propagate, the following call returns 50, pool checked-out count 0 and client engine None after disconnect |
| Incremental Python static checks | [checks-receipt.json](checks-receipt.json.md) exact configured fatal flake8 flags plus py_compile for all three changed Python files, all exit 0; no gratuitous formatting changes |

No Go source is changed, so Go -race is not applicable. Standard CPython has no repository-supported race detector. The bounded async gather/session/error probe is concrete public async validation, not a claim of sanitizer instrumentation. No shared mutable aggregation state is introduced: the helper's dictionary is call-local and reuses the object identity already used for object count.

## Reproduction

The receipt commands ran from clients/python in the isolated checkout with a private editable installation. The archived .py.txt launch/diagnostic sources and .toml.txt configs are byte-for-byte executed inputs; the suffix prevents automatic execution from this archive. [server-buildinfo.txt](server-buildinfo.txt.md) is a display copy with trailing whitespace removed. [server-buildinfo.txt.gz](server-buildinfo.txt.gz.md) preserves every original byte; [server-buildinfo-archive.json](server-buildinfo-archive.json.md) records the original/display hashes and normalization. Decompress and verify the original hash when auditing raw metadata. Production/test/evidence text passes the repository whitespace check. [SHA256SUMS](SHA256SUMS.md) covers every file except itself. Machine paths are retained for provenance and must be adjusted together when rerunning.

1. Check out the recorded SDK head. Create a Python 3.12 virtual environment, install `-e './clients/python[test]' pytest-subtests flake8`, and retain/pin the versions listed in [python-packages.txt](python-packages.txt.md). Confirm the imported matrixone module is that checkout.
2. Build or select the recorded clean server revision using the repository native-build instructions. The recorded native binary is not included. Keep an isolated data root and unused loopback ports. Install the configs with .toml names, update all paths and the launcher binary/library roots together; do not point the harness at another running instance.
3. Copy [run_online.py.txt](run_online.py.txt.md) and [async_challenge.py.txt](async_challenge.py.txt.md) to .py names outside tracked source. Run run_online.py with the private interpreter. It connects without an initial database, creates test, runs the **whole online file in definition order**, then runs the async probe. The existing online fixture persists its test_metadata_db across its classes; selecting only a Session class does not reproduce the same setup contract.
4. Run the offline and static commands from [checks-receipt.json](checks-receipt.json.md). Inspect named pass results, source identities, terminal exits and exact log digests before reusing the evidence.

The SDK keeps one rows_cnt per object_name while preserving per-column size/null sums. No engine, wire, SQL, transaction, extra query, persistent state or new execution path is changed. Runtime complexity remains O(metadata rows) time and O(unique objects) space; no benchmark speedup is claimed. Earlier full-offline/release-server counts in the PR were historical reports and are not substituted for this bundle. New-head CI and reviewer reapproval remain separate checks.

## Exact artifact extraction

Only Markdown presentation files are added so this documentation does not expand the Python SDK PR into full kernel CI. Each artifact page is readable and records its original filename, byte length and SHA256. Fences may add a presentation newline; extraction slices bytes by the recorded stored length. The gzip page stores base64, decoded before checking its original length/hash. No receipt payload has been edited. original-README.md is the original bundle README payload. WRAPPER-SHA256SUMS.md covers all delivered presentation files except itself.

Run this from the repository root; it creates a fresh directory outside tracked source and reconstructs every original artifact, including the original SHA256SUMS. Then run its checksum verification.

```python
from pathlib import Path
import json, hashlib, base64, subprocess
source = Path("docs/evidence/30994-python-metadata")
output = Path("/tmp/pr30994-evidence-extracted")
output.mkdir(exist_ok=False)
for page in source.glob("*.md"):
    raw = page.read_bytes()
    fields = [line[10:] for line in raw.splitlines() if line.startswith(b"Artifact: ")]
    if not fields:
        continue
    meta = json.loads(fields[0])
    assert Path(meta["name"]).name == meta["name"]
    body = raw.split(b"\n````", 1)[1].split(b"\n", 1)[1]
    payload = body[:meta["stored_bytes"]]
    if meta["encoding"] == "base64":
        payload = base64.b64decode(payload)
    assert len(payload) == meta["bytes"]
    assert hashlib.sha256(payload).hexdigest() == meta["sha256"]
    (output / meta["name"]).write_bytes(payload)
subprocess.run(["shasum", "-a", "256", "-c", "SHA256SUMS"], cwd=output, check=True)
```
