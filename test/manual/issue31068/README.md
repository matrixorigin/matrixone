# SHOW CREATE comment replay and persisted DUMP compatibility

[PR #31069](https://github.com/matrixorigin/matrixone/pull/31069), [issue #31068](https://github.com/matrixorigin/matrixone/issues/31068).

SHOW CREATE must escape backslashes in stored column/table comments in default SQL mode, preserve already-doubled quotes, and leave backslashes literal under NO_BACKSLASH_ESCAPES. Its output is replayable SQL. DUMP's existing schema fingerprint instead needs the historical column/table comment representation: changing those bytes rejects an unchanged schema from an old archive. The dedicated hash-only planner entry preserves that representation; it does not change index/default quoting, archive versions, strict LOAD equality or existing normalization/fallback branches. It must never be used to produce replay SQL.

The compatibility fixture in `test/distributed/resources/table_dump_comment_legacy` was produced by a real server with frozen main `12eeeb0205b5e87faef48660ddbcd8cb5e18baea`. Its manifest, READY and two object files are unedited, 3,884 bytes total. It contains two main rows and a secondary index, with backslashes/quotes in column, table and index comments and a backslash in a default. The fixture README and manifest record provenance and object size/SHA256. Keeping this old archive immutable provides an independent regression oracle; do not regenerate it with current code.

`comment/comment_backslash_roundtrip.sql` loads the old full archive, checks both rows and a FORCE INDEX predicate, then checks current metadata DUMP/CREATE LIKE/LOAD, preserved comment bytes, mismatch rejection with an empty target, and stage/database cleanup. The original SHOW/replay/HEX oracles are retained. New tests are not skipped or ignored. The added data consists of two rows; it is a compatibility check, not a scale workload.

## Reproduction

Use the repository's Go1.27.1 and verified CPU native artifacts. Follow `.agents/skills/mo-dev/references/cgo-build-test.md`; `mo-cgo-test` supplies the repository's compile/link/runtime native setup.

```sh
GOTOOLCHAIN=go1.27.1 .agents/skills/mo-dev/scripts/mo-cgo-test -count=1 \
  -run 'TestShowCreateTableCommentBackslashRoundTrip' ./pkg/sql/plan
GOTOOLCHAIN=go1.27.1 .agents/skills/mo-dev/scripts/mo-cgo-test -count=1 \
  -run 'TestTableSchemaHash(StoredCommentCompatibility|StillRejectsCommentAndSchemaDifferences)' ./pkg/frontend
```

Use mo-tester normal comparison against an owned, ready instance (SQL and ISCP startup), with `-n -g -o -p test/distributed/cases/comment -s test/distributed/resources`. Do not accept regenerated results without reviewing their values/errors. The retained evidence records the changed case, whole comment group, teardown checks, and same-instance repeat.

For independent producer/consumer validation, build a baseline service from frozen main and the candidate service with `make build-with-prebuilt-native`. Put them at `$PR31069_RUNTIME_ROOT/mo-service-baseline` and `mo-service-candidate`. The recorded baseline used a two-file Go overlay for `pkg/sql/plan/build_show_util.go` and `pkg/frontend/table_dump.go`, pinning both to main. Those are the only production changes. Its binary commit metadata therefore identifies the checkout rather than the overlaid production contents; use the source snapshots/build commands/binary hashes in `evidence.json`.

Create separate fresh `producer` and `consumer` directories with log/tn/cn/launch configs, using the recorded runtime config contents in `evidence.json`, and adjust the absolute launch paths to those directories. Each directory's dict link points to the repository's staged dict. Ports16869/17869/17870, CN29400, TN30400, and log/raft/gossip33401/33400/33402 must be unused. Run the clusters sequentially; share only the archive directory, not mo-data. Both source and target schemas are created explicitly so CREATE LIKE's former replay bug cannot hide a schema difference.

```sh
export PR31069_REPO=/absolute/path/to/candidate/checkout
export PR31069_RUNTIME_ROOT=/absolute/path/to/fresh/runtime
export PR31069_EVIDENCE_DIR=/absolute/path/to/results
python3 -m venv "$PR31069_RUNTIME_ROOT/venv"
"$PR31069_RUNTIME_ROOT/venv/bin/pip" install pymysql==1.1.2
"$PR31069_RUNTIME_ROOT/venv/bin/python" cross-version.py produce
"$PR31069_RUNTIME_ROOT/venv/bin/python" cross-version.py consume
"$PR31069_RUNTIME_ROOT/venv/bin/python" cross-version.py reverse
```

The producer flushes both main and index relations before real full DUMP. The consumer asserts full data, HEX comment parity, FORCE INDEX results, unchanged old archive hashes and strict rejection of changed column/table/index comments, types and defaults. It emits a new archive with the same schema fingerprint; the old server then loads it and asserts data/index/comment parity. METADATA ONLY retains object references and is not an empty-data archive; its check runs after full LOAD has made the referenced objects available. The harness owns and closes its connections/process groups, records startup and terminal results, and preserves unexpected errors instead of masking them with cleanup errors.

The recorded production code is `4c797463ad`; `51b5fda9c0` subsequently adds test AST cleanup only. No SELECT/DML hot path, new shared state or new persistence version is introduced. Context normalization and typed errors stay with the existing owners. Evidence exclusions and mode-specific checks are explicit in `evidence.json`; pending CI is not a local pass.
