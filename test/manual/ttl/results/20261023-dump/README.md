# TTL ALTER dump replay correction

Addresses P2 in review 5618378841 on PR #30072 at
`13780b807a0a92fa1580edbeac04656132c95199`.
First merge main `0594cdb955` and regenerate the only conflicted file,
`pkg/sql/parsers/dialect/mysql/mysql_sql.go`, from the cleanly merged grammar.
Merge commit `f91bd4f582` retains the unfixed replay-DDL production bytes.

## Contract and first owner

A dump's CREATE SQL must reproduce the current logical definition whose hash
LOAD verifies. Stored Createsql is historical CREATE provenance; TTL ALTER
changes structured constraints without keeping that string synchronized.
Schema hashing already reconstructs ordinary supported definitions, whereas
manifest generation normally returned the historical string. This divergence
makes dumps reject their own reconstructed tables after add/modify/remove TTL.

Use the existing dump reconstruction owner for every reconstructible definition,
matching the schema hash's current-definition boundary. Retain the existing
non-reconstructible fallback and legacy-type/collation normalization. The owner
clones catalog metadata before reconstruction and keeps expression witnesses.
No TTL-specific string patch, catalog protocol, transaction state, or worker
retry/scheduling behavior changes. Remove the redundant BLOB-only rebuild gate:
ordinary canonical reconstruction already preserves that subtype. The change
adds one bounded schema reconstruction at DUMP, proportional to the definition;
it does not add work to foreground SELECT/DML or TTL cleanup. No new process,
concurrency state, queue, timer, or lifecycle owner is introduced, so new race
stress is not selected for this stateless metadata correction.

## Oracles, fixture cost and negative control

`TestTableDumpManifestCreateSQLUsesCurrentPolicy` checks typed parsed policy
identity/count for unchanged/add/modify/remove states and proves the shared
catalog definition remains protobuf-equivalent.

`TestTTLDumpReplayAfterAlter` reuses the canonical shared one-CN fixture and
MySQL entry point. Eight cases cover the four policy states in metadata-only
and populated modes. Each case creates one table, reads the manifest, drops the
source, replays only manifest.create_sql, and LOADs from that dump. SHOW CREATE
is a post-load oracle, never a repair of the input. Populated cases flush and
restore one row with a fractional timestamp and payload; both modes verify the
restored default by inserting another row. Temp directories and the database,
context, client and connection have lexical cleanup. No sleeps or new topology.

Before the fix, the unchanged controls do not fail; all six mutation cases fail
LOAD with `target table schema does not match table dump`. Production bytes are
those of the merged baseline, with the regression test overlaid. The terminal
failure excerpt is retained in before.txt with indentation/trailing whitespace
normalized. This includes populated restoration, extending the review's empty
metadata-only counterexample. The same eight cases pass after the fix.

## Reproduce and validation scope

With the repository Go/native environment configured:

```sh
.agents/skills/mo-dev/scripts/mo-cgo-test ./pkg/tests/sqlintegration -run '^TestTTLDumpReplayAfterAlter$' -count=1
.agents/skills/mo-dev/scripts/mo-cgo-test -json ./pkg/frontend -run 'TableDump|TableSchemaHash|DumpTable|LoadTable' -count=1
.agents/skills/mo-dev/scripts/mo-cgo-test -json ./pkg/sql/parsers/dialect/mysql ./pkg/sql/plan ./pkg/sql/ttl -count=1
.agents/skills/mo-dev/scripts/mo-cgo-test -json ./pkg/tests/sqlintegration ./pkg/cnservice -run '^TestTTL|^TestTimestamp(DefaultDumpRoundtrip|AddHistoricalDumpRoundtrip)$|^TestCronRegistration' -count=1
```

The manual verify.sh now includes dump-owner tests, the new native TTL replay
case, and existing default/historical-expression dump compatibility controls.
Only the commands recorded here were executed for this correction; the complete
manual script, service build and fuzz/race workloads are not claimed as rerun.
The selected frontend and native suites cover existing legacy text/BLOB,
collation, expression binding, defaults, historical timestamp behavior and dump
validation contracts. Existing full parser/planner/TTL suites cover the merged
consumers. Incremental configured lint covers frontend, sqlintegration and the
regenerated parser; the repository error-construction check and script/diff
checks also pass. Counts, terminal test events, exact source/log fingerprints
and command exits are retained with the manifest. Verbose logs remain in the
execution workspace. Native input/artifact fingerprints from 20261022-merge
remain applicable: this main merge does not change CGo/thirdparty inputs.

The self-review covers every changed hunk: shared replay-DDL selection and
fallback, removal of its obsolete subtype gate, typed/public regressions and
cleanup, compatibility consumers, both language designs, verification script,
and parser regeneration. The independent product syntax/visibility/performance
review is not treated as resolved by this restore bug fix.
