# Parser conflict resolution against main

PR source: `6b38fcd09af96073175588c6eac78fbfe3d9aee5`.
Merged main: `75ff736835c3d00d40dc00a0a1d3b11129388e5b`.

Only generated `pkg/sql/parsers/dialect/mysql/mysql_sql.go` conflicted. The
MySQL grammar merged cleanly, retaining TTL and main's empty-CTE-body rejection.
Regenerate the complete parser from this grammar; do not select one parent's
parse tables. Generation reports no conflicts. Other overlapping compile and
planner files merge automatically; no TTL implementation change is introduced.
Prior review corrections, independent cron registration and validation records
remain in the branch.

Commands under the repository Go environment:

```sh
(cd pkg/sql/parsers/dialect/mysql && go run github.com/matrixorigin/matrixone/pkg/sql/parsers/goyacc -o mysql_sql.go mysql_sql.y && gofmt -w mysql_sql.go)
.agents/skills/mo-dev/scripts/mo-cgo-test ./pkg/sql/parsers/dialect/mysql ./pkg/sql/plan ./pkg/sql/ttl -count=1
.agents/skills/mo-dev/scripts/mo-cgo-test ./pkg/tests/sqlintegration ./pkg/cnservice -run '^(TestTTL|TestCronRegistration)' -count=1
```

Build a fresh worktree-local CPU library using `make -C cgo -j4` and the
unchanged primary thirdparty installation. The CGo and thirdparties source
trees are unchanged between the PR head and merged main. No primary artifact
or provenance stamp is modified. Tests run through the repository wrapper.
Logs and source identities are retained alongside this record. Existing race
and stress evidence remains historical: the conflict resolution changes no
concurrency implementation or test contract, and does not claim fresh stress
or performance validation. Remote CI must run on the new merge commit.
