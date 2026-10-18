# Main merge and parser conflict resolution

Merge prior PR head `38e336d9b30aa57887d347dc577b80791d2238ad` with
main `e8b3ed7f21c2b1966d74814adaa2bcd96d8c129f`.
The only conflicted file was generated `mysql_sql.go`. The grammar merged
cleanly; regenerate the complete parser from that merged grammar rather than
editing generated parse tables. The generator reports no conflicts.

Commands (repository-configured Go environment):

```sh
(cd pkg/sql/parsers/dialect/mysql && go run github.com/matrixorigin/matrixone/pkg/sql/parsers/goyacc -o mysql_sql.go mysql_sql.y && gofmt -w mysql_sql.go)
.agents/skills/mo-dev/scripts/mo-cgo-test ./pkg/sql/parsers/dialect/mysql ./pkg/sql/plan ./pkg/sql/ttl -count=1
.agents/skills/mo-dev/scripts/mo-cgo-test ./pkg/tests/sqlintegration -run '^TestTTL' -count=1
```

The wrapper rejected primary-worktree artifact reuse because main changes the
tracked native build inputs. Built this worktree's CPU library with `make -C cgo
-j4`, using the unchanged thirdparty installation from the primary worktree
(`thirdparties` and `cgo` source trees have no upstream changes). Tests use the
repository wrapper and the freshly compiled local library. No provenance stamp
was fabricated and no primary native artifact was changed.

Also normalize whitespace in the previous negative-control excerpt and update
its log hash. This changes evidence formatting only. `git diff origin/main
--check` passes; upstream snapshot result formatting is retained unchanged.
