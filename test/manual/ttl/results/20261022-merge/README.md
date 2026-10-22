# Main merge and parser conflict resolution

PR parent: `804bd56493eb4c0e4e312318a2eca6dc6590bdb5`.
Main parent: `75ae9a79d626622e6ccec0bcb28412f5fcf84c7f`.

Only generated `pkg/sql/parsers/dialect/mysql/mysql_sql.go` conflicted. The
source grammar merged automatically. Regenerate the entire parser from that
merged grammar, retaining TTL and main's newer locking and view syntax.
Generation reports no conflicts; a second generation has identical SHA256.
No TTL product semantics or worker ownership are changed by this resolution.

Commands under the repository Go environment:

```sh
(cd pkg/sql/parsers/dialect/mysql && go run github.com/matrixorigin/matrixone/pkg/sql/parsers/goyacc -o mysql_sql.go mysql_sql.y && gofmt -w mysql_sql.go)
make -C cgo clean
make -C cgo -j4
.agents/skills/mo-dev/scripts/mo-cgo-test ./pkg/sql/parsers/dialect/mysql ./pkg/sql/plan ./pkg/sql/ttl -count=1
.agents/skills/mo-dev/scripts/mo-cgo-test ./pkg/tests/sqlintegration ./pkg/cnservice -run '^(TestTTL|TestCronRegistration)' -count=1
```

Main changes CGo sources and headers, so all worktree-local CGo outputs are
cleaned and rebuilt. Thirdparty installation is copied from the primary
worktree into a real worktree-local directory: upstream's only thirdparty build change adds the arm64 portable
architecture branch; this host uses x86_64/haswell and retains the same dependency
versions and build configuration. The primary installation is not modified. A symlink was initially tried but
caused relative CGo includes such as `../../../cgo/sorted64.h` to resolve into
the primary checkout and read older declarations. Compiler include tracing
identified this environment mismatch; the local directory copy restores the
current-header resolution without changing product code.
Tests use the repository wrapper and the freshly built worktree-local CPU
library. No reusable top-level provenance stamp is fabricated. Native build
output and artifact fingerprints are retained with the test evidence.

The self-review scope is the generated-parser resolution, its merged grammar,
and automatically merged compile/planner consumers. Retained logs and manifest
bind the exact tested sources. Historical race/stress results are not claimed as
fresh runs: the resolution introduces no new concurrent state or protocol.
New merge-head remote CI and the separate product-design discussion remain
independent of the local merge verification.
