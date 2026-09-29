ESQL / SQL TVF — Implementation Plan
====================================

Scope: two new table functions `esql_tvf(esql, schema, conn)` and `sql_tvf(sql, schema, conn)`,
plus four scalar functions `esql_tvf_connect/disconnect`, `sql_tvf_connect/disconnect`.
Foreign-data, query-time integration. Reuses the `parse_jsonl_data` schema spec and the CSV
external-scan parser. Session-scoped connection cache with automatic cleanup.

Branch: `feat-esql-sql-tvf` (off `upstream/main`).


1. Design decisions (first principles)
--------------------------------------

**D1 — schema is bind-time, args are runtime.** The output column set must be known at plan
time (the optimizer/downstream operators need typed columns). So the `schema` argument (2nd
arg) is read as a *constant literal* from `tbl.Func.Exprs[1]` at bind time — exactly the
`parse_jsonl_data` mechanism. The `esql`/`sql` query text (1st arg) and the `conn` handle (3rd
arg) are ordinary runtime expressions evaluated by the operator's arg executors — `conn` is
typically a session variable (`@h`) produced by `..._connect()`, so it cannot be a bind-time
constant.

**D2 — schema spec reuse.** Extract the `parse_jsonl_data` schema→`[]*plan.ColDef` builder into
a shared helper `BuildColDefsFromSchemaSpec(optstr string)` (short + long JSON format,
`ParseJsonlOptions`/`ParseJsonlOptionsCol`, type switch). Both `parse_jsonl_data` and the new
TVFs call it. If schema is absent or a NULL literal → a single column `result` of type `json`;
each row is a JSON array whose elements are the raw CSV field strings.

**D3 — one CSV path for both TVFs.** Model on `pkg/sql/colexec/external/reader_datastream.go`.
Build a synthetic `ExternalParam` (`.Cols` = schema ColDefs, `.Extern` = a fixed CSV dialect),
feed `newCSVParserFromReader` + `CsvReader.makeBatchRows`.
  - `esql_tvf`: ES|QL `_query` API returns CSV directly (`format=csv`); the HTTP response body
    is the `io.Reader`. Skip the 1 header line.
  - `sql_tvf`: a `database/sql` driver returns typed `*sql.Rows`, not CSV. A small streaming
    encoder turns rows into CSV bytes on an `io.Reader`: every non-NULL field quoted, SQL NULL
    written as the unquoted null sentinel the csvparser already recognizes. This keeps ONE
    row→batch conversion path (`getColData`, honoring the user schema types) and makes the two
    TVFs share the entire operator; only the `io.Reader` source differs.
  - The user's declared schema drives coercion; a value that doesn't fit errors ("error if the
    format errors", per spec).

**D4 — single operator, two names.** One `foreignTvfState` parameterized by a source kind
(`esql`|`sql`) carried in `TblFunc.Param` alongside the schema. Two plan builders set the name;
both execution-side switch cases construct the same state with a kind flag. Smallest general
change, no duplicated CSV/batch logic.

**D5 — connection cache lives on the frontend Session (true session lifetime).** Not on
`BaseProcess` (that is per-query and cleared at query end — wrong lifetime). Add to
`*frontend.Session`:
  - `foreignConns map[string]process.ForeignConn` (handle → connection),
  - `foreignConnByHash map[string]string` (config hash → handle, for reuse),
  - mutex.
  Expose through NEW methods on the minimal `process.Session` interface (implemented by
  `*frontend.Session`, reached from colexec via `proc.GetSession()`):
  `PutForeignConn(handle string, c ForeignConn)`, `GetForeignConn(handle string) (ForeignConn, bool)`,
  `RemoveForeignConn(handle string)`, `LookupForeignConnByHash/PutHash`.
  Define `type ForeignConn interface { Close() error }` in package `process` (no import cycle;
  concrete wrappers just need a `Close`). Close-all is added to the existing
  `Session.Close()` (session.go:1224); the Session already has a finalizer (session.go:1210)
  that calls `Close()`, so no new finalizer is needed — just the CloseAll hook.

**D6 — connection resolution routine (shared).** Given (connArgValue, proc):
  - conn non-NULL → look up the handle in the session cache; error "connection handle not
    found or disconnected" if absent.
  - conn NULL/omitted → read session var `@esql_tvf_config` / `@sql_tvf_config` via
    `proc.GetResolveVariableFunc()`, connect-or-reuse a default (keyed by config hash), use it.
  `..._connect(NULL)` uses the same default-config fallback.

**D7 — parallelism.** A source TVF (no children) compiles via `compileSingleTableFunction`
(`Mcpu=1`) automatically — one instance, no intra-op parallelism, placed on the compiling CN
(`c.addr`), which is the session's CN, so the session-local connection cache is reachable.
Multiple TVF calls in one query are separate scopes → run in parallel. Downstream join/agg
parallelize per optimizer. We also set `TblFunc.IsSingle = true` (matches parse_jsonl) to guard
the cross-apply path. **Verification during impl:** confirm the node is not dispatched to a
remote CN; add `Stats.ForceOneCN` only if placement testing shows it can leave `c.addr`.

**D8 — connect/disconnect are volatile scalar builtins.** They return a `varchar` handle (e.g.
`"esql:3"` — debuggable), touch `proc`/session, and have side effects, so they are marked
volatile (like `uuid`) to prevent constant-folding / multiple evaluation.


2. New / changed files
----------------------

New leaf package `pkg/sql/foreigntvf/` (imported by both `function` and `table_function`;
imports only `process` + DB/ES clients — no cycle):
  - `conn.go`     — `EsqlConn`/`SqlConn` implementing `process.ForeignConn`; config hashing.
  - `esql.go`     — strictly parse a whitelisted es config JSON (endpoint/credential fields only; unknown fields rejected); connect; run ES|QL → `io.ReadCloser` (CSV).
  - `sql.go`      — parse `{driver,dsn}`; `sql.Open`; run query; `*sql.Rows` → CSV `io.Reader`.
  - `config.go`   — resolve config (arg vs `@..._config` session var); connect-or-reuse.

Plan builder — `pkg/sql/plan/esql_sql_tvf.go` (new): `buildEsqlTvf` / `buildSqlTvf`; reuse the
extracted schema helper; set `TblFunc.Name/Param(kind+schema)/IsSingle`; `Args` = query+conn.

Execution operator — `pkg/sql/colexec/table_function/foreign_tvf.go` (new): `foreignTvfState`
(`tvfState`); resolve conn via `proc.GetSession()`; obtain CSV `io.Reader` from `foreigntvf`;
drive the reused CSV→batch path; no-schema → single `json` array column.

CSV reuse helper — either reuse from within `external` or add exported
`external.NewCSVBatchReader(extern, cols, r)` (decide by import-cycle check; prefer reusing
`reader_datastream.go`'s pattern).

Wiring edits:
  - `pkg/sql/plan/parse_jsonl_tvf.go` — extract shared schema helper (behavior-preserving).
  - `pkg/sql/plan/query_builder.go` — `buildTableFunction` switch: add `esql_tvf`, `sql_tvf`.
  - `pkg/sql/colexec/table_function/table_function.go` — `Prepare` switch: same two names.
  - `pkg/sql/plan/function/function_id.go` — 4 new ids before `FUNCTION_END_NUMBER` (bump it);
    add to `functionIdRegister`.
  - `pkg/sql/plan/function/list_builtIn.go` — 4 overloads (varchar arg → varchar ret), volatile.
  - `pkg/sql/plan/function/func_builtin.go` — 4 impls reading `proc`, using `foreigntvf` +
    session cache.
  - `pkg/frontend/session.go` — cache fields, interface-method impls, `Close()` CloseAll.
  - `pkg/vm/process/types.go` — `ForeignConn` interface + 4 methods on `Session` interface.
  - `go.mod` — add `github.com/elastic/go-elasticsearch/v8`; promote `jackc/pgx/v5` to direct;
    add `github.com/sijms/go-ora/v2` (Oracle, pure Go) + `github.com/microsoft/go-mssqldb`
    (SQL Server, pure Go). MySQL + Mongo already present.


3. Task order
-------------

1. `go.mod` deps + a throwaway build to confirm all clients compile (pure-Go, cgo-free).
2. `process.ForeignConn` + `Session` interface methods; `*frontend.Session` cache + `Close()` hook.
3. Extract shared schema helper from `parse_jsonl_tvf.go`; keep parse_jsonl green.
4. `pkg/sql/foreigntvf` — conn, esql, sql, config resolution (unit-testable without a server).
5. Scalar connect/disconnect (ids, overloads, impls, volatile).
6. Plan builders + operator + CSV reuse; register both names in the two switches.
7. Parallelism/placement verification (single-instance, session-CN, multi-instance parallel).
8. Tests (see §5).
9. `mo-self-review` gate before PR.


5. Test strategy (decided)
--------------------------

Scope for this PR: `esql_tvf` + `sql_tvf(mysql, postgres)`. Oracle/SQL Server deferred behind an
extensible driver registry (adding them later is a blank-import + registry line).

**Go unit tests** (no server): shared schema helper, config parse (ES + `{driver,dsn}`),
`sql.Rows`→CSV encoder including NULL-vs-empty, handle/cache lifecycle.

**sql_tvf BVT — loopback MO (self-contained).** A normal mo-tester case under
`test/distributed/cases/esql_tvf/` that does `sql_tvf_connect('{"driver":"mysql","dsn":"...127.0.0.1:6001..."}')`
pointing `sql_tvf` back at the MO server itself, then `select * from sql_tvf('select ...', schema, @h)`.
No external service; runs in the normal PR BVT suite. Also exercises the postgres driver only if a
loopback PG is available — otherwise postgres is covered by unit tests + the E2E harness.

**esql_tvf E2E — ephemeral ES, mirroring the mongodb harness (NOT in PR BVT).** New files
mirroring `optools/mongodb_ci.bash` + `etc/launch-mongodb-local/`:
  - `etc/launch-esql-local/compose.yaml` — single-node Elasticsearch (`discovery.type=single-node`,
    ES ≥ 8.11 for ES|QL), random published port, security configured with a generated password.
  - `optools/esql_ci.bash` — `require docker go`; build MO; `docker compose up -d`; wait for ES
    green; seed an index via the ES bulk API; start a fresh `mo-service` on port 0; run the Go
    driver over the MO DSN; `trap cleanup EXIT` → kill MO, `docker compose down --volumes
    --remove-orphans`, rm the `mktemp -d` dir (guarded basename check, as mongodb does); collect +
    sanitize logs.
  - `test/esqltvf/esql_e2e_local.go` — Go driver: connect to MO via DSN, `esql_tvf_connect('{es Config}')`,
    run `esql_tvf(...)` asserting schema-typed rows, pushdown, and the no-schema json-array path.
  - `Makefile` target `test-esql-tvf-e2e-local: @optools/esql_ci.bash e2e-local`.
  - `.github/workflows/esql-tvf.yml` — `on: workflow_dispatch` (manual, like mongodb-connector.yml).


4. Risks / open items
---------------------

- **NULL vs empty in the sql→CSV encoder.** Must use a sentinel the csvparser treats as NULL and
  quote all real strings; verify against `getColData`'s null detection. (Unit test this.)
- **Import cycle** around the CSV helper (`table_function` ↔ `external`). Resolve by exporting a
  thin reader helper in `external` if needed.
- **Remote placement.** Confirm the single-instance source TVF stays on the session CN; the
  session-local cache breaks if it doesn't. `ForceOneCN` is the fallback.
- **ES|QL availability / version.** ES|QL `_query` needs ES ≥ 8.11. Config is
  `elasticsearch.Config` JSON. If no test ES cluster, esql_tvf BVT uses a fixture source.
- **Oracle/SQL Server drivers.** Confirm the pure-Go choices build cgo-free in CI (no OCI).
- **Disconnect-while-in-use.** A MO session runs one query at a time, so a TVF call and a
  `disconnect()` never overlap within a session — no locking beyond the cache mutex needed.
