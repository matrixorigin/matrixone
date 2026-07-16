# PR 24659 Parser-Aware Statement Boundaries Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make rewrite/remap policy materialization follow MatrixOne's top-level MySQL statement boundaries so compound `CREATE TASK ... BEGIN ... END` bodies remain one statement.

**Architecture:** The MySQL grammar records only separators reduced by the top-level `stmt_list` production. A narrow parser helper uses those byte offsets to split SQL, while the existing lexical splitter remains available for invalid-SQL recording. Rewrite, hint, remap, and statement-record consumers switch to the parser-aware helper.

**Tech Stack:** Go 1.22+, MatrixOne MySQL goyacc grammar, testify, MatrixOne frontend/CGo test environment.

## Global Constraints

- Do not implement compound-statement depth tracking in the scanner.
- Do not redesign rewrite/remap policy transport, `UserInput`, or computation wrappers.
- Keep `SplitSqlBySemicolon` behavior unchanged.
- Preserve empty/comment-only fragments, trailing-semicolon behavior, and semicolons inside strings/comments.
- Generated `pkg/sql/parsers/dialect/mysql/mysql_sql.go` must be reproducible from `mysql_sql.y`.
- Production changes must be preceded by a failing regression test and verified red-green.

---

### Task 1: Grammar-Owned Top-Level Statement Boundaries

**Files:**
- Modify: `pkg/sql/parsers/dialect/mysql/mysql_sql.y`
- Modify: `pkg/sql/parsers/dialect/mysql/mysql_lexer.go`
- Modify (generated): `pkg/sql/parsers/dialect/mysql/mysql_sql.go`
- Modify: `pkg/sql/parsers/sqlparse.go`
- Modify: `pkg/sql/parsers/sqlparse_test.go`
- Modify: `pkg/frontend/rewrite_rule.go`
- Modify: `pkg/frontend/rewrite_rule_test.go`
- Modify: `pkg/frontend/mysql_cmd_executor.go`
- Modify: `pkg/frontend/mysql_cmd_executor_test.go`

**Interfaces:**
- Produces: `parsers.SplitSqlByStatement(ctx context.Context, sql string) ([]string, error)`.
- Consumes: MySQL lexer token byte positions and top-level `stmt_list` grammar reductions.
- Preserves: `SplitSqlBySemicolon(sql string) []string` and `HandleSqlForRecord(sql string) []string` compatibility.

- [ ] **Step 1: Add failing parser-boundary tests**

Add focused coverage to `pkg/sql/parsers/sqlparse_test.go`:

```go
func TestSplitSqlByStatementCompoundBody(t *testing.T) {
	ctx := context.Background()
	compound := "create task task_quotes when ('gate' = 'gate') as begin\n" +
		"  insert into gate_sink select 'gate-ok';\n" +
		"  select case when 1 = 1 then 'PASS' else 'FAIL' end;\n" +
		"end"

	got, err := SplitSqlByStatement(ctx, compound+";")
	require.NoError(t, err)
	require.Equal(t, []string{compound}, got)

	got, err = SplitSqlByStatement(ctx, compound+"; select 2")
	require.NoError(t, err)
	require.Equal(t, []string{compound, "select 2"}, got)
}

func TestSplitSqlByStatementPreservesFragmentContract(t *testing.T) {
	ctx := context.Background()
	got, err := SplitSqlByStatement(ctx,
		"select ';' as semi /* block ; */;; -- comment ;\nselect 2; /* tail ; comment */")
	require.NoError(t, err)
	require.Equal(t, []string{
		"select ';' as semi /* block ; */",
		"",
		"-- comment ;\nselect 2",
		"/* tail ; comment */",
	}, got)
}
```

- [ ] **Step 2: Run the parser tests and verify RED**

Run:

```bash
go test -count=1 ./pkg/sql/parsers -run '^TestSplitSqlByStatement'
```

Expected: compilation fails because `SplitSqlByStatement` is not defined.

- [ ] **Step 3: Record top-level separator positions in the grammar**

In `pkg/sql/parsers/dialect/mysql/mysql_sql.y`, add a token-end position to `%struct`:

```yacc
%struct {
    id  int
    str string
    item interface{}
    pos int
}
```

Record only the separator in the top-level production:

```yacc
stmt_list:
    stmt
    {
        if $1 != nil {
            yylex.(*Lexer).AppendStmt($1)
        }
    }
|   stmt_list ';' stmt
    {
        yylex.(*Lexer).AppendTopLevelSemicolon($<pos>2)
        if $3 != nil {
            yylex.(*Lexer).AppendStmt($3)
        }
    }
```

Do not add the callback to `stmt_list_return`; its separators belong to a compound body.

- [ ] **Step 4: Implement the lexer and parser helper**

In `pkg/sql/parsers/dialect/mysql/mysql_lexer.go`, add state and reset it in `setScanner`:

```go
type Lexer struct {
	scanner                   *Scanner
	stmts                     []tree.Statement
	paramIndex                int
	lower                     int64
	lastToken                 int
	topLevelSemicolonEnds     []int
}

func (l *Lexer) setScanner(s *Scanner, lower int64) {
	l.scanner = s
	l.stmts = nil
	l.paramIndex = 0
	l.lower = lower
	l.lastToken = 0
	l.topLevelSemicolonEnds = nil
}

func (l *Lexer) AppendTopLevelSemicolon(end int) {
	l.topLevelSemicolonEnds = append(l.topLevelSemicolonEnds, end)
}
```

In the existing `Lex` method, insert the position assignment immediately after `typ, str := l.scanner.Scan()` and before `l.scanner.LastToken = str`:

```go
lval.pos = l.scanner.Pos
```

Add the parser-owned splitter in the same file. It must free parser-created ASTs:

```go
func SplitSqlByStatement(ctx context.Context, sql string, lower int64) ([]string, error) {
	lexer := NewLexer(dialect.MYSQL, sql, lower)
	defer PutScanner(lexer.scanner)
	if yyParse(lexer) != 0 {
		for _, stmt := range lexer.stmts {
			stmt.Free()
		}
		return nil, lexer.scanner.LastError
	}
	defer func() {
		for _, stmt := range lexer.stmts {
			stmt.Free()
		}
	}()

	fragments := make([]string, 0, len(lexer.topLevelSemicolonEnds)+1)
	start := 0
	for _, end := range lexer.topLevelSemicolonEnds {
		fragments = append(fragments, strings.TrimSpace(sql[start:end-1]))
		start = end
	}
	tail := strings.TrimSpace(sql[start:])
	if len(lexer.topLevelSemicolonEnds) == 0 || tail != "" {
		fragments = append(fragments, tail)
	}
	if len(fragments) == 0 {
		return []string{""}, nil
	}
	return fragments, nil
}
```

In `pkg/sql/parsers/sqlparse.go`, expose the dialect-fixed wrapper:

```go
func SplitSqlByStatement(ctx context.Context, sql string) ([]string, error) {
	return mysql.SplitSqlByStatement(ctx, sql, 1)
}
```

- [ ] **Step 5: Regenerate the MySQL parser and verify parser GREEN**

Run:

```bash
make -C pkg/sql/parsers/dialect/mysql mysql_sql.go
gofmt -w pkg/sql/parsers/dialect/mysql/mysql_lexer.go pkg/sql/parsers/sqlparse.go pkg/sql/parsers/sqlparse_test.go
go test -count=1 ./pkg/sql/parsers -run '^TestSplitSqlByStatement'
```

Expected: parser generation succeeds and both new tests pass.

- [ ] **Step 6: Add the failing end-to-end rewrite regression**

Extend `TestRewriteSQLMaterializesPolicyPerStatement` in `pkg/frontend/rewrite_rule_test.go`:

```go
t.Run("compound create task is one policy boundary", func(t *testing.T) {
	ses := newSession(t, "select * from db.t where role_keep = 1", "")
	sql := "create task task_quotes when ('gate' = 'gate') as begin\n" +
		"  insert into gate_sink select 'gate-ok';\n" +
		"  select case when 1 = 1 then 'PASS' else 'FAIL' end;\n" +
		"end"

	rewritten, err := rewriteSQL(ctx, ses, sql)
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(rewritten, `/*+ {"rewrites"`))

	stmts, err := parsers.Parse(ctx, dialect.MYSQL, rewritten, 1)
	require.NoError(t, err)
	defer func() {
		for _, stmt := range stmts {
			stmt.Free()
		}
	}()
	require.Len(t, stmts, 1)
	require.IsType(t, &tree.CreateSQLTask{}, stmts[0])
	require.NoError(t, parsers.AddRewriteHints(ctx, stmts, rewritten))
})
```

Add this second subtest beside it; it covers `compound + SELECT` with a distinct inline remap on the SELECT:

```go
t.Run("compound boundary preserves following remap", func(t *testing.T) {
		ses := newSession(t, "select * from db.t where role_keep = 1", "")
		compound := "create task task_quotes when ('gate' = 'gate') as begin\n" +
			"  insert into gate_sink select 'gate-ok';\n" +
			"  select case when 1 = 1 then 'PASS' else 'FAIL' end;\n" +
			"end"
		sql := compound + `; /*+ {"remapdb":{"src":"dst"}} */ select * from src.t`

		rewritten, err := rewriteSQL(ctx, ses, sql)
		require.NoError(t, err)
		remaps, err := extractRemapDbByStatement(ctx, rewritten)
		require.NoError(t, err)
		require.Len(t, remaps, 2)
		require.Empty(t, remaps[0])
		require.Equal(t, "dst", remaps[1]["src"])

		records, err := sqlForRecordByStatement(ctx, rewritten)
		require.NoError(t, err)
		require.Len(t, records, 2)
		require.Contains(t, records[0], "insert into gate_sink")
		require.Contains(t, records[0], "select case when")
	})
```

- [ ] **Step 7: Run the frontend regression and verify RED**

Run with the repository CGo environment:

```bash
CGO_CFLAGS="-I/Users/yanghaoyang/repo/matrixone/cgo -I/Users/yanghaoyang/repo/matrixone/thirdparties/install/include" \
CGO_LDFLAGS="-L/Users/yanghaoyang/repo/matrixone/thirdparties/install/lib -lusearch_c" \
DYLD_LIBRARY_PATH="/Users/yanghaoyang/repo/matrixone/cgo:/Users/yanghaoyang/repo/matrixone/thirdparties/install/lib" \
go test -ldflags="-extldflags '-L/Users/yanghaoyang/repo/matrixone/cgo -lmo -L/Users/yanghaoyang/repo/matrixone/thirdparties/install/lib -Wl,-rpath,@executable_path/lib'" \
  -count=1 -timeout 120s ./pkg/frontend -run '^TestRewriteSQLMaterializesPolicyPerStatement$'
```

Expected: failure with `parse hints bug` or an assertion showing multiple policy fragments inside the compound body.

- [ ] **Step 8: Switch only boundary-sensitive consumers**

In `pkg/frontend/rewrite_rule.go`, make `rewriteSQL` and `extractRemapDbByStatement` call `parsers.SplitSqlByStatement`. Propagate errors and update callers to handle them:

```go
fragments, err := parsers.SplitSqlByStatement(ctx, sql)
if err != nil {
	return sql, err
}
```

```go
func extractRemapDbByStatement(ctx context.Context, sql string) ([]map[string]string, error) {
	fragments, err := parsers.SplitSqlByStatement(ctx, sql)
	if err != nil {
		return nil, err
	}
	remaps := make([]map[string]string, 0, len(fragments))
	for _, fragment := range fragments {
		if parsers.FragmentHasStatement(fragment) {
			remaps = append(remaps, extractInlineRemapDb(fragment))
		}
	}
	if len(remaps) == 0 {
		return []map[string]string{nil}, nil
	}
	return remaps, nil
}
```

In `pkg/sql/parsers/sqlparse.go`, apply these exact substitutions to `extractLeadingHints`; leave the byte-level leading-comment loop between them unchanged:

```diff
-func extractLeadingHints(sql string) []string {
+func extractLeadingHints(ctx context.Context, sql string) ([]string, error) {
 	if len(sql) == 0 {
-		return []string{""}
+		return []string{""}, nil
 	}
 
-	fragments := SplitSqlBySemicolon(sql)
+	fragments, err := SplitSqlByStatement(ctx, sql)
+	if err != nil {
+		return nil, err
+	}
@@
-	return results
+	return results, nil
 }
```

At the start of `AddRewriteHints`, replace `hints := extractLeadingHints(sql)` with:

```go
hints, err := extractLeadingHints(ctx, sql)
if err != nil {
	return err
}
```

Keep the existing count validation, AST mutation conditions, and error text in place. Update direct `extractLeadingHints` tests to pass `context.Background()` and assert `require.NoError(t, err)` before comparing the returned hints.

In `pkg/sql/parsers/sqlparse.go`, reuse the existing sanitizer per parser-owned statement. Joining its lexical subparts preserves secret masking without duplicating the sanitizer:

```go
func HandleSqlForRecordByStatement(ctx context.Context, sql string) ([]string, error) {
	fragments, err := SplitSqlByStatement(ctx, sql)
	if err != nil {
		return nil, err
	}
	records := make([]string, len(fragments))
	for i, fragment := range fragments {
		records[i] = strings.Join(HandleSqlForRecord(fragment), ";")
	}
	return records, nil
}
```

Replace `sqlForRecordByStatement` in `pkg/frontend/mysql_cmd_executor.go` with:

```go
func sqlForRecordByStatement(ctx context.Context, sql string) ([]string, error) {
	fragments, err := parsers.SplitSqlByStatement(ctx, sql)
	if err != nil {
		return nil, err
	}
	records, err := parsers.HandleSqlForRecordByStatement(ctx, sql)
	if err != nil {
		return nil, err
	}
	if len(fragments) == 1 {
		return records, nil
	}
	byStatement := make([]string, 0, len(records))
	for i, fragment := range fragments {
		if parsers.FragmentHasStatement(fragment) {
			byStatement = append(byStatement, records[i])
		}
	}
	if len(byStatement) == 0 {
		return []string{""}, nil
	}
	return byStatement, nil
}
```

In `doComQuery`, replace the assignment with error propagation:

```go
sqlRecord, err := sqlForRecordByStatement(execCtx.reqCtx, input.getSql())
if err != nil {
	return err
}
```

Update existing direct calls in tests to pass `context.Background()` and check the returned error.

- [ ] **Step 9: Verify frontend GREEN and regression alignment**

Run:

```bash
CGO_CFLAGS="-I/Users/yanghaoyang/repo/matrixone/cgo -I/Users/yanghaoyang/repo/matrixone/thirdparties/install/include" \
CGO_LDFLAGS="-L/Users/yanghaoyang/repo/matrixone/thirdparties/install/lib -lusearch_c" \
DYLD_LIBRARY_PATH="/Users/yanghaoyang/repo/matrixone/cgo:/Users/yanghaoyang/repo/matrixone/thirdparties/install/lib" \
go test -ldflags="-extldflags '-L/Users/yanghaoyang/repo/matrixone/cgo -lmo -L/Users/yanghaoyang/repo/matrixone/thirdparties/install/lib -Wl,-rpath,@executable_path/lib'" \
  -count=1 -timeout 120s ./pkg/frontend \
  -run '^(TestRewriteSQLMaterializesPolicyPerStatement|TestHandleAnalyzeStmtInheritsCurrentStatementRewriteOnly)$'
```

Expected: all selected frontend tests pass, including the compound regression.

- [ ] **Step 10: Run package verification**

Run:

```bash
go test -count=1 -timeout 120s ./pkg/sql/parsers/...
go build ./pkg/sql/parsers/... ./pkg/frontend
go vet ./pkg/sql/parsers/...
CGO_CFLAGS="-I/Users/yanghaoyang/repo/matrixone/cgo -I/Users/yanghaoyang/repo/matrixone/thirdparties/install/include" \
CGO_LDFLAGS="-L/Users/yanghaoyang/repo/matrixone/thirdparties/install/lib -lusearch_c" \
go vet ./pkg/frontend
```

Then run the full frontend package using the CGo command from Step 9 without `-run`. Expected: exit 0 with no hangs.

Inspect:

```bash
git diff --check
git diff --stat origin/fix/issue-23122-analyze-check-show-profile...HEAD
git status --short
```

- [ ] **Step 11: Self-review and commit**

Verify the complete functional closure:

- grammar boundary capture -> rewrite materialization -> parser -> hint attachment;
- per-statement remap extraction and AST application;
- compound SQL statement recording;
- empty/comment/trailing-semicolon compatibility;
- parser-created ASTs freed on success and error.

Commit:

```bash
git add pkg/sql/parsers/dialect/mysql/mysql_sql.y \
  pkg/sql/parsers/dialect/mysql/mysql_lexer.go \
  pkg/sql/parsers/dialect/mysql/mysql_sql.go \
  pkg/sql/parsers/sqlparse.go pkg/sql/parsers/sqlparse_test.go \
  pkg/frontend/rewrite_rule.go pkg/frontend/rewrite_rule_test.go \
  pkg/frontend/mysql_cmd_executor.go pkg/frontend/mysql_cmd_executor_test.go
git commit -m "fix: split rewrite policies at parser boundaries"
```
