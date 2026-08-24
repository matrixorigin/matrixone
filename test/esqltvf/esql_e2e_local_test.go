// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSQLEscape(t *testing.T) {
	cases := map[string]string{
		"plain":         "plain",
		"it's":          "it''s",
		"a''b":          "a''''b",
		"":              "",
		`back\slash ok`: `back\slash ok`,
	}
	for in, want := range cases {
		if got := sqlEscape(in); got != want {
			t.Fatalf("sqlEscape(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestESConfigJSONShape(t *testing.T) {
	cfg := esConfig{Addresses: []string{"http://h:9200"}, Username: "u", Password: "p"}
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// keys must match elasticsearch.Config's field names (case-insensitive
	// JSON matching; underscores would NOT match)
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"addresses", "username", "password"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("missing key %q in %s", k, b)
		}
	}
}

func TestWriteReport(t *testing.T) {
	dir := t.TempDir()
	r := report{Status: "passed", Cases: []string{"a", "b"}}
	if err := writeReport(dir, r); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var back report
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if back.Status != "passed" || len(back.Cases) != 2 {
		t.Fatalf("report round-trip mismatch: %+v", back)
	}
	if _, err := os.Stat(filepath.Join(dir, "summary.md")); err != nil {
		t.Fatalf("summary.md missing: %v", err)
	}
	// writeReport creates the report directory itself
	nested := filepath.Join(dir, "no", "such")
	if err := writeReport(nested, r); err != nil {
		t.Fatalf("nested report dir: %v", err)
	}
	if _, err := os.Stat(filepath.Join(nested, "report.json")); err != nil {
		t.Fatalf("nested report.json missing: %v", err)
	}
}

// scalarDriver is a minimal database/sql driver whose every query returns one
// row with the single string value "42" — enough to exercise expectScalar and
// waitForMO without a server.
type scalarDriver struct{}
type scalarConn struct{}
type scalarStmt struct{}
type scalarRows struct{ done bool }

func (scalarDriver) Open(string) (driver.Conn, error)         { return scalarConn{}, nil }
func (scalarConn) Prepare(string) (driver.Stmt, error)        { return scalarStmt{}, nil }
func (scalarConn) Close() error                               { return nil }
func (scalarConn) Begin() (driver.Tx, error)                  { return nil, driver.ErrSkip }
func (scalarStmt) Close() error                               { return nil }
func (scalarStmt) NumInput() int                              { return 0 }
func (scalarStmt) Exec([]driver.Value) (driver.Result, error) { return driver.ResultNoRows, nil }
func (scalarStmt) Query([]driver.Value) (driver.Rows, error)  { return &scalarRows{}, nil }
func (r *scalarRows) Columns() []string                       { return []string{"v"} }
func (r *scalarRows) Close() error                            { return nil }
func (r *scalarRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0] = []byte("42")
	return nil
}

func TestExpectScalarAndWaitForMO(t *testing.T) {
	sql.Register("esqltvf-scalar", scalarDriver{})
	db, err := sql.Open("esqltvf-scalar", "any")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	if err := waitForMO(ctx, db); err != nil {
		t.Fatalf("waitForMO: %v", err)
	}
	if err := expectScalar(ctx, db, "select 42", "42"); err != nil {
		t.Fatalf("expectScalar match: %v", err)
	}
	if err := expectScalar(ctx, db, "select 42", "43"); err == nil {
		t.Fatal("expectScalar should report a mismatch")
	}
}

// scriptedDriver answers each statement of run()'s E2E script the way the
// real MO+ES stack does, so the whole driver flow (both TVF paths, the
// external-table cases, redaction, timezone-sensitive timestamps, and the
// cross-kind rejection) is executable as a unit test. The answers mirror the
// seeded employees index in optools/esql_ci.bash; if the script and this
// oracle drift, the test fails loudly.
type scriptedDriver struct{}
type scriptedConn struct{}
type scriptedStmt struct{ q string }
type scriptedRows struct {
	cols []string
	rows [][]driver.Value
	next int
}

// scriptedTZ is the fake session time zone, updated by "set time_zone" execs.
// The test is single-goroutine, so package state is race-free.
var scriptedTZ = "+00:00"

func (scriptedDriver) Open(string) (driver.Conn, error)    { return scriptedConn{}, nil }
func (scriptedConn) Prepare(q string) (driver.Stmt, error) { return scriptedStmt{q: q}, nil }
func (scriptedConn) Close() error                          { return nil }
func (scriptedConn) Begin() (driver.Tx, error)             { return nil, driver.ErrSkip }
func (s scriptedStmt) Close() error                        { return nil }
func (s scriptedStmt) NumInput() int                       { return 0 }

func (s scriptedStmt) Exec([]driver.Value) (driver.Result, error) {
	if strings.HasPrefix(s.q, "set time_zone = ") {
		scriptedTZ = strings.Trim(strings.TrimPrefix(s.q, "set time_zone = "), "'")
	}
	return driver.ResultNoRows, nil
}

func (s scriptedStmt) Query([]driver.Value) (driver.Rows, error) {
	one := func(v string) (driver.Rows, error) {
		return &scriptedRows{cols: []string{"v"}, rows: [][]driver.Value{{[]byte(v)}}}, nil
	}
	q := s.q
	switch {
	case strings.Contains(q, "'I', @hsql"):
		// the kind check fires before any query is sent
		return nil, errors.New(`invalid input: connection handle "sql:feedfacefeedface" is a sql connection; esql_tvf accepts only esql connections`)
	case strings.HasPrefix(q, "show create table"):
		return &scriptedRows{
			cols: []string{"Table", "Create Table"},
			rows: [][]driver.Value{{[]byte("emp_inline"),
				[]byte(`CREATE EXTERNAL TABLE emp_inline (...) engine = esql with ("config" = '<redacted>')`)}},
		}, nil
	case strings.Contains(q, "count(distinct __mo_query)"):
		return one("2")
	case strings.Contains(q, "salary is null"):
		return one("1")
	case strings.Contains(q, "max(salary)"):
		return one("150000")
	case strings.Contains(q, "salary > 100000"):
		return one("2")
	case strings.Contains(q, "__mo_query in ("):
		return one("4")
	case strings.Contains(q, "hired >="):
		return one("3")
	case strings.Contains(q, "cast(hired as char)") && strings.Contains(q, "emp_ts"):
		if scriptedTZ == "+08:00" {
			return one("2023-06-15 16:30:00.123")
		}
		return one("2023-06-15 08:30:00.123")
	case strings.Contains(q, "cast(hired as char)"):
		// schema-mode TVF renders the +08:00 instant at timestamp(0)
		return one("2023-06-15 16:30:00")
	case strings.Contains(q, "count(*)"):
		return one("5")
	default:
		return one("1")
	}
}

func (r *scriptedRows) Columns() []string { return r.cols }
func (r *scriptedRows) Close() error      { return nil }
func (r *scriptedRows) Next(dest []driver.Value) error {
	if r.next >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.next])
	r.next++
	return nil
}

// TestRunAgainstScriptedDriver executes the complete E2E script against the
// scripted oracle: every case the real run records must be recorded here too,
// in the same order.
func TestRunAgainstScriptedDriver(t *testing.T) {
	sql.Register("esqltvf-script", scriptedDriver{})
	db, err := sql.Open("esqltvf-script", "any")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	scriptedTZ = "+00:00"

	var r report
	dsn := "root:111@tcp(127.0.0.1:6001)/?timeout=5s&readTimeout=30s&writeTimeout=30s"
	if err := run(context.Background(), db, dsn, "http://127.0.0.1:9200", "elastic", "pw", &r); err != nil {
		t.Fatalf("run: %v", err)
	}
	want := []string{
		"esql_tvf_connect", "count-all", "esql-where", "mo-where", "null-salary",
		"max-salary", "no-schema-default-conn", "esql_tvf_disconnect",
		"tvf-short-schema", "exttab-create", "exttab-count-all",
		"exttab-local-predicate", "exttab-null", "exttab-in-two-queries",
		"exttab-session-config", "exttab-inline-config",
		"exttab-iso8601-timestamp", "exttab-timestamp-non-utc-session",
		"tvf-iso8601-timestamp", "cross-kind-handle-rejected",
	}
	if len(r.Cases) != len(want) {
		t.Fatalf("cases = %v, want %v", r.Cases, want)
	}
	for i := range want {
		if r.Cases[i] != want[i] {
			t.Fatalf("case[%d] = %q, want %q (all: %v)", i, r.Cases[i], want[i], r.Cases)
		}
	}
}
