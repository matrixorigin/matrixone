// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestQGenTranslation(t *testing.T) {
	q, err := renderQGen(2, "-- qgen\nselect 'a;limit 2' as a; limit 100;")
	if err != nil {
		t.Fatal(err)
	}
	if q.Statements[0] != "select 'a;limit 2' as a LIMIT 100" {
		t.Fatal(q.Statements)
	}
	q, err = renderQGen(1, "select date '1998-12-01' - interval '90' day (3); limit -1;")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(q.Statements[0], "(3)") {
		t.Fatal(q.Statements)
	}
	q, err = renderQGen(15, "create view revenue0 (x) as select 1; select * from revenue0; drop view revenue0; limit -1;")
	if err != nil {
		t.Fatal(err)
	}
	a, b := q.rendered("a"), q.rendered("b")
	if strings.Contains(a.Statements[0], "_b") || !strings.Contains(b.Statements[0], "_b") {
		t.Fatal("view identity leaked")
	}
	if !strings.Contains(q.Statements[0], "{{view}}") {
		t.Fatal("render mutated template")
	}
	if _, err := renderQGen(2, "select 1;limit 10;"); err == nil {
		t.Fatal("accepted wrong qualification limit")
	}
	if _, err := splitSQL("select 'unfinished"); err == nil {
		t.Fatal("accepted partial SQL")
	}
	parts, err := splitSQL("select 'it''s;ok';/* ; */ select 2 -- ;\n;")
	if err != nil || len(parts) != 2 {
		t.Fatalf("%q %v", parts, err)
	}
}

func TestCanonicalKeysAndSpaces(t *testing.T) {
	tables := canonicalTables()
	if !reflect.DeepEqual(tables[4].Key, []int{0, 1}) || !reflect.DeepEqual(tables[7].Key, []int{0, 3}) {
		t.Fatal("incorrect primary keys")
	}
	for _, table := range tables {
		if strings.Contains(table.ddl(), "lineitem_id") {
			t.Fatal("synthetic key")
		}
	}
	v, err := normalize(" x ", Column{Kind: "text"})
	if err != nil || v.Value != " x " {
		t.Fatal(v, err)
	}
	v, err = normalize(" x ", Column{Kind: "text", Char: true})
	if err != nil || v.Value != " x" {
		t.Fatal(v, err)
	}
}

func TestNumericContract(t *testing.T) {
	for _, s := range []string{"1.2300", "+001.23"} {
		v, err := normalize(s, Column{Kind: "decimal"})
		if err != nil || v.Value != "1.23" {
			t.Fatal(v, err)
		}
	}
	a, b := Cell{Value: "1.230001"}, Cell{Value: "1.230002"}
	ok, err := equalCell(a, b, Column{Kind: "decimal"}, PhysicalColumn{}, PhysicalColumn{})
	if err != nil || ok {
		t.Fatal("exact decimal mismatch was hidden")
	}
	pa := PhysicalColumn{Type: "DECIMAL", Scale: 6, HasScale: true}
	pb := PhysicalColumn{Type: "DOUBLE"}
	ok, err = equalCell(a, b, Column{Kind: "approximate"}, pa, pb)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	ok, err = equalCell(a, Cell{Value: "1.231"}, Column{Kind: "approximate"}, pa, pb)
	if err != nil || ok {
		t.Fatal("broad numeric tolerance")
	}
	if _, err := fixedError(PhysicalColumn{Type: "DECIMAL"}); err == nil {
		t.Fatal("missing scale silently accepted")
	}
	if _, err := fixedError(PhysicalColumn{Type: "DECIMAL", Scale: 1, HasScale: true}); err == nil {
		t.Fatal("insufficient precision accepted")
	}
}

func testCapture(t *testing.T, root, name string, rows []Row) Capture {
	t.Helper()
	f, err := os.Create(filepath.Join(root, name))
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	id, err := identify(filepath.Join(root, name))
	if err != nil {
		t.Fatal(err)
	}
	id.Path = name
	return Capture{Version: formatVersion, File: id, Rows: int64(len(rows)), Columns: []PhysicalColumn{{Type: "BIGINT"}, {Type: "VARCHAR"}}}
}

func TestFullComparisonPeersMultiplicityAndOrdering(t *testing.T) {
	root := t.TempDir()
	q := Query{Number: 1, Columns: []Column{{Kind: "int"}, {Kind: "text"}}, Order: []OrderKey{{Column: 0}}}
	rows := []Row{{{Value: "1"}, {Value: "a"}}, {{Value: "1"}, {Value: "b"}}, {{Value: "2"}, {Value: "c"}}}
	a := testCapture(t, root, "a.rows", rows)
	b := testCapture(t, root, "b.rows", []Row{rows[1], rows[0], rows[2]})
	if err := compareCaptures(root, root, a, b, q); err != nil {
		t.Fatal(err)
	}
	b = testCapture(t, root, "dup.rows", []Row{rows[0], rows[0], rows[2]})
	if err := compareCaptures(root, root, a, b, q); err == nil {
		t.Fatal("duplicate multiplicity mismatch accepted")
	}
	b = testCapture(t, root, "order.rows", []Row{rows[2], rows[0], rows[1]})
	if err := compareCaptures(root, root, a, b, q); err == nil {
		t.Fatal("invalid ordering accepted")
	}
	b = a
	b.Rows++
	if err := compareCaptures(root, root, a, b, q); err == nil {
		t.Fatal("corrupt receipt count accepted")
	}
	if err := os.WriteFile(filepath.Join(root, a.File.Path), []byte("truncated"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := compareCaptures(root, root, a, a, q); err == nil {
		t.Fatal("corrupt capture accepted")
	}
}

func TestInputCompositeOrdering(t *testing.T) {
	path := filepath.Join(t.TempDir(), "input.tbl")
	table := canonicalTables()[4]
	if err := os.WriteFile(path, []byte("1|9|1|2.00|x |\n1|2|1|2.00|y|\n2|3|1|2.00|z|\n"), 0600); err != nil {
		t.Fatal(err)
	}
	r, err := openInput(path, table)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.f.Close() })
	for _, want := range []string{"2", "9", "3"} {
		row, err := r.next()
		if err != nil || row[1].Value != want {
			t.Fatal(row, err)
		}
	}
	if _, err := r.next(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
}

func TestDuckDBBoundaryFixture(t *testing.T) {
	ctx, cancel := context.WithTimeoutCause(context.Background(), 15*time.Second, errors.New("fixture deadline"))
	defer cancel()
	e := Engine{Name: "duckdb", Database: filepath.Join(t.TempDir(), "fixture.db"), Threads: 1, MemoryBytes: 256 << 20}
	db, err := openEngine(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	conn, err := configureConn(ctx, db, e)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	q := Query{Columns: []Column{{Kind: "int"}, {Kind: "decimal"}, {Kind: "text"}}, Statements: []string{"SELECT * FROM (VALUES (1, CAST('1234567890123.45' AS DECIMAL(15,2)), ' x '),(2,NULL,NULL)) t(i,d,s) ORDER BY i"}}
	cap, err := executeUnit(ctx, conn, q, filepath.Join(t.TempDir(), "fixture.rows"), captureDisk)
	if err != nil {
		t.Fatal(err)
	}
	if cap.Rows != 2 || cap.FirstRowNS == nil || cap.LastRowNS == nil {
		t.Fatal(cap)
	}
	q = Query{Columns: []Column{{Kind: "int"}}, Statements: []string{"SELECT 1 WHERE false"}}
	cap, err = executeUnit(ctx, conn, q, filepath.Join(t.TempDir(), "empty.rows"), captureDisk)
	if err != nil || cap.Rows != 0 || cap.FirstRowNS != nil {
		t.Fatal(cap, err)
	}
	q = Query{Columns: []Column{{Kind: "int"}}, ResultStatement: 1, Statements: []string{"CREATE VIEW tpch_revenue_fixture AS SELECT 1", "SELECT missing FROM tpch_revenue_fixture", "DROP VIEW tpch_revenue_fixture"}, Cleanup: "DROP VIEW IF EXISTS tpch_revenue_fixture"}
	if _, err := executeUnit(ctx, conn, q, filepath.Join(t.TempDir(), "failed.rows"), captureDisk); err == nil {
		t.Fatal("expected SELECT failure")
	}
	var count int
	if err := conn.QueryRowContext(ctx, "SELECT count(*) FROM duckdb_views() WHERE view_name='tpch_revenue_fixture'").Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	t.Run("load without correctness scans", func(t *testing.T) {
		root := t.TempDir()
		// Native loading accepts unordered input. Correctness checks belong in
		// separate tests, not in the benchmark preparation path.
		if err := os.WriteFile(filepath.Join(root, "region.tbl"), []byte("1|AMERICA|x|\n0|AFRICA|y|\n"), 0600); err != nil {
			t.Fatal(err)
		}
		d := Dataset{Tables: canonicalTables()[:1], Inputs: map[string]FileID{"region": {Path: "region.tbl"}}}
		if _, err := bulkLoad(ctx, conn, e, root, d); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRowContext(ctx, "SELECT count(*) FROM region").Scan(&count); err != nil || count != 2 {
			t.Fatal(count, err)
		}
	})
}

func TestArtifactBoundsAndPaths(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "x"), []byte("123"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := within(root, "../x"); err == nil {
		t.Fatal("path escape accepted")
	}
	if err := publishJSON(filepath.Join(root, "receipt"), map[string]int{"v": 1}); err != nil {
		t.Fatal(err)
	}
	if err := publishJSON(filepath.Join(root, "receipt"), map[string]int{"v": 2}); err == nil {
		t.Fatal("receipt overwritten")
	}
	var out strings.Builder
	w := limitedWriter{Writer: &out, Remaining: 2}
	if _, err := w.Write([]byte("123")); err == nil || out.Len() != 0 {
		t.Fatal("partial oversized write accepted")
	}
}

func TestLimitBoundaryMembership(t *testing.T) {
	root := t.TempDir()
	q := Query{Number: 2, Limit: 2, Columns: []Column{{Kind: "int"}, {Kind: "text"}}, Order: []OrderKey{{Column: 0}}}
	rows := []Row{{{Value: "1"}, {Value: "a"}}, {{Value: "2"}, {Value: "b"}}, {{Value: "2"}, {Value: "c"}}, {{Value: "3"}, {Value: "d"}}}
	full := testCapture(t, root, "full.rows", rows)
	legal := testCapture(t, root, "legal.rows", []Row{rows[0], rows[2]})
	if err := validateLimit(root, root, legal, full, q); err != nil {
		t.Fatal(err)
	}
	illegal := testCapture(t, root, "illegal.rows", []Row{rows[1], rows[2]})
	if err := validateLimit(root, root, illegal, full, q); err == nil {
		t.Fatal("skipped preceding group")
	}
	illegal = testCapture(t, root, "foreign.rows", []Row{rows[0], {{Value: "2"}, {Value: "foreign"}}})
	if err := validateLimit(root, root, illegal, full, q); err == nil {
		t.Fatal("foreign boundary member accepted")
	}
}

func TestReferenceAndEngineeringRulesRemainSeparate(t *testing.T) {
	a, _ := number("348406.0542857143")
	b, _ := number("348406.02")
	if !referenceNumberEqual(a, b, "ratio") {
		t.Fatal("TPC reference ratio contract")
	}
	ok, err := equalCell(Cell{Value: a.FloatString(10)}, Cell{Value: b.FloatString(10)}, Column{Kind: "approximate"}, PhysicalColumn{Type: "DOUBLE"}, PhysicalColumn{Type: "DOUBLE"})
	if err != nil || ok {
		t.Fatal("reference tolerance leaked into engineering comparison")
	}
	q := queryContract(18)
	if q.Columns[5].Reference != "exact" {
		t.Fatal("quantity SUM must remain exact")
	}
}

func TestTableVerificationWindows(t *testing.T) {
	ctx, cancel := context.WithTimeoutCause(context.Background(), 15*time.Second, errors.New("verification windows fixture deadline"))
	defer cancel()
	e := Engine{Name: "duckdb", Database: filepath.Join(t.TempDir(), "windows.db"), Threads: 1, MemoryBytes: 256 << 20}
	db, err := openEngine(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	conn, err := configureConn(ctx, db, e)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	table := canonicalTables()[4]
	if _, err := conn.ExecContext(ctx, table.ddl()); err != nil {
		t.Fatal(err)
	}
	const input = "1|9|1|2.00|nine|\n1|2|2|3.00|two|\n3|8|3|4.00|eight|\n7|4|4|5.00|four|\n"
	const seed = "INSERT INTO partsupp VALUES (1,9,1,2,'nine'),(1,2,2,3,'two'),(3,8,3,4,'eight'),(7,4,4,5,'four')"
	root := t.TempDir()
	for _, tc := range []struct {
		name, change, want string
		window             int
		emptyInput, cancel bool
	}{
		{name: "one row target preserves composite group", window: 1},
		{name: "exact group boundary", window: 2},
		{name: "multiple groups per range", window: 3},
		{name: "extra lower key", window: 1, change: "INSERT INTO partsupp VALUES(-1,1,1,1,'extra')", want: "content differs"},
		{name: "extra key in gap", window: 1, change: "INSERT INTO partsupp VALUES(2,1,1,1,'extra')", want: "content differs"},
		{name: "extra upper key", window: 1, change: "INSERT INTO partsupp VALUES(9,1,1,1,'extra')", want: "unexpected database row"},
		{name: "extra composite member", window: 1, change: "INSERT INTO partsupp VALUES(1,5,1,1,'extra')", want: "content differs"},
		{name: "missing composite member", window: 1, change: "DELETE FROM partsupp WHERE ps_partkey=1 AND ps_suppkey=9", want: "missing input rows"},
		{name: "missing middle group", window: 1, change: "DELETE FROM partsupp WHERE ps_partkey=3", want: "missing input rows"},
		{name: "missing final group", window: 1, change: "DELETE FROM partsupp WHERE ps_partkey=7", want: "missing input rows"},
		{name: "wrong value", window: 1, change: "UPDATE partsupp SET ps_comment='changed' WHERE ps_partkey=3", want: "content differs"},
		{name: "empty input and table", window: 1, emptyInput: true, change: "DELETE FROM partsupp"},
		{name: "empty input extra table rows", window: 1, emptyInput: true, want: "unexpected database row"},
		{name: "empty table nonempty input", window: 1, change: "DELETE FROM partsupp", want: "missing input rows"},
		{name: "invalid window", window: 0, want: "positive window"},
		{name: "cancelled", window: 1, cancel: true, want: "context canceled"},
		{name: "null leading key after schema change", window: 1, change: "CREATE OR REPLACE TABLE partsupp AS SELECT * FROM partsupp UNION ALL SELECT NULL,1,1,1,'null'", want: "unexpected database row"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := conn.ExecContext(ctx, "DELETE FROM partsupp"); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.ExecContext(ctx, seed); err != nil {
				t.Fatal(err)
			}
			if tc.change != "" {
				if _, err := conn.ExecContext(ctx, tc.change); err != nil {
					t.Fatal(err)
				}
			}
			text := input
			if tc.emptyInput {
				text = ""
			}
			if err := os.WriteFile(filepath.Join(root, "partsupp.tbl"), []byte(text), 0600); err != nil {
				t.Fatal(err)
			}
			queryCtx, stop := context.WithCancel(ctx)
			defer stop()
			if tc.cancel {
				stop()
			}
			err := verifyTableWindows(queryCtx, conn, root, table, FileID{Path: "partsupp.tbl"}, tc.window)
			if tc.want == "" && err != nil || tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("want %q, got %v", tc.want, err)
			}
		})
	}
	if _, err := conn.ExecContext(ctx, "DROP TABLE partsupp"); err != nil {
		t.Fatal(err)
	}
}
