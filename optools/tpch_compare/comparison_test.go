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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Only REGION has a row: verification needs the canonical schema, not DBGEN
// volume. Synthetic query sources exercise manifest validation without tools.
func testDataset(t *testing.T) (string, Dataset) {
	t.Helper()
	root := t.TempDir()
	d := Dataset{Version: formatVersion, Specification: "3.0.1", Scale: "1", Tables: canonicalTables(), Inputs: map[string]FileID{}}
	write := func(name, text string) FileID {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, name), []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
		id, err := identify(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		id.Path = name
		return id
	}
	for _, table := range d.Tables {
		text := ""
		if table.Name == "region" {
			text = "0|AFRICA|old|\n"
		}
		d.Inputs[table.Name] = write(table.Name+".tbl", text)
	}
	for n := 1; n <= 22; n++ {
		raw := "select 1;"
		if n == 15 {
			raw = "create view revenue0 as select 1; select * from revenue0; drop view revenue0;"
		}
		limit := queryContract(n).Limit
		if limit == 0 {
			limit = -1
		}
		raw += fmt.Sprintf(" limit %d;", limit)
		q, err := renderQGen(n, raw)
		if err != nil {
			t.Fatal(err)
		}
		q.Source = write(fmt.Sprintf("q%d.sql", n), raw)
		d.Queries = append(d.Queries, q)
	}
	if err := publishJSON(filepath.Join(root, "dataset.json"), d); err != nil {
		t.Fatal(err)
	}
	return root, d
}

func TestVerifyRejectsChangedInputIdentity(t *testing.T) {
	root, d := testDataset(t)
	ctx, cancel := context.WithTimeoutCause(context.Background(), 15*time.Second, errors.New("verification fixture deadline"))
	defer cancel()
	e := Engine{Name: "duckdb", Database: filepath.Join(t.TempDir(), "verify.db"), Threads: 1, MemoryBytes: 256 << 20}
	db, err := openEngine(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, table := range d.Tables {
		if _, err := db.ExecContext(ctx, table.ddl()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO region VALUES (0, 'AFRICA', 'old')"); err != nil {
		t.Fatal(err)
	}
	r := WorkerRequest{Version: formatVersion, Operation: "verify", Engine: e, DatasetRoot: root, OutputRoot: filepath.Join(t.TempDir(), "clean"), QueryDeadlineNS: int64(time.Second)}
	if receipt, err := runWorker(ctx, r); err != nil || receipt.Cleanup != "complete" {
		t.Fatalf("unchanged input verification: %+v, %v", receipt, err)
	}
	for _, tc := range []struct {
		name, table, from, to string
	}{
		{"integer becomes text", "region", "r_regionkey INTEGER", "r_regionkey VARCHAR"},
		{"integer width", "region", "r_regionkey INTEGER", "r_regionkey BIGINT"},
		{"unsigned integer", "region", "r_regionkey INTEGER", "r_regionkey UINTEGER"},
		{"nullable column", "region", "r_comment VARCHAR(152) NOT NULL", "r_comment VARCHAR(152)"},
		{"missing primary key", "region", ",PRIMARY KEY (r_regionkey)", ""},
		{"extra column", "region", ",PRIMARY KEY", ",extra INTEGER DEFAULT 0,PRIMARY KEY"},
		{"decimal scale", "supplier", "DECIMAL(15,2)", "DECIMAL(15,3)"},
		{"decimal precision", "supplier", "DECIMAL(15,2)", "DECIMAL(14,2)"},
		{"decimal becomes float", "supplier", "DECIMAL(15,2)", "DOUBLE"},
		{"date becomes text", "orders", "o_orderdate DATE", "o_orderdate VARCHAR"},
		{"composite key order", "partsupp", "PRIMARY KEY (ps_partkey,ps_suppkey)", "PRIMARY KEY (ps_suppkey,ps_partkey)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var table Table
			for _, candidate := range d.Tables {
				if candidate.Name == tc.table {
					table = candidate
				}
			}
			ddl := strings.Replace(table.ddl(), tc.from, tc.to, 1)
			if ddl == table.ddl() {
				t.Fatal("schema mutation did not change the fixture")
			}
			if _, err := db.ExecContext(ctx, "DROP TABLE "+table.Name); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				for _, sql := range []string{"DROP TABLE IF EXISTS " + table.Name, table.ddl()} {
					if _, err := db.ExecContext(ctx, sql); err != nil {
						t.Error(err)
					}
				}
				if table.Name == "region" {
					if _, err := db.ExecContext(ctx, "INSERT INTO region VALUES (0,'AFRICA','old')"); err != nil {
						t.Error(err)
					}
				}
			})
			if _, err := db.ExecContext(ctx, ddl); err != nil {
				t.Fatal(err)
			}
			if table.Name == "region" {
				if _, err := db.ExecContext(ctx, "INSERT INTO region (r_regionkey,r_name,r_comment) VALUES (0,'AFRICA','old')"); err != nil {
					t.Fatal(err)
				}
			}
			request := r
			request.OutputRoot = filepath.Join(t.TempDir(), "drift")
			receipt, err := runWorker(ctx, request)
			if err == nil || !strings.Contains(err.Error(), "schema") || receipt.Cleanup != "complete" {
				t.Fatalf("schema drift must fail with cleanup=%s: %v", receipt.Cleanup, err)
			}
		})
	}
	if err := os.WriteFile(filepath.Join(root, "region.tbl"), []byte("0|AFRICA|new|\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE region SET r_comment = 'new'"); err != nil {
		t.Fatal(err)
	}
	r.OutputRoot = filepath.Join(t.TempDir(), "changed")
	if _, err := runWorker(ctx, r); err == nil || !strings.Contains(err.Error(), "artifact changed: region.tbl") {
		t.Fatalf("changed input and matching database must reject the old identity: %v", err)
	}
}

func TestReferencePreservesVarchar(t *testing.T) {
	root, d := testDataset(t)
	const supplier = "Supplier#000000001"
	for name, content := range map[string]string{
		"q20.sql":       "SELECT name,address FROM reference_strings ORDER BY name; limit -1;",
		"reference.out": fmt.Sprintf("s_name|s_address\n%-25s|%-40s\n", supplier, "address"),
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	q, err := renderQGen(20, "SELECT name,address FROM reference_strings ORDER BY name; limit -1;")
	if err != nil {
		t.Fatal(err)
	}
	q.Source, err = identify(filepath.Join(root, "q20.sql"))
	if err != nil {
		t.Fatal(err)
	}
	q.Source.Path = "q20.sql"
	d.Queries[19] = q
	id, err := identify(filepath.Join(root, "reference.out"))
	if err != nil {
		t.Fatal(err)
	}
	id.Path = "reference.out"
	d.References = map[string]FileID{"20": id}
	if err := os.Remove(filepath.Join(root, "dataset.json")); err != nil {
		t.Fatal(err)
	}
	if err := publishJSON(filepath.Join(root, "dataset.json"), d); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeoutCause(context.Background(), 15*time.Second, errors.New("reference string fixture deadline"))
	defer cancel()
	e := Engine{Name: "duckdb", Database: filepath.Join(t.TempDir(), "reference.db"), Threads: 1, MemoryBytes: 256 << 20}
	db, err := openEngine(ctx, e)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(ctx, "CREATE TABLE reference_strings (name VARCHAR,address VARCHAR)"); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, supplier, address, input, reference, want string
		pass, ambiguous, tamper                         bool
	}{
		{name: "exact varchar and formatted reference", address: "address", pass: true},
		{name: "char padding", supplier: supplier + " ", address: "address", pass: true},
		{name: "varchar trailing space", address: "address "},
		{name: "varchar leading space", address: " address"},
		{name: "varchar interior space", address: "add ress"},
		{name: "varchar changed text", address: "addressx"},
		{name: "legitimate varchar trailing space", address: "address ", input: "address ", pass: true},
		{name: "removed varchar trailing space", address: "address", input: "address "},
		{name: "published reference remains authoritative", address: "address", reference: "different"},
		{name: "ambiguous input value", address: "address", ambiguous: true, want: "ambiguous"},
		{name: "changed decoding input hash", address: "address", tamper: true, want: "artifact changed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input, reference, name := tc.input, tc.reference, tc.supplier
			if input == "" {
				input = "address"
			}
			if reference == "" {
				reference = "address"
			}
			if name == "" {
				name = supplier
			}
			rows := "1|" + supplier + "|" + input + "|0|00-000-000-0000|0.00|comment|\n"
			if tc.ambiguous {
				rows += "2|" + supplier + "|address |0|00-000-000-0000|0.00|comment|\n"
			}
			for file, content := range map[string]string{
				"supplier.tbl":  rows,
				"reference.out": fmt.Sprintf("s_name|s_address\n%-25s|%-40s\n", supplier, reference),
			} {
				if err := os.WriteFile(filepath.Join(root, file), []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
				id, err := identify(filepath.Join(root, file))
				if err != nil {
					t.Fatal(err)
				}
				id.Path = file
				if file == "supplier.tbl" {
					d.Inputs["supplier"] = id
				} else {
					d.References["20"] = id
				}
			}
			if err := os.Remove(filepath.Join(root, "dataset.json")); err != nil {
				t.Fatal(err)
			}
			if err := publishJSON(filepath.Join(root, "dataset.json"), d); err != nil {
				t.Fatal(err)
			}
			if tc.tamper {
				if err := os.WriteFile(filepath.Join(root, "supplier.tbl"), []byte(strings.ReplaceAll(rows, "address", "changed")), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := db.ExecContext(ctx, "DELETE FROM reference_strings"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO reference_strings VALUES (?,?)", name, tc.address); err != nil {
				t.Fatal(err)
			}
			r := WorkerRequest{Version: formatVersion, Operation: "queries", Engine: e, DatasetRoot: root, OutputRoot: filepath.Join(t.TempDir(), "capture"), QueryDeadlineNS: int64(time.Second), Queries: []int{20}, Repetitions: 1, Reference: true}
			receipt, err := runWorker(ctx, r)
			want := tc.want
			if want == "" {
				want = "reference string differs"
			}
			if tc.pass && err != nil || !tc.pass && (err == nil || !strings.Contains(err.Error(), want)) || receipt.Cleanup != "complete" {
				t.Fatalf("reference exact-string result, cleanup=%s: %v", receipt.Cleanup, err)
			}
		})
	}
}

func TestCompareQualificationLimitsAndProofs(t *testing.T) {
	dataset, _ := testDataset(t)
	id, err := identify(filepath.Join(dataset, "dataset.json"))
	if err != nil {
		t.Fatal(err)
	}
	var rows []Row
	for n := 1; n <= 12; n++ {
		revenue := "10"
		if n == 1 {
			revenue = "20"
		}
		rows = append(rows, Row{{Value: fmt.Sprint(n)}, {Value: revenue}, {Value: "1995-03-15"}, {Value: "0"}})
	}
	tied := append(append([]Row(nil), rows[:1]...), rows[2:11]...)
	for _, tc := range []struct {
		name, leftVariant, rightVariant, proofVariant, want string
		left, right                                         []Row
		proof, oneProof, mismatchProof                      bool
		secondQuery                                         bool
		edit                                                func(string, *WorkerReceipt)
	}{
		{name: "qualification", left: rows[:10], right: rows[:10]},
		{name: "fewer than LIMIT", left: rows[:1], right: rows[:1]},
		{name: "empty result"},
		{name: "too many rows", left: rows[:11], right: rows[:11], want: "LIMIT"},
		{name: "unlimited primary", left: rows, right: rows, leftVariant: "unlimited_diagnostic", rightVariant: "unlimited_diagnostic", want: "variant"},
		{name: "mixed variants", left: rows[:10], right: rows[:10], rightVariant: "unlimited_diagnostic", want: "variant"},
		{name: "unknown variant", left: rows[:10], right: rows[:10], leftVariant: "unknown", want: "variant"},
		{name: "unproved ties", left: rows[:10], right: tied, want: "mismatch"},
		{name: "proved ties", left: rows[:10], right: tied, proof: true},
		{name: "proof covers only limited query", left: rows[:10], right: tied, proof: true, secondQuery: true},
		{name: "one proof", left: rows[:10], right: tied, oneProof: true, want: "both"},
		{name: "wrong proof variant", left: rows[:10], right: tied, proof: true, proofVariant: "qualification", want: "variant"},
		{name: "disagreeing full results", left: rows[:10], right: tied, proof: true, mismatchProof: true, want: "mismatch"},
		{name: "skipped preceding group", left: rows[1:11], right: rows[1:11], proof: true, want: "ordering group"},
		{name: "too many rows with proof", left: rows[:11], right: rows[:11], proof: true, want: "LIMIT"},
		{name: "short result with proof", left: rows[:1], right: rows[:1], proof: true, want: "LIMIT"},
		{name: "changed proof identity", left: rows[:10], right: tied, proof: true, want: "identity", edit: func(name string, r *WorkerReceipt) {
			if name == "full-right" {
				r.Dataset.SHA256 = "changed"
			}
		}},
		{name: "changed proof engine", left: rows[:10], right: tied, proof: true, want: "engine/build", edit: func(name string, r *WorkerReceipt) {
			if name == "full-right" {
				r.Engine = "mo"
			}
		}},
		{name: "changed proof build", left: rows[:10], right: tied, proof: true, want: "engine/build", edit: func(name string, r *WorkerReceipt) {
			if name == "full-right" {
				r.Binary.SHA256 = "changed"
			}
		}},
		{name: "wrong proof repetition", left: rows[:10], right: tied, proof: true, want: "repetition", edit: func(name string, r *WorkerReceipt) {
			if strings.HasPrefix(name, "full-") {
				r.Queries[0].Repetition++
			}
		}},
		{name: "failed proof", left: rows[:10], right: tied, proof: true, want: "incomplete", edit: func(name string, r *WorkerReceipt) {
			if name == "full-right" {
				r.Error = "query failed"
			}
		}},
		{name: "invalid limited capture", left: rows[:10], right: tied, proof: true, want: "capture", edit: func(name string, r *WorkerReceipt) {
			if name == "right" {
				r.Queries[0].Capture.Version++
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			write := func(name, engine, variant string, values []Row) string {
				t.Helper()
				if variant == "" {
					variant = "qualification"
				}
				capture := testCapture(t, root, name+".rows", values)
				capture.Columns = []PhysicalColumn{{Type: "BIGINT"}, {Type: "DECIMAL", Scale: 2, HasScale: true}, {Type: "DATE"}, {Type: "BIGINT"}}
				receipt := WorkerReceipt{Version: formatVersion, Engine: engine, Dataset: id, Variant: variant, Cleanup: "complete", Queries: []QueryReceipt{{Query: 3, Capture: capture}}}
				if tc.secondQuery && !strings.HasPrefix(name, "full-") {
					other := testCapture(t, root, name+"-q6.rows", []Row{{{Value: "1"}}})
					other.Columns = []PhysicalColumn{{Type: "DECIMAL", Scale: 2, HasScale: true}}
					receipt.Queries = append(receipt.Queries, QueryReceipt{Query: 6, Capture: other})
				}
				if tc.edit != nil {
					tc.edit(name, &receipt)
				}
				path := filepath.Join(root, name+".json")
				if err := publishJSON(path, receipt); err != nil {
					t.Fatal(err)
				}
				return path
			}
			args := []string{"compare", "--dataset", dataset, "--left", write("left", "mo", tc.leftVariant, tc.left), "--right", write("right", "duckdb", tc.rightVariant, tc.right)}
			if tc.proof || tc.oneProof {
				variant := tc.proofVariant
				if variant == "" {
					variant = "unlimited_diagnostic"
				}
				args = append(args, "--left-unlimited", write("full-left", "mo", variant, rows))
				if tc.proof {
					fullRight := append([]Row(nil), rows...)
					if tc.mismatchProof {
						fullRight[11] = Row{{Value: "999"}, {Value: "10"}, {Value: "1995-03-15"}, {Value: "0"}}
					}
					args = append(args, "--right-unlimited", write("full-right", "duckdb", variant, fullRight))
				}
			}
			var out strings.Builder
			err := cli(context.Background(), args, &out, io.Discard)
			if tc.want == "" {
				if err != nil || !strings.Contains(out.String(), "comparison passed") {
					t.Fatalf("comparison output %q, error %v", out.String(), err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) || out.Len() != 0 {
				t.Fatalf("want rejection containing %q, got output %q, error %v", tc.want, out.String(), err)
			}
		})
	}
}
