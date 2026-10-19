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
