// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package siriusbridge

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
)

func TestDescriptorBoundsPrecedeNativeAllocation(t *testing.T) {
	oversized := strings.Repeat("x", maxMetadataTextBytes+1)
	half := oversized[:maxMetadataTextBytes/2+1]
	// Reuse one manifest backing array: the preflight counts each future C
	// copy, without allocating 64 MiB just to exercise the aggregate bound.
	manifest := make([]byte, 4<<20)
	for _, test := range []struct {
		name   string
		change func(*Request)
	}{
		{"output name", func(r *Request) { r.Columns[0].Name = oversized }},
		{"aggregate output names", func(r *Request) { r.Columns = []Column{{Name: half}, {Name: half}} }},
		{"read column name", func(r *Request) { r.Reads[0].Columns[0].Name = oversized }},
		{"database", func(r *Request) { r.Reads[0].Database = oversized }},
		{"table", func(r *Request) { r.Reads[0].Table = oversized }},
		{"schema", func(r *Request) { r.Reads[0].Schema = oversized }},
		{"data root", func(r *Request) {
			r.Reads[0].Producer = nil
			r.Reads[0].TAEManifest = []byte{1}
			r.Reads[0].DataRoot = oversized
		}},
		{"NUL identity", func(r *Request) { r.Reads[0].Database = "db\x00other" }},
		{"output arrays", func(r *Request) { r.Columns = make([]Column, maxColumns+1) }},
		{"read arrays", func(r *Request) { r.Reads[0].Columns = make([]ReadColumn, maxColumns+1) }},
		{"canonical metadata includes arrays", func(r *Request) {
			r.Reads[0].Columns = make([]ReadColumn, maxColumns)
			for i := range r.Reads[0].Columns {
				r.Reads[0].Columns[i].Name = oversized[:900]
			}
		}},
		{"aggregate transient C copies", func(r *Request) {
			read := r.Reads[0]
			read.Producer = nil
			read.DataRoot = "/data"
			read.TAEManifest = manifest
			r.Reads = make([]Read, 16)
			for i := range r.Reads {
				r.Reads[i] = read
				r.Reads[i].BindingID = uint64(i + 1)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, d := testRuntime()
			t.Cleanup(func() { _ = r.Close(context.Background()) })
			req := testRequest()
			req.Reads = []Read{{BindingID: 1, Database: "db", Table: "t", Columns: []ReadColumn{{Column: Column{Name: "c"}}}, Producer: func(context.Context, *Input) error { return nil }}}
			var released atomic.Int32
			req.Release = func(context.Context) error { released.Add(1); return nil }
			test.change(&req)
			if _, err := r.Prepare(context.Background(), req); err == nil {
				t.Fatal("oversized descriptor accepted")
			}
			if d.prepares.Load() != 0 {
				t.Fatal("rejected descriptor reached native preparation")
			}
			if released.Load() != 1 {
				t.Fatalf("release count: %d", released.Load())
			}
		})
	}
}

func TestDescriptorBudgetPreservesMaximumPlan(t *testing.T) {
	req := testRequest()
	req.Plan = make([]byte, 16<<20)
	if err := validateRequest(req); err != nil {
		t.Fatal(err)
	}
}

func TestQueryIDIsOpaqueBinaryWhileTextRemainsNULFree(t *testing.T) {
	req := testRequest()
	req.QueryID = []byte{0, 1, 2, 3, 4, 0, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	if err := validateRequest(req); err != nil {
		t.Fatalf("opaque query ID: %v", err)
	}
	req.Columns[0].Name = "c\x00hidden"
	if err := validateRequest(req); err == nil {
		t.Fatal("NUL-containing text accepted")
	}
}
