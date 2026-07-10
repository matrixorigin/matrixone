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

package iscp

import (
	"encoding/json"
	"reflect"
	"testing"
)

// TestSplitInitSQL covers the multi-statement InitSQL format: a JSON array of
// statements (new), a JSON string (one statement), and a raw non-JSON statement
// (backward-compat for pre-existing InitSQLs like "SELECT 1").
func TestSplitInitSQL(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{"empty", "", nil},
		{"json array", `["INSERT INTO p SELECT ...", "SELECT f.* FROM p CROSS APPLY fulltext_wand_create(...)"]`,
			[]string{"INSERT INTO p SELECT ...", "SELECT f.* FROM p CROSS APPLY fulltext_wand_create(...)"}},
		{"json string", `"SELECT 1"`, []string{"SELECT 1"}},
		{"raw single", "SELECT 1", []string{"SELECT 1"}},
		{"raw insert", "INSERT INTO t VALUES (1)", []string{"INSERT INTO t VALUES (1)"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := splitInitSQL(c.in); !reflect.DeepEqual(got, c.want) {
				t.Fatalf("splitInitSQL(%q) = %v, want %v", c.in, got, c.want)
			}
		})
	}

	// Round-trip: what the retrieval producer emits (json.Marshal of a []string)
	// must split back to the same statements.
	stmts := []string{"INSERT INTO db.posting SELECT ...", "SELECT f.* FROM db.posting CROSS APPLY fulltext_wand_create(...)"}
	js, err := json.Marshal(stmts)
	if err != nil {
		t.Fatal(err)
	}
	if got := splitInitSQL(string(js)); !reflect.DeepEqual(got, stmts) {
		t.Fatalf("round-trip: got %v, want %v", got, stmts)
	}
}
