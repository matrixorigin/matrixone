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
	"encoding/json"
	"os"
	"path/filepath"
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
