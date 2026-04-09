// Copyright 2024 Matrix Origin
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

package writer

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/testinfra/types"
)

func TestWriteCases_Basic(t *testing.T) {
	tmp := t.TempDir()

	cases := []types.SuggestedCase{
		{
			Type:     types.TestBVT,
			Category: "function",
			Filename: "decimal_compare.test",
			Content:  "-- test decimal vs integer compare\nSELECT 1.0 = 1;\nSELECT 2.5 > 2;\n",
			Reason:   "missing coverage",
		},
		{
			Type:     types.TestBVT,
			Category: "window",
			Filename: "window_decimal.test",
			Content:  "-- test window function with decimal\nSELECT SUM(1.0) OVER();\n",
			Reason:   "missing coverage",
		},
	}

	written, err := WriteCases(tmp, cases)
	if err != nil {
		t.Fatalf("WriteCases: %v", err)
	}

	if len(written) != 2 {
		t.Fatalf("written = %d, want 2", len(written))
	}

	// Verify file content
	data, err := os.ReadFile(filepath.Join(tmp, "test", "distributed", "cases", "function", "decimal_compare.test"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(data) != cases[0].Content {
		t.Errorf("content mismatch: %q", string(data))
	}

	// Verify second file
	data2, err := os.ReadFile(filepath.Join(tmp, "test", "distributed", "cases", "window", "window_decimal.test"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(data2) != cases[1].Content {
		t.Errorf("content mismatch: %q", string(data2))
	}
}

func TestWriteCases_SkipExisting(t *testing.T) {
	tmp := t.TempDir()
	dir := filepath.Join(tmp, "test", "distributed", "cases", "function")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Pre-create the file
	if err := os.WriteFile(filepath.Join(dir, "existing.test"), []byte("original"), 0o644); err != nil {
		t.Fatal(err)
	}

	cases := []types.SuggestedCase{
		{
			Type:     types.TestBVT,
			Category: "function",
			Filename: "existing.test",
			Content:  "new content",
			Reason:   "test",
		},
	}

	written, err := WriteCases(tmp, cases)
	if err != nil {
		t.Fatalf("WriteCases: %v", err)
	}

	if len(written) != 0 {
		t.Errorf("should skip existing file, written = %v", written)
	}

	// Verify original content preserved
	data, err := os.ReadFile(filepath.Join(dir, "existing.test"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "original" {
		t.Errorf("existing file was overwritten: %q", string(data))
	}
}

func TestWriteCases_SkipNonBVT(t *testing.T) {
	tmp := t.TempDir()

	// Stability cases should go to output/ directory, not be skipped
	cases := []types.SuggestedCase{
		{
			Type:     types.TestStability,
			Category: "sysbench/mixed",
			Filename: "run.yml",
			Content:  "duration: 10\ntransaction:\n  - name: test\n",
			Reason:   "test",
		},
	}

	written, err := WriteCases(tmp, cases)
	if err != nil {
		t.Fatalf("WriteCases: %v", err)
	}
	if len(written) != 1 {
		t.Fatalf("should write stability case to output/, written = %v", written)
	}
	if !strings.Contains(written[0], "output") {
		t.Errorf("stability case should be in output/, got: %s", written[0])
	}

	// Verify file exists
	data, err := os.ReadFile(filepath.Join(tmp, written[0]))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !strings.Contains(string(data), "duration") {
		t.Errorf("content mismatch: %q", string(data))
	}
}

func TestWriteCases_PITRAndSnapshot(t *testing.T) {
	tmp := t.TempDir()

	cases := []types.SuggestedCase{
		{
			Type:     types.TestPITR,
			Category: "pitr",
			Filename: "pitr_new.sql",
			Content:  "CREATE PITR p1;\nSELECT 1;\ndrop pitr p1;\n",
			Reason:   "test",
		},
		{
			Type:     types.TestSnapshot,
			Category: "snapshot",
			Filename: "snap_new.sql",
			Content:  "CREATE SNAPSHOT s1;\nSELECT 1;\n",
			Reason:   "test",
		},
	}

	written, err := WriteCases(tmp, cases)
	if err != nil {
		t.Fatalf("WriteCases: %v", err)
	}
	if len(written) != 2 {
		t.Fatalf("written = %d, want 2", len(written))
	}
	// Both should be in test/distributed/cases/
	for _, w := range written {
		if !strings.HasPrefix(w, "test/distributed/cases/") {
			t.Errorf("PITR/Snapshot should be in test/distributed/cases/, got: %s", w)
		}
	}
}

func TestWriteCases_ChaosOutput(t *testing.T) {
	tmp := t.TempDir()

	cases := []types.SuggestedCase{
		{
			Type:     types.TestChaos,
			Category: "mo-chaos-config",
			Filename: "chaos_new_scenario.yaml",
			Content:  "chaos:\n  cm-chaos:\n    - name: test\n",
			Reason:   "test",
		},
	}

	written, err := WriteCases(tmp, cases)
	if err != nil {
		t.Fatalf("WriteCases: %v", err)
	}
	if len(written) != 1 {
		t.Fatalf("written = %d, want 1", len(written))
	}
	if !strings.Contains(written[0], "output/chaos") {
		t.Errorf("chaos case should be in output/chaos/, got: %s", written[0])
	}
}

func TestWriteCases_SkipEmpty(t *testing.T) {
	tmp := t.TempDir()

	cases := []types.SuggestedCase{
		{Type: types.TestBVT, Category: "", Filename: "a.test", Content: "x"},
		{Type: types.TestBVT, Category: "f", Filename: "", Content: "x"},
		{Type: types.TestBVT, Category: "f", Filename: "a.test", Content: ""},
	}

	written, err := WriteCases(tmp, cases)
	if err != nil {
		t.Fatalf("WriteCases: %v", err)
	}
	if len(written) != 0 {
		t.Errorf("should skip cases with empty fields, written = %v", written)
	}
}

func TestWriteCases_EmptyList(t *testing.T) {
	written, err := WriteCases(t.TempDir(), nil)
	if err != nil {
		t.Fatalf("WriteCases: %v", err)
	}
	if written != nil {
		t.Errorf("expected nil, got %v", written)
	}
}
