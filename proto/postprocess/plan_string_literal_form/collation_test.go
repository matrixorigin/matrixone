// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCollationPostprocessBoundaryAndIdempotence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "generated.go")
	input := "func (m *Type) Unmarshal(dAtA []byte) error {\n\tif len(dAtA) == 1 { return nil }\n\treturn nil\n}\nfunc other() error {\n\treturn nil\n}\n"
	if err := os.WriteFile(path, []byte(input), 0600); err != nil {
		t.Fatal(err)
	}
	if err := patchCollationMetadata(path, "Type"); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(first), "return m.ValidateCollation()") || !strings.Contains(string(first), "func other() error {\n\treturn nil\n}") {
		t.Fatalf("wrong boundary: %s", first)
	}
	if err := patchCollationMetadata(path, "Type"); err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatal("not idempotent")
	}
	if err := patchCollationMetadata(path, "Unknown"); err == nil {
		t.Fatal("missing owner accepted")
	}
	if err := patchCollationMetadata(path+"missing", "Type"); err == nil {
		t.Fatal("missing file accepted")
	}
	if err := os.WriteFile(path, []byte("func (m *Type) Unmarshal(dAtA []byte) error {\n}"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := patchCollationMetadata(path, "Type"); err == nil {
		t.Fatal("missing return accepted")
	}
}
