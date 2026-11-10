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
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestActualProtobufAdmissionRegeneration(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	input := filepath.Join(root, "pkg", "pb", "plan", "plan.pb.go")
	want, err := os.ReadFile(filepath.Join(root, "pkg", "pb", "plan", "collation_admission.pb.go"))
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		got, err := generate(input)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(want, got) {
			t.Fatal("admission visitor is stale or generation is nondeterministic")
		}
	}
}

func TestSchemaGraphPruningAndFutureCycles(t *testing.T) {
	input := filepath.Join(t.TempDir(), "fixture.pb.go")
	fixture := `package plan
	type Type struct{}
	type TableDef struct{ DefaultCharset, CollationVersion, KeyFormat uint32 }
	type IndexDef struct{ KeyFormat uint32 }
	type Expr struct { Typ Type; Expr isExpr_Expr }
	type isExpr_Expr interface { isExpr_Expr() }
	type Expr_F struct { F *Function }
	func (*Expr_F) isExpr_Expr() {}
	type Function struct { Args []*Expr }
	type Plan struct { Rows []*Expr; Values []Type; ByName map[string]*Expr; Future *Future; Opaque []byte; Scalar int; Imported external.Value }
	type Future struct { Next *Future; Typ Type }
	type Unrelated struct { Count int }
	`
	if err := os.WriteFile(input, []byte(fixture), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := generate(input)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"walkFuture", "p.Next", "p.Values", "p.ByName", "p.Future", "case *Expr_F:", "v.visited(p)"} {
		if !bytes.Contains(got, []byte(required)) {
			t.Errorf("missing typed owner path %s", required)
		}
	}
	for _, forbidden := range []string{"p.Opaque", "p.Scalar", "p.Imported", "walkUnrelated"} {
		if bytes.Contains(got, []byte(forbidden)) {
			t.Errorf("unrelated scalar path traversed: %s", forbidden)
		}
	}
	// New recursive shapes cannot silently lose their cycle protection.
	begin := bytes.Index(got, []byte("func (v *legacyCollationVisitor) walkFuture"))
	end := bytes.Index(got[begin+1:], []byte("\nfunc "))
	body := got[begin:]
	if end >= 0 {
		body = got[begin : begin+1+end]
	}
	if !bytes.Contains(body, []byte("v.visited(p)")) {
		t.Fatal("future recursive owner is unguarded")
	}
}

func TestInvalidGeneratorInput(t *testing.T) {
	if _, err := generate(filepath.Join(t.TempDir(), "missing.pb.go")); err == nil {
		t.Fatal("missing input accepted")
	}
	file := filepath.Join(t.TempDir(), "invalid.pb.go")
	if err := os.WriteFile(file, []byte("package plan\ntype broken {"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := generate(file); err == nil {
		t.Fatal("invalid Go input accepted")
	}
}
