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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestVisitExpressionsInOwnerContainerContract(t *testing.T) {
	type link struct {
		Expr *Expr
		Next *link
	}
	expression := func(position int32) *Expr {
		return &Expr{Expr: &Expr_P{P: &ParamRef{Pos: position}}}
	}
	first := &link{Expr: expression(1)}
	first.Next = first
	shared := expression(2)
	var absent *Expr
	owner := struct {
		Scalars     []int64
		ScalarArray [3]string
		ScalarMap   map[string]int64
		Cycle       *link
		Repeated    [2]*Expr
		Dynamic     []any
		Mapping     map[string]any
		hidden      *Expr
	}{
		Scalars: make([]int64, 4096), ScalarArray: [3]string{"a", "b", "c"},
		ScalarMap: map[string]int64{"value": 1}, Cycle: first,
		Repeated: [2]*Expr{shared, shared},
		Dynamic:  []any{nil, absent, struct{ Value any }{Value: expression(3)}},
		Mapping:  map[string]any{"only": []*Expr{expression(4)}},
		hidden:   expression(99),
	}
	var positions []int32
	require.NoError(t, VisitExpressionsInOwner(&owner, func(expr *Expr) error {
		positions = append(positions, expr.GetP().Pos)
		return nil
	}))
	require.Equal(t, []int32{1, 2, 2, 3, 4}, positions)

	stop := moerr.NewInternalErrorNoCtx("stop at repeated expression")
	positions = nil
	err := VisitExpressionsInOwner(owner, func(expr *Expr) error {
		positions = append(positions, expr.GetP().Pos)
		if expr == shared {
			return stop
		}
		return nil
	})
	require.ErrorIs(t, err, stop)
	require.Equal(t, []int32{1, 2}, positions)
	require.NoError(t, VisitExpressionsInOwner(nil, func(*Expr) error {
		t.Fatal("nil owner must not invoke visitor")
		return nil
	}))
}

func TestExpressionOwnerBoundaryValidationInContainers(t *testing.T) {
	invalid := &Expr{Expr: &Expr_Lit{Lit: &Literal{
		Value: &Literal_Sval{Sval: "invalid"}, LiteralForm: StringLiteralForm(99),
	}}}
	type alias = Expr
	type namedPointer *Expr
	cases := []struct {
		name  string
		owner any
	}{
		{"alias", (*alias)(invalid)},
		{"array", [1]*Expr{invalid}},
		{"map interface", map[string]any{"only": invalid}},
		{"slice interface", []any{nil, invalid}},
		{"generated column", &TableDef{Cols: []*ColDef{{GeneratedCol: &GeneratedCol{Expr: invalid}}}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			require.ErrorContains(t, ValidateStringLiteralFormsInOwner(test.owner), "invalid string literal form 99")
		})
	}
	// Keys and named pointers have never been expression roots at this boundary.
	// Keep those semantics while pruning scalar owner payloads.
	for _, owner := range []any{map[*Expr]int{invalid: 1}, namedPointer(invalid)} {
		require.NoError(t, VisitExpressionsInOwner(owner, func(*Expr) error {
			t.Fatal("unexpected expression root")
			return nil
		}))
	}
}

func BenchmarkVisitExpressionsInOwner(b *testing.B) {
	b.Run("expression", func(b *testing.B) {
		owner := &Expr{}
		visitor := func(*Expr) error { return nil }
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := VisitExpressionsInOwner(owner, visitor); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("tiny-owner", func(b *testing.B) {
		owner := &struct{ Expr *Expr }{Expr: &Expr{}}
		visitor := func(*Expr) error { return nil }
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := VisitExpressionsInOwner(owner, visitor); err != nil {
				b.Fatal(err)
			}
		}
	})
	for _, columns := range []int{1, 24, 256} {
		name := "ordinary"
		nodes := 8
		if columns == 1 {
			name = "single-column"
			nodes = 1
		}
		if columns == 256 {
			name = "wide"
		}
		b.Run(name, func(b *testing.B) {
			query := &Query{}
			for range nodes {
				node := &Node{TableDef: &TableDef{Name: "fixture", Cols: make([]*ColDef, columns)},
					ProjectList: []*Expr{{Expr: &Expr_Col{Col: &ColRef{ColPos: 0}}}},
				}
				for index := range node.TableDef.Cols {
					node.TableDef.Cols[index] = &ColDef{Name: "column", Typ: Type{Id: 23, Width: 8}}
				}
				query.Nodes = append(query.Nodes, node)
			}
			owner := &Plan{Plan: &Plan_Query{Query: query}}
			visitor := func(*Expr) error { return nil }
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := VisitExpressionsInOwner(owner, visitor); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	b.Run("scalar-payload", func(b *testing.B) {
		owner := &struct {
			Values []int64
			Expr   *Expr
		}{Values: make([]int64, 65536), Expr: &Expr{}}
		visitor := func(*Expr) error { return nil }
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := VisitExpressionsInOwner(owner, visitor); err != nil {
				b.Fatal(err)
			}
		}
	})
}
