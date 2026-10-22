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
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func referenceTableDumpBoundExpression(a, b *planpb.Expr) bool {
	if a == nil || b == nil {
		return a == b
	}
	left := proto.Clone(a).(*planpb.Expr)
	right := proto.Clone(b).(*planpb.Expr)
	var normalize func(*planpb.Expr)
	normalize = func(expr *planpb.Expr) {
		if expr == nil {
			return
		}
		// Type.Table is catalog lineage. Rebinding a persisted expression
		// against its current table fills it in, while older stored trees do
		// not carry it. It does not affect overload or value semantics.
		expr.Typ.Table = ""
		// Older TIME trees carry Scale but leave Width unset. New binding
		// mirrors Scale into Width for DDL metadata; the stored Scale is the
		// execution precision and must still match exactly.
		if expr.Typ.Id == int32(types.T_time) && expr.Typ.Width == expr.Typ.Scale {
			expr.Typ.Width = 0
		}
		if f := expr.GetF(); f != nil {
			for _, arg := range f.Args {
				normalize(arg)
			}
		}
		if list := expr.GetList(); list != nil {
			for _, entry := range list.List {
				normalize(entry)
			}
		}
		if lit := expr.GetLit(); lit != nil {
			normalize(lit.Src)
		}
	}
	normalize(left)
	normalize(right)
	return proto.Equal(left, right)
}

func TestTableDumpBoundComparisonPreservesNormalization(t *testing.T) {
	t.Parallel()
	leaf := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_time), Width: 6, Scale: 6, Table: "catalog"}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Timeval{Timeval: 123}}}}
	canonical := proto.Clone(leaf).(*planpb.Expr)
	canonical.Typ.Table = ""
	canonical.Typ.Width = 0
	cases := map[string]*planpb.Expr{
		"nil":          nil,
		"legacy":       leaf,
		"canonical":    canonical,
		"function":     {Expr: &planpb.Expr_F{F: &planpb.Function{Args: []*planpb.Expr{leaf, canonical, nil}}}},
		"list":         {Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{leaf, leaf, nil}}}},
		"source":       {Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Src: leaf}}},
		"variable":     {Expr: &planpb.Expr_V{}},
		"raw_column":   {Expr: &planpb.Expr_Raw{}},
		"window":       {Expr: &planpb.Expr_W{}},
		"subquery":     {Expr: &planpb.Expr_Sub{}},
		"correlated":   {Expr: &planpb.Expr_Corr{}},
		"target_type":  {Expr: &planpb.Expr_T{}},
		"max":          {Expr: &planpb.Expr_Max{}},
		"vector":       {Expr: &planpb.Expr_Vec{}},
		"fold":         {Expr: &planpb.Expr_Fold{}},
		"dependent":    {Expr: &planpb.Expr_Dependent{}},
		"slot":         {Expr: &planpb.Expr_Slot{}},
		"nil_function": {Expr: &planpb.Expr_F{}},
		"nil_literal":  {Expr: &planpb.Expr_Lit{}},
		"nil_list":     {Expr: &planpb.Expr_List{}},
		"param":        {Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 1}}},
		"column":       {Expr: &planpb.Expr_Col{Col: &planpb.ColRef{Name: "a"}}},
	}
	for name, original := range cases {
		t.Run(name, func(t *testing.T) {
			var before *planpb.Expr
			if original != nil {
				before = proto.Clone(original).(*planpb.Expr)
			}
			variants := []*planpb.Expr{original, nil}
			if original != nil {
				copy := proto.Clone(original).(*planpb.Expr)
				copy.Typ.Table = "another table"
				variants = append(variants, copy)
				for _, width := range []int32{0, 3, 6, 7} {
					copy := proto.Clone(original).(*planpb.Expr)
					copy.Typ.Width = width
					variants = append(variants, copy)
				}
				copy = proto.Clone(original).(*planpb.Expr)
				copy.AuxId = 17
				variants = append(variants, copy)
				copy = proto.Clone(original).(*planpb.Expr)
				copy.Ndv = 12
				variants = append(variants, copy)
			}
			for _, candidate := range variants {
				require.Equal(t, referenceTableDumpBoundExpression(original, candidate), sameTableDumpBoundExpression(original, candidate))
				require.Equal(t, referenceTableDumpBoundExpression(candidate, original), sameTableDumpBoundExpression(candidate, original))
			}
			require.True(t, proto.Equal(before, original), "comparison mutated input")
		})
	}
	require.True(t, sameTableDumpBoundExpression(leaf, canonical))
}

func BenchmarkTableDumpBoundComparison(b *testing.B) {
	expr := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 7}}}}
	for _, tc := range []struct {
		name    string
		compare func(*planpb.Expr, *planpb.Expr) bool
	}{
		{"reference", referenceTableDumpBoundExpression}, {"canonical_fast_path", sameTableDumpBoundExpression},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if !tc.compare(expr, expr) {
					b.Fatal("unequal")
				}
			}
		})
	}
}

func TestTableDumpBoundComparisonEdgesAndReaders(t *testing.T) {
	t.Parallel()
	left := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_time), Scale: 6, Width: 6, Table: "lineage"}}
	right := proto.Clone(left).(*planpb.Expr)
	right.Typ.Table = ""
	right.Typ.Width = 0
	for _, wrap := range []func(*planpb.Expr) *planpb.Expr{
		func(e *planpb.Expr) *planpb.Expr {
			return &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Args: []*planpb.Expr{e, e}}}}
		},
		func(e *planpb.Expr) *planpb.Expr {
			return &planpb.Expr{Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{e, e}}}}
		},
		func(e *planpb.Expr) *planpb.Expr {
			return &planpb.Expr{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Src: e}}}
		},
	} {
		require.True(t, sameTableDumpBoundExpression(wrap(left), wrap(right)))
	}
	sub := func(e *planpb.Expr) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_Sub{Sub: &planpb.SubqueryRef{Child: e}}}
	}
	require.False(t, referenceTableDumpBoundExpression(sub(left), sub(right)))
	require.False(t, sameTableDumpBoundExpression(sub(left), sub(right)), "do not expand normalization to other edges")
	canonical := proto.Clone(right).(*planpb.Expr)
	canonical.PreparedNumeric = &planpb.PreparedNumericMetadata{Fallback: true}
	require.False(t, sameTableDumpBoundExpression(right, canonical))
	canonical = proto.Clone(right).(*planpb.Expr)
	canonical.XXX_unrecognized = []byte{0xa0, 0x06, 0x01}
	require.Equal(t, referenceTableDumpBoundExpression(right, canonical), sameTableDumpBoundExpression(right, canonical))
	beforeLeft, beforeRight := proto.Clone(left), proto.Clone(right)
	var readers sync.WaitGroup
	result := make(chan bool, 2)
	for range 2 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			equal := true
			for range 64 {
				equal = equal && sameTableDumpBoundExpression(left, right)
			}
			result <- equal
		}()
	}
	readers.Wait()
	close(result)
	for equal := range result {
		require.True(t, equal)
	}
	require.True(t, proto.Equal(beforeLeft, left))
	require.True(t, proto.Equal(beforeRight, right))
}
