// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"math"
	"sync"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

var boundExpressionComparisons = []struct {
	name         string
	old, current func(*planpb.Expr, *planpb.Expr) bool
}{
	{"normal", referenceTableDumpBoundExpression, sameTableDumpBoundExpression},
	{"historical", referenceSameHistoricalFloatBoundExpression, sameHistoricalFloatBoundExpression},
	{"origin", referenceSameLegacyFloatOrigin, sameLegacyFloatOrigin},
}

// Marshal is only the immutability oracle. Equality is always the complete old
// implementation, including its canonical fast path and candidate-only edits.
// The generated marshaler cannot encode malformed nil repeated children or nil
// oneof payloads. Preserve their full graph as the immutability oracle instead;
// all encodable trees additionally use their exact protobuf bytes.
func comparisonBytes(e *planpb.Expr) (snapshot []byte, err error) {
	if e == nil {
		return nil, nil
	}
	defer func() {
		if recover() != nil {
			snapshot = []byte(spew.Sdump(e))
			err = nil
		}
	}()
	return proto.Marshal(e)
}

func assertBoundExpressionComparisons(t *testing.T, a, b *planpb.Expr) {
	t.Helper()
	beforeA, err := comparisonBytes(a)
	require.NoError(t, err)
	beforeB, err := comparisonBytes(b)
	require.NoError(t, err)
	for _, compare := range boundExpressionComparisons {
		require.Equal(t, compare.old(a, b), compare.current(a, b), compare.name)
	}
	afterA, err := comparisonBytes(a)
	require.NoError(t, err)
	afterB, err := comparisonBytes(b)
	require.NoError(t, err)
	require.Equal(t, beforeA, afterA, "stored input changed")
	require.Equal(t, beforeB, afterB, "candidate input changed")
}

func comparisonString() *planpb.Expr {
	return &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "value"}}}}
}

func TestProjectedBoundExpressionPolicies(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		edit       func(*planpb.Expr, *planpb.Expr)
		historical bool
	}{
		{"coercibility", func(a, b *planpb.Expr) { b.Typ.CollationCoercibilitySet = true; b.Typ.CollationCoercibility = 2 }, true},
		{"maximum", func(a, b *planpb.Expr) { b.Typ.CollationCoercibilitySet = true; b.Typ.CollationCoercibility = 6 }, true},
		{"unset", func(a, b *planpb.Expr) { b.Typ.CollationCoercibility = 2 }, false},
		{"explicit_zero", func(a, b *planpb.Expr) { b.Typ.CollationCoercibilitySet = true }, false},
		{"out_of_range", func(a, b *planpb.Expr) { b.Typ.CollationCoercibilitySet = true; b.Typ.CollationCoercibility = 7 }, false},
		{"stored_set", func(a, b *planpb.Expr) {
			a.Typ.CollationCoercibilitySet = true
			b.Typ.CollationCoercibilitySet = true
			b.Typ.CollationCoercibility = 2
		}, false},
		{"stored_nonzero", func(a, b *planpb.Expr) {
			a.Typ.CollationCoercibility = 1
			b.Typ.CollationCoercibilitySet = true
			b.Typ.CollationCoercibility = 2
		}, false},
		{"charset_mismatch", func(a, b *planpb.Expr) {
			b.Typ.Charset = uint32(types.CharsetBinary)
			b.Typ.CollationCoercibilitySet = true
			b.Typ.CollationCoercibility = 2
		}, false},
		{"stored_version", func(a, b *planpb.Expr) {
			a.Typ.CollationVersion = 1
			b.Typ.CollationCoercibilitySet = true
			b.Typ.CollationCoercibility = 2
		}, false},
		{"candidate_version", func(a, b *planpb.Expr) {
			b.Typ.CollationVersion = 1
			b.Typ.CollationCoercibilitySet = true
			b.Typ.CollationCoercibility = 2
		}, false},
		{"stored_conflict", func(a, b *planpb.Expr) {
			a.Typ.CollationMergeConflict = true
			b.Typ.CollationCoercibilitySet = true
			b.Typ.CollationCoercibility = 2
		}, false},
		{"candidate_conflict", func(a, b *planpb.Expr) {
			b.Typ.CollationMergeConflict = true
			b.Typ.CollationCoercibilitySet = true
			b.Typ.CollationCoercibility = 2
		}, false},
		{"non_string", func(a, b *planpb.Expr) {
			a.Typ.Id = int32(types.T_int64)
			b.Typ.Id = a.Typ.Id
			b.Typ.CollationCoercibilitySet = true
			b.Typ.CollationCoercibility = 2
		}, false},
		{"unsupported_charset", func(a, b *planpb.Expr) {
			a.Typ.Charset = 999
			b.Typ.Charset = 999
			b.Typ.CollationCoercibilitySet = true
			b.Typ.CollationCoercibility = 2
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := comparisonString(), comparisonString()
			tc.edit(a, b)
			assertBoundExpressionComparisons(t, a, b)
			require.Equal(t, tc.historical, sameHistoricalFloatBoundExpression(a, b))
			require.False(t, sameHistoricalFloatBoundExpression(b, a), "projection is candidate-only")
			for _, wrap := range []func(*planpb.Expr) *planpb.Expr{
				func(e *planpb.Expr) *planpb.Expr {
					return &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Args: []*planpb.Expr{e, nil, e}}}}
				},
				func(e *planpb.Expr) *planpb.Expr {
					return &planpb.Expr{Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{e, nil, e}}}}
				},
				func(e *planpb.Expr) *planpb.Expr {
					return &planpb.Expr{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true, Src: e}}}
				},
			} {
				assertBoundExpressionComparisons(t, wrap(a), wrap(b))
			}
		})
	}
	for _, charset := range []uint8{types.CharsetLegacy, types.CharsetBinary, types.CharsetUTF8MB4Bin, types.CharsetUTF8} {
		a, b := comparisonString(), comparisonString()
		a.Typ.Charset = uint32(charset)
		b.Typ.Charset = uint32(charset)
		b.Typ.CollationCoercibilitySet = true
		b.Typ.CollationCoercibility = 2
		assertBoundExpressionComparisons(t, a, b)
		require.True(t, sameHistoricalFloatBoundExpression(a, b))
	}
	cast := func(child *planpb.Expr, explicit bool, obj int64) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{Obj: obj}, SyntaxExplicitCast: explicit, Args: []*planpb.Expr{child}}}}
	}
	a, b := comparisonString(), comparisonString()
	b.Typ.CollationCoercibilitySet = true
	b.Typ.CollationCoercibility = 2
	old, bound := cast(a, false, function.EncodeOverloadID(function.CAST, 0)), cast(b, true, function.EncodeOverloadID(function.CAST, 0))
	assertBoundExpressionComparisons(t, old, bound)
	require.True(t, sameLegacyFloatOrigin(old, bound))
	require.False(t, sameHistoricalFloatBoundExpression(old, bound))
	bound.GetF().Func.Obj = function.EncodeOverloadID(function.CAST, 1)
	assertBoundExpressionComparisons(t, old, bound)
	require.False(t, sameLegacyFloatOrigin(old, bound))
	old.GetF().Func = nil
	bound.GetF().Func = nil
	bound.GetF().SyntaxExplicitCast = false
	assertBoundExpressionComparisons(t, old, bound)
	require.True(t, sameHistoricalFloatBoundExpression(old, bound))
	require.False(t, sameLegacyFloatOrigin(old, bound), "missing Func blocks historical child projection only for origin")
}

func TestProjectedBoundExpressionProtobuf(t *testing.T) {
	t.Parallel()
	leaf := comparisonString()
	leaf.Typ.Table = "lineage"
	cases := []*planpb.Expr{
		nil, {}, leaf,
		{Expr: &planpb.Expr_F{}}, {Expr: &planpb.Expr_List{}}, {Expr: &planpb.Expr_Lit{}},
		{Expr: &planpb.Expr_F{F: &planpb.Function{Args: []*planpb.Expr{nil, leaf, leaf}}}},
		{Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{nil, leaf, leaf}}}},
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true, Src: leaf, Value: &planpb.Literal_Sval{Sval: "still compared"}}}},
		{Expr: &planpb.Expr_Sub{Sub: &planpb.SubqueryRef{Child: leaf}}},
		{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 1}}},
		{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{Name: "a"}}},
		{Expr: &planpb.Expr_T{T: &planpb.TargetType{}}},
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Dval{Dval: math.NaN()}}}},
		{Ndv: math.NaN()}, {Selectivity: math.NaN()},
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Dval{Dval: 0}}}},
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Dval{Dval: math.Copysign(0, -1)}}}},
	}
	unknown := []byte{0xa0, 0x06, 0x01}
	for _, original := range cases {
		variants := []*planpb.Expr{nil, original}
		if original != nil {
			for _, edit := range []func(*planpb.Expr){
				func(e *planpb.Expr) { e.Typ.Table = "other" }, func(e *planpb.Expr) { e.AuxId = 12 }, func(e *planpb.Expr) { e.Ndv = 12 },
				func(e *planpb.Expr) { e.PreparedNumeric = &planpb.PreparedNumericMetadata{Fallback: true} },
				func(e *planpb.Expr) { e.XXX_unrecognized = unknown }, func(e *planpb.Expr) { e.Typ.XXX_unrecognized = unknown },
				func(e *planpb.Expr) { e.Typ.Id = int32(types.T_time); e.Typ.Width = 6; e.Typ.Scale = 6 },
				func(e *planpb.Expr) { e.Typ.Width = 7 },
				func(e *planpb.Expr) {
					if f := e.GetF(); f != nil {
						f.XXX_unrecognized = unknown
						f.AggConfig = []byte{1}
						f.Func = &planpb.ObjectRef{Obj: 1, XXX_unrecognized: unknown}
					}
				},
				func(e *planpb.Expr) {
					if l := e.GetList(); l != nil {
						l.XXX_unrecognized = unknown
					}
				},
				func(e *planpb.Expr) {
					if l := e.GetLit(); l != nil {
						l.XXX_unrecognized = unknown
						l.IsSerialized = true
						l.StringSource = 1
					}
				},
			} {
				c := proto.Clone(original).(*planpb.Expr)
				edit(c)
				variants = append(variants, c)
			}
		}
		for _, candidate := range variants {
			assertBoundExpressionComparisons(t, original, candidate)
			assertBoundExpressionComparisons(t, candidate, original)
		}
	}
	for _, compare := range boundExpressionComparisons {
		require.False(t, compare.current(cases[13], cases[13]), compare.name+": no pointer shortcut for NaN")
		require.True(t, compare.current(cases[16], cases[17]), compare.name+": signed zeros compare equal")
	}
	// Normalization must not cross a subquery child, even when the outer type
	// itself requires normalization and forces the projected comparison path.
	subA, subB := proto.Clone(cases[9]).(*planpb.Expr), proto.Clone(cases[9]).(*planpb.Expr)
	subA.Typ.Table = "outer"
	subB.GetSub().Child.Typ.Table = "different child"
	assertBoundExpressionComparisons(t, subA, subB)
	require.False(t, sameTableDumpBoundExpression(subA, subB))
	// Repeated nil and empty retain protobuf's equality, while oneof presence does not.
	assertBoundExpressionComparisons(t, &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{}}}, &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Args: []*planpb.Expr{}}}})
	assertBoundExpressionComparisons(t, &planpb.Expr{}, &planpb.Expr{Expr: &planpb.Expr_Lit{}})
}

func TestProjectedBoundExpressionReaders(t *testing.T) {
	t.Parallel()
	a, b := comparisonString(), comparisonString()
	a.Typ.Table = "lineage"
	b.Typ.CollationCoercibility = 2
	b.Typ.CollationCoercibilitySet = true
	a = &planpb.Expr{Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{a, a}}}}
	b = &planpb.Expr{Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{b, b}}}}
	beforeA, err := comparisonBytes(a)
	require.NoError(t, err)
	beforeB, err := comparisonBytes(b)
	require.NoError(t, err)
	var wg sync.WaitGroup
	results := make(chan bool, 4)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			equal := true
			for range 64 {
				for _, compare := range boundExpressionComparisons {
					equal = equal && compare.current(a, b) == compare.old(a, b)
				}
			}
			results <- equal
		}()
	}
	wg.Wait()
	close(results)
	for equal := range results {
		require.True(t, equal)
	}
	afterA, err := comparisonBytes(a)
	require.NoError(t, err)
	afterB, err := comparisonBytes(b)
	require.NoError(t, err)
	require.Equal(t, beforeA, afterA)
	require.Equal(t, beforeB, afterB)
}

// Restrict size/depth while varying matched/mismatched payloads and source edges;
// arbitrary protobuf bytes mostly exercise unmarshalling rather than projection.
func FuzzProjectedBoundExpression(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	f.Add([]byte{255, 8, 0, 9, 1, 2, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64 {
			data = data[:64]
		}
		pos := 0
		next := func() byte {
			if pos >= len(data) {
				return 0
			}
			v := data[pos]
			pos++
			return v
		}
		var makeExpr func(int) *planpb.Expr
		makeExpr = func(depth int) *planpb.Expr {
			v := next()
			if v%13 == 12 {
				return nil
			}
			e := comparisonString()
			e.Typ.Table = string([]byte{next()})
			e.Typ.CollationCoercibility = uint32(next() % 8)
			e.Typ.CollationCoercibilitySet = next()%2 == 0
			if v&8 != 0 {
				e.XXX_unrecognized = []byte{0xa0, 0x06, v}
			}
			if depth == 0 {
				return e
			}
			switch v % 5 {
			case 0:
				e.Expr = &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.CAST, int32(next()%2))}, SyntaxExplicitCast: next()%2 == 0, Args: []*planpb.Expr{makeExpr(depth - 1), makeExpr(depth - 1)}}}
			case 1:
				e.Expr = &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{makeExpr(depth - 1)}}}
			case 2:
				e.GetLit().Src = makeExpr(depth - 1)
				e.GetLit().Isnull = next()%2 == 0
			case 3:
				e.Expr = &planpb.Expr_Sub{Sub: &planpb.SubqueryRef{Child: makeExpr(depth - 1)}}
			}
			return e
		}
		a := makeExpr(3)
		b := proto.Clone(a).(*planpb.Expr)
		if b != nil {
			b.Typ.Table = ""
			b.Typ.CollationCoercibility = 0
			b.Typ.CollationCoercibilitySet = false
			if next()%2 == 0 {
				b.Expr = makeExpr(2).GetExpr()
			}
		}
		assertBoundExpressionComparisons(t, a, b)
		assertBoundExpressionComparisons(t, b, a)
	})
}
