// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// TestExprStructuralHashDistinguishesObjectRef guards against a regression
// where Expr_F was hashed only by ObjName. Two function exprs with identical
// names but different ObjectRef identities (e.g. different overload ids,
// schemas, or databases) must hash and compare as distinct; otherwise
// applyDistributivity can factor them as a common subexpression.
func TestExprStructuralHashDistinguishesObjectRef(t *testing.T) {
	colExpr := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Col{
			Col: &planpb.ColRef{RelPos: 0, ColPos: 0, Name: "a"},
		},
	}
	litExpr := MakePlan2Int64ConstExprWithType(1)

	mkFn := func(schema string, obj int64) *planpb.Expr {
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_bool)},
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{
						ObjName:    "foo",
						SchemaName: schema,
						Obj:        obj,
					},
					Args: []*planpb.Expr{DeepCopyExpr(colExpr), DeepCopyExpr(litExpr)},
				},
			},
		}
	}

	a := mkFn("db_a", 100)
	b := mkFn("db_b", 200)

	require.NotEqual(t, exprStructuralHash(a), exprStructuralHash(b),
		"hash must differ for same ObjName but different ObjectRef identity")
	require.False(t, exprStructuralEqual(a, b),
		"equal must return false for same ObjName but different ObjectRef identity")

	// Sanity check: an identical copy must hash + compare equal.
	c := mkFn("db_a", 100)
	require.Equal(t, exprStructuralHash(a), exprStructuralHash(c))
	require.True(t, exprStructuralEqual(a, c))
}

func int64Lit(v int64) *planpb.Expr {
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Lit{
			Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: v}},
		},
	}
}

func uint64Lit(v uint64) *planpb.Expr {
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_uint64)},
		Expr: &planpb.Expr_Lit{
			Lit: &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: v}},
		},
	}
}

func strLit(v string) *planpb.Expr {
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_varchar), Width: 64},
		Expr: &planpb.Expr_Lit{
			Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: v}},
		},
	}
}

func boolLit(v bool) *planpb.Expr {
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_Lit{
			Lit: &planpb.Literal{Value: &planpb.Literal_Bval{Bval: v}},
		},
	}
}

func nullLit() *planpb.Expr {
	return &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}},
	}
}

// TestExprStructuralHashLiteralVariants walks every literal variant the hash
// enumerates explicitly, ensuring distinct values hash / compare distinct and
// equal values compare equal.
func TestExprStructuralHashLiteralVariants(t *testing.T) {
	type pair struct {
		name string
		a, b *planpb.Expr
	}
	cases := []pair{
		{"i64", int64Lit(1), int64Lit(2)},
		{"u64", uint64Lit(1), uint64Lit(2)},
		{"string", strLit("a"), strLit("b")},
		{"bool", boolLit(true), boolLit(false)},
		{
			"i8",
			&planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int8)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I8Val{I8Val: 1}}}},
			&planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int8)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I8Val{I8Val: 2}}}},
		},
		{
			"u8",
			&planpb.Expr{Typ: planpb.Type{Id: int32(types.T_uint8)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_U8Val{U8Val: 1}}}},
			&planpb.Expr{Typ: planpb.Type{Id: int32(types.T_uint8)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_U8Val{U8Val: 2}}}},
		},
		{
			"double",
			&planpb.Expr{Typ: planpb.Type{Id: int32(types.T_float64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Dval{Dval: 1.5}}}},
			&planpb.Expr{Typ: planpb.Type{Id: int32(types.T_float64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Dval{Dval: 2.5}}}},
		},
		{
			"date",
			&planpb.Expr{Typ: planpb.Type{Id: int32(types.T_date)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Dateval{Dateval: 100}}}},
			&planpb.Expr{Typ: planpb.Type{Id: int32(types.T_date)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Dateval{Dateval: 200}}}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotEqual(t, exprStructuralHash(tc.a), exprStructuralHash(tc.b))
			require.False(t, exprStructuralEqual(tc.a, tc.b))
			require.True(t, exprStructuralEqual(tc.a, tc.a))
		})
	}
}

// TestExprStructuralEqualNullAndTypeMismatch covers the null-vs-non-null and
// cross-variant paths (e.g. literal vs function, literal vs column).
func TestExprStructuralEqualNullAndTypeMismatch(t *testing.T) {
	n1 := nullLit()
	n2 := nullLit()
	require.True(t, exprStructuralEqual(n1, n2))
	require.Equal(t, exprStructuralHash(n1), exprStructuralHash(n2))

	require.False(t, exprStructuralEqual(n1, int64Lit(0)),
		"null literal must not equal a non-null literal of the same type")

	col := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Col{
			Col: &planpb.ColRef{RelPos: 0, ColPos: 0, Name: "a"},
		},
	}
	require.False(t, exprStructuralEqual(col, int64Lit(0)),
		"column and literal must not compare equal")

	// Same Col shape but different RelPos/ColPos must differ.
	col2 := &planpb.Expr{
		Typ: col.Typ,
		Expr: &planpb.Expr_Col{
			Col: &planpb.ColRef{RelPos: 1, ColPos: 0, Name: "a"},
		},
	}
	require.False(t, exprStructuralEqual(col, col2))
	require.NotEqual(t, exprStructuralHash(col), exprStructuralHash(col2))

	// Different Typ widths should also differ.
	wideLit := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_varchar), Width: 128},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "a"}}},
	}
	narrowLit := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_varchar), Width: 64},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "a"}}},
	}
	require.False(t, exprStructuralEqual(wideLit, narrowLit))
	require.NotEqual(t, exprStructuralHash(wideLit), exprStructuralHash(narrowLit))

	// nil exprs.
	require.True(t, exprStructuralEqual(nil, nil))
	require.False(t, exprStructuralEqual(nil, col))
	require.False(t, exprStructuralEqual(col, nil))
}

// TestExprStructuralFunctionAndList covers Expr_F arg recursion (differences
// only in deep child nodes must propagate) and Expr_List ordering.
func TestExprStructuralFunctionAndList(t *testing.T) {
	col := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Col{
			Col: &planpb.ColRef{RelPos: 0, ColPos: 0, Name: "a"},
		},
	}
	mkIn := func(values ...int64) *planpb.Expr {
		vs := make([]*planpb.Expr, 0, len(values))
		for _, v := range values {
			vs = append(vs, int64Lit(v))
		}
		list := &planpb.Expr{
			Typ:  col.Typ,
			Expr: &planpb.Expr_List{List: &planpb.ExprList{List: vs}},
		}
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_bool)},
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "in"},
					Args: []*planpb.Expr{DeepCopyExpr(col), list},
				},
			},
		}
	}
	a := mkIn(1, 2, 3)
	b := mkIn(1, 2, 4)
	c := mkIn(1, 2, 3)

	require.True(t, exprStructuralEqual(a, c))
	require.Equal(t, exprStructuralHash(a), exprStructuralHash(c))

	require.False(t, exprStructuralEqual(a, b))
	require.NotEqual(t, exprStructuralHash(a), exprStructuralHash(b))

	// Arity mismatch: `in` vs `in` with one fewer element.
	shorter := mkIn(1, 2)
	require.False(t, exprStructuralEqual(a, shorter))

	// Cross-kind: column vs function must not compare equal.
	require.False(t, exprStructuralEqual(col, a))
}

// TestExprStructuralUncommonVariantFallback exercises the default-case
// Marshal-fallback path for variants the fast path doesn't enumerate
// (Expr_Sub is used here because it's easy to construct). Two Sub nodes with
// different NodeId must compare distinct via the proto-bytes fallback.
func TestExprStructuralUncommonVariantFallback(t *testing.T) {
	mk := func(nodeID int32) *planpb.Expr {
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_bool)},
			Expr: &planpb.Expr_Sub{
				Sub: &planpb.SubqueryRef{NodeId: nodeID},
			},
		}
	}
	a := mk(1)
	b := mk(2)
	c := mk(1)

	require.NotEqual(t, exprStructuralHash(a), exprStructuralHash(b))
	require.False(t, exprStructuralEqual(a, b))

	require.Equal(t, exprStructuralHash(a), exprStructuralHash(c))
	require.True(t, exprStructuralEqual(a, c))
}

// TestExprStructuralLiteralAllVariants covers every Literal variant enumerated
// by hashLitInto / literalEqual, including ones not easily produced through
// the binder. This fills in the rarer temporal / decimal / json / fval /
// defaultval / updateVal / enum / vec / u32 / i32 / i16 / u16 branches.
func TestExprStructuralLiteralAllVariants(t *testing.T) {
	type variant struct {
		name  string
		a, b  *planpb.Expr
		typID int32
	}
	// The interface type `isLiteral_Value` is unexported in the proto
	// package; we rely on the concrete variant types below satisfying the
	// Literal.Value field dynamically.
	mk := func(typID int32, va, vb interface{}) (*planpb.Expr, *planpb.Expr) {
		mkOne := func(v interface{}) *planpb.Expr {
			lit := &planpb.Literal{}
			// Assign via reflect-free approach: each concrete variant
			// already implements isLiteral_Value, so a direct field set
			// via a type switch would work — but the test helpers below
			// pass them typed as the variant, and Go allows assignment
			// if we go through an interface-typed Literal.Value field.
			switch v := v.(type) {
			case *planpb.Literal_I16Val:
				lit.Value = v
			case *planpb.Literal_I32Val:
				lit.Value = v
			case *planpb.Literal_U16Val:
				lit.Value = v
			case *planpb.Literal_U32Val:
				lit.Value = v
			case *planpb.Literal_Fval:
				lit.Value = v
			case *planpb.Literal_Timeval:
				lit.Value = v
			case *planpb.Literal_Datetimeval:
				lit.Value = v
			case *planpb.Literal_Timestampval:
				lit.Value = v
			case *planpb.Literal_EnumVal:
				lit.Value = v
			case *planpb.Literal_Jsonval:
				lit.Value = v
			case *planpb.Literal_Defaultval:
				lit.Value = v
			case *planpb.Literal_UpdateVal:
				lit.Value = v
			case *planpb.Literal_VecVal:
				lit.Value = v
			default:
				panic("unexpected literal variant in test helper")
			}
			return &planpb.Expr{
				Typ:  planpb.Type{Id: typID},
				Expr: &planpb.Expr_Lit{Lit: lit},
			}
		}
		return mkOne(va), mkOne(vb)
	}
	cases := []variant{}
	add := func(name string, typID int32, va, vb interface{}) {
		a, b := mk(typID, va, vb)
		cases = append(cases, variant{name, a, b, typID})
	}
	add("i16", int32(types.T_int16),
		&planpb.Literal_I16Val{I16Val: 1}, &planpb.Literal_I16Val{I16Val: 2})
	add("i32", int32(types.T_int32),
		&planpb.Literal_I32Val{I32Val: 10}, &planpb.Literal_I32Val{I32Val: 20})
	add("u16", int32(types.T_uint16),
		&planpb.Literal_U16Val{U16Val: 1}, &planpb.Literal_U16Val{U16Val: 2})
	add("u32", int32(types.T_uint32),
		&planpb.Literal_U32Val{U32Val: 1}, &planpb.Literal_U32Val{U32Val: 2})
	add("fval", int32(types.T_float32),
		&planpb.Literal_Fval{Fval: 1.25}, &planpb.Literal_Fval{Fval: 2.5})
	add("time", int32(types.T_time),
		&planpb.Literal_Timeval{Timeval: 100}, &planpb.Literal_Timeval{Timeval: 200})
	add("datetime", int32(types.T_datetime),
		&planpb.Literal_Datetimeval{Datetimeval: 1}, &planpb.Literal_Datetimeval{Datetimeval: 2})
	add("timestamp", int32(types.T_timestamp),
		&planpb.Literal_Timestampval{Timestampval: 1}, &planpb.Literal_Timestampval{Timestampval: 2})
	add("enum", int32(types.T_enum),
		&planpb.Literal_EnumVal{EnumVal: 1}, &planpb.Literal_EnumVal{EnumVal: 2})
	add("json", int32(types.T_json),
		&planpb.Literal_Jsonval{Jsonval: `{"a":1}`}, &planpb.Literal_Jsonval{Jsonval: `{"a":2}`})

	// These go through the default / Marshal fallback in hashLitInto and
	// literalEqual (Defaultval, UpdateVal, VecVal).
	add("defaultval", int32(types.T_bool),
		&planpb.Literal_Defaultval{Defaultval: true}, &planpb.Literal_Defaultval{Defaultval: false})
	add("updateval", int32(types.T_bool),
		&planpb.Literal_UpdateVal{UpdateVal: true}, &planpb.Literal_UpdateVal{UpdateVal: false})
	add("vecval", int32(types.T_array_float32),
		&planpb.Literal_VecVal{VecVal: "a"}, &planpb.Literal_VecVal{VecVal: "b"})

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotEqual(t, exprStructuralHash(tc.a), exprStructuralHash(tc.b),
				"%s: distinct values must hash differently", tc.name)
			require.False(t, exprStructuralEqual(tc.a, tc.b),
				"%s: distinct values must not compare equal", tc.name)
			require.True(t, exprStructuralEqual(tc.a, tc.a),
				"%s: identity must compare equal", tc.name)
		})
	}
}

// TestExprStructuralLiteralMismatchAcrossVariants guards literalEqual's
// variant-type check: a literal of one variant must not equal a literal of a
// different variant (e.g. i64 vs u64 even if the numeric value is the same).
func TestExprStructuralLiteralMismatchAcrossVariants(t *testing.T) {
	i := int64Lit(1)
	u := uint64Lit(1)
	require.False(t, exprStructuralEqual(i, u))
}

// TestObjectRefEqualNil covers the nil handling shortcut in objectRefEqual.
func TestObjectRefEqualNil(t *testing.T) {
	require.True(t, objectRefEqual(nil, nil))

	ref := &planpb.ObjectRef{ObjName: "x"}
	require.True(t, objectRefEqual(ref, ref))

	other := &planpb.ObjectRef{ObjName: "x"}
	require.True(t, objectRefEqual(ref, other))

	diff := &planpb.ObjectRef{ObjName: "y"}
	require.False(t, objectRefEqual(ref, diff))

	require.False(t, objectRefEqual(nil, ref))
	require.False(t, objectRefEqual(ref, nil))
}
