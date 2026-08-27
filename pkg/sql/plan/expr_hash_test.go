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
	"context"
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

func TestExprStructuralIdentityIncludesNegativeAuxID(t *testing.T) {
	a := int64Lit(1)
	a.AuxId = -1
	b := int64Lit(1)
	b.AuxId = -2
	c := int64Lit(1)
	c.AuxId = -1

	require.NotEqual(t, exprStructuralHash(a), exprStructuralHash(b))
	require.False(t, exprStructuralEqual(a, b))
	require.Equal(t, exprStructuralHash(a), exprStructuralHash(c))
	require.True(t, exprStructuralEqual(a, c))

	ordinaryA := int64Lit(1)
	ordinaryA.AuxId = 1
	ordinaryB := int64Lit(1)
	ordinaryB.AuxId = 2
	require.Equal(t, exprStructuralHash(ordinaryA), exprStructuralHash(ordinaryB))
	require.True(t, exprStructuralEqual(ordinaryA, ordinaryB))
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

func binStrLit(v string) *planpb.Expr {
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_varchar), Width: 64},
		Expr: &planpb.Expr_Lit{
			Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: v}, IsBin: true},
		},
	}
}

func TestExprStructuralHashDistinguishesIsBin(t *testing.T) {
	a := strLit("1")
	b := binStrLit("1")

	require.NotEqual(t, exprStructuralHash(a), exprStructuralHash(b),
		"hash must differ for same Sval but different IsBin")
	require.False(t, exprStructuralEqual(a, b),
		"equal must return false for same Sval but different IsBin")

	c := binStrLit("1")
	require.Equal(t, exprStructuralHash(b), exprStructuralHash(c))
	require.True(t, exprStructuralEqual(b, c))
}

func TestExprStructuralHashDistinguishesLiteralForm(t *testing.T) {
	a := strLit("1")
	b := strLit("1")
	a.GetLit().LiteralForm = planpb.StringLiteralForm_STRING_LITERAL_HEX
	b.GetLit().LiteralForm = planpb.StringLiteralForm_STRING_LITERAL_BIT

	require.NotEqual(t, exprStructuralHash(a), exprStructuralHash(b))
	require.False(t, exprStructuralEqual(a, b))
}

func TestExprStructuralIdentityNormalizesTextFormOnlyInTextDomain(t *testing.T) {
	for _, test := range []struct {
		name  string
		typ   types.Type
		equal bool
	}{
		{name: "text domain", typ: types.T_varchar.ToType(), equal: true},
		{name: "binary domain", typ: types.T_varbinary.ToType(), equal: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			none := &planpb.Expr{Typ: planpb.Type{
				Id: int32(test.typ.Oid), Charset: uint32(test.typ.Charset),
			}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
				Value: &planpb.Literal_Sval{Sval: "same"},
			}}}
			text := DeepCopyExpr(none)
			text.GetLit().LiteralForm = planpb.StringLiteralForm_STRING_LITERAL_TEXT

			if test.equal {
				require.Equal(t, exprStructuralHash(none), exprStructuralHash(text))
				require.True(t, exprStructuralEqual(none, text))
			} else {
				require.NotEqual(t, exprStructuralHash(none), exprStructuralHash(text))
				require.False(t, exprStructuralEqual(none, text))
			}
		})
	}
}

func TestExprStructuralHashIgnoresDiagnosticProvenance(t *testing.T) {
	literal := strLit("encoded")
	serializedLiteral := DeepCopyExpr(literal)
	serializedLiteral.GetLit().IsSerialized = true
	require.Equal(t, exprStructuralHash(literal), exprStructuralHash(serializedLiteral))
	require.True(t, exprStructuralEqual(literal, serializedLiteral))

	decimalLiteral := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 8, Scale: 2},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Value: &planpb.Literal_Decimal64Val{Decimal64Val: &planpb.Decimal64{A: 1234}},
		}},
	}
	serializedDecimal := DeepCopyExpr(decimalLiteral)
	serializedDecimal.GetLit().IsSerialized = true
	require.Equal(t, exprStructuralHash(decimalLiteral), exprStructuralHash(serializedDecimal),
		"fallback literal variants must also ignore diagnostic provenance")
	require.True(t, exprStructuralEqual(decimalLiteral, serializedDecimal))

	vectorExpr := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{
			Len:  2,
			Data: []byte("same executable vector"),
		}},
	}
	serializedVector := DeepCopyExpr(vectorExpr)
	serializedVector.GetVec().IsSerialized = true
	require.Equal(t, exprStructuralHash(vectorExpr), exprStructuralHash(serializedVector))
	require.True(t, exprStructuralEqual(vectorExpr, serializedVector))

	differentSource := DeepCopyExpr(vectorExpr)
	differentSource.GetVec().StringSource = uint32(types.StringSourceLiteral)
	require.NotEqual(t, exprStructuralHash(vectorExpr), exprStructuralHash(differentSource))
	require.False(t, exprStructuralEqual(vectorExpr, differentSource))

	literalSource := strLit("same literal")
	expressionSource := DeepCopyExpr(literalSource)
	expressionSource.GetLit().StringSource = uint32(types.StringSourceExpression) + 1
	require.NotEqual(t, exprStructuralHash(literalSource), exprStructuralHash(expressionSource))
	require.False(t, exprStructuralEqual(literalSource, expressionSource))

	differentData := DeepCopyExpr(vectorExpr)
	differentData.GetVec().Data = []byte("different executable vector")
	require.NotEqual(t, exprStructuralHash(vectorExpr), exprStructuralHash(differentData))
	require.False(t, exprStructuralEqual(vectorExpr, differentData))

	differentLen := DeepCopyExpr(vectorExpr)
	differentLen.GetVec().Len++
	require.NotEqual(t, exprStructuralHash(vectorExpr), exprStructuralHash(differentLen))
	require.False(t, exprStructuralEqual(vectorExpr, differentLen))
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

// TestApplyDistributivityIsBinNotFactored verifies that applyDistributivity
// does NOT factor out expressions that differ only in IsBin. Given:
//
//	(cast(a as int64) = _binary '1' AND p) OR (cast(a as int64) = '1' AND q)
//
// the two equality exprs must remain separate (not extracted as common).
func TestApplyDistributivityIsBinNotFactored(t *testing.T) {
	ctx := context.Background()

	col := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Col{
			Col: &planpb.ColRef{RelPos: 0, ColPos: 0, Name: "a"},
		},
	}

	mkEq := func(isBin bool) *planpb.Expr {
		lit := &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_varchar), Width: 1},
			Expr: &planpb.Expr_Lit{
				Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "1"}, IsBin: isBin},
			},
		}
		eq, _ := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{
			DeepCopyExpr(col), lit,
		})
		return eq
	}

	p := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_Col{
			Col: &planpb.ColRef{RelPos: 1, ColPos: 0, Name: "p"},
		},
	}
	q := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_Col{
			Col: &planpb.ColRef{RelPos: 1, ColPos: 1, Name: "q"},
		},
	}

	// (eq_binary AND p) OR (eq_text AND q)
	left, _ := BindFuncExprImplByPlanExpr(ctx, "and", []*planpb.Expr{mkEq(true), p})
	right, _ := BindFuncExprImplByPlanExpr(ctx, "and", []*planpb.Expr{mkEq(false), q})
	orExpr, _ := BindFuncExprImplByPlanExpr(ctx, "or", []*planpb.Expr{left, right})

	result := applyDistributivity(ctx, orExpr)

	// If factoring happened incorrectly, the result would be:
	//   eq AND (p OR q)   — a top-level "and"
	// Correct result keeps the OR at top level since no common factor exists.
	fn := result.GetF()
	require.NotNil(t, fn)
	require.Equal(t, "or", fn.Func.ObjName,
		"_binary '1' and '1' must not be factored as common; OR must remain at top")
}

func TestApplyDistributivityDoesNotFactorCrossDomainTextOverride(t *testing.T) {
	ctx := context.Background()
	varbinaryType := planpb.Type{Id: int32(types.T_varbinary), Charset: uint32(types.CharsetBinary)}
	col := &planpb.Expr{Typ: varbinaryType, Expr: &planpb.Expr_Col{
		Col: &planpb.ColRef{RelPos: 0, ColPos: 0, Name: "a"},
	}}
	mkEq := func(form planpb.StringLiteralForm) *planpb.Expr {
		return &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_bool)}, Expr: &planpb.Expr_F{
			F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "="}, Args: []*planpb.Expr{
				DeepCopyExpr(col),
				{Typ: varbinaryType, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
					Value: &planpb.Literal_Sval{Sval: "same"}, LiteralForm: form,
				}}},
			}},
		}}
	}
	p := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_bool)}, Expr: &planpb.Expr_Col{
		Col: &planpb.ColRef{RelPos: 1, ColPos: 0, Name: "p"},
	}}
	q := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_bool)}, Expr: &planpb.Expr_Col{
		Col: &planpb.ColRef{RelPos: 1, ColPos: 1, Name: "q"},
	}}
	left, err := BindFuncExprImplByPlanExpr(ctx, "and", []*planpb.Expr{
		mkEq(planpb.StringLiteralForm_STRING_LITERAL_TEXT), p,
	})
	require.NoError(t, err)
	right, err := BindFuncExprImplByPlanExpr(ctx, "and", []*planpb.Expr{
		mkEq(planpb.StringLiteralForm_STRING_LITERAL_NONE), q,
	})
	require.NoError(t, err)
	orExpr, err := BindFuncExprImplByPlanExpr(ctx, "or", []*planpb.Expr{left, right})
	require.NoError(t, err)

	result := applyDistributivity(ctx, orExpr)
	require.NotNil(t, result.GetF())
	require.Equal(t, "or", result.GetF().Func.ObjName)
}

func TestApplyDistributivityFindsJoinKeyBesideTernaryPredicate(t *testing.T) {
	ctx := context.Background()
	intType := planpb.Type{Id: int32(types.T_int64)}
	boolType := planpb.Type{Id: int32(types.T_bool)}
	col := func(rel, pos int32) *planpb.Expr {
		return &planpb.Expr{Typ: intType, Expr: &planpb.Expr_Col{
			Col: &planpb.ColRef{RelPos: rel, ColPos: pos},
		}}
	}
	lit := func(value int64) *planpb.Expr {
		return &planpb.Expr{Typ: intType, Expr: &planpb.Expr_Lit{
			Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: value}},
		}}
	}
	bind := func(name string, args ...*planpb.Expr) *planpb.Expr {
		expr, err := BindFuncExprImplByPlanExpr(ctx, name, args)
		require.NoError(t, err)
		return expr
	}

	joinKey := bind("=", col(0, 0), col(1, 0))
	branch := func(flag, low, high int64) *planpb.Expr {
		return bind("and",
			bind("and", DeepCopyExpr(joinKey), bind("=", col(0, 1), lit(flag))),
			bind("between", col(1, 1), lit(low), lit(high)))
	}
	orExpr := bind("or", branch(1, 10, 20), branch(2, 30, 40))
	require.Equal(t, boolType.Id, orExpr.Typ.Id)

	result := applyDistributivity(ctx, orExpr)
	conjuncts := splitPlanConjunction(result)
	require.Len(t, conjuncts, 2)
	require.True(t, exprStructuralEqual(joinKey, conjuncts[0]),
		"the cross-table equality must become a visible join key")
	require.Equal(t, "or", conjuncts[1].GetF().Func.ObjName)
}

func TestApplyDistributivityKeepsSingleTableDNFForKeyFolding(t *testing.T) {
	ctx := context.Background()
	intType := planpb.Type{Id: int32(types.T_int64)}
	col := func(pos int32) *planpb.Expr {
		return &planpb.Expr{Typ: intType, Expr: &planpb.Expr_Col{
			Col: &planpb.ColRef{RelPos: 0, ColPos: pos},
		}}
	}
	lit := func(value int64) *planpb.Expr {
		return &planpb.Expr{Typ: intType, Expr: &planpb.Expr_Lit{
			Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: value}},
		}}
	}
	bind := func(name string, args ...*planpb.Expr) *planpb.Expr {
		expr, err := BindFuncExprImplByPlanExpr(ctx, name, args)
		require.NoError(t, err)
		return expr
	}

	common := bind("=", col(0), lit(1))
	orExpr := bind("or",
		bind("and", DeepCopyExpr(common), bind("=", col(1), lit(2))),
		bind("and", DeepCopyExpr(common), bind("=", col(1), lit(3))))

	result := applyDistributivity(ctx, orExpr)
	require.Equal(t, "or", result.GetF().Func.ObjName,
		"single-table DNF must remain available to composite-key folding")
}
