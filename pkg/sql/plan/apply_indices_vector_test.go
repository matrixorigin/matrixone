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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestIsDescendingVectorSort(t *testing.T) {
	require.True(t, isDescendingVectorSort(plan.OrderBySpec_DESC))
	require.False(t, isDescendingVectorSort(plan.OrderBySpec_ASC))
	require.False(t, isDescendingVectorSort(plan.OrderBySpec_INTERNAL))
}

func TestPickVectorLimit(t *testing.T) {
	// Sort.Limit takes precedence over scan and project.
	limA := i64Lit(10)
	limB := i64Lit(20)
	limC := i64Lit(30)
	rankA := &plan.RankOption{}
	sort := &plan.Node{Limit: limA, RankOption: rankA}
	scan := &plan.Node{Limit: limB}
	proj := &plan.Node{Limit: limC}
	got, gotRank := pickVectorLimit(sort, scan, proj)
	require.Equal(t, limA, got)
	require.Equal(t, rankA, gotRank)

	// Sort has no limit → fall back to scan.
	sort2 := &plan.Node{}
	got, _ = pickVectorLimit(sort2, scan, proj)
	require.Equal(t, limB, got)

	// Sort+scan have no limit → fall back to project.
	scan2 := &plan.Node{}
	got, _ = pickVectorLimit(sort2, scan2, proj)
	require.Equal(t, limC, got)

	// None have a limit → nil, nil.
	got, gotRank = pickVectorLimit(sort2, scan2, &plan.Node{})
	require.Nil(t, got)
	require.Nil(t, gotRank)
}

func TestVectorResultPaginationIsSeparateFromCandidateBudget(t *testing.T) {
	candidate := makePlan2Uint64ConstExprWithType(5)
	resultLimit := makePlan2Uint64ConstExprWithType(3)
	resultOffset := makePlan2Uint64ConstExprWithType(2)
	ctx := &vectorSortContext{
		limit:        candidate,
		resultLimit:  resultLimit,
		resultOffset: resultOffset,
	}

	limit, offset := vectorResultPagination(ctx)
	require.Equal(t, uint64(3), limit.GetLit().GetU64Val())
	require.Equal(t, uint64(2), offset.GetLit().GetU64Val())
	require.Equal(t, uint64(5), ctx.limit.GetLit().GetU64Val())
}

func TestVectorResultPaginationRejectsMissingResultLimit(t *testing.T) {
	ctx := &vectorSortContext{
		limit:    makePlan2Uint64ConstExprWithType(15),
		sortNode: &plan.Node{Offset: makePlan2Uint64ConstExprWithType(5)},
	}
	limit, offset := vectorResultPagination(ctx)
	require.Nil(t, limit)
	require.Nil(t, offset)
	require.False(t, hasCompleteVectorPagination(ctx))
}

func TestVectorRewritesRejectMissingResultPagination(t *testing.T) {
	builder := &QueryBuilder{}
	ctx := &vectorSortContext{
		sortNode: &plan.Node{},
		scanNode: &plan.Node{},
		limit:    makePlan2Uint64ConstExprWithType(15),
	}

	tests := []struct {
		name  string
		apply func() (int32, error)
	}{
		{name: "hnsw", apply: func() (int32, error) { return builder.applyIndicesForSortUsingHnsw(7, ctx, nil, nil) }},
		{name: "cagra", apply: func() (int32, error) { return builder.applyIndicesForSortUsingCagra(7, ctx, nil, nil) }},
		{name: "ivfpq", apply: func() (int32, error) { return builder.applyIndicesForSortUsingIvfpq(7, ctx, nil, nil) }},
		{name: "ivfflat", apply: func() (int32, error) { return builder.applyIndicesForSortUsingIvfflat(7, ctx, nil, nil, nil) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.apply()
			require.NoError(t, err)
			require.Equal(t, int32(7), got)
		})
	}
}

func TestVectorPaginationSurvivesPluginRoundTrip(t *testing.T) {
	ctx := &vectorSortContext{
		limit:        makePlan2Uint64ConstExprWithType(3),
		resultLimit:  makePlan2Uint64ConstExprWithType(2),
		resultOffset: makePlan2Uint64ConstExprWithType(1),
	}

	pluginCtx, _ := toPlanplugin(ctx, nil)
	roundTrip, _ := fromPlanplugin(pluginCtx, nil)
	limit, offset := vectorResultPagination(roundTrip)

	require.Equal(t, uint64(3), roundTrip.limit.GetLit().GetU64Val())
	require.Equal(t, uint64(2), limit.GetLit().GetU64Val())
	require.Equal(t, uint64(1), offset.GetLit().GetU64Val())
}

func TestValidateVectorIndexSortRewrite(t *testing.T) {
	// nil context: rewrite is allowed (no-op path).
	b := &QueryBuilder{}
	ok, err := b.validateVectorIndexSortRewrite(nil)
	require.NoError(t, err)
	require.True(t, ok)

	// ASC ordering with complete pagination ownership: allowed.
	asc := &vectorSortContext{
		sortDirection: plan.OrderBySpec_ASC,
		limit:         makePlan2Uint64ConstExprWithType(15),
		resultLimit:   makePlan2Uint64ConstExprWithType(10),
		resultOffset:  makePlan2Uint64ConstExprWithType(5),
	}
	ok, err = b.validateVectorIndexSortRewrite(asc)
	require.NoError(t, err)
	require.True(t, ok)

	// DESC ordering: rewrite blocked, no error (caller leaves the original
	// exact path in place rather than failing the query).
	desc := &vectorSortContext{
		sortDirection: plan.OrderBySpec_DESC,
		limit:         makePlan2Uint64ConstExprWithType(10),
		resultLimit:   makePlan2Uint64ConstExprWithType(10),
	}
	ok, err = b.validateVectorIndexSortRewrite(desc)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestReplaceDistFnInExpr_Substitutes(t *testing.T) {
	const scanTag int32 = 11
	const tfTag int32 = 22
	const partPos int32 = 1

	// A constant-folded vector literal carries the RAW element bytes in VecVal (constant_fold.go).
	vecLit := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_array_float32)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: string(types.ArrayToBytes([]float32{1, 2, 3}))}}},
	}
	// Build l2_distance(col[scanTag, partPos], vecLit).
	distFn := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_float64)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &ObjectRef{ObjName: "l2_distance"},
			Args: []*plan.Expr{
				{
					Typ: plan.Type{Id: int32(types.T_array_float32)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: scanTag, ColPos: partPos, Name: "vec",
					}},
				},
				vecLit,
			},
		}},
	}

	scoreType := plan.Type{Id: int32(types.T_float64)}
	out := replaceDistFnInExpr(distFn, scanTag, partPos, "l2_distance", vecLit, tfTag, scoreType)
	col := out.GetCol()
	require.NotNil(t, col, "expected substitution to a ColRef into the table function")
	require.Equal(t, tfTag, col.RelPos)
	require.Equal(t, int32(1), col.ColPos)
	require.Equal(t, "score", col.Name)
}

// TestReplaceDistFnInExpr_DifferentVector_NoSubstitute pins the precision guard: a distance on the
// right column and metric but a DIFFERENT query vector than the index/ORDER BY key must NOT be
// rewritten to this index's score (that would silently report the wrong distance — 1 != 2).
func TestReplaceDistFnInExpr_DifferentVector_NoSubstitute(t *testing.T) {
	const scanTag int32 = 11
	const tfTag int32 = 22
	const partPos int32 = 1

	vecA := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_array_float32)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: string(types.ArrayToBytes([]float32{1, 2, 3}))}}},
	}
	vecB := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_array_float32)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: string(types.ArrayToBytes([]float32{9, 9, 9}))}}},
	}
	// distFn = l2_distance(col[scanTag,partPos], vecA).
	distFn := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_float64)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &ObjectRef{ObjName: "l2_distance"},
			Args: []*plan.Expr{
				{
					Typ:  plan.Type{Id: int32(types.T_array_float32)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: partPos, Name: "vec"}},
				},
				vecA,
			},
		}},
	}
	scoreType := plan.Type{Id: int32(types.T_float64)}
	// The index / ORDER BY query vector is vecB — DIFFERENT from vecA: must be left as a distance.
	out := replaceDistFnInExpr(distFn, scanTag, partPos, "l2_distance", vecB, tfTag, scoreType)
	require.NotNil(t, out.GetF(), "distance on a different query vector must stay a distance, not the index score")
	require.Equal(t, "l2_distance", out.GetF().Func.ObjName)
}

// TestReplaceDistFnInExpr_UnfoldedCastMatches reproduces the #26961 shape: the ORDER BY key's vector
// is already constant-folded (a VecVal), while the SELECT-side copy is still an unfolded
// cast('[...]' as vecf32) with differently formatted but numerically identical text. vecFloatKey must
// parse both to the same float32 array and match, so the wrapped SELECT distance is rewritten to score.
func TestReplaceDistFnInExpr_UnfoldedCastMatches(t *testing.T) {
	const scanTag int32 = 11
	const tfTag int32 = 22
	const partPos int32 = 1

	// ORDER BY (folded) vector: raw element bytes in VecVal, as ConstantFold produces.
	vecLit := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_array_float32)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: string(types.ArrayToBytes([]float32{0.15, 0.25, 0.35}))}}},
	}
	// SELECT-side (unfolded) vector: cast('0.150,0.250,0.350...' as vecf32) — same values, different text.
	castArg := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_array_float32)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &ObjectRef{ObjName: "cast"},
			Args: []*plan.Expr{{
				Typ:  plan.Type{Id: int32(types.T_varchar)},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "[0.150000,0.250000,0.350000]"}}},
			}},
		}},
	}
	distFn := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_float64)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &ObjectRef{ObjName: "l2_distance"},
			Args: []*plan.Expr{
				{
					Typ:  plan.Type{Id: int32(types.T_array_float32)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: partPos, Name: "vec"}},
				},
				castArg,
			},
		}},
	}
	scoreType := plan.Type{Id: int32(types.T_float64)}
	out := replaceDistFnInExpr(distFn, scanTag, partPos, "l2_distance", vecLit, tfTag, scoreType)
	col := out.GetCol()
	require.NotNil(t, col, "unfolded cast() of the same vector must still be rewritten to the index score")
	require.Equal(t, tfTag, col.RelPos)
	require.Equal(t, int32(1), col.ColPos)
}

// foldedVecExpr builds a constant-folded vector literal (raw element bytes in VecVal) of the type.
func foldedVecExpr(typ types.T, raw []byte) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(typ)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: string(raw)}}},
	}
}

// castTextVecExpr builds an unfolded cast('<text>' as vec<typ>) expression.
func castTextVecExpr(typ types.T, text string) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(typ)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &ObjectRef{ObjName: "cast"},
			Args: []*plan.Expr{{
				Typ:  plan.Type{Id: int32(types.T_varchar)},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: text}}},
			}},
		}},
	}
}

// TestVecFloatKey_QuantizingVecCastNotPeeled: a vector-to-vector cast changes the value, so the inner
// literal must NOT be peeled and parsed at the outer element type. cast(cast('[1.001]' as vecbf16) as
// vecf16) is 0x3c00 (bf16 truncates 1.001 to 1.0), while a direct vecf16 of the same text is 0x3c01 —
// treating them as the same query vector would rewrite a SELECT distance to a score computed for a
// different vector. The nested cast yields no key (fail-safe); the direct textual cast still does.
func TestVecFloatKey_QuantizingVecCastNotPeeled(t *testing.T) {
	// Sanity: the two encodings really do differ, so this is a genuine counterexample.
	direct, err := types.StringToArray[types.Float16]("[1.001]")
	require.NoError(t, err)
	viaBf16 := []types.Float16{types.Float16FromFloat32(types.BF16FromFloat32(1.001).ToFloat32())}
	require.NotEqual(t, direct, viaBf16, "sanity: vecf16(1.001) and vecbf16(1.001)->vecf16 must differ")

	nested := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_array_float16)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &ObjectRef{ObjName: "cast"},
			Args: []*plan.Expr{castTextVecExpr(types.T_array_bf16, "[1.001]")},
		}},
	}
	_, ok := vecFloatKey(nested, types.T_array_float16)
	require.False(t, ok, "a quantizing vector-to-vector cast must not be peeled to the inner literal")

	// Control: the plain textual cast('[...]' as vecf16) is still parsed and keyed.
	k, ok := vecFloatKey(castTextVecExpr(types.T_array_float16, "[1.001]"), types.T_array_float16)
	require.True(t, ok, "a direct textual cast must still yield a key")
	require.Equal(t, string(types.ArrayToBytes(direct)), k)
}

// TestVecFloatKey_Uint8ByteExactNoNaNCollision: two distinct folded vecuint8 vectors whose 4 raw bytes
// happen to be distinct float32 NaN payloads must produce distinct keys. Decoding as float32 (the old
// behavior) canonicalized both to "[NaN]" and made a distance to one silently rewrite to the other.
func TestVecFloatKey_Uint8ByteExactNoNaNCollision(t *testing.T) {
	a := foldedVecExpr(types.T_array_uint8, types.ArrayToBytes([]uint8{0, 0, 192, 127}))
	b := foldedVecExpr(types.T_array_uint8, types.ArrayToBytes([]uint8{1, 0, 192, 127}))
	ka, oka := vecFloatKey(a, types.T_array_uint8)
	kb, okb := vecFloatKey(b, types.T_array_uint8)
	require.True(t, oka)
	require.True(t, okb)
	require.NotEqual(t, ka, kb, "distinct uint8 vectors must not collide (byte-exact key, not float32 NaN)")
	// Documents the old collision: interpreted as float32 both canonicalize to the same NaN string.
	na := types.ArrayToString(types.BytesToArray[float32](types.ArrayToBytes([]uint8{0, 0, 192, 127})))
	nb := types.ArrayToString(types.BytesToArray[float32](types.ArrayToBytes([]uint8{1, 0, 192, 127})))
	require.Equal(t, na, nb, "sanity: as float32 both were [NaN] — the bug the byte-exact key fixes")
}

// TestVecFloatKey_NarrowTextVsFoldedMatch: a folded vecuint8 vector and an unfolded
// cast('[...]' as vecuint8) of the same values must yield the same key (so the rewrite still fires).
func TestVecFloatKey_NarrowTextVsFoldedMatch(t *testing.T) {
	folded := foldedVecExpr(types.T_array_uint8, types.ArrayToBytes([]uint8{1, 2, 3}))
	unfolded := castTextVecExpr(types.T_array_uint8, "[1, 2, 3]")
	kf, okf := vecFloatKey(folded, types.T_array_uint8)
	ku, oku := vecFloatKey(unfolded, types.T_array_uint8)
	require.True(t, okf)
	require.True(t, oku)
	require.Equal(t, kf, ku, "unfolded uint8 text must match the folded uint8 vector")
}

// TestReplaceDistFnInExpr_Uint8DifferentVectorNotRewritten: the end-to-end #P1 repro — a SELECT-side
// distance to a DIFFERENT vecuint8 vector than the ORDER BY key must stay a distance, not become score.
func TestReplaceDistFnInExpr_Uint8DifferentVectorNotRewritten(t *testing.T) {
	const scanTag, tfTag, partPos int32 = 11, 22, 1
	vecLit := foldedVecExpr(types.T_array_uint8, types.ArrayToBytes([]uint8{0, 0, 192, 127})) // ORDER BY key
	other := foldedVecExpr(types.T_array_uint8, types.ArrayToBytes([]uint8{1, 0, 192, 127}))  // SELECT vector
	distFn := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_float64)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &ObjectRef{ObjName: "l2_distance"},
			Args: []*plan.Expr{
				{Typ: plan.Type{Id: int32(types.T_array_uint8)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: partPos, Name: "vec"}}},
				other,
			},
		}},
	}
	out := replaceDistFnInExpr(distFn, scanTag, partPos, "l2_distance", vecLit, tfTag, plan.Type{Id: int32(types.T_float64)})
	require.NotNil(t, out.GetF(), "distance to a DIFFERENT uint8 vector must stay a distance, not the index score")
	require.Equal(t, "l2_distance", out.GetF().Func.ObjName)
}

// TestReplaceDistFnInExpr_EmptyCastFailSafeNoPanic: the #P2 repro — an unfolded cast(” as vecf32)
// SELECT distance must not panic the planner; it is unparseable, so it stays a distance (fail-safe).
func TestReplaceDistFnInExpr_EmptyCastFailSafeNoPanic(t *testing.T) {
	const scanTag, tfTag, partPos int32 = 11, 22, 1
	vecLit := foldedVecExpr(types.T_array_float32, types.ArrayToBytes([]float32{0.1, 0.2, 0.3}))
	for _, bad := range []string{"", "   ", "\t"} {
		distFn := &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_float64)},
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &ObjectRef{ObjName: "l2_distance"},
				Args: []*plan.Expr{
					{Typ: plan.Type{Id: int32(types.T_array_float32)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: partPos, Name: "vec"}}},
					castTextVecExpr(types.T_array_float32, bad),
				},
			}},
		}
		require.NotPanics(t, func() {
			out := replaceDistFnInExpr(distFn, scanTag, partPos, "l2_distance", vecLit, tfTag, plan.Type{Id: int32(types.T_float64)})
			require.NotNil(t, out.GetF(), "empty/whitespace cast is unparseable -> stays a distance")
			require.Equal(t, "l2_distance", out.GetF().Func.ObjName)
		}, "malformed textual literal %q must fail safe, not panic", bad)
	}
}

func TestReplaceDistFnInExpr_NoMatch(t *testing.T) {
	const scanTag int32 = 11
	const tfTag int32 = 22

	// nil expression — short circuit
	out := replaceDistFnInExpr(nil, scanTag, 0, "l2_distance", nil, tfTag, plan.Type{})
	require.Nil(t, out)

	// Wrong fn name → tree should walk into args but leave them unchanged here.
	other := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &ObjectRef{ObjName: "="},
			Args: []*plan.Expr{i64Lit(1), i64Lit(2)},
		}},
	}
	out = replaceDistFnInExpr(other, scanTag, 0, "l2_distance", nil, tfTag, plan.Type{})
	require.NotNil(t, out)
	// Outer is still the same "=" function.
	require.Equal(t, "=", out.GetF().Func.ObjName)
}

// makeDistFnFilter builds a comparison filter `cmpOp(distFn(col[scanTag,partPos], vecLit), bound)`.
func makeDistFnFilter(cmpOp, distFn string, scanTag, partPos int32, vecVal string, bound *plan.Expr) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &ObjectRef{ObjName: cmpOp},
			Args: []*plan.Expr{
				{
					Typ: plan.Type{Id: int32(types.T_float64)},
					Expr: &plan.Expr_F{F: &plan.Function{
						Func: &ObjectRef{ObjName: distFn},
						Args: []*plan.Expr{
							{
								Typ: plan.Type{Id: int32(types.T_array_float32)},
								Expr: &plan.Expr_Col{Col: &plan.ColRef{
									RelPos: scanTag, ColPos: partPos, Name: "vec",
								}},
							},
							{
								Typ:  plan.Type{Id: int32(types.T_array_float32)},
								Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: vecVal}}},
							},
						},
					}},
				},
				bound,
			},
		}},
	}
}

func TestGetDistRangeFromFilters_AllOps(t *testing.T) {
	const scanTag int32 = 11
	const partPos int32 = 1
	vecVal := "[1,2,3]"
	vecLitArg := &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: vecVal}}},
	}

	cases := []struct {
		op        string
		lower     bool
		exclusive bool
	}{
		{"<", false, true},
		{"<=", false, false},
		{">", true, true},
		{">=", true, false},
	}
	for _, tc := range cases {
		t.Run(tc.op, func(t *testing.T) {
			f := makeDistFnFilter(tc.op, "l2_distance", scanTag, partPos, vecVal, f32Lit(0.5))
			var b *QueryBuilder
			rem, dr := b.getDistRangeFromFilters([]*plan.Expr{f}, partPos, "l2_distance", vecLitArg)
			require.Empty(t, rem)
			require.NotNil(t, dr)
			if tc.lower {
				if tc.exclusive {
					require.Equal(t, plan.BoundType_EXCLUSIVE, dr.LowerBoundType)
				} else {
					require.Equal(t, plan.BoundType_INCLUSIVE, dr.LowerBoundType)
				}
				require.NotNil(t, dr.LowerBound)
			} else {
				if tc.exclusive {
					require.Equal(t, plan.BoundType_EXCLUSIVE, dr.UpperBoundType)
				} else {
					require.Equal(t, plan.BoundType_INCLUSIVE, dr.UpperBoundType)
				}
				require.NotNil(t, dr.UpperBound)
			}
		})
	}
}

func TestGetDistRangeFromFilters_NonMatching(t *testing.T) {
	const scanTag int32 = 11
	const partPos int32 = 1
	vecLitArg := &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "[1,2,3]"}}},
	}
	var b *QueryBuilder

	// Wrong distfn name → kept as residual.
	bad := makeDistFnFilter("<", "cosine_distance", scanTag, partPos, "[1,2,3]", f32Lit(0.5))
	rem, dr := b.getDistRangeFromFilters([]*plan.Expr{bad}, partPos, "l2_distance", vecLitArg)
	require.Len(t, rem, 1)
	require.Nil(t, dr)

	// Wrong column position → kept.
	bad2 := makeDistFnFilter("<", "l2_distance", scanTag, partPos+1, "[1,2,3]", f32Lit(0.5))
	rem, dr = b.getDistRangeFromFilters([]*plan.Expr{bad2}, partPos, "l2_distance", vecLitArg)
	require.Len(t, rem, 1)
	require.Nil(t, dr)

	// Mismatched vec literal → kept.
	bad3 := makeDistFnFilter("<", "l2_distance", scanTag, partPos, "[9,9,9]", f32Lit(0.5))
	rem, dr = b.getDistRangeFromFilters([]*plan.Expr{bad3}, partPos, "l2_distance", vecLitArg)
	require.Len(t, rem, 1)
	require.Nil(t, dr)

	// Unsupported operator → kept.
	bad4 := makeDistFnFilter("=", "l2_distance", scanTag, partPos, "[1,2,3]", f32Lit(0.5))
	rem, dr = b.getDistRangeFromFilters([]*plan.Expr{bad4}, partPos, "l2_distance", vecLitArg)
	require.Len(t, rem, 1)
	require.Nil(t, dr)

	// Filter is not a function call (just a literal) → kept.
	rem, dr = b.getDistRangeFromFilters([]*plan.Expr{f32Lit(0.5)}, partPos, "l2_distance", vecLitArg)
	require.Len(t, rem, 1)
	require.Nil(t, dr)
}

// Multiple same-side distance bounds must fold into the tightest bound (the
// intersection), independent of filter order, so the index enforces the correct
// range and does not depend on which predicate happens to appear first. See
// issue #25639.
func TestGetDistRangeFromFiltersKeepsTightestBound(t *testing.T) {
	const scanTag int32 = 11
	const partPos int32 = 1
	const vecVal = "[1,2,3]"
	vecLitArg := &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: vecVal}}},
	}
	var builder *QueryBuilder

	// Two lower bounds: `> 1 AND >= 10` is `>= 10`; keep the tighter (larger).
	{
		loose := makeDistFnFilter(">", "l2_distance", scanTag, partPos, vecVal, f32Lit(1))
		tight := makeDistFnFilter(">=", "l2_distance", scanTag, partPos, vecVal, f32Lit(10))
		remaining, distRange := builder.getDistRangeFromFilters(
			[]*plan.Expr{loose, tight}, partPos, "l2_distance", vecLitArg,
		)
		require.Empty(t, remaining)
		require.Equal(t, plan.BoundType_INCLUSIVE, distRange.LowerBoundType)
		require.Equal(t, tight.GetF().Args[1], distRange.LowerBound)
	}

	// Two upper bounds, both orders: `< 1.1 AND < 1.2` is `< 1.1`; the tighter
	// (smaller) upper bound wins regardless of input order (the #25639 case).
	for _, order := range [][2]float32{{1.1, 1.2}, {1.2, 1.1}} {
		fa := makeDistFnFilter("<", "l2_distance", scanTag, partPos, vecVal, f32Lit(order[0]))
		fb := makeDistFnFilter("<", "l2_distance", scanTag, partPos, vecVal, f32Lit(order[1]))
		remaining, distRange := builder.getDistRangeFromFilters(
			[]*plan.Expr{fa, fb}, partPos, "l2_distance", vecLitArg,
		)
		require.Empty(t, remaining)
		require.Equal(t, plan.BoundType_EXCLUSIVE, distRange.UpperBoundType)
		v, ok := plan.GetLiteralFloat64(distRange.UpperBound)
		require.True(t, ok)
		require.InDelta(t, 1.1, v, 1e-6)
	}

	// A non-literal bound must never be peeled into the range (the reader can't
	// evaluate it): it stays a residual filter, in both orders, while a literal
	// bound of the same side still becomes the range. Regression for the
	// "first bound accepted without validation" case.
	nonLit := func() *plan.Expr {
		return &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: scanTag, ColPos: 7}}}
	}
	for _, nonLitFirst := range []bool{true, false} {
		bad := makeDistFnFilter("<", "l2_distance", scanTag, partPos, vecVal, nonLit())
		good := makeDistFnFilter("<", "l2_distance", scanTag, partPos, vecVal, f32Lit(1.1))
		input := []*plan.Expr{bad, good}
		if !nonLitFirst {
			input = []*plan.Expr{good, bad}
		}
		remaining, distRange := builder.getDistRangeFromFilters(
			input, partPos, "l2_distance", vecLitArg,
		)
		require.Equal(t, []*plan.Expr{bad}, remaining) // non-literal kept as residual
		require.Equal(t, plan.BoundType_EXCLUSIVE, distRange.UpperBoundType)
		v, ok := plan.GetLiteralFloat64(distRange.UpperBound)
		require.True(t, ok)
		require.InDelta(t, 1.1, v, 1e-6)
	}
}

func TestPeelAndRewriteDistFnFilters_AllOps(t *testing.T) {
	const scanTag int32 = 11
	const partPos int32 = 1
	const tfTag int32 = 22
	vecVal := "[1,2,3]"
	vecLitArg := &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: vecVal}}},
	}
	scoreType := plan.Type{Id: int32(types.T_float64)}
	var b *QueryBuilder

	for _, op := range []string{"<", "<=", ">", ">="} {
		t.Run(op, func(t *testing.T) {
			f := makeDistFnFilter(op, "l2_distance", scanTag, partPos, vecVal, f32Lit(0.4))
			rem, peeled := b.peelAndRewriteDistFnFilters(
				[]*plan.Expr{f}, partPos, "l2_distance", vecLitArg, tfTag, scoreType)
			require.Empty(t, rem)
			require.Len(t, peeled, 1)

			peeledFn := peeled[0].GetF()
			require.NotNil(t, peeledFn)
			require.Equal(t, op, peeledFn.Func.ObjName)
			// Args[0] now references the table function's score column.
			col := peeledFn.Args[0].GetCol()
			require.NotNil(t, col)
			require.Equal(t, tfTag, col.RelPos)
			require.Equal(t, int32(1), col.ColPos)
			require.Equal(t, "score", col.Name)
		})
	}
}

func TestPeelAndRewriteDistFnFilters_KeepsNonMatching(t *testing.T) {
	const scanTag int32 = 11
	const partPos int32 = 1
	const tfTag int32 = 22
	vecLitArg := &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "[1,2,3]"}}},
	}
	scoreType := plan.Type{Id: int32(types.T_float64)}
	var b *QueryBuilder

	// "=" comparison isn't peeled.
	eq := makeDistFnFilter("=", "l2_distance", scanTag, partPos, "[1,2,3]", f32Lit(0.4))
	// Wrong distance fn name.
	wrongFn := makeDistFnFilter("<", "cosine_distance", scanTag, partPos, "[1,2,3]", f32Lit(0.4))
	// Wrong column position.
	wrongCol := makeDistFnFilter("<", "l2_distance", scanTag, partPos+1, "[1,2,3]", f32Lit(0.4))
	// Mismatched vec literal.
	wrongVec := makeDistFnFilter("<", "l2_distance", scanTag, partPos, "[9,9,9]", f32Lit(0.4))
	// Bare literal (not a function).
	bare := f32Lit(0.4)

	rem, peeled := b.peelAndRewriteDistFnFilters(
		[]*plan.Expr{eq, wrongFn, wrongCol, wrongVec, bare}, partPos, "l2_distance", vecLitArg, tfTag, scoreType)
	require.Empty(t, peeled)
	require.Len(t, rem, 5)
}

func TestReplaceDistFnExprsWithScoreCol(t *testing.T) {
	const scanTag int32 = 11
	const tfTag int32 = 22
	const partPos int32 = 1

	vecLit := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_array_float32)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: string(types.ArrayToBytes([]float32{1, 2, 3}))}}},
	}
	distFn := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_float64)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &ObjectRef{ObjName: "l2_distance"},
			Args: []*plan.Expr{
				{
					Typ: plan.Type{Id: int32(types.T_array_float32)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{
						RelPos: scanTag, ColPos: partPos, Name: "vec",
					}},
				},
				vecLit,
			},
		}},
	}
	exprs := []*plan.Expr{distFn}
	scoreType := plan.Type{Id: int32(types.T_float64)}
	replaceDistFnExprsWithScoreCol(exprs, scanTag, partPos, "l2_distance", vecLit, tfTag, scoreType)
	col := exprs[0].GetCol()
	require.NotNil(t, col)
	require.Equal(t, tfTag, col.RelPos)
}
