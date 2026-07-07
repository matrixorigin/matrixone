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

func TestValidateVectorIndexSortRewrite(t *testing.T) {
	// nil context: rewrite is allowed (no-op path).
	b := &QueryBuilder{}
	ok, err := b.validateVectorIndexSortRewrite(nil)
	require.NoError(t, err)
	require.True(t, ok)

	// ASC ordering: allowed.
	asc := &vectorSortContext{sortDirection: plan.OrderBySpec_ASC}
	ok, err = b.validateVectorIndexSortRewrite(asc)
	require.NoError(t, err)
	require.True(t, ok)

	// DESC ordering: rewrite blocked, no error (caller leaves the original
	// exact path in place rather than failing the query).
	desc := &vectorSortContext{sortDirection: plan.OrderBySpec_DESC}
	ok, err = b.validateVectorIndexSortRewrite(desc)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestReplaceDistFnInExpr_Substitutes(t *testing.T) {
	const scanTag int32 = 11
	const tfTag int32 = 22
	const partPos int32 = 1

	vecLit := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_array_float32)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "[1,2,3]"}}},
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

func TestReplaceDistFnInExpr_NoMatch(t *testing.T) {
	const scanTag int32 = 11
	const tfTag int32 = 22

	vecLit := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_array_float32)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "[1,2]"}}},
	}
	// nil expression — short circuit
	out := replaceDistFnInExpr(nil, scanTag, 0, "l2_distance", vecLit, tfTag, plan.Type{})
	require.Nil(t, out)

	// Wrong fn name → tree should walk into args but leave them unchanged here.
	other := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &ObjectRef{ObjName: "="},
			Args: []*plan.Expr{i64Lit(1), i64Lit(2)},
		}},
	}
	out = replaceDistFnInExpr(other, scanTag, 0, "l2_distance", vecLit, tfTag, plan.Type{})
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
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "[1,2,3]"}}},
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
