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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestBoundedBinarySliceRefinesBlobResultWidth(t *testing.T) {
	blobType := types.NewWithCharset(types.T_blob, 0, 0, types.CharsetBinary)
	source := &planpb.Expr{
		Typ: blobType.PlanType(),
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			IsBin:       true,
			LiteralForm: planpb.StringLiteralForm_STRING_LITERAL_HEX,
			Value:       &planpb.Literal_Sval{Sval: "61"},
		}},
	}
	count := makePlan2Int64ConstExprWithType(1)
	start := makePlan2Int64ConstExprWithType(1)

	t.Run("left-right", func(t *testing.T) {
		got := blobType
		refineLeftRightLiteralReturnType([]*planpb.Expr{source, count}, &got)
		require.Equal(t, types.T_varbinary, got.Oid)
		require.Equal(t, int32(1), got.Width)
		require.Equal(t, types.CharsetBinary, got.Charset)
	})

	t.Run("substring", func(t *testing.T) {
		got := blobType
		refineSubstringLiteralReturnType([]*planpb.Expr{source, start, count}, &got)
		require.Equal(t, types.T_varbinary, got.Oid)
		require.Equal(t, int32(1), got.Width)
		require.Equal(t, types.CharsetBinary, got.Charset)
	})

	t.Run("unknown-blob-domain-stays-unbounded", func(t *testing.T) {
		unknown := &planpb.Expr{
			Typ:  blobType.PlanType(),
			Expr: &planpb.Expr_V{V: &planpb.VarRef{Name: "v"}},
		}
		left := blobType
		refineLeftRightLiteralReturnType([]*planpb.Expr{unknown, count}, &left)
		require.Equal(t, blobType, left)

		substring := blobType
		refineSubstringLiteralReturnType([]*planpb.Expr{unknown, start, count}, &substring)
		require.Equal(t, blobType, substring)
	})
}

func TestHexBoundedBlobSlicePreservesSliceType(t *testing.T) {
	ctx := context.Background()
	blobType := types.T_blob.ToType()
	source := &planpb.Expr{Typ: blobType.PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}
	for _, name := range []string{"left", "right", "substring", "substr", "mid"} {
		for _, tc := range []struct {
			name      string
			length    *planpb.Expr
			wantOID   types.T
			wantWidth int32
		}{
			{"bounded", makePlan2Int64ConstExprWithType(1), types.T_varchar, 2},
			{"aggregate limit", makePlan2Int64ConstExprWithType(511), types.T_varchar, 1022},
			{"above aggregate limit", makePlan2Int64ConstExprWithType(512), types.T_varchar, 1024},
			{"hex result exceeds varchar", makePlan2Int64ConstExprWithType(32768), types.T_text, 0},
			{"unknown", &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}, types.T_text, 0},
		} {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				args := []*planpb.Expr{source, tc.length}
				if name == "substring" || name == "substr" || name == "mid" {
					args = []*planpb.Expr{source, makePlan2Int64ConstExprWithType(1), tc.length}
				}
				slice, err := BindFuncExprImplByPlanExpr(ctx, name, args)
				require.NoError(t, err)
				require.Equal(t, int32(types.T_blob), slice.Typ.Id)
				for _, aggregate := range []string{"bit_and", "bit_or", "bit_xor"} {
					bound, err := BindFuncExprImplByPlanExpr(ctx, aggregate, []*planpb.Expr{slice})
					require.NoError(t, err)
					require.Equal(t, slice.Typ.Id, bound.Typ.Id)
				}
				result, err := BindFuncExprImplByPlanExpr(ctx, "hex", []*planpb.Expr{slice})
				require.NoError(t, err)
				require.Equal(t, int32(tc.wantOID), result.Typ.Id)
				require.Equal(t, tc.wantWidth, result.Typ.Width)
			})
		}
	}

	variable := &planpb.Expr{Typ: blobType.PlanType(), Expr: &planpb.Expr_V{V: &planpb.VarRef{Name: "v"}}}
	slice, err := BindFuncExprImplByPlanExpr(ctx, "left", []*planpb.Expr{variable, makePlan2Int64ConstExprWithType(1)})
	require.NoError(t, err)
	result, err := BindFuncExprImplByPlanExpr(ctx, "hex", []*planpb.Expr{slice})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_text), result.Typ.Id)
}

func TestHexSliceOfConditionalBlobMetadata(t *testing.T) {
	ctx := context.Background()
	column := func(typ types.Type, pos int32) *planpb.Expr {
		return &planpb.Expr{Typ: typ.PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: pos}}}
	}
	condition := column(types.T_bool.ToType(), 0)
	text := column(types.NewWithCharset(types.T_varchar, 16, 0, types.CharsetUTF8), 1)
	binary := column(types.NewWithCharset(types.T_varbinary, 16, 0, types.CharsetBinary), 2)
	coalesced, err := BindFuncExprImplByPlanExpr(ctx, "coalesce", []*planpb.Expr{text, binary})
	require.NoError(t, err)
	for _, selector := range []string{"if", "case"} {
		t.Run(selector, func(t *testing.T) {
			selected, err := BindFuncExprImplByPlanExpr(ctx, selector, []*planpb.Expr{condition, binary, coalesced})
			require.NoError(t, err)
			require.Equal(t, int32(types.T_blob), selected.Typ.Id)
			require.Equal(t, possibleStringDomainText|possibleStringDomainBinary, possibleStringDomainsForExpr(selected))
			for _, name := range []string{"left", "right", "substring"} {
				args := []*planpb.Expr{selected, makePlan2Int64ConstExprWithType(1)}
				if name == "substring" {
					args = append(args, makePlan2Int64ConstExprWithType(1))
				}
				slice, err := BindFuncExprImplByPlanExpr(ctx, name, args)
				require.NoError(t, err)
				require.Equal(t, int32(types.T_blob), slice.Typ.Id)
				result, err := BindFuncExprImplByPlanExpr(ctx, "hex", []*planpb.Expr{slice})
				require.NoError(t, err)
				// The low-level binder has not consumed a SQL declaration boundary:
				// the selected value can still be a multibyte text row.
				require.Equal(t, int32(types.T_text), result.Typ.Id)
				require.Equal(t, int32(0), result.Typ.Width)
			}

			// SQL binding can consume a binary declaration with an explicit
			// representation cast before binding the slice.
			declared, err := appendExplicitCastBeforeExpr(ctx, selected, selected.Typ)
			require.NoError(t, err)
			require.Equal(t, possibleStringDomainBinary, possibleStringDomainsForExpr(declared))
			for _, name := range []string{"left", "right", "substring"} {
				args := []*planpb.Expr{declared, makePlan2Int64ConstExprWithType(1)}
				if name == "substring" {
					args = append(args, makePlan2Int64ConstExprWithType(1))
				}
				slice, err := BindFuncExprImplByPlanExpr(ctx, name, args)
				require.NoError(t, err)
				require.Equal(t, int32(types.T_blob), slice.Typ.Id)
				result, err := BindFuncExprImplByPlanExpr(ctx, "hex", []*planpb.Expr{slice})
				require.NoError(t, err)
				require.Equal(t, int32(types.T_varchar), result.Typ.Id)
				require.Equal(t, int32(2), result.Typ.Width)
			}
		})
	}
}
