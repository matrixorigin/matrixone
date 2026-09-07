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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestBindBitwiseAggregateSubstringBinaryWidth(t *testing.T) {
	ctx := context.Background()
	for _, source := range []struct {
		name string
		typ  types.Type
	}{
		{name: "varbinary", typ: types.NewWithCharset(types.T_varbinary, 512, 0, types.CharsetBinary)},
	} {
		t.Run(source.name, func(t *testing.T) {
			for _, test := range []struct {
				name      string
				start     int64
				length    int64
				hasLength bool
				wantWidth int32
				wantError bool
			}{
				{name: "three args bounded", start: 1, length: 511, hasLength: true, wantWidth: 511},
				{name: "three args still oversized", start: 1, length: 512, hasLength: true, wantWidth: 512, wantError: true},
				{name: "two args suffix", start: 2, wantWidth: 511},
				{name: "three args suffix", start: 2, length: 512, hasLength: true, wantWidth: 511},
				{name: "zero start", start: 0, length: 512, hasLength: true, wantWidth: 0},
				{name: "negative start", start: -2, wantWidth: 2},
				{name: "negative start with length", start: -2, length: 512, hasLength: true, wantWidth: 2},
				{name: "negative start before source", start: -513, wantWidth: 0},
				{name: "minimum negative start", start: -1 << 63, wantWidth: 0},
				{name: "two args still oversized", start: 1, wantWidth: 512, wantError: true},
			} {
				t.Run(test.name, func(t *testing.T) {
					sourceExpr := &planpb.Expr{
						Typ: makePlan2Type(&source.typ),
						Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
							RelPos: 0,
							ColPos: 0,
						}},
					}
					args := []*planpb.Expr{sourceExpr, makePlan2Int64ConstExprWithType(test.start)}
					if test.hasLength {
						args = append(args, makePlan2Int64ConstExprWithType(test.length))
					}
					substring, err := BindFuncExprImplByPlanExpr(ctx, "substring", args)
					require.NoError(t, err)
					require.Equal(t, int32(types.T_varbinary), substring.Typ.Id)
					require.Equal(t, test.wantWidth, substring.Typ.Width)

					for _, aggregateName := range []string{"bit_and", "bit_or", "bit_xor"} {
						_, err = BindFuncExprImplByPlanExpr(ctx, aggregateName, []*planpb.Expr{substring})
						if test.wantError {
							require.Error(t, err, "%s must reject SUBSTRING(..., %d)", aggregateName, test.length)
							moErr := moerr.DowncastError(err)
							require.Equal(t, moerr.ErrInvalidBitwiseAggregateOperandsSize, moErr.ErrorCode())
						} else {
							require.NoError(t, err, "%s must accept SUBSTRING(..., %d)", aggregateName, test.length)
						}
					}
				})
			}
		})
	}
}

func TestBinarySubstringBoundHelpers(t *testing.T) {
	signedLiteral := func(value int64) *planpb.Literal {
		return &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: value}}
	}
	unsignedLiteral := func(value uint64) *planpb.Literal {
		return &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: value}}
	}

	for _, test := range []struct {
		name  string
		lit   *planpb.Literal
		bound uint64
		known bool
	}{
		{name: "length nil", lit: nil, known: false},
		{name: "length null", lit: &planpb.Literal{Isnull: true}, known: false},
		{name: "length non-positive", lit: signedLiteral(0), bound: 0, known: true},
		{name: "length signed", lit: signedLiteral(7), bound: 7, known: true},
		{name: "length unsigned", lit: unsignedLiteral(9), bound: 9, known: true},
		{name: "length unsupported", lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "7"}}, known: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, known := binarySubstringLengthBound(test.lit)
			require.Equal(t, test.bound, got)
			require.Equal(t, test.known, known)
		})
	}

	for _, test := range []struct {
		name      string
		lit       *planpb.Literal
		wantBound uint64
		wantKnown bool
	}{
		{name: "start nil", lit: nil, wantKnown: false},
		{name: "start null", lit: &planpb.Literal{Isnull: true}, wantKnown: false},
		{name: "start zero", lit: signedLiteral(0), wantBound: 0, wantKnown: true},
		{name: "start positive in range", lit: signedLiteral(2), wantBound: 511, wantKnown: true},
		{name: "start positive out of range", lit: signedLiteral(513), wantBound: 0, wantKnown: true},
		{name: "start negative in range", lit: signedLiteral(-2), wantBound: 2, wantKnown: true},
		{name: "start negative out of range", lit: signedLiteral(-513), wantBound: 0, wantKnown: true},
		{name: "start minimum int64", lit: signedLiteral(-1 << 63), wantBound: 0, wantKnown: true},
		{name: "start unsigned zero", lit: unsignedLiteral(0), wantBound: 0, wantKnown: true},
		{name: "start unsigned in range", lit: unsignedLiteral(2), wantBound: 511, wantKnown: true},
		{name: "start unsigned out of range", lit: unsignedLiteral(513), wantBound: 0, wantKnown: true},
		{name: "start unsupported", lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "2"}}, wantKnown: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, known := binarySubstringStartBound(512, test.lit)
			require.Equal(t, test.wantBound, got)
			require.Equal(t, test.wantKnown, known)
		})
	}
}

func TestRefineBinarySubstringReturnTypeConservativeCases(t *testing.T) {
	varbinaryType := types.NewWithCharset(types.T_varbinary, 512, 0, types.CharsetBinary)
	sourceExpr := func(typ types.Type) *planpb.Expr {
		return &planpb.Expr{
			Typ: makePlan2Type(&typ),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
				RelPos: 0,
				ColPos: 0,
			}},
		}
	}
	newReturnType := func() types.Type {
		return types.NewWithCharset(types.T_varbinary, 512, 0, types.CharsetBinary)
	}

	returnType := newReturnType()
	refineSubstringLiteralReturnType(nil, &returnType)
	require.Equal(t, int32(512), returnType.Width)

	returnType = newReturnType()
	refineSubstringLiteralReturnType([]*planpb.Expr{
		sourceExpr(varbinaryType),
		makePlan2Int64ConstExprWithType(1),
		makePlan2Int64ConstExprWithType(0),
	}, &returnType)
	require.Equal(t, int32(0), returnType.Width)

	unboundedBinary := types.NewWithCharset(types.T_varbinary, 0, 0, types.CharsetBinary)
	returnType = newReturnType()
	refineSubstringLiteralReturnType([]*planpb.Expr{
		sourceExpr(unboundedBinary),
		makePlan2Int64ConstExprWithType(1),
	}, &returnType)
	require.Equal(t, int32(512), returnType.Width)

	startExpr := sourceExpr(types.T_int64.ToType())
	returnType = newReturnType()
	refineSubstringLiteralReturnType([]*planpb.Expr{
		sourceExpr(varbinaryType),
		startExpr,
	}, &returnType)
	require.Equal(t, int32(512), returnType.Width)

	returnType = newReturnType()
	refineSubstringLiteralReturnType([]*planpb.Expr{
		sourceExpr(varbinaryType),
		startExpr,
		makePlan2Int64ConstExprWithType(511),
	}, &returnType)
	require.Equal(t, int32(511), returnType.Width)

	returnType = newReturnType()
	refineSubstringLiteralReturnType([]*planpb.Expr{
		sourceExpr(unboundedBinary),
		startExpr,
		makePlan2Int64ConstExprWithType(511),
	}, &returnType)
	require.Equal(t, int32(511), returnType.Width)
}

func TestBindBitwiseAggregateLeavesBlobSubstringInTextDomain(t *testing.T) {
	ctx := context.Background()
	sourceType := types.T_blob.ToType()
	sourceExpr := &planpb.Expr{
		Typ: makePlan2Type(&sourceType),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: 0,
			ColPos: 0,
		}},
	}

	substring, err := BindFuncExprImplByPlanExpr(ctx, "substring", []*planpb.Expr{
		sourceExpr,
		makePlan2Int64ConstExprWithType(1),
		makePlan2Int64ConstExprWithType(511),
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_blob), substring.Typ.Id)

	for _, aggregateName := range []string{"bit_and", "bit_or", "bit_xor"} {
		_, err = BindFuncExprImplByPlanExpr(ctx, aggregateName, []*planpb.Expr{substring})
		require.Error(t, err, "%s must keep BLOB SUBSTRING outside binary aggregate support", aggregateName)
	}
}
