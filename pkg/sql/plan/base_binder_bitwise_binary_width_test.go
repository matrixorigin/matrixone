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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
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

func TestRefineCharacterSubstringAndLeftRightKeepDeclaredReturnTypes(t *testing.T) {
	textType := types.New(types.T_varchar, 512, 0)
	source := func() *planpb.Expr {
		return &planpb.Expr{
			Typ:  makePlan2Type(&textType),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}},
		}
	}
	returnType := func() types.Type { return types.New(types.T_varchar, 512, 0) }

	textSubstring := returnType()
	refineSubstringLiteralReturnType(
		[]*planpb.Expr{source(), makePlan2Int64ConstExprWithType(2)},
		&textSubstring,
	)
	require.Equal(t, int32(512), textSubstring.Width,
		"character SUBSTRING keeps the declared result width")

	textSubstring = returnType()
	refineSubstringLiteralReturnType(
		[]*planpb.Expr{source(), makePlan2Int64ConstExprWithType(2), makePlan2Int64ConstExprWithType(7)},
		&textSubstring,
	)
	require.Equal(t, int32(512), textSubstring.Width,
		"character SUBSTRING literal bounds must not change result metadata")

	for _, name := range []string{"left", "right"} {
		result := returnType()
		refineLeftRightLiteralReturnType(
			[]*planpb.Expr{source(), makePlan2Int64ConstExprWithType(7)}, &result)
		require.Equal(t, int32(512), result.Width,
			"character %s literal bounds must not change result metadata", name)
	}

	binaryType := types.NewWithCharset(types.T_varbinary, 512, 0, types.CharsetBinary)
	binarySource := func() *planpb.Expr {
		return &planpb.Expr{
			Typ:  makePlan2Type(&binaryType),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}},
		}
	}
	for _, name := range []string{"left", "right"} {
		result := returnType()
		result.Charset = types.CharsetBinary
		refineLeftRightLiteralReturnType(
			[]*planpb.Expr{binarySource(), makePlan2Int64ConstExprWithType(7)}, &result)
		require.Equal(t, int32(7), result.Width,
			"binary %s may retain literal byte bounds", name)
	}
}

func TestRefineStringSliceRequiresBinaryRuntimeDomain(t *testing.T) {
	ctx := context.Background()
	bind := func(t *testing.T, name string, source *planpb.Expr) *planpb.Expr {
		t.Helper()
		args := []*planpb.Expr{source, makePlan2Int64ConstExprWithType(1)}
		if name == "substring" {
			args = append(args, makePlan2Int64ConstExprWithType(2))
		}
		expr, err := BindFuncExprImplByPlanExpr(ctx, name, args)
		require.NoError(t, err)
		return expr
	}

	binaryLiteral := func(value string) *planpb.Expr {
		return makePlan2StringConstExprWithType(value, true)
	}
	varbinaryType := types.NewWithCharset(types.T_varbinary, 512, 0, types.CharsetBinary)

	for _, name := range []string{"left", "substring"} {
		wantNarrowWidth := int32(1)
		if name == "substring" {
			wantNarrowWidth = 2
		}
		t.Run(name+"/direct binary literal narrows", func(t *testing.T) {
			expr := bind(t, name, binaryLiteral("你好"))
			require.Equal(t, wantNarrowWidth, expr.Typ.Width)
		})

		t.Run(name+"/mixed conditional stays conservative", func(t *testing.T) {
			mixed, err := BindFuncExprImplByPlanExpr(ctx, "if", []*planpb.Expr{
				makePlan2BoolConstExprWithType(false),
				binaryLiteral("x"),
				makePlan2StringConstExprWithType("你好"),
			})
			require.NoError(t, err)
			expr := bind(t, name, mixed)
			require.Greater(t, expr.Typ.Width, int32(2))
		})

		t.Run(name+"/prepared value stays conservative", func(t *testing.T) {
			prepared := &planpb.Expr{
				Typ:  makePlan2Type(&varbinaryType),
				Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
			}
			expr := bind(t, name, prepared)
			require.Equal(t, int32(512), expr.Typ.Width)
		})

		t.Run(name+"/all binary conditional narrows", func(t *testing.T) {
			allBinary, err := BindFuncExprImplByPlanExpr(ctx, "if", []*planpb.Expr{
				makePlan2BoolConstExprWithType(true),
				binaryLiteral("x"),
				makePlan2VarBinaryConstExprWithType("你好"),
			})
			require.NoError(t, err)
			expr := bind(t, name, allBinary)
			require.Equal(t, wantNarrowWidth, expr.Typ.Width)
		})
	}

	t.Run("text literal overrides binary-shaped type", func(t *testing.T) {
		text := makePlan2VarBinaryConstExprWithType("你好")
		text.GetLit().LiteralForm = planpb.StringLiteralForm_STRING_LITERAL_TEXT
		expr := bind(t, "left", text)
		require.Equal(t, int32(6), expr.Typ.Width)
	})

	t.Run("derived mixed-domain column stays conservative", func(t *testing.T) {
		mixed, err := BindFuncExprImplByPlanExpr(ctx, "if", []*planpb.Expr{
			makePlan2BoolConstExprWithType(false),
			binaryLiteral("x"),
			makePlan2StringConstExprWithType("你好"),
		})
		require.NoError(t, err)
		builder := &QueryBuilder{
			qry: &planpb.Query{Nodes: []*planpb.Node{{
				NodeType:    planpb.Node_PROJECT,
				BindingTags: []int32{7},
				ProjectList: []*planpb.Expr{mixed},
			}}},
			tag2NodeID: map[int32]int32{7: 0},
		}
		binder := &baseBinder{builder: builder}
		derived := &planpb.Expr{
			Typ: makePlan2Type(&varbinaryType),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
				RelPos: 7,
				ColPos: 0,
			}},
		}
		binder.annotateStringDomainSources([]*planpb.Expr{derived})
		require.NotNil(t, derived.GetPreparedNumeric().GetStringDomainSource())
		expr := bind(t, "left", derived)
		require.Equal(t, int32(512), expr.Typ.Width)
	})
}

func TestCTASRepeatedDerivedStringDomainReferenceStaysConservative(t *testing.T) {
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, `
		create table repeated_derived_string_domain as
		select left(if(d.value is not null, d.value, _binary 'x'), 1) as sliced
		from (
			select if(n_nationkey > 0, X'ff', n_name) as value
			from nation
		) d`, 1)
	require.NoError(t, err)
	defer stmt.Free()

	plan, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
	require.NoError(t, err)
	var visible []*planpb.ColDef
	for _, col := range plan.GetDdl().GetCreateTable().GetTableDef().GetCols() {
		if !col.Hidden {
			visible = append(visible, col)
		}
	}
	require.Len(t, visible, 1)
	require.Equal(t, int32(types.T_varbinary), visible[0].Typ.Id)
	// The derived value is binary-shaped but can produce text at runtime. The
	// repeated reference in IF's condition and value branch must retain that
	// lineage; narrowing it to VARBINARY(1) would truncate a multibyte text row.
	require.Greater(t, visible[0].Typ.Width, int32(1))
}

func TestRefineCharacterStringReturnTypesUseFormattedNumericBounds(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal64, 5, 2)
	source := &planpb.Expr{
		Typ: makePlan2Type(&decimalType),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: 0,
			ColPos: 0,
		}},
	}

	for _, name := range []string{"substring"} {
		t.Run(name, func(t *testing.T) {
			args := []*planpb.Expr{source, makePlan2Int64ConstExprWithType(1)}
			if name == "substring" {
				args = append(args, makePlan2Int64ConstExprWithType(20))
			} else {
				args[1] = makePlan2Int64ConstExprWithType(20)
			}

			bound, err := BindFuncExprImplByPlanExpr(ctx, name, args)
			require.NoError(t, err)
			require.Equal(t, int32(types.T_varchar), bound.Typ.Id)
			// DECIMAL(5,2) can format -123.45 as seven characters. The
			// refinement must use that cast bound, not precision five.
			require.Equal(t, int32(7), bound.Typ.Width)
		})
	}
}

func TestBindPublicFunctionResultContracts(t *testing.T) {
	ctx := context.Background()
	sourceExpr := func(typ types.Type) *planpb.Expr {
		return &planpb.Expr{
			Typ: makePlan2Type(&typ),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
				RelPos: 0,
				ColPos: 0,
			}},
		}
	}
	length := makePlan2Int64ConstExprWithType(7)

	varbinary := types.NewWithCharset(types.T_varbinary, 128, 0, types.CharsetBinary)
	base64Expr, err := BindFuncExprImplByPlanExpr(ctx, "to_base64", []*planpb.Expr{
		sourceExpr(varbinary),
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_varchar), base64Expr.Typ.Id)
	require.Equal(t, int32(174), base64Expr.Typ.Width)

	varchar := types.New(types.T_varchar, 512, 0)
	for _, name := range []string{"substring", "left", "right"} {
		args := []*planpb.Expr{sourceExpr(varchar), length}
		if name == "substring" {
			args = append(args, length)
		}
		bound, err := BindFuncExprImplByPlanExpr(ctx, name, args)
		require.NoError(t, err)
		require.Equal(t, int32(types.T_varchar), bound.Typ.Id, name)
		require.Equal(t, int32(512), bound.Typ.Width, name)
	}

	inetNtoa, err := BindFuncExprImplByPlanExpr(ctx, "inet_ntoa", []*planpb.Expr{
		sourceExpr(varchar),
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_varchar), inetNtoa.Typ.Id)
	require.Equal(t, int32(31), inetNtoa.Typ.Width)

	isIPv4, err := BindFuncExprImplByPlanExpr(ctx, "is_ipv4", []*planpb.Expr{
		sourceExpr(varchar),
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_int32), isIPv4.Typ.Id)
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
