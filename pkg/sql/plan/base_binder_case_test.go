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
	"fmt"
	"math"
	"strings"
	"testing"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestPreparedNumericFallbackMetadataSurvivesProtoRoundTrip(t *testing.T) {
	original := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_float64)},
		PreparedNumeric: &planpb.PreparedNumericMetadata{
			Fallback:             true,
			ParamPos:             0,
			FallbackSource:       true,
			FallbackSourceNodeId: 7,
			FallbackSourceColPos: 2,
			StringDomainSource: &planpb.Expr{
				Typ:  planpb.Type{Id: int32(types.T_varchar)},
				Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 1}},
			},
		},
	}
	payload, err := proto.Marshal(original)
	require.NoError(t, err)

	var restored planpb.Expr
	require.NoError(t, proto.Unmarshal(payload, &restored))
	metadata := restored.GetPreparedNumeric()
	require.NotNil(t, metadata)
	require.True(t, metadata.GetFallback())
	require.Equal(t, int32(0), metadata.GetParamPos())
	require.True(t, metadata.GetFallbackSource())
	require.Equal(t, int32(7), metadata.GetFallbackSourceNodeId())
	require.Equal(t, int32(2), metadata.GetFallbackSourceColPos())
	require.Equal(t, int32(1), metadata.GetStringDomainSource().GetP().GetPos())
	require.Zero(t, restored.AuxId,
		"prepared numeric provenance must not be encoded as an executor memo id")
}

func TestTemporalBindingUsesPrivatePreparedProvenance(t *testing.T) {
	ctx := context.Background()
	value := makePlan2StringConstExprWithType("2024-02-29 12:34:56.123456")
	formatParam := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}

	for _, name := range []string{"str_to_date", "to_date"} {
		t.Run(name+" dynamic", func(t *testing.T) {
			bound, err := BindFuncExprImplByPlanExpr(ctx, name, []*planpb.Expr{DeepCopyExpr(value), DeepCopyExpr(formatParam)})
			require.NoError(t, err)
			require.Equal(t, int32(types.T_datetime), bound.Typ.Id)
			require.Equal(t, int32(6), bound.Typ.Scale)
			require.Len(t, bound.GetF().Args, 3)
			_, overload := function.DecodeOverloadID(bound.GetF().Func.Obj)
			require.Equal(t, int32(0), overload)
			original := DeepCopyExpr(bound)

			for _, format := range []string{"%Y-%m-%d", "%H:%i:%s", "%Y-%m-%d %H:%i:%s.%f"} {
				rebound, err := bindPreparedFuncExprImplByPlanExpr(
					ctx,
					bound,
					name,
					[]*planpb.Expr{DeepCopyExpr(value), makePlan2StringConstExprWithType(format)},
					nil,
				)
				require.NoError(t, err)
				require.Equal(t, int32(types.T_datetime), rebound.Typ.Id)
				require.Equal(t, int32(6), rebound.Typ.Scale)
				require.Len(t, rebound.GetF().Args, 3)
				_, reboundOverload := function.DecodeOverloadID(rebound.GetF().Func.Obj)
				require.Equal(t, int32(0), reboundOverload)
			}

			rule := NewResetParamRefRule(ctx, []*planpb.Expr{makePlan2StringConstExprWithType("%Y-%m-%d")})
			rewritten, err := rule.ApplyExpr(DeepCopyExpr(bound))
			require.NoError(t, err)
			require.Equal(t, int32(types.T_datetime), rewritten.Typ.Id)
			require.Equal(t, int32(6), rewritten.Typ.Scale)
			require.Len(t, rewritten.GetF().Args, 3)
			_, reboundOverload := function.DecodeOverloadID(rewritten.GetF().Func.Obj)
			require.Equal(t, int32(0), reboundOverload)
			require.True(t, proto.Equal(original, bound), "prepared rebinding must not mutate the cached bound expression")
		})

		for _, literal := range []struct {
			format       string
			wantType     types.T
			wantScale    int32
			wantOverload int32
		}{
			{format: "%Y-%m-%d", wantType: types.T_date, wantOverload: 1},
			{format: "%H:%i:%s.%f", wantType: types.T_time, wantScale: 6, wantOverload: 2},
			{format: "%Y-%m-%d %H:%i:%s.%f", wantType: types.T_datetime, wantScale: 6, wantOverload: 0},
		} {
			t.Run(name+" literal "+literal.format, func(t *testing.T) {
				bound, err := BindFuncExprImplByPlanExpr(ctx, name, []*planpb.Expr{DeepCopyExpr(value), makePlan2StringConstExprWithType(literal.format)})
				require.NoError(t, err)
				require.Equal(t, int32(literal.wantType), bound.Typ.Id)
				require.Equal(t, literal.wantScale, bound.Typ.Scale)
				require.Len(t, bound.GetF().Args, 3)
				_, overload := function.DecodeOverloadID(bound.GetF().Func.Obj)
				require.Equal(t, literal.wantOverload, overload)
			})
		}

		t.Run(name+" public internal shape rejected", func(t *testing.T) {
			_, err := BindFuncExprImplByPlanExpr(ctx, name, []*planpb.Expr{
				DeepCopyExpr(value),
				makePlan2StringConstExprWithType("%Y-%m-%d"),
				makePlan2DateConstNullExpr(types.T_date),
			})
			require.Error(t, err)
		})
	}

	date := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_datetime), Scale: 3}}
	for _, name := range []string{"date_add", "date_sub"} {
		t.Run(name+" public internal shape rejected", func(t *testing.T) {
			_, err := BindFuncExprImplByPlanExpr(ctx, name, []*planpb.Expr{
				DeepCopyExpr(date), makePlan2Int64ConstExprWithType(1), makePlan2Int64ConstExprWithType(int64(types.MicroSecond)),
			})
			require.Error(t, err)
		})
	}

	for _, alias := range []string{"adddate", "subdate"} {
		t.Run(alias+" normalizes once", func(t *testing.T) {
			bound, err := BindFuncExprImplByPlanExpr(ctx, alias, []*planpb.Expr{DeepCopyExpr(date), makePlan2Int64ConstExprWithType(1)})
			require.NoError(t, err)
			require.Len(t, bound.GetF().Args, 3)
			require.Equal(t, int32(3), bound.Typ.Scale)
		})
	}

	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_temporal from 'select str_to_date(?, ?)'")
	require.NoError(t, err)
	preparedPlan := prepared.GetDcl().GetPrepare().Plan
	preparedExpr := findPlanFunctionExpr(preparedPlan, "str_to_date")
	require.NotNil(t, preparedExpr)
	require.Equal(t, int32(types.T_datetime), preparedExpr.Typ.Id)
	require.Equal(t, int32(6), preparedExpr.Typ.Scale)
	require.Len(t, preparedExpr.GetF().Args, 3)
	preparedPlanCopy := DeepCopyPlan(preparedPlan)

	for _, execution := range []struct {
		name   string
		params []any
	}{
		{
			name: "SQL execute",
			params: []any{
				ParamValue{Value: "2024-02-29", SourceType: types.T_varchar.ToType(), HasSourceType: true},
				ParamValue{Value: "%Y-%m-%d", SourceType: types.T_varchar.ToType(), HasSourceType: true},
			},
		},
		{
			name: "binary execute",
			params: []any{
				ParamValue{Value: "2024-02-29 12:34:56.123456", IsBinaryProtocol: true},
				ParamValue{Value: "%Y-%m-%d %H:%i:%s.%f", IsBinaryProtocol: true},
			},
		},
	} {
		t.Run(execution.name, func(t *testing.T) {
			filled, err := FillValuesOfParamsInPlan(ctx, preparedPlan, execution.params)
			require.NoError(t, err)
			filledExpr := findPlanFunctionExpr(filled, "str_to_date")
			require.NotNil(t, filledExpr)
			require.Equal(t, int32(types.T_datetime), filledExpr.Typ.Id)
			require.Equal(t, int32(6), filledExpr.Typ.Scale)
			require.Len(t, filledExpr.GetF().Args, 3)
			_, overload := function.DecodeOverloadID(filledExpr.GetF().Func.Obj)
			require.Equal(t, int32(0), overload)
			require.True(t, proto.Equal(preparedPlanCopy, preparedPlan),
				"parameter filling must not mutate the cached prepared plan")
		})
	}
}

func TestPreparedBitCountDefaultsToBinaryAndSpecializesNumericValues(t *testing.T) {
	ctx := context.Background()
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_bit_count from 'select bit_count(?)'")
	require.NoError(t, err)
	preparePlan := prepared.GetDcl().GetPrepare().Plan
	require.Equal(t, []int32{0}, PreparedPlanBitCountFallbackParamPositions(preparePlan))
	require.False(t, PreparedPlanNeedsRuntimeSpecialization(preparePlan),
		"BIT_COUNT uses its cached marker-position trigger instead of a per-execute plan scan")
	fn := findPlanFunctionExpr(preparePlan, "bit_count")
	require.NotNil(t, fn)
	_, overload := function.DecodeOverloadID(fn.GetF().GetFunc().GetObj())
	require.Equal(t, int32(14), overload)
	require.Equal(t, int32(types.T_varbinary), fn.GetF().Args[0].Typ.Id)

	numericPlan, specialized, err := FillValuesOfParamsInPlanWithSpecialization(ctx, preparePlan, []any{
		ParamValue{Value: "64", PrepareParamKind: vector.PrepareParamInteger},
	})
	require.NoError(t, err)
	require.True(t, specialized)
	fn = findPlanFunctionExpr(numericPlan, "bit_count")
	require.NotNil(t, fn)
	_, overload = function.DecodeOverloadID(fn.GetF().GetFunc().GetObj())
	require.Equal(t, int32(7), overload)
	require.Equal(t, int32(types.T_int64), fn.GetF().Args[0].Typ.Id)

	stringPlan, specialized, err := FillValuesOfParamsInPlanWithSpecialization(ctx, preparePlan, []any{
		ParamValue{Value: "64", IsBinaryProtocol: true, PrepareParamKind: vector.PrepareParamNone},
	})
	require.NoError(t, err)
	fn = findPlanFunctionExpr(stringPlan, "bit_count")
	require.NotNil(t, fn)
	_, overload = function.DecodeOverloadID(fn.GetF().GetFunc().GetObj())
	require.Equal(t, int32(14), overload)
	require.False(t, specialized)

	explicitCast, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_bit_count_cast from 'select bit_count(cast(? as char))'")
	require.NoError(t, err)
	explicitCastPlan := explicitCast.GetDcl().GetPrepare().Plan
	require.Empty(t, PreparedPlanBitCountFallbackParamPositions(explicitCastPlan),
		"an explicit user cast owns the marker domain and must not be rebound")
	fn = findPlanFunctionExpr(explicitCastPlan, "bit_count")
	require.NotNil(t, fn)
	_, overload = function.DecodeOverloadID(fn.GetF().GetFunc().GetObj())
	require.Equal(t, int32(13), overload)
}

func TestPreparedRegexpResultDomainTransferUsesOnlyMatchOperands(t *testing.T) {
	textType := types.T_text.ToType()
	binaryType := types.T_varbinary.ToType()
	expr := func(typ types.Type) *planpb.Expr {
		return &planpb.Expr{Typ: makePlan2Type(&typ)}
	}

	tests := []struct {
		name           string
		function       string
		args           []*planpb.Expr
		dynamicArgs    []int
		preparedDomain types.StringDomain
		wantDepends    bool
	}{
		{
			name:        "replace replacement alone never owns result",
			function:    "regexp_replace",
			args:        []*planpb.Expr{expr(textType), expr(textType), expr(textType)},
			dynamicArgs: []int{2}, preparedDomain: types.StringDomainText,
		},
		{
			name:        "replace subject owns result",
			function:    "regexp_replace",
			args:        []*planpb.Expr{expr(textType), expr(textType), expr(textType)},
			dynamicArgs: []int{0}, preparedDomain: types.StringDomainText,
			wantDepends: true,
		},
		{
			name:        "replace pattern owns result",
			function:    "regexp_replace",
			args:        []*planpb.Expr{expr(textType), expr(textType), expr(textType)},
			dynamicArgs: []int{1}, preparedDomain: types.StringDomainText,
			wantDepends: true,
		},
		{
			name:        "fixed binary match pair dominates dynamic replacement",
			function:    "regexp_replace",
			args:        []*planpb.Expr{expr(binaryType), expr(binaryType), expr(textType)},
			dynamicArgs: []int{2}, preparedDomain: types.StringDomainBinary,
		},
		{
			name:        "substr correlated match operands own result",
			function:    "regexp_substr",
			args:        []*planpb.Expr{expr(textType), expr(textType)},
			dynamicArgs: []int{0, 1}, preparedDomain: types.StringDomainText,
			wantDepends: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.wantDepends,
				preparedRegexpResultDomainDependsOnDynamicOperands(
					test.function, test.args, test.dynamicArgs, test.preparedDomain))
		})
	}

	require.Equal(t, function.RegexpReplaceCompatibilityStringOperandCount,
		preparedRegexpCompatibilityStringOperandCount("regexp_replace", 5))
	require.Equal(t, function.RegexpMatchStringOperandCount,
		preparedRegexpResultStringOperandCount("regexp_replace", 5))
}

func TestPreparedRegexpScalarSubqueryPropagatesRuntimeDomain(t *testing.T) {
	ctx := context.Background()
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_regexp_scalar_domain from 'select regexp_instr("+
			"(select ? from nation limit 1), (select ? from nation limit 1), 2)'")
	require.NoError(t, err)
	preparedPlan := prepared.GetDcl().GetPrepare().Plan
	cached := proto.Clone(preparedPlan).(*planpb.Plan)
	preparedRegexp := findPlanFunctionExpr(preparedPlan, "regexp_instr")
	require.NotNil(t, preparedRegexp)
	require.Equal(t, int32(0), preparedRegexp.GetF().Args[0].GetPreparedNumeric().
		GetStringDomainSource().GetP().GetPos())
	require.Equal(t, int32(1), preparedRegexp.GetF().Args[1].GetPreparedNumeric().
		GetStringDomainSource().GetP().GetPos())

	varbinary := types.T_varbinary.ToType()
	binaryPlan, _, err := FillValuesOfParamsInPlanWithSpecialization(ctx, preparedPlan, []any{
		ParamValue{Value: "中中", IsBin: true, IsBinaryProtocol: true,
			RuntimeType: varbinary, HasRuntimeType: true},
		ParamValue{Value: "中", IsBin: true, IsBinaryProtocol: true,
			RuntimeType: varbinary, HasRuntimeType: true},
	})
	require.NoError(t, err)
	regexpInstr := findPlanFunctionExpr(binaryPlan, "regexp_instr")
	require.NotNil(t, regexpInstr)
	require.Equal(t, int32(types.T_varbinary), regexpInstr.GetF().Args[0].Typ.Id)
	require.Equal(t, int32(types.T_varbinary), regexpInstr.GetF().Args[1].Typ.Id)

	_, _, err = FillValuesOfParamsInPlanWithSpecialization(ctx, preparedPlan, []any{
		ParamValue{Value: "中中", IsBin: true, IsBinaryProtocol: true,
			RuntimeType: varbinary, HasRuntimeType: true},
		ParamValue{Value: "中", IsBinaryProtocol: true,
			RuntimeType: types.T_text.ToType(), HasRuntimeType: true},
	})
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrCharacterSetMismatch))

	require.True(t, proto.Equal(cached, preparedPlan),
		"execute-time scalar lineage must not mutate the cached plan")

	// An explicit cast is a semantic domain boundary. Runtime binary provenance
	// below it must not escape through scalar-subquery flattening.
	explicit, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_regexp_scalar_cast from 'select regexp_instr("+
			"(select cast(? as varchar) from nation limit 1), ''中'', 2)'")
	require.NoError(t, err)
	explicitPlan, _, err := FillValuesOfParamsInPlanWithSpecialization(
		ctx, explicit.GetDcl().GetPrepare().Plan, []any{
			ParamValue{Value: "中中", IsBin: true, IsBinaryProtocol: true,
				RuntimeType: varbinary, HasRuntimeType: true},
		})
	require.NoError(t, err)
	explicitRegexp := findPlanFunctionExpr(explicitPlan, "regexp_instr")
	require.NotNil(t, explicitRegexp)
	require.Equal(t, int32(types.T_varchar), explicitRegexp.GetF().Args[0].Typ.Id)
}

func TestPreparedRegexpScalarSubqueryPropagatesDerivedColumnRuntimeDomain(t *testing.T) {
	ctx := context.Background()
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_regexp_derived_scalar_domain from 'select regexp_instr("+
			"(select d.subject from (select ? as subject) d), "+
			"(select d.pattern from (select ? as pattern) d), 2)'")
	require.NoError(t, err)
	preparedPlan := prepared.GetDcl().GetPrepare().Plan
	cached := proto.Clone(preparedPlan).(*planpb.Plan)

	varbinary := types.T_varbinary.ToType()
	binaryPlan, _, err := FillValuesOfParamsInPlanWithSpecialization(ctx, preparedPlan, []any{
		ParamValue{Value: "中中", IsBin: true, IsBinaryProtocol: true,
			RuntimeType: varbinary, HasRuntimeType: true},
		ParamValue{Value: "中", IsBin: true, IsBinaryProtocol: true,
			RuntimeType: varbinary, HasRuntimeType: true},
	})
	require.NoError(t, err)
	regexpInstr := findPlanFunctionExpr(binaryPlan, "regexp_instr")
	require.NotNil(t, regexpInstr)
	require.Equal(t, int32(types.T_varbinary), regexpInstr.GetF().Args[0].Typ.Id)
	require.Equal(t, int32(types.T_varbinary), regexpInstr.GetF().Args[1].Typ.Id)

	_, _, err = FillValuesOfParamsInPlanWithSpecialization(ctx, preparedPlan, []any{
		ParamValue{Value: "中中", IsBin: true, IsBinaryProtocol: true,
			RuntimeType: varbinary, HasRuntimeType: true},
		ParamValue{Value: "中", IsBinaryProtocol: true,
			RuntimeType: types.T_text.ToType(), HasRuntimeType: true},
	})
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrCharacterSetMismatch))

	textPlan, _, err := FillValuesOfParamsInPlanWithSpecialization(ctx, preparedPlan, []any{
		ParamValue{Value: "中中", IsBinaryProtocol: true,
			RuntimeType: types.T_text.ToType(), HasRuntimeType: true},
		ParamValue{Value: "中", IsBinaryProtocol: true,
			RuntimeType: types.T_text.ToType(), HasRuntimeType: true},
	})
	require.NoError(t, err)
	regexpInstr = findPlanFunctionExpr(textPlan, "regexp_instr")
	require.NotNil(t, regexpInstr)
	require.Equal(t, int32(types.T_text), regexpInstr.GetF().Args[0].Typ.Id)
	require.Equal(t, int32(types.T_text), regexpInstr.GetF().Args[1].Typ.Id)

	binaryPlan, _, err = FillValuesOfParamsInPlanWithSpecialization(ctx, preparedPlan, []any{
		ParamValue{Value: "中中", IsBin: true, IsBinaryProtocol: true,
			RuntimeType: varbinary, HasRuntimeType: true},
		ParamValue{Value: "中", IsBin: true, IsBinaryProtocol: true,
			RuntimeType: varbinary, HasRuntimeType: true},
	})
	require.NoError(t, err)
	regexpInstr = findPlanFunctionExpr(binaryPlan, "regexp_instr")
	require.NotNil(t, regexpInstr)
	require.Equal(t, int32(types.T_varbinary), regexpInstr.GetF().Args[0].Typ.Id)
	require.Equal(t, int32(types.T_varbinary), regexpInstr.GetF().Args[1].Typ.Id)
	require.True(t, proto.Equal(cached, preparedPlan),
		"execute-time derived-column lineage must not mutate the cached plan")

	explicit, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_regexp_derived_scalar_cast from 'select regexp_instr("+
			"(select d.subject from (select cast(? as varchar) as subject) d), "+
			"''中'', 2)'")
	require.NoError(t, err)
	explicitPlan, _, err := FillValuesOfParamsInPlanWithSpecialization(
		ctx, explicit.GetDcl().GetPrepare().Plan, []any{
			ParamValue{Value: "中中", IsBin: true, IsBinaryProtocol: true,
				RuntimeType: varbinary, HasRuntimeType: true},
		})
	require.NoError(t, err)
	regexpInstr = findPlanFunctionExpr(explicitPlan, "regexp_instr")
	require.NotNil(t, regexpInstr)
	require.Equal(t, int32(types.T_varchar), regexpInstr.GetF().Args[0].Typ.Id,
		"an explicit cast below a derived projection must remain authoritative")

	cte, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_regexp_cte_scalar_domain from 'with d as "+
			"(select ? as subject, ? as pattern) "+
			"select regexp_instr((select subject from d), (select pattern from d), 2)'")
	require.NoError(t, err)
	ctePlan := cte.GetDcl().GetPrepare().Plan
	binaryPlan, _, err = FillValuesOfParamsInPlanWithSpecialization(ctx, ctePlan, []any{
		ParamValue{Value: "中中", IsBin: true, IsBinaryProtocol: true,
			RuntimeType: varbinary, HasRuntimeType: true},
		ParamValue{Value: "中", IsBin: true, IsBinaryProtocol: true,
			RuntimeType: varbinary, HasRuntimeType: true},
	})
	require.NoError(t, err)
	regexpInstr = findPlanFunctionExpr(binaryPlan, "regexp_instr")
	require.NotNil(t, regexpInstr)
	require.Equal(t, int32(types.T_varbinary), regexpInstr.GetF().Args[0].Typ.Id)
	require.Equal(t, int32(types.T_varbinary), regexpInstr.GetF().Args[1].Typ.Id)
}

func TestPreparedRegexpDerivedScalarPreservesResultBranchParams(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_regexp_multi_derived_domain from 'select regexp_instr("+
			"(select d.subject from (select if(?, ?, ?) as subject) d), "+
			"?, 2)'")
	require.NoError(t, err)
	preparedRegexp := findPlanFunctionExpr(
		prepared.GetDcl().GetPrepare().Plan, "regexp_instr")
	require.NotNil(t, preparedRegexp)
	witness := preparedRegexp.GetF().Args[0].GetPreparedNumeric().GetStringDomainSource()
	require.NotNil(t, witness)
	require.Equal(t, "coalesce", witness.GetF().GetFunc().GetObjName())
	require.Equal(t, []int32{1, 2}, []int32{
		witness.GetF().Args[0].GetP().Pos,
		witness.GetF().Args[1].GetP().Pos,
	})

	ctx := context.Background()
	preparedPlan := prepared.GetDcl().GetPrepare().Plan
	cached := proto.Clone(preparedPlan).(*planpb.Plan)
	textPlan, _, err := FillValuesOfParamsInPlanWithSpecialization(ctx, preparedPlan, []any{
		ParamValue{Value: true, RuntimeType: types.T_bool.ToType(), HasRuntimeType: true},
		ParamValue{Value: int64(7), RuntimeType: types.T_int64.ToType(), HasRuntimeType: true},
		ParamValue{Value: int64(8), RuntimeType: types.T_int64.ToType(), HasRuntimeType: true},
		ParamValue{Value: "x", IsBinaryProtocol: true, RuntimeType: types.T_text.ToType(), HasRuntimeType: true},
	})
	require.NoError(t, err)
	regexpInstr := findPlanFunctionExpr(textPlan, "regexp_instr")
	require.NotNil(t, regexpInstr)
	require.Equal(t, types.StringDomainText,
		types.StaticStringDomain(makeTypeByPlan2Expr(regexpInstr.GetF().Args[0])))

	binaryPlan, _, err := FillValuesOfParamsInPlanWithSpecialization(ctx, preparedPlan, []any{
		ParamValue{Value: true, RuntimeType: types.T_bool.ToType(), HasRuntimeType: true},
		ParamValue{Value: "7", IsBin: true, IsBinaryProtocol: true,
			RuntimeType: types.T_varbinary.ToType(), HasRuntimeType: true},
		ParamValue{Value: "8", IsBin: true, IsBinaryProtocol: true,
			RuntimeType: types.T_varbinary.ToType(), HasRuntimeType: true},
		ParamValue{Value: "x", IsBin: true, IsBinaryProtocol: true,
			RuntimeType: types.T_varbinary.ToType(), HasRuntimeType: true},
	})
	require.NoError(t, err)
	regexpInstr = findPlanFunctionExpr(binaryPlan, "regexp_instr")
	require.NotNil(t, regexpInstr)
	require.Equal(t, int32(types.T_varbinary), regexpInstr.GetF().Args[0].Typ.Id)
	require.True(t, proto.Equal(cached, preparedPlan),
		"multi-branch runtime-domain specialization must not mutate the cached plan")
}

func TestStringDomainWitnessKeepsImplicitTextConversion(t *testing.T) {
	textType := types.T_text.ToType()
	param := &planpb.Expr{
		Typ:  makePlan2Type(&textType),
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	source := &planpb.Expr{
		Typ: makePlan2Type(&textType),
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: "substring"},
			Args: []*planpb.Expr{param, makePlan2Int64ConstExprWithType(1)},
		}},
	}
	domains := possibleStringDomainsForExpr(source)
	require.Equal(t, possibleStringDomainText|possibleStringDomainBinary, domains)
	witness := stringDomainSourceWitness(source, domains)
	require.Equal(t, "substring", witness.GetF().GetFunc().GetObjName())

	rule := NewResetParamRefRule(context.Background(), nil)
	rule.SetParamValues([]any{ParamValue{
		Value:          int64(7),
		RuntimeType:    types.T_int64.ToType(),
		HasRuntimeType: true,
	}})
	got, dynamic, domainless, err := rule.preparedExecutionExprType(witness)
	require.NoError(t, err)
	require.True(t, dynamic)
	require.False(t, domainless)
	require.Equal(t, types.StringDomainText, types.StaticStringDomain(got))

	rule.SetParamValues([]any{ParamValue{
		Value:            "7",
		IsBin:            true,
		IsBinaryProtocol: true,
		RuntimeType:      types.T_varbinary.ToType(),
		HasRuntimeType:   true,
	}})
	got, dynamic, domainless, err = rule.preparedExecutionExprType(witness)
	require.NoError(t, err)
	require.True(t, dynamic)
	require.False(t, domainless)
	require.Equal(t, types.StringDomainBinary, types.StaticStringDomain(got))

	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_regexp_substring_domain from 'select regexp_instr("+
			"substring(?, 1), ?, 2)'")
	require.NoError(t, err)
	preparedPlan := prepared.GetDcl().GetPrepare().Plan
	cached := proto.Clone(preparedPlan).(*planpb.Plan)

	textPlan, _, err := FillValuesOfParamsInPlanWithSpecialization(
		context.Background(), preparedPlan, []any{
			ParamValue{Value: int64(7), RuntimeType: types.T_int64.ToType(), HasRuntimeType: true},
			ParamValue{Value: "x", IsBinaryProtocol: true, RuntimeType: types.T_text.ToType(), HasRuntimeType: true},
		})
	require.NoError(t, err)
	regexpInstr := findPlanFunctionExpr(textPlan, "regexp_instr")
	require.NotNil(t, regexpInstr)
	require.Equal(t, types.StringDomainText,
		types.StaticStringDomain(makeTypeByPlan2Expr(regexpInstr.GetF().Args[0])))

	binaryPlan, _, err := FillValuesOfParamsInPlanWithSpecialization(
		context.Background(), preparedPlan, []any{
			ParamValue{Value: "7", IsBin: true, IsBinaryProtocol: true,
				RuntimeType: types.T_varbinary.ToType(), HasRuntimeType: true},
			ParamValue{Value: "x", IsBin: true, IsBinaryProtocol: true,
				RuntimeType: types.T_varbinary.ToType(), HasRuntimeType: true},
		})
	require.NoError(t, err)
	regexpInstr = findPlanFunctionExpr(binaryPlan, "regexp_instr")
	require.NotNil(t, regexpInstr)
	require.Equal(t, int32(types.T_varbinary), regexpInstr.GetF().Args[0].Typ.Id)
	require.True(t, proto.Equal(cached, preparedPlan),
		"substring runtime-domain specialization must not mutate the cached plan")
}

func TestPreparedNumericMetadataIsSparse(t *testing.T) {
	require.Nil(t, (&planpb.Expr{}).GetPreparedNumeric())
	// Five resident scalar fields made Expr 184 bytes. One optional pointer
	// keeps ordinary expressions at a bounded 168 bytes on 64-bit targets.
	require.Equal(t, uintptr(168), unsafe.Sizeof(planpb.Expr{}))
}

var benchmarkPreparedNumericDeepCopySink *planpb.Expr

func BenchmarkDeepCopyExprPreparedNumericMetadata(b *testing.B) {
	ordinary := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_float64)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: "abs"},
			Args: []*planpb.Expr{{
				Typ:  planpb.Type{Id: int32(types.T_float64)},
				Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
			}},
		}},
	}
	prepared := DeepCopyExpr(ordinary)
	prepared.PreparedNumeric = &planpb.PreparedNumericMetadata{Fallback: true, ParamPos: 0}

	for _, test := range []struct {
		name string
		expr *planpb.Expr
	}{
		{name: "ordinary", expr: ordinary},
		{name: "prepared-metadata", expr: prepared},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				benchmarkPreparedNumericDeepCopySink = DeepCopyExpr(test.expr)
			}
		})
	}
}

func TestPreparedScalarNumericOverloadsUseDoubleDomain(t *testing.T) {
	for _, name := range []string{"abs", "sleep"} {
		t.Run(name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(false), t,
				fmt.Sprintf("prepare stmt_%s from 'select %s(?)'", name, name))
			require.NoError(t, err)

			fn := findPlanFunctionExpr(p.GetDcl().GetPrepare().Plan, name)
			require.NotNil(t, fn)
			require.Len(t, fn.GetF().Args, 1)
			require.Equal(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id)
			require.Zero(t, fn.GetF().Args[0].AuxId,
				"deferred overload metadata must not consume executor AuxId space")
			cast := fn.GetF().Args[0].GetF()
			require.NotNil(t, cast)
			require.Equal(t, "cast", cast.Func.GetObjName())
			require.NotNil(t, cast.Args[0].GetP())
			require.Equal(t, int32(types.T_float64), cast.Args[1].Typ.Id)
		})
	}

	t.Run("ordinary column keeps native overload", func(t *testing.T) {
		p, err := runOneStmt(NewMockOptimizer(false), t,
			"prepare stmt_abs_column from 'select abs(n_regionkey) from nation'")
		require.NoError(t, err)

		fn := findPlanFunctionExpr(p.GetDcl().GetPrepare().Plan, "abs")
		require.NotNil(t, fn)
		require.NotEqual(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id)
	})

	t.Run("parameter nested in arithmetic", func(t *testing.T) {
		p, err := runOneStmt(NewMockOptimizer(false), t,
			"prepare stmt_abs_expr from 'select abs(? + 0)'")
		require.NoError(t, err)

		fn := findPlanFunctionExpr(p.GetDcl().GetPrepare().Plan, "abs")
		require.NotNil(t, fn)
		require.Equal(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id)
	})

	t.Run("scalar literal keeps native overload", func(t *testing.T) {
		p, err := runOneStmt(NewMockOptimizer(false), t,
			"prepare stmt_abs_scalar_literal from 'select abs((select n_regionkey from nation where n_regionkey = 0))'")
		require.NoError(t, err)

		fn := findPlanFunctionExpr(p.GetDcl().GetPrepare().Plan, "abs")
		require.NotNil(t, fn)
		require.NotEqual(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id)
	})
}

func TestContainsPreparedParamExprVariants(t *testing.T) {
	param := tree.NewParamExpr(0)
	literal := tree.NewNumVal(int64(1), "1", false, tree.P_int64)
	binder := &baseBinder{sysCtx: context.Background()}
	hasParam := func(expr tree.Expr) bool {
		found, err := binder.hasPreparedNumericParamExprs([]tree.Expr{expr}, 0)
		require.NoError(t, err)
		return found
	}
	function := func(name string, args ...tree.Expr) tree.Expr {
		return &tree.FuncExpr{
			Func:  tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName(name)),
			Exprs: args,
		}
	}

	tests := []struct {
		name string
		expr tree.Expr
		want bool
	}{
		{name: "parameter", expr: param, want: true},
		{name: "binary", expr: tree.NewBinaryExpr(tree.PLUS, literal, param), want: true},
		{name: "unary", expr: tree.NewUnaryExpr(tree.UNARY_MINUS, param), want: true},
		{name: "parenthesized", expr: tree.NewParentExpr(param), want: true},
		{name: "function", expr: function("abs", param), want: true},
		{name: "cast", expr: tree.NewCastExpr(param, tree.TYPE_DOUBLE), want: true},
		{name: "bit cast", expr: tree.NewBitCastExpr(param, tree.TYPE_LONG), want: true},
		{name: "if condition", expr: function("if", param, literal, literal), want: false},
		{name: "if result", expr: function("if", literal, param, literal), want: true},
		{name: "case operand", expr: tree.NewCaseExpr(param, nil, literal), want: false},
		{name: "case when condition", expr: tree.NewCaseExpr(nil, []*tree.When{tree.NewWhen(param, literal)}, nil), want: false},
		{name: "case when value", expr: tree.NewCaseExpr(nil, []*tree.When{tree.NewWhen(literal, param)}, nil), want: true},
		{name: "case else", expr: tree.NewCaseExpr(nil, nil, param), want: true},
		{name: "tuple", expr: &tree.Tuple{Exprs: tree.Exprs{literal, param}}, want: true},
		{name: "scalar subquery", expr: tree.NewSubquery(
			&tree.SelectClause{Exprs: tree.SelectExprs{{Expr: param}}}, false), want: true},
		{name: "scalar literal subquery", expr: tree.NewSubquery(
			&tree.SelectClause{Exprs: tree.SelectExprs{{Expr: literal}}}, false), want: false},
		{name: "exists subquery", expr: tree.NewSubquery(
			&tree.SelectClause{Exprs: tree.SelectExprs{{Expr: param}}}, true), want: false},
		{name: "literal", expr: literal, want: false},
		{name: "empty case", expr: tree.NewCaseExpr(nil, nil, nil), want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, hasParam(tc.expr))
		})
	}
}

func TestPreparedNumericAstWalkersCoverSelectAndCastShapes(t *testing.T) {
	param := tree.NewParamExpr(0)
	literal := tree.NewNumVal(int64(1), "1", false, tree.P_int64)

	doubleCast := tree.NewCastExpr(param, tree.TYPE_DOUBLE)
	integerCast := tree.NewCastExpr(param, tree.TYPE_LONG)
	require.True(t, containsExplicitFloatCast(doubleCast))
	require.False(t, containsExplicitFloatCast(integerCast))
	require.True(t, containsExplicitFloatCast(tree.NewBitCastExpr(doubleCast, nil)))
	require.True(t, containsExplicitFloatCast(tree.NewBinaryExpr(tree.PLUS, integerCast, doubleCast)))
	require.True(t, containsExplicitFloatCast(tree.NewUnaryExpr(tree.UNARY_MINUS, doubleCast)))
	require.True(t, containsExplicitFloatCast(tree.NewParentExpr(doubleCast)))
	require.True(t, containsExplicitFloatCast(&tree.FuncExpr{Exprs: tree.Exprs{doubleCast}}))
	require.True(t, containsExplicitFloatCast(tree.NewCaseExpr(doubleCast, nil, nil)))
	require.True(t, containsExplicitFloatCast(tree.NewCaseExpr(nil,
		[]*tree.When{tree.NewWhen(doubleCast, literal)}, nil)))
	require.True(t, containsExplicitFloatCast(tree.NewCaseExpr(nil,
		[]*tree.When{tree.NewWhen(literal, doubleCast)}, nil)))
	require.True(t, containsExplicitFloatCast(tree.NewCaseExpr(nil, nil, doubleCast)))
	require.True(t, containsExplicitFloatCast(&tree.Tuple{Exprs: tree.Exprs{doubleCast}}))
	require.True(t, containsExplicitFloatCast(tree.NewSubquery(
		&tree.SelectClause{Exprs: tree.SelectExprs{{Expr: doubleCast}}}, false)))
	require.False(t, containsExplicitFloatCast(literal))
	require.True(t, containsExplicitFloatCasts(tree.Exprs{integerCast, doubleCast}))
	require.False(t, containsExplicitFloatCasts(tree.Exprs{integerCast}))
}

func TestPreparedNumericPlanHelpersCoverDeferredAndIntegerPaths(t *testing.T) {
	floatType := planpb.Type{Id: int32(types.T_float64)}
	paramExpr := func(pos int32) *planpb.Expr {
		return &planpb.Expr{Typ: floatType, Expr: &planpb.Expr_P{
			P: &planpb.ParamRef{Pos: pos},
		}}
	}
	literalExpr := func(value string) *planpb.Expr {
		return &planpb.Expr{Typ: floatType, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Value: &planpb.Literal_Sval{Sval: value},
		}}}
	}
	functionExpr := func(name string, args ...*planpb.Expr) *planpb.Expr {
		return &planpb.Expr{Typ: floatType, Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: name}, Args: args,
		}}}
	}

	deferredArg := functionExpr("cast", paramExpr(0), floatTypeExprForTest())
	deferredArg.PreparedNumeric = &planpb.PreparedNumericMetadata{Fallback: true, ParamPos: 0}
	deferredAbs := functionExpr("abs", deferredArg)
	queryPlan := &Plan{Plan: &Plan_Query{Query: &Query{
		Steps: []int32{0},
		Nodes: []*Node{{NodeType: planpb.Node_PROJECT, ProjectList: []*planpb.Expr{deferredAbs}}},
	}}}
	require.True(t, PreparedPlanHasDeferredNumericFunction(queryPlan))
	deferredSubqueryArg := &planpb.Expr{
		Typ: floatType,
		PreparedNumeric: &planpb.PreparedNumericMetadata{
			Fallback: true,
			ParamPos: 0,
		},
		Expr: &planpb.Expr_Sub{Sub: &planpb.SubqueryRef{Child: deferredArg}},
	}
	deferredSubqueryAbs := functionExpr("abs", deferredSubqueryArg)
	queryPlan.GetQuery().Nodes = []*Node{{NodeType: planpb.Node_PROJECT, ProjectList: []*planpb.Expr{deferredSubqueryAbs}}}
	require.True(t, PreparedPlanHasDeferredNumericFunction(queryPlan))
	require.False(t, PreparedPlanHasDeferredNumericFunction(nil))
	require.False(t, PreparedPlanHasDeferredNumericFunction(&Plan{}))
	require.False(t, PreparedPlanHasDeferredNumericFunction(&Plan{Plan: &Plan_Query{
		Query: &Query{Steps: []int32{0}, Nodes: []*Node{{ProjectList: []*planpb.Expr{
			functionExpr("abs", literalExpr("1")),
		}}}},
	}}))

	// Exercise the ResetParamRefRule's exact integer/DECIMAL specialization,
	// including signed, signed-range unsigned, wide unsigned, invalid, NULL,
	// and missing protocol-kind values.
	newLiteralParam := func(value string, isNull bool) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Isnull: isNull, Value: &planpb.Literal_Sval{Sval: value},
		}}}
	}
	for _, tc := range []struct {
		name  string
		value string
		want  types.T
	}{
		{name: "negative", value: "-3", want: types.T_int64},
		{name: "signed range", value: "3", want: types.T_int64},
		{name: "wide unsigned", value: "9223372036854775808", want: types.T_uint64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rule := NewResetParamRefRule(context.Background(), []*planpb.Expr{newLiteralParam(tc.value, false)})
			rule.SetParamKinds([]vector.PrepareParamKind{vector.PrepareParamInteger})
			bound, ok := rule.typedIntegerParamExpr(0)
			require.True(t, ok)
			require.Equal(t, int32(tc.want), bound.Typ.Id)
		})
	}
	for _, tc := range []struct {
		name  string
		param *planpb.Expr
		kinds []vector.PrepareParamKind
		pos   int32
	}{
		{name: "invalid text", param: newLiteralParam("bad", false), kinds: []vector.PrepareParamKind{vector.PrepareParamInteger}},
		{name: "null", param: newLiteralParam("", true), kinds: []vector.PrepareParamKind{vector.PrepareParamInteger}},
		{name: "empty", param: newLiteralParam("", false), kinds: []vector.PrepareParamKind{vector.PrepareParamInteger}},
		{name: "wrong kind", param: newLiteralParam("1", false), kinds: []vector.PrepareParamKind{vector.PrepareParamFloat}},
		{name: "bad position", param: newLiteralParam("1", false), kinds: []vector.PrepareParamKind{vector.PrepareParamInteger}, pos: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rule := NewResetParamRefRule(context.Background(), []*planpb.Expr{tc.param})
			rule.SetParamKinds(tc.kinds)
			bound, ok := rule.typedIntegerParamExpr(tc.pos)
			require.False(t, ok)
			require.Nil(t, bound)
		})
	}

	rule := NewResetParamRefRule(context.Background(), []*planpb.Expr{newLiteralParam("-7", false)})
	rule.SetParamKinds([]vector.PrepareParamKind{vector.PrepareParamInteger})
	rebound, changed, err := rule.rebindPreparedIntegerExpr(paramExpr(0))
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, int32(types.T_int64), rebound.Typ.Id)
	_, changed, err = rule.rebindPreparedIntegerExpr(nil)
	require.NoError(t, err)
	require.False(t, changed)
	_, changed, err = rule.rebindPreparedIntegerExpr(literalExpr("1"))
	require.NoError(t, err)
	require.False(t, changed)
	_, changed, err = rule.rebindPreparedIntegerExpr(&planpb.Expr{Expr: &planpb.Expr_List{
		List: &planpb.ExprList{List: []*planpb.Expr{paramExpr(0), literalExpr("1")}},
	}})
	require.NoError(t, err)
	require.True(t, changed)
	_, changed, err = rule.rebindPreparedIntegerExpr(&planpb.Expr{Expr: &planpb.Expr_List{
		List: &planpb.ExprList{List: []*planpb.Expr{literalExpr("1")}},
	}})
	require.NoError(t, err)
	require.False(t, changed)

	decimalCases := []struct {
		value string
		want  types.T
	}{
		{value: "123456789012.3456", want: types.T_decimal64},
		{value: "1234567890123456789012345678901234.5678", want: types.T_decimal128},
		{value: "12345678901234567890123456789012345.6789", want: types.T_decimal256},
	}
	for _, decimalCase := range decimalCases {
		decimalRule := NewResetParamRefRule(context.Background(), []*planpb.Expr{
			newLiteralParam(decimalCase.value, false),
		})
		decimalRule.SetParamKinds([]vector.PrepareParamKind{vector.PrepareParamDecimal})
		decimal, ok, err := decimalRule.typedDecimalParamExpr(0)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, int32(decimalCase.want), decimal.Typ.Id)
	}
	decimalRule := NewResetParamRefRule(context.Background(), []*planpb.Expr{
		newLiteralParam("12345678901234567890123456789012345.6789", false),
	})
	decimalRule.SetParamKinds([]vector.PrepareParamKind{vector.PrepareParamDecimal})
	decimal, ok, err := decimalRule.typedDecimalParamExpr(0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int32(types.T_decimal256), decimal.Typ.Id)
	require.True(t, decimalRule.allDecimalParamRefs(paramExpr(0)))
	require.False(t, decimalRule.allDecimalParamRefs(literalExpr("1")))
	rebound, changed, err = decimalRule.rebindPreparedDecimalExpr(paramExpr(0))
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, int32(types.T_decimal256), rebound.Typ.Id)
	_, changed, err = decimalRule.rebindPreparedDecimalExpr(nil)
	require.NoError(t, err)
	require.False(t, changed)
	rule = NewResetParamRefRule(context.Background(), []*planpb.Expr{newLiteralParam("7", false)})
	rule.SetParamKinds([]vector.PrepareParamKind{vector.PrepareParamInteger})
	rebound, changed, err = rule.rebindPreparedIntegerExpr(deferredSubqueryArg)
	require.NoError(t, err)
	require.True(t, changed)
	if child := rebound.GetSub().Child; child.GetF() != nil {
		require.Equal(t, int64(7), child.GetF().Args[0].GetLit().GetI64Val())
	} else {
		require.Equal(t, int64(7), child.GetLit().GetI64Val())
	}

	// Cover the selective numeric-value walkers used by ABS's CASE/IF forms.
	ifExpr := functionExpr("if", literalExpr("0"), paramExpr(0), literalExpr("1"))
	caseExpr := functionExpr("case", paramExpr(0), literalExpr("1"), literalExpr("2"))
	caseValueExpr := functionExpr("case", literalExpr("0"), paramExpr(0), literalExpr("2"))
	for _, tc := range []struct {
		name string
		expr *planpb.Expr
		want bool
	}{
		{name: "nil", expr: nil, want: false},
		{name: "direct", expr: paramExpr(0), want: true},
		{name: "if result", expr: ifExpr, want: true},
		{name: "case condition", expr: caseExpr, want: false},
		{name: "case value", expr: caseValueExpr, want: true},
		{name: "literal", expr: literalExpr("1"), want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := len(preparedNumericValueParamPositions(tc.expr)) > 0
			require.Equal(t, tc.want, got)
		})
	}
	positions := make(map[int32]struct{})
	collectNumericValueParamPositions(ifExpr, positions)
	collectNumericValueParamPositions(caseValueExpr, positions)
	collectNumericValueParamPositions(&planpb.Expr{Expr: &planpb.Expr_List{
		List: &planpb.ExprList{List: []*planpb.Expr{paramExpr(1)}},
	}}, positions)
	require.Contains(t, positions, int32(0))
	require.Contains(t, positions, int32(1))

	integerRule := NewResetParamRefRule(context.Background(), []*planpb.Expr{newLiteralParam("1", false)})
	integerRule.SetParamKinds([]vector.PrepareParamKind{vector.PrepareParamInteger})
	require.True(t, integerRule.allIntegerParamRefs(paramExpr(0)))
	require.False(t, integerRule.allIntegerParamRefs(literalExpr("1")))
	integerRule.SetParamKinds([]vector.PrepareParamKind{vector.PrepareParamFloat})
	require.False(t, integerRule.allIntegerParamRefs(paramExpr(0)))
}

func TestPreparedNumericRuntimeLiteralRebindingHelpers(t *testing.T) {
	ctx := context.Background()
	provenIntegral := makePlan2Float64ConstExprWithType(1)
	provenIntegral.GetLit().Src = makePlan2Int64ConstExprWithType(1)
	provenDecimal := makePlan2Float64ConstExprWithType(1.25)
	decimalType := types.New(types.T_decimal64, 3, 2)
	decimalSource, err := preparedRuntimeParamExpr(ctx, "1.25", false, decimalType)
	require.NoError(t, err)
	provenDecimal.GetLit().Src = decimalSource
	doubleType := types.T_float64.ToType()
	explicitDouble, err := appendExplicitCastBeforeExpr(
		ctx, makePlan2Int64ConstExprWithType(1), makePlan2Type(&doubleType))
	require.NoError(t, err)
	implicitDouble, err := makePlan2CastExpr(
		ctx, makePlan2Int64ConstExprWithType(1), makePlan2Type(&doubleType))
	require.NoError(t, err)

	for _, tc := range []struct {
		name     string
		expr     *planpb.Expr
		want     bool
		wantType types.T
	}{
		{name: "proven integral source", expr: provenIntegral, want: true, wantType: types.T_int64},
		{name: "proven decimal source", expr: provenDecimal, want: true, wantType: types.T_decimal64},
		{name: "implicit numeric cast", expr: implicitDouble, want: true, wantType: types.T_int64},
		{name: "source-less integral scientific float", expr: makePlan2Float64ConstExprWithType(1)},
		{name: "source-less fractional scientific float", expr: makePlan2Float64ConstExprWithType(0.1)},
		{name: "explicit double", expr: explicitDouble},
		{name: "non-float", expr: makePlan2Int64ConstExprWithType(1)},
		{name: "nil", expr: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, ok := provisionalExactNumericSource(tc.expr)
			require.Equal(t, tc.want, ok)
			if tc.want {
				require.Equal(t, int32(tc.wantType), source.Typ.Id)
			}
		})
	}

	floatRule := NewResetParamRefRule(ctx, nil)
	floatRule.SetParamValues([]any{ParamValue{
		Value:            -1.5,
		PrepareParamKind: vector.PrepareParamFloat,
		HasRuntimeType:   true,
		RuntimeType:      types.T_float64.ToType(),
		RetainParamRef:   true,
	}})
	bound, ok, err := floatRule.typedRuntimeParamExpr(0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int32(types.T_float64), bound.Typ.Id)
	require.Equal(t, int32(0), bound.GetLit().GetSrc().GetP().Pos)

	boolRule := NewResetParamRefRule(ctx, []*planpb.Expr{{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Value: &planpb.Literal_Bval{Bval: true},
		}},
	}})
	boolRule.SetParamKinds([]vector.PrepareParamKind{vector.PrepareParamBoolean})
	runtimeType, ok := boolRule.runtimeParamType(0)
	require.True(t, ok)
	require.Equal(t, types.T_bool, runtimeType.Oid)
}

func TestPreparedNumericRebindPreservesUnsupportedAndNullBoundOccurrences(t *testing.T) {
	ctx := context.Background()
	prepared := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_float64)},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	for _, tc := range []struct {
		name  string
		value ParamValue
		bound *planpb.Expr
	}{
		{
			name: "unsupported integer source preserves materialized text",
			value: ParamValue{
				Value: "bad", PrepareParamKind: vector.PrepareParamInteger,
			},
			bound: &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text)},
				Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "bad"}}}},
		},
		{
			name: "unsupported text runtime type preserves already materialized bound",
			value: ParamValue{
				Value: "foo", MaterializedValue: "foo",
				RuntimeType: types.T_text.ToType(), HasRuntimeType: true,
			},
			bound: &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text)},
				Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "foo"}}}},
		},
		{
			name: "NULL source preserves materialized NULL",
			value: ParamValue{
				Value: nil, PrepareParamKind: vector.PrepareParamInteger,
				RuntimeType: types.T_int64.ToType(), HasRuntimeType: true,
			},
			bound: &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)},
				Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true, Value: &planpb.Literal_I64Val{I64Val: 0}}}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rule := NewResetParamRefRule(ctx, nil)
			rule.SetParamValues([]any{tc.value})
			rule.SetParamKinds([]vector.PrepareParamKind{vector.PrepareParamInteger})
			got, changed, err := rule.rebindPreparedNumericExprWithBound(
				prepared, tc.bound, map[int32]struct{}{0: {}})
			require.NoError(t, err)
			require.False(t, changed)
			require.Same(t, tc.bound, got,
				"unsupported and NULL execute values must keep the current materialized occurrence")
		})
	}
}

func TestPreparedPrecisionFallbackMaterializesOnlyMatchingParam(t *testing.T) {
	ctx := context.Background()
	param := func(pos int32) *planpb.Expr {
		return &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_text)},
			Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}},
		}
	}
	textLiteral := func(value string) *planpb.Expr {
		return &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_text)},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: value}}},
		}
	}
	function := func(name string, args ...*planpb.Expr) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: name},
			Args: args,
		}}}
	}
	int64Type := types.T_int64.ToType()
	precisionCast, err := makePlan2CastExpr(ctx, param(0), makePlan2Type(&int64Type))
	require.NoError(t, err)
	require.True(t, isImplicitPreparedParamCast(precisionCast))
	round := function("round", param(1), precisionCast)
	list := &planpb.Expr{Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{param(0), param(1)}}}}
	subquery := &planpb.Expr{Expr: &planpb.Expr_Sub{Sub: &planpb.SubqueryRef{
		Child: function("coalesce", param(0), param(1)),
	}}}
	literalSource := &planpb.Expr{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
		Value: &planpb.Literal_Sval{Sval: "prepared"},
		Src:   function("coalesce", param(0), param(1)),
	}}}
	tests := []struct {
		name  string
		expr  *planpb.Expr
		check func(*testing.T, *planpb.Expr)
	}{
		{
			name: "round precision cast",
			expr: round,
			check: func(t *testing.T, got *planpb.Expr) {
				require.Equal(t, "round", got.GetF().GetFunc().GetObjName())
				require.Equal(t, int32(1), got.GetF().GetArgs()[0].GetP().Pos)
				cast := got.GetF().GetArgs()[1]
				require.Equal(t, "cast", cast.GetF().GetFunc().GetObjName())
				require.Equal(t, int32(types.T_int64), cast.Typ.Id)
				require.Equal(t, "1.5tail", cast.GetF().GetArgs()[0].GetLit().GetSval())
			},
		},
		{
			name: "list",
			expr: list,
			check: func(t *testing.T, got *planpb.Expr) {
				require.Equal(t, "1.5tail", got.GetList().GetList()[0].GetLit().GetSval())
				require.Equal(t, int32(1), got.GetList().GetList()[1].GetP().Pos)
			},
		},
		{
			name: "scalar subquery",
			expr: subquery,
			check: func(t *testing.T, got *planpb.Expr) {
				child := got.GetSub().GetChild()
				require.Equal(t, "coalesce", child.GetF().GetFunc().GetObjName())
				require.Equal(t, "1.5tail", child.GetF().GetArgs()[0].GetLit().GetSval())
				require.Equal(t, int32(1), child.GetF().GetArgs()[1].GetP().Pos)
			},
		},
		{
			name: "literal source",
			expr: literalSource,
			check: func(t *testing.T, got *planpb.Expr) {
				require.Equal(t, "prepared", got.GetLit().GetSval())
				source := got.GetLit().GetSrc()
				require.Equal(t, "1.5tail", source.GetF().GetArgs()[0].GetLit().GetSval())
				require.Equal(t, int32(1), source.GetF().GetArgs()[1].GetP().Pos)
			},
		},
	}
	rule := NewResetParamRefRule(ctx, []*planpb.Expr{textLiteral("1.5tail"), textLiteral("not-target")})
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			materialized, changed := rule.materializePreparedParam(test.expr, 0)
			require.True(t, changed)
			require.NotSame(t, test.expr, materialized)
			require.True(t, exprContainsPreparedPosition(test.expr, 0), "source tree should remain unchanged")
			require.False(t, exprContainsPreparedPosition(materialized, 0), "only the selected marker should be replaced")
			require.True(t, exprContainsPreparedPosition(materialized, 1), "the unrelated marker must be preserved")
			test.check(t, materialized)
		})
	}
	t.Run("invalid position leaves expression unchanged", func(t *testing.T) {
		unmatched := param(0)
		got, changed := rule.materializePreparedParam(unmatched, -1)
		require.False(t, changed)
		require.Same(t, unmatched, got)
	})
	t.Run("unmatched marker and leaf remain unchanged", func(t *testing.T) {
		unmatched := param(1)
		got, changed := rule.materializePreparedParam(unmatched, 0)
		require.False(t, changed)
		require.Same(t, unmatched, got)

		leaf := &planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}
		got, changed = rule.materializePreparedParam(leaf, 0)
		require.False(t, changed)
		require.Same(t, leaf, got)
	})

	// Exercise the rebinder fallback that materializes invalid precision text
	// beneath its existing cast rather than dropping the wrapper.
	fallback, changed, err := rule.rebindPreparedNumericExprWithRole(
		precisionCast, nil, map[int32]struct{}{}, preparedStringMathRoleControl)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, "cast", fallback.GetF().GetFunc().GetObjName())
	require.Equal(t, int32(types.T_int64), fallback.Typ.Id)
	require.Equal(t, "1.5tail", fallback.GetF().GetArgs()[0].GetLit().GetSval())
}

func TestUnwrapImplicitPreparedParamCastRetainsParamSource(t *testing.T) {
	ctx := context.Background()
	source := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_text)},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	literal := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_text)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Value: &planpb.Literal_Sval{Sval: "3.33"},
			Src:   source,
		}},
	}
	targetType := types.T_int64.ToType()
	cast, err := makePlan2CastExpr(ctx, literal, makePlan2Type(&targetType))
	require.NoError(t, err)

	rewritten, ok := unwrapImplicitPreparedParamCast(ctx, cast, true)
	require.True(t, ok)
	position, found := preparedRuntimeSourceParamPosition(rewritten)
	require.True(t, found)
	require.Equal(t, 0, position)
}

func TestPreparedNumericRuntimeParamValueLiteralKinds(t *testing.T) {
	params := []*planpb.Expr{
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I8Val{I8Val: -8}}}},
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I16Val{I16Val: -16}}}},
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: -32}}}},
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: -64}}}},
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_U8Val{U8Val: 8}}}},
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_U16Val{U16Val: 16}}}},
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_U32Val{U32Val: 32}}}},
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: 64}}}},
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Fval{Fval: 1.25}}}},
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Dval{Dval: 2.5}}}},
		{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Bval{Bval: true}}}},
	}
	wants := []any{
		int8(-8), int16(-16), int32(-32), int64(-64),
		uint8(8), uint16(16), uint32(32), uint64(64),
		float32(1.25), float64(2.5), true,
	}
	rule := NewResetParamRefRule(context.Background(), params)
	for pos, want := range wants {
		value, _, ok := rule.runtimeParamValue(pos)
		require.True(t, ok, "position %d", pos)
		require.Equal(t, want, value, "position %d", pos)
	}

	nullRule := NewResetParamRefRule(context.Background(), []*planpb.Expr{{
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}},
	}})
	value, _, ok := nullRule.runtimeParamValue(0)
	require.True(t, ok)
	require.Nil(t, value)
	_, _, ok = nullRule.runtimeParamValue(-1)
	require.False(t, ok)
	_, _, ok = nullRule.runtimeParamValue(1)
	require.False(t, ok)

	runtimeType, ok := rule.runtimeParamType(3)
	require.True(t, ok)
	require.Equal(t, types.T_int64, runtimeType.Oid)
}

func floatTypeExprForTest() *planpb.Expr {
	return &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_float64)}}
}

func TestPreparedScalarNumericOverloadsCoverSubqueryAndExactInteger(t *testing.T) {
	ctx := context.Background()

	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_abs_exact from 'select abs(?)'")
	require.NoError(t, err)
	queryPlan := prepared.GetDcl().GetPrepare().Plan
	filled, err := FillValuesOfParamsInPlan(ctx, queryPlan, []any{
		ParamValue{Value: "-9007199254740993", PrepareParamKind: vector.PrepareParamInteger},
	})
	require.NoError(t, err)
	fn := findPlanFunctionExpr(filled, "abs")
	require.NotNil(t, fn)
	require.Equal(t, int32(types.T_int64), fn.Typ.Id)
	require.Equal(t, int32(types.T_int64), fn.GetF().Args[0].Typ.Id)
	require.Equal(t, int64(-9007199254740993), fn.GetF().Args[0].GetLit().GetI64Val())

	prepared, err = runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_abs_multi_exact from 'select abs(? + ?)'")
	require.NoError(t, err)
	queryPlan = prepared.GetDcl().GetPrepare().Plan
	require.Equal(t, []int32{0, 1}, PreparedPlanNumericFallbackParamPositions(queryPlan))
	filled, err = FillValuesOfParamsInPlan(ctx, queryPlan, []any{
		ParamValue{Value: "-9007199254740993", PrepareParamKind: vector.PrepareParamInteger},
		ParamValue{Value: "0", PrepareParamKind: vector.PrepareParamInteger},
	})
	require.NoError(t, err)
	fn = findPlanFunctionExpr(filled, "abs")
	require.NotNil(t, fn)
	require.Equal(t, int32(types.T_int64), fn.Typ.Id)
	require.Equal(t, int32(types.T_int64), fn.GetF().Args[0].Typ.Id)

	prepared, err = runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_abs_multi_decimal from 'select abs(? + ?)'")
	require.NoError(t, err)
	filled, err = FillValuesOfParamsInPlan(ctx, prepared.GetDcl().GetPrepare().Plan, []any{
		ParamValue{Value: "-9007199254740993", PrepareParamKind: vector.PrepareParamInteger},
		ParamValue{Value: "0.5", PrepareParamKind: vector.PrepareParamDecimal},
	})
	require.NoError(t, err)
	fn = findPlanFunctionExpr(filled, "abs")
	require.NotNil(t, fn)
	require.True(t, types.T(fn.Typ.Id).IsDecimal())
	require.True(t, types.T(fn.GetF().Args[0].Typ.Id).IsDecimal())

	for _, sql := range []string{
		"prepare stmt_abs_nested_arithmetic from 'select abs(? + 0)'",
		"prepare stmt_abs_nested_if from 'select abs(if(1, ?, 0))'",
		"prepare stmt_abs_nested_case from 'select abs(case when 1 then ? else 0 end)'",
		"prepare stmt_abs_nested_scalar from 'select abs((select ?))'",
	} {
		prepared, err = runOneStmt(NewMockOptimizer(false), t, sql)
		require.NoError(t, err)
		filled, err = FillValuesOfParamsInPlan(ctx, prepared.GetDcl().GetPrepare().Plan, []any{
			ParamValue{Value: "-9007199254740993", PrepareParamKind: vector.PrepareParamInteger},
		})
		require.NoError(t, err)
		fn = findPlanFunctionExpr(filled, "abs")
		require.NotNil(t, fn)
		require.Equal(t, int32(types.T_int64), fn.Typ.Id, sql)
		require.Equal(t, int32(types.T_int64), fn.GetF().Args[0].Typ.Id, sql)
	}

	prepared, err = runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_abs_subquery from 'select abs((select ?))'")
	require.NoError(t, err)
	fn = findPlanFunctionExpr(prepared.GetDcl().GetPrepare().Plan, "abs")
	require.NotNil(t, fn)
	require.Equal(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id)
	require.True(t, PreparedPlanHasDeferredNumericFunction(prepared.GetDcl().GetPrepare().Plan))

	prepared, err = runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_abs_case_condition from 'select abs(case when ? then n_regionkey else n_regionkey end) from nation'")
	require.NoError(t, err)
	fn = findPlanFunctionExpr(prepared.GetDcl().GetPrepare().Plan, "abs")
	require.NotNil(t, fn)
	// The marker only controls CASE flow. It must not force the BIGINT result
	// branches through the deferred DOUBLE overload.
	require.NotEqual(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id)

	prepared, err = runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_abs_if_condition from 'select abs(if(?, n_regionkey, n_regionkey)) from nation'")
	require.NoError(t, err)
	fn = findPlanFunctionExpr(prepared.GetDcl().GetPrepare().Plan, "abs")
	require.NotNil(t, fn)
	require.NotEqual(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id)

	prepared, err = runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_abs_explicit_double from 'select abs(cast(? as double))'")
	require.NoError(t, err)
	queryPlan = prepared.GetDcl().GetPrepare().Plan
	require.Empty(t, PreparedPlanNumericFallbackParamPositions(queryPlan),
		"an explicit DOUBLE cast fixes the overload at PREPARE time")
	fn = findPlanFunctionExpr(queryPlan, "abs")
	require.NotNil(t, fn)
	require.True(t, isExplicitPreparedCast(fn.GetF().Args[0]))
	copiedPlan := DeepCopyPlan(queryPlan)
	copiedFn := findPlanFunctionExpr(copiedPlan, "abs")
	require.NotNil(t, copiedFn)
	require.Truef(t, isExplicitPreparedCast(copiedFn.GetF().Args[0]),
		"explicit cast overload was lost: original=%d copied=%d",
		fn.GetF().Args[0].GetF().GetFunc().GetObj(), copiedFn.GetF().Args[0].GetF().GetFunc().GetObj())
	require.False(t, PreparedPlanNeedsRuntimeSpecialization(queryPlan))
	filled, err = FillValuesOfParamsInPlan(ctx, queryPlan, []any{
		ParamValue{Value: "9007199254740993", PrepareParamKind: vector.PrepareParamInteger},
	})
	require.NoError(t, err)
	fn = findPlanFunctionExpr(filled, "abs")
	require.NotNil(t, fn)
	// An explicit DOUBLE cast is a user-requested precision boundary and must
	// not be specialized back to an integer overload.
	require.Equal(t, int32(types.T_float64), fn.Typ.Id)

	prepared, err = runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_abs_decimal from 'select abs(?)'")
	require.NoError(t, err)
	filled, err = FillValuesOfParamsInPlan(ctx, prepared.GetDcl().GetPrepare().Plan, []any{
		ParamValue{
			Value:            "12345678901234567890123456789012345.6789",
			PrepareParamKind: vector.PrepareParamDecimal,
		},
	})
	require.NoError(t, err)
	fn = findPlanFunctionExpr(filled, "abs")
	require.NotNil(t, fn)
	// DECIMAL values must bypass the prepare-time DOUBLE fallback as well;
	// otherwise high-precision values are rounded before ABS sees them.
	require.Equal(t, int32(types.T_decimal256), fn.Typ.Id)
	decimalArg := fn.GetF().Args[0]
	require.Equal(t, int32(types.T_decimal256), decimalArg.Typ.Id)
	require.Equal(t, "cast", decimalArg.GetF().Func.GetObjName())
	require.Equal(t, "12345678901234567890123456789012345.6789", decimalArg.GetF().Args[0].GetLit().GetSval())

	prepared, err = runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt_abs_nested_scalar_round from 'select abs((select round(? + 0) from nation limit 1))'")
	require.NoError(t, err)
	queryPlan = prepared.GetDcl().GetPrepare().Plan
	require.Equal(t, []int32{0}, PreparedPlanNumericFallbackParamPositions(queryPlan))
	filled, err = FillValuesOfParamsInPlan(ctx, queryPlan, []any{
		ParamValue{Value: "-9007199254740993", PrepareParamKind: vector.PrepareParamInteger},
	})
	require.NoError(t, err)
	fn = findPlanFunctionExpr(filled, "abs")
	require.NotNil(t, fn)
	require.Equal(t, int32(types.T_int64), fn.Typ.Id)
	round := findPlanFunctionExpr(filled, "round")
	require.NotNil(t, round, "the scalar subquery projection must remain after rebinding")
	require.Equal(t, int32(types.T_int64), round.Typ.Id)
	require.NotNil(t, round.GetF().Args[0].GetF())
	require.Equal(t, int32(types.T_int64), round.GetF().Args[0].Typ.Id)
}

func TestPreparedMathStringParametersRebindToNumericOverloads(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name string
		sql  string
		fn   string
		want types.T
	}{
		{name: "abs", sql: "prepare stmt_math_abs from 'select abs(?)'", fn: "abs", want: types.T_float64},
		{name: "ceil", sql: "prepare stmt_math_ceil from 'select ceil(?)'", fn: "ceil", want: types.T_float64},
		{name: "ceiling", sql: "prepare stmt_math_ceiling from 'select ceiling(?)'", fn: "ceiling", want: types.T_float64},
		{name: "floor", sql: "prepare stmt_math_floor from 'select floor(?)'", fn: "floor", want: types.T_float64},
		{name: "round", sql: "prepare stmt_math_round from 'select round(?)'", fn: "round", want: types.T_float64},
		{name: "sign", sql: "prepare stmt_math_sign from 'select sign(?)'", fn: "sign", want: types.T_int64},
		{name: "truncate", sql: "prepare stmt_math_truncate from 'select truncate(?)'", fn: "truncate", want: types.T_float64},
		{name: "mod", sql: "prepare stmt_math_mod from 'select mod(?, 2)'", fn: "mod", want: types.T_float64},
	} {
		t.Run(test.name, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.NoError(t, err)
			preparedPlan := prepared.GetDcl().GetPrepare().Plan
			require.Equal(t, []int32{0}, PreparedPlanNumericFallbackParamPositions(preparedPlan))

			filled, err := FillValuesOfParamsInPlan(ctx, preparedPlan, []any{ParamValue{
				Value:          "1.5tail",
				RuntimeType:    types.T_varchar.ToType(),
				HasRuntimeType: true,
			}})
			require.NoError(t, err)
			fn := findPlanFunctionExpr(filled, test.fn)
			require.NotNil(t, fn)
			require.Equal(t, int32(test.want), fn.Typ.Id)
			// Character parameters are explicitly cast to DOUBLE so execution
			// reuses the stable numeric overload and its warning/binary semantics.
			require.Equal(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id)
		})
	}
}

func TestMathStringPlannerBindsLiteralsAndVarcharColumnsToDouble(t *testing.T) {
	ctx := context.Background()
	queries := []struct {
		name      string
		sql       string
		functions []string
	}{
		{
			name:      "literal",
			sql:       "select abs('1.5tail'), ceil('1.5tail'), floor('1.5tail'), round('1.5tail'), sign('1.5tail'), truncate('1.5tail', 1)",
			functions: []string{"abs", "ceil", "floor", "round", "sign", "truncate"},
		},
		{
			name:      "varchar column",
			sql:       "select abs(n_name), ceil(n_name), floor(n_name), mod(n_name, 2), round(n_name, 1), sign(n_name), truncate(n_name, 1) from nation",
			functions: []string{"abs", "ceil", "floor", "mod", "round", "sign", "truncate"},
		},
	}

	for _, query := range queries {
		t.Run(query.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, query.sql, 1)
			require.NoError(t, err)
			built, err := BuildPlan(NewMockCompilerContext(false), stmt, false)
			require.NoError(t, err)
			optimized, err := NewBaseOptimizer(NewMockCompilerContext(false)).Optimize(stmt, false)
			require.NoError(t, err)
			optimizedPlan := &planpb.Plan{Plan: &planpb.Plan_Query{Query: optimized}}

			for _, name := range query.functions {
				fn := findPlanFunctionExpr(built, name)
				require.NotNil(t, fn, name)
				_, castOverload := function.DecodeOverloadID(fn.GetF().Args[0].GetF().GetFunc().GetObj())
				require.Equal(t, int32(2), castOverload, fn.String())
				require.Equal(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id, fn.String())
				if name == "mod" {
					require.Equal(t, int32(types.T_float64), fn.GetF().Args[1].Typ.Id, fn.String())
				}
				if name == "truncate" {
					require.Len(t, fn.GetF().Args, 2)
					require.Equal(t, int32(types.T_int64), fn.GetF().Args[1].Typ.Id, fn.String())
				}
				optFn := findPlanFunctionExpr(optimizedPlan, name)
				if optFn != nil {
					_, optCastOverload := function.DecodeOverloadID(optFn.GetF().Args[0].GetF().GetFunc().GetObj())
					require.Equal(t, int32(2), optCastOverload, optFn.String())
					require.Equal(t, int32(types.T_float64), optFn.GetF().Args[0].Typ.Id, optFn.String())
					if name == "mod" {
						require.Equal(t, int32(types.T_float64), optFn.GetF().Args[1].Typ.Id, optFn.String())
					}
				}
			}
		})
	}
}

func TestPreparedNestedMathStringParameterRebindsToNumericOverload(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name        string
		sql         string
		fn          string
		value       any
		runtimeType types.Type
		want        types.T
	}{
		{name: "abs plus string", sql: "prepare stmt_nested_abs from 'select abs(? + 0)'", fn: "abs", value: "1.5tail", runtimeType: types.T_varchar.ToType(), want: types.T_float64},
		{name: "ceil plus string", sql: "prepare stmt_nested_ceil from 'select ceil(? + 0)'", fn: "ceil", value: "1.5tail", runtimeType: types.T_varchar.ToType(), want: types.T_float64},
		{name: "floor plus string", sql: "prepare stmt_nested_floor from 'select floor(? + 0)'", fn: "floor", value: "1.5tail", runtimeType: types.T_varchar.ToType(), want: types.T_float64},
		{name: "round plus string", sql: "prepare stmt_nested_round from 'select round(? + 0)'", fn: "round", value: "1.5tail", runtimeType: types.T_varchar.ToType(), want: types.T_float64},
		{name: "sign plus string", sql: "prepare stmt_nested_sign from 'select sign(? + 0)'", fn: "sign", value: "1.5tail", runtimeType: types.T_varchar.ToType(), want: types.T_int64},
		{name: "truncate plus string", sql: "prepare stmt_nested_truncate from 'select truncate(? + 0, 1)'", fn: "truncate", value: "1.5tail", runtimeType: types.T_varchar.ToType(), want: types.T_float64},
		{name: "mod plus string", sql: "prepare stmt_nested_mod from 'select mod(? + 0, 2)'", fn: "mod", value: "1.5tail", runtimeType: types.T_varchar.ToType(), want: types.T_float64},
		{name: "abs plus integer", sql: "prepare stmt_nested_abs_int from 'select abs(? + 0)'", fn: "abs", value: int64(2), runtimeType: types.T_int64.ToType(), want: types.T_int64},
		{name: "round plus integer", sql: "prepare stmt_nested_round_int from 'select round(? + 0)'", fn: "round", value: int64(2), runtimeType: types.T_int64.ToType(), want: types.T_int64},
		{name: "mod plus integer", sql: "prepare stmt_nested_mod_int from 'select mod(? + 0, 2)'", fn: "mod", value: int64(2), runtimeType: types.T_int64.ToType(), want: types.T_int64},
	} {
		t.Run(test.name, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.NoError(t, err)
			preparedPlan := prepared.GetDcl().GetPrepare().Plan
			filled, err := FillValuesOfParamsInPlan(ctx, preparedPlan, []any{ParamValue{
				Value:          test.value,
				RuntimeType:    test.runtimeType,
				HasRuntimeType: true,
			}})
			require.NoError(t, err)
			fn := findPlanFunctionExpr(filled, test.fn)
			require.NotNil(t, fn)
			require.Equal(t, int32(test.want), fn.Typ.Id)
		})
	}
}

func TestPreparedMathStringValueAndPrecisionRoles(t *testing.T) {
	ctx := context.Background()
	type evalResult struct {
		typ    types.T
		scale  int32
		isNull bool
		value  any
	}
	type executionMode struct {
		mysqlNumericCompatibility bool
		matrixOneNative           bool
	}
	eval := func(t *testing.T, expr *planpb.Expr, mode ...executionMode) (evalResult, error) {
		t.Helper()
		proc := testutil.NewProc(t)
		if len(mode) > 0 {
			proc.GetSessionInfo().MySQLNumericCompatibilityMode = mode[0].mysqlNumericCompatibility
			proc.GetSessionInfo().MatrixOneNativeMode = mode[0].matrixOneNative
		}
		executor, err := colexec.NewExpressionExecutor(proc, expr)
		require.NoError(t, err)
		defer executor.Free()
		result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
		if err != nil {
			return evalResult{}, err
		}
		require.NotNil(t, result)
		observed := evalResult{
			typ:    result.GetType().Oid,
			scale:  result.GetType().Scale,
			isNull: result.IsNull(0),
		}
		if observed.isNull {
			return observed, nil
		}
		// Eval returns an executor-owned, reusable vector. Copy the scalar value
		// before executor.Free; freeing result here would double-release it.
		switch observed.typ {
		case types.T_int64:
			observed.value = vector.GetFixedAtWithTypeCheck[int64](result, 0)
		case types.T_float64:
			observed.value = vector.GetFixedAtWithTypeCheck[float64](result, 0)
		case types.T_decimal64:
			observed.value = vector.GetFixedAtWithTypeCheck[types.Decimal64](result, 0).Format(result.GetType().Scale)
		default:
			t.Fatalf("unsupported evaluated type %v", observed.typ)
		}
		return observed, nil
	}
	stringParam := func(value string) ParamValue {
		return ParamValue{Value: value, SourceType: types.T_varchar.ToType(), HasSourceType: true}
	}

	for _, name := range []string{"round", "truncate", "ceil", "ceiling", "floor"} {
		t.Run(name+" rejects non-integer precision without DOUBLE prefix", func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t,
				"prepare stmt_math_precision from 'select "+name+"(?, ?)'")
			require.NoError(t, err)
			preparePlan := prepared.GetDcl().GetPrepare().Plan
			original := proto.Clone(preparePlan)
			precision := "1.5tail"
			filled, specialized, fillErr := FillValuesOfParamsInPlanWithSpecialization(ctx, preparePlan, []any{
				stringParam("1.5"),
				stringParam(precision),
			})
			require.NoError(t, fillErr, precision)
			require.True(t, specialized, precision)
			fn := findPlanFunctionExpr(filled, name)
			require.NotNil(t, fn, precision)
			require.Len(t, fn.GetF().Args, 2)
			precisionCast := fn.GetF().Args[1]
			require.Equal(t, int32(types.T_int64), precisionCast.Typ.Id, precisionCast.String())
			require.NotNil(t, precisionCast.GetF())
			_, castOverload := function.DecodeOverloadID(precisionCast.GetF().Func.Obj)
			require.Equal(t, int32(0), castOverload, "precision must not use the private integer-prefix cast")
			require.Equal(t, int32(types.T_text), precisionCast.GetF().Args[0].Typ.Id,
				"precision must retain the INT64 cast's text source, not a DOUBLE source")
			require.Equal(t, precision, precisionCast.GetF().Args[0].GetLit().GetSval())
			direct, err := runOneStmt(NewMockOptimizer(false), t,
				fmt.Sprintf("select %s('1.5', '%s')", name, precision))
			require.NoError(t, err)
			directFn := findPlanFunctionExpr(direct, name)
			require.NotNil(t, directFn)
			for _, compatibility := range []bool{false, true} {
				mode := executionMode{mysqlNumericCompatibility: compatibility}
				for _, expr := range []*planpb.Expr{directFn, fn} {
					_, evalErr := eval(t, expr, mode)
					require.ErrorContains(t, evalErr, "invalid argument cast to int, bad value "+precision,
						"precision=%q compatibility=%t must keep direct/prepared INT64 error semantics", precision, compatibility)
				}
			}
			valid, _, err := FillValuesOfParamsInPlanWithSpecialization(ctx, preparePlan,
				[]any{stringParam("1.5"), stringParam("2")})
			require.NoError(t, err)
			got, err := eval(t, findPlanFunctionExpr(valid, name))
			require.NoError(t, err, "the same template must work after rejected precision values")
			require.Equal(t, float64(1.5), got.value)
			require.True(t, proto.Equal(original, preparePlan), "EXECUTE must not mutate the cached template")
		})

		if name == "round" {
			t.Run(name+" precision keeps actual source and ordinary cast", func(t *testing.T) {
				prepared, err := runOneStmt(NewMockOptimizer(false), t,
					"prepare stmt_precision_sources from 'select "+name+"(1.25, ?)'")
				require.NoError(t, err)
				preparePlan := prepared.GetDcl().GetPrepare().Plan
				original := proto.Clone(preparePlan)
				for _, tc := range []struct {
					name     string
					value    ParamValue
					want     int64
					wantNull bool
					wantErr  bool
				}{
					{name: "text integer", value: stringParam("2"), want: 2},
					{name: "DOUBLE halfway", value: ParamValue{Value: float64(2.5), RuntimeType: types.T_float64.ToType(), HasRuntimeType: true}, want: 3},
					{name: "negative DOUBLE halfway", value: ParamValue{Value: float64(-2.5), RuntimeType: types.T_float64.ToType(), HasRuntimeType: true}, want: -3},
					{name: "DOUBLE upper overflow", value: ParamValue{Value: float64(0x1p63), RuntimeType: types.T_float64.ToType(), HasRuntimeType: true}, wantErr: true},
					{name: "DOUBLE lower overflow", value: ParamValue{Value: math.Nextafter(-0x1p63, math.Inf(-1)), RuntimeType: types.T_float64.ToType(), HasRuntimeType: true}, wantErr: true},
					{name: "SQL EXECUTE DECIMAL halfway", value: ParamValue{Value: "2.5", PrepareParamKind: vector.PrepareParamDecimal, SourceType: types.New(types.T_decimal64, 2, 1), HasSourceType: true}, want: 3},
					{name: "COM_STMT DECIMAL halfway", value: ParamValue{Value: "2.5", PrepareParamKind: vector.PrepareParamDecimal, RuntimeType: types.New(types.T_decimal64, 2, 1), HasRuntimeType: true}, want: 3},
					{name: "exact integer beyond DOUBLE", value: ParamValue{Value: int64(9007199254740993), RuntimeType: types.T_int64.ToType(), HasRuntimeType: true}, want: 9007199254740993},
					{name: "maximum signed text", value: stringParam("9223372036854775807"), want: 9223372036854775807},
					{name: "minimum signed text", value: stringParam("-9223372036854775808"), want: -9223372036854775808},
					{name: "unsigned overflow", value: ParamValue{Value: uint64(9223372036854775808), RuntimeType: types.T_uint64.ToType(), HasRuntimeType: true}, wantErr: true},
					{name: "NULL", value: ParamValue{SourceType: types.T_varchar.ToType(), HasSourceType: true}, wantNull: true},
				} {
					t.Run(tc.name, func(t *testing.T) {
						filled, _, err := FillValuesOfParamsInPlanWithSpecialization(ctx, preparePlan, []any{tc.value})
						require.NoError(t, err)
						fn := findPlanFunctionExpr(filled, name)
						require.NotNil(t, fn)
						precision := fn.GetF().Args[1]
						require.Equal(t, int32(types.T_int64), precision.Typ.Id)
						if cast := precision.GetF(); cast != nil {
							require.Equal(t, "cast", cast.Func.ObjName)
							_, overload := function.DecodeOverloadID(cast.Func.Obj)
							require.Equal(t, int32(0), overload)
						}
						got, evalErr := eval(t, precision)
						if tc.wantErr {
							require.True(t, moerr.IsMoErrCode(evalErr, moerr.ErrOutOfRange), "%v", evalErr)
							return
						}
						require.NoError(t, evalErr)
						require.Equal(t, tc.wantNull, got.isNull)
						if !tc.wantNull {
							require.Equal(t, tc.want, got.value)
						}
					})
				}
				require.True(t, proto.Equal(original, preparePlan))
			})
		}

		t.Run(name+" value marker still uses numeric-prefix source", func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t,
				"prepare stmt_math_value from 'select "+name+"(?, 0)'")
			require.NoError(t, err)
			filled, _, err := FillValuesOfParamsInPlanWithSpecialization(ctx,
				prepared.GetDcl().GetPrepare().Plan, []any{stringParam("1.5tail")})
			require.NoError(t, err)
			fn := findPlanFunctionExpr(filled, name)
			require.NotNil(t, fn)
			require.Equal(t, int32(types.T_float64), fn.GetF().Args[0].Typ.Id, fn.String())
			got, evalErr := eval(t, fn, executionMode{mysqlNumericCompatibility: true})
			require.NoError(t, evalErr)
			require.Equal(t, types.T_float64, got.typ)
			require.False(t, got.isNull)
			want := float64(2)
			if name == "truncate" || name == "floor" {
				want = 1
			}
			require.Equal(t, want, got.value)
		})

		t.Run(name+" value runtime domains keep precision separate", func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t,
				"prepare stmt_math_runtime_domains from 'select "+name+"(?, ?)'")
			require.NoError(t, err)
			preparePlan := prepared.GetDcl().GetPrepare().Plan
			precision := ParamValue{Value: int64(0), RuntimeType: types.T_int64.ToType(), HasRuntimeType: true}
			for _, test := range []struct {
				name      string
				value     ParamValue
				wantType  types.T
				wantValue any
				wantScale int32
				wantNull  bool
			}{
				{name: "NULL", value: ParamValue{SourceType: types.T_varchar.ToType(), HasSourceType: true}, wantNull: true},
				{name: "integer", value: ParamValue{Value: int64(15), RuntimeType: types.T_int64.ToType(), HasRuntimeType: true}, wantType: types.T_int64, wantValue: int64(15)},
				{name: "DOUBLE", value: ParamValue{Value: float64(1.5), RuntimeType: types.T_float64.ToType(), HasRuntimeType: true}, wantType: types.T_float64, wantValue: float64(2)},
				{name: "DECIMAL", value: ParamValue{Value: "1.5", PrepareParamKind: vector.PrepareParamDecimal}, wantType: types.T_decimal64, wantValue: "2", wantScale: 0},
				{name: "binary text", value: ParamValue{Value: "1", IsBin: true, IsBinaryProtocol: true,
					RuntimeType: types.T_text.ToType(), HasRuntimeType: true}, wantType: types.T_float64, wantValue: float64(49)},
			} {
				t.Run(test.name, func(t *testing.T) {
					filled, _, fillErr := FillValuesOfParamsInPlanWithSpecialization(ctx, preparePlan,
						[]any{test.value, precision})
					require.NoError(t, fillErr)
					fn := findPlanFunctionExpr(filled, name)
					require.NotNil(t, fn)
					require.Equal(t, int32(types.T_int64), fn.GetF().Args[1].Typ.Id, fn.String())
					got, evalErr := eval(t, fn)
					require.NoError(t, evalErr)
					require.Equal(t, test.wantNull, got.isNull)
					if !test.wantNull {
						if test.wantType != types.T_any {
							require.Equal(t, test.wantType, got.typ)
						}
						if test.wantType == types.T_decimal64 {
							require.Equal(t, test.wantScale, got.scale,
								"precision zero must produce the expected DECIMAL scale")
						}
						wantValue := test.wantValue
						if name == "truncate" || name == "floor" {
							switch test.name {
							case "DOUBLE":
								wantValue = float64(1)
							case "DECIMAL":
								wantValue = "1"
							}
						}
						require.Equal(t, wantValue, got.value)
					}
				})
			}
		})
	}

	for _, tc := range []struct {
		function string
		sql      string
	}{
		{function: "round", sql: "prepare stmt_nested_precision from 'select abs(round(1, ?))'"},
		{function: "truncate", sql: "prepare stmt_nested_truncate_precision from 'select abs(truncate(1, ?))'"},
	} {
		t.Run("nested ABS does not upgrade "+tc.function+" precision", func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t, tc.sql)
			require.NoError(t, err)
			filled, _, err := FillValuesOfParamsInPlanWithSpecialization(ctx,
				prepared.GetDcl().GetPrepare().Plan, []any{stringParam("1.5tail")})
			require.NoError(t, err)
			root := findPlanFunctionExpr(filled, "abs")
			require.NotNil(t, root)
			function := findPlanFunctionExpr(filled, tc.function)
			require.NotNil(t, function)
			require.Equal(t, int32(types.T_int64), function.GetF().Args[1].Typ.Id, function.String())
			_, evalErr := eval(t, root)
			require.ErrorContains(t, evalErr, "invalid argument cast to int, bad value 1.5tail")
		})
	}

	for _, tc := range []struct {
		name string
		sql  string
		fn   string
		want string
	}{
		{name: "round precision inner abs", sql: "prepare stmt_inner_abs from 'select round(12.34, abs(?))'", want: "12.34"},
		{name: "truncate precision inner abs", sql: "prepare stmt_truncate_inner_abs from 'select truncate(12.34, abs(?))'", fn: "truncate", want: "12.34"},
		{name: "round precision inner arithmetic", sql: "prepare stmt_inner_arithmetic from 'select round(12.34, ? + 0)'", want: "12.34"},
		{name: "round precision explicit signed", sql: "prepare stmt_explicit_signed from 'select round(12.34, cast(? as signed))'", want: "12.30"},
		{name: "round precision explicit double", sql: "prepare stmt_explicit_double from 'select round(12.34, cast(? as double))'", want: "12.34"},
		{name: "ceil precision explicit double", sql: "prepare stmt_ceil_explicit_double from 'select ceil(12.34, cast(? as double))'", fn: "ceil", want: "12.34"},
		{name: "floor precision selector", sql: "prepare stmt_floor_selector from 'select floor(12.34, if(true, cast(? as double), 0e0))'", fn: "floor", want: "12.34"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t, tc.sql)
			require.NoError(t, err)
			filled, _, err := FillValuesOfParamsInPlanWithSpecialization(ctx,
				prepared.GetDcl().GetPrepare().Plan, []any{stringParam("1.5tail")})
			require.NoError(t, err)
			fnName := tc.fn
			if fnName == "" {
				fnName = "round"
			}
			fn := findPlanFunctionExpr(filled, fnName)
			require.NotNil(t, fn)
			require.Len(t, fn.GetF().Args, 2)
			precisionArg := fn.GetF().Args[1]
			require.Equal(t, int32(types.T_int64), precisionArg.Typ.Id, fn.String())
			precisionCast := precisionArg.GetF()
			require.NotNil(t, precisionCast)
			require.Equal(t, "cast", precisionCast.GetFunc().GetObjName())

			switch tc.name {
			case "round precision inner abs", "truncate precision inner abs":
				// The marker is a value of the nested ABS and therefore keeps
				// the permissive DOUBLE source. The outer function's precision
				// cast remains INT64 and is not rebound to the ABS source.
				innerExpr := precisionCast.GetArgs()[0]
				inner := innerExpr.GetF()
				require.NotNil(t, inner)
				require.Equal(t, "abs", inner.GetFunc().GetObjName())
				require.Equal(t, int32(types.T_float64), innerExpr.Typ.Id)
			case "round precision inner arithmetic":
				// The arithmetic node owns the marker's numeric-prefix value
				// semantics, while the enclosing precision cast stays INT64.
				innerExpr := precisionCast.GetArgs()[0]
				inner := innerExpr.GetF()
				require.NotNil(t, inner)
				require.Equal(t, "+", inner.GetFunc().GetObjName())
				require.Len(t, inner.GetArgs(), 2)
				require.Equal(t, int32(types.T_float64), innerExpr.Typ.Id)
			case "round precision explicit signed":
				// CAST(? AS SIGNED) is an explicit integer boundary. It uses
				// the dedicated explicit CAST overload and must not be treated
				// as a provisional prepared numeric source.
				_, overload := function.DecodeOverloadID(precisionCast.GetFunc().GetObj())
				require.Equal(t, int32(1), overload)
				require.Equal(t, int32(types.T_text), precisionCast.GetArgs()[0].Typ.Id)
			case "round precision explicit double", "ceil precision explicit double":
				// The outer math function adds only its implicit INT64 cast;
				// the user-written DOUBLE cast remains an explicit overload
				// inside it and is not removed by role-aware fallback.
				_, overload := function.DecodeOverloadID(precisionCast.GetFunc().GetObj())
				require.Equal(t, int32(0), overload)
				innerExpr := precisionCast.GetArgs()[0]
				inner := innerExpr.GetF()
				require.NotNil(t, inner)
				_, overload = function.DecodeOverloadID(inner.GetFunc().GetObj())
				require.Equal(t, int32(1), overload)
				require.Equal(t, int32(types.T_float64), innerExpr.Typ.Id)
			}
			got, evalErr := eval(t, fn, executionMode{mysqlNumericCompatibility: true})
			require.NoError(t, evalErr)
			require.False(t, got.isNull)
			require.Equal(t, types.T_decimal64, got.typ)
			require.Equal(t, tc.want, got.value)
		})
	}

	for _, sql := range []string{
		"prepare stmt_mod_left from 'select mod(?, 2)'",
		"prepare stmt_mod_right from 'select mod(2, ?)'",
	} {
		t.Run(sql, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t, sql)
			require.NoError(t, err)
			filled, _, err := FillValuesOfParamsInPlanWithSpecialization(ctx,
				prepared.GetDcl().GetPrepare().Plan, []any{stringParam("1.5tail")})
			require.NoError(t, err)
			fn := findPlanFunctionExpr(filled, "mod")
			require.NotNil(t, fn)
			require.Equal(t, int32(types.T_float64), fn.Typ.Id, fn.String())
			got, evalErr := eval(t, fn, executionMode{mysqlNumericCompatibility: true})
			require.NoError(t, evalErr)
			require.Equal(t, types.T_float64, got.typ)
			require.False(t, got.isNull)
			want := float64(1.5)
			if strings.Contains(sql, "mod(2,") {
				want = 0.5
			}
			require.Equal(t, want, got.value)
		})
	}
	t.Run("shared ParamRef keeps value and precision source channels isolated", func(t *testing.T) {
		prepared, err := runOneStmt(NewMockOptimizer(false), t,
			"prepare stmt_shared_param_roles from 'select round(abs(?), ?)'")
		require.NoError(t, err)
		preparePlan := prepared.GetDcl().GetPrepare().Plan
		round := findPlanFunctionExpr(preparePlan, "round")
		require.NotNil(t, round)
		precisionParam, ok := implicitPreparedParam(round.GetF().Args[1])
		require.True(t, ok)
		precisionParam.Pos = 0 // Model one shared ParamRef feeding both roles.

		filled, _, err := FillValuesOfParamsInPlanWithSpecialization(ctx, preparePlan,
			[]any{stringParam("1.5tail")})
		require.NoError(t, err)
		boundRound := findPlanFunctionExpr(filled, "round")
		require.NotNil(t, boundRound)
		innerAbs := findPlanFunctionExpr(filled, "abs")
		require.NotNil(t, innerAbs)
		require.Equal(t, int32(types.T_float64), innerAbs.GetF().Args[0].Typ.Id)
		boundPrecision := boundRound.GetF().Args[1]
		require.Equal(t, int32(types.T_int64), boundPrecision.Typ.Id)
		require.Equal(t, int32(types.T_text), boundPrecision.GetF().Args[0].Typ.Id,
			"shared value/control plan was rebound as: %s", boundRound.String())
		_, evalErr := eval(t, boundRound, executionMode{mysqlNumericCompatibility: true})
		require.ErrorContains(t, evalErr, "invalid argument cast to int, bad value 1.5tail")
	})
	t.Run("same prepared template rechecks incomplete token after mode flip", func(t *testing.T) {
		prepared, err := runOneStmt(NewMockOptimizer(false), t,
			"prepare stmt_mode_flip from 'select abs(?)'")
		require.NoError(t, err)
		filled, specialized, err := FillValuesOfParamsInPlanWithSpecialization(ctx,
			prepared.GetDcl().GetPrepare().Plan, []any{ParamValue{
				Value: "1.5tail", SourceType: types.T_varchar.ToType(), HasSourceType: true,
				EnableNumericPrefix: true,
			}})
		require.NoError(t, err)
		require.True(t, specialized)
		abs := findPlanFunctionExpr(filled, "abs")
		require.NotNil(t, abs)

		_, err = eval(t, abs)
		require.Error(t, err, "the unset compatibility mode must stay strict")
		mysqlValue, err := eval(t, abs, executionMode{mysqlNumericCompatibility: true})
		require.NoError(t, err)
		require.Equal(t, float64(1.5), mysqlValue.value)
		_, err = eval(t, abs)
		require.Error(t, err, "switching back to default strict mode must not reuse the prefix result")
		_, err = eval(t, abs, executionMode{
			mysqlNumericCompatibility: true,
			matrixOneNative:           true,
		})
		require.Error(t, err, "MATRIXONE_NATIVE must reject an incomplete numeric token")
		mysqlValue, err = eval(t, abs, executionMode{mysqlNumericCompatibility: true})
		require.NoError(t, err, "the same prepared template must work again in MySQL mode")
		require.Equal(t, float64(1.5), mysqlValue.value)
	})
	for _, name := range []string{"round", "truncate"} {
		t.Run(name+" explicit value CAST matches direct SQL", func(t *testing.T) {
			directPlan, err := runOneStmt(NewMockOptimizer(false), t,
				"select "+name+"(cast('1.5tail' as signed), 0)")
			require.NoError(t, err)
			directFn := findPlanFunctionExpr(directPlan, name)
			require.NotNil(t, directFn, directPlan.String())
			directCast := directFn.GetF().Args[0]
			require.True(t, isExplicitPreparedCast(directCast), directFn.String())
			require.True(t, isStringBackedType(makeTypeByPlan2Expr(directCast.GetF().Args[0])))
			directSource := directCast.GetF().Args[0].GetLit()
			require.NotNil(t, directSource, directFn.String())
			require.Equal(t, "1.5tail", directSource.GetSval())
			directResult, err := eval(t, directFn)
			require.NoError(t, err)
			require.False(t, directResult.isNull)
			require.Equal(t, types.T_int64, directResult.typ)
			require.Equal(t, int64(1), directResult.value)

			prepared, err := runOneStmt(NewMockOptimizer(false), t,
				"prepare stmt_explicit_value_cast_"+name+
					" from 'select "+name+"(cast(? as signed), ?)'")
			require.NoError(t, err)
			boundPlan, _, err := FillValuesOfParamsInPlanWithSpecialization(
				ctx, prepared.GetDcl().GetPrepare().Plan, []any{
					stringParam("1.5tail"),
					ParamValue{Value: int64(0), RuntimeType: types.T_int64.ToType(), HasRuntimeType: true},
				})
			require.NoError(t, err)
			boundFn := findPlanFunctionExpr(boundPlan, name)
			require.NotNil(t, boundFn, boundPlan.String())
			require.True(t, isExplicitPreparedCast(boundFn.GetF().Args[0]), boundFn.String())
			require.Equal(t, int32(types.T_int64), boundFn.GetF().Args[0].Typ.Id, boundFn.String())
			boundSource := boundFn.GetF().Args[0].GetF().Args[0]
			require.True(t, isStringBackedType(makeTypeByPlan2Expr(boundSource)), boundFn.String())
			require.NotNil(t, boundSource.GetLit(), boundFn.String())
			require.Equal(t, "1.5tail", boundSource.GetLit().GetSval(), boundFn.String())
			require.Equal(t, int32(types.T_int64), boundFn.GetF().Args[1].Typ.Id, boundFn.String())

			boundResult, err := eval(t, boundFn)
			require.NoError(t, err)
			require.False(t, boundResult.isNull)
			require.Equal(t, types.T_int64, boundResult.typ)
			require.Equal(t, int64(1), boundResult.value)
			require.Equal(t, directResult.typ, boundResult.typ)
			require.Equal(t, directResult.scale, boundResult.scale)
			require.Equal(t, directResult.value, boundResult.value)
		})
	}
}

func TestBindFuncExprImplByPlanExpr_CaseDifferentDecimalScale(t *testing.T) {
	ctx := context.Background()

	condExpr := makePlan2BoolConstExprWithType(false)
	thenExpr := makeDecimal128ConstExpr("-58140.00", 23, 2)
	elseExpr := makeDecimal128ConstExpr("-408180.5580000", 38, 7)

	result, err := BindFuncExprImplByPlanExpr(ctx, "case", []*planpb.Expr{condExpr, thenExpr, elseExpr})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, int32(types.T_decimal128), result.Typ.Id)
	require.Equal(t, int32(38), result.Typ.Width)
	require.Equal(t, int32(7), result.Typ.Scale)

	funcExpr := result.GetF()
	require.NotNil(t, funcExpr)
	require.Len(t, funcExpr.Args, 3)

	arg1 := funcExpr.Args[1]
	require.True(t, isCastExpr(arg1), "THEN value should be cast when CASE decimal branch scales differ")
	require.Equal(t, int32(types.T_decimal128), arg1.Typ.Id)
	require.Equal(t, int32(38), arg1.Typ.Width)
	require.Equal(t, int32(7), arg1.Typ.Scale)
	require.False(t, isCastExpr(funcExpr.Args[2]), "ELSE value already has the common decimal scale")
}

func TestFoldedImplicitStringCastPreservesNumericBinaryLiteralMarker(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	defer proc.Free()

	source := makePlan2StringConstExprWithType("1", true)
	source.GetLit().LiteralForm = planpb.StringLiteralForm_STRING_LITERAL_HEX
	toType := types.T_binary.ToType()
	toType.Width = 1

	implicit, err := appendCastBeforeExpr(ctx, source, makePlan2Type(&toType))
	require.NoError(t, err)
	foldedImplicit, err := ConstantFold(batch.EmptyForConstFoldBatch, implicit, proc, false, true)
	require.NoError(t, err)
	require.NotNil(t, foldedImplicit.GetLit())
	require.True(t, foldedImplicit.GetLit().GetIsBin(),
		"folding an implicit string cast must retain the HEX/BIT numeric-literal marker")

	explicit, err := appendSyntaxExplicitCastBeforeExpr(ctx, source, makePlan2Type(&toType))
	require.NoError(t, err)
	foldedExplicit, err := ConstantFold(batch.EmptyForConstFoldBatch, explicit, proc, false, true)
	require.NoError(t, err)
	require.NotNil(t, foldedExplicit.GetLit())
	require.False(t, foldedExplicit.GetLit().GetIsBin(),
		"an explicit cast is a numeric-literal provenance boundary")
}

func TestBoundFlowControlAfterImplicitStringCastFoldRetainsHexBitNumericRows(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	defer proc.Free()

	input := batch.NewWithSize(3)
	defer input.Clean(proc.Mp())
	conditionRows := [][]bool{
		{true, false, false, false},
		{false, true, false, false},
		{false, false, true, false},
	}
	for i, values := range conditionRows {
		input.Vecs[i] = vector.NewVec(types.T_bool.ToType())
		require.NoError(t, vector.AppendFixedList(input.Vecs[i], values, nil, proc.Mp()))
	}
	input.SetRowCount(4)
	signedType := types.T_int64.ToType()

	column := func(pos int32) *planpb.Expr {
		return &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_bool)},
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: pos}},
		}
	}
	bind := func(name string, args ...*planpb.Expr) *planpb.Expr {
		expr, err := BindFuncExprImplByPlanExpr(ctx, name, args)
		require.NoError(t, err)
		return expr
	}
	text := makePlan2StringConstExprWithType("1")
	binaryType := types.T_binary.ToType()
	binaryType.Width = 1
	explicitBinary, err := appendSyntaxExplicitCastBeforeExpr(ctx, text, makePlan2Type(&binaryType))
	require.NoError(t, err)

	for _, literal := range []struct {
		name string
		form planpb.StringLiteralForm
	}{
		{name: "hex", form: planpb.StringLiteralForm_STRING_LITERAL_HEX},
		{name: "bit", form: planpb.StringLiteralForm_STRING_LITERAL_BIT},
	} {
		t.Run(literal.name, func(t *testing.T) {
			numericLiteral := makePlan2StringConstExprWithType("1", true)
			numericLiteral.GetLit().LiteralForm = literal.form
			elseNull := makePlan2NullConstExprWithType()
			caseExpr := bind("case",
				column(0), DeepCopyExpr(explicitBinary),
				column(1), numericLiteral,
				column(2), makePlan2StringConstExprWithType("1"),
				elseNull,
			)
			ifExpr := bind("if", column(1), DeepCopyExpr(numericLiteral), DeepCopyExpr(explicitBinary))

			for _, flow := range []struct {
				name string
				expr *planpb.Expr
			}{
				{name: "case", expr: caseExpr},
				{name: "if", expr: ifExpr},
			} {
				t.Run(flow.name, func(t *testing.T) {
					abs := bind("abs", flow.expr)
					folded, err := ConstantFold(batch.EmptyForConstFoldBatch, abs, proc, false, true)
					require.NoError(t, err)
					features, err := planpb.RequiredRemoteExpressionFeatures(folded)
					require.NoError(t, err)
					require.True(t, features.NumericBinaryLiteralProvenance,
						"the actual binder/folded CASE/IF plan must request its v94 row-marker trailer")

					executor, err := colexec.NewExpressionExecutor(proc, folded)
					require.NoError(t, err)
					defer executor.Free()
					result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
					require.NoError(t, err)
					values := vector.MustFixedColNoTypeCheck[float64](result)
					require.Equal(t, float64(1), values[0], "ordinary BINARY branch must remain text-numeric")
					require.Equal(t, float64(49), values[1], "selected HEX/BIT branch must keep numeric-literal provenance")
					if flow.name == "case" {
						require.Equal(t, float64(1), values[2], "ordinary text branch must remain text-numeric")
						require.True(t, result.IsNull(3), "ELSE NULL must remain NULL")
					} else {
						require.Equal(t, float64(1), values[2], "ordinary BINARY fallback must remain text-numeric")
						require.Equal(t, float64(1), values[3], "ordinary BINARY fallback must remain text-numeric")
					}
				})
			}
		})
	}

	t.Run("uniformly marked branches preserve their numeric value", func(t *testing.T) {
		hex := makePlan2StringConstExprWithType("1", true)
		hex.GetLit().LiteralForm = planpb.StringLiteralForm_STRING_LITERAL_HEX
		otherHex := makePlan2StringConstExprWithType("2", true)
		otherHex.GetLit().LiteralForm = planpb.StringLiteralForm_STRING_LITERAL_HEX
		flow := bind("case", column(0), hex, otherHex)
		cast, err := appendSyntaxExplicitCastBeforeExpr(ctx, flow, makePlan2Type(&signedType))
		require.NoError(t, err)
		features, err := planpb.RequiredRemoteExpressionFeatures(cast)
		require.NoError(t, err)
		require.True(t, features.NumericBinaryLiteralProvenance,
			"the v93 executor drops even uniform flow-control marker values")

		executor, err := colexec.NewExpressionExecutor(proc, cast)
		require.NoError(t, err)
		defer executor.Free()
		result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		values := vector.MustFixedColNoTypeCheck[int64](result)
		require.Equal(t, []int64{49, 50, 50, 50}, values)
	})

	t.Run("marked branch with null fallback still preserves its value", func(t *testing.T) {
		hex := makePlan2StringConstExprWithType("1", true)
		hex.GetLit().LiteralForm = planpb.StringLiteralForm_STRING_LITERAL_HEX
		flow := bind("case", column(0), hex, makePlan2NullConstExprWithType())
		cast, err := appendSyntaxExplicitCastBeforeExpr(ctx, flow, makePlan2Type(&signedType))
		require.NoError(t, err)
		features, err := planpb.RequiredRemoteExpressionFeatures(cast)
		require.NoError(t, err)
		require.True(t, features.NumericBinaryLiteralProvenance,
			"NULL alternatives do not make the selected literal marker compatible with v93")

		executor, err := colexec.NewExpressionExecutor(proc, cast)
		require.NoError(t, err)
		defer executor.Free()
		result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
		require.NoError(t, err)
		values := vector.MustFixedColNoTypeCheck[int64](result)
		require.Equal(t, int64(49), values[0])
		for row := 1; row < input.RowCount(); row++ {
			require.True(t, result.IsNull(uint64(row)), "row %d must remain NULL", row)
		}
	})
}

func TestBuildPreparedCaseConditionParameter(t *testing.T) {
	ctx := context.Background()
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL,
		"select case when ? then v else -v end from (select 1 as v) t", 1)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(NewMockCompilerContext(true), stmt, true)
	require.NoError(t, err)
	require.NoError(t, NormalizePrepareParamRefs(ctx, queryPlan))

	caseExpr := findPlanFunctionExpr(queryPlan, "case")
	require.NotNil(t, caseExpr)
	require.Len(t, caseExpr.GetF().Args, 3)
	requirePreparedCaseCondition(t, caseExpr.GetF().Args[0], true)

	for _, test := range []struct {
		name    string
		value   any
		wantNil bool
		want    string
	}{
		{name: "true", value: "1", want: "1"},
		{name: "false", value: "0", want: "0"},
		{name: "null", value: nil, wantNil: true},
		{name: "binary true", value: ParamValue{Value: "1", IsBin: true}, want: "1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			filled, err := FillValuesOfParamsInPlan(ctx, queryPlan, []any{test.value})
			require.NoError(t, err)
			filledCase := findPlanFunctionExpr(filled, "case")
			require.NotNil(t, filledCase)
			requirePreparedCaseCondition(t, filledCase.GetF().Args[0], false)

			conditionArg := filledCase.GetF().Args[0].GetF().Args[0]
			if test.wantNil {
				require.True(t, conditionArg.GetLit().GetIsnull())
			} else {
				require.Equal(t, test.want, conditionArg.GetLit().GetSval())
				require.Equal(t, test.name == "binary true", conditionArg.GetLit().GetIsBin())
			}
		})
	}
}

func TestBuildSearchedCaseAcceptsImplicitBooleanConditions(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
	}{
		{
			name: "literal null",
			sql:  "select case when null then 'a' else 'b' end",
		},
		{
			name: "null arithmetic",
			sql:  "select case when 0 / 0 then 'a' else 'b' end",
		},
		{
			name: "mixed conditions",
			sql:  "select case when null then 'a' when 1 = 1 then 'b' else 'c' end",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, test.sql, 1)
			require.NoError(t, err)

			queryPlan, err := BuildPlan(NewMockCompilerContext(true), stmt, false)
			require.NoError(t, err)
			require.NotNil(t, findPlanFunctionExpr(queryPlan, "case"))
		})
	}
}

func requirePreparedCaseCondition(t *testing.T, condition *planpb.Expr, hasParam bool) {
	t.Helper()
	require.Equal(t, int32(types.T_bool), condition.Typ.Id)
	cast := condition.GetF()
	require.NotNil(t, cast)
	require.Equal(t, "cast", cast.Func.GetObjName())
	require.NotEmpty(t, cast.Args)
	if hasParam {
		require.NotNil(t, cast.Args[0].GetP())
		require.Equal(t, int32(0), cast.Args[0].GetP().Pos)
	} else {
		require.NotNil(t, cast.Args[0].GetLit())
	}
}

func findPlanFunctionExpr(queryPlan *planpb.Plan, name string) *planpb.Expr {
	var find func(*planpb.Expr) *planpb.Expr
	find = func(expr *planpb.Expr) *planpb.Expr {
		if expr == nil {
			return nil
		}
		if fn := expr.GetF(); fn != nil {
			if fn.Func.GetObjName() == name {
				return expr
			}
			for _, arg := range fn.Args {
				if found := find(arg); found != nil {
					return found
				}
			}
		}
		if list := expr.GetList(); list != nil {
			for _, arg := range list.List {
				if found := find(arg); found != nil {
					return found
				}
			}
		}
		return nil
	}

	if query := queryPlan.GetQuery(); query != nil {
		for _, node := range query.Nodes {
			for _, exprs := range [][]*planpb.Expr{
				node.ProjectList,
				node.FilterList,
				node.OnList,
				node.AggList,
				node.GroupBy,
			} {
				for _, expr := range exprs {
					if found := find(expr); found != nil {
						return found
					}
				}
			}
		}
	}
	return nil
}
