// Copyright 2021 Matrix Origin
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
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestPreparedDecimalPrefixCastStartsAtMORPCVersion21(t *testing.T) {
	rt := runtime.ServiceRuntime("")
	defer rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCLatestVersion)
	proc := testutil.NewProc(t)

	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion20)
	require.False(t, preparedDecimalPrefixCastEnabled(proc))
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion21)
	require.True(t, preparedDecimalPrefixCastEnabled(proc))
}

func TestBaseBindParamMaterializesRuntimeProtocolKindOnlyWhenMarked(t *testing.T) {
	proc := testutil.NewProc(t)
	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte("9007199254740993"), false, proc.Mp()))
	proc.SetOwnedPrepareParamsWithMeta(params, nil, []vector.PrepareParamKind{vector.PrepareParamInteger})

	compilerCtx := NewMockCompilerContext(true)
	compilerCtx.GetProcessFunc = func() *process.Process { return proc }
	binder := &baseBinder{
		sysCtx:  WithPrepareRuntimeParams(context.Background()),
		builder: &QueryBuilder{compCtx: compilerCtx},
	}
	expr, err := binder.baseBindParam(&tree.ParamExpr{Offset: 1}, 0, false)
	require.NoError(t, err)
	require.IsType(t, &planpb.Literal_I64Val{}, expr.GetLit().GetValue())
	require.Equal(t, int64(9007199254740993), expr.GetLit().GetI64Val())

	binder.sysCtx = context.Background()
	expr, err = binder.baseBindParam(&tree.ParamExpr{Offset: 1}, 0, false)
	require.NoError(t, err)
	require.NotNil(t, expr.GetP())
}

func TestBaseBindParamMaterializesOnlySelectedRuntimePositions(t *testing.T) {
	proc := testutil.NewProc(t)
	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte("5"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(params, []byte("9007199254740992.0001"), false, proc.Mp()))
	proc.SetOwnedPrepareParamsWithMeta(params, nil, []vector.PrepareParamKind{
		vector.PrepareParamInteger,
		vector.PrepareParamNone,
	})

	compilerCtx := NewMockCompilerContext(true)
	compilerCtx.GetProcessFunc = func() *process.Process { return proc }
	binder := &baseBinder{
		sysCtx:  WithPrepareRuntimeParams(context.Background(), 1),
		builder: &QueryBuilder{compCtx: compilerCtx},
	}
	projection, err := binder.baseBindParam(&tree.ParamExpr{Offset: 1}, 0, false)
	require.NoError(t, err)
	require.NotNil(t, projection.GetP())

	predicate, err := binder.baseBindParam(&tree.ParamExpr{Offset: 2}, 0, false)
	require.NoError(t, err)
	require.Equal(t, "9007199254740992.0001", predicate.GetLit().GetSval())
	require.True(t, predicate.ExactDecimalParam)
}

func TestRuntimeBooleanDecimalComparisonUsesExactIntegerDomain(t *testing.T) {
	decimalType := types.New(types.T_decimal128, 20, 4)
	column := &planpb.Expr{
		Typ:  MakePlan2Type(&decimalType),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}},
	}
	boolean := makePlan2BoolConstExprWithType(true)
	boolean.ExactDecimalParam = true

	expr, err := BindFuncExprImplByPlanExpr(context.Background(), "=", []*planpb.Expr{column, boolean})
	require.NoError(t, err)
	for _, arg := range expr.GetF().Args {
		require.True(t, types.T(arg.Typ.Id).IsDecimal(), arg.String())
	}
}

func TestRuntimeFloatDecimalComparisonsKeepExactDecimalDomain(t *testing.T) {
	decimalType := types.New(types.T_decimal128, 20, 4)
	for _, operator := range []string{"=", "<=>", "!=", "<", "<=", ">", ">="} {
		for _, reversed := range []bool{false, true} {
			t.Run(operator+strconv.FormatBool(reversed), func(t *testing.T) {
				column := &planpb.Expr{
					Typ:  MakePlan2Type(&decimalType),
					Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}},
				}
				value := makePlan2Float64ConstExprWithType(9007199254740992)
				value.ExactDecimalParam = true
				args := []*planpb.Expr{column, value}
				if reversed {
					args[0], args[1] = args[1], args[0]
				}
				expr, err := BindFuncExprImplByPlanExpr(context.Background(), operator, args)
				require.NoError(t, err)
				for _, arg := range expr.GetF().Args {
					require.True(t, types.T(arg.Typ.Id).IsDecimal(), arg.String())
				}
			})
		}
	}
}

func TestRuntimeRangeWithStaticStringUsesOneRealDomain(t *testing.T) {
	decimalType := types.New(types.T_decimal128, 20, 4)
	column := &planpb.Expr{
		Typ:  MakePlan2Type(&decimalType),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}},
	}
	runtime := makePlan2StringConstExprWithType("9007199254740992.00005")
	runtime.ExactDecimalParam = true
	static := makePlan2StringConstExprWithType("9007199254740992.99995")
	operands := []*planpb.Expr{column, runtime, static}

	normalized, err := normalizePreparedDecimalRange(context.Background(), operands)
	require.NoError(t, err)
	require.True(t, normalized)
	for _, operand := range operands {
		require.Equal(t, int32(types.T_float64), operand.Typ.Id, operand.String())
	}
}

func TestRuntimeNumericLeftINWithStaticStringUsesRealDomain(t *testing.T) {
	left := makePlan2Int64ConstExprWithType(9007199254740993)
	left.ExactDecimalParam = true
	decimalType := types.New(types.T_decimal128, 20, 4)
	decimal := &planpb.Expr{
		Typ:  MakePlan2Type(&decimalType),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}},
	}
	static := makePlan2StringConstExprWithType("9007199254740993.0")
	list := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_tuple)},
		Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
			decimal, static,
		}}},
	}

	expr, err := BindFuncExprImplByPlanExpr(context.Background(), "in", []*planpb.Expr{left, list})
	require.NoError(t, err)
	require.True(t, expressionComparisonsUseType(expr, types.T_float64), expr.String())
}

func TestRuntimeParamRebuildBindsMixedINBeforeOptimization(t *testing.T) {
	proc := testutil.NewProc(t)
	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte("9007199254740993"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(params, []byte("9007199254740993.0"), false, proc.Mp()))
	proc.SetOwnedPrepareParamsWithMeta(params, nil, []vector.PrepareParamKind{
		vector.PrepareParamInteger,
		vector.PrepareParamNone,
	})

	compilerCtx := NewMockCompilerContext(true)
	compilerCtx.GetProcessFunc = func() *process.Process { return proc }
	compilerCtx.SetContext(WithPrepareRuntimeParams(context.Background()))
	stmt, err := parsers.ParseOne(
		compilerCtx.GetContext(),
		dialect.MYSQL,
		"prepare p from 'select ? in (?, 9007199254740992.0001)'",
		1,
	)
	require.NoError(t, err)
	defer stmt.Free()

	rebuilt, err := BuildPlan(compilerCtx, stmt, false)
	require.NoError(t, err)
	query := rebuilt.GetDcl().GetPrepare().GetPlan().GetQuery()
	require.NotNil(t, query)

	for _, node := range query.Nodes {
		for _, expression := range append(append([]*planpb.Expr{}, node.ProjectList...), node.FilterList...) {
			require.False(t, exprContainsParam(expression), expression.String())
		}
	}
}

func TestRuntimeRebuildPreservesUnmaterializedProtocolPositions(t *testing.T) {
	proc := testutil.NewProc(t)
	params := vector.NewVec(types.T_text.ToType())
	for _, value := range []string{"9007199254740992.0001", "7"} {
		require.NoError(t, vector.AppendBytes(params, []byte(value), false, proc.Mp()))
	}
	proc.SetOwnedPrepareParamsWithMeta(params, nil, []vector.PrepareParamKind{
		vector.PrepareParamNone,
		vector.PrepareParamInteger,
	})

	compilerCtx := NewMockCompilerContext(true)
	decimalType := types.New(types.T_decimal128, 20, 4)
	compilerCtx.tables["part"].Cols[7].Typ = makePlan2Type(&decimalType)
	compilerCtx.GetProcessFunc = func() *process.Process { return proc }
	compilerCtx.SetContext(WithPrepareRuntimeParams(context.Background(), 0))
	stmt, err := parsers.ParseOne(
		compilerCtx.GetContext(), dialect.MYSQL,
		"prepare p from 'select p_partkey from part where p_retailprice = ? and p_partkey = ?'", 1)
	require.NoError(t, err)
	defer stmt.Free()

	rebuilt, err := BuildPlan(compilerCtx, stmt, false)
	require.NoError(t, err)
	require.Equal(t, []int32{1}, runtimePlanParamPositions(rebuilt.GetDcl().GetPrepare().GetPlan()))
}

func TestRuntimeTupleINAstKeepsDecimalDomainForFloatParam(t *testing.T) {
	proc := testutil.NewProc(t)
	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte("9007199254740992"), false, proc.Mp()))
	proc.SetOwnedPrepareParamsWithMeta(params, nil, []vector.PrepareParamKind{vector.PrepareParamFloat})

	compilerCtx := NewMockCompilerContext(true)
	decimalType := types.New(types.T_decimal128, 20, 4)
	compilerCtx.tables["part"].Cols[7].Typ = makePlan2Type(&decimalType)
	compilerCtx.GetProcessFunc = func() *process.Process { return proc }
	compilerCtx.SetContext(WithPrepareRuntimeParams(context.Background(), 0))
	stmt, err := parsers.ParseOne(
		compilerCtx.GetContext(), dialect.MYSQL,
		"prepare p from 'select p_partkey from part where (p_retailprice,p_partkey) in ((?,3))'", 1)
	require.NoError(t, err)
	defer stmt.Free()

	rebuilt, err := BuildPlan(compilerCtx, stmt, false)
	require.NoError(t, err)
	foundDecimalComparison := false
	for _, node := range rebuilt.GetDcl().GetPrepare().GetPlan().GetQuery().Nodes {
		for _, filter := range node.FilterList {
			require.False(t, decimalExpressionCastsToFloat(filter), filter.String())
			foundDecimalComparison = foundDecimalComparison || decimalComparisonUsesExactDomain(filter)
		}
	}
	require.True(t, foundDecimalComparison)
}

func decimalComparisonUsesExactDomain(expr *planpb.Expr) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func.GetObjName() == "=" && len(fn.Args) == 2 &&
			types.T(fn.Args[0].Typ.Id).IsDecimal() && types.T(fn.Args[1].Typ.Id).IsDecimal() {
			return true
		}
		for _, arg := range fn.Args {
			if decimalComparisonUsesExactDomain(arg) {
				return true
			}
		}
	}
	return false
}

func decimalExpressionCastsToFloat(expr *planpb.Expr) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func.GetObjName() == "cast" && len(fn.Args) > 0 &&
			types.T(fn.Args[0].Typ.Id).IsDecimal() && types.T(expr.Typ.Id) == types.T_float64 {
			return true
		}
		for _, arg := range fn.Args {
			if decimalExpressionCastsToFloat(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if decimalExpressionCastsToFloat(item) {
				return true
			}
		}
	}
	return false
}

func runtimePlanParamPositions(p *planpb.Plan) []int32 {
	positions := make([]int32, 0)
	var visit func(*planpb.Expr)
	visit = func(expr *planpb.Expr) {
		if expr == nil {
			return
		}
		if param := expr.GetP(); param != nil {
			positions = append(positions, param.Pos)
		}
		if fn := expr.GetF(); fn != nil {
			for _, arg := range fn.Args {
				visit(arg)
			}
		}
		if list := expr.GetList(); list != nil {
			for _, item := range list.List {
				visit(item)
			}
		}
	}
	query := p.GetQuery()
	for _, node := range query.Nodes {
		for _, expr := range append(append([]*planpb.Expr{}, node.ProjectList...), node.FilterList...) {
			visit(expr)
		}
	}
	return positions
}

func exprContainsParam(expr *planpb.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.GetP() != nil {
		return true
	}
	if function := expr.GetF(); function != nil {
		for _, arg := range function.Args {
			if exprContainsParam(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if exprContainsParam(item) {
				return true
			}
		}
	}
	return false
}

func TestRuntimeMixedINPreservesIndependentWireDomains(t *testing.T) {
	decimalType := types.New(types.T_decimal128, 20, 4)
	column := &planpb.Expr{
		Typ:  MakePlan2Type(&decimalType),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}},
	}
	textParam := makePlan2StringConstExprWithType("9007199254740992.0001")
	textParam.ExactDecimalParam = true
	floatParam := makePlan2Float64ConstExprWithType(0)
	floatParam.ExactDecimalParam = true
	list := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_tuple)},
		Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
			textParam, floatParam,
		}}},
	}

	expr, err := BindFuncExprImplByPlanExpr(context.Background(), "in", []*planpb.Expr{column, list})
	require.NoError(t, err)
	comparisonTypes := collectComparisonOperandTypes(expr)
	require.Len(t, comparisonTypes, 2)
	require.True(t, comparisonTypes[0][0].IsDecimal())
	require.True(t, comparisonTypes[0][1].IsDecimal())
	require.Equal(t, types.T_float64, comparisonTypes[1][0])
	require.Equal(t, types.T_float64, comparisonTypes[1][1])
}

func TestStaticFloatINUsesOneRealListDomain(t *testing.T) {
	decimalType := types.New(types.T_decimal128, 38, 10)
	column := &planpb.Expr{
		Typ:  MakePlan2Type(&decimalType),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}},
	}
	for _, floatExpr := range []*planpb.Expr{
		makePlan2Float64ConstExprWithType(9007199254740992),
		func() *planpb.Expr {
			expr, err := BindFuncExprImplByPlanExpr(context.Background(), "+", []*planpb.Expr{
				makePlan2Float64ConstExprWithType(9007199254740992),
				makePlan2Float64ConstExprWithType(0),
			})
			require.NoError(t, err)
			return expr
		}(),
	} {
		list := &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_tuple)},
			Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
				floatExpr, makePlan2StringConstExprWithType("0"),
			}}},
		}
		expr, err := BindFuncExprImplByPlanExpr(context.Background(), "in",
			[]*planpb.Expr{DeepCopyExpr(column), list})
		require.NoError(t, err)
		comparisonTypes := collectComparisonOperandTypes(expr)
		require.NotEmpty(t, comparisonTypes)
		for _, comparison := range comparisonTypes {
			require.Equal(t, types.T_float64, comparison[0])
			require.Equal(t, types.T_float64, comparison[1])
		}
	}
}

func TestPreparedRangeWithRowBoundExpandsToComparisons(t *testing.T) {
	decimalType := types.New(types.T_decimal128, 20, 4)
	constant := makeDecimal128ConstExpr("2.0000", 20, 4)
	column := &planpb.Expr{
		Typ:  MakePlan2Type(&decimalType),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}},
	}
	expr, err := bindPreparedRangeOperands(context.Background(), false,
		[]*Expr{constant, column, DeepCopyExpr(constant)})
	require.NoError(t, err)
	require.Equal(t, "and", expr.GetF().GetFunc().GetObjName())
	require.Equal(t, ">=", expr.GetF().GetArgs()[0].GetF().GetFunc().GetObjName())
	require.Equal(t, "<=", expr.GetF().GetArgs()[1].GetF().GetFunc().GetObjName())

	expr, err = bindPreparedRangeOperands(context.Background(), false,
		[]*Expr{constant, DeepCopyExpr(constant), DeepCopyExpr(constant)})
	require.NoError(t, err)
	require.Equal(t, "between", expr.GetF().GetFunc().GetObjName())
}

func TestTupleRuntimeFloatNormalizesToExactDecimal(t *testing.T) {
	floatParam := makePlan2Float64ConstExprWithType(9007199254740992)
	floatParam.ExactDecimalParam = true
	normalized, err := normalizeTuplePreparedDecimalValue(context.Background(), floatParam)
	require.NoError(t, err)
	require.True(t, types.T(normalized.Typ.Id).IsDecimal())
	require.True(t, normalized.ExactDecimalParam)
}

func TestRuntimePreparedNumericExactRetainsDecimal256Domain(t *testing.T) {
	tests := []struct {
		marker string
		oid    types.T
		width  int32
		scale  int32
	}{
		{marker: "mo_runtime_numeric:8:18:2", oid: types.T_decimal64, width: 18, scale: 2},
		{marker: "mo_runtime_numeric:8:38:4", oid: types.T_decimal128, width: 38, scale: 4},
		{marker: "mo_runtime_numeric:8:46:10", oid: types.T_decimal256, width: 46, scale: 10},
		{marker: "mo_runtime_numeric:7:76:9", oid: types.T_float64},
		{marker: "mo_runtime_numeric:5:0:0", oid: types.T_text},
		{marker: "mo_runtime_numeric:9:65:30", oid: types.T_decimal256, width: 65, scale: 30},
	}
	for _, test := range tests {
		expr := &Expr{Typ: planpb.Type{Enumvalues: test.marker}}
		typ, _, found := runtimePreparedNumericType(expr)
		require.True(t, found)
		require.Equal(t, test.oid, typ.Oid)
		require.Equal(t, test.width, typ.Width)
		require.Equal(t, test.scale, typ.Scale)
	}
}

func TestRuntimePreparedNumericCommonTypeOverflowUsesApproximateNumericDomain(t *testing.T) {
	paramType := types.T_text.ToType()
	param := &Expr{
		Typ:  planpb.Type{Id: int32(types.T_text), Enumvalues: "mo_runtime_numeric:8:46:10"},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	peerType := types.New(types.T_decimal256, 76, 9)
	peer := &Expr{Typ: MakePlan2Type(&peerType)}

	resolutionTypes := decimalParamCommonTypeResolutionTypes(
		"greatest", []*Expr{param, peer}, []types.Type{paramType, peerType}, true,
	)
	require.Len(t, resolutionTypes, 2)
	require.Equal(t, types.T_float64, resolutionTypes[0].Oid)
	require.Equal(t, types.T_float64, resolutionTypes[1].Oid)
}

func TestRuntimePreparedNumericOverflowMaterializesFloatCast(t *testing.T) {
	paramType := types.T_text.ToType()
	param := &Expr{
		Typ:  planpb.Type{Id: int32(types.T_text), Enumvalues: "mo_runtime_numeric:8:76:9"},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	peerType := types.New(types.T_decimal256, 46, 10)
	args := []*Expr{param, {Typ: MakePlan2Type(&peerType)}}
	argsType := []types.Type{paramType, peerType}
	resolutionTypes := []types.Type{types.T_float64.ToType(), types.T_float64.ToType()}

	require.NoError(t, normalizeDecimalParamCommonTypeCastSources(
		context.Background(), args, argsType, resolutionTypes, types.T_float64.ToType(),
	))
	require.Equal(t, types.T_float64, types.T(args[0].Typ.Id))
	require.Equal(t, types.T_float64, argsType[0].Oid)
	require.NotNil(t, args[0].GetF())
	require.Equal(t, "cast", args[0].GetF().GetFunc().GetObjName())
	require.Equal(t, "mo_runtime_numeric:8:76:9", args[0].GetF().GetArgs()[0].Typ.Enumvalues)
	require.Equal(t, types.T_float64, types.T(args[1].Typ.Id))
	require.Equal(t, types.T_float64, argsType[1].Oid)
	require.NotNil(t, args[1].GetF())
	require.Equal(t, "cast", args[1].GetF().GetFunc().GetObjName())
}

func TestRuntimePreparedNumericOverflowBindsEveryOperandAsFloat(t *testing.T) {
	param := &Expr{
		Typ:  planpb.Type{Id: int32(types.T_text), Enumvalues: "mo_runtime_numeric:4:76:9"},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	peerType := types.New(types.T_decimal256, 46, 10)
	peer := &Expr{
		Typ:  MakePlan2Type(&peerType),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}},
	}

	bound, err := BindFuncExprImplByPlanExpr(
		context.Background(), "least", []*Expr{param, peer},
	)
	require.NoError(t, err)
	require.Equal(t, types.T_float64, types.T(bound.Typ.Id))
	require.Len(t, bound.GetF().GetArgs(), 2)
	for _, arg := range bound.GetF().GetArgs() {
		require.Equal(t, types.T_float64, types.T(arg.Typ.Id))
		require.NotNil(t, arg.GetF())
		require.Equal(t, "cast", arg.GetF().GetFunc().GetObjName())
	}
}

func TestPreparedIntegerAndBoolUseStableDecimalCommonDomain(t *testing.T) {
	peerType := types.New(types.T_decimal64, 10, 2)
	for _, param := range []*Expr{
		makePlan2Int64ConstExprWithType(-42),
		makePlan2Uint64ConstExprWithType(math.MaxUint64),
		makePlan2BoolConstExprWithType(true),
	} {
		param.ExactDecimalParam = true
		peer := &Expr{Typ: MakePlan2Type(&peerType), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}
		bound, err := BindFuncExprImplByPlanExpr(context.Background(), "coalesce", []*Expr{param, peer})
		require.NoError(t, err)
		require.Equal(t, types.T_decimal256, types.T(bound.Typ.Id))
		require.Equal(t, int32(65), bound.Typ.Width)
		require.Equal(t, int32(30), bound.Typ.Scale)
	}
}

func TestPreparedNumericCommonTypeResolutionGuards(t *testing.T) {
	paramType := types.T_text.ToType()
	param := &Expr{
		Typ:  planpb.Type{Id: int32(types.T_text)},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	decimalType := types.New(types.T_decimal256, 65, 30)
	decimalExpr := &Expr{Typ: MakePlan2Type(&decimalType)}
	args := []*Expr{param, decimalExpr}
	typesWithDecimal := []types.Type{paramType, decimalType}

	require.Equal(t, typesWithDecimal,
		decimalParamCommonTypeResolutionTypes("abs", args, typesWithDecimal, true))
	require.Equal(t, typesWithDecimal,
		decimalParamCommonTypeResolutionTypes("coalesce", args[:1], typesWithDecimal, true))
	require.Equal(t, typesWithDecimal,
		decimalParamCommonTypeResolutionTypes("coalesce", args, typesWithDecimal, false))
	require.Equal(t, "mo_decimal_common_type_dependency", param.Typ.Enumvalues)

	plainTypes := []types.Type{paramType, types.T_int64.ToType()}
	require.Equal(t, plainTypes,
		decimalParamCommonTypeResolutionTypes("coalesce", args, plainTypes, true))
	unsupportedTypes := []types.Type{paramType, types.T_json.ToType()}
	require.Equal(t, unsupportedTypes,
		decimalParamCommonTypeResolutionTypes("coalesce", args, unsupportedTypes, true))

	tooWide := types.New(types.T_decimal256, 76, 0)
	param.Typ.Enumvalues = ""
	require.Equal(t, []types.Type{paramType, tooWide},
		decimalParamCommonTypeResolutionTypes(
			"coalesce", []*Expr{param, {Typ: MakePlan2Type(&tooWide)}},
			[]types.Type{paramType, tooWide}, true,
		))

	param.Typ.Enumvalues = "mo_runtime_numeric:8:46:10"
	specialArgs := []*Expr{
		param,
		decimalExpr,
		{Typ: planpb.Type{Id: int32(types.T_bool)}},
		{Typ: planpb.Type{Id: int32(types.T_bit)}},
		{Typ: planpb.Type{Id: int32(types.T_year)}},
	}
	specialTypes := []types.Type{
		paramType,
		decimalType,
		types.T_bool.ToType(),
		types.T_bit.ToType(),
		types.T_year.ToType(),
	}
	resolved := decimalParamCommonTypeResolutionTypes("least", specialArgs, specialTypes, true)
	require.Equal(t, types.T_decimal256, resolved[0].Oid)
	require.Equal(t, types.T_uint8, resolved[2].Oid)
	require.Equal(t, types.T_decimal128, resolved[3].Oid)
	require.Equal(t, types.T_decimal64, resolved[4].Oid)

	floatParam := makePlan2Float64ConstExprWithType(0)
	floatParam.ExactDecimalParam = true
	stringParam := makePlan2StringConstExprWithType("9007199254740992.0000000002")
	stringParam.ExactDecimalParam = true
	greatestArgs := []*Expr{stringParam, floatParam, decimalExpr}
	greatestTypes := []types.Type{types.T_varchar.ToType(), types.T_float64.ToType(), decimalType}
	require.Equal(t, greatestTypes,
		decimalParamCommonTypeResolutionTypes("greatest", greatestArgs, greatestTypes, true))
}

func TestPreparedDecimalCommonTypeHelperGuardsAndLists(t *testing.T) {
	nilFunction := &Expr{Expr: &planpb.Expr_F{F: nil}}
	require.False(t, isFoldableDecimalComparisonConstant(nilFunction))
	require.False(t, isFoldableDecimalComparisonConstant(&Expr{
		Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*Expr{nil}}},
	}))
	require.True(t, isFoldableDecimalComparisonConstant(&Expr{
		Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*Expr{{
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 1}}},
		}}}},
	}))

	require.NoError(t, normalizeDecimalParamCommonTypeCastSources(
		context.Background(), []*Expr{{}}, nil, nil, types.T_decimal64.ToType(),
	))

	paramType := types.T_text.ToType()
	param := &Expr{
		Typ:  planpb.Type{Id: int32(types.T_text)},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	dynamicType := types.New(types.T_decimal256, 65, 30)
	castTypes := applyDecimalParamCommonTypeCasts(
		[]*Expr{param},
		[]types.Type{paramType},
		[]types.Type{dynamicType},
		dynamicType,
		nil,
	)
	require.Equal(t, []types.Type{dynamicType}, castTypes)
	require.Nil(t, applyDecimalParamCommonTypeCasts(
		[]*Expr{param}, []types.Type{paramType}, nil, types.T_int64.ToType(), nil,
	))
	tooWide := types.New(types.T_decimal256, 76, 0)
	require.Nil(t, applyDecimalParamCommonTypeCasts(
		[]*Expr{param, {Typ: MakePlan2Type(&tooWide)}},
		[]types.Type{paramType, tooWide},
		[]types.Type{dynamicType, tooWide},
		dynamicType,
		nil,
	))
}

func expressionComparisonsUseType(expr *planpb.Expr, expected types.T) bool {
	if expr == nil {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func != nil && (fn.Func.ObjName == "=" || fn.Func.ObjName == "!=") {
			for _, arg := range fn.Args {
				if types.T(arg.Typ.Id) != expected {
					return false
				}
			}
		}
		for _, arg := range fn.Args {
			if !expressionComparisonsUseType(arg, expected) {
				return false
			}
		}
	}
	return true
}

func collectComparisonOperandTypes(expr *planpb.Expr) (ret [][]types.T) {
	if expr == nil {
		return nil
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func != nil && (fn.Func.ObjName == "=" || fn.Func.ObjName == "!=") {
			operandTypes := make([]types.T, len(fn.Args))
			for i, arg := range fn.Args {
				operandTypes[i] = types.T(arg.Typ.Id)
			}
			ret = append(ret, operandTypes)
		}
		for _, arg := range fn.Args {
			ret = append(ret, collectComparisonOperandTypes(arg)...)
		}
	}
	return ret
}
