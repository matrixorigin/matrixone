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
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

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

func TestRuntimeMixedINUsesOneRealDomainBeforeVectorConstruction(t *testing.T) {
	decimalType := types.New(types.T_decimal128, 20, 4)
	column := &planpb.Expr{
		Typ:  MakePlan2Type(&decimalType),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}},
	}
	textParam := makePlan2StringConstExprWithType("9007199254740992.0001")
	textParam.ExactDecimalParam = true
	floatParam := makePlan2Float64ConstExprWithType(9007199254740992)
	floatParam.ExactDecimalParam = true
	list := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_tuple)},
		Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
			textParam, floatParam,
		}}},
	}

	expr, err := BindFuncExprImplByPlanExpr(context.Background(), "in", []*planpb.Expr{column, list})
	require.NoError(t, err)
	require.True(t, expressionComparisonsUseType(expr, types.T_float64))
}

func TestTupleRuntimeFloatNormalizesToExactDecimal(t *testing.T) {
	floatParam := makePlan2Float64ConstExprWithType(9007199254740992)
	floatParam.ExactDecimalParam = true
	normalized, err := normalizeTuplePreparedDecimalValue(context.Background(), floatParam)
	require.NoError(t, err)
	require.True(t, types.T(normalized.Typ.Id).IsDecimal())
	require.True(t, normalized.ExactDecimalParam)
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
