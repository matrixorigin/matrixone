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
