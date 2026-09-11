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

package compile

import (
	"context"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func hexCompatibilityExpr(overload int32, arg *plan.Expr) *plan.Expr {
	typ := types.New(types.T_varchar, 16, 0)
	return &plan.Expr{Typ: plan2.MakePlan2Type(&typ), Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.HEX, overload), ObjName: "hex"},
		Args: []*plan.Expr{arg},
	}}}
}

func TestHexProtocolPlanAdmissionAndCachedRun(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	expr := hexCompatibilityExpr(
		function.HexMySQLNumericOverloadStart,
		&plan.Expr{
			Typ: plan.Type{Id: int32(types.T_decimal64), Width: 3, Scale: 1},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_Decimal64Val{Decimal64Val: &plan.Decimal64{A: 155}},
			}},
		},
	)
	for _, tc := range []struct {
		name  string
		table *plan.TableDef
	}{
		{"check", &plan.TableDef{Checks: []*plan.CheckDef{{Check: expr}}}},
		{"default", &plan.TableDef{Cols: []*plan.ColDef{{Default: &plan.Default{Expr: expr}}}}},
		{"generated", &plan.TableDef{Cols: []*plan.ColDef{{GeneratedCol: &plan.GeneratedCol{Expr: expr}}}}},
		{"on_update", &plan.TableDef{Cols: []*plan.ColDef{{OnUpdate: &plan.OnUpdate{Expr: expr}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pn := &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{
				Definition: &plan.DataDefinition_CreateTable{CreateTable: &plan.CreateTable{TableDef: tc.table}},
			}}}
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion64)
			require.NoError(t, validateHexMySQLNumericProtocol(proc, pn))
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion63)
			c := &Compile{proc: proc, pn: pn}
			require.ErrorContains(t, c.Compile(context.Background(), pn, nil), "protocol version 64")
			_, err := c.Run(0)
			require.ErrorContains(t, err, "protocol version 64")
		})
	}

	for _, version := range []any{defines.MORPCVersion63, int64(62), nil, "63"} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		require.ErrorContains(t, validateHexMySQLNumericProtocol(proc, expr), "protocol version 64")
		require.NoError(t, validateHexMySQLNumericProtocol(
			proc, hexCompatibilityExpr(5, plan2.MakePlan2Float64ConstExprWithType(15.5))))
		require.NoError(t, validateHexMySQLNumericProtocol(proc, plan2.MakePlan2Int64ConstExprWithType(12)))
	}
	require.ErrorContains(t, validateHexMySQLNumericProtocol(nil, expr), "protocol version 64")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion64)
	require.Zero(t, testing.AllocsPerRun(100, func() {
		require.NoError(t, validateHexMySQLNumericProtocol(proc, expr))
	}))
}

func legacyHexCast(arg *plan.Expr, target types.Type, explicit bool) *plan.Expr {
	overload := int32(0)
	if explicit {
		overload = 1
	}
	return &plan.Expr{Typ: plan2.MakePlan2Type(&target), Expr: &plan.Expr_F{F: &plan.Function{
		Func:               &plan.ObjectRef{Obj: function.EncodeOverloadID(function.CAST, overload), ObjName: "cast"},
		Args:               []*plan.Expr{arg},
		SyntaxExplicitCast: explicit,
	}}}
}

func hexExprOverload(expr *plan.Expr) int32 {
	_, overload := function.DecodeOverloadID(expr.GetF().GetFunc().GetObj())
	return overload
}

func tableHexExpressions(table *plan.TableDef) []*plan.Expr {
	return []*plan.Expr{
		table.Checks[0].Check,
		table.Cols[0].Default.Expr,
		table.Cols[1].GeneratedCol.Expr,
		table.Cols[2].OnUpdate.Expr,
	}
}

func TestHexMigratesLegacyPersistedExpressionsAtVersion64(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)

	decimalValue, _, err := types.Parse128("9007199254740993")
	require.NoError(t, err)
	decimalArg := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_decimal128), Width: 38},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Decimal128Val{
			Decimal128Val: &plan.Decimal128{A: int64(decimalValue.B0_63), B: int64(decimalValue.B64_127)},
		}}},
	}
	legacyDecimal := hexCompatibilityExpr(5, legacyHexCast(decimalArg, types.T_float64.ToType(), false))
	catalogTable := &plan.TableDef{
		Checks: []*plan.CheckDef{{Check: plan2.DeepCopyExpr(legacyDecimal)}},
		Cols: []*plan.ColDef{
			{Default: &plan.Default{Expr: plan2.DeepCopyExpr(legacyDecimal)}},
			{GeneratedCol: &plan.GeneratedCol{Expr: plan2.DeepCopyExpr(legacyDecimal)}},
			{OnUpdate: &plan.OnUpdate{Expr: plan2.DeepCopyExpr(legacyDecimal)}},
		},
	}
	catalogPlan := &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{Definition: &plan.DataDefinition_CreateTable{CreateTable: &plan.CreateTable{TableDef: catalogTable}}}}}
	executionPlan := plan2.DeepCopyPlan(catalogPlan)
	executionTable := executionPlan.GetDdl().GetCreateTable().GetTableDef()

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion63)
	require.NoError(t, validateHexMySQLNumericProtocol(proc, executionPlan))
	for _, expr := range tableHexExpressions(executionTable) {
		require.Equal(t, int32(5), hexExprOverload(expr))
	}

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion64)
	require.NoError(t, validateHexMySQLNumericProtocol(proc, executionPlan))
	for _, expr := range tableHexExpressions(executionTable) {
		require.Equal(t, int32(9), hexExprOverload(expr))
		require.Equal(t, types.T_decimal128, types.T(expr.GetF().GetArgs()[0].Typ.Id))
	}
	for _, expr := range tableHexExpressions(catalogTable) {
		require.Equal(t, int32(5), hexExprOverload(expr), "catalog expression must stay unchanged")
	}
	require.NoError(t, validateHexMySQLNumericProtocol(proc, executionPlan))
	for _, expr := range tableHexExpressions(executionTable) {
		require.Equal(t, int32(9), hexExprOverload(expr), "migration must be idempotent")
	}

	migrated := tableHexExpressions(executionTable)[0]
	executor, err := colexec.NewExpressionExecutor(proc, migrated)
	require.NoError(t, err)
	defer executor.Free()
	out, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	require.NoError(t, err)
	require.Equal(t, "20000000000001", string(out.GetBytesAt(0)))

	legacyExplicitWithoutSyntax := legacyHexCast(
		plan2.MakePlan2Float64ConstExprWithType(15.5), types.T_float64.ToType(), true)
	legacyExplicitWithoutSyntax.GetF().SyntaxExplicitCast = false
	for _, tc := range []struct {
		name string
		expr *plan.Expr
		want int32
	}{
		{name: "ordinary_float32", expr: hexCompatibilityExpr(4, plan2.MakePlan2Float32ConstExprWithType(14.5)),
			want: function.HexFloat32Overload},
		{name: "ordinary_float64", expr: hexCompatibilityExpr(5, plan2.MakePlan2Float64ConstExprWithType(14.5)),
			want: function.HexFloat64Overload},
		{name: "explicit_float64", expr: hexCompatibilityExpr(5, legacyHexCast(
			plan2.MakePlan2Float64ConstExprWithType(15.5), types.T_float64.ToType(), true)),
			want: function.HexExplicitFloat64Overload},
		{name: "legacy_explicit_float64_without_syntax_flag",
			expr: hexCompatibilityExpr(5, legacyExplicitWithoutSyntax), want: function.HexExplicitFloat64Overload},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queryPlan := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{
				ProjectList: []*plan.Expr{tc.expr},
			}}}}}
			require.NoError(t, validateHexMySQLNumericProtocol(proc, queryPlan))
			require.Equal(t, tc.want, hexExprOverload(queryPlan.GetQuery().GetNodes()[0].GetProjectList()[0]))
		})
	}
}

func TestHexRemoteProtocolSenderAndReceiver(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	for _, id := range []int32{5, function.HexFloat64Overload, function.HexMySQLNumericOverloadStart} {
		project := projection.NewArgument()
		project.ProjectList = []*plan.Expr{
			hexCompatibilityExpr(id, plan2.MakePlan2Float64ConstExprWithType(15.5)),
		}
		scope := &Scope{Proc: proc, RootOp: project}

		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion63)
		legacyData, sendErr := encodeRemoteScope(scope, proc)
		_, localEncodeErr := encodeScope(scope)
		if id < function.HexMySQLNumericOverloadStart {
			require.NoError(t, sendErr)
			require.NoError(t, localEncodeErr)
			decoded, receiveErr := decodeScope(legacyData, proc, true, nil)
			require.NoError(t, receiveErr)
			require.NotNil(t, decoded)
		} else {
			require.ErrorContains(t, sendErr, "protocol version 64")
			require.ErrorContains(t, localEncodeErr, "protocol version 64")
		}

		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion64)
		data, err := encodeRemoteScope(scope, proc)
		require.NoError(t, err)
		decoded, err := decodeScope(data, proc, true, nil)
		require.NoError(t, err)
		require.NotNil(t, decoded)

		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion63)
		_, receiveErr := decodeScope(data, proc, true, nil)
		child := new(pipeline.Pipeline)
		require.NoError(t, child.Unmarshal(data))
		childErr := validateHexMySQLNumericProtocol(proc, &pipeline.Pipeline{Children: []*pipeline.Pipeline{child}})
		if id < function.HexMySQLNumericOverloadStart {
			require.NoError(t, receiveErr)
			require.NoError(t, childErr)
		} else {
			require.ErrorContains(t, receiveErr, "protocol version 64")
			require.ErrorContains(t, childErr, "protocol version 64")
		}
	}
}
