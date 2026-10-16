// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"encoding/json"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func foldedIntervalInt64(t *testing.T, expr *planpb.Expr) int64 {
	t.Helper()
	folded, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(expr), testutil.NewProcess(t), false, true)
	require.NoError(t, err)
	require.NotNil(t, folded.GetLit(), folded.String())
	return folded.GetLit().GetI64Val()
}

func TestCalendarIntervalTypedBinding(t *testing.T) {
	for _, oid := range []types.T{types.T_decimal64, types.T_decimal128, types.T_decimal256, types.T_float32, types.T_float64, types.T_char, types.T_varchar, types.T_text, types.T_any} {
		for _, unit := range []string{"DAY", "SECOND"} {
			t.Run(oid.String()+unit, func(t *testing.T) {
				source := &planpb.Expr{Typ: types.New(oid, 18, 7).PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}
				args, err := resetDateFunctionArgs(context.Background(), &planpb.Expr{Typ: types.T_date.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}, makeIntervalExpr(source, unit))
				require.NoError(t, err)
				require.Equal(t, "to_calendar_interval", args[1].GetF().Func.ObjName)
				wantSource := oid
				if oid == types.T_any {
					wantSource = types.T_varchar
				}
				require.Equal(t, int32(wantSource), args[1].GetF().Args[0].Typ.Id)
				if unit == "DAY" {
					require.Equal(t, int32(types.T_date), args[0].Typ.Id)
					require.Equal(t, int64(types.Day), args[2].GetLit().GetI64Val())
				} else {
					require.Equal(t, int32(types.T_datetime), args[0].Typ.Id)
					require.Equal(t, int64(types.MicroSecond), args[2].GetLit().GetI64Val())
				}
			})
		}
	}
}

func TestCalendarIntervalPreparedTimeKeepsLegacyNormalizer(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "prepare calendar_time from select date_add(cast('12:00:00' as time(6)), interval ? second)")
	require.NoError(t, err)
	p := prepared.GetDcl().GetPrepare().GetPlan()
	require.Nil(t, findPlanFunctionExpr(p, "to_calendar_interval"))
	for _, typ := range []types.Type{types.New(types.T_decimal64, 18, 7), types.T_float64.ToType()} {
		filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), p, []any{ParamValue{Value: "1.1234567", SourceType: typ, HasSourceType: true}})
		require.NoError(t, err)
		require.Nil(t, findPlanFunctionExpr(filled, "to_calendar_interval"))
	}
}

func TestCalendarIntervalPersistedAdmission(t *testing.T) {
	proc := testutil.NewProcess(t)
	expr, err := BindFuncExprImplByPlanExpr(proc.Ctx, "to_calendar_interval", []*planpb.Expr{makeVarcharColumnExpr(0, 0), makeInt64Const(int64(types.Day))})
	require.NoError(t, err)
	for _, col := range []*planpb.ColDef{
		{Default: &planpb.Default{Expr: expr}},
		{GeneratedCol: &planpb.GeneratedCol{Expr: expr, IsStored: true}},
	} {
		owner := &planpb.TableDef{Cols: []*planpb.ColDef{col}}
		floor, err := RequiredPersistedExpressionProtocolVersion(owner)
		require.NoError(t, err)
		require.Equal(t, defines.MORPCVersion115, floor)
		rt := moruntime.ServiceRuntime(proc.GetService())
		for _, key := range []string{moruntime.MOProtocolVersion, moruntime.PersistedExpressionProtocolFloor, moruntime.PersistedExpressionProtocolAuthoringFloor} {
			original, present := rt.GetGlobalVariables(key)
			t.Cleanup(func() {
				if present {
					rt.SetGlobalVariables(key, original)
				} else if current, ok := rt.GetGlobalVariables(key); ok {
					rt.CompareAndDeleteGlobalVariables(key, current)
				}
			})
		}

		for _, version := range []int64{defines.MORPCVersion114, defines.MORPCVersion115} {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion115)
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, version)
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, version)
			readErr := RequirePersistedExpressionProtocol(proc.Ctx, proc, owner)
			writeErr := RequirePersistedExpressionProtocolForAuthoring(proc.Ctx, proc, owner)
			if version < defines.MORPCVersion115 {
				require.ErrorContains(t, readErr, "115")
				require.ErrorContains(t, writeErr, "115")
			} else {
				require.NoError(t, readErr)
				require.NoError(t, writeErr)
			}
		}
	}
}

func requireDecimalCalendarProtocol(t *testing.T, expr *planpb.Expr) {
	t.Helper()
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.CalendarIntervalNormalization)
	require.True(t, features.DecimalLiteralSemantics, "source decimal provenance remains visible")
}

// A view can fold the entire arithmetic expression before serialization;
// authoring must still observe the helper while binding its function tree.
func TestCalendarIntervalViewAdmissionBeforeFold(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	proc := ctx.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldProtocol, hadProtocol := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldFloor, hadFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadProtocol {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, oldFloor)
		} else if value, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, value)
		}
	})
	const sql = "create view v_calendar as select adddate(date '2024-01-01',cast(1.5 as decimal(3,1))) as day_result"
	build := func(floor int64) (*Plan, error) {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion115)
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, floor)
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		if err != nil {
			return nil, err
		}
		defer stmt.Free()
		return BuildPlan(&rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: sql}, stmt, false)
	}
	_, err := build(defines.MORPCVersion114)
	require.ErrorContains(t, err, "protocol version 115")
	created, err := build(defines.MORPCVersion115)
	require.NoError(t, err)
	var data ViewData
	require.NoError(t, json.Unmarshal([]byte(created.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()), &data))
	require.NotNil(t, data.RequiredProtocolVersion)
	require.Equal(t, defines.MORPCVersion115, *data.RequiredProtocolVersion)
}
