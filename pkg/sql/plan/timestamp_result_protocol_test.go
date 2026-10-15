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
	"encoding/json"
	"fmt"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// A decoded expression owns the physical result type, independently of binding.
func TestTimestampSerializedResultContracts(t *testing.T) {
	for _, zone := range []*time.Location{time.UTC, time.FixedZone("+08", 8*60*60)} {
		for oid, inputType := range []types.T{types.T_date, types.T_datetime, types.T_timestamp, types.T_varchar, types.T_char} {
			for _, legacy := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/%s/legacy%t", zone, inputType, legacy), func(t *testing.T) {
					proc := newPlanTestProcess(t)
					proc.GetSessionInfo().TimeZone = zone
					dt, err := types.ParseDatetime("2024-02-29 12:34:56.123456", 6)
					require.NoError(t, err)
					if inputType == types.T_date {
						dt = dt.ToDate().ToDatetime()
					}
					input := batch.NewWithSize(1)
					defer input.Clean(proc.Mp())
					input.Vecs[0] = vector.NewVec(inputType.ToTypeWithScale(6))
					for _, isNull := range []bool{false, true} {
						switch inputType {
						case types.T_date:
							err = vector.AppendFixed(input.Vecs[0], dt.ToDate(), isNull, proc.Mp())
						case types.T_datetime:
							err = vector.AppendFixed(input.Vecs[0], dt, isNull, proc.Mp())
						case types.T_timestamp:
							err = vector.AppendFixed(input.Vecs[0], dt.ToTimestamp(zone), isNull, proc.Mp())
						default:
							err = vector.AppendBytes(input.Vecs[0], []byte(dt.String2(6)), isNull, proc.Mp())
						}
						require.NoError(t, err)
					}
					input.SetRowCount(2)
					col := &planpb.Expr{Typ: input.Vecs[0].GetType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
					expr, err := BindFuncExprImplByPlanExpr(proc.Ctx, "timestamp", []*planpb.Expr{col})
					require.NoError(t, err)
					require.Equal(t, function.EncodeOverloadID(function.TIMESTAMP, int32(oid+6)), expr.GetF().Func.Obj)
					if legacy {
						expr.GetF().Func.Obj = function.EncodeOverloadID(function.TIMESTAMP, int32(oid))
						expr.Typ = types.T_timestamp.ToTypeWithScale(6).PlanType()
					}
					data, err := expr.Marshal()
					require.NoError(t, err)
					decoded := &planpb.Expr{}
					require.NoError(t, decoded.Unmarshal(data))
					features, err := planpb.RequiredRemoteExpressionFeatures(decoded)
					require.NoError(t, err)
					require.False(t, features.InvalidTemporalResultContract)
					require.Equal(t, !legacy, features.TimestampDatetimeResultContracts)
					exec, err := colexec.NewExpressionExecutor(proc, decoded)
					require.NoError(t, err)
					defer exec.Free()
					for _, mask := range [][]bool{nil, {false, true}, nil} {
						result, err := exec.Eval(proc, []*batch.Batch{input}, mask)
						require.NoError(t, err)
						require.True(t, result.IsNull(1))
						if mask != nil {
							require.True(t, result.IsNull(0))
							continue
						}
						require.False(t, result.IsNull(0))
						if legacy {
							require.Equal(t, types.T_timestamp, result.GetType().Oid)
							require.Equal(t, dt.ToTimestamp(zone), vector.GetFixedAtWithTypeCheck[types.Timestamp](result, 0))
						} else {
							require.Equal(t, types.T_datetime, result.GetType().Oid)
							require.Equal(t, dt, vector.GetFixedAtWithTypeCheck[types.Datetime](result, 0))
						}
					}
				})
			}
		}
	}
}

func TestTimestampRejectsMismatchedSerializedResults(t *testing.T) {
	proc := newPlanTestProcess(t)
	for _, oid := range []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		want := types.T_datetime
		if oid >= 5 {
			want = types.T_timestamp
		}
		expr := &planpb.Expr{Typ: want.ToType().PlanType(), Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.TIMESTAMP, oid)}}}}
		_, err := RequiredPersistedExpressionProtocolVersion(expr)
		require.ErrorContains(t, err, "requires rebinding")
		exec, err := colexec.NewExpressionExecutor(proc, expr)
		require.ErrorContains(t, err, "requires rebinding")
		require.Nil(t, exec, "reject before allocating a result wrapper")
	}
}

func TestTimestampPersistedAdmissionAndFoldedView(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	proc := ctx.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	keys := []string{moruntime.MOProtocolVersion, moruntime.PersistedExpressionProtocolFloor, moruntime.PersistedExpressionProtocolAuthoringFloor}
	for _, key := range keys {
		value, had := rt.GetGlobalVariables(key)
		t.Cleanup(func() {
			if had {
				rt.SetGlobalVariables(key, value)
			} else if current, ok := rt.GetGlobalVariables(key); ok {
				rt.CompareAndDeleteGlobalVariables(key, current)
			}
		})
	}
	expr, err := BindFuncExprImplByPlanExpr(proc.Ctx, "timestamp", []*planpb.Expr{{Typ: types.T_datetime.ToTypeWithScale(3).PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}})
	require.NoError(t, err)
	owners := map[string]any{
		"default":                              &planpb.TableDef{Cols: []*planpb.ColDef{{Default: &planpb.Default{Expr: expr}}}},
		"on update":                            &planpb.TableDef{Cols: []*planpb.ColDef{{OnUpdate: &planpb.OnUpdate{Expr: expr}}}},
		"generated or functional index column": &planpb.TableDef{Cols: []*planpb.ColDef{{GeneratedCol: &planpb.GeneratedCol{Expr: expr}}}},
		"check":                                &planpb.TableDef{Checks: []*planpb.CheckDef{{Check: expr}}},
		"view query":                           &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}},
	}
	for name, owner := range owners {
		t.Run(name, func(t *testing.T) {
			floor, err := RequiredPersistedExpressionProtocolVersion(owner)
			require.NoError(t, err)
			require.Equal(t, defines.MORPCVersion114, floor)
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion114)
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, defines.MORPCVersion113)
			require.ErrorContains(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, owner), "version 114")
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, defines.MORPCVersion114)
			require.NoError(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, owner))
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion113)
			require.ErrorContains(t, RequirePersistedExpressionProtocolForAuthoring(proc.Ctx, proc, owner), "version 114")
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion114)
			require.NoError(t, RequirePersistedExpressionProtocolForAuthoring(proc.Ctx, proc, owner))
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion113)
			require.ErrorContains(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, owner), "version 114", "rollback fails closed even if a newer floor remains")
		})
	}
	legacy := DeepCopyExpr(expr)
	legacy.GetF().Func.Obj = function.EncodeOverloadID(function.TIMESTAMP, 1)
	legacy.Typ = types.T_timestamp.ToType().PlanType()
	required, err := RequiredPersistedExpressionProtocolVersion(legacy)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion98, required)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion113)
	require.NoError(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, legacy))

	const sql = "create view v_timestamp_contract as select timestamp('2024-02-29 12:34:56.123') as v"
	build := func(floor int64) (*Plan, error) {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion114)
		rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, floor)
		stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
		if err != nil {
			return nil, err
		}
		defer stmt.Free()
		return BuildPlan(&rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: sql}, stmt, false)
	}
	_, err = build(defines.MORPCVersion113)
	require.ErrorContains(t, err, "version 114")
	created, err := build(defines.MORPCVersion114)
	require.NoError(t, err)
	var view ViewData
	require.NoError(t, json.Unmarshal([]byte(created.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()), &view))
	require.NotNil(t, view.RequiredProtocolVersion)
	require.Equal(t, defines.MORPCVersion114, *view.RequiredProtocolVersion, "pre-fold observation survives stored SQL replay")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion113)
	require.ErrorContains(t, RequirePersistedProtocolVersion(proc.Ctx, proc, *view.RequiredProtocolVersion), "version 114")
}
