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
	"encoding/json"
	"fmt"
	"testing"

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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// Execute decoded expressions through the actual consumer that allocates the
// result wrapper from Expr.Typ, including historical persisted narrow vectors.
func TestDateNumericSerializedExecution(t *testing.T) {
	for _, tc := range []struct {
		name         string
		id, overload int32
		input        types.T
		legacy       types.T
		want         int64
	}{
		{"week date", function.WEEK, 0, types.T_date, types.T_uint8, 52},
		{"week datetime", function.WEEK, 1, types.T_datetime, types.T_uint8, 52},
		{"week date mode", function.WEEK, 2, types.T_date, types.T_uint8, 1},
		{"week datetime mode", function.WEEK, 3, types.T_datetime, types.T_uint8, 1},
		{"dayofyear", function.DAYOFYEAR, 0, types.T_date, types.T_uint16, 366},
	} {
		for _, resultType := range []types.T{types.T_int32, tc.legacy} {
			t.Run(fmt.Sprintf("%s/%s", tc.name, resultType), func(t *testing.T) {
				proc := testutil.NewProcess(t)
				proc.Base.SessionInfo.DefaultWeekFormat = 0
				proc.Base.SessionInfo.DefaultWeekFormatSet = true
				input := batch.NewWithSize(1)
				defer input.Clean(proc.Mp())
				input.Vecs[0] = vector.NewVec(tc.input.ToType())
				dates := []types.Date{types.DateFromCalendar(2024, 12, 31), types.ZeroDate, types.DateFromCalendar(2025, 1, 1)}
				for i, d := range dates {
					if tc.input == types.T_date {
						require.NoError(t, vector.AppendFixed(input.Vecs[0], d, i == 2, proc.Mp()))
					} else {
						require.NoError(t, vector.AppendFixed(input.Vecs[0], d.ToDatetime(), i == 2, proc.Mp()))
					}
				}
				input.SetRowCount(3)
				args := []*planpb.Expr{{Typ: planpb.Type{Id: int32(tc.input)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}}
				if tc.id == function.WEEK && tc.overload >= 2 {
					args = append(args, MakePlan2Int64ConstExprWithType(3))
				}
				expr := &planpb.Expr{Typ: planpb.Type{Id: int32(resultType)}, Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(tc.id, tc.overload)}, Args: args}}}
				data, err := expr.Marshal()
				require.NoError(t, err)
				decoded := &planpb.Expr{}
				require.NoError(t, decoded.Unmarshal(data))
				require.Equal(t, function.EncodeOverloadID(tc.id, tc.overload), decoded.GetF().Func.Obj)
				features, err := planpb.RequiredRemoteExpressionFeatures(decoded)
				require.NoError(t, err)
				require.Equal(t, resultType == types.T_int32, features.DateNumericResultContracts)
				executor, err := colexec.NewExpressionExecutor(proc, decoded)
				require.NoError(t, err)
				defer executor.Free()
				for _, mask := range [][]bool{nil, {false, true, true}, nil} {
					result, err := executor.Eval(proc, []*batch.Batch{input}, mask)
					require.NoError(t, err)
					require.Equal(t, resultType, result.GetType().Oid)
					var got int64
					switch resultType {
					case types.T_int32:
						got = int64(vector.GetFixedAtWithTypeCheck[int32](result, 0))
					case types.T_uint8:
						got = int64(vector.GetFixedAtWithTypeCheck[uint8](result, 0))
					case types.T_uint16:
						got = int64(vector.GetFixedAtWithTypeCheck[uint16](result, 0))
					}
					if mask == nil {
						require.False(t, result.IsNull(0))
						require.Equal(t, tc.want, got)
					} else {
						require.True(t, result.IsNull(0), "masked valid row")
					}
					require.True(t, result.IsNull(1))
					require.True(t, result.IsNull(2))
				}
			})
		}
	}
}

func TestDateNumericPersistedAdmissionAndFoldedView(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	proc := ctx.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldFloor, hadFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		} else if current, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, current)
		}
		if hadFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, oldFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, current)
		}
	})
	expr := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int32)}, Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.DAYOFYEAR, 0)}, Args: []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_date)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}}}}}
	owner := &planpb.TableDef{Cols: []*planpb.ColDef{{GeneratedCol: &planpb.GeneratedCol{Expr: expr}}}}
	required, err := RequiredPersistedExpressionProtocolVersion(owner)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion112, required)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion111)
	require.ErrorContains(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, owner), "protocol version 112")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion112)
	require.NoError(t, RequirePersistedExpressionProtocol(proc.Ctx, proc, owner))
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion111)
	require.ErrorContains(t, RequirePersistedExpressionProtocolForAuthoring(proc.Ctx, proc, owner), "protocol version 112")
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion112)
	require.NoError(t, RequirePersistedExpressionProtocolForAuthoring(proc.Ctx, proc, owner))
	expr.Typ.Id = int32(types.T_uint16)
	required, err = RequiredPersistedExpressionProtocolVersion(owner)
	require.NoError(t, err)
	require.NotEqual(t, defines.MORPCVersion112, required, "legacy serialized ABI remains supported")

	for _, call := range []string{"week('2024-12-31',3)", "dayofyear('2024-12-31')"} {
		t.Run(call, func(t *testing.T) {
			createSQL := "create view v_calendar as select " + call + " as v"
			build := func(floor int64) (*Plan, error) {
				rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, floor)
				stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, createSQL, 1)
				if err != nil {
					return nil, err
				}
				defer stmt.Free()
				return BuildPlan(&rootSQLCompilerContext{MockCompilerContext: ctx, rootSQL: createSQL}, stmt, false)
			}
			_, err := build(defines.MORPCVersion111)
			require.ErrorContains(t, err, "protocol version 112")
			created, err := build(defines.MORPCVersion112)
			require.NoError(t, err)
			var view ViewData
			require.NoError(t, json.Unmarshal([]byte(created.GetDdl().GetCreateView().GetTableDef().GetViewSql().GetView()), &view))
			require.NotNil(t, view.RequiredProtocolVersion)
			require.Equal(t, defines.MORPCVersion112, *view.RequiredProtocolVersion, "pre-fold observation fences the stored SQL")
		})
	}
}
