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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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

func octCompatibilityExpr(overload int32, arg *plan.Expr) *plan.Expr {
	typ := types.T_decimal128.ToType()
	if overload >= function.OctStringOverloadStart {
		typ = types.New(types.T_varchar, 65, 0)
	}
	return &plan.Expr{Typ: plan2.MakePlan2Type(&typ), Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.OCT, overload), ObjName: "oct"},
		Args: []*plan.Expr{arg},
	}}}
}

func TestOctLegacySerializedExpression(t *testing.T) {
	// Explicit old Obj AND old Typ reproduce the catalog ABI. Resolving the
	// return type from today's registry would hide the original wrapper panic.
	for _, tc := range []struct {
		name  string
		id    int32
		arg   *plan.Expr
		want  string
		fails bool
	}{
		{"integer", 7, plan2.MakePlan2Int64ConstExprWithType(-1), "1777777777777777777777", false},
		{"float_rounding", 9, &plan.Expr{Typ: plan.Type{Id: int32(types.T_float64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dval{Dval: 12.6}}}}, "15", false},
		{"date_string", 12, plan2.MakePlan2StringConstExprWithType("2007-08-02"), "3727", false},
		{"invalid_string", 12, plan2.MakePlan2StringConstExprWithType("12tail"), "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			for _, null := range []bool{false, true} {
				arg := plan2.DeepCopyExpr(tc.arg)
				arg.GetLit().Isnull = null
				if null {
					arg.GetLit().LiteralForm = plan.StringLiteralForm_STRING_LITERAL_NONE
				}
				original := octCompatibilityExpr(tc.id, arg)
				data, err := original.Marshal()
				require.NoError(t, err)
				restored := new(plan.Expr)
				require.NoError(t, restored.Unmarshal(data))
				executor, err := colexec.NewExpressionExecutor(proc, restored)
				require.NoError(t, err)
				func() {
					defer executor.Free()
					out, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
					if tc.fails && !null {
						require.Error(t, err)
						return
					}
					require.NoError(t, err)
					value, isNull := vector.GenerateFunctionFixedTypeParameter[types.Decimal128](out).GetValue(0)
					require.Equal(t, null, isNull)
					if !null {
						require.Equal(t, tc.want, value.Format(0))
					}
				}()
			}
		})
	}
}

func TestOctProtocolPlanAdmissionAndCachedRun(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	expr := octCompatibilityExpr(function.OctStringOverloadStart+7, plan2.MakePlan2Int64ConstExprWithType(12))
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
			pn := &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{Definition: &plan.DataDefinition_CreateTable{CreateTable: &plan.CreateTable{TableDef: tc.table}}}}}
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion62)
			require.NoError(t, validateOctStringProtocol(proc, pn))
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion58)
			c := &Compile{proc: proc, pn: pn}
			require.ErrorContains(t, c.Compile(context.Background(), pn, nil), "protocol version 62")
			_, err := c.Run(0)
			require.ErrorContains(t, err, "protocol version 62")
		})
	}
	for _, version := range []any{int64(58), defines.MORPCVersion59, defines.MORPCVersion60, defines.MORPCVersion61, nil, "61"} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		require.ErrorContains(t, validateOctStringProtocol(proc, expr), "protocol version 62")
		require.NoError(t, validateOctStringProtocol(proc, octCompatibilityExpr(7, plan2.MakePlan2Int64ConstExprWithType(12))))
		require.NoError(t, validateOctStringProtocol(proc, plan2.MakePlan2Int64ConstExprWithType(12)))
	}
	require.ErrorContains(t, validateOctStringProtocol(nil, expr), "protocol version 62")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion62)
	require.Zero(t, testing.AllocsPerRun(100, func() { require.NoError(t, validateOctStringProtocol(proc, expr)) }))
}

func TestOctRemoteProtocolSenderAndReceiver(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	for _, id := range []int32{7, function.OctStringOverloadStart + 7} {
		project := projection.NewArgument()
		project.ProjectList = []*plan.Expr{octCompatibilityExpr(id, plan2.MakePlan2Int64ConstExprWithType(12))}
		scope := &Scope{Proc: proc, RootOp: project}
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion62)
		data, err := encodeRemoteScope(scope, proc)
		require.NoError(t, err)
		decoded, err := decodeScope(data, proc, true, nil)
		require.NoError(t, err)
		require.NotNil(t, decoded)
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion58)
		_, sendErr := encodeRemoteScope(scope, proc)
		_, localEncodeErr := encodeScope(scope)
		_, receiveErr := decodeScope(data, proc, true, nil)
		if id < function.OctStringOverloadStart {
			require.NoError(t, sendErr)
			require.NoError(t, localEncodeErr)
			require.NoError(t, receiveErr)
		} else {
			require.ErrorContains(t, sendErr, "protocol version 62")
			require.ErrorContains(t, localEncodeErr, "protocol version 62")
			require.ErrorContains(t, receiveErr, "protocol version 62")
			child := new(pipeline.Pipeline)
			require.NoError(t, child.Unmarshal(data))
			require.ErrorContains(t, validateOctStringProtocol(proc, &pipeline.Pipeline{Children: []*pipeline.Pipeline{child}}), "protocol version 62")
		}
	}
}
