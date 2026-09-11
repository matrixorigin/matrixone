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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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

func TestHexRemoteProtocolSenderAndReceiver(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	for _, id := range []int32{5, function.HexMySQLNumericOverloadStart} {
		project := projection.NewArgument()
		project.ProjectList = []*plan.Expr{
			hexCompatibilityExpr(id, plan2.MakePlan2Float64ConstExprWithType(15.5)),
		}
		scope := &Scope{Proc: proc, RootOp: project}
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion64)
		data, err := encodeRemoteScope(scope, proc)
		require.NoError(t, err)
		decoded, err := decodeScope(data, proc, true, nil)
		require.NoError(t, err)
		require.NotNil(t, decoded)

		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion63)
		_, sendErr := encodeRemoteScope(scope, proc)
		_, localEncodeErr := encodeScope(scope)
		_, receiveErr := decodeScope(data, proc, true, nil)
		if id < function.HexMySQLNumericOverloadStart {
			require.NoError(t, sendErr)
			require.NoError(t, localEncodeErr)
			require.NoError(t, receiveErr)
			continue
		}
		require.ErrorContains(t, sendErr, "protocol version 64")
		require.ErrorContains(t, localEncodeErr, "protocol version 64")
		require.ErrorContains(t, receiveErr, "protocol version 64")
		child := new(pipeline.Pipeline)
		require.NoError(t, child.Unmarshal(data))
		require.ErrorContains(t,
			validateHexMySQLNumericProtocol(proc, &pipeline.Pipeline{Children: []*pipeline.Pipeline{child}}),
			"protocol version 64")
	}
}
