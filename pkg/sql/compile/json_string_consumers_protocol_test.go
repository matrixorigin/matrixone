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

package compile

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func bindJSONStringConsumerExpr(t *testing.T, name string) *planpb.Expr {
	t.Helper()
	jsonColumn := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_json)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}
	stringConstant := func(value string) *planpb.Expr {
		return plan2.MakePlan2StringConstExprWithType(value)
	}

	var args []*planpb.Expr
	switch name {
	case "concat":
		args = []*planpb.Expr{jsonColumn, stringConstant("!")}
	case "concat_ws":
		args = []*planpb.Expr{stringConstant("|"), jsonColumn, stringConstant("!")}
	case "elt":
		args = []*planpb.Expr{plan2.MakePlan2Int64ConstExprWithType(1), jsonColumn, stringConstant("!")}
	default:
		t.Fatalf("unsupported JSON string consumer %q", name)
	}

	expr, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), name, args)
	require.NoError(t, err)
	_, overloadID := function.DecodeOverloadID(expr.GetF().Func.Obj)
	require.Equal(t, int32(1), overloadID)
	return expr
}

func jsonStringConsumerCreateTablePlan(expr *planpb.Expr, owner string) *planpb.Plan {
	tableDef := &planpb.TableDef{}
	switch owner {
	case "check":
		tableDef.Checks = []*planpb.CheckDef{{Check: expr}}
	case "default":
		tableDef.Cols = []*planpb.ColDef{{Default: &planpb.Default{Expr: expr}}}
	case "generated":
		tableDef.Cols = []*planpb.ColDef{{GeneratedCol: &planpb.GeneratedCol{Expr: expr}}}
	case "on_update":
		tableDef.Cols = []*planpb.ColDef{{OnUpdate: &planpb.OnUpdate{Expr: expr}}}
	}
	return &planpb.Plan{Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{
		Definition: &planpb.DataDefinition_CreateTable{CreateTable: &planpb.CreateTable{TableDef: tableDef}},
	}}}
}

func TestJSONStringConsumerProtocolPlanAdmissionAndCachedRun(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion86)
		}
	})

	for _, name := range []string{"concat", "concat_ws", "elt"} {
		t.Run(name, func(t *testing.T) {
			expr := bindJSONStringConsumerExpr(t, name)
			for _, owner := range []string{"check", "default", "generated", "on_update"} {
				t.Run(owner, func(t *testing.T) {
					queryPlan := jsonStringConsumerCreateTablePlan(expr, owner)
					rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion86)
					require.NoError(t, validateJSONStringConsumerProtocol(proc, queryPlan))

					rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion75)
					c := &Compile{proc: proc, pn: queryPlan}
					require.ErrorContains(t,
						c.Compile(context.Background(), queryPlan, nil),
						"protocol version 76")
					_, err := c.Run(0)
					require.ErrorContains(t, err, "protocol version 76")
				})
			}
		})
	}
}

func TestJSONStringConsumerRollingUpgradeFencesOldWorker(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	expr := bindJSONStringConsumerExpr(t, "concat")
	qry := &planpb.Query{
		Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}},
		Steps: []int32{0},
	}
	project := projection.NewArgument()
	defer project.Release()
	project.ProjectList = []*planpb.Expr{expr}
	scope := &Scope{
		Magic:    Remote,
		Proc:     c.proc,
		NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"},
		RootOp:   project,
	}
	workers := engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}

	// New coordinator + old-head worker: placement must fall back before a
	// pipeline carrying overload 1 can be sent to the old worker.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion86)
	client.version = defines.MORPCVersion75
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = workers
	require.NoError(t, c.constrainJSONStringConsumerWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
	_, err := encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination")

	// The same new coordinator may use the distributed path once the selected
	// worker reports the new capability.
	client.version = defines.MORPCVersion86
	c.execType = plan2.ExecTypeAP_MULTICN
	c.cnList = workers
	require.NoError(t, c.constrainJSONStringConsumerWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	data, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// An old-head receiver rejects the already serialized overload instead of
	// reaching GetFunctionById, while a v76 receiver accepts and executes it.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion75)
	_, err = decodeScope(data, c.proc, true, nil)
	require.ErrorContains(t, err, "protocol version 76")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion86)
	decoded, err := decodeScope(data, c.proc, true, nil)
	require.NoError(t, err)
	require.NotNil(t, decoded)
	require.Positive(t, client.calls)
	require.Equal(t, client.calls, client.releases)
}
