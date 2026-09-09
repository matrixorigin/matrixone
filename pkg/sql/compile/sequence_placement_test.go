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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func sequenceExprForCompileTest(fid int32) *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(fid, 0)},
	}}}
}

func TestSequenceBearingQueryUsesOneCNExecType(t *testing.T) {
	sequence := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.NEXTVAL, 0)},
	}}}
	qry := &plan.Query{Nodes: []*plan.Node{{ProjectList: []*plan.Expr{sequence}}}}

	require.Equal(t, plan2.ExecTypeAP_ONECN, sequenceExecType(plan2.ExecTypeAP_MULTICN, qry))
	require.Equal(t, plan2.ExecTypeAP_ONECN, sequenceExecType(plan2.ExecTypeAP_ONECN, qry))
	require.Equal(t, plan2.ExecTypeTP, sequenceExecType(plan2.ExecTypeTP, qry))
	require.Equal(t, plan2.ExecTypeAP_MULTICN, sequenceExecType(plan2.ExecTypeAP_MULTICN, &plan.Query{}))
}

func TestLastInsertIDExprUsesOneCNExecType(t *testing.T) {
	expr := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.LAST_INSERT_ID, function.LastInsertIDExprOverload)},
	}}}
	qry := &plan.Query{Nodes: []*plan.Node{{ProjectList: []*plan.Expr{expr}}}}

	require.Equal(t, plan2.ExecTypeAP_ONECN, sequenceExecType(plan2.ExecTypeAP_MULTICN, qry))
	require.Equal(t, plan2.ExecTypeAP_MULTICN, sequenceExecType(plan2.ExecTypeAP_MULTICN, &plan.Query{Nodes: []*plan.Node{{
		ProjectList: []*plan.Expr{sequenceExprForCompileTest(function.LAST_INSERT_ID)},
	}}}))
}

func TestSequenceBearingQueryRejectsNonCoordinatorScope(t *testing.T) {
	sequence := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.NEXTVAL, 0)},
	}}}
	qry := &plan.Query{Nodes: []*plan.Node{{ProjectList: []*plan.Expr{sequence}}}}
	coordinator := engine.Node{Id: "cn-local", Addr: "127.0.0.1:6001"}

	require.NoError(t, validateSequenceScopePlacement(qry, coordinator, []*Scope{{
		NodeInfo: coordinator,
		PreScopes: []*Scope{{
			NodeInfo: engine.Node{Addr: coordinator.Addr, Mcpu: 4},
		}},
	}}))
	require.Error(t, validateSequenceScopePlacement(qry, coordinator, []*Scope{{
		NodeInfo: coordinator,
		PreScopes: []*Scope{{
			NodeInfo: engine.Node{Id: "cn-remote", Addr: "127.0.0.1:6002"},
		}},
	}}))
}

func TestSequenceFreeQueryDoesNotRequireCoordinatorScopes(t *testing.T) {
	qry := &plan.Query{Nodes: []*plan.Node{{ProjectList: []*plan.Expr{{
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.ABS, 0)},
		}},
	}}}}}
	coordinator := engine.Node{Id: "cn-local", Addr: "127.0.0.1:6001"}
	require.NoError(t, validateSequenceScopePlacement(qry, coordinator, []*Scope{{
		NodeInfo: engine.Node{Id: "cn-remote", Addr: "127.0.0.1:6002"},
	}}))
}

func TestReadOfTableDefaultDoesNotForceSequencePlacement(t *testing.T) {
	qry := &plan.Query{
		Steps: []int32{0},
		Nodes: []*plan.Node{{
			NodeType: plan.Node_TABLE_SCAN,
			ProjectList: []*plan.Expr{{
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 0}},
			}},
			TableDef: &plan.TableDef{Cols: []*plan.ColDef{{
				Name:    "id",
				Default: &plan.Default{Expr: sequenceExprForCompileTest(function.NEXTVAL)},
			}}},
		}},
	}
	require.Equal(t, plan2.ExecTypeAP_MULTICN, sequenceExecType(plan2.ExecTypeAP_MULTICN, qry))
}

func TestSequencePlacementPreservesLocalDOP(t *testing.T) {
	qry := &plan.Query{
		Steps: []int32{0},
		Nodes: []*plan.Node{{
			ProjectList: []*plan.Expr{sequenceExprForCompileTest(function.NEXTVAL)},
			Stats: &plan.Stats{
				// More than one DOP unit is intentional: this is the minimum
				// multi-block shape that proves the sequence cap does not turn
				// local parallel evaluation into a single worker.
				BlockNum: 32,
			},
		}},
	}
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: qry}}
	plan2.CalcQueryDOP(p, 4, 1, sequenceExecType(plan2.ExecTypeAP_MULTICN, qry))
	require.Equal(t, int32(3), qry.Nodes[0].Stats.Dop)
}

func TestSequencePlacementPinsCurrentCNAndKeepsSequenceFreeMultiCN(t *testing.T) {
	local := schedule.Worker{ID: "cn-local", Addr: "127.0.0.1:6001", Mcpu: 8}
	remote := schedule.Worker{ID: "cn-remote", Addr: "127.0.0.1:6002", Mcpu: 8}
	sequence := &plan.Query{Nodes: []*plan.Node{{ProjectList: []*plan.Expr{sequenceExprForCompileTest(function.NEXTVAL)}}}}

	sequenceDecision := schedule.DecideQueryPlacement(schedule.QueryRequest{
		ExecKind:   toScheduleExecKind(sequenceExecType(plan2.ExecTypeAP_MULTICN, sequence)),
		CurrentCN:  local,
		Candidates: schedule.Workers{local, remote},
	})
	require.True(t, sequenceDecision.Satisfied)
	require.Equal(t, schedule.ReasonLocalExecType, sequenceDecision.Reason)
	require.Equal(t, schedule.Workers{local}, sequenceDecision.Workers)

	sequenceFree := &plan.Query{Nodes: []*plan.Node{{ProjectList: []*plan.Expr{sequenceExprForCompileTest(function.ABS)}}}}
	controlDecision := schedule.DecideQueryPlacement(schedule.QueryRequest{
		ExecKind:   toScheduleExecKind(sequenceExecType(plan2.ExecTypeAP_MULTICN, sequenceFree)),
		CurrentCN:  local,
		Candidates: schedule.Workers{local, remote},
	})
	require.True(t, controlDecision.Satisfied)
	require.Equal(t, schedule.ReasonMultiCN, controlDecision.Reason)
	require.Len(t, controlDecision.Workers, 2)
}

func TestSequencePlacementUsesCompileSchedulerForTwoCNControl(t *testing.T) {
	local := engine.Node{Id: "cn-local", Addr: "local:6001", Mcpu: 8}
	remote := engine.Node{Id: "cn-remote", Addr: "remote:6001", Mcpu: 8}
	sequence := &plan.Query{
		Steps: []int32{0},
		Nodes: []*plan.Node{{ProjectList: []*plan.Expr{sequenceExprForCompileTest(function.NEXTVAL)}}},
	}

	sequenceCompile := NewMockCompile(t)
	sequenceCompile.addr = local.Addr
	sequenceCompile.ncpu = 8
	sequenceCompile.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: sequence}}
	sequenceCompile.execType = sequenceExecType(plan2.ExecTypeAP_MULTICN, sequence)
	sequenceCompile.e = &schedulerTestEngine{nodes: engine.Nodes{local, remote}}
	sequenceNodes, err := sequenceCompile.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, plan2.ExecTypeAP_ONECN, sequenceCompile.execType)
	require.Equal(t, engine.Nodes{{Addr: local.Addr, Mcpu: local.Mcpu}}, sequenceNodes)

	control := &plan.Query{
		Steps: []int32{0},
		Nodes: []*plan.Node{{ProjectList: []*plan.Expr{sequenceExprForCompileTest(function.ABS)}}},
	}
	controlCompile := NewMockCompile(t)
	controlCompile.addr = local.Addr
	controlCompile.ncpu = 8
	controlCompile.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: control}}
	controlCompile.execType = sequenceExecType(plan2.ExecTypeAP_MULTICN, control)
	controlCompile.e = &schedulerTestEngine{nodes: engine.Nodes{local, remote}}
	controlNodes, err := controlCompile.scheduleQueryWorkers()
	require.NoError(t, err)
	require.Equal(t, plan2.ExecTypeAP_MULTICN, controlCompile.execType)
	require.Len(t, controlNodes, 2)
}

func TestSequencePlacementOverridesForcedMultiCN(t *testing.T) {
	sequence := &plan.Query{Nodes: []*plan.Node{{ProjectList: []*plan.Expr{sequenceExprForCompileTest(function.NEXTVAL)}}}}
	plan2.SetForceScanOnMultiCN(true)
	defer plan2.SetForceScanOnMultiCN(false)

	require.Equal(t, plan2.ExecTypeAP_MULTICN, plan2.GetExecType(sequence, false, false))
	require.Equal(t, plan2.ExecTypeAP_ONECN, sequenceExecType(plan2.GetExecType(sequence, false, false), sequence))
}
