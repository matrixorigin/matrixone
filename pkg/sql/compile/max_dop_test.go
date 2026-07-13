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
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	ivfflatplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

func TestForceSingleScanDistinctAgg(t *testing.T) {
	node := &plan.Node{
		AggList: []*plan.Expr{
			{
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{
							ObjName: "sum",
						},
						Args: []*plan.Expr{
							{
								Expr: &plan.Expr_Lit{
									Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}},
								},
							},
						},
					},
				},
			},
		},
	}
	node.AggList[0].Expr.(*plan.Expr_F).F.Func.Obj = -9223372036854775808

	require.True(t, forceSingleScan(node))
}

func TestToScheduledQueryWorkersKeepsLocalIdentityWithoutRoute(t *testing.T) {
	workers := toScheduledQueryWorkers(engine.Nodes{
		{Id: "cn-local", Mcpu: 4},
		{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: 8},
		{},
	})

	require.Equal(t, schedule.Workers{
		{ID: "cn-local", Mcpu: 4},
		{ID: "cn-remote", Addr: "cn-remote:6001", Mcpu: 8},
	}, workers)
}

func TestToScheduledQueryWorkersKeepsLocalSentinel(t *testing.T) {
	workers := toScheduledQueryWorkers(engine.Nodes{
		{Mcpu: 1},
		{},
	})

	require.Equal(t, schedule.Workers{{Mcpu: 1}}, workers)
}

func TestScheduledQueryWorkersDoesNotMaterializeLocalRoute(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "cn-local:6001"
	c.cnList = engine.Nodes{
		{Id: "cn-local", Mcpu: 4},
	}

	require.Equal(t, schedule.Workers{{ID: "cn-local", Mcpu: 4}}, c.scheduledQueryWorkers())
}

func TestMaterializeScheduledWorkerUsesLocalRouteAtExecutionBoundary(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "cn-local:6001"

	node := c.materializeScheduledWorker(schedule.Worker{ID: "cn-local", Mcpu: 4})
	require.Equal(t, engine.Node{Id: "cn-local", Addr: "cn-local:6001", Mcpu: 4}, node)

	remote := c.materializeScheduledWorker(schedule.Worker{ID: "cn-remote", Addr: "cn-remote:6001", Mcpu: 8})
	require.Equal(t, engine.Node{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: 8}, remote)
}

func TestShouldWarnScanPlacement(t *testing.T) {
	for _, reason := range []string{
		schedule.ReasonScanNoWorkers,
		schedule.ReasonScanMissingStats,
		schedule.ReasonScanSingleWorker,
		schedule.ReasonScanQueryLocalExec,
		schedule.ReasonScanQueryFallbackCN,
	} {
		require.True(t, shouldWarnScanPlacement(reason), reason)
	}

	for _, reason := range []string{
		schedule.ReasonScanSmallBlocks,
		schedule.ReasonScanForceOneCN,
		schedule.ReasonScanForceSingle,
		schedule.ReasonScanMultiCN,
	} {
		require.False(t, shouldWarnScanPlacement(reason), reason)
	}
}

func TestSingleWorkerStageNodePrefersSingleScheduledWorker(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "cn-local:6001"
	c.cnList = engine.Nodes{
		{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: 8},
	}

	node := c.singleWorkerStageNode()
	require.Equal(t, "cn-remote", node.Id)
	require.Equal(t, "cn-remote:6001", node.Addr)
}

func TestSingleWorkerStageNodeFallsBackToCurrentCNForMultiCNQuery(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "cn-local:6001"
	c.cnList = engine.Nodes{
		{Id: "cn-a", Addr: "cn-a:6001", Mcpu: 4},
		{Id: "cn-b", Addr: "cn-b:6001", Mcpu: 4},
	}

	node := c.singleWorkerStageNode()
	require.Equal(t, "cn-local:6001", node.Addr)
}

func TestQueryWorkerStageNodesKeepsScheduledWorkers(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "cn-local:6001"
	c.cnList = engine.Nodes{
		{Id: "cn-local", Mcpu: 0},
		{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: 8},
	}

	nodes := c.queryWorkerStageNodes()
	require.Equal(t, engine.Nodes{
		{Id: "cn-local", Addr: "cn-local:6001", Mcpu: 1},
		{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: 8},
	}, nodes)
}

func TestQueryWorkerStageNodesCanonicalizesCurrentCNAddress(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "cn-local:6001"
	c.cnList = engine.Nodes{
		{Addr: "cn-remote:6001", Mcpu: 8},
		{Id: "cn-local", Mcpu: 4},
	}

	nodes := c.queryWorkerStageNodes()
	require.Equal(t, engine.Nodes{
		{Addr: "cn-remote:6001", Mcpu: 8},
		{Id: "cn-local", Addr: "cn-local:6001", Mcpu: 4},
	}, nodes)
}

func TestQueryWorkerStageNodesKeepsLocalSentinel(t *testing.T) {
	c := NewMockCompile(t)
	c.cnList = engine.Nodes{{Mcpu: 1}}

	nodes := c.queryWorkerStageNodes()
	require.Equal(t, engine.Nodes{{Mcpu: 1}}, nodes)
}

func TestNewScopeListOnSingleWorkerStageKeepsRemoteSingleWorker(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "cn-local:6001"
	c.anal = &AnalyzeModule{}
	c.cnList = engine.Nodes{
		{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: 8},
	}

	scopes := c.newScopeListOnSingleWorkerStage(2, 2)
	require.Len(t, scopes, 2)
	for _, s := range scopes {
		require.Equal(t, "cn-remote", s.NodeInfo.Id)
		require.Equal(t, "cn-remote:6001", s.NodeInfo.Addr)
		require.Equal(t, 1, s.NodeInfo.Mcpu)
	}
}

func TestScopeNodeWithMcpuKeepsWorkerIdentity(t *testing.T) {
	node := scopeNodeWithMcpu(engine.Node{
		Id:   "cn-1",
		Addr: "cn-1:6001",
		Mcpu: 8,
	}, 0)

	require.Equal(t, "cn-1", node.Id)
	require.Equal(t, "cn-1:6001", node.Addr)
	require.Equal(t, 1, node.Mcpu)
}

func TestConstructScopeForExternalNodeKeepsWorkerIdentity(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.anal = &AnalyzeModule{qry: &plan.Query{}}

	scope := c.constructScopeForExternalNode(engine.Node{
		Id:   "remote",
		Addr: "remote:6001",
		Mcpu: 8,
	}, true)

	require.Equal(t, "remote", scope.NodeInfo.Id)
	require.Equal(t, "remote:6001", scope.NodeInfo.Addr)
	require.Equal(t, 1, scope.NodeInfo.Mcpu)
}

func TestSameExecutionNodeUsesIdentityAndDoesNotMatchEmptyAddressToRemote(t *testing.T) {
	require.True(t, sameExecutionNode(
		engine.Node{Id: "cn-local", Mcpu: 1},
		engine.Node{Id: "cn-local", Addr: "stale:6001", Mcpu: 1},
	))
	require.False(t, sameExecutionNode(
		engine.Node{Id: "cn-local", Mcpu: 1},
		engine.Node{Id: "cn-remote", Addr: "remote:6001", Mcpu: 1},
	))
	require.False(t, sameExecutionNode(
		engine.Node{Mcpu: 1},
		engine.Node{Addr: "remote:6001", Mcpu: 1},
	))
}

func TestMergeScopesByStageNodesUsesExecutionIdentity(t *testing.T) {
	c := NewMockCompile(t)
	c.anal = &AnalyzeModule{}
	local := &Scope{
		Magic:    Remote,
		NodeInfo: engine.Node{Id: "cn-local", Mcpu: 1},
		Proc:     c.proc.NewNoContextChildProc(0),
	}
	remote := &Scope{
		Magic:    Remote,
		NodeInfo: engine.Node{Id: "cn-remote", Addr: "remote:6001", Mcpu: 1},
		Proc:     c.proc.NewNoContextChildProc(0),
	}

	grouped := c.mergeScopesByStageNodes([]*Scope{local, remote}, engine.Nodes{
		{Id: "cn-local", Mcpu: 1},
		{Id: "cn-remote", Addr: "remote:6001", Mcpu: 1},
	})

	require.Len(t, grouped, 2)
	require.Equal(t, "cn-local", grouped[0].NodeInfo.Id)
	require.Len(t, grouped[0].PreScopes, 1)
	require.Equal(t, "cn-local", grouped[0].PreScopes[0].NodeInfo.Id)
	require.Equal(t, "cn-remote", grouped[1].NodeInfo.Id)
	require.Len(t, grouped[1].PreScopes, 1)
	require.Equal(t, "cn-remote", grouped[1].PreScopes[0].NodeInfo.Id)
}

func TestScopeIpAddrMatchDoesNotTreatEmptyLocalAddressAsRemoteMatch(t *testing.T) {
	scope := &Scope{NodeInfo: engine.Node{Addr: "remote:6001"}}
	require.False(t, scope.ipAddrMatch(""))

	scope.NodeInfo.Addr = ""
	require.True(t, scope.ipAddrMatch(""))
}

func TestGenerateNodesRespectsNodeDOPOnMultiCN(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "cn-local:6001"
	c.e = newStubEngineForGenerateNodes("testdb", "t")
	c.cnList = engine.Nodes{
		{Id: "cn1", Addr: "cn-local:6001", Mcpu: 8},
		{Id: "cn2", Addr: "cn-local:6001", Mcpu: 12},
	}

	node := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		ObjRef: &plan.ObjectRef{
			SchemaName: "testdb",
			ObjName:    "t",
		},
		TableDef: &plan.TableDef{
			Name: "t",
		},
		Stats: &plan.Stats{
			BlockNum: int32(plan2.BlockThresholdForOneCN + 1),
			Dop:      2,
		},
	}

	nodes, err := c.generateNodes(node)
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	require.Equal(t, 2, nodes[0].Mcpu)
	require.Equal(t, 2, nodes[1].Mcpu)
}

func TestGenerateNodesKeepsScheduledLocalWorkerWithoutAddress(t *testing.T) {
	c := NewMockCompile(t)
	c.e = newStubEngineForGenerateNodes("testdb", "t")
	c.cnList = engine.Nodes{
		{Id: "cn-local", Mcpu: 8},
		{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: 12},
	}

	node := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		ObjRef: &plan.ObjectRef{
			SchemaName: "testdb",
			ObjName:    "t",
		},
		TableDef: &plan.TableDef{
			Name: "t",
		},
		Stats: &plan.Stats{
			BlockNum: int32(plan2.BlockThresholdForOneCN + 1),
			Dop:      4,
		},
	}

	nodes, err := c.generateNodes(node)
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	require.Equal(t, "cn-local", nodes[0].Id)
	require.Empty(t, nodes[0].Addr)
	require.Equal(t, 4, nodes[0].Mcpu)
	require.Equal(t, int32(2), nodes[0].CNCNT)
	require.Equal(t, int32(0), nodes[0].CNIDX)
	require.Nil(t, nodes[0].Data)
	require.Equal(t, "cn-remote", nodes[1].Id)
	require.Equal(t, "cn-remote:6001", nodes[1].Addr)
	require.Equal(t, 4, nodes[1].Mcpu)
	require.Equal(t, int32(2), nodes[1].CNCNT)
	require.Equal(t, int32(1), nodes[1].CNIDX)
	require.NotNil(t, nodes[1].Data)
}

func TestGenerateNodesCapsByCNMcpuWhenDOPIsLarger(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "cn-local:6001"
	c.e = newStubEngineForGenerateNodes("testdb", "t")
	c.cnList = engine.Nodes{
		{Id: "cn1", Addr: "cn-local:6001", Mcpu: 3},
		{Id: "cn2", Addr: "cn-local:6001", Mcpu: 6},
	}

	node := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		ObjRef: &plan.ObjectRef{
			SchemaName: "testdb",
			ObjName:    "t",
		},
		TableDef: &plan.TableDef{
			Name: "t",
		},
		Stats: &plan.Stats{
			BlockNum: int32(plan2.BlockThresholdForOneCN + 1),
			Dop:      4,
		},
	}

	nodes, err := c.generateNodes(node)
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	require.Equal(t, 3, nodes[0].Mcpu)
	require.Equal(t, 4, nodes[1].Mcpu)
}

func TestGenerateNodesUsesMultiCNForSmallIvfEntriesInternalScan(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "cn-local:6001"
	c.e = newStubEngineForGenerateNodes("testdb", "idx_entries")
	c.cnList = engine.Nodes{
		{Id: "cn1", Addr: "cn-local:6001", Mcpu: 4},
		{Id: "cn2", Addr: "cn-local:6001", Mcpu: 4},
	}

	node := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		ObjRef: &plan.ObjectRef{
			SchemaName: "testdb",
			ObjName:    "idx_entries",
		},
		TableDef: &plan.TableDef{
			Name:      "idx_entries",
			TableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
		},
		Stats: &plan.Stats{
			BlockNum: 1,
			Dop:      1,
		},
		IndexReaderParam: &plan.IndexReaderParam{
			OrderBy: []*plan.OrderBySpec{{Expr: &plan.Expr{}}},
			Limit: &plan.Expr{
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 10}},
				},
			},
		},
	}

	nodes, err := c.generateNodes(node)
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	require.Equal(t, int32(2), nodes[0].CNCNT)
	require.Equal(t, int32(0), nodes[0].CNIDX)
	require.Equal(t, int32(2), nodes[1].CNCNT)
	require.Equal(t, int32(1), nodes[1].CNIDX)
}

func TestGenerateNodesUsesMultiCNForSmallIvfSearchFunctionScan(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "cn-local:6001"
	c.e = newStubEngineForGenerateNodes("testdb", "idx_entries")
	c.cnList = engine.Nodes{
		{Id: "cn1", Addr: "cn-local:6001", Mcpu: 4},
		{Id: "cn2", Addr: "cn-local:6001", Mcpu: 4},
	}

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		ObjRef: &plan.ObjectRef{
			SchemaName: "testdb",
			ObjName:    "idx_entries",
		},
		TableDef: &plan.TableDef{
			Name:      "idx_entries",
			TableType: "func_table",
			TblFunc:   &plan.TableFunction{Name: ivfflatplan.IVFFLATSearchFuncName},
		},
		Stats: &plan.Stats{
			BlockNum: 1,
			Dop:      1,
		},
		IndexReaderParam: &plan.IndexReaderParam{
			Limit: &plan.Expr{
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 10}},
				},
			},
		},
	}

	nodes, err := c.generateNodes(node)
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	require.Equal(t, int32(2), nodes[0].CNCNT)
	require.Equal(t, int32(0), nodes[0].CNIDX)
	require.Equal(t, int32(2), nodes[1].CNCNT)
	require.Equal(t, int32(1), nodes[1].CNIDX)
}

func TestGenerateNodesPreservesPartitionedIvfEntriesPhysicalOwnership(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "cn-local:6001"
	c.e = newStubEngineForGenerateNodes("testdb", "idx_entries")
	c.cnList = engine.Nodes{
		{Id: "cn1", Addr: "cn-local:6001", Mcpu: 4},
		{Id: "cn2", Addr: "cn-remote:6001", Mcpu: 4},
	}

	node := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		ObjRef: &plan.ObjectRef{
			SchemaName: "testdb",
			ObjName:    "idx_entries",
		},
		TableDef: &plan.TableDef{
			Name:      "idx_entries",
			TableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
		},
		Stats: &plan.Stats{
			BlockNum: 1,
			Dop:      1,
		},
		IndexReaderParam: &plan.IndexReaderParam{
			Limit: &plan.Expr{
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 10}},
				},
			},
			OrigFuncName:   "l2_distance",
			PartitionCnCnt: 2,
			PartitionCnIdx: 1,
		},
	}

	nodes, err := c.generateNodes(node)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, "cn-local:6001", nodes[0].Addr)
	require.Equal(t, int32(2), nodes[0].CNCNT)
	require.Equal(t, int32(1), nodes[0].CNIDX)
}

func TestCompileTableFunctionDispatchesIvfSearchToAllCNs(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "cn-local:6001"
	c.execType = plan2.ExecTypeAP_MULTICN
	c.anal = &AnalyzeModule{isFirst: true}
	c.pn = &plan.Plan{
		Plan: &plan.Plan_Query{
			Query: &plan.Query{},
		},
	}
	c.cnList = engine.Nodes{
		{Id: "cn1", Addr: "cn-local:6001", Mcpu: 4, CNCNT: 2, CNIDX: 0},
		{Id: "cn2", Addr: "cn-remote:6001", Mcpu: 4, CNCNT: 2, CNIDX: 1},
	}

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		TableDef: &plan.TableDef{
			Name:      "idx_entries",
			TableType: "func_table",
			Cols: []*plan.ColDef{
				{Name: "pkid", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "score", Typ: plan.Type{Id: int32(types.T_float64)}},
			},
			TblFunc: &plan.TableFunction{Name: ivfflatplan.IVFFLATSearchFuncName},
		},
		Stats: &plan.Stats{BlockNum: 1, Dop: 1},
		IndexReaderParam: &plan.IndexReaderParam{
			Limit: &plan.Expr{
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 10}},
				},
			},
		},
	}

	scopes, err := c.compileTableFunction(node, nil)
	require.NoError(t, err)
	require.Len(t, scopes, 2)
	require.Equal(t, Remote, scopes[0].Magic)
	require.Equal(t, "cn-local:6001", scopes[0].NodeInfo.Addr)
	require.Equal(t, 1, scopes[0].NodeInfo.Mcpu)
	require.Equal(t, int32(2), scopes[0].NodeInfo.CNCNT)
	require.Equal(t, int32(0), scopes[0].NodeInfo.CNIDX)
	require.Equal(t, Remote, scopes[1].Magic)
	require.Equal(t, "cn-remote:6001", scopes[1].NodeInfo.Addr)
	require.Equal(t, 1, scopes[1].NodeInfo.Mcpu)
	require.Equal(t, int32(2), scopes[1].NodeInfo.CNCNT)
	require.Equal(t, int32(1), scopes[1].NodeInfo.CNIDX)

	op0, ok := scopes[0].RootOp.(*table_function.TableFunction)
	require.True(t, ok)
	require.Equal(t, int32(2), op0.IndexReaderParam.GetPartitionCnCnt())
	require.Equal(t, int32(0), op0.IndexReaderParam.GetPartitionCnIdx())
	op1, ok := scopes[1].RootOp.(*table_function.TableFunction)
	require.True(t, ok)
	require.Equal(t, int32(2), op1.IndexReaderParam.GetPartitionCnCnt())
	require.Equal(t, int32(1), op1.IndexReaderParam.GetPartitionCnIdx())
}

type readonlyWorkspaceForIvfTest struct{ *Ws }

func (*readonlyWorkspaceForIvfTest) Readonly() bool { return true }

func TestShouldDispatchIvfSearchMultiCNRejectsWritableWorkspace(t *testing.T) {
	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc:   &plan.TableFunction{Name: ivfflatplan.IVFFLATSearchFuncName},
		},
	}
	cnList := engine.Nodes{{Addr: "cn1:6001"}, {Addr: "cn2:6001"}}

	var writable client.Workspace = &Ws{}
	require.False(t, shouldDispatchIvfSearchMultiCN(node, plan2.ExecTypeAP_MULTICN, cnList, writable))
	require.True(t, shouldDispatchIvfSearchMultiCN(node, plan2.ExecTypeAP_MULTICN, cnList, &readonlyWorkspaceForIvfTest{Ws: &Ws{}}))
	require.True(t, shouldDispatchIvfSearchMultiCN(node, plan2.ExecTypeAP_MULTICN, cnList, nil))
}

func TestGenerateNodesKeepsForceOneCNIvfEntriesInternalScanOnCurrentCN(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "cn-local:6001"
	c.e = newStubEngineForGenerateNodes("testdb", "idx_entries")
	c.cnList = engine.Nodes{
		{Id: "cn1", Addr: "cn-local:6001", Mcpu: 4},
		{Id: "cn2", Addr: "cn-local:6001", Mcpu: 4},
	}

	node := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		ObjRef: &plan.ObjectRef{
			SchemaName: "testdb",
			ObjName:    "idx_entries",
		},
		TableDef: &plan.TableDef{
			Name:      "idx_entries",
			TableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
		},
		Stats: &plan.Stats{
			BlockNum:   1,
			Dop:        1,
			ForceOneCN: true,
		},
		IndexReaderParam: &plan.IndexReaderParam{
			OrderBy: []*plan.OrderBySpec{{Expr: &plan.Expr{}}},
			Limit: &plan.Expr{
				Expr: &plan.Expr_Lit{
					Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 10}},
				},
			},
		},
	}

	nodes, err := c.generateNodes(node)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, int32(1), nodes[0].CNCNT)
}

func TestGenerateNodesKeepsSmallNonIvfScanOnCurrentCN(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "cn-local:6001"
	c.e = newStubEngineForGenerateNodes("testdb", "t")
	c.cnList = engine.Nodes{
		{Id: "cn1", Addr: "cn-local:6001", Mcpu: 8},
		{Id: "cn2", Addr: "cn-remote:6001", Mcpu: 12},
	}

	node := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		ObjRef: &plan.ObjectRef{
			SchemaName: "testdb",
			ObjName:    "t",
		},
		TableDef: &plan.TableDef{
			Name: "t",
		},
		Stats: &plan.Stats{
			BlockNum: int32(plan2.BlockThresholdForOneCN),
			Dop:      4,
		},
	}

	nodes, err := c.generateNodes(node)
	require.NoError(t, err)
	require.Equal(t, engine.Nodes{{
		Id:    "cn1",
		Addr:  "cn-local:6001",
		Mcpu:  4,
		CNCNT: 1,
	}}, nodes)
}

func TestGenerateNodesKeepsLargeScanOnCurrentCNWhenNoWorkers(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "cn-local:6001"
	c.e = newStubEngineForGenerateNodes("testdb", "t")

	node := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		ObjRef: &plan.ObjectRef{
			SchemaName: "testdb",
			ObjName:    "t",
		},
		TableDef: &plan.TableDef{
			Name: "t",
		},
		Stats: &plan.Stats{
			BlockNum: int32(plan2.BlockThresholdForOneCN + 1),
			Dop:      4,
		},
	}

	nodes, err := c.generateNodes(node)
	require.NoError(t, err)
	require.Equal(t, engine.Nodes{{
		Addr:  "cn-local:6001",
		Mcpu:  4,
		CNCNT: 1,
	}}, nodes)
}

func TestGenerateNodesKeepsSingleRemoteQueryWorkerForLocalOnlyScan(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "cn-local:6001"
	c.e = newStubEngineForGenerateNodes("testdb", "t")
	c.cnList = engine.Nodes{
		{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: 8},
	}
	c.queryPlacement = schedule.QueryDecision{
		ExecKind:  schedule.QueryExecAPMultiCN,
		Workers:   schedule.Workers{{ID: "cn-remote", Addr: "cn-remote:6001", Mcpu: 8}},
		Reason:    schedule.ReasonMultiCN,
		Satisfied: true,
	}

	node := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		ObjRef: &plan.ObjectRef{
			SchemaName: "testdb",
			ObjName:    "t",
		},
		TableDef: &plan.TableDef{
			Name: "t",
		},
		Stats: &plan.Stats{
			BlockNum: int32(plan2.BlockThresholdForOneCN + 1),
			Dop:      4,
		},
	}

	nodes, err := c.generateNodes(node)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, "cn-remote", nodes[0].Id)
	require.Equal(t, "cn-remote:6001", nodes[0].Addr)
	require.Equal(t, 4, nodes[0].Mcpu)
	require.Equal(t, int32(1), nodes[0].CNCNT)
	require.Equal(t, int32(0), nodes[0].CNIDX)
	require.NotNil(t, nodes[0].Data)
}

func TestGenerateNodesKeepsLocalScanMcpuAtLeastOne(t *testing.T) {
	for _, dop := range []int32{0, -2} {
		c := NewMockCompile(t)
		c.addr = "cn-local:6001"
		c.e = newStubEngineForGenerateNodes("testdb", "t")
		c.cnList = engine.Nodes{
			{Id: "cn-local", Addr: "cn-local:6001", Mcpu: 8},
			{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: 8},
		}

		node := &plan.Node{
			NodeType: plan.Node_TABLE_SCAN,
			ObjRef: &plan.ObjectRef{
				SchemaName: "testdb",
				ObjName:    "t",
			},
			TableDef: &plan.TableDef{
				Name: "t",
			},
			Stats: &plan.Stats{
				BlockNum: int32(plan2.BlockThresholdForOneCN),
				Dop:      dop,
			},
		}

		nodes, err := c.generateNodes(node)
		require.NoError(t, err)
		require.Equal(t, engine.Nodes{{
			Id:    "cn-local",
			Addr:  "cn-local:6001",
			Mcpu:  1,
			CNCNT: 1,
		}}, nodes)
	}
}

func TestGenerateNodesKeepsForceOneCNOnCurrentCN(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "cn-local:6001"
	c.e = newStubEngineForGenerateNodes("testdb", "t")
	c.cnList = engine.Nodes{
		{Id: "cn1", Addr: "cn-local:6001", Mcpu: 8},
		{Id: "cn2", Addr: "cn-remote:6001", Mcpu: 12},
	}

	node := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		ObjRef: &plan.ObjectRef{
			SchemaName: "testdb",
			ObjName:    "t",
		},
		TableDef: &plan.TableDef{
			Name: "t",
		},
		Stats: &plan.Stats{
			BlockNum:   int32(plan2.BlockThresholdForOneCN + 1),
			Dop:        6,
			ForceOneCN: true,
		},
	}

	nodes, err := c.generateNodes(node)
	require.NoError(t, err)
	require.Equal(t, engine.Nodes{{
		Id:    "cn1",
		Addr:  "cn-local:6001",
		Mcpu:  6,
		CNCNT: 1,
	}}, nodes)
}

func TestGenerateNodesKeepsTableCloneOnCurrentCN(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "cn-local:6001"
	c.e = newStubEngineForGenerateNodes("testdb", "t")
	c.cnList = engine.Nodes{
		{Id: "cn-local", Addr: "cn-local:6001", Mcpu: 8},
		{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: 8},
	}

	node := &plan.Node{
		NodeType: plan.Node_TABLE_CLONE,
		ObjRef: &plan.ObjectRef{
			SchemaName: "testdb",
			ObjName:    "t",
		},
		TableDef: &plan.TableDef{
			Name: "t",
		},
		Stats: &plan.Stats{
			BlockNum: int32(plan2.BlockThresholdForOneCN + 1),
			Dop:      6,
		},
	}

	nodes, err := c.generateNodes(node)
	require.NoError(t, err)
	require.Equal(t, engine.Nodes{{
		Id:    "cn-local",
		Addr:  "cn-local:6001",
		Mcpu:  1,
		CNCNT: 1,
	}}, nodes)
}

func TestGenerateNodesCollectsRemoteTombstonesOnce(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "cn-local:6001"
	tombstones := readutil.NewEmptyTombstoneData()
	rel := newStubRelation("t")
	rel.tombstones = tombstones
	db := newStubDatabase("testdb")
	db.rels["t"] = rel
	e := newStubEngine()
	e.dbs["testdb"] = db
	c.e = e
	c.cnList = engine.Nodes{
		{Id: "cn-local", Addr: "cn-local:6001", Mcpu: 4},
		{Id: "cn-remote-1", Addr: "cn-remote-1:6001", Mcpu: 4},
		{Id: "cn-remote-2", Addr: "cn-remote-2:6001", Mcpu: 4},
	}

	node := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		ObjRef: &plan.ObjectRef{
			SchemaName: "testdb",
			ObjName:    "t",
		},
		TableDef: &plan.TableDef{
			Name: "t",
		},
		Stats: &plan.Stats{
			BlockNum: int32(plan2.BlockThresholdForOneCN + 1),
			Dop:      2,
		},
	}

	nodes, err := c.generateNodes(node)
	require.NoError(t, err)
	require.Len(t, nodes, 3)
	require.Nil(t, nodes[0].Data)
	for i := 1; i < len(nodes); i++ {
		require.NotNil(t, nodes[i].Data)
		require.Same(t, tombstones, nodes[i].Data.GetTombstones())
	}
	require.Equal(t, 1, rel.collectTombstonesCall)
}

func TestGenerateNodesReturnsRemoteTombstoneError(t *testing.T) {
	expected := errors.New("collect tombstones failed")
	c := NewMockCompile(t)
	c.addr = "cn-local:6001"
	rel := newStubRelation("t")
	rel.collectTombstonesErr = expected
	db := newStubDatabase("testdb")
	db.rels["t"] = rel
	e := newStubEngine()
	e.dbs["testdb"] = db
	c.e = e
	c.cnList = engine.Nodes{
		{Id: "cn-local", Addr: "cn-local:6001", Mcpu: 4},
		{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: 4},
	}

	node := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		ObjRef: &plan.ObjectRef{
			SchemaName: "testdb",
			ObjName:    "t",
		},
		TableDef: &plan.TableDef{
			Name: "t",
		},
		Stats: &plan.Stats{
			BlockNum: int32(plan2.BlockThresholdForOneCN + 1),
			Dop:      2,
		},
	}

	nodes, err := c.generateNodes(node)
	require.ErrorIs(t, err, expected)
	require.Nil(t, nodes)
	require.Equal(t, 1, rel.collectTombstonesCall)
}

func newStubEngineForGenerateNodes(dbName, tblName string) *stubEngine {
	e := newStubEngine()
	db := newStubDatabase(dbName)
	db.rels[tblName] = newStubRelation(tblName)
	e.dbs[dbName] = db
	return e
}
