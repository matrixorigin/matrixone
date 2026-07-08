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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
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
