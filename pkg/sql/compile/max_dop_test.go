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
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

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

func newStubEngineForGenerateNodes(dbName, tblName string) *stubEngine {
	e := newStubEngine()
	db := newStubDatabase(dbName)
	db.rels[tblName] = newStubRelation(tblName)
	e.dbs[dbName] = db
	return e
}
