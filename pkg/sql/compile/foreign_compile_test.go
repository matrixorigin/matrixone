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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// foreignTestNode builds a FOREIGN_TB scan node: one declared column plus the
// trailing hidden __mo_query column, with the given filter conjuncts.
func foreignTestNode(defaultQuery string, filters ...*plan.Expr) *plan.Node {
	return &plan.Node{
		NodeType: plan.Node_EXTERNAL_SCAN,
		TableDef: &plan.TableDef{
			Cols: []*plan.ColDef{
				{Name: "a", ColId: 1, Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: catalog.ExternalQuery, ColId: catalog.ExternalQueryColId,
					Typ: plan.Type{Id: int32(types.T_varchar)}},
			},
		},
		ExternScan: &plan.ExternScan{
			Type:           int32(plan.ExternType_FOREIGN_TB),
			TbColToDataCol: map[string]int32{"a": 0},
			ForeignScan: &plan.ForeignScan{
				Kind:         "sql",
				Config:       `{"driver":"mysql","dsn":"unused"}`,
				DefaultQuery: defaultQuery,
			},
		},
		FilterList: filters,
	}
}

func foreignQueryEq(text string) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "=", Obj: int64(function.EQUAL) << 32},
			Args: []*plan.Expr{
				{Typ: plan.Type{Id: int32(types.T_varchar)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1, Name: catalog.ExternalQuery}}},
				{Typ: plan.Type{Id: int32(types.T_varchar)},
					Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: text}}}},
			},
		}},
	}
}

func TestCompileForeignScan(t *testing.T) {
	newCompile := func() *Compile {
		c := NewMockCompile(t)
		c.addr = "local:6001"
		c.anal = &AnalyzeModule{qry: &plan.Query{}}
		return c
	}

	// __mo_query = '<text>' derives the query list; the generating conjunct is
	// consumed (removed from the row-level filter list)
	c := newCompile()
	node := foreignTestNode("", foreignQueryEq("select 1"))
	scopes, err := c.compileForeignScan(node, true)
	require.NoError(t, err)
	require.Len(t, scopes, 1)
	require.Empty(t, node.FilterList)
	op, ok := scopes[0].RootOp.(*external.External)
	require.True(t, ok)
	require.Same(t, node.ExternScan.ForeignScan, op.Es.ForeignScan)
	require.Equal(t, []string{"select 1"}, op.Es.FileList)
	require.NotNil(t, op.Es.Extern)

	// no predicate: the 'query' option is the default
	c = newCompile()
	node = foreignTestNode("select default")
	scopes, err = c.compileForeignScan(node, true)
	require.NoError(t, err)
	op = scopes[0].RootOp.(*external.External)
	require.Equal(t, []string{"select default"}, op.Es.FileList)

	// no predicate and no 'query' option: a clear error
	c = newCompile()
	node = foreignTestNode("")
	_, err = c.compileForeignScan(node, true)
	require.ErrorContains(t, err, "__mo_query")

	// missing scan metadata is a clean error
	c = newCompile()
	_, err = c.compileForeignScan(&plan.Node{ExternScan: &plan.ExternScan{}}, true)
	require.ErrorContains(t, err, "missing scan metadata")

	// a row-level conjunct stays in the filter list next to the consumed one
	c = newCompile()
	rowLevel := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: ">", Obj: int64(function.GREAT_THAN) << 32},
			Args: []*plan.Expr{
				{Typ: plan.Type{Id: int32(types.T_int64)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0, Name: "a"}}},
				{Typ: plan.Type{Id: int32(types.T_int64)},
					Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 5}}}},
			},
		}},
	}
	node = foreignTestNode("", foreignQueryEq("select 1"), rowLevel)
	scopes, err = c.compileForeignScan(node, true)
	require.NoError(t, err)
	require.Len(t, node.FilterList, 1)
	require.Equal(t, ">", node.FilterList[0].GetF().Func.GetObjName())
	op = scopes[0].RootOp.(*external.External)
	require.Equal(t, []string{"select 1"}, op.Es.FileList)
}

// TestShuffleStageNodesKeepForeignScanCN proves a session-pinned foreign
// external scan participates in the shuffle receiver stage set like a
// SINK_SCAN: when the scheduled query workers exclude the session CN, the
// scan's CN is appended so a receiver tree exists to start the scope
// (otherwise a distributed DEDUP shuffle would wait forever).
func TestShuffleStageNodesKeepForeignScanCN(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "cn-session:6001"
	c.anal = &AnalyzeModule{qry: &plan.Query{}}

	node := foreignTestNode("select 1")
	scopes, err := c.compileForeignScan(node, true)
	require.NoError(t, err)
	require.Len(t, scopes, 1)

	// scheduled query workers do NOT include the session CN
	c.cnList = engine.Nodes{
		{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: 8},
	}
	stageNodes, local := c.shuffleJoinStageNodes(scopes, nil)
	require.True(t, local, "foreign scan must count as a local pinned source")
	var hasSession bool
	for _, n := range stageNodes {
		if n.Addr == "cn-session:6001" {
			hasSession = true
			require.Equal(t, 1, n.Mcpu)
		}
	}
	require.True(t, hasSession, "session CN must be in the receiver stage set")

	// an ordinary (non-foreign) scope stays fully distributed
	normalScope := &Scope{RootOp: merge.NewArgument()}
	_, local = c.shuffleJoinStageNodes([]*Scope{normalScope}, nil)
	require.False(t, local)
}
