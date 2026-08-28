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
	"time"

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

// foreignRowFilter builds `<col at pos> <op> <int literal>`.
func foreignRowFilter(op string, obj function.FuncExplainLayout, colPos int32, colName string, val int64) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: op, Obj: int64(obj) << 32},
			Args: []*plan.Expr{
				{Typ: plan.Type{Id: int32(types.T_int64)},
					Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: colPos, Name: colName}}},
				{Typ: plan.Type{Id: int32(types.T_int64)},
					Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: val}}}},
			},
		}},
	}
}

// TestCompileForeignScanPushdown covers the opt-in predicate pushdown for
// ENGINE = SQL: an opted-in table's query text is wrapped as a derived table
// carrying the renderable conjuncts, and exactly those stop being evaluated
// locally.
func TestCompileForeignScanPushdown(t *testing.T) {
	newCompile := func() *Compile {
		c := NewMockCompile(t)
		c.addr = "local:6001"
		c.anal = &AnalyzeModule{qry: &plan.Query{}}
		c.proc.Base.SessionInfo.TimeZone = time.UTC
		return c
	}
	aGt5 := func() *plan.Expr { return foreignRowFilter(">", function.GREAT_THAN, 0, "a", 5) }

	// DEFAULT: the query text is untouched and MO filters
	c := newCompile()
	node := foreignTestNode("", foreignQueryEq("select a from src"), aGt5())
	scopes, err := c.compileForeignScan(node, true)
	require.NoError(t, err)
	op := scopes[0].RootOp.(*external.External)
	require.Equal(t, []string{"select a from src"}, op.Es.FileList)
	require.Len(t, node.FilterList, 1)

	// OPTED IN: wrapped, and the conjunct leaves the local filter list
	c = newCompile()
	node = foreignTestNode("", foreignQueryEq("select a from src"), aGt5())
	node.ExternScan.ForeignScan.Pushdown = true
	scopes, err = c.compileForeignScan(node, true)
	require.NoError(t, err)
	sent := scopes[0].RootOp.(*external.External).Es.FileList[0]
	require.Contains(t, sent, "select * from (")
	require.Contains(t, sent, "select a from src")
	// identifiers go bare: the quoting character is dialect-specific and
	// sql_tvf speaks to PostgreSQL as well as MySQL
	require.Contains(t, sent, " where (a > 5)")
	require.NotContains(t, sent, "`a`")
	require.Empty(t, node.FilterList, "a pushed conjunct is not evaluated twice")

	// the 'query' table option is wrapped the same way
	c = newCompile()
	node = foreignTestNode("select a from src", aGt5())
	node.ExternScan.ForeignScan.Pushdown = true
	scopes, err = c.compileForeignScan(node, true)
	require.NoError(t, err)
	require.Contains(t, scopes[0].RootOp.(*external.External).Es.FileList[0], " where (a > 5)")

	// a conjunct the deparser cannot render has nothing to send, so it stays
	// local and the wrapper carries only what was rendered
	c = newCompile()
	unpushable := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "abs"},
			Args: []*plan.Expr{{Typ: plan.Type{Id: int32(types.T_int64)},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0, Name: "a"}}}},
		}},
	}
	node = foreignTestNode("", foreignQueryEq("select a from src"), aGt5(), unpushable)
	node.ExternScan.ForeignScan.Pushdown = true
	scopes, err = c.compileForeignScan(node, true)
	require.NoError(t, err)
	sent = scopes[0].RootOp.(*external.External).Es.FileList[0]
	require.Contains(t, sent, " where (a > 5)")
	require.NotContains(t, sent, "abs")
	require.Len(t, node.FilterList, 1)
	require.Equal(t, "abs", node.FilterList[0].GetF().Func.GetObjName())

	// nothing renderable at all: no wrapper, so a text that cannot be a
	// derived table is not made into one for nothing
	c = newCompile()
	node = foreignTestNode("", foreignQueryEq("select a from src"), unpushable)
	node.ExternScan.ForeignScan.Pushdown = true
	scopes, err = c.compileForeignScan(node, true)
	require.NoError(t, err)
	require.Equal(t, []string{"select a from src"}, scopes[0].RootOp.(*external.External).Es.FileList)
	require.Len(t, node.FilterList, 1)

	// ESQL never wraps: the wrapper is SQL, ES|QL has no derived tables
	c = newCompile()
	node = foreignTestNode("", foreignQueryEq("from idx"), aGt5())
	node.ExternScan.ForeignScan.Kind = "esql"
	node.ExternScan.ForeignScan.Pushdown = true
	scopes, err = c.compileForeignScan(node, true)
	require.NoError(t, err)
	require.Equal(t, []string{"from idx"}, scopes[0].RootOp.(*external.External).Es.FileList)
	require.Len(t, node.FilterList, 1)
}

// TestCompileForeignScanPushdownMasksSyntheticCols proves a predicate on a
// synthetic column is never sent: those columns are made up inside MO and the
// source has no counterpart, so pushing one would turn a working query into an
// unknown-column error at the source.
func TestCompileForeignScanPushdownMasksSyntheticCols(t *testing.T) {
	c := NewMockCompile(t)
	c.addr = "local:6001"
	c.anal = &AnalyzeModule{qry: &plan.Query{}}
	c.proc.Base.SessionInfo.TimeZone = time.UTC

	// __mo_file_line is an error-mode column synthesized by the reader; it
	// sits before the trailing file-level __mo_query column
	node := foreignTestNode("")
	node.TableDef.Cols = []*plan.ColDef{
		{Name: "a", ColId: 1, Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: catalog.ExternalFileLine, ColId: catalog.ExternalFileLineColId,
			Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: catalog.ExternalQuery, ColId: catalog.ExternalQueryColId,
			Typ: plan.Type{Id: int32(types.T_varchar)}},
	}
	queryEq := foreignQueryEq("select a from src")
	queryEq.GetF().Args[0].GetCol().ColPos = 2
	node.FilterList = []*plan.Expr{
		queryEq,
		foreignRowFilter(">", function.GREAT_THAN, 1, catalog.ExternalFileLine, 1),
	}
	node.ExternScan.ForeignScan.Pushdown = true

	scopes, err := c.compileForeignScan(node, true)
	require.NoError(t, err)
	sent := scopes[0].RootOp.(*external.External).Es.FileList[0]
	require.Equal(t, "select a from src", sent, "a synthetic column must not be sent to the source")
	require.NotContains(t, sent, catalog.ExternalFileLine)
	require.Len(t, node.FilterList, 1, "and it must still be applied locally")
}
