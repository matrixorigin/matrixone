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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
)

func scanCutoffTestPlan() (*Compile, *plan.Node, *plan.Node) {
	typ := plan.Type{Id: int32(types.T_int64), NotNullable: true}
	key := &plan.Expr{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0, Name: "t.k"}}}
	physicalKey := &plan.Expr{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0, Name: "k"}}}
	header := plan.MsgHeader{MsgTag: 42, MsgType: int32(message.MsgTopValue)}
	scan := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN, TableDef: &plan.TableDef{Cols: []*plan.ColDef{{Name: "k", Typ: typ}}},
		ProjectList: []*plan.Expr{physicalKey}, OrderBy: []*plan.OrderBySpec{{Expr: physicalKey}}, RecvMsgList: []plan.MsgHeader{header},
	}
	sort := &plan.Node{NodeType: plan.Node_SORT, Children: []int32{0}, OrderBy: []*plan.OrderBySpec{{Expr: key}}, SendMsgList: []plan.MsgHeader{header}}
	query := &plan.Query{StmtType: plan.Query_SELECT, Nodes: []*plan.Node{scan, sort}}
	return &Compile{pn: &plan.Plan{Plan: &plan.Plan_Query{Query: query}}}, scan, sort
}

func TestPaginationCutoffPlanAdmission(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*Compile, *plan.Node, *plan.Node)
		want bool
	}{
		{name: "ordinary scan", want: true},
		{name: "physical nullable", edit: func(_ *Compile, scan, _ *plan.Node) { scan.TableDef.Cols[0].Typ.NotNullable = false }},
		{name: "partition", edit: func(_ *Compile, scan, _ *plan.Node) { scan.TableDef.Partition = &plan.Partition{} }},
		{name: "mismatched tag", edit: func(_ *Compile, scan, _ *plan.Node) { scan.RecvMsgList[0].MsgTag++ }},
		{name: "mismatched direction", edit: func(_ *Compile, scan, _ *plan.Node) { scan.OrderBy[0].Flag = plan.OrderBySpec_DESC }},
		{name: "mismatched physical type", edit: func(_ *Compile, scan, _ *plan.Node) { scan.TableDef.Cols[0].Typ.Id = int32(types.T_int32) }},
		{name: "ambiguous name", edit: func(_ *Compile, scan, _ *plan.Node) { scan.TableDef.Cols[0].Name = "t.k" }},
		{name: "dotted neighbor", edit: func(_ *Compile, scan, _ *plan.Node) {
			scan.TableDef.Cols = append(scan.TableDef.Cols, &plan.ColDef{Name: "t.k", Typ: scan.TableDef.Cols[0].Typ})
		}},
		{name: "unicode fold is not a name match", edit: func(_ *Compile, scan, _ *plan.Node) {
			scan.TableDef.Cols[0].Name = "ς"
			scan.OrderBy[0].Expr.GetCol().Name = "σ"
		}},
		{name: "runtime filter", edit: func(_ *Compile, scan, _ *plan.Node) { scan.RuntimeFilterProbeList = []*plan.RuntimeFilterSpec{{}} }},
		{name: "sql calc found rows", edit: func(c *Compile, _, _ *plan.Node) { c.stmt = sqlCalcFoundRowsTestStatement() }},
		{name: "prepared tp is reusable", edit: func(c *Compile, _, _ *plan.Node) { c.pn.IsPrepare = true }},
		{name: "prepared ap is statement owned", want: true, edit: func(c *Compile, _, _ *plan.Node) {
			c.pn.IsPrepare = true
			c.execType = plan2.ExecTypeAP_ONECN
		}},
		{name: "expression can fail", edit: func(_ *Compile, scan, _ *plan.Node) {
			col := scan.ProjectList[0]
			div, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "div", []*plan.Expr{plan2.MakePlan2Int64ConstExprWithType(1), col})
			if err != nil {
				panic(err)
			}
			scan.ProjectList = append(scan.ProjectList, div)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, scan, sort := scanCutoffTestPlan()
			if tc.edit != nil {
				tc.edit(c, scan, sort)
			}
			got, tag := c.paginationCutoffScan(sort)
			if tc.want {
				require.Same(t, scan, got)
				require.Equal(t, int32(42), tag)
			} else {
				require.Nil(t, got)
				require.Zero(t, tag)
			}
		})
	}
}

func TestPaginationCutoffFromSQL(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want bool
	}{
		{name: "nonprimary alias and filter", sql: "select b as x, a as y from select_test.bind_select where c > 10 order by x, y limit 5 offset 17000", want: true},
		{name: "unsafe filter", sql: "select b as x, a as y from select_test.bind_select where c % 2 = 0 order by x, y limit 5 offset 17000"},
		{name: "nullable key", sql: "select n from tpch.returning_bool order by n limit 5 offset 17000"},
		{name: "found rows", sql: "select sql_calc_found_rows b from select_test.bind_select order by b limit 5 offset 17000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCompileForShuffleGroupTest(t)
			ctx := plan2.NewMockCompilerContext(true, c.proc)
			stmts, err := mysql.Parse(context.Background(), tc.sql, 1)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			c.stmt = stmts[0]
			c.pn, err = plan2.BuildPlan(ctx, stmts[0], false, nil)
			require.NoError(t, err)
			var scan, sort *plan.Node
			for _, n := range c.pn.GetQuery().Nodes {
				switch n.NodeType {
				case plan.Node_TABLE_SCAN:
					scan = n
				case plan.Node_SORT:
					sort = n
				}
			}
			require.NotNil(t, scan)
			require.NotNil(t, sort)
			candidate, tag := c.paginationCutoffScan(sort)
			if !tc.want {
				require.Nil(t, candidate)
				require.Zero(t, tag)
				return
			}
			require.Same(t, scan, candidate)
			require.Positive(t, tag)
			require.Equal(t, "bind_select.b", scan.OrderBy[0].Expr.GetCol().Name)
			require.Equal(t, tag, scan.RecvMsgList[0].MsgTag)
			tableOp := table_scan.NewArgument()
			s := &Scope{Magic: Remote, Proc: c.proc, RootOp: tableOp, NodeInfo: engine.Node{Mcpu: 1},
				DataSource: &Source{node: scan, TableDef: scan.TableDef, OrderBy: scan.OrderBy, RecvMsgList: scan.RecvMsgList}}
			t.Cleanup(func() {
				require.NoError(t, vm.HandleAllOp(s.RootOp, func(_ vm.Operator, op vm.Operator) error {
					op.Release()
					return nil
				}))
			})
			result := c.compileSort(sort, []*Scope{s})
			require.Len(t, result, 1)
			var found *order.Order
			require.NoError(t, vm.HandleAllOp(result[0].RootOp, func(_ vm.Operator, op vm.Operator) error {
				if o, ok := op.(*order.Order); ok {
					found = o
				}
				return nil
			}))
			require.NotNil(t, found, "large OFFSET must compile to Order")
			require.Equal(t, uint64(17005), found.ScanCutoffLimit)
			require.Equal(t, tag, found.ScanCutoffTag)
		})
	}
}

func TestRemoteOrderScanCutoffBound(t *testing.T) {
	valid := func() *plan.Expr { return plan2.MakePlan2Uint64ConstExprWithType(17000) }
	for _, tc := range []struct {
		name string
		expr *plan.Expr
		want bool
	}{
		{name: "valid", expr: valid(), want: true},
		{name: "absent"},
		{name: "small", expr: plan2.MakePlan2Uint64ConstExprWithType(16384)},
		{name: "wrong type", expr: plan2.MakePlan2Int64ConstExprWithType(17000)},
		{name: "null", expr: func() *plan.Expr { e := valid(); e.GetLit().Isnull = true; return e }()},
		{name: "runtime source", expr: func() *plan.Expr { e := valid(); e.GetLit().Src = plan2.MakePlan2Int64ConstExprWithType(1); return e }()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value, ok := remoteOrderScanCutoffBound(tc.expr)
			require.Equal(t, tc.want, ok)
			if ok {
				require.Equal(t, uint64(17000), value)
			}
		})
	}
}

func TestScanCutoffScopeGenerationAndPairing(t *testing.T) {
	_, scan, sort := scanCutoffTestPlan()
	tableOp := table_scan.NewArgument()
	defer tableOp.Release()
	op := order.NewArgument()
	defer op.Release()
	op.OrderBySpec = sort.OrderBy
	op.ScanCutoffLimit, op.ScanCutoffTag = 30, 42
	op.AppendChild(tableOp)
	scope := &Scope{Magic: Remote, RootOp: tableOp, DataSource: &Source{node: scan, TableDef: scan.TableDef, OrderBy: scan.OrderBy, RecvMsgList: scan.RecvMsgList}}
	require.True(t, scanCutoffScopeMatches(scope, scan))
	scope.RootOp = op
	require.Equal(t, int32(42), initScanCutoff(scope))
	first := op.ScanCutoff
	require.NotNil(t, first)
	worker := dupOperator(op, 0, 2).(*order.Order)
	defer worker.Release()
	require.Same(t, first, worker.ScanCutoff)
	require.Equal(t, int32(42), initScanCutoff(scope))
	require.NotSame(t, first, op.ScanCutoff)
	require.Same(t, first, worker.ScanCutoff, "a new generation cannot clear a live worker's state")
	scope.DataSource.RecvMsgList = nil
	require.Zero(t, initScanCutoff(scope))
	require.Nil(t, op.ScanCutoff, "a rejected retry cannot retain old witnesses")

	scope.DataSource.RecvMsgList = scan.RecvMsgList
	project := projection.NewArgument()
	defer project.Release()
	project.AppendChild(tableOp)
	op.SetChild(project, 0)
	scope.RootOp = project
	require.True(t, scanCutoffScopeMatches(scope, scan))
	scope.RootOp = op
	require.Equal(t, int32(42), initScanCutoff(scope), "a transparent projection preserves the input contract")

	innerOrder := order.NewArgument()
	defer innerOrder.Release()
	innerOrder.AppendChild(tableOp)
	op.SetChild(innerOrder, 0)
	scope.RootOp = innerOrder
	require.False(t, scanCutoffScopeMatches(scope, scan))
	scope.RootOp = op
	require.Zero(t, initScanCutoff(scope), "a second Order is not a transparent scan input")
	require.Nil(t, op.ScanCutoff)
}

func TestScanCutoffReaderTopologyAfterRangeCollection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		data   engine.RelData
		merged bool
		want   bool
	}{
		{name: "blocks", data: readutil.NewBlockListRelationData(0), want: true},
		{name: "objects", data: &readutil.ObjListRelData{}, want: true},
		{name: "combined ranges", data: &disttae.CombinedRelData{}},
		{name: "unknown ranges", data: &readerPathAttachFailure{RelData: readutil.NewBlockListRelationData(0)}},
		{name: "merged readers", data: readutil.NewBlockListRelationData(0), merged: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewMockCompile(t)
			_, scan, sort := scanCutoffTestPlan()
			scan.TableDef.Pkey = &plan.PrimaryKeyDef{PkeyColName: "k"}
			scan.TableDef.Name2ColIndex = map[string]int32{"k": 0}
			tableOp := table_scan.NewArgument()
			defer tableOp.Release()
			op := order.NewArgument()
			defer op.Release()
			op.OrderBySpec, op.ScanCutoffLimit, op.ScanCutoffTag = sort.OrderBy, 17000, 42
			op.AppendChild(tableOp)
			rel := new(readerPathCaptureRelation)
			s := &Scope{Magic: Remote, Proc: c.proc, RootOp: op, NodeInfo: engine.Node{Mcpu: 1, CNCNT: 1},
				DataSource: &Source{Rel: rel, TableDef: scan.TableDef, OrderBy: scan.OrderBy, RecvMsgList: scan.RecvMsgList}}
			ranges := configureReaderPathTest(t, c, s)
			s.DataSource.node.NodeType = plan.Node_TABLE_SCAN
			ranges.rangesData = tc.data
			ds := readutil.NewRemoteDataSource(c.proc.Ctx, nil, timestamp.Timestamp{}, nil)
			rd, err := readutil.NewReader(c.proc.Ctx, c.proc.Mp(), nil, nil, scan.TableDef, timestamp.Timestamp{}, nil, ds, 0, engine.FilterHint{})
			require.NoError(t, err)
			var resultReader engine.Reader = rd
			if tc.merged {
				resultReader = readutil.NewMergeReader([]engine.Reader{rd, &readutil.EmptyReader{}})
			}
			defer func() { require.NoError(t, resultReader.Close()) }()
			rel.readers = []engine.Reader{resultReader}
			got, err := buildScanParallelRun(s, c)
			require.NoError(t, err)
			require.Same(t, s, got)
			require.Nil(t, s.NodeInfo.Data, "range ownership transferred to the reader")
			require.Equal(t, tc.want, op.ScanCutoff != nil)
			if tc.merged {
				require.Len(t, resultReader.GetOrderBy(), 1, "a merged reader can report only its first ordered child")
			}
		})
	}
}
