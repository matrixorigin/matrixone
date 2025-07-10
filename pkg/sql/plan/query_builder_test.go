// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildTable_AlterView(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	type arg struct {
		obj   *ObjectRef
		table *TableDef
	}
	store := make(map[string]arg)

	store["db.a"] = arg{
		&plan.ObjectRef{},
		&plan.TableDef{
			TableType: catalog.SystemOrdinaryRel,
			Cols: []*ColDef{
				{
					Name: "a",
					Typ: plan.Type{
						Id:    int32(types.T_varchar),
						Width: types.MaxVarcharLen,
						Table: "a",
					},
				},
			},
		}}

	vData, err := json.Marshal(ViewData{
		"create view v as select a from a",
		"db",
	})
	assert.NoError(t, err)

	store["db.v"] = arg{nil,
		&plan.TableDef{
			TableType: catalog.SystemViewRel,
			ViewSql: &plan.ViewDef{
				View: string(vData),
			}},
	}
	ctx := NewMockCompilerContext2(ctrl)
	ctx.EXPECT().ResolveVariable(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()
	ctx.EXPECT().Resolve(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(schemaName string, tableName string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
			if schemaName == "" {
				schemaName = "db"
			}
			x := store[schemaName+"."+tableName]
			return x.obj, x.table, nil
		}).AnyTimes()
	ctx.EXPECT().GetContext().Return(context.Background()).AnyTimes()
	ctx.EXPECT().GetProcess().Return(nil).AnyTimes()
	ctx.EXPECT().Stats(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	ctx.EXPECT().GetBuildingAlterView().Return(true, "db", "v").AnyTimes()
	ctx.EXPECT().DatabaseExists(gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	ctx.EXPECT().GetLowerCaseTableNames().Return(int64(1)).AnyTimes()
	ctx.EXPECT().GetSubscriptionMeta(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	qb := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
	tb := &tree.TableName{}
	tb.SchemaName = "db"
	tb.ObjectName = "v"
	bc := NewBindContext(qb, nil)
	_, err = qb.buildTable(tb, bc, -1, nil)
	assert.Error(t, err)
}

func Test_cte(t *testing.T) {
	sqls := []string{
		"select table_catalog, table_schema, table_name, table_type, engine\nfrom information_schema.tables\nwhere table_schema = 'mo_catalog' and table_type = 'BASE TABLE'\norder by table_name;",
	}
	testutil.NewProc(t)
	mock := NewMockOptimizer(false)

	for _, sql := range sqls {
		logicPlan, err := runOneStmt(mock, t, sql)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		outPutPlan(logicPlan, true, t)
	}
}

type Kase struct {
	sql     string
	comment string
}

var rightCases = []Kase{
	{
		"with recursive c as (select a from cte_test.t1 union all select a+1 from c where a < 2 union all select a from c where a < 2), d as (select a from c union all select a+1 from d where a < 2) select distinct tt.* from ( SELECT * FROM c UNION ALL SELECT * FROM d) tt order by tt.a;",
		"",
	},
	{
		"select * from cte_test.v2",
		"",
	},
	{
		"with \n    qn as (select * from t2),\n    qn2 as (\n        with qn3 as (select * from qn)\n        select * from qn3\n    )\nselect * from bvt_test2.t3 where exists (select * from qn);",
		"",
	},
	{
		"select information_schema.REFERENTIAL_CONSTRAINTS.CONSTRAINT_SCHEMA,\n       information_schema.REFERENTIAL_CONSTRAINTS.CONSTRAINT_NAME,\n       information_schema.REFERENTIAL_CONSTRAINTS.TABLE_NAME,\n       information_schema.REFERENTIAL_CONSTRAINTS.REFERENCED_TABLE_NAME,\n       information_schema.REFERENTIAL_CONSTRAINTS.UNIQUE_CONSTRAINT_NAME,\n       information_schema.REFERENTIAL_CONSTRAINTS.UNIQUE_CONSTRAINT_SCHEMA,\n       information_schema.KEY_COLUMN_USAGE.COLUMN_NAME\nfrom information_schema.REFERENTIAL_CONSTRAINTS\n         join information_schema.KEY_COLUMN_USAGE\n              on (information_schema.REFERENTIAL_CONSTRAINTS.CONSTRAINT_SCHEMA =\n                  information_schema.KEY_COLUMN_USAGE.CONSTRAINT_SCHEMA and\n                  information_schema.REFERENTIAL_CONSTRAINTS.CONSTRAINT_NAME =\n                  information_schema.KEY_COLUMN_USAGE.CONSTRAINT_NAME and\n                  information_schema.REFERENTIAL_CONSTRAINTS.TABLE_NAME =\n                  information_schema.KEY_COLUMN_USAGE.TABLE_NAME)\nwhere (information_schema.REFERENTIAL_CONSTRAINTS.CONSTRAINT_SCHEMA in ('plat_content') or\n       information_schema.REFERENTIAL_CONSTRAINTS.CONSTRAINT_SCHEMA in ('plat_content'))\norder by information_schema.KEY_COLUMN_USAGE.CONSTRAINT_SCHEMA asc,\n         information_schema.KEY_COLUMN_USAGE.CONSTRAINT_NAME asc,\n         information_schema.KEY_COLUMN_USAGE.ORDINAL_POSITION asc;",
		"",
	},
	{
		"select * from bvt_test3.cte_view order by EmployeeLevel",
		"",
	},
	{
		"select * from cte_test.c",
		"",
	},
	{
		"with \n\tc as (\n\t\tselect * from cte_test2.vt1\n\t)\nselect \n\t*\nfrom\n\t(\n\t\tselect * from c\n\t\tunion all\n\t\tselect * from cte_test2.vv1\n\t)",
		"",
	},
	{
		"select * from vv2; ",
		"",
	},
	{
		"select * from vv3; ",
		"",
	},
}

func TestRightCases(t *testing.T) {
	testutil.NewProc(t)
	mock := NewMockOptimizer(false)
	for _, kase := range rightCases {
		_, err := runOneStmt(mock, t, kase.sql)
		require.NoError(t, err, kase.comment, kase.sql)
	}
}

var wrongCases = []Kase{
	{
		"WITH qn2 AS (SELECT a FROM qn WHERE a IS NULL or a>0),\nqn AS (SELECT b as a FROM bvt_test1.t1)\nSELECT qn2.a  FROM qn2;",
		"SQL parser error: table \"qn\" does not exist",
	},
	{
		"with qn1 as (with qn3 as (select * from qn2) select * from qn3),\n     qn2 as (select 1)\nselect * from qn1;",
		"SQL parser error: table \"qn2\" does not exist",
	},
	{
		"WITH qn2 AS (SELECT a FROM qn WHERE a IS NULL or a>0),\nqn AS (SELECT b as a FROM qn2)\nSELECT qn.a  FROM qn;",

		"SQL parser error: table \"qn\" does not exist",
	},
	{

		"with qn as (select * from t2 where t2.b=t3.a)\nselect * from bvt_test2.t3 where exists (select * from qn);",
		"invalid input: missing FROM-clause entry for table 't3'",
	},
	{

		"with qn as (select * from t2 where t2.b=t3.a)\nselect * from bvt_test2.t3 where not exists (select * from qn);",

		"invalid input: missing FROM-clause entry for table 't3'",
	},
}

func TestWrongCases(t *testing.T) {
	testutil.NewProc(t)
	mock := NewMockOptimizer(false)
	for _, kase := range wrongCases {
		_, err := runOneStmt(mock, t, kase.sql)
		require.Error(t, err, kase.comment, kase.sql)
	}
}

func TestDefaultBigStats(t *testing.T) {
	stats := DefaultBigStats()
	require.Greater(t, stats.BlockNum, int32(BlockThresholdForOneCN))
}

func genBuilderAndCtx() (builder *QueryBuilder, bindCtx *BindContext) {
	builder = NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindCtx = NewBindContext(builder, nil)

	typ := types.T_int64.ToType()
	plan2Type := makePlan2Type(&typ)
	bind := &Binding{
		tag:            1,
		nodeId:         0,
		db:             "select_test",
		table:          "bind_select",
		tableID:        0,
		cols:           []string{"a", "b", "c"},
		colIsHidden:    []bool{false, false, false},
		types:          []*plan.Type{&plan2Type, &plan2Type, &plan2Type},
		refCnts:        []uint{0, 0, 0},
		colIdByName:    map[string]int32{"a": 0, "b": 1, "c": 2},
		isClusterTable: false,
		defaults:       []string{"", "", ""},
	}
	bindCtx.bindings = append(bindCtx.bindings, bind)
	bindCtx.bindingByTable[bind.table] = bind
	for _, col := range bind.cols {
		bindCtx.bindingByCol[col] = bind
	}
	bindCtx.bindingByTag[bind.tag] = bind
	return
}

func TestQueryBuilder_bindWhere(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()
	bindCtx.binder = NewWhereBinder(builder, bindCtx)

	stmts, _ := parsers.Parse(context.TODO(), dialect.MYSQL, "select * from select_test.bind_select where a > 0 and b < 0 or c = 0", 1)
	clause := stmts[0].(*tree.Select).Select.(*tree.SelectClause).Where

	newNodeID, boundFilterList, notCacheable, err := builder.bindWhere(bindCtx, clause, 0)
	require.NoError(t, err)
	require.Equal(t, int32(0), newNodeID)
	require.Equal(t, 1, len(boundFilterList))
	require.Equal(t, int32(types.T_bool), boundFilterList[0].Typ.Id)
	// a > 0 and b < 0 or c = 0
	{
		funcExpr0, ok := boundFilterList[0].Expr.(*plan.Expr_F)
		require.True(t, ok)
		require.Equal(t, "or", funcExpr0.F.Func.ObjName)
		require.Equal(t, 2, len(funcExpr0.F.Args))
		// a > 0 and b < 0
		{
			funcExpr1, ok := funcExpr0.F.Args[0].Expr.(*plan.Expr_F)
			require.True(t, ok)
			require.Equal(t, "and", funcExpr1.F.Func.ObjName)
			// a > 0
			funcExpr2, ok := funcExpr1.F.Args[0].Expr.(*plan.Expr_F)
			require.True(t, ok)
			require.Equal(t, ">", funcExpr2.F.Func.ObjName)
			require.Equal(t, "a", funcExpr2.F.Args[0].Expr.(*plan.Expr_Col).Col.Name)
			require.Equal(t, int64(0), funcExpr2.F.Args[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val)
			// b < 0
			funcExpr3, ok := funcExpr1.F.Args[1].Expr.(*plan.Expr_F)
			require.True(t, ok)
			require.Equal(t, "<", funcExpr3.F.Func.ObjName)
			require.Equal(t, "b", funcExpr3.F.Args[0].Expr.(*plan.Expr_Col).Col.Name)
			require.Equal(t, int64(0), funcExpr3.F.Args[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val)
		}
		// c = 0
		funcExpr4, ok := funcExpr0.F.Args[1].Expr.(*plan.Expr_F)
		require.True(t, ok)
		require.Equal(t, "=", funcExpr4.F.Func.ObjName)
		require.Equal(t, "c", funcExpr4.F.Args[0].Expr.(*plan.Expr_Col).Col.Name)
		require.Equal(t, int64(0), funcExpr4.F.Args[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val)
	}

	require.False(t, notCacheable)
}

func TestQueryBuilder_bindGroupBy(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()

	stmts, _ := parsers.Parse(context.TODO(), dialect.MYSQL, "select a from select_test.bind_select group by a", 1)
	clause := stmts[0].(*tree.Select).Select.(*tree.SelectClause).GroupBy

	_, err := builder.bindGroupBy(bindCtx, clause, nil, nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(bindCtx.groups))
	colExpr, ok := bindCtx.groups[0].Expr.(*plan.Expr_Col)
	require.True(t, ok)
	require.Equal(t, "a", colExpr.Col.Name)

	require.Equal(t, 1, len(bindCtx.groupByAst))
	require.Equal(t, int32(0), bindCtx.groupByAst["bind_select.a"])

	require.Equal(t, 1, len(bindCtx.groupingFlag))
	require.True(t, bindCtx.groupingFlag[0])

	// TODO time window ast
}

func TestQueryBuilder_bindHaving(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()

	stmts, _ := parsers.Parse(context.TODO(), dialect.MYSQL, "select a from select_test.bind_select group by a having a > 0", 1)
	selectClause := stmts[0].(*tree.Select).Select.(*tree.SelectClause)

	_, err := builder.bindGroupBy(bindCtx, selectClause.GroupBy, nil, nil, nil)
	require.NoError(t, err)

	boundHavingList, err := builder.bindHaving(bindCtx, selectClause.Having, NewHavingBinder(builder, bindCtx))
	require.NoError(t, err)
	require.Equal(t, 1, len(boundHavingList))
	require.Equal(t, int32(types.T_bool), boundHavingList[0].Typ.Id)

	funcExpr0, ok := boundHavingList[0].Expr.(*plan.Expr_F)
	require.True(t, ok)
	require.Equal(t, ">", funcExpr0.F.Func.ObjName)
}

func TestQueryBuilder_bindProjection(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()

	// select a, *
	selectList := tree.SelectExprs{
		tree.SelectExpr{
			Expr: tree.NewUnresolvedName(tree.NewCStr("bind_select", 0), tree.NewCStr("a", 0)),
		},
		tree.SelectExpr{
			Expr: tree.NewUnresolvedName(tree.NewCStr("bind_select", 0), tree.NewCStr("a", 0)),
		},
		tree.SelectExpr{
			Expr: tree.NewUnresolvedName(tree.NewCStr("bind_select", 0), tree.NewCStr("b", 0)),
		},
		tree.SelectExpr{
			Expr: tree.NewUnresolvedName(tree.NewCStr("bind_select", 0), tree.NewCStr("c", 0)),
		},
	}

	havingBinder := NewHavingBinder(builder, bindCtx)
	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)
	resultLen, _, err := builder.bindProjection(bindCtx, projectionBinder, selectList, false)
	require.NoError(t, err)
	require.Equal(t, 4, resultLen)
}

// TODO
func TestQueryBuilder_bindTimeWindow(t *testing.T) {}

func TestQueryBuilder_bindOrderBy(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()

	stmts, _ := parsers.Parse(context.TODO(), dialect.MYSQL, "select a, b from select_test.bind_select order by a desc, b asc", 1)
	orderList := stmts[0].(*tree.Select).OrderBy

	havingBinder := NewHavingBinder(builder, bindCtx)
	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)
	boundOrderBys, err := builder.bindOrderBy(bindCtx, orderList, projectionBinder, nil)
	require.NoError(t, err)
	require.Equal(t, 2, len(boundOrderBys))
	require.Equal(t, int32(types.T_int64), boundOrderBys[0].Expr.Typ.Id)
	_, ok := boundOrderBys[0].Expr.Expr.(*plan.Expr_Col)
	require.True(t, ok)
	require.Equal(t, plan.OrderBySpec_DESC, boundOrderBys[0].Flag)

	require.Equal(t, int32(types.T_int64), boundOrderBys[1].Expr.Typ.Id)
	_, ok = boundOrderBys[1].Expr.Expr.(*plan.Expr_Col)
	require.True(t, ok)
	require.Equal(t, plan.OrderBySpec_ASC, boundOrderBys[1].Flag)
}

func TestQueryBuilder_bindLimit(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()

	stmts, _ := parsers.Parse(context.TODO(), dialect.MYSQL, "select a from select_test.bind_select limit 1, 5", 1)
	astLimit := stmts[0].(*tree.Select).Limit

	boundOffsetExpr, boundCountExpr, err := builder.bindLimit(bindCtx, astLimit)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_uint64), boundOffsetExpr.Typ.Id)
	offsetExpr, ok := boundOffsetExpr.Expr.(*plan.Expr_Lit)
	require.True(t, ok)
	require.Equal(t, uint64(1), offsetExpr.Lit.Value.(*plan.Literal_U64Val).U64Val)

	require.Equal(t, int32(types.T_uint64), boundCountExpr.Typ.Id)
	countExpr, ok := boundCountExpr.Expr.(*plan.Expr_Lit)
	require.True(t, ok)
	require.Equal(t, uint64(5), countExpr.Lit.Value.(*plan.Literal_U64Val).U64Val)
}

func TestQueryBuilder_bindValues(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindCtx := NewBindContext(builder, nil)

	stmts, _ := parsers.Parse(context.TODO(), dialect.MYSQL, "select a from (values row(1)) as tmp(a)", 1)
	tables := stmts[0].(*tree.Select).Select.(*tree.SelectClause).From.Tables
	joinTable := tables[0].(*tree.JoinTableExpr)
	aliasedTable := joinTable.Left.(*tree.AliasedTableExpr)
	parenTable := aliasedTable.Expr.(*tree.ParenTableExpr)
	valuesClause := parenTable.Expr.(*tree.Select).Select.(*tree.ValuesClause)

	nodeID, selectList, err := builder.bindValues(bindCtx, valuesClause)
	assert.NoError(t, err)
	assert.Equal(t, int32(0), nodeID)
	assert.Equal(t, 1, len(selectList))
}

func TestQueryBuilder_appendWhereNode(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindCtx := NewBindContext(builder, nil)

	stmts, _ := parsers.Parse(context.TODO(), dialect.MYSQL, "select * from select_test.bind_select where a = 0", 1)
	selectClause := stmts[0].(*tree.Select).Select.(*tree.SelectClause)

	nodeID, err := builder.buildFrom(selectClause.From.Tables, bindCtx, true)
	require.NoError(t, err)
	require.Equal(t, int32(0), nodeID)

	bindCtx.binder = NewWhereBinder(builder, bindCtx)
	nodeID, boundFilterList, notCacheable, err := builder.bindWhere(bindCtx, selectClause.Where, nodeID)
	require.NoError(t, err)

	nodeID = builder.appendWhereNode(bindCtx, nodeID, boundFilterList, notCacheable)
	require.Equal(t, int32(1), nodeID)
	require.Equal(t, 2, len(builder.qry.Nodes))

	filterNode := builder.qry.Nodes[1]
	require.Equal(t, int32(0), filterNode.Children[0])
	require.Equal(t, plan.Node_FILTER, filterNode.NodeType)
	require.Equal(t, "=", filterNode.FilterList[0].Expr.(*plan.Expr_F).F.Func.ObjName)
	require.Equal(t, "a", filterNode.FilterList[0].Expr.(*plan.Expr_F).F.Args[0].Expr.(*plan.Expr_Col).Col.Name)
	require.Equal(t, int64(0), filterNode.FilterList[0].Expr.(*plan.Expr_F).F.Args[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val)
}

// TODO
func TestQueryBuilder_appendSampleNode(t *testing.T) {}

func TestQueryBuilder_appendAggNode(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindCtx := NewBindContext(builder, nil)

	stmts, _ := parsers.Parse(context.TODO(), dialect.MYSQL, "select a from select_test.bind_select group by a having a > 0", 1)
	selectClause := stmts[0].(*tree.Select).Select.(*tree.SelectClause)

	nodeID, err := builder.buildFrom(selectClause.From.Tables, bindCtx, true)
	require.NoError(t, err)
	require.Equal(t, int32(0), nodeID)

	_, err = builder.bindGroupBy(bindCtx, selectClause.GroupBy, nil, nil, nil)
	require.NoError(t, err)
	boundHavingList, err := builder.bindHaving(bindCtx, selectClause.Having, NewHavingBinder(builder, bindCtx))
	require.NoError(t, err)

	nodeID, err = builder.appendAggNode(bindCtx, nodeID, boundHavingList, false)
	require.NoError(t, err)
	require.Equal(t, int32(2), nodeID)
	require.Equal(t, 3, len(builder.qry.Nodes))

	aggNode := builder.qry.Nodes[1]
	require.Equal(t, int32(1), aggNode.NodeId)
	require.Equal(t, int32(0), aggNode.Children[0])
	require.Equal(t, plan.Node_AGG, aggNode.NodeType)
	require.Equal(t, "a", aggNode.GroupBy[0].Expr.(*plan.Expr_Col).Col.Name)
	require.True(t, aggNode.GroupingFlag[0])

	filterNode := builder.qry.Nodes[2]
	require.Equal(t, int32(2), filterNode.NodeId)
	require.Equal(t, int32(1), filterNode.Children[0])
	require.Equal(t, plan.Node_FILTER, filterNode.NodeType)
	require.Equal(t, ">", filterNode.FilterList[0].Expr.(*plan.Expr_F).F.Func.ObjName)
	require.Equal(t, "", filterNode.FilterList[0].Expr.(*plan.Expr_F).F.Args[0].Expr.(*plan.Expr_Col).Col.Name)
	require.Equal(t, int64(0), filterNode.FilterList[0].Expr.(*plan.Expr_F).F.Args[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val)
}

// TODO
func TestQueryBuilder_appendTimeWindowNode(t *testing.T) {}

// TODO
func TestQueryBuilder_appendWindowNode(t *testing.T) {}

func TestQueryBuilder_appendProjectionNode(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindCtx := NewBindContext(builder, nil)

	stmts, _ := parsers.Parse(context.TODO(), dialect.MYSQL, "select a from select_test.bind_select", 1)
	selectClause := stmts[0].(*tree.Select).Select.(*tree.SelectClause)

	nodeID, selectList, _, notCacheable, _, _, _, _ := builder.bindSelectClause(bindCtx, selectClause, nil, nil, nil, true)

	havingBinder := NewHavingBinder(builder, bindCtx)
	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)
	_, notCacheable, _ = builder.bindProjection(bindCtx, projectionBinder, selectList, notCacheable)

	nodeID, err := builder.appendProjectionNode(bindCtx, nodeID, notCacheable)
	require.NoError(t, err)
	require.Equal(t, int32(1), nodeID)

	projectionNode := builder.qry.Nodes[1]
	require.Equal(t, int32(1), projectionNode.NodeId)
	require.Equal(t, int32(0), projectionNode.Children[0])
}

func TestQueryBuilder_appendDistinctNode(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindCtx := NewBindContext(builder, nil)

	stmts, _ := parsers.Parse(context.TODO(), dialect.MYSQL, "select distinct a from select_test.bind_select", 1)
	selectClause := stmts[0].(*tree.Select).Select.(*tree.SelectClause)

	nodeID, err := builder.buildFrom(selectClause.From.Tables, bindCtx, true)
	require.NoError(t, err)
	require.Equal(t, int32(0), nodeID)

	nodeID = builder.appendDistinctNode(bindCtx, nodeID)
	require.Equal(t, int32(1), nodeID)

	distinctNode := builder.qry.Nodes[1]
	require.Equal(t, int32(1), distinctNode.NodeId)
	require.Equal(t, int32(0), distinctNode.Children[0])
}

func TestQueryBuilder_appendSortNode(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindCtx := NewBindContext(builder, nil)

	stmts, _ := parsers.Parse(context.TODO(), dialect.MYSQL, "select a from select_test.bind_select order by a", 1)
	selectClause := stmts[0].(*tree.Select).Select.(*tree.SelectClause)
	orderList := stmts[0].(*tree.Select).OrderBy

	nodeID, err := builder.buildFrom(selectClause.From.Tables, bindCtx, true)
	require.NoError(t, err)
	require.Equal(t, int32(0), nodeID)

	havingBinder := NewHavingBinder(builder, bindCtx)
	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)
	boundOrderBys, _ := builder.bindOrderBy(bindCtx, orderList, projectionBinder, nil)

	nodeID = builder.appendSortNode(bindCtx, nodeID, boundOrderBys)
	require.Equal(t, int32(1), nodeID)

	sortNode := builder.qry.Nodes[1]
	require.Equal(t, int32(1), sortNode.NodeId)
	require.Equal(t, int32(0), sortNode.Children[0])
}

func TestQueryBuilder_appendResultProjectionNode(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindCtx := NewBindContext(builder, nil)

	stmts, _ := parsers.Parse(context.TODO(), dialect.MYSQL, "select a from select_test.bind_select order by a", 1)
	selectClause := stmts[0].(*tree.Select).Select.(*tree.SelectClause)
	orderList := stmts[0].(*tree.Select).OrderBy

	// bind select clause
	nodeID, selectList, _, notCacheable, _, havingBinder, _, _ := builder.bindSelectClause(bindCtx, selectClause, nil, nil, nil, true)
	// bind projection
	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)
	resultLen, notCacheable, _ := builder.bindProjection(bindCtx, projectionBinder, selectList, notCacheable)
	// bind	order by
	boundOrderBys, _ := builder.bindOrderBy(bindCtx, orderList, projectionBinder, nil)

	// append projection node
	nodeID, _ = builder.appendProjectionNode(bindCtx, nodeID, notCacheable)
	// append sort node
	nodeID = builder.appendSortNode(bindCtx, nodeID, boundOrderBys)
	// append result projection node
	nodeID = builder.appendResultProjectionNode(bindCtx, nodeID, resultLen)
	require.Equal(t, int32(3), nodeID)

	resultProjectionNode := builder.qry.Nodes[3]
	require.Equal(t, int32(3), resultProjectionNode.NodeId)
	require.Equal(t, int32(2), resultProjectionNode.Children[0])
}
