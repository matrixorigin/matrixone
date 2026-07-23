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
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	sqliceberg "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChooseRowCarrier(t *testing.T) {
	makeExpr := func(typ types.T, width int32) *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(typ), Width: width}}
	}
	makeAgg := func(name string, typ types.T, width int32, args ...*plan.Expr) *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{Id: int32(typ), Width: width},
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: name},
				Args: args,
			}},
		}
	}

	t.Run("fixed width before varlen", func(t *testing.T) {
		exprs := []*plan.Expr{
			makeExpr(types.T_varchar, 8),
			makeExpr(types.T_int64, 0),
			makeExpr(types.T_int32, 0),
		}
		require.Equal(t, 2, chooseRowCarrier(exprs, false))
	})

	t.Run("short varlen before long varlen", func(t *testing.T) {
		exprs := []*plan.Expr{
			makeExpr(types.T_varchar, 1024),
			makeExpr(types.T_varchar, 32),
		}
		require.Equal(t, 1, chooseRowCarrier(exprs, false))
	})

	t.Run("stable tie break", func(t *testing.T) {
		exprs := []*plan.Expr{
			makeExpr(types.T_int32, 0),
			makeExpr(types.T_int32, 0),
		}
		require.Equal(t, 0, chooseRowCarrier(exprs, false))
	})

	t.Run("table columns use the same cost model", func(t *testing.T) {
		cols := []*plan.ColDef{
			{Name: "payload", Typ: plan.Type{Id: int32(types.T_varchar), Width: 4096}},
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int8)}},
		}
		require.Equal(t, 1, chooseTableRowCarrier(plan.Node_TABLE_SCAN, cols))
	})

	t.Run("external scan excludes hidden columns", func(t *testing.T) {
		cols := []*plan.ColDef{
			{Name: "payload", Typ: plan.Type{Id: int32(types.T_varchar), Width: 4096}},
			{Name: "__mo_fake_pk_col", Hidden: true, Typ: plan.Type{Id: int32(types.T_uint64)}},
		}
		require.Equal(t, 0, chooseTableRowCarrier(plan.Node_EXTERNAL_SCAN, cols))
		require.Equal(t, 1, chooseTableRowCarrier(plan.Node_TABLE_SCAN, cols))
	})

	t.Run("internal and unknown types are conservative", func(t *testing.T) {
		for _, typ := range []int32{
			-1,
			int32(types.T_any),
			int32(types.T_interval),
			int32(types.T_tuple),
			256,
			999,
		} {
			exprs := []*plan.Expr{
				{Typ: plan.Type{Id: typ}},
				makeExpr(types.T_int8, 0),
			}
			require.Equal(t, 1, chooseRowCarrier(exprs, false), "type %d", typ)
		}
	})

	t.Run("cheap aggregate before expensive varlen aggregate", func(t *testing.T) {
		exprs := []*plan.Expr{
			makeAgg("group_concat", types.T_text, 0),
			makeAgg("sum", types.T_int64, 0),
			makeAgg("starcount", types.T_int64, 0),
		}
		require.Equal(t, 2, chooseRowCarrier(exprs, true))
	})

	t.Run("aggregate input width affects cost", func(t *testing.T) {
		exprs := []*plan.Expr{
			makeAgg("count", types.T_int64, 0, makeExpr(types.T_varchar, 4096)),
			makeAgg("sum", types.T_int64, 0, makeExpr(types.T_int32, 0)),
		}
		require.Equal(t, 1, chooseRowCarrier(exprs, true))
	})

	t.Run("union combines both inputs", func(t *testing.T) {
		left := []*plan.Expr{
			makeExpr(types.T_int16, 0),
			makeExpr(types.T_int32, 0),
		}
		right := []*plan.Expr{
			makeExpr(types.T_varchar, 8),
			makeExpr(types.T_int32, 0),
		}
		require.Equal(t, 1, chooseUnionRowCarrier(left, right, 2))
	})
}

func TestCanPruneSampleExprs(t *testing.T) {
	makeCol := func(tag, pos int32, notNullable bool) *plan.Expr {
		return &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_int64), NotNullable: notNullable},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tag, ColPos: pos}},
		}
	}
	makeScan := func(tag int32, notNullable ...bool) *plan.Node {
		cols := make([]*plan.ColDef, len(notNullable))
		for i := range notNullable {
			cols[i] = &plan.ColDef{Typ: plan.Type{Id: int32(types.T_int64), NotNullable: notNullable[i]}}
		}
		return &plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			BindingTags: []int32{tag},
			TableDef:    &plan.TableDef{Cols: cols},
		}
	}

	t.Run("single expression needs no cross-expression proof", func(t *testing.T) {
		node := &plan.Node{AggList: []*plan.Expr{{Expr: &plan.Expr_F{F: &plan.Function{}}}}}
		require.True(t, canPruneSampleExprs(node, &plan.Node{NodeType: plan.Node_PROJECT}))
	})

	t.Run("direct not null table columns", func(t *testing.T) {
		node := &plan.Node{AggList: []*plan.Expr{makeCol(7, 0, true), makeCol(7, 1, true)}}
		require.True(t, canPruneSampleExprs(node, makeScan(7, true, true)))
	})

	t.Run("function inference is not runtime proof", func(t *testing.T) {
		functionExpr := &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_json), NotNullable: true},
			Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "json_extract"}}},
		}
		node := &plan.Node{AggList: []*plan.Expr{functionExpr, makeCol(7, 1, true)}}
		require.False(t, canPruneSampleExprs(node, makeScan(7, true, true)))
	})

	t.Run("derived projection is not runtime proof", func(t *testing.T) {
		node := &plan.Node{AggList: []*plan.Expr{makeCol(7, 0, true), makeCol(7, 1, true)}}
		child := makeScan(7, true, true)
		child.NodeType = plan.Node_PROJECT
		require.False(t, canPruneSampleExprs(node, child))
	})

	t.Run("nullable table column", func(t *testing.T) {
		node := &plan.Node{AggList: []*plan.Expr{makeCol(7, 0, true), makeCol(7, 1, true)}}
		require.False(t, canPruneSampleExprs(node, makeScan(7, true, false)))
	})
}

func TestExprCanRemoveProjectFunctionMetadata(t *testing.T) {
	bind := func(name string, args ...*plan.Expr) *plan.Expr {
		expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
		require.NoError(t, err)
		return expr
	}
	sequence := func(name string) *plan.Expr {
		return MakePlan2StringConstExprWithType(name)
	}

	require.True(t, exprCanRemoveProject(bind("abs", MakePlan2Int64ConstExprWithType(1))))
	require.False(t, exprCanRemoveProject(bind("sleep", MakePlan2Int64ConstExprWithType(0))))
	require.False(t, exprCanRemoveProject(bind("nextval", sequence("sample_seq"))))
	require.False(t, exprCanRemoveProject(bind("setval", sequence("sample_seq"), sequence("10"))))

	nested := bind("isnull", bind("nextval", sequence("sample_seq")))
	require.False(t, exprCanRemoveProject(nested))

	unknown := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: -1}}}}
	require.False(t, exprCanRemoveProject(unknown))
}

func TestRemoveSimpleProjectionsVolatileRefCount(t *testing.T) {
	for _, test := range []struct {
		name        string
		refCnt      int
		parentType  plan.Node_NodeType
		wantProject bool
	}{
		{name: "unused", refCnt: 0, parentType: plan.Node_PROJECT, wantProject: true},
		{name: "single project consumer", refCnt: 1, parentType: plan.Node_PROJECT, wantProject: true},
		{name: "single sort consumer", refCnt: 1, parentType: plan.Node_SORT, wantProject: true},
		{name: "single join consumer", refCnt: 1, parentType: plan.Node_JOIN, wantProject: true},
		{name: "multiple consumers", refCnt: 2, parentType: plan.Node_PROJECT, wantProject: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			builder, bindCtx := genBuilderAndCtx()
			volatileExpr, err := BindFuncExprImplByPlanExpr(
				context.Background(),
				"nextval",
				[]*plan.Expr{MakePlan2StringConstExprWithType("sample_seq")},
			)
			require.NoError(t, err)

			scanID := builder.appendNode(&plan.Node{NodeType: plan.Node_TABLE_SCAN}, bindCtx)
			projectTag := builder.genNewBindTag()
			projectID := builder.appendNode(&plan.Node{
				NodeType:    plan.Node_PROJECT,
				Children:    []int32{scanID},
				BindingTags: []int32{projectTag},
				ProjectList: []*plan.Expr{volatileExpr},
			}, bindCtx)
			refCnts := map[[2]int32]int{{projectTag, 0}: test.refCnt}

			newNodeID, _ := builder.removeSimpleProjections(projectID, test.parentType, false, refCnts)
			require.Equal(t, test.wantProject, newNodeID == projectID)
		})
	}
}

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
		Stmt:            "create view v as select a from a",
		DefaultDatabase: "db",
		SecurityType:    "DEFINER",
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

func TestBindViewUsesStoredSQLModeForPipesAsConcat(t *testing.T) {
	sqlMode := "PIPES_AS_CONCAT"
	builder, nodeID := buildViewForSQLModeTest(t, "v_pipe", ViewData{
		Stmt:            "create view v_pipe as select a||b as c from t",
		DefaultDatabase: "db",
		SQLMode:         &sqlMode,
		SecurityType:    "DEFINER",
	})

	projectExpr := builder.qry.Nodes[nodeID].ProjectList[0]
	require.True(t, exprContainsFunc(projectExpr, "concat"))
	require.False(t, exprContainsFunc(projectExpr, "or"))
}

func TestBindViewUsesStoredSQLModeForANSIQuotes(t *testing.T) {
	sqlMode := "ANSI_QUOTES"
	builder, nodeID := buildViewForSQLModeTest(t, "v_ansi", ViewData{
		Stmt:            `create view v_ansi as select "a" as c from t`,
		DefaultDatabase: "db",
		SQLMode:         &sqlMode,
		SecurityType:    "DEFINER",
	})

	projectExpr := builder.qry.Nodes[nodeID].ProjectList[0]
	require.NotNil(t, projectExpr.GetCol())
	require.Equal(t, "a", projectExpr.GetCol().Name)
}

func TestBindViewWithoutStoredSQLModeUsesLegacyPipeConcat(t *testing.T) {
	builder, nodeID := buildViewForSQLModeTest(t, "v_legacy_pipe", ViewData{
		Stmt:            "create view v_legacy_pipe as select a||b as c from t",
		DefaultDatabase: "db",
		SecurityType:    "DEFINER",
	})

	projectExpr := builder.qry.Nodes[nodeID].ProjectList[0]
	require.True(t, exprContainsFunc(projectExpr, "concat"))
	require.False(t, exprContainsFunc(projectExpr, "or"))
}

func buildViewForSQLModeTest(t *testing.T, viewName string, viewData ViewData) (*QueryBuilder, int32) {
	t.Helper()
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	type arg struct {
		obj   *ObjectRef
		table *TableDef
	}
	viewJSON, err := json.Marshal(viewData)
	require.NoError(t, err)

	store := map[string]arg{
		"db.t": {
			obj: &plan.ObjectRef{SchemaName: "db", ObjName: "t"},
			table: &plan.TableDef{
				DbName:    "db",
				Name:      "t",
				TableType: catalog.SystemOrdinaryRel,
				Cols: []*ColDef{
					{Name: "a", Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen, Table: "t"}},
					{Name: "b", Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen, Table: "t"}},
				},
			},
		},
		"db." + viewName: {
			obj: &plan.ObjectRef{SchemaName: "db", ObjName: viewName},
			table: &plan.TableDef{
				DbName:    "db",
				Name:      viewName,
				TableType: catalog.SystemViewRel,
				ViewSql:   &plan.ViewDef{View: string(viewJSON)},
			},
		},
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
	ctx.EXPECT().GetBuildingAlterView().Return(false, "", "").AnyTimes()
	ctx.EXPECT().DatabaseExists(gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	ctx.EXPECT().GetLowerCaseTableNames().Return(int64(1)).AnyTimes()
	ctx.EXPECT().GetSubscriptionMeta(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	ctx.EXPECT().DefaultDatabase().Return("db").AnyTimes()
	ctx.EXPECT().GetAccountId().Return(uint32(0), nil).AnyTimes()
	ctx.EXPECT().GetQueryingSubscription().Return(nil).AnyTimes()

	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
	bindCtx := NewBindContext(builder, nil)
	tableName := &tree.TableName{}
	tableName.SchemaName = "db"
	tableName.ObjectName = tree.Identifier(viewName)
	nodeID, err := builder.buildTable(tableName, bindCtx, -1, nil)
	require.NoError(t, err)
	require.Equal(t, plan.Node_PROJECT, builder.qry.Nodes[nodeID].NodeType)
	require.Len(t, builder.qry.Nodes[nodeID].ProjectList, 1)
	return builder, nodeID
}

func exprContainsFunc(expr *plan.Expr, name string) bool {
	if expr == nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil {
		return false
	}
	if fn.Func.GetObjName() == name {
		return true
	}
	for _, arg := range fn.Args {
		if exprContainsFunc(arg, name) {
			return true
		}
	}
	return false
}

func TestTempTableAliasBindingUsesOriginName(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := NewMockCompilerContext2(ctrl)
	ctx.EXPECT().ResolveVariable(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()
	ctx.EXPECT().Resolve(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(schemaName string, tableName string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
			if schemaName == "" {
				schemaName = "db"
			}
			obj := &plan.ObjectRef{
				SchemaName: schemaName,
				ObjName:    "__mo_tmp_real",
			}
			tbl := &plan.TableDef{
				DbName: schemaName,
				Name:   "__mo_tmp_real",
				Cols: []*plan.ColDef{
					{
						Name: "a",
						Typ:  plan.Type{Id: int32(types.T_int64)},
					},
					{
						Name: "b",
						Typ:  plan.Type{Id: int32(types.T_int64)},
					},
				},
			}
			return obj, tbl, nil
		}).AnyTimes()
	ctx.EXPECT().GetContext().Return(context.Background()).AnyTimes()
	ctx.EXPECT().GetProcess().Return(nil).AnyTimes()
	ctx.EXPECT().Stats(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	ctx.EXPECT().GetBuildingAlterView().Return(false, "", "").AnyTimes()
	ctx.EXPECT().DatabaseExists(gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	ctx.EXPECT().GetLowerCaseTableNames().Return(int64(1)).AnyTimes()
	ctx.EXPECT().GetSubscriptionMeta(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	ctx.EXPECT().DefaultDatabase().Return("db").AnyTimes()
	ctx.EXPECT().GetAccountId().Return(uint32(0), nil).AnyTimes()
	ctx.EXPECT().GetQueryingSubscription().Return(nil).AnyTimes()

	qb := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)
	bc := NewBindContext(qb, nil)
	tb := &tree.TableName{}
	tb.SchemaName = "db"
	tb.ObjectName = "t1"
	nodeID, err := qb.buildTable(&tree.AliasedTableExpr{Expr: tb}, bc, -1, nil)
	require.NoError(t, err)

	_, ok := bc.bindingByTable["t1"]
	require.True(t, ok)
	_, ok = bc.bindingByTable["__mo_tmp_real"]
	require.False(t, ok)

	require.Equal(t, "t1", qb.qry.Nodes[nodeID].TableDef.OriginalName)
	require.Equal(t, "__mo_tmp_real", qb.qry.Nodes[nodeID].TableDef.Name)
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

func TestAppendSelectListNameConstHeading(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindCtx := NewBindContext(builder, nil)

	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, "select name_const('myname', 14), name_const(123, -456), name_const('myname', 14) as alias_name, name_const(('paren_name'), (14))", 1)
	require.NoError(t, err)
	selectClause := stmts[0].(*tree.Select).Select.(*tree.SelectClause)

	_, err = appendSelectList(builder, bindCtx, nil, selectClause.Exprs...)
	require.NoError(t, err)
	require.Equal(t, []string{"myname", "123", "alias_name", "paren_name"}, bindCtx.headings)
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

func TestQueryBuilder_bindGroupByNull(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()

	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, "select count(*) from select_test.bind_select group by null", 1)
	require.NoError(t, err)
	selectClause := stmts[0].(*tree.Select).Select.(*tree.SelectClause)

	_, err = builder.bindGroupBy(bindCtx, selectClause.GroupBy, selectClause.Exprs, nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(bindCtx.groups))
	require.True(t, isNullExpr(bindCtx.groups[0]))

	require.Equal(t, 1, len(bindCtx.groupingFlag))
	require.True(t, bindCtx.groupingFlag[0])
}

func TestQueryBuilder_bindGroupByOrdinalPosition(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()

	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, "select a + 1 as x from select_test.bind_select group by 1", 1)
	require.NoError(t, err)
	selectClause := stmts[0].(*tree.Select).Select.(*tree.SelectClause)

	_, err = builder.bindGroupBy(bindCtx, selectClause.GroupBy, selectClause.Exprs, nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(bindCtx.groups))
	funcExpr, ok := bindCtx.groups[0].Expr.(*plan.Expr_F)
	require.True(t, ok)
	require.Equal(t, "+", funcExpr.F.Func.ObjName)
	require.Equal(t, "a", funcExpr.F.Args[0].Expr.(*plan.Expr_Col).Col.Name)
	require.Equal(t, int64(1), funcExpr.F.Args[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val)
}

func TestQueryBuilderBuildRollupOrderByGroupingExpression(t *testing.T) {
	stmts, err := parsers.Parse(
		context.TODO(),
		dialect.MYSQL,
		`select grouping(a) as grouping_a, grouping(b) as grouping_b, count(*)
		from select_test.bind_select
		group by a, b with rollup
		order by GROUPING(A), GROUPING(B)`,
		1,
	)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(NewMockCompilerContext(true), stmts[0], false)
	require.NoError(t, err)

	query := queryPlan.GetQuery()
	require.NotEmpty(t, query.Steps)
	rootNode := query.Nodes[query.Steps[len(query.Steps)-1]]
	require.Equal(t, plan.Node_PROJECT, rootNode.NodeType)
	require.Len(t, rootNode.ProjectList, 3)

	var sortNode *plan.Node
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_SORT {
			sortNode = node
			break
		}
	}
	require.NotNil(t, sortNode)
	require.Len(t, sortNode.OrderBy, 2)
	require.Equal(t, int32(3), sortNode.OrderBy[0].Expr.GetCol().ColPos)
	require.Equal(t, int32(4), sortNode.OrderBy[1].Expr.GetCol().ColPos)
	require.Len(t, query.Nodes[sortNode.Children[0]].ProjectList, 5)
}

func TestQueryBuilderBuildRollupOrderByGroupingExpressionWithStar(t *testing.T) {
	testCases := []struct {
		name       string
		selectList string
	}{
		{name: "star", selectList: "*"},
		{name: "qualified star", selectList: "t.*"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			stmts, err := parsers.Parse(
				context.TODO(),
				dialect.MYSQL,
				fmt.Sprintf(`select %s from select_test.bind_select as t
				group by a, b, c with rollup order by grouping(a)`, testCase.selectList),
				1,
			)
			require.NoError(t, err)

			queryPlan, err := BuildPlan(NewMockCompilerContext(true), stmts[0], false)
			require.NoError(t, err)

			query := queryPlan.GetQuery()
			rootNode := query.Nodes[query.Steps[len(query.Steps)-1]]
			require.Equal(t, plan.Node_PROJECT, rootNode.NodeType)
			require.Len(t, rootNode.ProjectList, 3)
			require.Len(t, query.Headings, 3)

			var sortNode *plan.Node
			for _, node := range query.Nodes {
				if node.NodeType == plan.Node_SORT {
					sortNode = node
					break
				}
			}
			require.NotNil(t, sortNode)
			require.Len(t, query.Nodes[sortNode.Children[0]].ProjectList, 4)
		})
	}
}

func TestQueryBuilderBuildRollupOrderByGroupingExpressionUsesSelectAlias(t *testing.T) {
	stmts, err := parsers.Parse(
		context.TODO(),
		dialect.MYSQL,
		`select 100 as b, grouping(a) as grouping_a, count(*)
		from select_test.bind_select
		group by a, b with rollup
		order by grouping(a) + b`,
		1,
	)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(NewMockCompilerContext(true), stmts[0], false)
	require.NoError(t, err)

	foundHiddenProject := false
	for _, node := range queryPlan.GetQuery().Nodes {
		if node.NodeType != plan.Node_PROJECT || len(node.ProjectList) != 4 {
			continue
		}
		hiddenFunc := node.ProjectList[3].GetF()
		if hiddenFunc == nil || hiddenFunc.Func.ObjName != "+" {
			continue
		}
		foundHiddenProject = true
		require.Len(t, hiddenFunc.Args, 2)
		require.True(t, planExprContainsInt64Literal(hiddenFunc.Args[1], 100))
	}
	require.True(t, foundHiddenProject)
}

func planExprContainsInt64Literal(expr *plan.Expr, expected int64) bool {
	if literal := expr.GetLit(); literal != nil && literal.GetI64Val() == expected {
		return true
	}
	if function := expr.GetF(); function != nil {
		for _, arg := range function.Args {
			if planExprContainsInt64Literal(arg, expected) {
				return true
			}
		}
	}
	return false
}

func TestQueryBuilderBuildRollupOrderByGroupingExpressionIsReentrant(t *testing.T) {
	for _, isPrepare := range []bool{false, true} {
		t.Run(fmt.Sprintf("prepare=%t", isPrepare), func(t *testing.T) {
			stmts, err := parsers.Parse(
				context.TODO(),
				dialect.MYSQL,
				`select grouping(a), count(*) from select_test.bind_select
				group by a with rollup order by grouping(a) + 1`,
				1,
			)
			require.NoError(t, err)

			selectStmt := stmts[0].(*tree.Select)
			orderExpr := tree.String(selectStmt.OrderBy[0].Expr, dialect.MYSQL)
			firstNodeCount := 0
			for buildIndex := range 2 {
				queryPlan, buildErr := BuildPlan(NewMockCompilerContext(true), stmts[0], isPrepare)
				require.NoError(t, buildErr)
				require.NotNil(t, queryPlan.GetQuery())
				require.Equal(t, orderExpr, tree.String(selectStmt.OrderBy[0].Expr, dialect.MYSQL))
				if buildIndex == 0 {
					firstNodeCount = len(queryPlan.GetQuery().Nodes)
				} else {
					require.Len(t, queryPlan.GetQuery().Nodes, firstNodeCount)
				}
			}
		})
	}
}

func TestQueryBuilderBuildRollupOrderByQualifiedGroupingColumn(t *testing.T) {
	stmts, err := parsers.Parse(
		context.TODO(),
		dialect.MYSQL,
		`select grouping(a) as grouping_a, count(*)
		from select_test.bind_select as t
		group by a with rollup
		order by grouping(a)`,
		1,
	)
	require.NoError(t, err)

	selectStmt := stmts[0].(*tree.Select)
	selectClause := selectStmt.Select.(*tree.SelectClause)
	orderFunc := selectStmt.OrderBy[0].Expr.(*tree.FuncExpr)
	orderFunc.Exprs[0] = tree.NewUnresolvedName(tree.NewCStr("t", 0), tree.NewCStr("a", 0))

	queryPlan, err := BuildPlan(NewMockCompilerContext(true), stmts[0], false)
	require.NoError(t, err)

	query := queryPlan.GetQuery()
	rootNode := query.Nodes[query.Steps[len(query.Steps)-1]]
	require.Equal(t, plan.Node_PROJECT, rootNode.NodeType)
	require.Len(t, rootNode.ProjectList, len(selectClause.Exprs))
	require.Len(t, query.Headings, len(selectClause.Exprs))

	var sortNode *plan.Node
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_SORT {
			sortNode = node
			break
		}
	}
	require.NotNil(t, sortNode)
	require.Len(t, sortNode.OrderBy, 1)
	require.Equal(t, int32(2), sortNode.OrderBy[0].Expr.GetCol().ColPos)
	require.Len(t, query.Nodes[sortNode.Children[0]].ProjectList, 3)
}

func TestQueryBuilderBuildRollupOrderByGroupingExpressionIgnoresAliasShadowing(t *testing.T) {
	stmts, err := parsers.Parse(
		context.TODO(),
		dialect.MYSQL,
		`select grouping(a) as b, grouping(b) as grouping_b, count(*)
		from select_test.bind_select
		group by a, b with rollup
		order by grouping(b)`,
		1,
	)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(NewMockCompilerContext(true), stmts[0], false)
	require.NoError(t, err)

	query := queryPlan.GetQuery()
	var sortNode *plan.Node
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_SORT {
			sortNode = node
			break
		}
	}
	require.NotNil(t, sortNode)
	require.Len(t, sortNode.OrderBy, 1)
	require.NotNil(t, sortNode.OrderBy[0].Expr.GetCol())
	require.Equal(t, int32(3), sortNode.OrderBy[0].Expr.GetCol().ColPos)
	require.Len(t, query.Nodes[sortNode.Children[0]].ProjectList, 4)
}

func TestQueryBuilderBuildRollupOrderByGroupingBinaryExpression(t *testing.T) {
	stmts, err := parsers.Parse(
		context.TODO(),
		dialect.MYSQL,
		`select grouping(a) + 0 as ga, count(*)
		from select_test.bind_select
		group by a with rollup
		order by grouping(a) + 0`,
		1,
	)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(NewMockCompilerContext(true), stmts[0], false)
	require.NoError(t, err)

	query := queryPlan.GetQuery()
	var sortNode *plan.Node
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_SORT {
			sortNode = node
			break
		}
	}
	require.NotNil(t, sortNode)
	require.Len(t, sortNode.OrderBy, 1)
	require.NotNil(t, sortNode.OrderBy[0].Expr.GetCol())
	require.Equal(t, int32(2), sortNode.OrderBy[0].Expr.GetCol().ColPos)
	require.Len(t, query.Nodes[sortNode.Children[0]].ProjectList, 3)
}

func TestQueryBuilderBuildRollupOrderByNestedGroupingExpression(t *testing.T) {
	stmts, err := parsers.Parse(
		context.TODO(),
		dialect.MYSQL,
		`select if(grouping(a), 1, 0) as ga, count(*)
		from select_test.bind_select
		group by a with rollup
		order by if(grouping(a), 1, 0)`,
		1,
	)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(NewMockCompilerContext(true), stmts[0], false)
	require.NoError(t, err)

	query := queryPlan.GetQuery()
	var sortNode *plan.Node
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_SORT {
			sortNode = node
			break
		}
	}
	require.NotNil(t, sortNode)
	require.Len(t, sortNode.OrderBy, 1)
	require.NotNil(t, sortNode.OrderBy[0].Expr.GetCol())
	require.Equal(t, int32(2), sortNode.OrderBy[0].Expr.GetCol().ColPos)
	require.Len(t, query.Nodes[sortNode.Children[0]].ProjectList, 3)
}

func TestQueryBuilderBuildRollupOrderByAmbiguousGroupingColumn(t *testing.T) {
	stmts, err := parsers.Parse(
		context.TODO(),
		dialect.MYSQL,
		`select count(*)
		from select_test.bind_select as t1, select_test.bind_select as t2
		group by t1.a, t2.a with rollup
		order by grouping(a)`,
		1,
	)
	require.NoError(t, err)

	_, err = BuildPlan(NewMockCompilerContext(true), stmts[0], false)
	require.Error(t, err)
}

func TestAppendGroupingSetOrderByProjects(t *testing.T) {
	stmts, err := parsers.Parse(
		context.TODO(),
		dialect.MYSQL,
		`select grouping(a, b) as grouping_ab, count(*) as row_count
		from select_test.bind_select
		group by a, b with rollup
		order by grouping(a, b) desc, 2, grouping(a)`,
		1,
	)
	require.NoError(t, err)

	selectStmt := stmts[0].(*tree.Select)
	selectClause := selectStmt.Select.(*tree.SelectClause)
	branchExprs, rewrittenOrderBy, orderResolve, err := prepareGroupingSetOrderByProjects(nil, selectStmt.OrderBy, selectClause.Exprs, false)
	require.NoError(t, err)
	require.Len(t, branchExprs, 4)
	require.Len(t, selectClause.Exprs, 2)

	// Hidden grouping keys are tracked positionally (k-th appended hidden column)
	// and resolved to an absolute ordinal later against the star-expanded width.
	// grouping(a, b) -> 0th hidden, ordinal 2 passes through, grouping(a) -> 1st hidden.
	require.Equal(t, []int{0, -1, 1}, orderResolve.hiddenIdx)
	require.Equal(t, tree.Descending, selectStmt.OrderBy[0].Direction)

	existingPos, ok := rewrittenOrderBy[1].Expr.(*tree.NumVal)
	require.True(t, ok)
	pos, ok := existingPos.Int64()
	require.True(t, ok)
	require.Equal(t, int64(2), pos)
}

func TestAppendGroupingSetOrderByProjectsIgnoresAliasShadowing(t *testing.T) {
	stmts, err := parsers.Parse(
		context.TODO(),
		dialect.MYSQL,
		`select grouping(a) as b, grouping(b) as grouping_b, count(*)
		from select_test.bind_select
		group by a, b with rollup
		order by grouping(b)`,
		1,
	)
	require.NoError(t, err)

	selectStmt := stmts[0].(*tree.Select)
	selectClause := selectStmt.Select.(*tree.SelectClause)
	branchExprs, _, orderResolve, err := prepareGroupingSetOrderByProjects(nil, selectStmt.OrderBy, selectClause.Exprs, false)
	require.NoError(t, err)
	require.Len(t, branchExprs, 4)

	// grouping(b) -> 0th appended hidden key (visible len 3, hidden at tail).
	require.Equal(t, []int{0}, orderResolve.hiddenIdx)
}

func TestAppendGroupingSetOrderByProjectsDefersAmbiguousColumnToBinder(t *testing.T) {
	stmts, err := parsers.Parse(
		context.TODO(),
		dialect.MYSQL,
		`select grouping(a) as grouping_a, count(*)
		from select_test.bind_select as t1, select_test.bind_select as t2
		group by t1.a, t2.a with rollup
		order by grouping(a)`,
		1,
	)
	require.NoError(t, err)

	selectStmt := stmts[0].(*tree.Select)
	selectClause := selectStmt.Select.(*tree.SelectClause)
	branchExprs, _, orderResolve, err := prepareGroupingSetOrderByProjects(nil, selectStmt.OrderBy, selectClause.Exprs, false)
	require.NoError(t, err)
	require.Len(t, branchExprs, 3)

	require.Equal(t, []int{0}, orderResolve.hiddenIdx)
}

func TestAppendGroupingSetOrderByProjectsSupportsGroupAlias(t *testing.T) {
	stmts, err := parsers.Parse(
		context.TODO(),
		dialect.MYSQL,
		`select a as group_a, grouping(a) as grouping_a, count(*)
		from select_test.bind_select
		group by group_a with rollup
		order by grouping(a)`,
		1,
	)
	require.NoError(t, err)

	selectStmt := stmts[0].(*tree.Select)
	selectClause := selectStmt.Select.(*tree.SelectClause)
	branchExprs, _, orderResolve, err := prepareGroupingSetOrderByProjects(nil, selectStmt.OrderBy, selectClause.Exprs, false)
	require.NoError(t, err)
	require.Len(t, branchExprs, 4)

	require.Equal(t, []int{0}, orderResolve.hiddenIdx)
}

// For SELECT DISTINCT, a grouping-related ORDER BY expression is deferred until
// the generated branch has real table bindings and a star-expanded projection.
// No hidden key is injected, so DISTINCT semantics are preserved.
func TestAppendGroupingSetOrderByProjectsDistinctReusesVisibleProjection(t *testing.T) {
	stmts, err := parsers.Parse(
		context.TODO(),
		dialect.MYSQL,
		`select distinct grouping(a), count(*)
		from select_test.bind_select
		group by a with rollup
		order by grouping(a)`,
		1,
	)
	require.NoError(t, err)

	selectStmt := stmts[0].(*tree.Select)
	selectClause := selectStmt.Select.(*tree.SelectClause)
	branchExprs, rewrittenOrderBy, orderResolve, err := prepareGroupingSetOrderByProjects(nil, selectStmt.OrderBy, selectClause.Exprs, true)
	require.NoError(t, err)
	// No hidden order key appended: the branch list stays equal to the select list.
	require.Len(t, branchExprs, len(selectClause.Exprs))
	require.Equal(t, []int{-1}, orderResolve.hiddenIdx)
	require.Equal(t, []bool{true}, orderResolve.bindDistinct)
	require.Equal(t,
		tree.String(selectStmt.OrderBy[0].Expr, dialect.MYSQL),
		tree.String(rewrittenOrderBy[0].Expr, dialect.MYSQL))
}

// For SELECT DISTINCT, an ORDER BY expression that is not a visible select-list
// expression is also deferred; the bound DISTINCT path below rejects it with
// the same error as ordinary ORDER BY instead of injecting a hidden key.
func TestAppendGroupingSetOrderByProjectsDistinctRejectsHiddenKey(t *testing.T) {
	stmts, err := parsers.Parse(
		context.TODO(),
		dialect.MYSQL,
		`select distinct grouping(a)
		from select_test.bind_select
		group by a with rollup
		order by grouping(a) + a`,
		1,
	)
	require.NoError(t, err)

	selectStmt := stmts[0].(*tree.Select)
	selectClause := selectStmt.Select.(*tree.SelectClause)
	branchExprs, _, orderResolve, err := prepareGroupingSetOrderByProjects(nil, selectStmt.OrderBy, selectClause.Exprs, true)
	require.NoError(t, err)
	require.Len(t, branchExprs, len(selectClause.Exprs))
	require.Equal(t, []int{-1}, orderResolve.hiddenIdx)
	require.Equal(t, []bool{true}, orderResolve.bindDistinct)
}

// End-to-end guard: the DISTINCT rejection must also fire through the full plan
// builder, confirming selectClause.Distinct is propagated into the rewrite.
func TestQueryBuilderBuildRollupOrderByGroupingExpressionDistinctRejectsHiddenKey(t *testing.T) {
	stmts, err := parsers.Parse(
		context.TODO(),
		dialect.MYSQL,
		`select distinct grouping(a)
		from select_test.bind_select
		group by a with rollup
		order by grouping(a) + a`,
		1,
	)
	require.NoError(t, err)

	_, err = BuildPlan(NewMockCompilerContext(true), stmts[0], false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "for SELECT DISTINCT, ORDER BY expressions must appear in select list")
}

// End-to-end: for SELECT DISTINCT, an ORDER BY expression that differs from the
// select-list expression only by function-name case or by outer parentheses is
// a valid visible-projection match and must not be rejected.
func TestQueryBuilderBuildRollupOrderByGroupingExpressionDistinctAcceptsEquivalent(t *testing.T) {
	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "function name case",
			sql: `select distinct GROUPING(A)
				from select_test.bind_select
				group by a with rollup
				order by grouping(a)`,
		},
		{
			name: "outer parens",
			sql: `select distinct grouping(a)
				from select_test.bind_select
				group by a with rollup
				order by (grouping(a))`,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, testCase.sql, 1)
			require.NoError(t, err)

			_, err = BuildPlan(NewMockCompilerContext(true), stmts[0], false)
			require.NoError(t, err)
		})
	}
}

// For SELECT DISTINCT, qualifiers on names outside GROUPING() must be preserved
// so that e.g. t1.b and t2.b are not matched as the same expression.
func TestQueryBuilderBuildRollupOrderByGroupingExpressionDistinctPreservesOuterQualifiers(t *testing.T) {
	// t1.b vs t2.b — the ORDER BY expression is NOT the visible select-list
	// expression, so DISTINCT must reject it.
	sql := `select distinct grouping(a) + t1.b
		from (select a, b from select_test.bind_select) as t1,
		     (select b from select_test.bind_select) as t2
		group by t1.a, t1.b, t2.b with rollup
		order by grouping(a) + t2.b`
	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)

	_, err = BuildPlan(NewMockCompilerContext(true), stmts[0], false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "for SELECT DISTINCT, ORDER BY expressions must appear in select list")
}

func TestAppendGroupingSetOrderByNestedProjects(t *testing.T) {
	testCases := []struct {
		name            string
		sql             string
		expectRewritten bool
	}{
		{
			name: "mixed case grouping",
			sql: `select if(GROUPING(A), 'X', 'x') as grouping_a, count(*)
			from select_test.bind_select
			group by a with rollup
			order by IF(grouping(a), 'X', 'x')`,
			expectRewritten: true,
		},
		{
			name: "different literal case",
			sql: `select if(grouping(a), 'X', 'same') as grouping_a, count(*)
			from select_test.bind_select
			group by a with rollup
			order by if(grouping(a), 'x', 'same')`,
			expectRewritten: true,
		},
		{
			name: "ordinary expression",
			sql: `select a + 0 as adjusted_a, count(*)
			from select_test.bind_select
			group by a with rollup
			order by a + 0`,
			expectRewritten: false,
		},
		{
			name: "ambiguous grouping column",
			sql: `select if(grouping(a), 1, 0) as grouping_a, count(*)
			from select_test.bind_select as t1, select_test.bind_select as t2
			group by t1.a, t2.a with rollup
			order by if(grouping(a), 1, 0)`,
			expectRewritten: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, testCase.sql, 1)
			require.NoError(t, err)

			selectStmt := stmts[0].(*tree.Select)
			selectClause := selectStmt.Select.(*tree.SelectClause)
			selectExprBefore := tree.String(selectClause.Exprs[0].Expr, dialect.MYSQL)
			orderExprBefore := tree.String(selectStmt.OrderBy[0].Expr, dialect.MYSQL)
			_, _, orderResolve, err := prepareGroupingSetOrderByProjects(nil, selectStmt.OrderBy, selectClause.Exprs, false)
			require.NoError(t, err)

			// A recognized grouping key is appended as a hidden column (idx >= 0);
			// an ordinary expression is left for the binder (idx == -1).
			require.Equal(t, testCase.expectRewritten, orderResolve.hiddenIdx[0] >= 0)
			require.Equal(t, selectExprBefore, tree.String(selectClause.Exprs[0].Expr, dialect.MYSQL))
			require.Equal(t, orderExprBefore, tree.String(selectStmt.OrderBy[0].Expr, dialect.MYSQL))
		})
	}
}

func TestAppendGroupingSetOrderByNestedQualifiedProject(t *testing.T) {
	stmts, err := parsers.Parse(
		context.TODO(),
		dialect.MYSQL,
		`select if(grouping(a), 1, 0) as grouping_a, count(*)
		from select_test.bind_select as t
		group by a with rollup
		order by if(grouping(a), 1, 0)`,
		1,
	)
	require.NoError(t, err)

	selectStmt := stmts[0].(*tree.Select)
	selectClause := selectStmt.Select.(*tree.SelectClause)
	orderFunc := selectStmt.OrderBy[0].Expr.(*tree.FuncExpr)
	groupingFunc := orderFunc.Exprs[0].(*tree.FuncExpr)
	groupingFunc.Exprs[0] = tree.NewUnresolvedName(tree.NewCStr("t", 0), tree.NewCStr("a", 0))
	branchExprs, _, orderResolve, err := prepareGroupingSetOrderByProjects(nil, selectStmt.OrderBy, selectClause.Exprs, false)
	require.NoError(t, err)
	require.Len(t, branchExprs, 3)

	require.Equal(t, []int{0}, orderResolve.hiddenIdx)
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

// genBuilderAndCtxWithColumnType creates a builder and context with a column of specified type
func genBuilderAndCtxWithColumnType(typ types.T, colName string) (*QueryBuilder, *BindContext) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindCtx := NewBindContext(builder, nil)

	typesType := typ.ToType()
	plan2Type := makePlan2Type(&typesType)
	bind := &Binding{
		tag:            1,
		nodeId:         0,
		db:             "select_test",
		table:          "bind_select",
		tableID:        0,
		cols:           []string{colName},
		colIsHidden:    []bool{false},
		types:          []*plan.Type{&plan2Type},
		refCnts:        []uint{0},
		colIdByName:    map[string]int32{colName: 0},
		isClusterTable: false,
		defaults:       []string{""},
	}
	bindCtx.bindings = append(bindCtx.bindings, bind)
	bindCtx.bindingByTable[bind.table] = bind
	bindCtx.bindingByCol[colName] = bind
	bindCtx.bindingByTag[bind.tag] = bind
	return builder, bindCtx
}

func TestQueryBuilder_bindTimeWindow(t *testing.T) {
	tests := []struct {
		name          string
		colType       types.T
		colName       string
		expectError   bool
		expectCast    bool
		errorContains string
	}{
		// Temporal types - should work without casting
		{
			name:        "DATETIME type should work",
			colType:     types.T_datetime,
			colName:     "ts",
			expectError: false,
			expectCast:  false,
		},
		{
			name:        "DATE type should work",
			colType:     types.T_date,
			colName:     "ts",
			expectError: false,
			expectCast:  false,
		},
		{
			name:        "TIMESTAMP type should work",
			colType:     types.T_timestamp,
			colName:     "ts",
			expectError: false,
			expectCast:  false,
		},
		{
			name:        "TIME type should work",
			colType:     types.T_time,
			colName:     "ts",
			expectError: false,
			expectCast:  false,
		},
		// String types - should be automatically cast to DATETIME
		{
			name:        "VARCHAR type should be cast to DATETIME",
			colType:     types.T_varchar,
			colName:     "ts",
			expectError: false,
			expectCast:  true,
		},
		{
			name:        "CHAR type should be cast to DATETIME",
			colType:     types.T_char,
			colName:     "ts",
			expectError: false,
			expectCast:  true,
		},
		{
			name:        "TEXT type should be cast to DATETIME",
			colType:     types.T_text,
			colName:     "ts",
			expectError: false,
			expectCast:  true,
		},
		// Non-temporal types - should return error
		{
			name:          "INT type should return error",
			colType:       types.T_int64,
			colName:       "ts",
			expectError:   true,
			expectCast:    false,
			errorContains: "must be temporal in time window",
		},
		{
			name:          "FLOAT type should return error",
			colType:       types.T_float64,
			colName:       "ts",
			expectError:   true,
			expectCast:    false,
			errorContains: "must be temporal in time window",
		},
		{
			name:          "BOOL type should return error",
			colType:       types.T_bool,
			colName:       "ts",
			expectError:   true,
			expectCast:    false,
			errorContains: "must be temporal in time window",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder, bindCtx := genBuilderAndCtxWithColumnType(tt.colType, tt.colName)

			// Create a time window AST
			astTimeWindow := &tree.TimeWindow{
				Interval: &tree.Interval{
					Col:  tree.NewUnresolvedName(tree.NewCStr(tt.colName, 0)),
					Val:  tree.NewNumVal(int64(2), "2", false, tree.P_int64),
					Unit: "second",
				},
				Sliding: nil,
				Fill:    nil,
			}

			// Create help function
			helpFunc, err := makeHelpFuncForTimeWindow(astTimeWindow)
			require.NoError(t, err)

			// Create a time window group expression (mock)
			timeWindowGroup := &plan.Expr{
				Typ: plan.Type{
					Id: int32(types.T_datetime),
				},
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 1,
						ColPos: 0,
					},
				},
			}

			// Create projection binder
			havingBinder := NewHavingBinder(builder, bindCtx)
			projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)

			// Call bindTimeWindow
			fillType, fillVals, fillCols, interval, sliding, ts, wEnd, boundTimeWindowOrderBy, err := builder.bindTimeWindow(
				bindCtx,
				projectionBinder,
				astTimeWindow,
				timeWindowGroup,
				helpFunc,
			)

			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					require.Contains(t, err.Error(), tt.errorContains)
				}
				// Verify error message contains type information
				if tt.colType != types.T_varchar && tt.colType != types.T_char && tt.colType != types.T_text {
					require.Contains(t, err.Error(), types.Type{Oid: tt.colType}.String())
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, ts)
				require.NotNil(t, interval)
				require.NotNil(t, boundTimeWindowOrderBy)

				// Verify that ts is cast to DATETIME if expectCast is true
				if tt.expectCast {
					require.Equal(t, int32(types.T_datetime), ts.Typ.Id)
					// Verify it's a cast expression
					castExpr, ok := ts.Expr.(*plan.Expr_F)
					require.True(t, ok)
					require.Equal(t, "cast", castExpr.F.Func.ObjName)
				} else {
					// For temporal types, should keep original type
					require.Equal(t, int32(tt.colType), ts.Typ.Id)
				}

				// Verify boundTimeWindowOrderBy
				require.Equal(t, timeWindowGroup, boundTimeWindowOrderBy.Expr)
				require.Equal(t, plan.OrderBySpec_INTERNAL|plan.OrderBySpec_ASC|plan.OrderBySpec_NULLS_FIRST, boundTimeWindowOrderBy.Flag)

				// Verify wEnd is set when sliding is nil
				if astTimeWindow.Sliding == nil {
					require.NotNil(t, wEnd)
					require.Equal(t, ts.Typ.Id, wEnd.Typ.Id)
				} else {
					require.NotNil(t, sliding)
				}

				// Verify fillType is set correctly (should be NONE when Fill is nil)
				if astTimeWindow.Fill == nil {
					require.Equal(t, plan.Node_NONE, fillType)
					require.Nil(t, fillVals)
					require.Nil(t, fillCols)
				}
			}
		})
	}
}

func TestQueryBuilder_bindTimeWindow_WithSliding(t *testing.T) {
	builder, bindCtx := genBuilderAndCtxWithColumnType(types.T_datetime, "ts")

	astTimeWindow := &tree.TimeWindow{
		Interval: &tree.Interval{
			Col:  tree.NewUnresolvedName(tree.NewCStr("ts", 0)),
			Val:  tree.NewNumVal(int64(2), "2", false, tree.P_int64),
			Unit: "second",
		},
		Sliding: &tree.Sliding{
			Val:  tree.NewNumVal(int64(1), "1", false, tree.P_int64),
			Unit: "second",
		},
		Fill: nil,
	}

	helpFunc, err := makeHelpFuncForTimeWindow(astTimeWindow)
	require.NoError(t, err)

	timeWindowGroup := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_datetime)},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: 1, ColPos: 0},
		},
	}

	havingBinder := NewHavingBinder(builder, bindCtx)
	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)

	_, _, _, _, sliding, ts, wEnd, _, err := builder.bindTimeWindow(
		bindCtx,
		projectionBinder,
		astTimeWindow,
		timeWindowGroup,
		helpFunc,
	)

	require.NoError(t, err)
	require.NotNil(t, sliding)
	require.Nil(t, wEnd) // wEnd should be nil when sliding is present
	require.NotNil(t, ts)
	require.Equal(t, int32(types.T_datetime), ts.Typ.Id)
}

func TestQueryBuilder_bindTimeWindow_WithFill(t *testing.T) {
	builder, bindCtx := genBuilderAndCtxWithColumnType(types.T_datetime, "ts")

	// Set up context with times for fill
	bindCtx.times = []*plan.Expr{
		{
			Typ: plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{RelPos: 1, ColPos: 0},
			},
		},
	}

	fillTests := []struct {
		name     string
		fillMode tree.FillMode
		fillVal  tree.Expr
		expected plan.Node_FillType
	}{
		{
			name:     "FillPrev",
			fillMode: tree.FillPrev,
			fillVal:  nil,
			expected: plan.Node_PREV,
		},
		{
			name:     "FillNext",
			fillMode: tree.FillNext,
			fillVal:  nil,
			expected: plan.Node_NEXT,
		},
		{
			name:     "FillValue",
			fillMode: tree.FillValue,
			fillVal:  tree.NewNumVal(int64(0), "0", false, tree.P_int64),
			expected: plan.Node_VALUE,
		},
		{
			name:     "FillLinear",
			fillMode: tree.FillLinear,
			fillVal:  nil,
			expected: plan.Node_LINEAR,
		},
		{
			name:     "FillNone",
			fillMode: tree.FillNone,
			fillVal:  nil,
			expected: plan.Node_NONE,
		},
	}

	for _, tt := range fillTests {
		t.Run(tt.name, func(t *testing.T) {
			astTimeWindow := &tree.TimeWindow{
				Interval: &tree.Interval{
					Col:  tree.NewUnresolvedName(tree.NewCStr("ts", 0)),
					Val:  tree.NewNumVal(int64(2), "2", false, tree.P_int64),
					Unit: "second",
				},
				Sliding: nil,
				Fill: &tree.Fill{
					Mode: tt.fillMode,
					Val:  tt.fillVal,
				},
			}

			helpFunc, err := makeHelpFuncForTimeWindow(astTimeWindow)
			require.NoError(t, err)

			timeWindowGroup := &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_datetime)},
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{RelPos: 1, ColPos: 0},
				},
			}

			havingBinder := NewHavingBinder(builder, bindCtx)
			projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)

			fillType, fillVals, fillCols, _, _, _, _, _, err := builder.bindTimeWindow(
				bindCtx,
				projectionBinder,
				astTimeWindow,
				timeWindowGroup,
				helpFunc,
			)

			require.NoError(t, err)
			require.Equal(t, tt.expected, fillType)

			if tt.fillMode == tree.FillNone || tt.fillMode == tree.FillNull {
				require.Nil(t, fillVals)
				require.Nil(t, fillCols)
			} else if tt.fillVal != nil {
				require.NotNil(t, fillVals)
				require.NotNil(t, fillCols)
			}
		})
	}
}

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

func TestQueryBuilder_bindOrderByNull(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()

	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, "select a from select_test.bind_select order by null", 1)
	require.NoError(t, err)
	selectStmt := stmts[0].(*tree.Select)
	selectClause := selectStmt.Select.(*tree.SelectClause)

	havingBinder := NewHavingBinder(builder, bindCtx)
	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)
	_, _, err = builder.bindProjection(bindCtx, projectionBinder, selectClause.Exprs, false)
	require.NoError(t, err)

	boundOrderBys, err := builder.bindOrderBy(bindCtx, selectStmt.OrderBy, projectionBinder, selectClause.Exprs)
	require.NoError(t, err)
	require.Empty(t, boundOrderBys)
	require.Equal(t, 1, len(bindCtx.projects))
}

func TestQueryBuilder_bindOrderByNullKeepsFollowingKeys(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()

	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, "select a from select_test.bind_select order by null, a desc", 1)
	require.NoError(t, err)
	selectStmt := stmts[0].(*tree.Select)
	selectClause := selectStmt.Select.(*tree.SelectClause)

	havingBinder := NewHavingBinder(builder, bindCtx)
	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)
	_, _, err = builder.bindProjection(bindCtx, projectionBinder, selectClause.Exprs, false)
	require.NoError(t, err)

	boundOrderBys, err := builder.bindOrderBy(bindCtx, selectStmt.OrderBy, projectionBinder, selectClause.Exprs)
	require.NoError(t, err)
	require.Equal(t, 1, len(boundOrderBys))
	require.Equal(t, plan.OrderBySpec_DESC, boundOrderBys[0].Flag)

	colExpr, ok := boundOrderBys[0].Expr.Expr.(*plan.Expr_Col)
	require.True(t, ok)
	require.Equal(t, int32(0), colExpr.Col.ColPos)
}

func TestQueryBuilder_bindOrderByNullDistinct(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()
	bindCtx.isDistinct = true

	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, "select distinct a from select_test.bind_select order by null", 1)
	require.NoError(t, err)
	selectStmt := stmts[0].(*tree.Select)
	selectClause := selectStmt.Select.(*tree.SelectClause)

	havingBinder := NewHavingBinder(builder, bindCtx)
	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)
	_, _, err = builder.bindProjection(bindCtx, projectionBinder, selectClause.Exprs, false)
	require.NoError(t, err)

	boundOrderBys, err := builder.bindOrderBy(bindCtx, selectStmt.OrderBy, projectionBinder, selectClause.Exprs)
	require.NoError(t, err)
	require.Empty(t, boundOrderBys)
	require.Equal(t, 1, len(bindCtx.projects))
}

func TestQueryBuilder_bindOrderByNullDistinctKeepsFollowingKeys(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()
	bindCtx.isDistinct = true

	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, "select distinct a from select_test.bind_select order by null, a desc", 1)
	require.NoError(t, err)
	selectStmt := stmts[0].(*tree.Select)
	selectClause := selectStmt.Select.(*tree.SelectClause)

	havingBinder := NewHavingBinder(builder, bindCtx)
	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)
	_, _, err = builder.bindProjection(bindCtx, projectionBinder, selectClause.Exprs, false)
	require.NoError(t, err)

	boundOrderBys, err := builder.bindOrderBy(bindCtx, selectStmt.OrderBy, projectionBinder, selectClause.Exprs)
	require.NoError(t, err)
	require.Equal(t, 1, len(boundOrderBys))
	require.Equal(t, plan.OrderBySpec_DESC, boundOrderBys[0].Flag)
	require.Equal(t, 1, len(bindCtx.projects))

	colExpr, ok := boundOrderBys[0].Expr.Expr.(*plan.Expr_Col)
	require.True(t, ok)
	require.Equal(t, bindCtx.projectTag, colExpr.Col.RelPos)
	require.Equal(t, int32(0), colExpr.Col.ColPos)
}

func TestQueryBuilder_bindOrderByNullDistinctRejectsFollowingMissingSelectExpr(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()
	bindCtx.isDistinct = true

	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, "select distinct a from select_test.bind_select order by null, b", 1)
	require.NoError(t, err)
	selectStmt := stmts[0].(*tree.Select)
	selectClause := selectStmt.Select.(*tree.SelectClause)

	havingBinder := NewHavingBinder(builder, bindCtx)
	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)
	_, _, err = builder.bindProjection(bindCtx, projectionBinder, selectClause.Exprs, false)
	require.NoError(t, err)

	_, err = builder.bindOrderBy(bindCtx, selectStmt.OrderBy, projectionBinder, selectClause.Exprs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "for SELECT DISTINCT, ORDER BY expressions must appear in select list")
}

func bindDistinctOrderByForTest(sql string) (*QueryBuilder, *BindContext, []*plan.OrderBySpec, int, error) {
	builder, bindCtx := genBuilderAndCtx()
	return bindDistinctOrderByWithTestContext(sql, builder, bindCtx)
}

func bindDistinctOrderByWithTestContext(
	sql string,
	builder *QueryBuilder,
	bindCtx *BindContext,
) (*QueryBuilder, *BindContext, []*plan.OrderBySpec, int, error) {
	bindCtx.isDistinct = true
	bindCtx.projectTag = builder.genNewBindTag()

	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, sql, 1)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	selectStmt := stmts[0].(*tree.Select)
	selectClause := selectStmt.Select.(*tree.SelectClause)

	for i := range selectClause.Exprs {
		selectExpr := &selectClause.Exprs[i]
		_, columnLike := unwrapParenExpr(selectExpr.Expr).(*tree.UnresolvedName)
		selectExpr.Expr, err = bindCtx.qualifyColumnNames(selectExpr.Expr, NoAlias)
		if err != nil {
			return nil, nil, nil, 0, err
		}
		field := SelectField{ast: selectExpr.Expr, pos: int32(i)}
		if columnLike {
			key := windowExprAstKey(selectExpr.Expr)
			if _, exists := bindCtx.projectColByAst[key]; !exists {
				bindCtx.projectColByAst[key] = int32(i)
			}
		}
		if selectExpr.As != nil && !selectExpr.As.Empty() {
			field.aliasName = selectExpr.As.Compare()
			bindCtx.aliasMap[field.aliasName] = &aliasItem{idx: int32(i), astExpr: selectExpr.Expr}
			bindCtx.aliasFrequency[field.aliasName]++
		}
		bindCtx.projectByAst = append(bindCtx.projectByAst, field)
	}

	havingBinder := NewHavingBinder(builder, bindCtx)
	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)
	resultLen, _, err := builder.bindProjection(bindCtx, projectionBinder, selectClause.Exprs, false)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	boundOrderBys, err := builder.bindOrderBy(bindCtx, selectStmt.OrderBy, projectionBinder, selectClause.Exprs)
	return builder, bindCtx, boundOrderBys, resultLen, err
}

func TestQueryBuilder_bindOrderByDistinctDerivedSelectedColumns(t *testing.T) {
	_, bindCtx, boundOrderBys, _, err := bindDistinctOrderByForTest(
		"select distinct a, b from select_test.bind_select order by abs(a) + b desc, a",
	)
	require.NoError(t, err)

	require.Len(t, boundOrderBys, 2)
	derived := boundOrderBys[0].Expr.GetF()
	require.NotNil(t, derived)
	require.Equal(t, "+", derived.Func.ObjName)
	require.True(t, containsOnlyTags(boundOrderBys[0].Expr, map[int32]bool{bindCtx.projectTag: true}))
	require.Equal(t, plan.OrderBySpec_DESC, boundOrderBys[0].Flag)
	require.Equal(t, bindCtx.projectTag, boundOrderBys[1].Expr.GetCol().RelPos)
	require.Len(t, bindCtx.projects, 2, "derived ordering key must not enter the DISTINCT projection")
}

func TestQueryBuilder_bindOrderByDistinctDerivedAlias(t *testing.T) {
	_, bindCtx, boundOrderBys, _, err := bindDistinctOrderByForTest(
		"select distinct a + 1 as x from select_test.bind_select order by x * 2",
	)
	require.NoError(t, err)

	require.Len(t, boundOrderBys, 1)
	require.NotNil(t, boundOrderBys[0].Expr.GetF())
	require.True(t, containsOnlyTags(boundOrderBys[0].Expr, map[int32]bool{bindCtx.projectTag: true}))
	require.Len(t, bindCtx.projects, 1)
}

func TestQueryBuilder_bindOrderByDistinctDerivedNameResolution(t *testing.T) {
	t.Run("selected FROM column wins over colliding alias", func(t *testing.T) {
		_, bindCtx, boundOrderBys, _, err := bindDistinctOrderByForTest(
			"select distinct a as b, b from select_test.bind_select order by abs(b)",
		)
		require.NoError(t, err)
		require.Len(t, boundOrderBys, 1)

		absExpr := boundOrderBys[0].Expr.GetF()
		require.NotNil(t, absExpr)
		require.Len(t, absExpr.Args, 1)
		col := absExpr.Args[0].GetCol()
		require.NotNil(t, col)
		require.Equal(t, bindCtx.projectTag, col.RelPos)
		require.Equal(t, int32(1), col.ColPos)
	})

	t.Run("unselected FROM column does not fall back to colliding alias", func(t *testing.T) {
		_, _, _, _, err := bindDistinctOrderByForTest(
			"select distinct a as b from select_test.bind_select order by abs(b)",
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "for SELECT DISTINCT, ORDER BY expressions must appear in select list")
	})

	t.Run("parenthesized root preserves output ambiguity", func(t *testing.T) {
		_, _, _, _, err := bindDistinctOrderByForTest(
			"select distinct a as b, b from select_test.bind_select order by (b)",
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Column 'b' in order clause is ambiguous")
	})

	t.Run("nested name falls back to noncolliding alias", func(t *testing.T) {
		_, bindCtx, boundOrderBys, _, err := bindDistinctOrderByForTest(
			"select distinct a as x from select_test.bind_select order by abs(x)",
		)
		require.NoError(t, err)
		require.Len(t, boundOrderBys, 1)

		absExpr := boundOrderBys[0].Expr.GetF()
		require.NotNil(t, absExpr)
		require.Len(t, absExpr.Args, 1)
		col := absExpr.Args[0].GetCol()
		require.NotNil(t, col)
		require.Equal(t, bindCtx.projectTag, col.RelPos)
		require.Equal(t, int32(0), col.ColPos)
	})

	t.Run("qualified selected FROM column remains available", func(t *testing.T) {
		_, bindCtx, boundOrderBys, _, err := bindDistinctOrderByForTest(
			"select distinct a as b, b from select_test.bind_select order by abs(bind_select.b)",
		)
		require.NoError(t, err)
		require.Len(t, boundOrderBys, 1)

		col := boundOrderBys[0].Expr.GetF().Args[0].GetCol()
		require.NotNil(t, col)
		require.Equal(t, bindCtx.projectTag, col.RelPos)
		require.Equal(t, int32(1), col.ColPos)
	})

	t.Run("qualified unselected FROM column is rejected", func(t *testing.T) {
		_, _, _, _, err := bindDistinctOrderByForTest(
			"select distinct a as x from select_test.bind_select order by abs(bind_select.b)",
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "for SELECT DISTINCT, ORDER BY expressions must appear in select list")
	})
}

func TestQueryBuilder_bindOrderByDistinctDerivedAggregateAliasDoesNotRebind(t *testing.T) {
	_, bindCtx, boundOrderBys, _, err := bindDistinctOrderByForTest(
		"select distinct count(distinct a) as x from select_test.bind_select order by x + 1",
	)
	require.NoError(t, err)

	require.Len(t, boundOrderBys, 1)
	require.Len(t, bindCtx.aggregates, 1, "derived aliases must not bind their projected aggregate again")
	require.True(t, containsOnlyTags(boundOrderBys[0].Expr, map[int32]bool{bindCtx.projectTag: true}))
}

func TestQueryBuilder_bindOrderByDistinctDerivedSubqueryAliasDoesNotRebind(t *testing.T) {
	builder, bindCtx, boundOrderBys, _, err := bindDistinctOrderByForTest(
		"select distinct (select 1) as x from select_test.bind_select order by x + 1",
	)
	require.NoError(t, err)

	require.Len(t, boundOrderBys, 1)
	require.Len(t, builder.qry.Nodes, 2, "derived aliases must not build their projected subquery again")
	require.True(t, containsOnlyTags(boundOrderBys[0].Expr, map[int32]bool{bindCtx.projectTag: true}))
}

func TestQueryBuilder_bindOrderByDistinctDerivedFullTextReturnsSyntaxError(t *testing.T) {
	builder, bindCtx := genBuilderAndCtxWithColumnType(types.T_varchar, "body")
	_, _, _, _, err := bindDistinctOrderByWithTestContext(
		"select distinct body from select_test.bind_select order by match(body) against('database')",
		builder,
		bindCtx,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "for SELECT DISTINCT, ORDER BY expressions must appear in select list")
	require.NotContains(t, err.Error(), "escaped projection scope")

	builder, bindCtx = genBuilderAndCtxWithColumnType(types.T_varchar, "body")
	_, _, _, _, err = bindDistinctOrderByWithTestContext(
		"select distinct match(body) against('database') as score from select_test.bind_select order by match(body) against('database')",
		builder,
		bindCtx,
	)
	require.NoError(t, err, "an exact projected full-text expression must remain orderable")

	builder, bindCtx = genBuilderAndCtxWithColumnType(types.T_varchar, "body")
	_, bindCtx, boundOrderBys, _, err := bindDistinctOrderByWithTestContext(
		"select distinct match(body) against('database') as score from select_test.bind_select order by not score",
		builder,
		bindCtx,
	)
	require.NoError(t, err, "a derived expression over a projected full-text alias must remain orderable")
	require.Len(t, boundOrderBys, 1)
	require.True(t, containsOnlyTags(boundOrderBys[0].Expr, map[int32]bool{bindCtx.projectTag: true}))
}

func TestQueryBuilder_bindOrderByDistinctDerivedRejectsUnselectedInputs(t *testing.T) {
	tests := []string{
		"select distinct a from select_test.bind_select order by a + b",
		"select distinct a + 1 as x from select_test.bind_select order by a + 2",
		"select distinct a from select_test.bind_select order by count(*) + 1",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, _, _, _, err := bindDistinctOrderByForTest(sql)
			require.Error(t, err)
			require.Contains(t, err.Error(), "for SELECT DISTINCT, ORDER BY expressions must appear in select list")
		})
	}
}

func TestQueryBuilder_appendDistinctOrderProjectionNode(t *testing.T) {
	builder, bindCtx, boundOrderBys, resultLen, err := bindDistinctOrderByForTest(
		"select distinct a, b from select_test.bind_select order by abs(a), abs(a), b",
	)
	require.NoError(t, err)
	require.Len(t, boundOrderBys, 3)

	inputID := builder.appendNode(&plan.Node{NodeType: plan.Node_VALUE_SCAN}, bindCtx)
	projectID, err := builder.appendProjectionNode(bindCtx, inputID, false)
	require.NoError(t, err)
	distinctID := builder.appendDistinctNode(bindCtx, projectID)
	orderProjectID, orderTag, err := builder.appendDistinctOrderProjectionNode(bindCtx, distinctID, boundOrderBys)
	require.NoError(t, err)
	require.NotEqual(t, bindCtx.projectTag, orderTag)

	orderProject := builder.qry.Nodes[orderProjectID]
	require.Equal(t, plan.Node_PROJECT, orderProject.NodeType)
	require.Equal(t, distinctID, orderProject.Children[0])
	require.Len(t, orderProject.ProjectList, resultLen+1, "duplicate derived keys should share one hidden column")
	require.True(t, containsOnlyTags(orderProject.ProjectList[resultLen], map[int32]bool{bindCtx.projectTag: true}))
	for _, orderBy := range boundOrderBys {
		col := orderBy.Expr.GetCol()
		require.NotNil(t, col)
		require.Equal(t, orderTag, col.RelPos)
	}
	require.Equal(t, boundOrderBys[0].Expr.GetCol().ColPos, boundOrderBys[1].Expr.GetCol().ColPos)

	sortID := builder.appendSortNode(bindCtx, orderProjectID, boundOrderBys)
	resultID := builder.appendResultProjectionNode(bindCtx, sortID, resultLen, orderTag)
	resultProject := builder.qry.Nodes[resultID]
	require.Len(t, resultProject.ProjectList, resultLen)
	for _, expr := range resultProject.ProjectList {
		require.Equal(t, orderTag, expr.GetCol().RelPos)
	}
}

func TestQueryBuilder_bindOrderByOrdinalPosition(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()

	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, "select a + 1 as x, b from select_test.bind_select order by 1", 1)
	require.NoError(t, err)
	selectStmt := stmts[0].(*tree.Select)
	selectClause := selectStmt.Select.(*tree.SelectClause)

	havingBinder := NewHavingBinder(builder, bindCtx)
	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)
	_, _, err = builder.bindProjection(bindCtx, projectionBinder, selectClause.Exprs, false)
	require.NoError(t, err)

	boundOrderBys, err := builder.bindOrderBy(bindCtx, selectStmt.OrderBy, projectionBinder, selectClause.Exprs)
	require.NoError(t, err)
	require.Equal(t, 1, len(boundOrderBys))

	colExpr, ok := boundOrderBys[0].Expr.Expr.(*plan.Expr_Col)
	require.True(t, ok)
	require.Equal(t, bindCtx.projectTag, colExpr.Col.RelPos)
	require.Equal(t, int32(0), colExpr.Col.ColPos)

	funcExpr, ok := bindCtx.projects[0].Expr.(*plan.Expr_F)
	require.True(t, ok)
	require.Equal(t, "+", funcExpr.F.Func.ObjName)
	require.Equal(t, "a", funcExpr.F.Args[0].Expr.(*plan.Expr_Col).Col.Name)
	require.Equal(t, int64(1), funcExpr.F.Args[1].Expr.(*plan.Expr_Lit).Lit.Value.(*plan.Literal_I64Val).I64Val)
}

func TestQueryBuilder_buildSetOperationOrderByNull(t *testing.T) {
	cases := []struct {
		name      string
		sql       string
		sortCount int
	}{
		{
			name:      "union null only",
			sql:       "select 1 as a union select 2 order by null",
			sortCount: 0,
		},
		{
			name:      "union null then alias",
			sql:       "select 1 as a union select 2 order by null, a desc",
			sortCount: 1,
		},
		{
			name:      "intersect null only",
			sql:       "select 1 as a intersect select 1 order by null",
			sortCount: 0,
		},
		{
			name:      "except null only",
			sql:       "select 1 as a except select 2 order by null",
			sortCount: 0,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(true), t, tt.sql)
			require.NoError(t, err)

			sortNodes := collectReachableSortNodes(logicPlan.GetQuery())
			require.Equal(t, tt.sortCount, len(sortNodes))
			if tt.sortCount == 0 {
				return
			}

			require.Equal(t, 1, len(sortNodes[0].OrderBy))
			require.Equal(t, plan.OrderBySpec_DESC, sortNodes[0].OrderBy[0].Flag)
			require.NotNil(t, sortNodes[0].OrderBy[0].Expr.GetCol())
		})
	}
}

func collectReachableSortNodes(query *plan.Query) []*plan.Node {
	if query == nil || len(query.Steps) == 0 {
		return nil
	}

	var sortNodes []*plan.Node
	visited := make(map[int32]struct{})
	var visit func(int32)
	visit = func(nodeID int32) {
		if nodeID < 0 || int(nodeID) >= len(query.Nodes) {
			return
		}
		if _, ok := visited[nodeID]; ok {
			return
		}
		visited[nodeID] = struct{}{}

		node := query.Nodes[nodeID]
		if node == nil {
			return
		}
		if node.NodeType == plan.Node_SORT {
			sortNodes = append(sortNodes, node)
		}
		for _, childID := range node.Children {
			visit(childID)
		}
	}

	for _, rootID := range query.Steps {
		visit(rootID)
	}

	return sortNodes
}

type icebergTestCompilerContext struct {
	*MockCompilerContext
	proc *process.Process
}

func (c *icebergTestCompilerContext) GetProcess() *process.Process {
	return c.proc
}

func newIcebergTestCompilerContext(t *testing.T, loc *time.Location) *icebergTestCompilerContext {
	t.Helper()
	if loc == nil {
		loc = time.UTC
	}
	base := NewMockCompilerContext(true)
	base.objects["gold_orders"] = &plan.ObjectRef{
		DbName:  "tpch",
		ObjName: "gold_orders",
		Obj:     4242,
	}
	base.tables["gold_orders"] = &plan.TableDef{
		Name:      "gold_orders",
		TableType: catalog.SystemExternalRel,
		Createsql: sqliceberg.BuildCreateSQLEnvelope(model.TableMapping{
			Namespace:  "sales",
			TableName:  "orders",
			DefaultRef: model.DefaultRefMain,
			ReadMode:   model.ReadModeAppendOnly,
			WriteMode:  model.WriteModeReadOnly,
		}, "ksa_gold"),
		Cols: []*plan.ColDef{
			{
				Name:    "id",
				Typ:     plan.Type{Id: int32(types.T_int64), Width: 64, Table: "gold_orders"},
				Default: &plan.Default{NullAbility: true},
			},
		},
		Pkey: &plan.PrimaryKeyDef{},
	}
	base.objects["dim_orders"] = &plan.ObjectRef{
		DbName:  "tpch",
		ObjName: "dim_orders",
		Obj:     4343,
	}
	base.tables["dim_orders"] = &plan.TableDef{
		Name:      "dim_orders",
		TableType: catalog.SystemOrdinaryRel,
		Cols: []*plan.ColDef{
			{
				Name:    "id",
				Typ:     plan.Type{Id: int32(types.T_int64), Width: 64, Table: "dim_orders"},
				Default: &plan.Default{NullAbility: true},
			},
		},
		Pkey: &plan.PrimaryKeyDef{},
	}
	proc := testutil.NewProc(t)
	proc.Base.SessionInfo.TimeZone = loc
	return &icebergTestCompilerContext{
		MockCompilerContext: base,
		proc:                proc,
	}
}

func buildIcebergTestPlan(t *testing.T, ctx CompilerContext, sql string) (*Plan, error) {
	t.Helper()
	stmts, err := parsers.Parse(ctx.GetContext(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	return BuildPlan(ctx, stmts[0], false)
}

func mustMarshalLegacyExternParam(t *testing.T, param *tree.ExternParam) string {
	t.Helper()
	data, err := json.Marshal(param)
	require.NoError(t, err)
	return string(data)
}

func requireIcebergScan(t *testing.T, p *Plan) *plan.IcebergScan {
	t.Helper()
	require.NotNil(t, p)
	require.NotNil(t, p.GetQuery())
	for _, node := range p.GetQuery().Nodes {
		if node.GetExternScan().GetIcebergScan() != nil {
			return node.GetExternScan().GetIcebergScan()
		}
	}
	t.Fatalf("expected an Iceberg external scan")
	return nil
}

func TestQueryBuilderLegacyExternalTableNotDispatchedAsIceberg(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	ctx.objects["legacy_orders"] = &plan.ObjectRef{
		DbName:  "tpch",
		ObjName: "legacy_orders",
		Obj:     4343,
	}
	ctx.tables["legacy_orders"] = &plan.TableDef{
		Name:      "legacy_orders",
		TableType: catalog.SystemExternalRel,
		Createsql: mustMarshalLegacyExternParam(t, &tree.ExternParam{
			ExParamConst: tree.ExParamConst{
				ScanType: tree.INFILE,
				Filepath: "/data/legacy/orders/*.parquet",
				Format:   tree.PARQUET,
				Option:   []string{"format", "parquet"},
			},
		}),
		Cols: []*plan.ColDef{
			{
				Name:    "id",
				Typ:     plan.Type{Id: int32(types.T_int64), Width: 64, Table: "legacy_orders"},
				Default: &plan.Default{NullAbility: true},
			},
		},
	}

	p, err := buildIcebergTestPlan(t, ctx, "select id from legacy_orders")
	require.NoError(t, err)

	foundExternal := false
	for _, node := range p.GetQuery().GetNodes() {
		if node.GetNodeType() != plan.Node_EXTERNAL_SCAN {
			continue
		}
		foundExternal = true
		require.NotNil(t, node.GetExternScan())
		require.Equal(t, int32(plan.ExternType_EXTERNAL_TB), node.GetExternScan().GetType())
		require.Nil(t, node.GetExternScan().GetIcebergScan())
	}
	require.True(t, foundExternal, "expected a legacy external scan")
}

func TestQueryBuilderIcebergPersistentMappingCurrentSnapshot(t *testing.T) {
	ctx := newIcebergTestCompilerContext(t, time.UTC)

	p, err := buildIcebergTestPlan(t, ctx, "select id from gold_orders")
	require.NoError(t, err)

	scan := requireIcebergScan(t, p)
	require.Equal(t, uint64(4242), scan.MappingId)
	require.Equal(t, "sales", scan.Namespace)
	require.Equal(t, "orders", scan.Table)
	require.Equal(t, "main", scan.Ref)
	require.Zero(t, scan.SnapshotId)
	require.Zero(t, scan.TimestampAsOf)
	require.Equal(t, "append_only", scan.ReadMode)
}

func TestQueryBuilderIcebergComposesWithProjectionFilterJoinAggregate(t *testing.T) {
	ctx := newIcebergTestCompilerContext(t, time.UTC)

	p, err := buildIcebergTestPlan(t, ctx,
		"select g.id, count(d.id) from gold_orders g join dim_orders d on g.id = d.id where g.id > 10 group by g.id")
	require.NoError(t, err)

	query := p.GetQuery()
	require.NotNil(t, query)
	var hasIcebergScan, hasJoin, hasFilter, hasAgg, hasProject bool
	for _, node := range query.GetNodes() {
		switch node.GetNodeType() {
		case plan.Node_EXTERNAL_SCAN:
			if node.GetExternScan().GetIcebergScan() != nil {
				hasIcebergScan = true
			}
		case plan.Node_JOIN:
			hasJoin = true
		case plan.Node_FILTER:
			hasFilter = true
		case plan.Node_AGG:
			hasAgg = true
		case plan.Node_PROJECT:
			hasProject = true
		}
	}
	require.True(t, hasIcebergScan, "expected Iceberg persistent mapping scan")
	require.True(t, hasJoin, "expected join with ordinary table")
	require.True(t, hasFilter, "expected SQL filter to remain in MO plan")
	require.True(t, hasAgg, "expected aggregate over Iceberg scan")
	require.True(t, hasProject, "expected projection over Iceberg scan")
}

func TestQueryBuilderIcebergTimeTravelSnapshot(t *testing.T) {
	ctx := newIcebergTestCompilerContext(t, time.UTC)

	p, err := buildIcebergTestPlan(t, ctx, "select id from gold_orders for iceberg snapshot 78124581230123")
	require.NoError(t, err)

	scan := requireIcebergScan(t, p)
	require.Equal(t, int64(78124581230123), scan.SnapshotId)
	require.Zero(t, scan.TimestampAsOf)
	require.Equal(t, "main", scan.Ref)
}

func TestQueryBuilderIcebergTimeTravelTimestampUsesSessionTimezone(t *testing.T) {
	loc := time.FixedZone("KSA", 3*60*60)
	ctx := newIcebergTestCompilerContext(t, loc)

	p, err := buildIcebergTestPlan(t, ctx, "select id from gold_orders for iceberg timestamp as of timestamp '2026-06-01 00:00:00'")
	require.NoError(t, err)

	ts, err := types.ParseTimestamp(loc, "2026-06-01 00:00:00", 6)
	require.NoError(t, err)
	expectedMS := (int64(ts) - int64(types.UnixToTimestamp(0))) / 1000
	scan := requireIcebergScan(t, p)
	require.Zero(t, scan.SnapshotId)
	require.Equal(t, expectedMS, scan.TimestampAsOf)
}

func TestQueryBuilderIcebergNamedRef(t *testing.T) {
	ctx := newIcebergTestCompilerContext(t, time.UTC)

	p, err := buildIcebergTestPlan(t, ctx, "select id from gold_orders for iceberg ref audit_branch")
	require.NoError(t, err)

	scan := requireIcebergScan(t, p)
	require.Zero(t, scan.SnapshotId)
	require.Zero(t, scan.TimestampAsOf)
	require.Equal(t, "audit_branch", scan.Ref)
}

func TestQueryBuilderIcebergTimeTravelRejectsNativeSnapshotMix(t *testing.T) {
	ctx := newIcebergTestCompilerContext(t, time.UTC)

	_, err := buildIcebergTestPlan(t, ctx, "select id from gold_orders {timestamp = '2026-06-01 00:00:00'} for iceberg snapshot 1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot combine MO snapshot hint with FOR ICEBERG time travel")
}

func TestQueryBuilderIcebergTimeTravelRequiresIcebergTable(t *testing.T) {
	ctx := NewMockCompilerContext(true)

	_, err := buildIcebergTestPlan(t, ctx, "select n_name from nation for iceberg snapshot 1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "FOR ICEBERG requires an Iceberg external table")
}

func TestQueryBuilder_bindOrderByEnum(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindCtx := NewBindContext(builder, nil)

	enumType := types.T_enum.ToType()
	plan2Type := makePlan2Type(&enumType)
	plan2Type.Enumvalues = "low,medium,high,critical"

	intType := types.T_int64.ToType()
	plan2IntType := makePlan2Type(&intType)

	bind := &Binding{
		tag:            1,
		nodeId:         0,
		db:             "select_test",
		table:          "bind_select",
		tableID:        0,
		cols:           []string{"id", "val"},
		colIsHidden:    []bool{false, false},
		types:          []*plan.Type{&plan2IntType, &plan2Type},
		refCnts:        []uint{0, 0},
		colIdByName:    map[string]int32{"id": 0, "val": 1},
		isClusterTable: false,
		defaults:       []string{"", ""},
	}
	bindCtx.bindings = append(bindCtx.bindings, bind)
	bindCtx.bindingByTable[bind.table] = bind
	for _, col := range bind.cols {
		bindCtx.bindingByCol[col] = bind
	}
	bindCtx.bindingByTag[bind.tag] = bind

	stmts, _ := parsers.Parse(context.TODO(), dialect.MYSQL, "select id, val from select_test.bind_select order by val", 1)
	selectClause := stmts[0].(*tree.Select).Select.(*tree.SelectClause)
	orderList := stmts[0].(*tree.Select).OrderBy

	havingBinder := NewHavingBinder(builder, bindCtx)
	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)
	_, _, err := builder.bindProjection(bindCtx, projectionBinder, selectClause.Exprs, false)
	require.NoError(t, err)

	boundOrderBys, err := builder.bindOrderBy(bindCtx, orderList, projectionBinder, selectClause.Exprs)
	require.NoError(t, err)
	require.Equal(t, 1, len(boundOrderBys))
	// The ORDER BY expression should use the original ENUM type (uint16), not varchar
	require.Equal(t, int32(types.T_enum), boundOrderBys[0].Expr.Typ.Id)
}

func TestQueryBuilder_bindOrderByEnumDistinct(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindCtx := NewBindContext(builder, nil)
	bindCtx.isDistinct = true

	enumType := types.T_enum.ToType()
	plan2Type := makePlan2Type(&enumType)
	plan2Type.Enumvalues = "low,medium,high,critical"

	intType := types.T_int64.ToType()
	plan2IntType := makePlan2Type(&intType)

	bind := &Binding{
		tag:            1,
		nodeId:         0,
		db:             "select_test",
		table:          "bind_select",
		tableID:        0,
		cols:           []string{"id", "val"},
		colIsHidden:    []bool{false, false},
		types:          []*plan.Type{&plan2IntType, &plan2Type},
		refCnts:        []uint{0, 0},
		colIdByName:    map[string]int32{"id": 0, "val": 1},
		isClusterTable: false,
		defaults:       []string{"", ""},
	}
	bindCtx.bindings = append(bindCtx.bindings, bind)
	bindCtx.bindingByTable[bind.table] = bind
	for _, col := range bind.cols {
		bindCtx.bindingByCol[col] = bind
	}
	bindCtx.bindingByTag[bind.tag] = bind

	stmts, _ := parsers.Parse(context.TODO(), dialect.MYSQL, "select distinct val from select_test.bind_select order by val", 1)
	selectClause := stmts[0].(*tree.Select).Select.(*tree.SelectClause)
	orderList := stmts[0].(*tree.Select).OrderBy

	havingBinder := NewHavingBinder(builder, bindCtx)
	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)
	_, _, err := builder.bindProjection(bindCtx, projectionBinder, selectClause.Exprs, false)
	require.NoError(t, err)
	require.Equal(t, 1, len(bindCtx.projects))

	boundOrderBys, err := builder.bindOrderBy(bindCtx, orderList, projectionBinder, selectClause.Exprs)
	require.NoError(t, err)
	require.Equal(t, 1, len(boundOrderBys))
	require.Equal(t, 2, len(bindCtx.projects))
	// DISTINCT should still sort by the original ENUM index, not the visible varchar.
	require.Equal(t, int32(types.T_enum), boundOrderBys[0].Expr.Typ.Id)
	require.Equal(t, int32(types.T_enum), bindCtx.projects[1].Typ.Id)
	col := boundOrderBys[0].Expr.GetCol()
	require.NotNil(t, col)
	require.Equal(t, bindCtx.projectTag, col.RelPos)
	require.Equal(t, int32(1), col.ColPos)
}

func TestQueryBuilder_bindOrderByDistinctLargeExpr(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()
	bindCtx.isDistinct = true

	longLiteral := strings.Repeat("x", 300)
	sql := "select distinct concat('" + longLiteral + "', a) from select_test.bind_select order by concat('" + longLiteral + "', a)"
	stmts, _ := parsers.Parse(context.TODO(), dialect.MYSQL, sql, 1)
	selectClause := stmts[0].(*tree.Select).Select.(*tree.SelectClause)
	orderList := stmts[0].(*tree.Select).OrderBy

	havingBinder := NewHavingBinder(builder, bindCtx)
	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)
	_, _, err := builder.bindProjection(bindCtx, projectionBinder, selectClause.Exprs, false)
	require.NoError(t, err)
	require.Equal(t, 1, len(bindCtx.projects))

	boundOrderBys, err := builder.bindOrderBy(bindCtx, orderList, projectionBinder, selectClause.Exprs)
	require.NoError(t, err)
	require.Equal(t, 1, len(boundOrderBys))
	require.Equal(t, 1, len(bindCtx.projects))
	col := boundOrderBys[0].Expr.GetCol()
	require.NotNil(t, col)
	require.Equal(t, bindCtx.projectTag, col.RelPos)
	require.Equal(t, int32(0), col.ColPos)
}

func TestAppendOrderByProjectExprLargeExprDedup(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindCtx := NewBindContext(builder, nil)

	stringType := types.T_varchar.ToType()
	expr := &plan.Expr{
		Typ: makePlan2Type(&stringType),
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Sval{Sval: strings.Repeat("x", 300)},
			},
		},
	}

	firstPos, err := appendOrderByProjectExpr(bindCtx, expr)
	require.NoError(t, err)
	secondPos, err := appendOrderByProjectExpr(bindCtx, DeepCopyExpr(expr))
	require.NoError(t, err)

	require.Equal(t, firstPos, secondPos)
	require.Equal(t, int32(0), firstPos)
	require.Equal(t, 1, len(bindCtx.projects))
	require.Equal(t, 1, len(bindCtx.projectByExpr))
}

func TestQueryBuilder_bindLimit(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()

	stmts, _ := parsers.Parse(context.TODO(), dialect.MYSQL, "select a from select_test.bind_select limit 1, 5", 1)
	astLimit := stmts[0].(*tree.Select).Limit

	boundOffsetExpr, boundCountExpr, _, err := builder.bindLimit(bindCtx, astLimit, nil)
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

func TestQueryBuilderBindLimitFoldsAndNormalizesConstants(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()

	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, "select a from select_test.bind_select limit 2 + 3 offset 0 + 0", 1)
	require.NoError(t, err)
	astLimit := stmts[0].(*tree.Select).Limit

	offset, limit, _, err := builder.bindLimit(bindCtx, astLimit, nil)
	require.NoError(t, err)
	require.Nil(t, offset, "constant OFFSET 0 should not disable no-offset optimization paths")
	require.Equal(t, uint64(5), limit.GetLit().GetU64Val())
}

func TestQueryBuilderBindLimitReportsConstantOverflow(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()

	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, "select a from select_test.bind_select limit 18446744073709551615 + 1", 1)
	require.NoError(t, err)
	astLimit := stmts[0].(*tree.Select).Limit

	_, _, _, err = builder.bindLimit(bindCtx, astLimit, nil)
	require.Error(t, err)
}

func TestQueryBuilderBindLimitKeepsVariableDynamic(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()

	stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, "select a from select_test.bind_select limit @page_size", 1)
	require.NoError(t, err)
	astLimit := stmts[0].(*tree.Select).Limit

	_, limit, _, err := builder.bindLimit(bindCtx, astLimit, nil)
	require.NoError(t, err)
	require.NotNil(t, limit.GetF())
	require.True(t, containsDynamicParam(limit))
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

func TestQueryBuilder_appendWindowNode(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindCtx := NewBindContext(builder, nil)
	bindCtx.groupTag = builder.GenNewBindTag()
	bindCtx.aggregateTag = builder.GenNewBindTag()
	bindCtx.projectTag = builder.GenNewBindTag()
	bindCtx.windowTag = builder.GenNewBindTag()
	bindCtx.sampleTag = builder.GenNewBindTag()

	stmts, _ := parsers.Parse(context.TODO(), dialect.MYSQL, "select a, lag(a) over (order by a) as prev_a from select_test.bind_select group by a having prev_a > 0", 1)
	selectClause := stmts[0].(*tree.Select).Select.(*tree.SelectClause)

	nodeID, selectList, _, notCacheable, _, havingBinder, boundHavingList, err := builder.bindSelectClause(bindCtx, selectClause, nil, nil, nil, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(boundHavingList))
	require.Len(t, bindCtx.windows, 1)

	preWindowHavingList, postWindowHavingList := splitWindowDependentHavingFilters(boundHavingList, bindCtx.windowTag)
	require.Len(t, preWindowHavingList, 0)
	require.Len(t, postWindowHavingList, 1)

	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)
	_, _, err = builder.bindProjection(bindCtx, projectionBinder, selectList, notCacheable)
	require.NoError(t, err)
	require.Len(t, bindCtx.windows, 1)

	nodeID, err = builder.appendAggNode(bindCtx, nodeID, boundHavingList, false)
	require.NoError(t, err)
	require.Equal(t, plan.Node_AGG, builder.qry.Nodes[nodeID].NodeType)

	nodeID, err = builder.appendWindowNode(bindCtx, nodeID, boundHavingList)
	require.NoError(t, err)
	require.Equal(t, plan.Node_FILTER, builder.qry.Nodes[nodeID].NodeType)
	require.Len(t, builder.qry.Nodes[nodeID].FilterList, 1)
	require.Equal(t, ">", builder.qry.Nodes[nodeID].FilterList[0].Expr.(*plan.Expr_F).F.Func.ObjName)
	require.True(t, containsTag(builder.qry.Nodes[nodeID].FilterList[0], bindCtx.windowTag))

	windowNodeFound := false
	for _, node := range builder.qry.Nodes {
		if node.NodeType == plan.Node_WINDOW {
			windowNodeFound = true
			break
		}
	}
	require.True(t, windowNodeFound)
}

func TestSplitWindowDependentHavingFilters_WithSubqueryChild(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindCtx := NewBindContext(builder, nil)
	bindCtx.groupTag = builder.GenNewBindTag()
	bindCtx.aggregateTag = builder.GenNewBindTag()
	bindCtx.projectTag = builder.GenNewBindTag()
	bindCtx.windowTag = builder.GenNewBindTag()
	bindCtx.sampleTag = builder.GenNewBindTag()

	stmts, err := parsers.Parse(
		context.TODO(),
		dialect.MYSQL,
		"select a, lag(a) over (order by a) as prev_a from select_test.bind_select group by a having prev_a in (select 1)",
		1,
	)
	require.NoError(t, err)

	selectClause := stmts[0].(*tree.Select).Select.(*tree.SelectClause)
	_, _, _, _, _, _, boundHavingList, err := builder.bindSelectClause(bindCtx, selectClause, nil, nil, nil, true)
	require.NoError(t, err)
	require.Len(t, boundHavingList, 1)
	require.IsType(t, &plan.Expr_Sub{}, boundHavingList[0].Expr)
	require.True(t, containsTag(boundHavingList[0], bindCtx.windowTag))

	preWindowHavingList, postWindowHavingList := splitWindowDependentHavingFilters(boundHavingList, bindCtx.windowTag)
	require.Len(t, preWindowHavingList, 0)
	require.Len(t, postWindowHavingList, 1)
}

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
	nodeID = builder.appendResultProjectionNode(bindCtx, nodeID, resultLen, bindCtx.projectTag)
	require.Equal(t, int32(3), nodeID)

	resultProjectionNode := builder.qry.Nodes[3]
	require.Equal(t, int32(3), resultProjectionNode.NodeId)
	require.Equal(t, int32(2), resultProjectionNode.Children[0])
}

func TestQueryBuilder_buildRemapErrorMessage(t *testing.T) {
	builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, false)
	builder.nameByColRef = map[[2]int32]string{
		{1, 0}: "col1",
		{1, 1}: "col2",
		{2, 0}: "col3",
	}

	tests := []struct {
		name         string
		missingCol   [2]int32
		colName      string
		colMap       map[[2]int32][2]int32
		remapInfo    *RemapInfo
		wantContains []string
		desc         string
	}{
		{
			name:       "basic error message",
			missingCol: [2]int32{1, 2},
			colName:    "missing_col",
			colMap: map[[2]int32][2]int32{
				{1, 0}: {0, 0},
				{1, 1}: {0, 1},
			},
			remapInfo: nil,
			wantContains: []string{
				"Column remapping failed",
				"Missing Column",
				"RelPos=1, ColPos=2",
				"missing_col",
				"Available Columns in Context",
			},
			desc: "Basic error message should contain all required sections",
		},
		{
			name:       "error with context - FILTER node",
			missingCol: [2]int32{1, 2},
			colName:    "missing_col",
			colMap: map[[2]int32][2]int32{
				{1, 0}: {0, 0},
			},
			remapInfo: &RemapInfo{
				step: 0,
				node: &plan.Node{
					NodeId:   10,
					NodeType: plan.Node_FILTER,
					FilterList: []*plan.Expr{
						{
							Typ: plan.Type{Id: int32(types.T_bool)},
							Expr: &plan.Expr_F{
								F: &plan.Function{
									Func: &plan.ObjectRef{ObjName: "="},
									Args: []*plan.Expr{
										{
											Typ: plan.Type{Id: int32(types.T_int64)},
											Expr: &plan.Expr_Col{
												Col: &plan.ColRef{
													RelPos: 1,
													ColPos: 2,
													Name:   "missing_col",
												},
											},
										},
										{
											Typ: plan.Type{Id: int32(types.T_int64)},
											Expr: &plan.Expr_Lit{
												Lit: &plan.Literal{
													Value: &plan.Literal_I64Val{I64Val: 1},
												},
											},
										},
									},
								},
							},
						},
					},
				},
				tip:        "FilterList",
				srcExprIdx: 0,
			},
			wantContains: []string{
				"Context",
				"Step: 0",
				"Node ID: 10",
				"Node Type: FILTER",
				"Tip: FilterList",
				"Expression Index: 0",
				"Related Expression",
			},
			desc: "Error with FILTER node context should include all context information",
		},
		{
			name:       "error with context - PROJECT node",
			missingCol: [2]int32{1, 2},
			colName:    "missing_col",
			colMap:     map[[2]int32][2]int32{},
			remapInfo: &RemapInfo{
				step: 1,
				node: &plan.Node{
					NodeId:   20,
					NodeType: plan.Node_PROJECT,
					ProjectList: []*plan.Expr{
						{
							Typ: plan.Type{Id: int32(types.T_int64)},
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{
									RelPos: 1,
									ColPos: 2,
									Name:   "missing_col",
								},
							},
						},
					},
				},
				tip:        "ProjectList",
				srcExprIdx: 0,
			},
			wantContains: []string{
				"Node Type: PROJECT",
				"No columns available in context",
			},
			desc: "Error with PROJECT node and no available columns",
		},
		{
			name:       "error with context - AGG node",
			missingCol: [2]int32{1, 2},
			colName:    "missing_col",
			colMap: map[[2]int32][2]int32{
				{1, 0}: {0, 0},
				{2, 0}: {0, 1},
			},
			remapInfo: &RemapInfo{
				step: 2,
				node: &plan.Node{
					NodeId:   30,
					NodeType: plan.Node_AGG,
					AggList: []*plan.Expr{
						{
							Typ: plan.Type{Id: int32(types.T_int64)},
							Expr: &plan.Expr_F{
								F: &plan.Function{
									Func: &plan.ObjectRef{ObjName: "sum"},
									Args: []*plan.Expr{
										{
											Typ: plan.Type{Id: int32(types.T_int64)},
											Expr: &plan.Expr_Col{
												Col: &plan.ColRef{
													RelPos: 1,
													ColPos: 2,
													Name:   "missing_col",
												},
											},
										},
									},
								},
							},
						},
					},
				},
				srcExprIdx: 0,
			},
			wantContains: []string{
				"Node Type: AGG",
				"Available Columns in Context",
				"col1",
				"col3",
			},
			desc: "Error with AGG node should include aggregation expression",
		},
		{
			name:       "error without tip",
			missingCol: [2]int32{1, 2},
			colName:    "missing_col",
			colMap:     map[[2]int32][2]int32{},
			remapInfo: &RemapInfo{
				step: 0,
				node: &plan.Node{
					NodeId:   10,
					NodeType: plan.Node_FILTER,
				},
				tip:        "",
				srcExprIdx: -1,
			},
			wantContains: []string{
				"Context",
				"Step: 0",
				"Node ID: 10",
			},
			desc: "Error without tip and expression index should still show context",
		},
		{
			name:       "error with invalid expression index",
			missingCol: [2]int32{1, 2},
			colName:    "missing_col",
			colMap:     map[[2]int32][2]int32{},
			remapInfo: &RemapInfo{
				step: 0,
				node: &plan.Node{
					NodeId:     10,
					NodeType:   plan.Node_FILTER,
					FilterList: []*plan.Expr{},
				},
				srcExprIdx: 5, // Out of bounds
			},
			wantContains: []string{
				"Context",
				"Expression Index: 5",
			},
			desc: "Error with out-of-bounds expression index should still show index",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := builder.buildRemapErrorMessage(tt.missingCol, tt.colName, tt.colMap, tt.remapInfo)

			// Check that result contains all expected strings
			for _, wantStr := range tt.wantContains {
				assert.Contains(t, result, wantStr, "%s: should contain '%s'", tt.desc, wantStr)
			}

			// Check that result is not empty
			assert.NotEmpty(t, result, "%s: result should not be empty", tt.desc)

			// Check that result starts with error header
			assert.Contains(t, result, "Column remapping failed", "%s: should start with error header", tt.desc)
		})
	}
}

func TestParseRankOption(t *testing.T) {
	ctx := context.TODO()

	t.Run("valid mode pre", func(t *testing.T) {
		options := map[string]string{
			"mode": "pre",
		}
		rankOption, err := parseRankOption(options, ctx)
		require.NoError(t, err)
		require.NotNil(t, rankOption)
		require.Equal(t, "pre", rankOption.Mode)
	})

	t.Run("valid mode post", func(t *testing.T) {
		options := map[string]string{
			"mode": "post",
		}
		rankOption, err := parseRankOption(options, ctx)
		require.NoError(t, err)
		require.NotNil(t, rankOption)
		require.Equal(t, "post", rankOption.Mode)
	})

	t.Run("valid mode case insensitive", func(t *testing.T) {
		options := map[string]string{
			"MODE": "PRE",
		}
		rankOption, err := parseRankOption(options, ctx)
		require.NoError(t, err)
		require.NotNil(t, rankOption)
		require.Equal(t, "pre", rankOption.Mode)
	})

	t.Run("valid mode with whitespace", func(t *testing.T) {
		options := map[string]string{
			"mode": "  post  ",
		}
		rankOption, err := parseRankOption(options, ctx)
		require.NoError(t, err)
		require.NotNil(t, rankOption)
		require.Equal(t, "post", rankOption.Mode)
	})

	t.Run("invalid mode value", func(t *testing.T) {
		options := map[string]string{
			"mode": "invalid",
		}
		rankOption, err := parseRankOption(options, ctx)
		require.Error(t, err)
		require.Nil(t, rankOption)
		require.Contains(t, err.Error(), "mode must be 'pre', 'post', 'force', or 'auto'")
		require.Contains(t, err.Error(), "invalid")
	})

	t.Run("valid mode force", func(t *testing.T) {
		options := map[string]string{
			"mode": "force",
		}
		rankOption, err := parseRankOption(options, ctx)
		require.NoError(t, err)
		require.NotNil(t, rankOption)
		require.Equal(t, "force", rankOption.Mode)
	})

	t.Run("empty options map", func(t *testing.T) {
		options := map[string]string{}
		rankOption, err := parseRankOption(options, ctx)
		require.NoError(t, err)
		require.Nil(t, rankOption)
	})

	t.Run("nil options map", func(t *testing.T) {
		var options map[string]string = nil
		rankOption, err := parseRankOption(options, ctx)
		require.NoError(t, err)
		require.Nil(t, rankOption)
	})

	t.Run("options without mode", func(t *testing.T) {
		options := map[string]string{
			"fudge_factor": "3.0",
			"nprobe":       "10",
		}
		rankOption, err := parseRankOption(options, ctx)
		require.NoError(t, err)
		require.Nil(t, rankOption)
	})

	t.Run("options with mode and other options", func(t *testing.T) {
		options := map[string]string{
			"mode":         "pre",
			"fudge_factor": "3.0",
			"nprobe":       "10",
		}
		rankOption, err := parseRankOption(options, ctx)
		require.NoError(t, err)
		require.NotNil(t, rankOption)
		require.Equal(t, "pre", rankOption.Mode)
	})

	t.Run("case insensitive key matching", func(t *testing.T) {
		options := map[string]string{
			"MoDe": "post",
		}
		rankOption, err := parseRankOption(options, ctx)
		require.NoError(t, err)
		require.NotNil(t, rankOption)
		require.Equal(t, "post", rankOption.Mode)
	})

	t.Run("multiple mode keys with different cases", func(t *testing.T) {
		options := map[string]string{
			"mode": "pre",
			"MODE": "post",
		}
		// Should match the first one found (order is not guaranteed in map iteration)
		rankOption, err := parseRankOption(options, ctx)
		require.NoError(t, err)
		require.NotNil(t, rankOption)
		// The actual value depends on map iteration order, but should be either "pre" or "post"
		require.Contains(t, []string{"pre", "post"}, rankOption.Mode)
	})
}

// TestBaseBinder_bindComparisonExpr tests bindComparisonExpr with various comparison operators
func TestBaseBinder_bindComparisonExpr(t *testing.T) {
	testCases := []struct {
		name      string
		sql       string
		expectErr bool
		setupFunc func() (*QueryBuilder, *BindContext) // Optional custom setup
		checkFunc func(t *testing.T, expr *plan.Expr, err error)
	}{
		// Basic comparison operators
		{
			name:      "EQUAL: a = 1",
			sql:       "a = 1",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "=", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "LESS_THAN: a < 1",
			sql:       "a < 1",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "<", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "LESS_THAN_EQUAL: a <= 1",
			sql:       "a <= 1",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "<=", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "GREAT_THAN: a > 1",
			sql:       "a > 1",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, ">", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "GREAT_THAN_EQUAL: a >= 1",
			sql:       "a >= 1",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, ">=", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "NOT_EQUAL: a <> 1",
			sql:       "a <> 1",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "<>", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "NULL_SAFE_EQUAL: a <=> 1",
			sql:       "a <=> 1",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "<=>", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "LIKE: a LIKE 'test%'",
			sql:       "a LIKE 'test%'",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "like", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "NOT_LIKE: a NOT LIKE 'test%'",
			sql:       "a NOT LIKE 'test%'",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				// NOT_LIKE should be converted to NOT(LIKE(...))
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "not", funcExpr.F.Func.ObjName)
			},
		},
		// Note: ILIKE requires string types, but 'a' column is BIGINT in genBuilderAndCtx
		// So we'll test ILIKE with a string column setup
		{
			name:      "ILIKE: string_col ILIKE 'test%'",
			sql:       "string_col ILIKE 'test%'",
			expectErr: false,
			setupFunc: func() (*QueryBuilder, *BindContext) {
				builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
				bindCtx := NewBindContext(builder, nil)
				typ := types.T_varchar.ToType()
				plan2Type := makePlan2Type(&typ)
				bind := &Binding{
					tag:            1,
					nodeId:         0,
					db:             "select_test",
					table:          "bind_select",
					tableID:        0,
					cols:           []string{"string_col"},
					colIsHidden:    []bool{false},
					types:          []*plan.Type{&plan2Type},
					refCnts:        []uint{0},
					colIdByName:    map[string]int32{"string_col": 0},
					isClusterTable: false,
					defaults:       []string{""},
				}
				bindCtx.bindings = append(bindCtx.bindings, bind)
				bindCtx.bindingByTable[bind.table] = bind
				bindCtx.bindingByCol["string_col"] = bind
				bindCtx.bindingByTag[bind.tag] = bind
				return builder, bindCtx
			},
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "ilike", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "NOT_ILIKE: string_col NOT ILIKE 'test%'",
			sql:       "string_col NOT ILIKE 'test%'",
			expectErr: false,
			setupFunc: func() (*QueryBuilder, *BindContext) {
				builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
				bindCtx := NewBindContext(builder, nil)
				typ := types.T_varchar.ToType()
				plan2Type := makePlan2Type(&typ)
				bind := &Binding{
					tag:            1,
					nodeId:         0,
					db:             "select_test",
					table:          "bind_select",
					tableID:        0,
					cols:           []string{"string_col"},
					colIsHidden:    []bool{false},
					types:          []*plan.Type{&plan2Type},
					refCnts:        []uint{0},
					colIdByName:    map[string]int32{"string_col": 0},
					isClusterTable: false,
					defaults:       []string{""},
				}
				bindCtx.bindings = append(bindCtx.bindings, bind)
				bindCtx.bindingByTable[bind.table] = bind
				bindCtx.bindingByCol["string_col"] = bind
				bindCtx.bindingByTag[bind.tag] = bind
				return builder, bindCtx
			},
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				// NOT_ILIKE should be converted to NOT(ILIKE(...))
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "not", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "IN: a IN (1, 2, 3)",
			sql:       "a IN (1, 2, 3)",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "in", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "NOT_IN: a NOT IN (1, 2, 3)",
			sql:       "a NOT IN (1, 2, 3)",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "not_in", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "Tuple IN: (a, b) IN ((1, 2), (3, 4))",
			sql:       "(a, b) IN ((1, 2), (3, 4))",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "or", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "Tuple IN with Paren: ((a, b)) IN (((1, 2)), ((3, 4)))",
			sql:       "((a, b)) IN (((1, 2)), ((3, 4)))",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "or", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "Tuple NOT IN: (a, b) NOT IN ((1, 2), (3, 4))",
			sql:       "(a, b) NOT IN ((1, 2), (3, 4))",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "not", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "Tuple IN length mismatch: (a, b) IN ((1, 2, 3))",
			sql:       "(a, b) IN ((1, 2, 3))",
			expectErr: true,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.Error(t, err)
				require.Contains(t, err.Error(), "tuple length mismatch")
			},
		},
		// Tuple comparisons
		{
			name:      "Tuple EQUAL: (a, b) = (1, 2)",
			sql:       "(a, b) = (1, 2)",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				// Tuple comparison should result in AND of individual comparisons
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "and", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "Tuple EQUAL different lengths: (a, b) = (1, 2, 3)",
			sql:       "(a, b) = (1, 2, 3)",
			expectErr: true,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.Error(t, err)
				require.Contains(t, err.Error(), "different length")
			},
		},
		{
			name:      "Tuple LESS_THAN: (a, b) < (1, 2)",
			sql:       "(a, b) < (1, 2)",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				// Tuple < should result in complex expression with AND/OR
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				// Should be either "and" or "or" depending on the logic
				require.Contains(t, []string{"and", "or"}, funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "Tuple NOT_EQUAL: (a, b) <> (1, 2)",
			sql:       "(a, b) <> (1, 2)",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				// Tuple <> should result in OR of individual comparisons
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "or", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "Tuple LESS_THAN_EQUAL: (a, b) <= (1, 2)",
			sql:       "(a, b) <= (1, 2)",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				// Tuple <= should result in complex expression with AND/OR
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Contains(t, []string{"and", "or"}, funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "Tuple GREAT_THAN: (a, b) > (1, 2)",
			sql:       "(a, b) > (1, 2)",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				// Tuple > should result in complex expression with AND/OR
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Contains(t, []string{"and", "or"}, funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "Tuple GREAT_THAN_EQUAL: (a, b) >= (1, 2)",
			sql:       "(a, b) >= (1, 2)",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				// Tuple >= should result in complex expression with AND/OR
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Contains(t, []string{"and", "or"}, funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "Tuple LESS_THAN different lengths: (a, b) < (1, 2, 3)",
			sql:       "(a, b) < (1, 2, 3)",
			expectErr: true,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.Error(t, err)
				require.Contains(t, err.Error(), "different length")
			},
		},
		// REG_MATCH and NOT_REG_MATCH
		{
			name:      "REG_MATCH: string_col REGEXP 'pattern'",
			sql:       "string_col REGEXP 'pattern'",
			expectErr: false,
			setupFunc: func() (*QueryBuilder, *BindContext) {
				builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
				bindCtx := NewBindContext(builder, nil)
				typ := types.T_varchar.ToType()
				plan2Type := makePlan2Type(&typ)
				bind := &Binding{
					tag:            1,
					nodeId:         0,
					db:             "select_test",
					table:          "bind_select",
					tableID:        0,
					cols:           []string{"string_col"},
					colIsHidden:    []bool{false},
					types:          []*plan.Type{&plan2Type},
					refCnts:        []uint{0},
					colIdByName:    map[string]int32{"string_col": 0},
					isClusterTable: false,
					defaults:       []string{""},
				}
				bindCtx.bindings = append(bindCtx.bindings, bind)
				bindCtx.bindingByTable[bind.table] = bind
				bindCtx.bindingByCol["string_col"] = bind
				bindCtx.bindingByTag[bind.tag] = bind
				return builder, bindCtx
			},
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "reg_match", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "NOT_REG_MATCH: string_col NOT REGEXP 'pattern'",
			sql:       "string_col NOT REGEXP 'pattern'",
			expectErr: false,
			setupFunc: func() (*QueryBuilder, *BindContext) {
				builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
				bindCtx := NewBindContext(builder, nil)
				typ := types.T_varchar.ToType()
				plan2Type := makePlan2Type(&typ)
				bind := &Binding{
					tag:            1,
					nodeId:         0,
					db:             "select_test",
					table:          "bind_select",
					tableID:        0,
					cols:           []string{"string_col"},
					colIsHidden:    []bool{false},
					types:          []*plan.Type{&plan2Type},
					refCnts:        []uint{0},
					colIdByName:    map[string]int32{"string_col": 0},
					isClusterTable: false,
					defaults:       []string{""},
				}
				bindCtx.bindings = append(bindCtx.bindings, bind)
				bindCtx.bindingByTable[bind.table] = bind
				bindCtx.bindingByCol["string_col"] = bind
				bindCtx.bindingByTag[bind.tag] = bind
				return builder, bindCtx
			},
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "not_reg_match", funcExpr.F.Func.ObjName)
			},
		},
		// Single element tuple comparisons (edge case)
		{
			name:      "Single element tuple EQUAL: (a) = (1)",
			sql:       "(a) = (1)",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				// Single element tuple should still work
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				// For single element, it should be just "="
				require.Equal(t, "=", funcExpr.F.Func.ObjName)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Use custom setup if provided, otherwise use default
			var builder *QueryBuilder
			var bindCtx *BindContext
			if tc.setupFunc != nil {
				builder, bindCtx = tc.setupFunc()
			} else {
				builder, bindCtx = genBuilderAndCtx()
			}
			whereBinder := NewWhereBinder(builder, bindCtx)

			// Parse SQL to get comparison expression
			stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, "select * from bind_select where "+tc.sql, 1)
			if err != nil {
				if tc.expectErr {
					return
				}
				t.Fatalf("Failed to parse SQL: %v", err)
			}

			selectStmt := stmts[0].(*tree.Select)
			whereClause := selectStmt.Select.(*tree.SelectClause).Where
			if whereClause == nil {
				t.Fatalf("No WHERE clause found")
			}

			// Extract comparison expression
			compExpr, ok := whereClause.Expr.(*tree.ComparisonExpr)
			if !ok {
				t.Fatalf("WHERE clause is not a ComparisonExpr")
			}

			// Bind the comparison expression
			expr, err := whereBinder.bindComparisonExpr(compExpr, 0, false)

			if tc.expectErr {
				require.Error(t, err)
				if tc.checkFunc != nil {
					tc.checkFunc(t, expr, err)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, expr)
				if tc.checkFunc != nil {
					tc.checkFunc(t, expr, err)
				}
			}
		})
	}
}

func TestBaseBinderBindUnaryMinusMinInt64Literal(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()
	whereBinder := NewWhereBinder(builder, bindCtx)

	testCases := []struct {
		name       string
		sql        string
		checkValue func(t *testing.T, expr *plan.Expr)
	}{
		{
			name: "min int64 boundary",
			sql:  "-9223372036854775808",
			checkValue: func(t *testing.T, expr *plan.Expr) {
				require.Equal(t, int32(types.T_int64), expr.Typ.Id)
				require.Equal(t, int64(math.MinInt64), expr.GetLit().GetI64Val())
			},
		},
		{
			name: "below min int64 keeps decimal",
			sql:  "-9223372036854775809",
			checkValue: func(t *testing.T, expr *plan.Expr) {
				require.Equal(t, int32(types.T_decimal128), expr.Typ.Id)
				require.NotNil(t, expr.GetLit().GetDecimal128Val())
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, "select "+tc.sql+" from bind_select", 1)
			require.NoError(t, err)

			selectStmt := stmts[0].(*tree.Select)
			selectClause := selectStmt.Select.(*tree.SelectClause)
			unaryExpr, ok := selectClause.Exprs[0].Expr.(*tree.UnaryExpr)
			require.True(t, ok)

			expr, err := whereBinder.bindUnaryExpr(unaryExpr, 0, false)
			require.NoError(t, err)
			require.NotNil(t, expr.GetLit())
			tc.checkValue(t, expr)
		})
	}
}

// TestBaseBinder_bindUnaryExpr tests bindUnaryExpr with various unary operators
func TestBaseBinder_bindUnaryExpr(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()
	whereBinder := NewWhereBinder(builder, bindCtx)

	testCases := []struct {
		name      string
		sql       string
		expectErr bool
		checkFunc func(t *testing.T, expr *plan.Expr, err error)
	}{
		{
			name:      "UNARY_MINUS: -a",
			sql:       "-a",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "unary_minus", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "UNARY_PLUS: +a",
			sql:       "+a",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "unary_plus", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "UNARY_TILDE: ~a",
			sql:       "~a",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "unary_tilde", funcExpr.F.Func.ObjName)
			},
		},
		// Note: UNARY_MARK (?a) is not a standard SQL syntax and may not be parseable
		// Skipping this test case as it's not a valid SQL expression
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse SQL to get unary expression
			stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, "select "+tc.sql+" from bind_select", 1)
			if err != nil {
				if tc.expectErr {
					return
				}
				t.Fatalf("Failed to parse SQL: %v", err)
			}

			selectStmt := stmts[0].(*tree.Select)
			selectClause := selectStmt.Select.(*tree.SelectClause)
			if len(selectClause.Exprs) == 0 {
				t.Fatalf("No select expressions found")
			}

			// Extract unary expression
			unaryExpr, ok := selectClause.Exprs[0].Expr.(*tree.UnaryExpr)
			if !ok {
				t.Fatalf("Select expression is not a UnaryExpr")
			}

			// Bind the unary expression
			expr, err := whereBinder.bindUnaryExpr(unaryExpr, 0, false)

			if tc.expectErr {
				require.Error(t, err)
				if tc.checkFunc != nil {
					tc.checkFunc(t, expr, err)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, expr)
				if tc.checkFunc != nil {
					tc.checkFunc(t, expr, err)
				}
			}
		})
	}
}

// TestBaseBinder_bindBinaryExpr tests bindBinaryExpr with various binary operators
func TestBaseBinder_bindBinaryExpr(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()
	whereBinder := NewWhereBinder(builder, bindCtx)

	testCases := []struct {
		name      string
		sql       string
		expectErr bool
		checkFunc func(t *testing.T, expr *plan.Expr, err error)
	}{
		{
			name:      "PLUS: a + 1",
			sql:       "a + 1",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "+", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "MINUS: a - 1",
			sql:       "a - 1",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "-", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "MULTI: a * 1",
			sql:       "a * 1",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "*", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "MOD: a % 1",
			sql:       "a % 1",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "%", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "DIV: a / 1",
			sql:       "a / 1",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "/", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "INTEGER_DIV: a div 1",
			sql:       "a div 1",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "div", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "BIT_XOR: a ^ 1",
			sql:       "a ^ 1",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "^", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "BIT_OR: a | 1",
			sql:       "a | 1",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "|", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "BIT_AND: a & 1",
			sql:       "a & 1",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "&", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "LEFT_SHIFT: a << 1",
			sql:       "a << 1",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "<<", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "RIGHT_SHIFT: a >> 1",
			sql:       "a >> 1",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, ">>", funcExpr.F.Func.ObjName)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse SQL to get binary expression
			stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, "select "+tc.sql+" from bind_select", 1)
			if err != nil {
				if tc.expectErr {
					return
				}
				t.Fatalf("Failed to parse SQL: %v", err)
			}

			selectStmt := stmts[0].(*tree.Select)
			selectClause := selectStmt.Select.(*tree.SelectClause)
			if len(selectClause.Exprs) == 0 {
				t.Fatalf("No select expressions found")
			}

			// Extract binary expression
			binaryExpr, ok := selectClause.Exprs[0].Expr.(*tree.BinaryExpr)
			if !ok {
				t.Fatalf("Select expression is not a BinaryExpr")
			}

			// Bind the binary expression
			expr, err := whereBinder.bindBinaryExpr(binaryExpr, 0, false)

			if tc.expectErr {
				require.Error(t, err)
				if tc.checkFunc != nil {
					tc.checkFunc(t, expr, err)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, expr)
				if tc.checkFunc != nil {
					tc.checkFunc(t, expr, err)
				}
			}
		})
	}
}

// TestBaseBinder_bindRangeCond tests bindRangeCond with BETWEEN and NOT BETWEEN
func TestBaseBinder_bindRangeCond(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()
	whereBinder := NewWhereBinder(builder, bindCtx)

	testCases := []struct {
		name      string
		sql       string
		expectErr bool
		checkFunc func(t *testing.T, expr *plan.Expr, err error)
	}{
		{
			name:      "BETWEEN: a BETWEEN 1 AND 10",
			sql:       "a BETWEEN 1 AND 10",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "between", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "NOT BETWEEN: a NOT BETWEEN 1 AND 10",
			sql:       "a NOT BETWEEN 1 AND 10",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				// NOT BETWEEN should be converted to OR of two comparisons
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "or", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "BETWEEN with tuple: (a, b) BETWEEN (1, 2) AND (10, 20)",
			sql:       "(a, b) BETWEEN (1, 2) AND (10, 20)",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				// Tuple BETWEEN should be converted to AND of two comparisons
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "and", funcExpr.F.Func.ObjName)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse SQL to get range condition
			stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, "select * from bind_select where "+tc.sql, 1)
			if err != nil {
				if tc.expectErr {
					return
				}
				t.Fatalf("Failed to parse SQL: %v", err)
			}

			selectStmt := stmts[0].(*tree.Select)
			whereClause := selectStmt.Select.(*tree.SelectClause).Where
			if whereClause == nil {
				t.Fatalf("No WHERE clause found")
			}

			// Extract range condition
			rangeCond, ok := whereClause.Expr.(*tree.RangeCond)
			if !ok {
				t.Fatalf("WHERE clause is not a RangeCond")
			}

			// Bind the range condition
			expr, err := whereBinder.bindRangeCond(rangeCond, 0, false)

			if tc.expectErr {
				require.Error(t, err)
				if tc.checkFunc != nil {
					tc.checkFunc(t, expr, err)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, expr)
				if tc.checkFunc != nil {
					tc.checkFunc(t, expr, err)
				}
			}
		})
	}
}

// TestBaseBinder_baseBindExpr tests baseBindExpr with various expression types
func TestBaseBinder_baseBindExpr(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()
	whereBinder := NewWhereBinder(builder, bindCtx)

	testCases := []struct {
		name      string
		sql       string
		expectErr bool
		checkFunc func(t *testing.T, expr *plan.Expr, err error)
	}{
		{
			name:      "ParenExpr: (a)",
			sql:       "(a)",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
			},
		},
		{
			name:      "OrExpr: a OR b",
			sql:       "a OR b",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "or", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "AndExpr: a AND b",
			sql:       "a AND b",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "and", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "NotExpr: NOT a",
			sql:       "NOT a",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "not", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "IsNullExpr: a IS NULL",
			sql:       "a IS NULL",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "isnull", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "IsNotNullExpr: a IS NOT NULL",
			sql:       "a IS NOT NULL",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
				funcExpr, ok := expr.Expr.(*plan.Expr_F)
				require.True(t, ok)
				require.Equal(t, "isnotnull", funcExpr.F.Func.ObjName)
			},
		},
		{
			name:      "CastExpr: CAST(a AS INT)",
			sql:       "CAST(a AS INT)",
			expectErr: false,
			checkFunc: func(t *testing.T, expr *plan.Expr, err error) {
				require.NoError(t, err)
				require.NotNil(t, expr)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse SQL to get expression
			stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, "select "+tc.sql+" from bind_select", 1)
			if err != nil {
				if tc.expectErr {
					return
				}
				t.Fatalf("Failed to parse SQL: %v", err)
			}

			selectStmt := stmts[0].(*tree.Select)
			selectClause := selectStmt.Select.(*tree.SelectClause)
			if len(selectClause.Exprs) == 0 {
				t.Fatalf("No select expressions found")
			}

			// Bind the expression using baseBindExpr
			expr, err := whereBinder.baseBindExpr(selectClause.Exprs[0].Expr, 0, false)

			if tc.expectErr {
				require.Error(t, err)
				if tc.checkFunc != nil {
					tc.checkFunc(t, expr, err)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, expr)
				if tc.checkFunc != nil {
					tc.checkFunc(t, expr, err)
				}
			}
		})
	}
}

// TestAppendGroupingSetOrderByProjectsOrdinalWithinVisibleLen verifies that a
// positional ORDER BY reference within the visible select list passes through,
// while a grouping key is tracked as a hidden column.
func TestAppendGroupingSetOrderByProjectsOrdinalWithinVisibleLen(t *testing.T) {
	stmts, err := parsers.Parse(
		context.TODO(),
		dialect.MYSQL,
		`select a, count(*)
		from select_test.bind_select
		group by a with rollup
		order by 2, grouping(a)`,
		1,
	)
	require.NoError(t, err)

	selectStmt := stmts[0].(*tree.Select)
	selectClause := selectStmt.Select.(*tree.SelectClause)
	branchExprs, rewrittenOrderBy, orderResolve, err := prepareGroupingSetOrderByProjects(nil, selectStmt.OrderBy, selectClause.Exprs, false)
	require.NoError(t, err)
	// visible: [a, count(*)] + hidden: [grouping(a)] = 3
	require.Len(t, branchExprs, 3)

	// First ORDER BY: ordinal 2 -> passes through; second: grouping(a) -> 0th hidden.
	require.Equal(t, []int{-1, 0}, orderResolve.hiddenIdx)
	existingPos, ok := rewrittenOrderBy[0].Expr.(*tree.NumVal)
	require.True(t, ok)
	pos, ok := existingPos.Int64()
	require.True(t, ok)
	require.Equal(t, int64(2), pos)
}

// TestAppendGroupingSetOrderByProjectsOrdinalOneWithinVisibleLen verifies that
// ordinal 1 within a 2-column visible select list is accepted.
func TestAppendGroupingSetOrderByProjectsOrdinalOneWithinVisibleLen(t *testing.T) {
	stmts, err := parsers.Parse(
		context.TODO(),
		dialect.MYSQL,
		`select a, count(*)
		from select_test.bind_select
		group by a with rollup
		order by 1, grouping(a)`,
		1,
	)
	require.NoError(t, err)

	selectStmt := stmts[0].(*tree.Select)
	selectClause := selectStmt.Select.(*tree.SelectClause)
	branchExprs, _, orderResolve, err := prepareGroupingSetOrderByProjects(nil, selectStmt.OrderBy, selectClause.Exprs, false)
	require.NoError(t, err)
	require.Len(t, branchExprs, 3)
	require.Equal(t, []int{-1, 0}, orderResolve.hiddenIdx)
}

// TestGroupingSetOrderByOrdinalStarExpansion is an end-to-end guard for the
// star-expansion contract in the grouping-set ORDER BY path:
//   - positional ORDER BY references are validated against the *visible*,
//     star-expanded select list, not the raw AST list nor the hidden-expanded
//     projection; and
//   - a grouping() sort key resolves to its hidden projection column, not to a
//     visible column, even when '*' widens the visible list.
//
// bind_select has columns (a, b, c), so `*, count(*)` yields 4 visible columns.
func TestGroupingSetOrderByOrdinalStarExpansion(t *testing.T) {
	mock := NewMockOptimizer(false)

	// Valid visible ordinals (3 -> c, 4 -> count(*)) must be accepted even though
	// the raw AST select list has only 2 entries.
	pass := []string{
		"select *, count(*) from select_test.bind_select group by a, b, c with rollup order by 3, grouping(a)",
		"select *, count(*) from select_test.bind_select group by a, b, c with rollup order by 4",
	}
	for _, sql := range pass {
		_, err := runOneStmt(mock, t, sql)
		require.NoError(t, err, "sql=%s", sql)
	}

	// Ordinal 5 addresses a hidden grouping key (visible width is 4) and must be
	// rejected; ordinal 6 is out of range entirely.
	fail := []string{
		"select *, count(*) from select_test.bind_select group by a, b, c with rollup order by 5, grouping(a)",
		"select *, count(*) from select_test.bind_select group by a, b, c with rollup order by 6",
	}
	for _, sql := range fail {
		_, err := runOneStmt(mock, t, sql)
		require.Error(t, err, "sql=%s", sql)
		require.Contains(t, err.Error(), "is not in select list", "sql=%s", sql)
	}
}

// TestGroupingSetOrderByGroupingResolvesHiddenColumnWithStar verifies Bug 2: a
// grouping() ORDER BY key must sort by the appended hidden projection column,
// not by a visible column shifted in by '*' expansion.
func TestGroupingSetOrderByGroupingResolvesHiddenColumnWithStar(t *testing.T) {
	mock := NewMockOptimizer(false)

	starPlan, err := runOneStmt(mock, t,
		"select *, count(*) from select_test.bind_select group by a, b, c with rollup order by grouping(a)")
	require.NoError(t, err)
	starSort := firstSortColPos(t, starPlan)

	// Without '*' the visible list is [a, b, c, count(*)] and the hidden
	// grouping(a) column lands right after it. With '*' the visible list is the
	// same width, so the hidden column position must be identical.
	explicitPlan, err := runOneStmt(mock, t,
		"select a, b, c, count(*) from select_test.bind_select group by a, b, c with rollup order by grouping(a)")
	require.NoError(t, err)
	explicitSort := firstSortColPos(t, explicitPlan)

	require.Equal(t, explicitSort, starSort,
		"grouping(a) must resolve to the same hidden column with and without '*'")
	// The hidden grouping column sits past the 4 visible columns (0-based >= 4).
	require.GreaterOrEqual(t, starSort, int32(4))
}

// firstSortColPos returns the ColPos of the first ORDER BY key of the first SORT
// node in the plan, failing the test if the key is not a plain column reference.
func firstSortColPos(t *testing.T, p *Plan) int32 {
	for _, n := range p.GetQuery().Nodes {
		if n.NodeType == plan.Node_SORT {
			require.NotEmpty(t, n.OrderBy)
			col := n.OrderBy[0].Expr.GetCol()
			require.NotNil(t, col, "first sort key is not a column reference")
			return col.ColPos
		}
	}
	t.Fatal("no SORT node in plan")
	return -1
}

// TestGroupingSetDistinctOrderByStarExpansion verifies that on the DISTINCT
// grouping-set path a grouping() ORDER BY key matching a visible select item
// resolves to that item's star-expanded position, not its raw AST index.
// bind_select has columns (a, b, c), so `*, grouping(a)` yields 4 visible
// columns and grouping(a) must sort by 0-based ColPos 3 — the same position as
// in the explicit-column spelling.
func TestGroupingSetDistinctOrderByStarExpansion(t *testing.T) {
	mock := NewMockOptimizer(false)

	starPlan, err := runOneStmt(mock, t,
		"select distinct *, grouping(a) as ga from select_test.bind_select group by a, b, c with rollup order by grouping(a)")
	require.NoError(t, err)
	starSort := firstSortColPos(t, starPlan)

	explicitPlan, err := runOneStmt(mock, t,
		"select distinct a, b, c, grouping(a) as ga from select_test.bind_select group by a, b, c with rollup order by grouping(a)")
	require.NoError(t, err)
	explicitSort := firstSortColPos(t, explicitPlan)

	require.Equal(t, explicitSort, starSort,
		"DISTINCT grouping(a) must resolve to the same visible column with and without '*'")
	require.Equal(t, int32(3), starSort)
}

// TestGroupingSetDistinctOrderByAliasShadowing verifies clause-correct alias
// semantics when matching a DISTINCT ORDER BY expression against the visible
// select list. Nested names prefer source columns over aliases, matching the
// ordinary distinctOrderBinder.
func TestGroupingSetDistinctOrderByAliasShadowing(t *testing.T) {
	mock := NewMockOptimizer(false)

	p, err := runOneStmt(mock, t,
		"select distinct a as b, grouping(a) + b from select_test.bind_select group by a, b with rollup order by grouping(a) + b")
	require.NoError(t, err)
	require.Equal(t, int32(1), firstSortColPos(t, p))

	// Sanity: without shadowing the same shape must still be accepted, sorting
	// by the matched visible column (0-based ColPos 1).
	p, err = runOneStmt(mock, t,
		"select distinct a, grouping(a) + b from select_test.bind_select group by a, b with rollup order by grouping(a) + b")
	require.NoError(t, err)
	require.Equal(t, int32(1), firstSortColPos(t, p))
}

func TestGroupingSetDistinctOrderAliasResolutionParity(t *testing.T) {
	mock := NewMockOptimizer(true)

	_, err := runOneStmt(mock, t,
		"select distinct grouping(a), abs(b) as b from select_test.bind_select "+
			"group by a, b with rollup order by grouping(a) + b")
	require.Error(t, err)
	require.Contains(t, err.Error(), "for SELECT DISTINCT, ORDER BY expressions must appear in select list")

	_, err = runOneStmt(mock, t,
		"select distinct grouping(a), abs(b) as x, -abs(b) as x from select_test.bind_select "+
			"group by a, b with rollup order by grouping(a) + x")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Column 'x' in order clause is ambiguous")
}

// TestGroupingSetDistinctOrderByBetweenStars verifies that a DISTINCT ORDER BY
// grouping() key matching a visible item that sits BETWEEN two star expansions
// (t1.*, grouping(a), t2.*) resolves through the bound visible projection.
// t1 (bind_select) expands to 3 columns (a, b, c), so grouping(a) as ga lands
// at 0-based ColPos 3.
func TestGroupingSetDistinctOrderByBetweenStars(t *testing.T) {
	mock := NewMockOptimizer(false)

	p, err := runOneStmt(mock, t,
		"select distinct t1.*, grouping(a) as ga, t2.* from select_test.bind_select as t1, nation as t2 group by a, b, c, n_nationkey, n_name, n_regionkey, n_comment with rollup order by grouping(a)")
	require.NoError(t, err)
	require.Equal(t, int32(3), firstSortColPos(t, p))
}

func TestGroupingSetDistinctOrderByBoundIdentity(t *testing.T) {
	mock := NewMockOptimizer(false)

	// The unqualified b in ORDER BY resolves unambiguously to t1.b and therefore
	// matches the selected bound expression grouping(a) + t1.b.
	p, err := runOneStmt(mock, t,
		"select distinct grouping(a) + t1.b from select_test.bind_select as t1 group by t1.a, t1.b with rollup order by grouping(a) + b")
	require.NoError(t, err)
	require.Equal(t, int32(0), firstSortColPos(t, p))

	// An unqualified GROUPING argument is ambiguous across self-join inputs and
	// must fail instead of being text-matched to either visible grouping bit.
	_, err = runOneStmt(mock, t,
		"select distinct grouping(a) from select_test.bind_select as t1, select_test.bind_select as t2 group by t1.a, t2.a with rollup order by grouping(a)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "ambiguous")
}

func TestQualifyBoundGroupingOrderExprPreservesRelationIdentity(t *testing.T) {
	_, bindCtx := genBuilderAndCtx()
	t1 := bindCtx.bindings[0]
	t1.table = "t1"
	t1.tag = 1
	t2 := *t1
	t2.table = "t2"
	t2.tag = 2
	bindCtx.bindings = []*Binding{t1, &t2}
	bindCtx.bindingByTable = map[string]*Binding{"t1": t1, "t2": &t2}
	bindCtx.bindingByTag = map[int32]*Binding{1: t1, 2: &t2}
	bindCtx.bindingByCol = map[string]*Binding{}

	makeGrouping := func(table string) tree.Expr {
		stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, "select grouping(a)", 1)
		require.NoError(t, err)
		expr := stmts[0].(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
		funcExpr := expr.(*tree.FuncExpr)
		funcExpr.Exprs[0] = tree.NewUnresolvedName(tree.NewCStr(table, 0), tree.NewCStr("a", 0))
		return funcExpr
	}

	t1Expr, err := qualifyBoundGroupingOrderExpr(bindCtx, makeGrouping("t1"))
	require.NoError(t, err)
	t2Expr, err := qualifyBoundGroupingOrderExpr(bindCtx, makeGrouping("t2"))
	require.NoError(t, err)
	require.NotEqual(t, tree.String(t1Expr, dialect.MYSQL), tree.String(t2Expr, dialect.MYSQL))
	require.Equal(t, "grouping(t1.a)", tree.String(t1Expr, dialect.MYSQL))
	require.Equal(t, "grouping(t2.a)", tree.String(t2Expr, dialect.MYSQL))
}

func hasNodeType(p *Plan, nt plan.Node_NodeType) bool {
	for _, n := range p.GetQuery().Nodes {
		if n.NodeType == nt {
			return true
		}
	}
	return false
}

// hasAggAboveUnionAll reports whether an AGG node was appended after the last
// UNION ALL, i.e. a whole-result de-duplication step sits above the grouping-set
// union chain (the DISTINCT node is rewritten to AGG). Nodes are in append order,
// with the union built before any top-level DISTINCT.
func hasAggAboveUnionAll(p *Plan) bool {
	nodes := p.GetQuery().Nodes
	lastUnion := -1
	for i, n := range nodes {
		if n.NodeType == plan.Node_UNION_ALL {
			lastUnion = i
		}
	}
	if lastUnion < 0 {
		return false
	}
	for _, n := range nodes[lastUnion+1:] {
		if n.NodeType == plan.Node_AGG {
			return true
		}
	}
	return false
}

// TestGroupingSetDistinctGlobalDedup verifies that SELECT DISTINCT over a
// grouping-set (ROLLUP) expansion de-duplicates the whole result: the branches
// are stitched with UNION ALL (so every ROLLUP super-aggregate / grand-total row
// survives) and a single DISTINCT step (rewritten to AGG) sits above the union.
// The non-distinct form has no such de-dup step.
func TestGroupingSetDistinctGlobalDedup(t *testing.T) {
	mock := NewMockOptimizer(false)

	distinctPlan, err := runOneStmt(mock, t,
		"select distinct a, grouping(a) as ga from select_test.bind_select group by a, b with rollup")
	require.NoError(t, err)
	require.True(t, hasNodeType(distinctPlan, plan.Node_UNION_ALL),
		"DISTINCT grouping-set must keep UNION ALL so ROLLUP rows survive")
	require.True(t, hasAggAboveUnionAll(distinctPlan),
		"DISTINCT grouping-set must de-duplicate the whole result above the union")

	allPlan, err := runOneStmt(mock, t,
		"select a, grouping(a) as ga from select_test.bind_select group by a, b with rollup")
	require.NoError(t, err)
	require.True(t, hasNodeType(allPlan, plan.Node_UNION_ALL),
		"non-distinct grouping-set keeps UNION ALL")
	require.False(t, hasAggAboveUnionAll(allPlan),
		"non-distinct grouping-set has no whole-result de-dup above the union")
}

func TestGroupingSetDistinctOrderByHiddenKeyAfterDedup(t *testing.T) {
	mock := NewMockOptimizer(true)

	p, err := runOneStmt(mock, t,
		"select distinct a from select_test.bind_select group by a, b with rollup order by rand()")
	require.NoError(t, err)

	nodes := p.GetQuery().Nodes
	lastUnion := -1
	for i, node := range nodes {
		if node.NodeType == plan.Node_UNION_ALL {
			lastUnion = i
		}
	}
	require.NotEqual(t, -1, lastUnion)

	distinctGroup := -1
	for i := lastUnion + 1; i < len(nodes); i++ {
		if nodes[i].NodeType == plan.Node_AGG {
			distinctGroup = i
			break
		}
	}
	require.NotEqual(t, -1, distinctGroup)
	require.Len(t, nodes[distinctGroup].GroupBy, 1,
		"the hidden random order key must not enter the visible DISTINCT tuple")

	hasOrderProject := false
	for i := distinctGroup + 1; i < len(nodes); i++ {
		if nodes[i].NodeType == plan.Node_PROJECT && len(nodes[i].ProjectList) == 2 {
			hasOrderProject = true
			break
		}
	}
	require.True(t, hasOrderProject, "the hidden random key must be projected after DISTINCT")
}

func TestGroupingSetDistinctDerivedGroupingOrder(t *testing.T) {
	mock := NewMockOptimizer(true)
	for _, testCase := range []struct {
		name    string
		groupBy string
	}{
		{name: "rollup", groupBy: "a, b with rollup"},
		{name: "cube", groupBy: "cube(a, b)"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			p, err := runOneStmt(mock, t,
				"select distinct grouping(a) as ga, b from select_test.bind_select "+
					"group by "+testCase.groupBy+" order by grouping(a) + cast(b as int)")
			require.NoError(t, err)
			require.True(t, hasAggAboveUnionAll(p))
		})
	}

	_, err := runOneStmt(mock, t,
		"select distinct b from select_test.bind_select group by a, b with rollup order by grouping(a) + b")
	require.Error(t, err)
	require.Contains(t, err.Error(), "for SELECT DISTINCT, ORDER BY expressions must appear in select list")
}

func TestGroupingSetDistinctListGroupingOrder(t *testing.T) {
	mock := NewMockOptimizer(true)
	for _, testCase := range []struct {
		name    string
		groupBy string
		orderBy string
	}{
		{
			name:    "rollup in",
			groupBy: "a, b with rollup",
			orderBy: "grouping(a) + (b in (1, 2))",
		},
		{
			name:    "rollup not in",
			groupBy: "a, b with rollup",
			orderBy: "grouping(a) + (b not in (1, 2))",
		},
		{
			name:    "cube",
			groupBy: "cube(a, b)",
			orderBy: "grouping(a) + (b in (1, 2))",
		},
		{
			name:    "grouping sets",
			groupBy: "grouping sets ((a, b), (a), ())",
			orderBy: "grouping(a) + (b in (1, 2))",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			p, err := runOneStmt(mock, t,
				"select distinct grouping(a), b from select_test.bind_select "+
					"group by "+testCase.groupBy+" order by "+testCase.orderBy)
			require.NoError(t, err)
			require.True(t, hasAggAboveUnionAll(p))
		})
	}

	_, err := runOneStmt(mock, t,
		"select distinct grouping(a), b from select_test.bind_select "+
			"group by a, b with rollup order by grouping(a) + (b in (a, 2))")
	require.Error(t, err)
	require.Contains(t, err.Error(), "for SELECT DISTINCT, ORDER BY expressions must appear in select list")
}

func TestRemapGroupingSetDistinctOrderExprList(t *testing.T) {
	builder, branchCtx := genBuilderAndCtx()
	unionCtx := NewBindContext(builder, nil)
	unionCtx.projectTag = builder.GenNewBindTag()

	intType := plan.Type{Id: int32(types.T_int64)}
	selected := GetColExpr(intType, 1, 1)
	unionCtx.projects = []*plan.Expr{DeepCopyExpr(selected)}
	selectedKey, err := projectExprKey(selected)
	require.NoError(t, err)
	branchCtx.projectByExpr[selectedKey] = 0

	listExpr := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_tuple)},
		Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{
			DeepCopyExpr(selected),
			makePlan2Int64ConstExprWithType(2),
		}}},
	}
	remapped, err := remapGroupingSetDistinctOrderExpr(
		context.Background(), listExpr, branchCtx, unionCtx, 1, true)
	require.NoError(t, err)
	require.Equal(t, unionCtx.projectTag, remapped.GetList().List[0].GetCol().RelPos)
	require.Equal(t, int32(0), remapped.GetList().List[0].GetCol().ColPos)
	require.NotSame(t, listExpr, remapped)

	vectorExpr := &plan.Expr{
		Typ:  intType,
		Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: 2}},
	}
	remappedVector, err := remapGroupingSetDistinctOrderExpr(
		context.Background(), vectorExpr, branchCtx, unionCtx, 1, false)
	require.NoError(t, err)
	require.NotSame(t, vectorExpr, remappedVector)

	unselectedList := DeepCopyExpr(listExpr)
	unselectedList.GetList().List[0] = GetColExpr(intType, 1, 0)
	_, err = remapGroupingSetDistinctOrderExpr(
		context.Background(), unselectedList, branchCtx, unionCtx, 1, false)
	require.Error(t, err)

	nilList := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_tuple)},
		Expr: &plan.Expr_List{},
	}
	_, err = remapGroupingSetDistinctOrderExpr(
		context.Background(), nilList, branchCtx, unionCtx, 1, false)
	require.Error(t, err)
}

func TestGroupingSetDistinctOrderProjectionBoundaries(t *testing.T) {
	mock := NewMockOptimizer(true)

	_, err := runOneStmt(mock, t,
		"select distinct grouping(a) + b from select_test.bind_select "+
			"group by a, b with rollup order by (grouping(a) + b) + 1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "for SELECT DISTINCT, ORDER BY expressions must appear in select list")

	p, err := runOneStmt(mock, t,
		"select distinct grouping(a), abs(b) as x from select_test.bind_select "+
			"group by a, b with rollup order by grouping(a) + x")
	require.NoError(t, err)
	require.True(t, hasAggAboveUnionAll(p))

	_, err = runOneStmt(mock, t,
		"select distinct grouping(a), abs(b) from select_test.bind_select "+
			"group by a, b with rollup order by grouping(a) + abs(b)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "for SELECT DISTINCT, ORDER BY expressions must appear in select list")

	_, err = runOneStmt(mock, t,
		"select distinct grouping(a), abs(b) as x from select_test.bind_select "+
			"group by a, b with rollup order by grouping(a) + abs(b) + x")
	require.Error(t, err)
	require.Contains(t, err.Error(), "for SELECT DISTINCT, ORDER BY expressions must appear in select list")
}

func TestGroupingSetDistinctOrderAliasIdentity(t *testing.T) {
	mock := NewMockOptimizer(true)
	queries := []string{
		"select distinct grouping(a), abs(b) as x, abs(b) as y " +
			"from select_test.bind_select group by a, b with rollup order by grouping(a) + y",
		"select distinct grouping(a), abs(b) as x " +
			"from select_test.bind_select group by a, b with rollup order by grouping(a) + nullif(x, 0)",
		"select distinct grouping(a), abs(b) as x " +
			"from select_test.bind_select group by a, b with rollup " +
			"order by grouping(a) + ((x, x + 1) in ((1, 2), (2, 3)))",
	}
	for _, query := range queries {
		p, err := runOneStmt(mock, t, query)
		require.NoError(t, err, query)
		require.True(t, hasAggAboveUnionAll(p), query)
	}
}

func TestGroupingSetDistinctOrderKeepsNestedVolatileCall(t *testing.T) {
	mock := NewMockOptimizer(true)
	p, err := runOneStmt(mock, t,
		"select distinct grouping(a), rand() as r from select_test.bind_select "+
			"group by a, b with rollup order by grouping(a) + rand()")
	require.NoError(t, err)

	containsRand := func(expr *plan.Expr) bool {
		var visit func(*plan.Expr) bool
		visit = func(current *plan.Expr) bool {
			if current == nil {
				return false
			}
			if fn := current.GetF(); fn != nil {
				if fn.Func != nil && strings.EqualFold(fn.Func.ObjName, "rand") {
					return true
				}
				for _, arg := range fn.Args {
					if visit(arg) {
						return true
					}
				}
			}
			return false
		}
		return visit(expr)
	}

	foundOrderProjection := false
	for _, node := range p.GetQuery().Nodes {
		if node.NodeType != plan.Node_PROJECT || len(node.ProjectList) != 3 {
			continue
		}
		if containsRand(node.ProjectList[2]) {
			foundOrderProjection = true
			require.Nil(t, node.ProjectList[2].GetCol(),
				"the nested volatile call must not reuse the RAND select output")
			break
		}
	}
	require.True(t, foundOrderProjection)
}

func TestGroupingSetDistinctVectorProjects(t *testing.T) {
	for _, typ := range []types.T{types.T_array_float32, types.T_array_float64} {
		t.Run(typ.String(), func(t *testing.T) {
			mock := NewMockOptimizer(true)
			vectorCol := mock.ctxt.tables["bind_select"].Cols[0]
			vectorCol.Name = "v"
			vectorCol.OriginName = "v"
			vectorCol.Typ = plan.Type{Id: int32(typ), Width: 3}
			for _, groupBy := range []string{"v with rollup", "cube(v)"} {
				_, err := runOneStmt(mock, t,
					"select distinct v from select_test.bind_select group by "+groupBy)
				require.NoError(t, err)
			}
		})
	}
}

func TestIffVectorCommonType(t *testing.T) {
	mock := NewMockOptimizer(true)
	for _, query := range []string{
		"select if(false, cast('[0,0]' as vecf32(2)), cast('[1.0000000001,2]' as vecf64(2)))",
		"select if(false, cast('[1.0000000001,2]' as vecf64(2)), cast('[0,0]' as vecf32(2)))",
	} {
		p, err := runOneStmt(mock, t, query)
		require.NoError(t, err)
		root := p.GetQuery().Nodes[p.GetQuery().Steps[0]]
		require.Equal(t, int32(types.T_array_float64), root.ProjectList[0].Typ.Id)
		require.Equal(t, int32(2), root.ProjectList[0].Typ.Width)
	}

	_, err := runOneStmt(mock, t,
		"select if(false, cast('[1,2]' as vecf32(2)), cast('[3,4,5]' as vecf32(3)))")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid argument function if")
}

func TestNormalizeGroupingSetDistinctProjectsTypedNull(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()
	for _, typ := range []types.T{
		types.T_int64,
		types.T_varchar,
		types.T_decimal128,
		types.T_date,
		types.T_uuid,
		types.T_array_float32,
		types.T_array_float64,
	} {
		t.Run(typ.String(), func(t *testing.T) {
			project := GetColExpr(
				plan.Type{Id: int32(typ), NotNullable: false},
				bindCtx.projectTag,
				0,
			)
			normalized, err := normalizeGroupingSetDistinctProjects(builder.GetContext(), []*plan.Expr{project})
			require.NoError(t, err)
			require.Len(t, normalized, 1)

			ifExpr := normalized[0].GetF()
			require.NotNil(t, ifExpr)
			require.Equal(t, "if", ifExpr.Func.ObjName)
			require.Len(t, ifExpr.Args, 3)
			nullLiteral := ifExpr.Args[1].GetLit()
			require.NotNil(t, nullLiteral)
			require.True(t, nullLiteral.Isnull)
			require.Equal(t, int32(typ), ifExpr.Args[1].Typ.Id)
		})
	}

	mock := NewMockOptimizer(true)
	_, err := runOneStmt(mock, t,
		"select distinct snapshot_id from mo_catalog.mo_snapshots group by snapshot_id with rollup")
	require.NoError(t, err, "UUID grouping-set DISTINCT must build without an ANY-to-UUID cast")
}
