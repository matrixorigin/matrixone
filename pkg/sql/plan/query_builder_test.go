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
		require.Contains(t, err.Error(), "mode must be 'pre', 'post', or 'force'")
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
