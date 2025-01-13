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

	"github.com/stretchr/testify/require"

	"github.com/golang/mock/gomock"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"

	"github.com/stretchr/testify/assert"
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
		func(schemaName string, tableName string, snapshot *Snapshot) (*ObjectRef, *TableDef) {
			if schemaName == "" {
				schemaName = "db"
			}
			x := store[schemaName+"."+tableName]
			return x.obj, x.table
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
	testutil.NewProc()
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
	testutil.NewProc()
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
	testutil.NewProc()
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
