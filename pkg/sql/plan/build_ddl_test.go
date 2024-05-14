// Copyright 2021 - 2022 Matrix Origin
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

package plan

import (
	"context"
	"encoding/json"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/util/executor"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/assert"
)

func TestBuildAlterView(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	type arg struct {
		obj   *ObjectRef
		table *TableDef
	}

	sql1 := "alter view v as select a from a"
	sql2 := "alter view v as select a from v"
	sql3 := "alter view v as select a from vx"

	store := make(map[string]arg)

	vData, err := json.Marshal(ViewData{
		"create view v as select a from a",
		"db",
	})
	assert.NoError(t, err)

	store["db.v"] = arg{&plan.ObjectRef{},
		&plan.TableDef{
			TableType: catalog.SystemViewRel,
			ViewSql: &plan.ViewDef{
				View: string(vData),
			}},
	}

	vxData, err := json.Marshal(ViewData{
		"create view vx as select a from v",
		"db",
	})
	assert.NoError(t, err)
	store["db.vx"] = arg{&plan.ObjectRef{},
		&plan.TableDef{
			TableType: catalog.SystemViewRel,
			ViewSql: &plan.ViewDef{
				View: string(vxData),
			}},
	}

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

	store["db.verror"] = arg{&plan.ObjectRef{},
		&plan.TableDef{
			TableType: catalog.SystemViewRel},
	}

	ctx := NewMockCompilerContext2(ctrl)
	ctx.EXPECT().GetUserName().Return("sys:dump").AnyTimes()
	ctx.EXPECT().DefaultDatabase().Return("db").AnyTimes()
	ctx.EXPECT().Resolve(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(schemaName string, tableName string, snapshot Snapshot) (*ObjectRef, *TableDef) {
			if schemaName == "" {
				schemaName = "db"
			}
			x := store[schemaName+"."+tableName]
			return x.obj, x.table
		}).AnyTimes()
	ctx.EXPECT().SetBuildingAlterView(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	ctx.EXPECT().ResolveVariable(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()
	ctx.EXPECT().GetAccountId().Return(catalog.System_Account, nil).AnyTimes()
	ctx.EXPECT().GetContext().Return(context.Background()).AnyTimes()
	ctx.EXPECT().GetProcess().Return(nil).AnyTimes()
	ctx.EXPECT().Stats(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	ctx.EXPECT().GetQueryingSubscription().Return(nil).AnyTimes()
	ctx.EXPECT().DatabaseExists(gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	ctx.EXPECT().ResolveById(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	ctx.EXPECT().GetStatsCache().Return(nil).AnyTimes()
	ctx.EXPECT().GetSnapshot().Return(nil).AnyTimes()
	ctx.EXPECT().SetViews(gomock.Any()).AnyTimes()
	ctx.EXPECT().SetSnapshot(gomock.Any()).AnyTimes()

	ctx.EXPECT().GetRootSql().Return(sql1).AnyTimes()
	stmt1, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql1, 1, 0)
	assert.NoError(t, err)
	_, err = buildAlterView(stmt1.(*tree.AlterView), ctx)
	assert.NoError(t, err)

	//direct recursive refrence
	ctx.EXPECT().GetRootSql().Return(sql2).AnyTimes()
	ctx.EXPECT().GetBuildingAlterView().Return(true, "db", "v").AnyTimes()
	stmt2, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql2, 1, 0)
	assert.NoError(t, err)
	_, err = buildAlterView(stmt2.(*tree.AlterView), ctx)
	assert.Error(t, err)
	assert.EqualError(t, err, "internal error: there is a recursive reference to the view v")

	//indirect recursive refrence
	stmt3, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql3, 1, 0)
	ctx.EXPECT().GetBuildingAlterView().Return(true, "db", "vx").AnyTimes()
	assert.NoError(t, err)
	_, err = buildAlterView(stmt3.(*tree.AlterView), ctx)
	assert.Error(t, err)
	assert.EqualError(t, err, "internal error: there is a recursive reference to the view v")

	sql4 := "alter view noexists as select a from a"
	stmt4, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql4, 1, 0)
	assert.NoError(t, err)
	_, err = buildAlterView(stmt4.(*tree.AlterView), ctx)
	assert.Error(t, err)

	sql5 := "alter view verror as select a from a"
	stmt5, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql5, 1, 0)
	assert.NoError(t, err)
	_, err = buildAlterView(stmt5.(*tree.AlterView), ctx)
	assert.Error(t, err)
}

func TestBuildLockTables(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	type arg struct {
		obj   *ObjectRef
		table *TableDef
	}

	store := make(map[string]arg)

	sql1 := "lock tables t1 read"
	sql2 := "lock tables t1 read, t2 write"
	sql3 := "lock tables t1 read, t1 write"

	store["db.t1"] = arg{
		&plan.ObjectRef{},
		&plan.TableDef{
			TableType: catalog.SystemOrdinaryRel,
			Cols: []*ColDef{
				{
					Name: "a",
					Typ: plan.Type{
						Id:    int32(types.T_varchar),
						Width: types.MaxVarcharLen,
						Table: "t1",
					},
				},
			},
		}}

	ctx := NewMockCompilerContext2(ctrl)
	ctx.EXPECT().DefaultDatabase().Return("db").AnyTimes()
	ctx.EXPECT().Resolve(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(schemaName string, tableName string, snapshot Snapshot) (*ObjectRef, *TableDef) {
			if schemaName == "" {
				schemaName = "db"
			}
			x := store[schemaName+"."+tableName]
			return x.obj, x.table
		}).AnyTimes()
	ctx.EXPECT().ResolveVariable(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()
	ctx.EXPECT().GetAccountId().Return(catalog.System_Account, nil).AnyTimes()
	ctx.EXPECT().GetContext().Return(context.Background()).AnyTimes()
	ctx.EXPECT().GetProcess().Return(nil).AnyTimes()
	ctx.EXPECT().Stats(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	ctx.EXPECT().GetRootSql().Return(sql1).AnyTimes()
	stmt1, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql1, 1, 0)
	assert.NoError(t, err)
	_, err = buildLockTables(stmt1.(*tree.LockTableStmt), ctx)
	assert.NoError(t, err)

	ctx.EXPECT().GetRootSql().Return(sql2).AnyTimes()
	stmt2, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql2, 1, 0)
	assert.NoError(t, err)
	_, err = buildLockTables(stmt2.(*tree.LockTableStmt), ctx)
	assert.Error(t, err)

	store["db.t2"] = arg{
		&plan.ObjectRef{},
		&plan.TableDef{
			TableType: catalog.SystemOrdinaryRel,
			Cols: []*ColDef{
				{
					Name: "a",
					Typ: plan.Type{
						Id:    int32(types.T_varchar),
						Width: types.MaxVarcharLen,
						Table: "t2",
					},
				},
			},
		}}

	_, err = buildLockTables(stmt2.(*tree.LockTableStmt), ctx)
	assert.NoError(t, err)

	ctx.EXPECT().GetRootSql().Return(sql3).AnyTimes()
	stmt3, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql3, 1, 0)
	assert.NoError(t, err)
	_, err = buildLockTables(stmt3.(*tree.LockTableStmt), ctx)
	assert.Error(t, err)
}

func TestBuildCreateTable(t *testing.T) {
	mock := NewMockOptimizer(false)
	rt := moruntime.DefaultRuntime()
	moruntime.SetupProcessLevelRuntime(rt)
	moruntime.ProcessLevelRuntime().SetGlobalVariables(moruntime.InternalSQLExecutor, executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		return executor.Result{}, nil
	}))
	sqls := []string{
		`CREATE TABLE t3(
					col1 INT NOT NULL,
					col2 DATE NOT NULL UNIQUE KEY,
					col3 INT NOT NULL,
					col4 INT NOT NULL,
					PRIMARY KEY (col1),
					KEY(col3),
					KEY(col3) )`,
		`CREATE TABLE t2 (
						col1 INT NOT NULL,
						col2 DATE NOT NULL,
						col3 INT NOT NULL,
						col4 INT NOT NULL,
						UNIQUE KEY (col1),
						UNIQUE KEY (col3)
					);`,
		`CREATE TABLE t2 (
						col1 INT NOT NULL,
						col2 DATE NOT NULL,
						col3 INT NOT NULL,
						col4 INT NOT NULL,
						UNIQUE KEY (col1),
						UNIQUE KEY (col1, col3)
					);`,
		`CREATE TABLE t2 (
					col1 INT NOT NULL KEY,
					col2 DATE NOT NULL,
					col3 INT NOT NULL,
					col4 INT NOT NULL,
					UNIQUE KEY (col1),
					UNIQUE KEY (col1, col3)
				);`,

		`CREATE TABLE t2 (
					col1 INT NOT NULL,
					col2 DATE NOT NULL,
					col3 INT NOT NULL,
					col4 INT NOT NULL,
					KEY (col1)
				);`,

		`CREATE TABLE t2 (
					col1 INT NOT NULL KEY,
					col2 DATE NOT NULL,
					col3 INT NOT NULL,
					col4 INT NOT NULL
				);`,

		`CREATE TABLE t2 (
					col1 INT NOT NULL KEY,
					col2 DATE NOT NULL,
					col3 INT NOT NULL,
					col4 INT NOT NULL,
					KEY (col1)
				);`,

		`CREATE TABLE t2 (
					col1 INT NOT NULL,
					col2 DATE NOT NULL,
					col3 INT NOT NULL,
					col4 INT NOT NULL,
					KEY (col1)
				);`,

		`CREATE TABLE t2 (
					col1 INT NOT NULL KEY,
					col2 DATE NOT NULL,
					col3 INT NOT NULL,
					col4 INT NOT NULL,
					UNIQUE KEY (col1),
					UNIQUE KEY (col1, col3)
				);`,

		`CREATE TABLE t1 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			UNIQUE KEY (col1 DESC)
		);`,

		`CREATE TABLE t2 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			UNIQUE KEY (col1 ASC)
		);`,

		"CREATE TABLE t2 (" +
			"	`PRIMARY` INT NOT NULL, " +
			"	col2 DATE NOT NULL, " +
			"	col3 INT NOT NULL," +
			"	col4 INT NOT NULL," +
			"	UNIQUE KEY (`PRIMARY`)," +
			"	UNIQUE KEY (`PRIMARY`, col3)" +
			");",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestBuildCreateTableError(t *testing.T) {
	mock := NewMockOptimizer(false)
	sqlerrs := []string{
		`CREATE TABLE t1 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL unique key,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY (col1),
			unique key col2 (col3)
		);`,

		`CREATE TABLE t1 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY (col1),
			unique key idx_sp1 (col2),
			unique key idx_sp1 (col3)
		);`,

		`CREATE TABLE t1 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY (col1),
			unique key idx_sp1 (col2),
			key idx_sp1 (col3)
		);`,

		`CREATE TABLE t2 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL UNIQUE KEY,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			PRIMARY KEY (col1),
			KEY col2 (col3)
		);`,

		`CREATE TABLE t2 (
			col1 INT NOT NULL KEY,
			col2 DATE NOT NULL KEY,
			col3 INT NOT NULL,
			col4 INT NOT NULL
		);`,

		`CREATE TABLE t3 (
			col1 INT NOT NULL,
			col2 DATE NOT NULL,
			col3 INT NOT NULL,
			col4 INT NOT NULL,
			UNIQUE KEY uk1 ((col1 + col3))
		);`,
	}
	runTestShouldError(mock, t, sqlerrs)
}

func TestBuildAlterTable(t *testing.T) {
	mock := NewMockOptimizer(false)
	// should pass
	sqls := []string{
		"ALTER TABLE emp ADD UNIQUE idx1 (empno, ename);",
		"ALTER TABLE emp ADD UNIQUE INDEX idx1 (empno, ename);",
		"ALTER TABLE emp ADD INDEX idx1 (ename, sal);",
		"ALTER TABLE emp ADD INDEX idx2 (ename, sal DESC);",
		"ALTER TABLE emp ADD UNIQUE INDEX idx1 (empno ASC);",
		//"alter table emp drop foreign key fk1",
		//"alter table nation add FOREIGN KEY fk_t1(n_nationkey) REFERENCES nation2(n_nationkey)",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestBuildAlterTableError(t *testing.T) {
	mock := NewMockOptimizer(false)
	// should pass
	sqls := []string{
		"ALTER TABLE emp ADD UNIQUE idx1 ((empno+1) DESC, ename);",
		"ALTER TABLE emp ADD INDEX idx2 (ename, (sal*30) DESC);",
		"ALTER TABLE emp ADD UNIQUE INDEX idx1 ((empno+20), (sal*30));",
	}
	runTestShouldError(mock, t, sqls)
}

func TestCreateSingleTable(t *testing.T) {
	sql := "create cluster table a (a int);"
	mock := NewMockOptimizer(false)
	logicPlan, err := buildSingleStmt(mock, t, sql)
	if err != nil {
		t.Fatalf("%+v", err)
	}
	outPutPlan(logicPlan, true, t)
}

func TestCreateTableAsSelect(t *testing.T) {
	mock := NewMockOptimizer(false)
	sqls := []string{"CREATE TABLE t1 (a int, b char(5)); CREATE TABLE t2 (c float) as select b, a from t1"}
	runTestShouldPass(mock, t, sqls, false, false)
}
