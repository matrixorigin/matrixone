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
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
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
		func(schemaName string, tableName string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
			if schemaName == "" {
				schemaName = "db"
			}
			x := store[schemaName+"."+tableName]
			return x.obj, x.table, nil
		}).AnyTimes()
	ctx.EXPECT().SetBuildingAlterView(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
	ctx.EXPECT().ResolveVariable(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()
	ctx.EXPECT().GetAccountId().Return(catalog.System_Account, nil).AnyTimes()
	ctx.EXPECT().GetContext().Return(context.Background()).AnyTimes()
	ctx.EXPECT().GetProcess().Return(nil).AnyTimes()
	ctx.EXPECT().Stats(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	ctx.EXPECT().GetQueryingSubscription().Return(nil).AnyTimes()
	ctx.EXPECT().DatabaseExists(gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	ctx.EXPECT().ResolveById(gomock.Any(), gomock.Any()).Return(nil, nil, nil).AnyTimes()
	ctx.EXPECT().GetStatsCache().Return(nil).AnyTimes()
	ctx.EXPECT().GetSnapshot().Return(nil).AnyTimes()
	ctx.EXPECT().SetViews(gomock.Any()).AnyTimes()
	ctx.EXPECT().SetSnapshot(gomock.Any()).AnyTimes()
	ctx.EXPECT().GetLowerCaseTableNames().Return(int64(1)).AnyTimes()
	ctx.EXPECT().GetSubscriptionMeta(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	ctx.EXPECT().GetRootSql().Return(sql1).AnyTimes()
	stmt1, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql1, 1)
	assert.NoError(t, err)
	_, err = buildAlterView(stmt1.(*tree.AlterView), ctx)
	assert.NoError(t, err)

	//direct recursive refrence
	ctx.EXPECT().GetRootSql().Return(sql2).AnyTimes()
	ctx.EXPECT().GetBuildingAlterView().Return(true, "db", "v").AnyTimes()
	stmt2, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql2, 1)
	assert.NoError(t, err)
	_, err = buildAlterView(stmt2.(*tree.AlterView), ctx)
	assert.Error(t, err)
	assert.EqualError(t, err, "internal error: there is a recursive reference to the view v")

	//indirect recursive refrence
	stmt3, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql3, 1)
	ctx.EXPECT().GetBuildingAlterView().Return(true, "db", "vx").AnyTimes()
	assert.NoError(t, err)
	_, err = buildAlterView(stmt3.(*tree.AlterView), ctx)
	assert.Error(t, err)
	assert.EqualError(t, err, "internal error: there is a recursive reference to the view v")

	sql4 := "alter view noexists as select a from a"
	stmt4, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql4, 1)
	assert.NoError(t, err)
	_, err = buildAlterView(stmt4.(*tree.AlterView), ctx)
	assert.Error(t, err)

	sql5 := "alter view verror as select a from a"
	stmt5, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql5, 1)
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
		func(schemaName string, tableName string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
			if schemaName == "" {
				schemaName = "db"
			}
			x := store[schemaName+"."+tableName]
			return x.obj, x.table, nil
		}).AnyTimes()
	ctx.EXPECT().ResolveVariable(gomock.Any(), gomock.Any(), gomock.Any()).Return("", nil).AnyTimes()
	ctx.EXPECT().GetAccountId().Return(catalog.System_Account, nil).AnyTimes()
	ctx.EXPECT().GetContext().Return(context.Background()).AnyTimes()
	ctx.EXPECT().GetProcess().Return(nil).AnyTimes()
	ctx.EXPECT().Stats(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	ctx.EXPECT().GetRootSql().Return(sql1).AnyTimes()
	stmt1, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql1, 1)
	assert.NoError(t, err)
	_, err = buildLockTables(stmt1.(*tree.LockTableStmt), ctx)
	assert.NoError(t, err)

	ctx.EXPECT().GetRootSql().Return(sql2).AnyTimes()
	stmt2, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql2, 1)
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
	stmt3, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql3, 1)
	assert.NoError(t, err)
	_, err = buildLockTables(stmt3.(*tree.LockTableStmt), ctx)
	assert.Error(t, err)
}

func TestBuildCreateTable(t *testing.T) {
	mock := NewMockOptimizer(false)
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime("", rt)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, executor.NewMemExecutor(func(sql string) (executor.Result, error) {
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

func TestParseDuration(t *testing.T) {

	cases := []struct {
		period      uint64
		unit        string
		expected    time.Duration
		expectedErr error
	}{
		// nil input
		{expectedErr: moerr.NewInvalidArg(context.Background(), "time unit", "")},
		// 0 second
		{0, "second", 0, nil},
		// 1 second
		{1, "second", time.Second, nil},
		// 2 minute
		{2, "minute", 2 * time.Minute, nil},
		// 3 hour
		{3, "hour", 3 * time.Hour, nil},
		// 4 day
		{4, "day", 4 * 24 * time.Hour, nil},
		// 5 week
		{5, "week", 5 * 7 * 24 * time.Hour, nil},
		// 6 month
		{6, "month", 6 * 30 * 24 * time.Hour, nil},
		// invalid time unit: year
		{7, "year", 0, moerr.NewInvalidArg(context.Background(), "time unit", "year")},
	}

	for _, c := range cases {
		duration, err := parseDuration(context.Background(), c.period, c.unit)
		assert.Equal(t, c.expected, duration)
		assert.Equal(t, err, c.expectedErr)
	}
}

func Test_buildTableDefs(t *testing.T) {
	stmt := &tree.CreateTable{
		Temporary:          false,
		IsClusterTable:     false,
		IfNotExists:        false,
		Table:              tree.TableName{},
		Defs:               nil,
		Options:            nil,
		PartitionOption:    nil,
		ClusterByOption:    nil,
		Param:              nil,
		AsSource:           &tree.Select{Select: &tree.SelectClause{From: &tree.From{}}},
		IsDynamicTable:     false,
		DTOptions:          nil,
		IsAsSelect:         true,
		IsAsLike:           false,
		LikeTableName:      tree.TableName{},
		SubscriptionOption: nil,
	}

	ctx := &MockCompilerContext{}

	createTable := &plan.CreateTable{
		Database: "db",
		TableDef: &plan.TableDef{
			Name: "table",
		},
	}

	err := buildTableDefs(stmt, ctx, createTable, nil)
	assert.Error(t, err)
}

func TestBuildCreatePitr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Helper to create a base stmt
	baseStmt := func() *tree.CreatePitr {
		return &tree.CreatePitr{
			IfNotExists: true,
			Name:        "pitr1",
			Level:       tree.PITRLEVELCLUSTER,
			PitrValue:   1,
			PitrUnit:    "h",
		}
	}

	t.Run("sys account can create cluster level pitr", func(t *testing.T) {
		ctx := &MockCompilerContext{}
		ctx.GetAccountNameFunc = func() string { return "sys" }
		ctx.GetAccountIdFunc = func() (uint32, error) { return 1, nil }
		stmt := baseStmt()
		plan, err := buildCreatePitr(stmt, ctx)
		assert.NoError(t, err)
		assert.NotNil(t, plan)
		require.Equal(t, ctx.GetAccountName(), "sys")
	})

	t.Run("non-sys account cannot create cluster level pitr", func(t *testing.T) {
		ctx := &MockCompilerContext{}
		ctx.GetAccountNameFunc = func() string { return "user1" }
		ctx.GetAccountIdFunc = func() (uint32, error) { return 2, nil }
		stmt := baseStmt()
		_, err := buildCreatePitr(stmt, ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "only sys tenant can create cluster level pitr")
	})

	t.Run("sys account can create account level pitr for self", func(t *testing.T) {
		ctx := &MockCompilerContext{}
		ctx.GetAccountNameFunc = func() string { return "sys" }
		ctx.GetAccountIdFunc = func() (uint32, error) { return 1, nil }
		ctx.ResolveAccountIdsFunc = func(_ []string) ([]uint32, error) { return []uint32{1}, nil }
		stmt := baseStmt()
		stmt.Level = tree.PITRLEVELACCOUNT
		stmt.AccountName = "sys"
		plan, err := buildCreatePitr(stmt, ctx)
		assert.NoError(t, err)
		assert.NotNil(t, plan)
	})

	t.Run("non-sys account cannot create account level pitr for other", func(t *testing.T) {
		ctx := &MockCompilerContext{}
		ctx.GetAccountNameFunc = func() string { return "user1" }
		ctx.GetAccountIdFunc = func() (uint32, error) { return 2, nil }
		stmt := baseStmt()
		stmt.Level = tree.PITRLEVELACCOUNT
		stmt.AccountName = "other"
		_, err := buildCreatePitr(stmt, ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "only sys tenant can create tenant level pitr for other tenant")
	})

	t.Run("invalid pitr value", func(t *testing.T) {
		ctx := &MockCompilerContext{}
		ctx.GetAccountNameFunc = func() string { return "sys" }
		ctx.GetAccountIdFunc = func() (uint32, error) { return 1, nil }
		stmt := baseStmt()
		stmt.PitrValue = 0
		_, err := buildCreatePitr(stmt, ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid pitr value")
	})

	t.Run("invalid pitr unit", func(t *testing.T) {
		ctx := &MockCompilerContext{}
		ctx.GetAccountNameFunc = func() string { return "sys" }
		ctx.GetAccountIdFunc = func() (uint32, error) { return 1, nil }
		stmt := baseStmt()
		stmt.PitrUnit = "invalid"
		_, err := buildCreatePitr(stmt, ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid pitr unit")
	})

	t.Run("reserved pitr name", func(t *testing.T) {
		ctx := &MockCompilerContext{}
		ctx.GetAccountNameFunc = func() string { return "sys" }
		ctx.GetAccountIdFunc = func() (uint32, error) { return 1, nil }
		stmt := baseStmt()
		stmt.Name = "sys_mo_catalog_pitr"
		_, err := buildCreatePitr(stmt, ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "pitr name is reserved")
	})

	t.Run("database level pitr, database not exist", func(t *testing.T) {
		ctx := &MockCompilerContext{}
		ctx.GetAccountNameFunc = func() string { return "sys" }
		ctx.GetAccountIdFunc = func() (uint32, error) { return 1, nil }
		ctx.DatabaseExistsFunc = func(string, *Snapshot) bool { return false }
		stmt := baseStmt()
		stmt.Level = tree.PITRLEVELDATABASE
		stmt.DatabaseName = "db1"
		_, err := buildCreatePitr(stmt, ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "database db1 does not exist")
	})

	t.Run("database level pitr, database exists", func(t *testing.T) {
		ctx := &MockCompilerContext{}
		ctx.GetAccountNameFunc = func() string { return "sys" }
		ctx.GetAccountIdFunc = func() (uint32, error) { return 1, nil }
		ctx.DatabaseExistsFunc = func(string, *Snapshot) bool { return true }
		ctx.GetDatabaseIdFunc = func(string, *Snapshot) (uint64, error) { return 123, nil }
		stmt := baseStmt()
		stmt.Level = tree.PITRLEVELDATABASE
		stmt.DatabaseName = "db1"
		plan, err := buildCreatePitr(stmt, ctx)
		assert.NoError(t, err)
		assert.NotNil(t, plan)
	})

	t.Run("table level pitr, table not exist", func(t *testing.T) {
		ctx := &MockCompilerContext{}
		ctx.GetAccountNameFunc = func() string { return "sys" }
		ctx.GetAccountIdFunc = func() (uint32, error) { return 1, nil }
		ctx.DatabaseExistsFunc = func(string, *Snapshot) bool { return true }
		ctx.ResolveFunc = func(string, string, *Snapshot) (*ObjectRef, *TableDef) { return nil, nil }
		stmt := baseStmt()
		stmt.Level = tree.PITRLEVELTABLE
		stmt.DatabaseName = "db1"
		stmt.TableName = "tb1"
		_, err := buildCreatePitr(stmt, ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "table db1.tb1 does not exist")
	})

	t.Run("table level pitr, table exists", func(t *testing.T) {
		ctx := &MockCompilerContext{}
		ctx.GetAccountNameFunc = func() string { return "sys" }
		ctx.GetAccountIdFunc = func() (uint32, error) { return 1, nil }
		ctx.DatabaseExistsFunc = func(string, *Snapshot) bool { return true }
		ctx.ResolveFunc = func(string, string, *Snapshot) (*ObjectRef, *TableDef) { return &ObjectRef{}, &TableDef{TblId: 456} }
		stmt := baseStmt()
		stmt.Level = tree.PITRLEVELTABLE
		stmt.DatabaseName = "db1"
		stmt.TableName = "tb1"
		plan, err := buildCreatePitr(stmt, ctx)
		assert.NoError(t, err)
		assert.NotNil(t, plan)
	})
}
