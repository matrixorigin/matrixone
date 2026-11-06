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
	"time"

	"github.com/stretchr/testify/require"

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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
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
		Stmt:            "create view v as select a from a",
		DefaultDatabase: "db",
		SecurityType:    "DEFINER",
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
		Stmt:            "create view vx as select a from v",
		DefaultDatabase: "db",
		SecurityType:    "DEFINER",
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
	require.Equal(t, ctx.GetAccountName(), "")

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

func TestBuildCreateIndexOnExternalTableError(t *testing.T) {
	mock := NewEmptyMockOptimizer()
	ctx := mock.CurrentContext().(*MockCompilerContext)
	ctx.objects["ext_idx"] = &plan.ObjectRef{
		SchemaName: "tpch",
		ObjName:    "ext_idx",
	}
	ctx.tables["ext_idx"] = &plan.TableDef{
		Name:      "ext_idx",
		TableType: catalog.SystemExternalRel,
		Cols: []*plan.ColDef{
			{Name: "col_int32", Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "col_varchar", Typ: plan.Type{Id: int32(types.T_varchar), Width: 100}},
			{Name: "part_id", Typ: plan.Type{Id: int32(types.T_int32)}},
		},
	}

	sqls := []string{
		"CREATE INDEX idx_ext ON ext_idx(col_int32);",
		"CREATE UNIQUE INDEX uidx_ext ON ext_idx(col_int32);",
		"CREATE FULLTEXT INDEX fidx_ext ON ext_idx(col_varchar);",
		"ALTER TABLE ext_idx ADD INDEX idx_ext2 (col_int32);",
		"ALTER TABLE ext_idx ADD UNIQUE (col_varchar);",
		"ALTER TABLE ext_idx ADD FULLTEXT INDEX fidx_ext2 (col_varchar);",
	}
	for _, sql := range sqls {
		_, err := runOneStmt(mock, t, sql)
		require.Error(t, err, sql)
		require.Contains(t, err.Error(), "cannot create index on external table", sql)
	}
}

func TestBuildCreateExternalTableInlineIndexError(t *testing.T) {
	mock := NewMockOptimizer(false)
	sqls := []string{
		"CREATE EXTERNAL TABLE ext_inline_col_key (id INT KEY) INFILE {'filepath'='data.txt', 'format'='csv'};",
		"CREATE EXTERNAL TABLE ext_inline_col_unique (id INT UNIQUE) INFILE {'filepath'='data.txt', 'format'='csv'};",
		"CREATE EXTERNAL TABLE ext_inline_col_pk (id INT PRIMARY KEY) INFILE {'filepath'='data.txt', 'format'='csv'};",
		"CREATE EXTERNAL TABLE ext_inline_table_key (id INT, KEY (id)) INFILE {'filepath'='data.txt', 'format'='csv'};",
		"CREATE EXTERNAL TABLE ext_inline_table_unique (id INT, UNIQUE KEY uk_id (id)) INFILE {'filepath'='data.txt', 'format'='csv'};",
		"CREATE EXTERNAL TABLE ext_inline_table_fulltext (doc VARCHAR(100), FULLTEXT ft_doc (doc)) INFILE {'filepath'='data.txt', 'format'='csv'};",
	}
	for _, sql := range sqls {
		_, err := runOneStmt(mock, t, sql)
		require.Error(t, err, sql)
		require.Contains(t, err.Error(), "cannot create index on external table", sql)
	}
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

func TestBuildIndexAllowsEnumAndTextBlobPrefix(t *testing.T) {
	mock := NewMockOptimizer(false)
	sqls := []string{
		"CREATE TABLE enum_idx_ok1 (id VARCHAR(191) PRIMARY KEY, role ENUM('a','b','c'), INDEX idx_role(role));",
		"CREATE TABLE enum_idx_ok2 (id VARCHAR(191) PRIMARY KEY, role ENUM('a','b','c'), UNIQUE INDEX uq_role(role));",
		"CREATE TABLE enum_idx_ok3 (id VARCHAR(191) PRIMARY KEY, name VARCHAR(191), role ENUM('a','b','c'), INDEX idx_name_role(name, role));",
		"CREATE TABLE text_prefix_ok1 (id INT PRIMARY KEY, t TEXT, INDEX idx_t(t(100)));",
		"CREATE TABLE text_prefix_ok2 (id INT PRIMARY KEY, t TEXT, UNIQUE INDEX uq_t(t(100)));",
		"CREATE TABLE blob_prefix_ok1 (id INT PRIMARY KEY, b BLOB, INDEX idx_b(b(100)));",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestBuildIndexRejectsTextBlobPlainIndex(t *testing.T) {
	mock := NewMockOptimizer(false)
	sqlerrs := []string{
		"CREATE TABLE text_plain_err1 (id INT PRIMARY KEY, t TEXT, INDEX idx_t(t));",
		"CREATE TABLE text_plain_err2 (id INT PRIMARY KEY, t TEXT, UNIQUE INDEX uq_t(t));",
		"CREATE TABLE text_comp_pk_err (id INT, t TEXT, PRIMARY KEY(id, t));",
		"CREATE TABLE blob_plain_err1 (id INT PRIMARY KEY, b BLOB, INDEX idx_b(b));",
		"CREATE TABLE blob_comp_pk_err (b BLOB, id INT, PRIMARY KEY(b, id));",
	}
	runTestShouldError(mock, t, sqlerrs)
}

func TestBuildRegularSecondaryIndexPersistsPrefixLengths(t *testing.T) {
	mock := NewMockOptimizer(false)
	tests := []struct {
		name   string
		sql    string
		column string
		length int
	}{
		{
			name:   "text",
			sql:    "CREATE TABLE text_prefix_secondary_ok (id INT PRIMARY KEY, t TEXT, INDEX idx_t(t(100)));",
			column: "t",
			length: 100,
		},
		{
			name:   "blob",
			sql:    "CREATE TABLE blob_prefix_secondary_ok (id INT PRIMARY KEY, b BLOB, INDEX idx_b(b(100)));",
			column: "b",
			length: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(mock, t, tt.sql)
			require.NoError(t, err)

			createTable := logicPlan.GetDdl().GetCreateTable()
			require.NotNil(t, createTable)
			require.Len(t, createTable.GetTableDef().GetIndexes(), 1)

			indexDef := createTable.GetTableDef().GetIndexes()[0]
			prefixLengths := catalog.IndexPrefixLengthsFromParams(indexDef.IndexAlgoParams)
			require.Equal(t, tt.length, prefixLengths[tt.column])
		})
	}
}

func TestBuildVectorIndexAllowsIvfFlatOnly(t *testing.T) {
	mock := NewMockOptimizer(false)
	sqls := []string{
		"CREATE TABLE vec_idx_ok1 (id INT PRIMARY KEY, embedding VECF32(3), KEY idx_emb USING ivfflat (embedding) lists = 2 op_type 'vector_l2_ops');",
		"CREATE TABLE vec_idx_ok2 (id INT PRIMARY KEY, embedding VECF64(3), KEY idx_emb USING ivfflat (embedding) lists = 2 op_type 'vector_l2_ops');",
	}
	runTestShouldPass(mock, t, sqls, false, false)

	sqlerrs := []string{
		"CREATE TABLE vec_idx_err1 (id INT PRIMARY KEY, embedding VECF32(3), KEY idx_emb (embedding));",
		"CREATE TABLE vec_idx_err2 (id INT PRIMARY KEY, embedding VECF64(3), KEY idx_emb (embedding));",
	}
	runTestShouldError(mock, t, sqlerrs)
}

func TestBuildIndexAllowsRTreeGeometry(t *testing.T) {
	mock := NewMockOptimizer(false)
	sqls := []string{
		"CREATE TABLE geo_spatial_ok (id INT PRIMARY KEY, g POINT NOT NULL, KEY idx_g USING RTREE (g));",
		"CREATE TABLE geo_spatial_nullable_ok (id INT PRIMARY KEY, g POINT, KEY idx_g USING RTREE (g));",
	}
	runTestShouldPass(mock, t, sqls, false, false)
}

func TestGeometryDDLGuardsSQLPaths(t *testing.T) {
	mock := NewMockOptimizer(false)
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime("", rt)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		return executor.Result{}, nil
	}))

	sqlerrs := []string{
		"CREATE TABLE geo_default_err (g GEOMETRY DEFAULT 'POINT(1 1)');",
		"CREATE TABLE geo_pk_err (g GEOMETRY PRIMARY KEY);",
		"CREATE TABLE geo_uk_err (g GEOMETRY UNIQUE KEY);",
		"CREATE TABLE geo_idx_err (g GEOMETRY, KEY(g));",
		"ALTER TABLE emp ADD COLUMN g GEOMETRY UNIQUE KEY;",
		"ALTER TABLE emp ADD COLUMN g GEOMETRY PRIMARY KEY;",
	}
	runTestShouldError(mock, t, sqlerrs)
}

func TestGeometryColumnValidationSQLPaths(t *testing.T) {
	mock := NewMockOptimizer(false)
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime("", rt)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		return executor.Result{}, nil
	}))

	sqls := []string{
		"CREATE TABLE geo_point_ok (g POINT);",
		"CREATE TABLE geo_any_ok (g GEOMETRY);",
	}
	runTestShouldPass(mock, t, sqls, false, false)
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

func TestCastPreservesNonNull(t *testing.T) {
	numeric := []types.T{types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64, types.T_decimal64, types.T_decimal128, types.T_decimal256}
	targets := append(append([]types.T{}, numeric...), types.T_char, types.T_varchar, types.T_text)
	for _, from := range numeric {
		for _, to := range targets {
			expr := &plan.Expr{Typ: plan.Type{Id: int32(to)}, Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.CAST, 0), ObjName: "cast"},
				Args: []*plan.Expr{{Typ: plan.Type{Id: int32(from)}}, {Typ: plan.Type{Id: int32(to)}}},
			}}}
			require.True(t, castPreservesNonNull(expr), "%v to %v", from, to)
			require.False(t, isNonNullLiteralFallback(expr), "nonliteral numeric casts cannot prove a constant fallback")
		}
	}
	for _, test := range []struct {
		from types.T
		to   types.T
		want bool
	}{
		{types.T_decimal64, types.T_float64, true},
		{types.T_decimal128, types.T_float64, true},
		{types.T_decimal256, types.T_float64, true},
		{types.T_int64, types.T_decimal64, true},
		{types.T_uint64, types.T_decimal128, true},
		{types.T_int64, types.T_decimal256, true},
		{types.T_varchar, types.T_decimal64, true},
		{types.T_char, types.T_decimal128, true},
		{types.T_text, types.T_decimal256, true},
		{types.T_char, types.T_varchar, true},
		{types.T_varchar, types.T_varchar, true},
		{types.T_text, types.T_varchar, true},
		{types.T_varchar, types.T_text, true},
		{types.T_varchar, types.T_float64, false},
		{types.T_varchar, types.T_date, false},
		{types.T_json, types.T_varchar, false},
	} {
		expr := &plan.Expr{Typ: plan.Type{Id: int32(test.to)}, Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.CAST, 0), ObjName: "cast"},
			Args: []*plan.Expr{{Typ: plan.Type{Id: int32(test.from)}}, {Typ: plan.Type{Id: int32(test.to)}}},
		}}}
		require.Equal(t, test.want, castPreservesNonNull(expr), "%v to %v", test.from, test.to)
		require.False(t, isNonNullLiteralFallback(expr), "conversion proof alone must not turn a nonliteral into a constant proof")
	}
}

func TestNonNullLiteralFallback(t *testing.T) {
	literal := func(typ types.T, null bool) *plan.Expr {
		lit := &plan.Literal{Isnull: null, Value: &plan.Literal_I64Val{I64Val: 0}}
		if typ == types.T_int32 {
			lit.Value = &plan.Literal_I32Val{I32Val: 0}
		} else if typ == types.T_uint64 {
			lit.Value = &plan.Literal_U64Val{U64Val: 0}
		} else if typ == types.T_float64 {
			lit.Value = &plan.Literal_Dval{Dval: 0}
		} else if typ == types.T_float32 {
			lit.Value = &plan.Literal_Fval{Fval: 0}
		} else if typ == types.T_varchar {
			lit.Value = &plan.Literal_Sval{Sval: "0"}
		}
		return &plan.Expr{Typ: plan.Type{Id: int32(typ)}, Expr: &plan.Expr_Lit{Lit: lit}}
	}
	cast := func(input *plan.Expr, typ types.T) *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(typ)}, Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.CAST, 0), ObjName: "cast"},
			Args: []*plan.Expr{input, {Typ: plan.Type{Id: int32(typ)}}},
		}}}
	}
	column := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}, Expr: &plan.Expr_Col{Col: &plan.ColRef{}}}
	fixedDouble := cast(literal(types.T_int64, false), types.T_float64)
	fixedDouble.Typ.Width = 10
	fixedDouble.GetF().Args[1].Typ.Width = 10
	otherOverload := cast(literal(types.T_int64, false), types.T_float64)
	otherOverload.GetF().Func.Obj = function.EncodeOverloadID(function.CAST, 1)
	missingTarget := cast(literal(types.T_int64, false), types.T_float64)
	missingTarget.GetF().Args = missingTarget.GetF().Args[:1]
	for _, test := range []struct {
		name string
		expr *plan.Expr
		want bool
	}{
		{"literal", literal(types.T_int64, false), true},
		{"null literal", literal(types.T_int64, true), false},
		{"integer double", cast(literal(types.T_int64, false), types.T_float64), true},
		{"unsigned double", cast(literal(types.T_uint64, false), types.T_float64), true},
		{"same double", cast(literal(types.T_float64, false), types.T_float64), true},
		{"float widening", cast(literal(types.T_float32, false), types.T_float64), true},
		{"integer widening", cast(literal(types.T_int32, false), types.T_int64), true},
		{"nested widening", cast(cast(literal(types.T_int32, false), types.T_int64), types.T_float64), true},
		{"integer narrowing is nonnull or error", cast(literal(types.T_int64, false), types.T_int32), true},
		{"sign change is nonnull or error", cast(literal(types.T_int64, false), types.T_uint64), true},
		{"string conversion", cast(literal(types.T_varchar, false), types.T_float64), false},
		{"null conversion", cast(literal(types.T_int64, true), types.T_float64), false},
		{"column annotation", column, false},
		{"converted column", cast(column, types.T_float64), false},
		{"fixed double is nonnull or error", fixedDouble, true},
		{"other cast overload", otherOverload, false},
		{"missing cast target", missingTarget, false},
	} {
		t.Run(test.name, func(t *testing.T) { require.Equal(t, test.want, isNonNullLiteralFallback(test.expr)) })
	}
}

func TestRefreshConditionalNullabilityNeverStrengthens(t *testing.T) {
	for _, fid := range []int32{function.CASE, function.IFF} {
		for _, initiallyNonNull := range []bool{false, true} {
			for _, resultNonNull := range []bool{false, true} {
				args := []*plan.Expr{
					{Typ: plan.Type{NotNullable: false}},
					{Typ: plan.Type{NotNullable: resultNonNull}},
					{Typ: plan.Type{NotNullable: true}},
				}
				expr := &plan.Expr{Typ: plan.Type{NotNullable: initiallyNonNull}, Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(fid, 0)}, Args: args,
				}}}
				refreshExprNullabilityFromInput(expr, nil)
				require.Equal(t, initiallyNonNull && resultNonNull, expr.Typ.NotNullable)
			}
		}
		for _, args := range [][]*plan.Expr{nil, {{Typ: plan.Type{NotNullable: true}}, {Typ: plan.Type{NotNullable: true}}}} {
			expr := &plan.Expr{Typ: plan.Type{NotNullable: true}, Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(fid, 0)}, Args: args,
			}}}
			refreshExprNullabilityFromInput(expr, nil)
			require.False(t, expr.Typ.NotNullable, "missing ELSE or invalid shape cannot preserve a proof")
		}
	}
}

func TestCreateTableAsSelectOutputNullability(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		nullable []bool
	}{
		{"count control", "select count(*) as c from nation", []bool{false}},
		{"union null right", "select count(*) as c from nation union all select null", []bool{true}},
		{"union null left", "select null as c union all select count(*) from nation", []bool{true}},
		{"union nonnull control", "select count(*) as c from nation union all select 1", []bool{false}},
		{"union distinct", "select count(*) as c from nation union select null", []bool{true}},
		{"union three branches", "select count(*) as c from nation union all select null union all select 1", []bool{true}},
		{"union derived", "select c from (select count(*) as c from nation union all select null) u", []bool{true}},
		{"union cte", "with u as (select count(*) as c from nation union all select null) select c from u", []bool{true}},
		{"left join", "select n.n_nationkey as l, r.r_regionkey as r from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false, true}},
		{"right join", "select n.n_nationkey as l, r.r_regionkey as r from nation n right join region r on n.n_regionkey=r.r_regionkey", []bool{true, false}},
		{"full join", "select n.n_nationkey as l, r.r_regionkey as r from nation n full join region r on n.n_regionkey=r.r_regionkey", []bool{true, true}},
		{"inner join control", "select n.n_nationkey as l, r.r_regionkey as r from nation n join region r on n.n_regionkey=r.r_regionkey", []bool{false, false}},
		{"null extending expression", "select r.r_regionkey+1 as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{true}},
		{"coalesce control", "select coalesce(r.r_regionkey,0) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"scalar subquery", "select (select r.r_regionkey from region r where r.r_regionkey=n.n_regionkey) as v from nation n", []bool{true}},
		{"grouped null extension", "select r.r_regionkey as k, count(*) as c from nation n left join region r on n.n_regionkey=r.r_regionkey group by r.r_regionkey", []bool{true, false}},
		{"distinct null extension", "select distinct r.r_regionkey as k from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{true}},
		{"coalesce mixed dynamic fallback", "select coalesce(r.r_name,cast(1/r.r_regionkey as char)) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{true}},
		{"ifnull mixed dynamic fallback", "select ifnull(r.r_name,cast(1/r.r_regionkey as char)) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{true}},
		{"coalesce mixed null fallback", "select coalesce(r.r_name,cast(null as char)) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{true}},
		{"ifnull mixed null fallback", "select ifnull(r.r_name,cast(null as char)) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{true}},
		{"coalesce mixed string source", "select coalesce(r.r_name,0) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"ifnull mixed string source", "select ifnull(r.r_name,0) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"coalesce mixed numeric source", "select coalesce(r.r_regionkey,'ok') as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"ifnull mixed numeric source", "select ifnull(r.r_regionkey,'ok') as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"ifnull float fallback", "select ifnull(1/r.r_regionkey,cast(0 as float)) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"coalesce float fallback", "select coalesce(1/r.r_regionkey,cast(0 as float)) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"ifnull unsigned fallback", "select ifnull(1/r.r_regionkey,cast(0 as unsigned)) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"coalesce unsigned fallback", "select coalesce(1/r.r_regionkey,cast(0 as unsigned)) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"ifnull decimal fallback", "select ifnull(1/r.r_regionkey,0.0) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"coalesce decimal fallback", "select coalesce(1/r.r_regionkey,0.0) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"ifnull decimal128 fallback", "select ifnull(1/r.r_regionkey,cast(0 as decimal(38,1))) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"coalesce decimal128 fallback", "select coalesce(1/r.r_regionkey,cast(0 as decimal(38,1))) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"ifnull converted integer source", "select ifnull(r.r_regionkey,0e0) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"ifnull explicitly converted source", "select ifnull(cast(r.r_name as varchar(20)),'ok') as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"ifnull converted string source", "select ifnull(r.r_name,'ok') as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"ifnull converted integer fallback", "select ifnull(1/r.r_regionkey,0) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"coalesce converted integer fallback", "select coalesce(1/r.r_regionkey,0) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"ifnull explicit double fallback", "select ifnull(1/r.r_regionkey,cast(0 as double)) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"coalesce explicit double fallback", "select coalesce(1/r.r_regionkey,cast(0 as double)) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"ifnull direct double fallback", "select ifnull(1/r.r_regionkey,0e0) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"coalesce direct double fallback", "select coalesce(1/r.r_regionkey,0e0) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"case untrusted division branch", "select case when n_comment='x' then 1/n_nationkey else 7 end as v from nation", []bool{true}},
		{"if untrusted division branch", "select if(n_comment='x',1/n_nationkey,7) as v from nation", []bool{true}},
		{"case untrusted regex branch", "select case when n_comment='x' then regexp_substr(n_name,'z') else '7' end as v from nation", []bool{true}},
		{"if untrusted regex branch", "select if(n_comment='x',regexp_substr(n_name,'z'),'7') as v from nation", []bool{true}},
		{"case nullable condition nonnull branches", "select case when r.r_regionkey=1 then 11 else 22 end as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"if nullable condition nonnull branches", "select if(r.r_regionkey=1,11,22) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"case multiple conditions", "select case when r.r_regionkey=1 then 11 when r.r_regionkey=2 then 22 else 33 end as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"case implicit null else", "select case when r.r_regionkey=1 then 11 end as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{true}},
		{"case nullable then", "select case when n.n_nationkey=1 then r.r_regionkey else 22 end as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{true}},
		{"if nullable else", "select if(n.n_nationkey=1,11,r.r_regionkey) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{true}},
		{"case explicit null else", "select case when r.r_regionkey=1 then 11 else null end as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{true}},
		{"nested case if", "select case when r.r_regionkey=1 then if(r.r_regionkey=2,11,22) else 33 end as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"ifnull control", "select ifnull(r.r_regionkey,0) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{false}},
		{"random bytes signed zero group", "select random_bytes(cast(0 as signed)) as v,count(*) as c from nation group by random_bytes(cast(0 as signed))", []bool{true, false}},
		{"random bytes unsigned zero group", "select random_bytes(cast(0 as unsigned)) as v,count(*) as c from nation group by random_bytes(cast(0 as unsigned))", []bool{true, false}},
		{"random bytes signed large group", "select random_bytes(cast(1025 as signed)) as v,count(*) as c from nation group by random_bytes(cast(1025 as signed))", []bool{true, false}},
		{"random bytes unsigned large group", "select random_bytes(cast(1025 as unsigned)) as v,count(*) as c from nation group by random_bytes(cast(1025 as unsigned))", []bool{true, false}},
		{"random bytes signed zero distinct", "select distinct random_bytes(cast(0 as signed)) as v from nation", []bool{true}},
		{"random bytes unsigned zero distinct", "select distinct random_bytes(cast(0 as unsigned)) as v from nation", []bool{true}},
		{"random bytes signed large distinct", "select distinct random_bytes(cast(1025 as signed)) as v from nation", []bool{true}},
		{"random bytes unsigned large distinct", "select distinct random_bytes(cast(1025 as unsigned)) as v from nation", []bool{true}},
		{"ifnull random bytes signed", "select ifnull(case when ascii(random_bytes(cast(n_nationkey as signed))) < 128 then 1 else null end,0) as v from nation", []bool{true}},
		{"ifnull random bytes unsigned", "select ifnull(case when ascii(random_bytes(cast(n_nationkey as unsigned))) < 128 then 1 else null end,0) as v from nation", []bool{true}},
		{"ifnull random bytes constant", "select ifnull(case when ascii(random_bytes(1)) < 128 then 1 else null end,0) as v from nation", []bool{true}},
		{"coalesce random bytes control", "select coalesce(case when ascii(random_bytes(cast(n_nationkey as signed))) < 128 then 1 else null end,0) as v from nation", []bool{false}},
		{"intersect left control", "select count(*) as c from nation intersect select null", []bool{false}},
		{"minus nullable left", "select null as c minus select count(*) from nation", []bool{true}},
		{"union intersect precedence", "select null as c union all select count(*) from nation intersect select 1", []bool{true}},
		{"pruned union output", "select c from (select 1 as k, count(*) as c from nation union all select 2, null) u", []bool{true}},
		{"coalesce null-producing fallback", "select coalesce(n_comment,1/n_nationkey) as v from nation", []bool{true}},
		{"ifnull null-producing fallback", "select ifnull(n_comment,1/n_nationkey) as v from nation", []bool{true}},
		{"coalesce derived fallback", "select coalesce(x,d) as v from (select n_comment as x,1/n_nationkey as d from nation) t", []bool{true}},
		{"ifnull derived fallback", "select ifnull(x,d) as v from (select n_comment as x,1/n_nationkey as d from nation) t", []bool{true}},
		{"coalesce join fallback", "select coalesce(r.r_regionkey,1/n.n_nationkey) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{true}},
		{"ifnull join fallback", "select ifnull(r.r_regionkey,1/n.n_nationkey) as v from nation n left join region r on n.n_regionkey=r.r_regionkey", []bool{true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			require.True(t, mock.ctxt.tables["nation"].Cols[0].Typ.NotNullable)
			require.True(t, mock.ctxt.tables["region"].Cols[0].Typ.NotNullable)
			p, err := buildSingleStmt(mock, t, "create table ctas_nullability as "+test.query)
			require.NoError(t, err)
			var cols []*plan.ColDef
			for _, col := range p.GetDdl().GetCreateTable().GetTableDef().GetCols() {
				if !col.Hidden {
					cols = append(cols, col)
				}
			}
			require.Len(t, cols, len(test.nullable))
			for i, nullable := range test.nullable {
				require.Equal(t, nullable, cols[i].GetDefault().GetNullAbility(), "column %d default", i)
				require.Equal(t, !nullable, cols[i].Typ.NotNullable, "column %d type", i)
			}
		})
	}
}

func TestCreateTableAsSelectDecimalFallback(t *testing.T) {
	for _, decimal := range []types.T{types.T_decimal64, types.T_decimal128} {
		for _, expr := range []string{"ifnull(r.r_regionkey,0)", "coalesce(r.r_regionkey,0.0)", "ifnull(r.r_regionkey,cast(0 as float))", "coalesce(r.r_regionkey,cast(0 as unsigned))"} {
			mock := NewMockOptimizer(false)
			mock.ctxt.tables["region"].Cols[0].Typ = plan.Type{Id: int32(decimal), Width: 18, Scale: 2, NotNullable: true}
			p, err := buildSingleStmt(mock, t, "create table decimal_fallback as select "+expr+" as v from nation n left join region r on n.n_regionkey=r.r_regionkey")
			require.NoError(t, err)
			col := p.GetDdl().GetCreateTable().GetTableDef().Cols[0]
			require.True(t, col.Typ.NotNullable, "%v %s", decimal, expr)
			require.False(t, col.Default.NullAbility, "%v %s", decimal, expr)
		}
	}
}

func TestCreateTableAsSelectPreservesSourceMetadata(t *testing.T) {
	for name, outerJoin := range map[string]bool{"direct": false, "null extended": true} {
		t.Run(name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			source := mock.ctxt.tables["nation"].Cols[1]
			source.Typ.Id = int32(types.T_enum)
			source.Typ.Enumvalues = "low,high"
			mock.ctxt.tables["nation"].Cols[0].Default = &plan.Default{OriginString: "7", NullAbility: false}
			query := "select n.n_name as priority, n.n_nationkey as source_key from nation n"
			if outerJoin {
				query += " right join region r on n.n_regionkey=r.r_regionkey"
			}
			p, err := buildSingleStmt(mock, t, "create table ctas_metadata as "+query)
			require.NoError(t, err)
			col := p.GetDdl().GetCreateTable().GetTableDef().Cols[0]
			require.Equal(t, int32(types.T_enum), col.Typ.Id)
			require.Equal(t, "low,high", col.Typ.Enumvalues)
			key := p.GetDdl().GetCreateTable().GetTableDef().Cols[1]
			require.Equal(t, "7", key.Default.OriginString)
			require.Equal(t, outerJoin, key.Default.NullAbility)
			require.Equal(t, outerJoin, col.Default.NullAbility)
			require.Equal(t, !outerJoin, col.Typ.NotNullable)
			require.True(t, source.Typ.NotNullable, "join must not mutate source metadata")
		})
	}
}

func TestCreateTableAsSelectQuotesIdentifiers(t *testing.T) {
	mock := NewMockOptimizer(false)
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "non-ASCII select alias",
			sql:  "CREATE TABLE ctas_alias AS SELECT N_NAME AS `中文别名` FROM NATION",
			want: "insert into `tpch`.`ctas_alias` select * from (select `nation`.`N_NAME` as `中文别名` from `nation`)",
		},
		{
			name: "reserved table alias",
			sql:  "CREATE TABLE ctas_alias AS SELECT `order`.N_NAME AS `select` FROM NATION AS `order`",
			want: "insert into `tpch`.`ctas_alias` select * from (select `order`.`N_NAME` as `select` from `nation` as `order`)",
		},
		{
			name: "embedded backtick in target name",
			sql:  "CREATE TABLE `ctas``alias` AS SELECT N_NAME FROM NATION",
			want: "insert into `tpch`.`ctas``alias` select * from (select `nation`.`N_NAME` from `nation`)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := buildSingleStmt(mock, t, test.sql)
			require.NoError(t, err)

			createTable := logicPlan.GetDdl().GetCreateTable()
			require.NotNil(t, createTable)
			require.Equal(t, test.want, createTable.GetCreateAsSelectSql())
		})
	}
}

func TestCreateTableAsSelectPreservesIntervalSyntax(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "interval expressions",
			sql:  "select date_add(col2, interval(45, day)), date_sub(col2, interval(5, day)) from time01",
			want: "select date_add(col2, interval 45 day), date_sub(col2, interval 5 day) from time01",
		},
		{
			name: "interval text in identifier",
			sql:  "select `interval(x,day)` from src as `interval(y,month)`",
			want: "select `interval(x,day)` from src as `interval(y,month)`",
		},
		{
			name: "doubled backtick in identifier",
			sql:  "select `a``interval(x,day)` from src",
			want: "select `a``interval(x,day)` from src",
		},
		{
			name: "unclosed backtick",
			sql:  "select `interval(x,day)",
			want: "select `interval(x,day)",
		},
		{
			name: "quoted interval operand",
			sql:  "select date_add(col2, interval(`a,b)`, day)) from src",
			want: "select date_add(col2, interval `a,b)` day) from src",
		},
		{
			name: "single quoted string",
			sql:  "select 'interval(1,day)' as c",
			want: "select 'interval(1,day)' as c",
		},
		{
			name: "double quoted string",
			sql:  `select "interval(1,day)" as c`,
			want: `select "interval(1,day)" as c`,
		},
		{
			name: "doubled quote in string",
			sql:  "select 'a''interval(1,day)' as c",
			want: "select 'a''interval(1,day)' as c",
		},
		{
			name: "backslash escaped quote in string",
			sql:  `select 'a\'interval(1,day)' as c`,
			want: `select 'a\'interval(1,day)' as c`,
		},
		{
			name: "unclosed quoted string",
			sql:  "select 'interval(1,day)",
			want: "select 'interval(1,day)",
		},
		{
			name: "identifier prefix",
			sql:  "select myinterval(1, day), $interval(2, day), 中文interval(3, day)",
			want: "select myinterval(1, day), $interval(2, day), 中文interval(3, day)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, restoreIntervalSyntaxForCTAS(test.sql))
		})
	}
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
		assert.Error(t, err)
		assert.Nil(t, plan)
	})
}

func TestConstructAddedPartitionDefsErrors(t *testing.T) {
	ctx := NewEmptyCompilerContext()
	ctx.SetContext(context.Background())

	makeTableDef := func() *plan.TableDef {
		return &plan.TableDef{
			Name: "t1",
			Cols: []*plan.ColDef{
				{
					Name: "a",
					Typ:  plan.Type{Id: int32(types.T_int32)},
					Default: &plan.Default{
						NullAbility: true,
					},
				},
			},
		}
	}

	newClause := func(parts ...*tree.Partition) *tree.AlterPartitionAddPartitionClause {
		return tree.NewAlterPartitionAddPartitionClause(tree.AlterPartitionAddPartition, parts)
	}

	t.Run("parse error on invalid createsql", func(t *testing.T) {
		tdef := makeTableDef()
		tdef.Createsql = "$$$"
		_, err := constructAddedPartitionDefs(ctx, tdef, newClause())
		assert.Error(t, err)
	})

	t.Run("not a create table in createsql", func(t *testing.T) {
		tdef := makeTableDef()
		tdef.Createsql = "create view v as select 1"
		_, err := constructAddedPartitionDefs(ctx, tdef, newClause())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported ADD PARTITION not in create table")
	})

	t.Run("table without partition option", func(t *testing.T) {
		tdef := makeTableDef()
		tdef.Createsql = "create table t1 (a int)"
		_, err := constructAddedPartitionDefs(ctx, tdef, newClause())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Partition management on a not partitioned table is not possible")
	})

	t.Run("unsupported method: HASH", func(t *testing.T) {
		tdef := makeTableDef()
		tdef.Createsql = "create table t1 (a int) partition by hash(a) partitions 2"
		_, err := constructAddedPartitionDefs(ctx, tdef, newClause())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported partition method in ADD PARTITION")
	})

	// RANGE cases (create table has existing one partition p0 < 10)
	rangeCreate := "create table t1 (a int) partition by range (a) (partition p0 values less than (10))"

	t.Run("RANGE: more than one value in values less than", func(t *testing.T) {
		tdef := makeTableDef()
		tdef.Createsql = rangeCreate
		v1 := tree.NewNumVal[int64](20, "20", false, tree.P_int64)
		v2 := tree.NewNumVal[int64](30, "30", false, tree.P_int64)
		p1 := &tree.Partition{Name: tree.Identifier("p1"), Values: tree.NewValuesLessThan(tree.Exprs{v1, v2})}
		_, err := constructAddedPartitionDefs(ctx, tdef, newClause(p1))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "RANGE PARTITIONING can only have one parameter")
	})

	t.Run("RANGE: MAXVALUE must be last", func(t *testing.T) {
		tdef := makeTableDef()
		tdef.Createsql = rangeCreate
		max := tree.NewMaxValue()
		pMax := &tree.Partition{Name: tree.Identifier("pmax"), Values: tree.NewValuesLessThan(tree.Exprs{max})}
		p2 := &tree.Partition{Name: tree.Identifier("p2"), Values: tree.NewValuesLessThan(tree.Exprs{tree.NewNumVal[int64](20, "20", false, tree.P_int64)})}
		_, err := constructAddedPartitionDefs(ctx, tdef, newClause(pMax, p2))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "MAXVALUE must be the last RANGE partition")
	})

	t.Run("RANGE: values less than must be strictly increasing", func(t *testing.T) {
		tdef := makeTableDef()
		tdef.Createsql = rangeCreate
		p1 := &tree.Partition{Name: tree.Identifier("p1"), Values: tree.NewValuesLessThan(tree.Exprs{tree.NewNumVal[int64](5, "5", false, tree.P_int64)})}
		_, err := constructAddedPartitionDefs(ctx, tdef, newClause(p1))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "VALUES LESS THAN value must be strictly increasing")
	})

	// LIST cases
	listCreate := "create table t1 (a int) partition by list (a) (partition p0 values in (1))"

	t.Run("LIST: empty values", func(t *testing.T) {
		tdef := makeTableDef()
		tdef.Createsql = listCreate
		p1 := &tree.Partition{Name: tree.Identifier("p1"), Values: tree.NewValuesIn(tree.Exprs{})}
		_, err := constructAddedPartitionDefs(ctx, tdef, newClause(p1))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "LIST PARTITIONING must have at least one value")
	})

	t.Run("LIST: duplicate within same partition", func(t *testing.T) {
		tdef := makeTableDef()
		tdef.Createsql = listCreate
		v := tree.NewNumVal[int64](2, "2", false, tree.P_int64)
		p1 := &tree.Partition{Name: tree.Identifier("p1"), Values: tree.NewValuesIn(tree.Exprs{v, v})}
		_, err := constructAddedPartitionDefs(ctx, tdef, newClause(p1))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate values within the same LIST partition are not allowed")
	})

	t.Run("LIST: duplicate across partitions", func(t *testing.T) {
		tdef := makeTableDef()
		tdef.Createsql = listCreate
		v := tree.NewNumVal[int64](1, "1", false, tree.P_int64)
		p1 := &tree.Partition{Name: tree.Identifier("p1"), Values: tree.NewValuesIn(tree.Exprs{v})}
		_, err := constructAddedPartitionDefs(ctx, tdef, newClause(p1))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "LIST PARTITIONING values must be unique across partitions")
	})
}
