// Copyright 2024 Matrix Origin
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

package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/assert"

	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func mock_mo_intra_system_change_propagation_log(
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
) (err error) {
	sql := frontend.MoCatalogMoISCPLogDDL

	v, ok := moruntime.ServiceRuntime("").GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	exec := v.(executor.SQLExecutor)
	txn, err := de.NewTxnOperator(ctx, de.Now())
	if err != nil {
		return err
	}
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(txn)

	_, err = exec.Exec(ctx, sql, opts)
	if err != nil {
		return err
	}
	if err = txn.Commit(ctx); err != nil {
		return err
	}
	return err
}

func mock_mo_indexes(
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
) (err error) {
	sql := "CREATE TABLE `mo_catalog`.`mo_indexes` ( " +
		"`id` bigint unsigned NOT NULL," +
		"`table_id` bigint unsigned NOT NULL," +
		"`database_id` bigint unsigned NOT NULL," +
		"`name` varchar(64) NOT NULL," +
		"`type` varchar(11) NOT NULL," +
		"`algo` varchar(11) DEFAULT NULL," +
		"`algo_table_type` varchar(11) DEFAULT NULL," +
		"`algo_params` varchar(2048) DEFAULT NULL," +
		"`is_visible` tinyint NOT NULL," +
		"`hidden` tinyint NOT NULL," +
		"`comment` varchar(2048) NOT NULL," +
		"`column_name` varchar(256) NOT NULL," +
		"`ordinal_position` int unsigned NOT NULL," +
		"`options` text DEFAULT NULL," +
		"`index_table_name` varchar(5000) DEFAULT NULL," +
		"PRIMARY KEY (`table_id`,`column_name`)" + // use table_id as primary key instead of id to avoid duplicate
		")"

	v, ok := moruntime.ServiceRuntime("").GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	exec := v.(executor.SQLExecutor)
	txn, err := de.NewTxnOperator(ctx, de.Now())
	if err != nil {
		return err
	}
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(txn)

	_, err = exec.Exec(ctx, sql, opts)
	if err != nil {
		return err
	}
	if err = txn.Commit(ctx); err != nil {
		return err
	}
	return err
}
func mock_mo_foreign_keys(
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
) (err error) {
	sql := "CREATE TABLE `mo_catalog`.`mo_foreign_keys` (" +
		"`constraint_name` varchar(5000) NOT NULL," +
		"`constraint_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`db_name` varchar(5000) NOT NULL," +
		"`db_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`table_name` varchar(5000) NOT NULL," +
		"`table_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`column_name` varchar(256) NOT NULL," +
		"`column_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`refer_db_name` varchar(5000) NOT NULL," +
		"`refer_db_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`refer_table_name` varchar(5000) NOT NULL," +
		"`refer_table_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`refer_column_name` varchar(256) NOT NULL," +
		"`refer_column_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`on_delete` varchar(128) NOT NULL," +
		"`on_update` varchar(128) NOT NULL," +
		"PRIMARY KEY (`constraint_name`,`constraint_id`,`db_name`,`db_id`,`table_name`,`table_id`,`column_name`,`column_id`,`refer_db_name`,`refer_db_id`,`refer_table_name`,`refer_table_id`,`refer_column_name`,`refer_column_id`)" +
		")"

	result, err := execSql(de, ctx, sql)
	result.Close()
	return err
}

func execSql(
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
	sql string,
) (result executor.Result, err error) {
	v, ok := moruntime.ServiceRuntime("").GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	exec := v.(executor.SQLExecutor)
	txn, err := de.NewTxnOperator(ctx, de.Now())
	if err != nil {
		return
	}
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(txn)

	result, err = exec.Exec(ctx, sql, opts)
	if err != nil {
		return
	}
	if err = txn.Commit(ctx); err != nil {
		return result, err
	}
	return result, nil
}

func CreateDBAndTableForHNSWAndGetAppendData(
	t *testing.T,
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
	databaseName string,
	tableName string,
	rowCount int,
) *containers.Batch {
	// int64 is column 3, array_float32 is column 18
	schema := catalog2.MockSchemaAll(20, 3)
	txn, err := de.NewTxnOperator(ctx, de.Now())
	assert.NoError(t, err)

	err = de.Engine.Create(ctx, databaseName, txn)
	assert.NoError(t, err)

	database, err := de.Engine.Database(ctx, databaseName, txn)
	assert.NoError(t, err)

	engineTblDef, err := testutil.EngineTableDefBySchema(schema)
	assert.NoError(t, err)

	// add index
	indexColName := schema.ColDefs[18].Name
	engineTblDef = testutil.EngineDefAddIndex(engineTblDef, indexColName)

	err = database.Create(ctx, tableName, engineTblDef)
	assert.NoError(t, err)

	_, err = database.Relation(ctx, tableName, nil)
	assert.NoError(t, err)

	err = txn.Commit(ctx)
	assert.NoError(t, err)

	return catalog2.MockBatch(schema, rowCount)
}

func CreateDBAndTableForCNConsumerAndGetAppendData(
	t *testing.T,
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
	databaseName string,
	tableName string,
	rowCount int,
) *containers.Batch {
	createDBSql := fmt.Sprintf("create database if not exists %s", databaseName)
	createTableSql := fmt.Sprintf(
		`create table %s.%s (
		id int primary key,
		name varchar,
		i1 bool,
		i2 TINYINT,
		i3 SMALLINT,
		i4 INT,
		i5 BIGINT,
		i6 FLOAT,
		i7 DOUBLE,
		i8 TIMESTAMP,
		i9 TINYINT UNSIGNED,
		i10 SMALLINT UNSIGNED,
		i11 INT UNSIGNED,
		i12 BIGINT UNSIGNED
		)`, databaseName, tableName)

	v, ok := moruntime.ServiceRuntime("").
		GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	exec := v.(executor.SQLExecutor)
	_, err := exec.Exec(ctx, createDBSql, executor.Options{})
	assert.NoError(t, err)
	_, err = exec.Exec(ctx, createTableSql, executor.Options{})
	assert.NoError(t, err)

	return containers.MockBatchWithAttrs(
		[]types.Type{
			types.T_int32.ToType(),
			types.T_varchar.ToType(),
			types.T_bool.ToType(), //i1
			types.T_int8.ToType(),
			types.T_int16.ToType(),
			types.T_int32.ToType(),
			types.T_int64.ToType(), //i5
			types.T_float32.ToType(),
			types.T_float64.ToType(),
			types.T_timestamp.ToType(),
			types.T_uint8.ToType(),
			types.T_uint16.ToType(),
			types.T_uint32.ToType(),
			types.T_uint64.ToType(), //i12
		},
		[]string{"id", "name", "i1", "i2", "i3", "i4", "i5", "i6", "i7", "i8", "i9", "i10", "i11", "i12"},
		rowCount,
		0,
		nil,
	)
}

func GetTestISCPExecutorOption() *iscp.ISCPExecutorOption {
	return &iscp.ISCPExecutorOption{
		GCInterval:             time.Millisecond * 100,
		GCTTL:                  time.Millisecond,
		SyncTaskInterval:       time.Millisecond * 100,
		FlushWatermarkInterval: time.Millisecond * 500,
		RetryTimes:             1,
	}
}

func CheckTableData(
	t *testing.T,
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
	dbName string,
	tableName string,
	tableID uint64,
	indexName string,
) {
	asyncIndexDBName := iscp.TargetDbName
	asyncIndexTableName := fmt.Sprintf("test_table_%d_%v", tableID, indexName)
	sql1 := fmt.Sprintf(
		"SELECT * FROM %v.%v EXCEPT SELECT * FROM %v.%v;",
		dbName, tableName,
		asyncIndexDBName, asyncIndexTableName,
	)
	result1, err := execSql(de, ctx, sql1)
	assert.NoError(t, err)
	defer result1.Close()
	rowCount := 0
	result1.ReadRows(func(rows int, cols []*vector.Vector) bool {
		rowCount += rows
		return true
	})
	assert.Equal(t, rowCount, 0)

	sql2 := fmt.Sprintf(
		"SELECT * FROM %v.%v EXCEPT SELECT * FROM %v.%v;",
		asyncIndexDBName, asyncIndexTableName,
		dbName, tableName,
	)
	result2, err := execSql(de, ctx, sql2)
	assert.NoError(t, err)
	defer result2.Close()
	rowCount = 0
	result2.ReadRows(func(rows int, cols []*vector.Vector) bool {
		rowCount += rows
		return true
	})
	assert.Equal(t, rowCount, 0)
}
