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
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

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
		i8 TINYINT UNSIGNED,
		i9 SMALLINT UNSIGNED,
		i10 INT UNSIGNED,
		i11 BIGINT UNSIGNED
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
			types.T_uint8.ToType(),
			types.T_uint16.ToType(),
			types.T_uint32.ToType(),
			types.T_uint64.ToType(), //i11
		},
		[]string{"id", "name", "i1", "i2", "i3", "i4", "i5", "i6", "i7", "i8", "i9", "i10", "i11"},
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
