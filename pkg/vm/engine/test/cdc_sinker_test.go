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
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/defines"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

func TestCDC_Sinker1(t *testing.T) {
	var mock sqlmock.Sqlmock
	mockFn := func(_, _, _ string, _ int, _ string) (db *sql.DB, err error) {
		db, mock, err = sqlmock.New()
		return
	}
	stub := gostub.Stub(&cdc.OpenDbConn, mockFn)
	defer stub.Reset()

	executor, err := cdc.NewExecutor(
		"root",
		"123456",
		"127.0.0.1",
		3306,
		3,
		3*time.Second,
		cdc.CDCDefaultSendSqlTimeout,
		false,
	)
	require.NoError(t, err)
	defer executor.Close()

	ar := cdc.NewCdcActiveRoutine()
	sink := cdc.NewMysqlSinker2(
		executor,
		1,
		"task1",
		&cdc.DbTableInfo{
			SinkDbName:  "test_db",
			SinkTblName: "test_tbl",
		},
		nil, // watermark updater not required for this test
		nil, // statement builder not required for transaction commands
		ar,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go sink.Run(ctx, ar)

	mock.ExpectBegin()
	sink.SendBegin()
	sink.SendDummy()
	require.Eventually(t, func() bool {
		return executor.HasActiveTx()
	}, time.Second, 10*time.Millisecond)

	mock.ExpectRollback()
	sink.SendRollback()
	sink.SendDummy()
	require.Eventually(t, func() bool {
		return !executor.HasActiveTx()
	}, time.Second, 10*time.Millisecond)

	mock.ExpectBegin()
	sink.SendBegin()
	sink.SendDummy()
	require.Eventually(t, func() bool {
		return executor.HasActiveTx()
	}, time.Second, 10*time.Millisecond)

	mock.ExpectCommit()
	sink.SendCommit()
	sink.SendDummy()
	require.Eventually(t, func() bool {
		return !executor.HasActiveTx()
	}, time.Second, 10*time.Millisecond)

	cancel()
	sink.Close()
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCDCUtil1(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()
	schema := catalog2.MockSchemaAll(10, 0)
	schema.Name = tableName
	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	_, rel, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	id := rel.GetTableID(context.Background())
	require.NoError(t, err)

	txn, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
	require.NoError(t, err)
	_, err = cdc.GetTableDef(ctx, txn, disttaeEngine.Engine, id)
	require.NoError(t, err)
}
