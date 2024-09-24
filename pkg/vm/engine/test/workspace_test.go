// Copyright 2022 Matrix Origin
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
	"encoding/hex"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	testutil2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"math/rand"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/require"
)

func Test_BigDeleteWriteS3(t *testing.T) {
	var (
		//err          error
		mp           *mpool.MPool
		accountId    = catalog.System_Account
		tableName    = "test_reader_table"
		databaseName = "test_reader_database"

		primaryKeyIdx = 3

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	// mock a schema with 4 columns and the 4th column as primary key
	// the first column is the 9th column in the predefined columns in
	// the mock function. Here we exepct the type of the primary key
	// is types.T_char or types.T_varchar
	schema := catalog2.MockSchemaEnhanced(4, primaryKeyIdx, 9)
	schema.Name = tableName

	{
		opt, err := testutil.GetS3SharedFileServiceOption(ctx, testutil.GetDefaultTestPath("test", t))
		require.NoError(t, err)

		disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(
			ctx,
			testutil.TestOptions{TaeEngineOptions: opt},
			t,
			testutil.WithDisttaeEngineInsertEntryMaxCount(1),
			testutil.WithDisttaeEngineWorkspaceThreshold(1),
		)
		defer func() {
			disttaeEngine.Close(ctx)
			taeEngine.Close(true)
			rpcAgent.Close()
		}()

		_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
		require.NoError(t, err)
	}

	insertCnt := 150
	deleteCnt := 100
	var bat2 *batch.Batch

	{
		// insert 150 rows
		_, table, txn, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		bat := catalog2.MockBatch(schema, insertCnt)
		err = table.Write(ctx, containers.ToCNBatch(bat))
		require.NoError(t, err)

		txn.GetWorkspace().(*disttae.Transaction).ForEachTableWrites(
			table.GetDBID(ctx), table.GetTableID(ctx), 1, func(entry disttae.Entry) {
				waitedDeletes := vector.MustFixedColWithTypeCheck[types.Rowid](entry.Bat().GetVector(0))
				waitedDeletes = waitedDeletes[:deleteCnt]
				bat2 = batch.NewWithSize(1)
				bat2.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
				bat2.SetRowCount(len(waitedDeletes))
				require.NoError(t, vector.AppendFixedList[types.Rowid](bat2.Vecs[0], waitedDeletes, nil, mp))
			})

		//delete 100 rows
		require.NoError(t, table.Delete(ctx, bat2, catalog.Row_ID))
		require.NoError(t, txn.Commit(ctx))
	}

	{
		_, _, reader, err := testutil.GetTableTxnReader(
			ctx,
			disttaeEngine,
			databaseName,
			tableName,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
		_, err = reader.Read(ctx, ret.Attrs, nil, mp, ret)
		require.NoError(t, err)
		require.Equal(t, insertCnt-deleteCnt, ret.RowCount())
	}
}

func randomVarcharByLength(ll int) string {
	bb := make([]byte, ll/2)
	for i := 0; i < ll/2; i++ {
		bb[i] = byte(rand.Intn(256))
	}

	str := hex.EncodeToString(bb)
	fmt.Println(str)
	return str
}

func Test_CNTransferTombstoneObjects(t *testing.T) {
	var (
		opts         testutil.TestOptions
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	opts.TaeEngineOptions = config.WithLongScanAndCKPOpts(nil)

	opt, err := testutil.GetS3SharedFileServiceOption(ctx, testutil.GetDefaultTestPath("test", t))
	require.NoError(t, err)

	opts.TaeEngineOptions.Fs = opt.Fs

	p := testutil.InitEnginePack(opts, t)
	defer p.Close()

	schema := catalog2.MockSchemaEnhanced(1, 0, 3)
	schema.Name = tableName

	txnop := p.StartCNTxn()
	_, rel := p.CreateDBAndTable(txnop, databaseName, schema)
	require.NotNil(t, rel)
	require.NoError(t, txnop.Commit(ctx))

	txnop = p.StartCNTxn()
	v, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
	require.True(t, ok)

	exec := v.(executor.SQLExecutor)

	deletion.SetCNFlushDeletesThreshold(1)

	{
		res, err := exec.Exec(p.Ctx,
			fmt.Sprintf("insert into `%s`.`%s` select * from generate_series(1, 100*100*10)g;",
				databaseName, tableName,
			),
			executor.Options{}.
				WithTxn(txnop).
				WithWaitCommittedLogApplied())
		require.NoError(t, err)
		res.Close()
		require.NoError(t, txnop.Commit(ctx))
	}

	{
		exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {

			_, err := txn.Exec(
				fmt.Sprintf("delete from `%s`.`%s` where 1=1;", databaseName, tableName),
				executor.StatementOption{})
			require.NoError(t, err)

			p.D.SubscribeTable(ctx, rel.GetDBID(ctx), rel.GetTableID(ctx), true)

			testutil2.MergeBlocks(t, 0, p.T.GetDB(), databaseName, schema, true)

			return nil
		}, executor.Options{})
	}
}
