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
	"testing"
	"time"
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
