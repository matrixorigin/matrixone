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
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	testutil2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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

func Test_CNTransferTombstoneObjects2(t *testing.T) {
	var (
		opts         testutil.TestOptions
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(0))

	opts.TaeEngineOptions = config.WithLongScanAndCKPOpts(nil)

	opt, err := testutil.GetS3SharedFileServiceOption(ctx, testutil.GetDefaultTestPath("test", t))
	require.NoError(t, err)

	opts.TaeEngineOptions.Fs = opt.Fs

	p := testutil.InitEnginePack(opts, t)
	defer p.Close()

	schema := catalog2.MockSchemaEnhanced(1, 0, 2)
	schema.Name = tableName

	cnTxnOp := p.StartCNTxn()
	_, rel := p.CreateDBAndTable(cnTxnOp, databaseName, schema)
	require.NotNil(t, rel)
	require.NoError(t, cnTxnOp.Commit(ctx))

	{
		_, _, cnTxnOp, err = p.D.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		cnTxnOp.GetWorkspace().StartStatement()
		err = cnTxnOp.GetWorkspace().IncrStatementID(ctx, false)
		require.NoError(t, err)
	}

	bat := catalog2.MockBatch(schema, 20)
	bats := bat.Split(2)

	require.Equal(t, 2, len(bats))
	require.Equal(t, 10, bats[0].Length())

	// append data
	{
		for i := 0; i < len(bats); i++ {
			tnTxnOp, err := p.T.GetDB().StartTxn(nil)
			require.NoError(t, err)

			tnDB, err := tnTxnOp.GetDatabase(databaseName)
			require.NoError(t, err)

			tnRel, err := tnDB.GetRelationByName(tableName)
			require.NoError(t, err)

			err = tnRel.Append(ctx, bats[i])
			require.NoError(t, err)

			require.NoError(t, tnTxnOp.Commit(ctx))

			testutil2.CompactBlocks(t, 0, p.T.GetDB(), databaseName, schema, true)
		}
	}

	// read row id and pk data
	tombstoneBat := batch.NewWithSize(2)
	tombstoneBat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	tombstoneBat.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	{
		txn, _, reader, err := testutil.GetTableTxnReader(
			ctx, p.D, databaseName, tableName, nil, p.Mp, t,
		)
		require.NoError(t, err)

		ret := testutil.EmptyBatchFromSchema(schema)

		for {
			done, err := reader.Read(ctx,
				[]string{schema.GetPrimaryKey().Name, catalog.Row_ID}, nil, p.Mp, ret)

			if done {
				break
			}

			require.NoError(t, err)
			for i := range ret.RowCount() {
				err = vector.AppendFixed[types.Rowid](
					tombstoneBat.Vecs[0],
					vector.GetFixedAtNoTypeCheck[types.Rowid](ret.Vecs[1], i),
					false, p.Mp)
				require.NoError(t, err)

				err = vector.AppendFixed[int32](
					tombstoneBat.Vecs[1],
					vector.GetFixedAtNoTypeCheck[int32](ret.Vecs[0], i),
					false, p.Mp)
				require.NoError(t, err)
			}
		}

		require.NoError(t, txn.Commit(ctx))

		tombstoneBat.SetRowCount(tombstoneBat.Vecs[0].Length())
		require.Equal(t, bat.Length(), tombstoneBat.Vecs[0].Length())
	}

	// merge data objects
	{
		testutil2.MergeBlocks(t, 0, p.T.GetDB(), databaseName, schema, true)

		ss, err := p.D.GetPartitionStateStats(ctx, rel.GetDBID(ctx), rel.GetTableID(ctx))
		require.NoError(t, err)

		fmt.Println(ss.String())
		require.Equal(t, 1, ss.DataObjectsVisible.ObjCnt)
	}

	// mock tombstone object and put it into workspace
	{
		w, err := colexec.NewS3TombstoneWriter()
		require.NoError(t, err)

		w.StashBatch(rel.GetProcess().(*process.Process), tombstoneBat)
		_, ss, err := w.SortAndSync(rel.GetProcess().(*process.Process))
		require.NoError(t, err)

		require.Equal(t, bat.Length(), int(ss.Rows()))

		tbat := batch.NewWithSize(1)
		tbat.Attrs = []string{catalog.ObjectMeta_ObjectStats}
		tbat.Vecs[0] = vector.NewVec(types.T_text.ToType())
		err = vector.AppendBytes(tbat.Vecs[0], ss.Marshal(), false, p.Mp)
		require.NoError(t, err)

		tbat.SetRowCount(tbat.Vecs[0].Length())

		transaction := cnTxnOp.GetWorkspace().(*disttae.Transaction)
		err = transaction.WriteFile(
			disttae.DELETE,
			0, rel.GetDBID(ctx), rel.GetTableID(ctx), databaseName, tableName,
			ss.ObjectLocation().String(),
			tbat,
			p.D.Engine.GetTNServices()[0],
		)
		require.NoError(t, err)

		err = cnTxnOp.UpdateSnapshot(ctx, p.D.Now())
		require.NoError(t, err)
	}

	// check result
	{
		expected := 20
		for i := 0; i < 2; i++ {
			if i == 1 {
				expected = 0
				require.NoError(t, cnTxnOp.Commit(ctx))
			}

			txnop := p.StartCNTxn()
			v, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
			require.True(t, ok)

			exec := v.(executor.SQLExecutor)

			res, err := exec.Exec(p.Ctx,
				fmt.Sprintf("select count(*) from `%s`.`%s`;",
					databaseName, tableName), executor.Options{}.WithTxn(txnop))
			require.NoError(t, err)
			require.NoError(t, txnop.Commit(ctx))

			res.ReadRows(func(rows int, cols []*vector.Vector) bool {
				require.Equal(t, 1, rows)
				require.Equal(t, 1, len(cols))

				result := vector.MustFixedColNoTypeCheck[int64](cols[0])
				require.Equal(t, expected, int(result[0]))
				return true
			})

			res.Close()
		}
	}
}
