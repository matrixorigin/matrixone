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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
)

func Test_ReaderCanReadRangesBlocksWithoutDeletes(t *testing.T) {
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_reader_table"
		databaseName = "test_reader_database"

		primaryKeyIdx int = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	opt, err := testutil.GetS3SharedFileServiceOption(ctx, testutil.GetDefaultTestPath("test", t))
	require.NoError(t, err)

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(ctx, testutil.TestOptions{TaeEngineOptions: opt}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	blockCnt := 10
	rowsCount := int(objectio.BlockMaxRows) * blockCnt
	bats := catalog2.MockBatch(schema, rowsCount).Split(blockCnt)

	// write table
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		for idx := 0; idx < blockCnt; idx++ {
			require.NoError(t, relation.Write(ctx, containers.ToCNBatch(bats[idx])))
		}

		require.NoError(t, txn.Commit(ctx))
	}

	// require.NoError(t, disttaeEngine.SubscribeTable(ctx, relation.GetDBID(ctx), relation.GetTableID(ctx), false))

	// TODO
	// {
	// 	stats, err := disttaeEngine.GetPartitionStateStats(ctx, relation.GetDBID(ctx), relation.GetTableID(ctx))
	// 	require.NoError(t, err)

	// 	require.Equal(t, blockCnt, stats.DataObjectsVisible.BlkCnt)
	// 	require.Equal(t, rowsCount, stats.DataObjectsVisible.RowCnt)
	// }

	expr := []*plan.Expr{
		disttae.MakeFunctionExprForTest("=", []*plan.Expr{
			disttae.MakeColExprForTest(int32(primaryKeyIdx), schema.ColDefs[primaryKeyIdx].Type.Oid, schema.ColDefs[primaryKeyIdx].Name),
			plan2.MakePlan2Int64ConstExprWithType(bats[0].Vecs[primaryKeyIdx].Get(0).(int64)),
		}),
	}

	txn, _, reader, err := testutil.GetTableTxnReader(
		ctx,
		disttaeEngine,
		databaseName,
		tableName,
		expr,
		mp,
		t,
	)
	require.NoError(t, err)

	resultHit := 0
	ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
	for idx := 0; idx < blockCnt; idx++ {
		_, err = reader.Read(ctx, ret.Attrs, expr[0], mp, ret)
		require.NoError(t, err)

		resultHit += int(ret.RowCount())
		ret.CleanOnlyData()
	}

	require.Equal(t, 1, resultHit)
	require.NoError(t, txn.Commit(ctx))
}

func TestReaderCanReadUncommittedInMemInsertAndDeletes(t *testing.T) {
	t.Skip("not finished")
	var (
		err          error
		mp           *mpool.MPool
		txn          client.TxnOperator
		accountId    = catalog.System_Account
		tableName    = "test_reader_table"
		databaseName = "test_reader_database"

		primaryKeyIdx int = 3

		relation engine.Relation
		_        engine.Database

		taeEngine     *testutil.TestTxnStorage
		rpcAgent      *testutil.MockRPCAgent
		disttaeEngine *testutil.TestDisttaeEngine
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = tableName

	opt, err := testutil.GetS3SharedFileServiceOption(ctx, testutil.GetDefaultTestPath("test", t))
	require.NoError(t, err)

	disttaeEngine, taeEngine, rpcAgent, mp = testutil.CreateEngines(ctx, testutil.TestOptions{TaeEngineOptions: opt}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	rowsCount := 10
	bat1 := catalog2.MockBatch(schema, rowsCount)

	// write table
	{
		_, relation, txn, err = disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		require.NoError(t, relation.Write(ctx, containers.ToCNBatch(bat1)))

		var bat2 *batch.Batch
		txn.GetWorkspace().(*disttae.Transaction).ForEachTableWrites(
			relation.GetDBID(ctx), relation.GetTableID(ctx), 1, func(entry disttae.Entry) {
				waitedDeletes := vector.MustFixedColWithTypeCheck[types.Rowid](entry.Bat().GetVector(0))
				waitedDeletes = waitedDeletes[:rowsCount/2]
				bat2 = batch.NewWithSize(1)
				bat2.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
				require.NoError(t, vector.AppendFixedList[types.Rowid](bat2.Vecs[0], waitedDeletes, nil, mp))
			})

		require.NoError(t, relation.Delete(ctx, bat2, catalog.Row_ID))
	}

	expr := []*plan.Expr{
		disttae.MakeFunctionExprForTest("=", []*plan.Expr{
			disttae.MakeColExprForTest(int32(primaryKeyIdx), schema.ColDefs[primaryKeyIdx].Type.Oid, schema.ColDefs[primaryKeyIdx].Name),
			plan2.MakePlan2Int64ConstExprWithType(bat1.Vecs[primaryKeyIdx].Get(9).(int64)),
		}),
	}

	reader, err := testutil.GetRelationReader(
		ctx,
		disttaeEngine,
		txn,
		relation,
		expr,
		mp,
		t,
	)
	require.NoError(t, err)

	ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
	_, err = reader.Read(ctx, ret.Attrs, expr[0], mp, nil, ret)
	require.NoError(t, err)

	require.Equal(t, 1, int(ret.RowCount()))
	require.NoError(t, txn.Commit(ctx))
}

func Test_ReaderCanReadCommittedInMemInsertAndDeletes(t *testing.T) {
	var (
		//err          error
		mp           *mpool.MPool
		accountId    = catalog.System_Account
		tableName    = "test_reader_table"
		databaseName = "test_reader_database"

		primaryKeyIdx int = 3

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
			testutil.WithDisttaeEngineWorkspaceThreshold(mpool.MB*2),
			testutil.WithDisttaeEngineInsertEntryMaxCount(10000),
		)
		defer func() {
			disttaeEngine.Close(ctx)
			taeEngine.Close(true)
			rpcAgent.Close()
		}()

		_, _, err = disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
		require.NoError(t, err)

	}

	{
		txn, err := taeEngine.StartTxn()
		require.NoError(t, err)

		database, _ := txn.GetDatabase(databaseName)
		rel, _ := database.GetRelationByName(schema.Name)

		rowsCnt := 10
		bat := catalog2.MockBatch(schema, rowsCnt)
		err = rel.Append(ctx, bat)
		require.Nil(t, err)

		err = txn.Commit(context.Background())
		require.Nil(t, err)

	}

	{
		txn, _ := taeEngine.StartTxn()
		database, _ := txn.GetDatabase(databaseName)
		rel, _ := database.GetRelationByName(schema.Name)

		iter := rel.MakeObjectIt(false)
		iter.Next()
		blkId := iter.GetObject().GetMeta().(*catalog2.ObjectEntry).AsCommonID()

		err := rel.RangeDelete(blkId, 0, 7, handle.DT_Normal)
		require.Nil(t, err)

		require.NoError(t, txn.Commit(context.Background()))
	}

	{

		txn, _, reader, err := testutil.GetTableTxnReader(
			ctx,
			disttaeEngine,
			databaseName,
			tableName,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		ret := batch.NewWithSize(1)
		for _, col := range schema.ColDefs {
			if col.Name == schema.ColDefs[primaryKeyIdx].Name {
				vec := vector.NewVec(col.Type)
				ret.Vecs[0] = vec
				ret.Attrs = []string{col.Name}
				break
			}
		}
		_, err = reader.Read(ctx, []string{schema.ColDefs[primaryKeyIdx].Name}, nil, mp, ret)
		require.NoError(t, err)
		require.True(t, ret.Allocated() > 0)

		require.Equal(t, 2, ret.RowCount())
		require.NoError(t, txn.Commit(ctx))
		ret.Clean(mp)
	}
	{
		_, relation, txn, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		rowsCnt := 4000
		bat := catalog2.MockBatch(schema, rowsCnt)
		pkVec := bat.Vecs[primaryKeyIdx].GetDownstreamVector()
		pkVec.CleanOnlyData()
		for i := 0; i < rowsCnt; i++ {
			buf := fmt.Sprintf("%s:%d", strings.Repeat("a", 200), i)
			vector.AppendBytes(pkVec, []byte(buf), false, mp)
		}
		defer bat.Close()
		require.NoError(
			t,
			testutil.WriteToRelation(
				ctx, txn, relation, containers.ToCNBatch(bat), true,
			),
		)

		reader, err := testutil.GetRelationReader(
			ctx,
			disttaeEngine,
			txn,
			relation,
			nil,
			mp,
			t,
		)
		require.NoError(t, err)

		nmp, _ := mpool.NewMPool("test", mpool.MB, mpool.NoFixed)

		ret := testutil.EmptyBatchFromSchema(schema, primaryKeyIdx)
		_, err = reader.Read(ctx, ret.Attrs, nil, nmp, ret)
		require.Error(t, err)
		require.NoError(t, txn.Commit(ctx))
	}

}
