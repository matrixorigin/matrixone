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
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/shard"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	testutil3 "github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/engine_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	testutil2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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

	var exes []colexec.ExpressionExecutor
	proc := testutil3.NewProcessWithMPool("", mp)
	expr := []*plan.Expr{
		engine_util.MakeFunctionExprForTest("=", []*plan.Expr{
			engine_util.MakeColExprForTest(int32(primaryKeyIdx), schema.ColDefs[primaryKeyIdx].Type.Oid, schema.ColDefs[primaryKeyIdx].Name),
			plan2.MakePlan2Int64ConstExprWithType(bats[0].Vecs[primaryKeyIdx].Get(0).(int64)),
		}),
	}
	for _, e := range expr {
		plan2.ReplaceFoldExpr(proc, e, &exes)
	}
	for _, e := range expr {
		plan2.EvalFoldExpr(proc, e, &exes)
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

	for _, exe := range exes {
		exe.Free()
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
		engine_util.MakeFunctionExprForTest("=", []*plan.Expr{
			engine_util.MakeColExprForTest(int32(primaryKeyIdx), schema.ColDefs[primaryKeyIdx].Type.Oid, schema.ColDefs[primaryKeyIdx].Name),
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
	_, err = reader.Read(ctx, ret.Attrs, expr[0], mp, ret)
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

func Test_ShardingHandler(t *testing.T) {
	var (
		//err          error
		//mp           *mpool.MPool
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

		disttaeEngine, taeEngine, rpcAgent, _ = testutil.CreateEngines(
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

	testutil2.CompactBlocks(t, 0, taeEngine.GetDB(), databaseName, schema, false)

	{

		txn, _ := taeEngine.StartTxn()
		database, _ := txn.GetDatabase(databaseName)
		rel, _ := database.GetRelationByName(schema.Name)

		iter := rel.MakeObjectIt(false)
		iter.Next()
		blkId := iter.GetObject().GetMeta().(*catalog2.ObjectEntry).AsCommonID()

		err := rel.RangeDelete(blkId, 0, 1, handle.DT_Normal)
		require.Nil(t, err)

		require.NoError(t, txn.Commit(context.Background()))

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
		var blkId *common.ID
		for iter.Next() {
			if !iter.GetObject().IsAppendable() {
				continue
			}
			blkId = iter.GetObject().GetMeta().(*catalog2.ObjectEntry).AsCommonID()
		}
		err := rel.RangeDelete(blkId, 0, 7, handle.DT_Normal)
		require.Nil(t, err)

		require.NoError(t, txn.Commit(context.Background()))

	}
	//handle collect tombstones.
	{
		_, rel, txn, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		pInfo, err := process.MockProcessInfoWithPro("", rel.GetProcess())
		require.NoError(t, err)
		readerBuildParam := shard.ReadParam{
			Process: pInfo,
			TxnTable: shard.TxnTable{
				DatabaseID:   rel.GetDBID(ctx),
				DatabaseName: databaseName,
				AccountID:    uint64(catalog.System_Account),
				TableName:    tableName,
			},
		}
		readerBuildParam.CollectTombstonesParam.CollectPolicy =
			engine.Policy_CollectCommittedTombstones

		res, err := disttae.HandleShardingReadCollectTombstones(
			ctx,
			shard.TableShard{},
			disttaeEngine.Engine,
			readerBuildParam,
			timestamp.Timestamp{},
			morpc.NewBuffer(),
		)
		require.NoError(t, err)

		tombstones, err := disttae.UnmarshalTombstoneData(res)
		require.NoError(t, err)

		require.True(t, tombstones.HasAnyInMemoryTombstone())

		readerBuildParam.GetColumMetadataScanInfoParam.ColumnName =
			schema.ColDefs[primaryKeyIdx].Name

		var m plan.MetadataScanInfos
		res, err = disttae.HandleShardingReadGetColumMetadataScanInfo(
			ctx,
			shard.TableShard{},
			disttaeEngine.Engine,
			readerBuildParam,
			timestamp.Timestamp{},
			morpc.NewBuffer(),
		)
		require.NoError(t, err)

		err = m.Unmarshal(res)
		require.NoError(t, err)

		require.NoError(t, txn.Commit(ctx))
	}

}

func Test_ShardingRemoteReader(t *testing.T) {
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

	testutil2.CompactBlocks(t, 0, taeEngine.GetDB(), databaseName, schema, false)

	{

		txn, _ := taeEngine.StartTxn()
		database, _ := txn.GetDatabase(databaseName)
		rel, _ := database.GetRelationByName(schema.Name)

		iter := rel.MakeObjectIt(false)
		iter.Next()
		blkId := iter.GetObject().GetMeta().(*catalog2.ObjectEntry).AsCommonID()

		err := rel.RangeDelete(blkId, 0, 1, handle.DT_Normal)
		require.Nil(t, err)

		require.NoError(t, txn.Commit(context.Background()))

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
		var blkId *common.ID
		for iter.Next() {
			if !iter.GetObject().IsAppendable() {
				continue
			}
			blkId = iter.GetObject().GetMeta().(*catalog2.ObjectEntry).AsCommonID()
		}
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
		_, err = reader.Read(ctx, []string{
			schema.ColDefs[primaryKeyIdx].Name}, nil, mp, ret)
		require.NoError(t, err)
		require.True(t, ret.Allocated() > 0)

		require.Equal(t, 2, ret.RowCount())
		require.NoError(t, txn.Commit(ctx))
		ret.Clean(mp)
	}

	{
		_, rel, txn, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		pInfo, err := process.MockProcessInfoWithPro("", rel.GetProcess())
		require.NoError(t, err)
		readerBuildParam := shard.ReadParam{
			Process: pInfo,
			TxnTable: shard.TxnTable{
				DatabaseID:   rel.GetDBID(ctx),
				DatabaseName: databaseName,
				AccountID:    uint64(catalog.System_Account),
				TableName:    tableName,
			},
		}
		relData, err := rel.Ranges(ctx, nil, 0)
		require.NoError(t, err)
		//TODO:: attach tombstones.
		//tombstones, err := rel.CollectTombstones(
		//	ctx,
		//	0,
		//	engine.Policy_CollectAllTombstones)
		data, err := relData.MarshalBinary()
		require.NoError(t, err)
		readerBuildParam.ReaderBuildParam.RelData = data
		readerBuildParam.ReaderBuildParam.ScanType = disttae.SMALL
		readerBuildParam.ReaderBuildParam.TombstoneApplyPolicy =
			int32(engine.Policy_SkipUncommitedInMemory | engine.Policy_SkipUncommitedS3)
		res, err := disttae.HandleShardingReadBuildReader(
			ctx,
			shard.TableShard{},
			disttaeEngine.Engine,
			readerBuildParam,
			timestamp.Timestamp{},
			morpc.NewBuffer(),
		)
		require.NoError(t, err)
		streamID := types.DecodeUuid(res)
		//readNext
		readNextParam := shard.ReadParam{
			Process: pInfo,
			TxnTable: shard.TxnTable{
				DatabaseID:   rel.GetDBID(ctx),
				DatabaseName: databaseName,
				AccountID:    uint64(catalog.System_Account),
				TableName:    tableName,
			},
		}
		readNextParam.ReadNextParam.Uuid = types.EncodeUuid(&streamID)
		readNextParam.ReadNextParam.Columns = []string{
			schema.ColDefs[primaryKeyIdx].Name}
		buildBatch := func() *batch.Batch {
			bat := batch.NewWithSize(1)
			for _, col := range schema.ColDefs {
				if col.Name == schema.ColDefs[primaryKeyIdx].Name {
					vec := vector.NewVec(col.Type)
					bat.Vecs[0] = vec
					bat.Attrs = []string{col.Name}
					break
				}
			}
			return bat
		}
		rows := 0
		for {
			bat := buildBatch()
			res, err := disttae.HandleShardingReadNext(
				ctx,
				shard.TableShard{},
				disttaeEngine.Engine,
				readNextParam,
				timestamp.Timestamp{},
				morpc.NewBuffer(),
			)
			require.NoError(t, err)
			isEnd := types.DecodeBool(res)
			if isEnd {
				break
			}
			res = res[1:]
			l := types.DecodeUint32(res)
			res = res[4:]
			if err := bat.UnmarshalBinary(res[:l]); err != nil {
				panic(err)
			}
			rows += int(bat.RowCount())
			bat.Clean(mp)
		}
		require.Equal(t, 2, rows)

		readCloseParam := shard.ReadParam{
			Process: pInfo,
			TxnTable: shard.TxnTable{
				DatabaseID:   rel.GetDBID(ctx),
				DatabaseName: databaseName,
				AccountID:    uint64(catalog.System_Account),
				TableName:    tableName,
			},
		}
		readCloseParam.ReadCloseParam.Uuid = types.EncodeUuid(&streamID)
		_, err = disttae.HandleShardingReadClose(
			ctx,
			shard.TableShard{},
			disttaeEngine.Engine,
			readCloseParam,
			timestamp.Timestamp{},
			morpc.NewBuffer(),
		)
		require.NoError(t, err)

		_, err = disttae.HandleShardingReadNext(
			ctx,
			shard.TableShard{},
			disttaeEngine.Engine,
			readNextParam,
			timestamp.Timestamp{},
			morpc.NewBuffer(),
		)
		require.Error(t, err)

		_, err = disttae.HandleShardingReadClose(
			ctx,
			shard.TableShard{},
			disttaeEngine.Engine,
			readCloseParam,
			timestamp.Timestamp{},
			morpc.NewBuffer(),
		)
		require.Error(t, err)

		require.NoError(t, txn.Commit(ctx))
	}

}

func Test_ShardingTableDelegate(t *testing.T) {
	var (
		//err          error
		//mp           *mpool.MPool
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

		disttaeEngine, taeEngine, rpcAgent, _ = testutil.CreateEngines(
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

	testutil2.CompactBlocks(t, 0, taeEngine.GetDB(), databaseName, schema, false)

	{

		txn, _ := taeEngine.StartTxn()
		database, _ := txn.GetDatabase(databaseName)
		rel, _ := database.GetRelationByName(schema.Name)

		iter := rel.MakeObjectIt(false)
		iter.Next()
		blkId := iter.GetObject().GetMeta().(*catalog2.ObjectEntry).AsCommonID()

		err := rel.RangeDelete(blkId, 0, 1, handle.DT_Normal)
		require.Nil(t, err)

		require.NoError(t, txn.Commit(context.Background()))

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
		var blkId *common.ID
		for iter.Next() {
			if !iter.GetObject().IsAppendable() {
				continue
			}
			blkId = iter.GetObject().GetMeta().(*catalog2.ObjectEntry).AsCommonID()
		}
		err := rel.RangeDelete(blkId, 0, 7, handle.DT_Normal)
		require.Nil(t, err)

		require.NoError(t, txn.Commit(context.Background()))

	}
	//start to build sharding readers.
	_, rel, txn, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)

	//relData, err := rel.Ranges(ctx, nil, 0)
	//require.NoError(t, err)
	shardSvr := testutil.MockShardService()
	delegate, _ := disttae.MockTableDelegate(rel, shardSvr)

	relData, err := delegate.Ranges(ctx, nil, 0)
	require.NoError(t, err)

	tomb, err := delegate.CollectTombstones(ctx, 0, engine.Policy_CollectAllTombstones)
	require.NoError(t, err)
	require.True(t, tomb.HasAnyInMemoryTombstone())

	_, err = delegate.BuildReaders(
		ctx,
		rel.GetProcess(),
		nil,
		relData,
		1,
		0,
		false,
		0,
	)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctx))
}

func Test_ShardingLocalReader(t *testing.T) {
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

	testutil2.CompactBlocks(t, 0, taeEngine.GetDB(), databaseName, schema, false)

	{

		txn, _ := taeEngine.StartTxn()
		database, _ := txn.GetDatabase(databaseName)
		rel, _ := database.GetRelationByName(schema.Name)

		iter := rel.MakeObjectIt(false)
		iter.Next()
		blkId := iter.GetObject().GetMeta().(*catalog2.ObjectEntry).AsCommonID()

		err := rel.RangeDelete(blkId, 0, 1, handle.DT_Normal)
		require.Nil(t, err)

		require.NoError(t, txn.Commit(context.Background()))

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
		var blkId *common.ID
		for iter.Next() {
			if !iter.GetObject().IsAppendable() {
				continue
			}
			blkId = iter.GetObject().GetMeta().(*catalog2.ObjectEntry).AsCommonID()
		}
		err := rel.RangeDelete(blkId, 0, 7, handle.DT_Normal)
		require.Nil(t, err)

		require.NoError(t, txn.Commit(context.Background()))

	}

	{
		//start to build sharding readers.
		_, rel, txn, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.NoError(t, err)

		relData, err := rel.Ranges(ctx, nil, 0)
		require.NoError(t, err)

		shardSvr := testutil.MockShardService()
		delegate, _ := disttae.MockTableDelegate(rel, shardSvr)
		num := 10
		rds, err := delegate.BuildShardingReaders(
			ctx,
			rel.GetProcess(),
			nil,
			relData,
			num,
			0,
			false,
			0,
		)
		require.NoError(t, err)

		rows := 0
		buildBatch := func() *batch.Batch {
			bat := batch.NewWithSize(1)
			for _, col := range schema.ColDefs {
				if col.Name == schema.ColDefs[primaryKeyIdx].Name {
					vec := vector.NewVec(col.Type)
					bat.Vecs[0] = vec
					bat.Attrs = []string{col.Name}
					break
				}
			}
			return bat
		}

		for _, r := range rds {
			for {
				bat := buildBatch()
				isEnd, err := r.Read(
					ctx,
					[]string{schema.ColDefs[primaryKeyIdx].Name},
					nil,
					mp,
					bat,
				)
				require.NoError(t, err)

				if isEnd {
					break
				}
				rows += int(bat.RowCount())
			}
		}

		require.Equal(t, 2, rows)

		err = txn.Commit(ctx)
		require.Nil(t, err)
	}

	//test set orderby
	shardingLRD := disttae.MockShardingLocalReader()
	shardingLRD.SetOrderBy(nil)
	shardingLRD.GetOrderBy()
	shardingLRD.SetFilterZM(nil)
}

func Test_SimpleReader(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	proc := testutil3.NewProcessWithMPool("", mp)
	pkType := types.T_int32.ToType()
	bat1 := engine_util.NewCNTombstoneBatch(
		&pkType,
	)
	defer bat1.Clean(mp)
	obj := types.NewObjectid()
	blk0 := types.NewBlockidWithObjectID(obj, 0)
	blk1 := types.NewBlockidWithObjectID(obj, 1)
	idx := int32(0)
	for i := 0; i < 10; i++ {
		vector.AppendFixed[int32](
			bat1.Vecs[1],
			idx,
			false,
			mp,
		)
		idx++
		rowid := types.NewRowid(blk0, uint32(i))
		vector.AppendFixed[types.Rowid](
			bat1.Vecs[0],
			*rowid,
			false,
			mp,
		)
	}
	for i := 0; i < 10; i++ {
		vector.AppendFixed[int32](
			bat1.Vecs[1],
			idx,
			false,
			mp,
		)
		idx++
		rowid := types.NewRowid(blk1, uint32(i))
		vector.AppendFixed[types.Rowid](
			bat1.Vecs[0],
			*rowid,
			false,
			mp,
		)
	}
	bat1.SetRowCount(bat1.Vecs[0].Length())

	w, err := colexec.NewS3TombstoneWriter()
	require.NoError(t, err)
	defer w.Free(mp)
	w.StashBatch(proc, bat1)
	_, stats, err := w.SortAndSync(proc)
	require.NoError(t, err)
	require.Equal(t, uint32(20), stats.Rows())
	t.Logf("stats: %s", stats.String())

	fs, err := fileservice.Get[fileservice.FileService](proc.GetFileService(), defines.SharedFileServiceName)
	require.NoError(t, err)

	r := disttae.SimpleTombstoneObjectReader(
		context.Background(), fs, &stats, timestamp.Timestamp{},
		disttae.WithColumns(
			[]uint16{0, 1},
			[]types.Type{objectio.RowidType, pkType},
		),
	)
	blockio.Start("")
	defer blockio.Stop("")
	bat2 := engine_util.NewCNTombstoneBatch(
		&pkType,
	)
	defer bat2.Clean(mp)
	done, err := r.Read(context.Background(), bat1.Attrs, nil, mp, bat2)
	require.NoError(t, err)
	require.False(t, done)
	require.Equal(t, 20, bat2.RowCount())
	pks := vector.MustFixedColWithTypeCheck[int32](bat2.Vecs[1])
	require.Equal(t, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, pks)
	rowids1 := vector.MustFixedColWithTypeCheck[types.Rowid](bat1.Vecs[0])
	rowids2 := vector.MustFixedColWithTypeCheck[types.Rowid](bat2.Vecs[0])
	for i := 0; i < bat1.RowCount(); i++ {
		require.Equal(t, rowids1[i], rowids2[i])
	}

	done, err = r.Read(context.Background(), bat1.Attrs, nil, mp, bat2)
	require.NoError(t, err)
	require.True(t, done)

	r = disttae.SimpleMultiObjectsReader(
		context.Background(), fs,
		[]objectio.ObjectStats{stats, stats}, timestamp.Timestamp{},
		disttae.WithColumns(
			[]uint16{0, 1},
			[]types.Type{objectio.RowidType, pkType},
		),
		disttae.WithTombstone(),
	)

	done, err = r.Read(context.Background(), bat1.Attrs, nil, mp, bat2)
	require.NoError(t, err)
	require.False(t, done)
	require.Equal(t, 20, bat2.RowCount())

	pks = vector.MustFixedColWithTypeCheck[int32](bat2.Vecs[1])
	require.Equal(t, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, pks)
	rowids2 = vector.MustFixedColWithTypeCheck[types.Rowid](bat2.Vecs[0])
	for i := 0; i < bat1.RowCount(); i++ {
		require.Equal(t, rowids1[i], rowids2[i])
	}

	done, err = r.Read(context.Background(), bat1.Attrs, nil, mp, bat2)
	require.NoError(t, err)
	require.False(t, done)
	require.Equal(t, 20, bat2.RowCount())

	pks = vector.MustFixedColWithTypeCheck[int32](bat2.Vecs[1])
	require.Equal(t, []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, pks)
	rowids2 = vector.MustFixedColWithTypeCheck[types.Rowid](bat2.Vecs[0])
	for i := 0; i < bat1.RowCount(); i++ {
		require.Equal(t, rowids1[i], rowids2[i])
	}

	done, err = r.Read(context.Background(), bat1.Attrs, nil, mp, bat2)
	require.NoError(t, err)
	require.True(t, done)
}
