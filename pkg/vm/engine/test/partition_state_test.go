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
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	testutil2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	ops "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Append(t *testing.T) {
	var (
		opts         testutil.TestOptions
		rel          handle.Relation
		database     handle.Database
		accountId    = uint32(0)
		tableName    = "test1"
		databaseName = "db1"
	)

	// make sure that disabled all auto ckp and flush
	opts.TaeEngineOptions = config.WithLongScanAndCKPOpts(nil)
	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, accountId)
	disttaeEngine, taeEngine, rpcAgent, _ := testutil.CreateEngines(ctx, opts, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	schema := catalog.MockSchema(3, 2)
	schema.Name = tableName
	schema.BlockMaxRows = 10000
	batchRows := schema.BlockMaxRows * 2 / 5
	cnt := uint32(20)
	bat := catalog.MockBatch(schema, int(batchRows*cnt))
	defer bat.Close()
	bats := bat.Split(20)

	{
		var err error
		txn, _ := taeEngine.GetDB().StartTxn(nil)
		database, err = txn.CreateDatabase(databaseName, "", "")
		assert.Nil(t, err)

		_, err = database.CreateRelation(schema)
		assert.Nil(t, err)

		err = txn.Commit(context.Background())
		assert.Nil(t, err)
	}

	var wg sync.WaitGroup
	now := time.Now()
	doAppend := func(b *containers.Batch) func() {
		return func() {
			defer wg.Done()

			var err error
			txn, _ := taeEngine.GetDB().StartTxn(nil)
			tmpDB, _ := txn.GetDatabase(databaseName)
			tmpRel, err := tmpDB.GetRelationByName(schema.Name)
			assert.Nil(t, err)
			err = tmpRel.Append(context.Background(), b)
			assert.Nil(t, err)
			err = txn.Commit(context.Background())
			assert.Nil(t, err)
		}
	}
	p, err := ants.NewPool(4)
	assert.Nil(t, err)
	defer p.Release()
	for _, toAppend := range bats {
		wg.Add(1)
		err = p.Submit(doAppend(toAppend))
		assert.Nil(t, err)
	}

	wg.Wait()

	t.Logf("Append takes: %s", time.Since(now))
	expectBlkCnt := (uint32(batchRows)*uint32(cnt)-1)/schema.BlockMaxRows + 1
	expectObjCnt := expectBlkCnt

	{
		txn, _ := taeEngine.GetDB().StartTxn(nil)
		database, _ = txn.GetDatabase(databaseName)
		rel, _ = database.GetRelationByName(schema.Name)
		_, err = rel.CreateObject(false)
		assert.Nil(t, err)
	}
	{
		txn, _ := taeEngine.GetDB().StartTxn(nil)
		database, _ = txn.GetDatabase(databaseName)
		rel, _ = database.GetRelationByName(schema.Name)
		objIt := rel.MakeObjectIt(false)
		objCnt := uint32(0)
		blkCnt := uint32(0)
		for objIt.Next() {
			objCnt++
			blkCnt += uint32(objIt.GetObject().BlkCnt())
		}
		objIt.Close()

		assert.Equal(t, expectObjCnt, objCnt)
		assert.Equal(t, expectBlkCnt, blkCnt)
	}

	t.Log(taeEngine.GetDB().Catalog.SimplePPString(common.PPL1))

	err = disttaeEngine.SubscribeTable(ctx, database.GetID(), rel.ID(), false)
	require.Nil(t, err)

	// check partition state without flush
	{
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, database.GetID(), rel.ID())
		require.Nil(t, err)

		fmt.Println(stats.String())

		require.Equal(t, int(batchRows*cnt), stats.InmemRows.VisibleCnt)
		require.Equal(t, 0, stats.DataObjectsVisible.ObjCnt)
	}

	// flush all aobj into one nobj
	testutil2.CompactBlocks(t, accountId, taeEngine.GetDB(), database.GetName(), schema, false)
	// check again after flush
	{
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, database.GetID(), rel.ID())
		require.Nil(t, err)

		fmt.Println(stats.String())

		require.Equal(t, int(batchRows*cnt), stats.DataObjectsVisible.RowCnt)
		require.Equal(t, 0, stats.InmemRows.VisibleCnt)
		require.Equal(t, 1, stats.DataObjectsVisible.ObjCnt)
		require.Equal(t, int(expectBlkCnt), stats.DataObjectsVisible.BlkCnt)
	}
}

func Test_Bug_CheckpointInsertObjectOverwrittenMergeDeletedObject(t *testing.T) {
	blockio.RunPipelineTest(
		func() {
			var (
				txn          txnif.AsyncTxn
				opts         testutil.TestOptions
				rel          handle.Relation
				database     handle.Database
				accountId    = uint32(0)
				tableName    = "test1"
				databaseName = "db1"
			)
			// make sure that disabled all auto ckp and flush
			opts.TaeEngineOptions = config.WithLongScanAndCKPOpts(nil)
			p := testutil.InitEnginePack(opts, t)
			defer p.Close()
			taeEngine := p.T

			// one object, two blocks

			schema := catalog.MockSchemaAll(3, 2)
			schema.Name = tableName
			schema.Comment = "rows:20;blks:2"
			schema.BlockMaxRows = 20
			schema.ObjectMaxBlocks = 2
			taeEngine.BindSchema(schema)

			rowsCnt := 40
			{
				dbBatch := catalog.MockBatch(schema, rowsCnt)
				defer dbBatch.Close()
				bat := containers.ToCNBatch(dbBatch)
				txnOp := p.StartCNTxn()
				_, handles := p.CreateDBAndTables(txnOp, databaseName, schema)
				tH := handles[0]
				require.Nil(t, tH.Write(p.Ctx, bat))
				require.Nil(t, txnOp.Commit(p.Ctx))
			}
			var err error
			disttaeEngine := p.D
			ctx := p.Ctx

			// checkpoint
			{
				// an obj recorded into ckp
				testutil2.CompactBlocks(t, accountId, taeEngine.GetDB(), databaseName, schema, false)
				txn, _ = taeEngine.GetDB().StartTxn(nil)
				ts := txn.GetStartTS()
				taeEngine.GetDB().ForceCheckpoint(ctx, ts.Next(), time.Second)
			}

			{
				txn, _ = taeEngine.GetDB().StartTxn(nil)
				database, err = txn.GetDatabase(databaseName)
				require.Nil(t, err)

				rel, err = database.GetRelationByName(tableName)
				require.Nil(t, err)

				// merge the obj into a new object
				// the obj recorded in the ckp has been deleted
				testutil2.CompactBlocks(t, accountId, taeEngine.GetDB(), databaseName, schema, false)
				testutil2.MergeBlocks(t, accountId, taeEngine.GetDB(), databaseName, schema, false)
			}

			err = disttaeEngine.SubscribeTable(ctx, database.GetID(), rel.ID(), false)
			require.Nil(t, err)

			// check partition state without consume ckp
			{
				stats, err := disttaeEngine.GetPartitionStateStats(ctx, database.GetID(), rel.ID())
				require.Nil(t, err)

				fmt.Println(stats.String())
				// should only have one object
				require.Equal(t, 1, stats.DataObjectsVisible.ObjCnt)
				require.Equal(t, rowsCnt, stats.DataObjectsVisible.RowCnt)
			}

			// consume ckp
			{
				txnOp, err := disttaeEngine.NewTxnOperator(ctx, disttaeEngine.Now())
				require.Nil(t, err)

				engineDB, err := disttaeEngine.Engine.Database(ctx, databaseName, txnOp)
				require.Nil(t, err)

				engineTbl, err := engineDB.Relation(ctx, tableName, nil)
				require.Nil(t, err)

				_, err = disttaeEngine.Engine.LazyLoadLatestCkp(ctx, engineTbl)
				require.Nil(t, err)

				stats, err := disttaeEngine.GetPartitionStateStats(ctx, database.GetID(), rel.ID())
				require.Nil(t, err)

				fmt.Println(stats.String())

				// should only have one object
				require.Equal(t, 1, stats.DataObjectsVisible.ObjCnt)
				require.Equal(t, rowsCnt, stats.DataObjectsVisible.RowCnt)
			}
		},
	)
}

// see PR#13644
// should remove the dirty block flag
func Test_Bug_MissCleanDirtyBlockFlag(t *testing.T) {
	var (
		txn          txnif.AsyncTxn
		opts         testutil.TestOptions
		rel          handle.Relation
		database     handle.Database
		accountId    = uint32(0)
		tableName    = "test1"
		databaseName = "db1"
	)

	// make sure that disabled all auto ckp and flush
	opts.TaeEngineOptions = config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(opts, t)
	defer p.Close()
	taeEngine := p.T

	// one object, two blocks

	schema := catalog.MockSchemaAll(3, 2)
	schema.Name = tableName
	schema.Comment = "rows:20;blks:2"
	schema.BlockMaxRows = 20
	schema.ObjectMaxBlocks = 2
	taeEngine.BindSchema(schema)

	rowsCnt := 40
	{
		dbBatch := catalog.MockBatch(schema, rowsCnt)
		defer dbBatch.Close()
		bat := containers.ToCNBatch(dbBatch)
		txnOp := p.StartCNTxn()
		_, handles := p.CreateDBAndTables(txnOp, databaseName, schema)
		tH := handles[0]
		require.Nil(t, tH.Write(p.Ctx, bat))
		require.Nil(t, txnOp.Commit(p.Ctx))
	}
	var err error
	disttaeEngine := p.D
	ctx := p.Ctx

	{
		txn, _ = taeEngine.GetDB().StartTxn(nil)
		database, _ = txn.GetDatabase(databaseName)
		rel, _ = database.GetRelationByName(schema.Name)
		txn.Commit(context.Background())
	}

	err = disttaeEngine.SubscribeTable(ctx, database.GetID(), rel.ID(), false)
	require.Nil(t, err)

	{
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, database.GetID(), rel.ID())
		require.Nil(t, err)

		fmt.Println(stats.String())
		require.Equal(t, 40, stats.InmemRows.VisibleCnt)
		require.Equal(t, 2, stats.InmemRows.VisibleDistinctBlockCnt)
	}

	{
		// flush all aobj into one nobj
		testutil2.CompactBlocks(t, accountId, taeEngine.GetDB(), databaseName, schema, false)

		stats, err := disttaeEngine.GetPartitionStateStats(ctx, database.GetID(), rel.ID())
		require.Nil(t, err)

		fmt.Println(stats.String())
		require.Equal(t, 0, stats.InmemRows.VisibleCnt)
		require.Equal(t, 1, stats.DataObjectsVisible.ObjCnt)
		require.Equal(t, 2, stats.DataObjectsVisible.BlkCnt)
		require.Equal(t, 40, stats.DataObjectsVisible.RowCnt)
	}

	{
		txn, _ = taeEngine.GetDB().StartTxn(nil)
		database, _ = txn.GetDatabase(databaseName)
		rel, _ = database.GetRelationByName(schema.Name)

		iter := rel.MakeObjectIt(false)
		iter.Next()
		blkId := iter.GetObject().GetMeta().(*catalog.ObjectEntry).AsCommonID()
		// delete one row on the 1st blk
		err = rel.RangeDelete(blkId, 0, 0, handle.DT_Normal)
		require.Nil(t, err)

		// delete two rows on the 2nd blk
		blkId.BlockID = *objectio.BuildObjectBlockid(iter.GetObject().GetMeta().(*catalog.ObjectEntry).ObjectName(), 1)
		err = rel.RangeDelete(blkId, 0, 1, handle.DT_Normal)
		assert.Nil(t, err)

		require.Nil(t, iter.Close())
		assert.Nil(t, txn.Commit(context.Background()))
	}

	// push deletes to cn
	{
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, database.GetID(), rel.ID())
		require.Nil(t, err)

		fmt.Println(stats.String())
		require.Equal(t, 3, stats.InmemRows.InvisibleCnt)
	}

	// push dela loc to cn and gc the in-mem deletes
	testutil2.CompactBlocks(t, accountId, taeEngine.GetDB(), databaseName, schema, false)

	{
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, database.GetID(), rel.ID())
		require.Nil(t, err)

		fmt.Println(stats.String())
		require.Equal(t, 0, stats.InmemRows.InvisibleCnt)
		require.Equal(t, 3, stats.TombstoneObjectsVisible.RowCnt)
	}
}

// see #PR17415
// consume and consume ckp should be an atomic operation
func Test_EmptyObjectStats(t *testing.T) {
	var (
		txn          txnif.AsyncTxn
		opts         testutil.TestOptions
		rel          handle.Relation
		database     handle.Database
		accountId    = uint32(0)
		tableName    = "test1"
		databaseName = "db1"
	)

	// make sure that disabled all auto ckp and flush
	opts.TaeEngineOptions = config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(opts, t)
	defer p.Close()
	taeEngine := p.T

	// one object, two blocks

	schema := catalog.MockSchemaAll(3, 2)
	schema.Name = tableName
	schema.Comment = "rows:20;blks:2"
	schema.BlockMaxRows = 20
	schema.ObjectMaxBlocks = 2
	taeEngine.BindSchema(schema)

	rowsCnt := 40

	{
		dbBatch := catalog.MockBatch(schema, rowsCnt)
		defer dbBatch.Close()
		bat := containers.ToCNBatch(dbBatch)
		txnOp := p.StartCNTxn()
		_, handles := p.CreateDBAndTables(txnOp, databaseName, schema)
		tH := handles[0]
		require.Nil(t, tH.Write(p.Ctx, bat))
		require.Nil(t, txnOp.Commit(p.Ctx))
	}

	// checkpoint
	{
		// an obj recorded into ckp
		testutil2.CompactBlocks(t, accountId, taeEngine.GetDB(), databaseName, schema, false)
		txn, _ = taeEngine.GetDB().StartTxn(nil)
		ts := txn.GetStartTS()
		taeEngine.GetDB().ForceCheckpoint(p.Ctx, ts.Next(), time.Second)
	}

	var err error
	{
		txn, _ = taeEngine.GetDB().StartTxn(nil)
		database, _ = txn.GetDatabase(databaseName)
		rel, _ = database.GetRelationByName(schema.Name)

		iter := rel.MakeObjectIt(false)
		iter.Next()
		blkId := iter.GetObject().GetMeta().(*catalog.ObjectEntry).AsCommonID()
		// delete one row on the 1st blk
		err = rel.RangeDelete(blkId, 0, 0, handle.DT_Normal)
		require.Nil(t, err)

		require.Nil(t, iter.Close())
		assert.Nil(t, txn.Commit(context.Background()))
	}

	// push dela loc to cn
	testutil2.CompactBlocks(t, accountId, taeEngine.GetDB(), databaseName, schema, false)
	disttaeEngine := p.D
	err = disttaeEngine.SubscribeTable(p.Ctx, database.GetID(), rel.ID(), false)
	require.Nil(t, err)

	{
		stats, err := disttaeEngine.GetPartitionStateStats(p.Ctx, database.GetID(), rel.ID())
		require.Nil(t, err)

		// before consume the ckp, the object in partition state expect to be empty
		for idx := range stats.Details.DataObjectList.Visible {
			require.Equal(t, uint32(0), stats.Details.DataObjectList.Visible[idx].Rows())
		}

	}

	// consume ckp
	{
		txnOp, err := disttaeEngine.NewTxnOperator(p.Ctx, disttaeEngine.Now())
		require.Nil(t, err)

		engineDB, err := disttaeEngine.Engine.Database(p.Ctx, databaseName, txnOp)
		require.Nil(t, err)

		engineTbl, err := engineDB.Relation(p.Ctx, tableName, nil)
		require.Nil(t, err)

		_, err = disttaeEngine.Engine.LazyLoadLatestCkp(p.Ctx, engineTbl)
		require.Nil(t, err)

		stats, err := disttaeEngine.GetPartitionStateStats(p.Ctx, database.GetID(), rel.ID())
		require.Nil(t, err)

		for idx := range stats.Details.DataObjectList.Visible {
			require.Equal(t, uint32(rowsCnt), stats.Details.DataObjectList.Visible[idx].Rows())
		}
	}
}

func Test_SubscribeUnsubscribeConsistency(t *testing.T) {
	var (
		txn          txnif.AsyncTxn
		opts         testutil.TestOptions
		rel          handle.Relation
		database     handle.Database
		accountId    = uint32(0)
		tableName    = "test1"
		databaseName = "db1"
	)

	// make sure that disabled all auto ckp and flush
	opts.TaeEngineOptions = config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(opts, t)
	defer p.Close()
	taeEngine := p.T

	// one object, two blocks

	schema := catalog.MockSchemaAll(3, 2)
	schema.Name = tableName
	schema.Comment = "rows:20;blks:2"
	schema.BlockMaxRows = 20
	schema.ObjectMaxBlocks = 2
	taeEngine.BindSchema(schema)

	rowsCnt := 40

	bat := catalog.MockBatch(schema, rowsCnt)
	defer bat.Close()
	bats := bat.Split(2)
	{
		txnOp := p.StartCNTxn()
		_, handles := p.CreateDBAndTables(txnOp, databaseName, schema)
		tH := handles[0]
		require.Nil(t, tH.Write(p.Ctx, containers.ToCNBatch(bats[0])))
		require.Nil(t, txnOp.Commit(p.Ctx))
		testutil2.CompactBlocks(t, accountId, taeEngine.GetDB(), databaseName, schema, false)
	}

	var err error
	{
		txn, _ = taeEngine.GetDB().StartTxn(nil)
		database, err = txn.GetDatabase(databaseName)
		require.Nil(t, err)
		rel, err = database.GetRelationByName(schema.Name)
		require.Nil(t, err)
		err = rel.Append(p.Ctx, bats[1])
		require.Nil(t, err)
		err = txn.Commit(context.Background())
		require.Nil(t, err)
	}
	disttaeEngine := p.D
	ctx := p.Ctx
	checkSubscribed := func() {
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, database.GetID(), rel.ID())
		require.Nil(t, err)
		require.Equal(t, 20, stats.InmemRows.VisibleCnt)
		require.Equal(t, 1, stats.DataObjectsInvisible.ObjCnt)
		require.Equal(t, 20, stats.DataObjectsInvisible.RowCnt)
	}

	checkUnSubscribed := func() {
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, database.GetID(), rel.ID())
		require.Nil(t, err)
		require.Equal(t, 0, stats.InmemRows.VisibleCnt)
		require.Equal(t, 0, stats.DataObjectsInvisible.ObjCnt)
		require.Equal(t, 0, stats.DataObjectsInvisible.RowCnt)
	}

	err = disttaeEngine.SubscribeTable(ctx, database.GetID(), rel.ID(), true)
	require.Nil(t, err)

	checkSubscribed()

	try := 10
	ticker := time.NewTicker(100 * time.Millisecond)
	for range ticker.C {
		err = disttaeEngine.Engine.UnsubscribeTable(ctx, database.GetID(), rel.ID())
		require.Nil(t, err)

		checkUnSubscribed()

		err = disttaeEngine.SubscribeTable(ctx, database.GetID(), rel.ID(), true)
		require.Nil(t, err)

		checkSubscribed()

		if try--; try <= 0 {
			break
		}
	}
}

// root case:
// the deletes in tombstone object will be skipped when reader apply deletes on the in-mem data.
func Test_Bug_DupEntryWhenGCInMemTombstones(t *testing.T) {
	var (
		opts         testutil.TestOptions
		tableName    = "test1"
		databaseName = "db1"
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	opts.TaeEngineOptions = config.WithLongScanAndCKPOpts(nil)
	p := testutil.InitEnginePack(opts, t)
	defer p.Close()

	schema := catalog.MockSchemaAll(3, 2)
	schema.Name = tableName
	schema.Comment = "rows:20;blks:2"
	schema.BlockMaxRows = 20
	schema.ObjectMaxBlocks = 2

	txnop := p.StartCNTxn()
	_, _ = p.CreateDBAndTable(txnop, databaseName, schema)
	require.NoError(t, txnop.Commit(ctx))

	txnop = p.StartCNTxn()
	v, ok := runtime.ServiceRuntime("").GetGlobalVariables(runtime.InternalSQLExecutor)
	require.True(t, ok)

	exec := v.(executor.SQLExecutor)

	// insert 4 rows
	{
		res, err := exec.Exec(p.Ctx,
			fmt.Sprintf("insert into `%s`.`%s` values(1,1,1),(2,2,2),(3,3,3),(4,4,4);",
				databaseName, tableName),
			executor.Options{}.
				WithTxn(txnop).
				WithWaitCommittedLogApplied())
		require.NoError(t, err)
		res.Close()
		require.NoError(t, txnop.Commit(ctx))
	}

	time.Sleep(time.Second)

	// delete row (1,1,1)
	{
		txnop = p.StartCNTxn()
		res, err := exec.Exec(p.Ctx,
			fmt.Sprintf("delete from `%s`.`%s` where `%s`=1;",
				databaseName, tableName, schema.GetPrimaryKey().Name),
			executor.Options{}.
				WithTxn(txnop).
				WithWaitCommittedLogApplied())

		require.NoError(t, err)
		res.Close()
		require.NoError(t, txnop.Commit(ctx))
	}

	// flush tombstone only
	{
		tnTxnop, err := p.T.GetDB().StartTxn(nil)
		require.NoError(t, err)

		dbHandle, err := tnTxnop.GetDatabase(databaseName)
		require.NoError(t, err)

		relHandle, err := dbHandle.GetRelationByName(tableName)
		require.NoError(t, err)

		it := relHandle.MakeObjectIt(true)
		it.Next()
		tombstone := it.GetObject().GetMeta().(*catalog.ObjectEntry)
		require.NoError(t, it.Close())

		worker := ops.NewOpWorker(context.Background(), "xx")
		worker.Start()
		defer worker.Stop()

		task1, err := jobs.NewFlushTableTailTask(
			tasks.WaitableCtx, tnTxnop, nil,
			[]*catalog.ObjectEntry{tombstone}, p.T.GetDB().Runtime)

		require.NoError(t, err)
		worker.SendOp(task1)
		err = task1.WaitDone(context.Background())
		require.NoError(t, err)
		require.NoError(t, tnTxnop.Commit(ctx))
	}

	time.Sleep(time.Second * 1)

	// check left rows
	{
		txnop = p.StartCNTxn()
		res, err := exec.Exec(p.Ctx,
			fmt.Sprintf("select * from `%s`.`%s` order by `%s` desc;",
				databaseName, tableName, schema.GetPrimaryKey().Name),
			executor.Options{}.
				WithTxn(txnop).
				WithWaitCommittedLogApplied())
		require.NoError(t, err)
		require.NoError(t, txnop.Commit(ctx))

		fmt.Println(common.MoBatchToString(res.Batches[0], 1000))
		require.Equal(t, res.Batches[0].RowCount(), 3)
		require.Equal(t, 0,
			slices.Compare(
				vector.MustFixedCol[int32](res.Batches[0].Vecs[schema.GetPrimaryKey().Idx]),
				[]int32{4, 3, 2}))

		res.Close()
	}

	// re-insert the deleted row (1,1,1)
	{
		txnop = p.StartCNTxn()
		res, err := exec.Exec(p.Ctx,
			fmt.Sprintf("insert into `%s`.`%s` values(1,1,1);",
				databaseName, tableName),
			executor.Options{}.
				WithTxn(txnop).
				WithWaitCommittedLogApplied())
		res.Close()
		require.NoError(t, err)
		require.NoError(t, txnop.Commit(ctx))
	}

	{
		txnop = p.StartCNTxn()
		res, err := exec.Exec(p.Ctx,
			fmt.Sprintf("select * from `%s`.`%s` order by `%s` desc;",
				databaseName, tableName, schema.GetPrimaryKey().Name),
			executor.Options{}.
				WithTxn(txnop).
				WithWaitCommittedLogApplied())
		require.NoError(t, err)
		require.NoError(t, txnop.Commit(ctx))

		fmt.Println(common.MoBatchToString(res.Batches[0], 1000))
		require.Equal(t, res.Batches[0].RowCount(), 4)
		require.Equal(t, 0,
			slices.Compare(
				vector.MustFixedCol[int32](res.Batches[0].Vecs[schema.GetPrimaryKey().Idx]),
				[]int32{4, 3, 2, 1}))
		res.Close()
	}
}
