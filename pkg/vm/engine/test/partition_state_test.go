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
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	testutil2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
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
		_, err = rel.CreateObject()
		assert.Nil(t, err)
	}
	{
		txn, _ := taeEngine.GetDB().StartTxn(nil)
		database, _ = txn.GetDatabase(databaseName)
		rel, _ = database.GetRelationByName(schema.Name)
		objIt := rel.MakeObjectIt()
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

	err = disttaeEngine.SubscribeTable(ctx, rel.ID(), database.GetID(), false)
	require.Nil(t, err)

	// check partition state without flush
	{
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, rel.ID(), database.GetID())
		require.Nil(t, err)

		fmt.Println(stats.String())

		require.Equal(t, int(batchRows*cnt), stats.InmemRows.VisibleCnt)
		require.Equal(t, 0, stats.DataObjectsVisible.ObjCnt)
	}

	// flush all aobj into one nobj
	testutil2.CompactBlocks(t, accountId, taeEngine.GetDB(), database.GetName(), schema, false)
	// check again after flush
	{
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, rel.ID(), database.GetID())
		require.Nil(t, err)

		fmt.Println(stats.String())

		require.Equal(t, int(batchRows*cnt), stats.DataObjectsVisible.RowCnt)
		require.Equal(t, 0, stats.InmemRows.VisibleCnt)
		require.Equal(t, 1, stats.DataObjectsVisible.ObjCnt)
		require.Equal(t, int(expectBlkCnt), stats.DataObjectsVisible.BlkCnt)
	}
}

func Test_Bug_CheckpointInsertObjectOverwrittenMergeDeletedObject(t *testing.T) {
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
	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, accountId)
	disttaeEngine, taeEngine, rpcAgent, _ := testutil.CreateEngines(ctx, opts, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	schema := catalog.MockSchemaAll(2, 0)
	schema.Name = tableName
	schema.BlockMaxRows = 20
	schema.ObjectMaxBlocks = 2
	taeEngine.BindSchema(schema)

	rowsCnt := 40
	bat := catalog.MockBatch(schema, rowsCnt)

	var err error
	{
		txn, _ = taeEngine.GetDB().StartTxn(nil)
		database, err = txn.CreateDatabase(databaseName, "", "")
		require.Nil(t, err)

		rel, err = database.CreateRelation(schema)
		require.Nil(t, err)

		err = rel.Append(ctx, bat)
		require.Nil(t, err)

		err = txn.Commit(context.Background())
		require.Nil(t, err)
	}

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

	err = disttaeEngine.SubscribeTable(ctx, rel.ID(), database.GetID(), false)
	require.Nil(t, err)

	// check partition state without consume ckp
	{
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, rel.ID(), database.GetID())
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

		stats, err := disttaeEngine.GetPartitionStateStats(ctx, rel.ID(), database.GetID())
		require.Nil(t, err)

		fmt.Println(stats.String())

		// should only have one object
		require.Equal(t, 1, stats.DataObjectsVisible.ObjCnt)
		require.Equal(t, rowsCnt, stats.DataObjectsVisible.RowCnt)
	}

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
	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, accountId)
	disttaeEngine, taeEngine, rpcAgent, _ := testutil.CreateEngines(ctx, opts, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	// one object, two blocks

	schema := catalog.MockSchemaAll(3, 2)
	schema.Name = tableName
	schema.BlockMaxRows = 20
	schema.ObjectMaxBlocks = 2
	taeEngine.BindSchema(schema)

	rowsCnt := 40
	bat := catalog.MockBatch(schema, rowsCnt)

	var err error
	{
		txn, _ = taeEngine.GetDB().StartTxn(nil)
		database, err = txn.CreateDatabase(databaseName, "", "")
		require.Nil(t, err)

		rel, err = database.CreateRelation(schema)
		require.Nil(t, err)

		err = rel.Append(ctx, bat)
		require.Nil(t, err)

		err = txn.Commit(context.Background())
		require.Nil(t, err)
	}

	err = disttaeEngine.SubscribeTable(ctx, rel.ID(), database.GetID(), false)
	require.Nil(t, err)

	{
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, rel.ID(), database.GetID())
		require.Nil(t, err)

		fmt.Println(stats.String(), stats.Details.DirtyBlocks)
		require.Equal(t, 40, stats.InmemRows.VisibleCnt)
		require.Equal(t, 2, stats.InmemRows.VisibleDistinctBlockCnt)
	}

	{
		// flush all aobj into one nobj
		testutil2.CompactBlocks(t, accountId, taeEngine.GetDB(), databaseName, schema, false)

		stats, err := disttaeEngine.GetPartitionStateStats(ctx, rel.ID(), database.GetID())
		require.Nil(t, err)

		fmt.Println(stats.String(), stats.Details.DirtyBlocks)
		require.Equal(t, 0, stats.InmemRows.VisibleCnt)
		require.Equal(t, 1, stats.DataObjectsVisible.ObjCnt)
		require.Equal(t, 2, stats.DataObjectsVisible.BlkCnt)
		require.Equal(t, 40, stats.DataObjectsVisible.RowCnt)
	}

	{
		txn, _ = taeEngine.GetDB().StartTxn(nil)
		database, _ = txn.GetDatabase(databaseName)
		rel, _ = database.GetRelationByName(schema.Name)

		iter := rel.MakeObjectIt()
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
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, rel.ID(), database.GetID())
		require.Nil(t, err)

		fmt.Println(stats.String(), stats.Details.DirtyBlocks)
		require.Equal(t, 3, stats.InmemRows.InvisibleCnt)
	}

	// push dela loc to cn and gc the in-mem deletes
	testutil2.CompactBlocks(t, accountId, taeEngine.GetDB(), databaseName, schema, false)

	{
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, rel.ID(), database.GetID())
		require.Nil(t, err)

		fmt.Println(stats.String(), stats.Details.DirtyBlocks)
		require.Equal(t, 0, stats.InmemRows.InvisibleCnt)
		require.Equal(t, 0, len(stats.Details.DirtyBlocks))
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
	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, accountId)
	disttaeEngine, taeEngine, rpcAgent, _ := testutil.CreateEngines(ctx, opts, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	// one object, two blocks

	schema := catalog.MockSchemaAll(3, 2)
	schema.Name = tableName
	schema.BlockMaxRows = 20
	schema.ObjectMaxBlocks = 2
	taeEngine.BindSchema(schema)

	rowsCnt := 40
	bat := catalog.MockBatch(schema, rowsCnt)

	var err error
	{
		txn, _ = taeEngine.GetDB().StartTxn(nil)
		database, err = txn.CreateDatabase(databaseName, "", "")
		require.Nil(t, err)

		rel, err = database.CreateRelation(schema)
		require.Nil(t, err)

		err = rel.Append(ctx, bat)
		require.Nil(t, err)

		err = txn.Commit(context.Background())
		require.Nil(t, err)
	}

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
		database, _ = txn.GetDatabase(databaseName)
		rel, _ = database.GetRelationByName(schema.Name)

		iter := rel.MakeObjectIt()
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
	err = disttaeEngine.SubscribeTable(ctx, rel.ID(), database.GetID(), false)
	require.Nil(t, err)

	{
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, rel.ID(), database.GetID())
		require.Nil(t, err)

		// before consume the ckp, the object in partition state expect to be empty
		for idx := range stats.Details.DataObjectList.Visible {
			require.Equal(t, uint32(0), stats.Details.DataObjectList.Visible[idx].Rows())
		}

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

		stats, err := disttaeEngine.GetPartitionStateStats(ctx, rel.ID(), database.GetID())
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
	ctx := context.WithValue(context.Background(), defines.TenantIDKey{}, accountId)
	disttaeEngine, taeEngine, rpcAgent, _ := testutil.CreateEngines(ctx, opts, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()

	// one object, two blocks

	schema := catalog.MockSchemaAll(3, 2)
	schema.Name = tableName
	schema.BlockMaxRows = 20
	schema.ObjectMaxBlocks = 2
	taeEngine.BindSchema(schema)

	rowsCnt := 40
	bat := catalog.MockBatch(schema, rowsCnt)
	bats := bat.Split(2)

	var err error
	{
		txn, _ = taeEngine.GetDB().StartTxn(nil)
		database, err = txn.CreateDatabase(databaseName, "", "")
		require.Nil(t, err)

		rel, err = database.CreateRelation(schema)
		require.Nil(t, err)

		err = rel.Append(ctx, bats[0])
		require.Nil(t, err)

		err = txn.Commit(context.Background())
		require.Nil(t, err)

		testutil2.CompactBlocks(t, accountId, taeEngine.GetDB(), databaseName, schema, false)

		txn, _ = taeEngine.GetDB().StartTxn(nil)
		database, err = txn.GetDatabase(databaseName)
		require.Nil(t, err)

		rel, err = database.GetRelationByName(schema.Name)
		require.Nil(t, err)

		err = rel.Append(ctx, bats[1])
		require.Nil(t, err)

		err = txn.Commit(context.Background())
		require.Nil(t, err)
	}

	checkSubscribed := func() {
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, rel.ID(), database.GetID())
		require.Nil(t, err)
		require.Equal(t, 20, stats.InmemRows.VisibleCnt)
		require.Equal(t, 1, stats.DataObjectsInvisible.ObjCnt)
		require.Equal(t, 20, stats.DataObjectsInvisible.RowCnt)
	}

	checkUnSubscribed := func() {
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, rel.ID(), database.GetID())
		require.Nil(t, err)
		require.Equal(t, 0, stats.InmemRows.VisibleCnt)
		require.Equal(t, 0, stats.DataObjectsInvisible.ObjCnt)
		require.Equal(t, 0, stats.DataObjectsInvisible.RowCnt)
	}

	err = disttaeEngine.SubscribeTable(ctx, rel.ID(), database.GetID(), true)
	require.Nil(t, err)

	checkSubscribed()

	try := 10
	ticker := time.NewTicker(100 * time.Millisecond)
	for range ticker.C {
		err = disttaeEngine.Engine.UnsubscribeTable(ctx, rel.ID(), database.GetID())
		require.Nil(t, err)

		checkUnSubscribed()

		err = disttaeEngine.SubscribeTable(ctx, rel.ID(), database.GetID(), true)
		require.Nil(t, err)

		checkSubscribed()

		if try--; try <= 0 {
			break
		}
	}
}
