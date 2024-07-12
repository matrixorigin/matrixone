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

	err = disttaeEngine.Engine.TryToSubscribeTable(ctx, rel.ID(), database.GetID())
	require.Nil(t, err)

	// check partition state without flush
	{
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, rel.ID(), database.GetID())
		require.Nil(t, err)

		fmt.Println(stats.String())

		expect := testutil.PartitionStateStats{
			DataObjectsVisible:   testutil.PObjectStats{},
			DataObjectsInvisible: testutil.PObjectStats{},
			InmemRows:            testutil.PInmemRowsStats{VisibleCnt: int(batchRows * cnt)},
			CheckpointCnt:        0,
		}

		require.Equal(t, expect, stats.Summary())
	}

	// flush all aobj into one nobj
	testutil2.CompactBlocks(t, accountId, taeEngine.GetDB(), database.GetName(), schema, false)
	// check again after flush
	{
		stats, err := disttaeEngine.GetPartitionStateStats(ctx, rel.ID(), database.GetID())
		require.Nil(t, err)

		fmt.Println(stats.String())

		expect := testutil.PartitionStateStats{
			DataObjectsVisible: testutil.PObjectStats{
				ObjCnt: 1, BlkCnt: int(expectBlkCnt), RowCnt: int(batchRows * cnt),
			},
			DataObjectsInvisible: testutil.PObjectStats{
				ObjCnt: int(expectObjCnt), BlkCnt: int(expectBlkCnt), RowCnt: int(batchRows * cnt),
			},
			InmemRows:     testutil.PInmemRowsStats{},
			CheckpointCnt: 0,
		}

		require.Equal(t, expect, stats.Summary())
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

	// delete to generate delta loc
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

	err = disttaeEngine.Engine.TryToSubscribeTable(ctx, rel.ID(), database.GetID())
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
