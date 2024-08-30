// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func TestReplayCatalog1(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	testutils.EnsureNoLeak(t)
	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	schemas := make([]*catalog.Schema, 4)
	for i := range schemas {
		schemas[i] = catalog.MockSchema(2, 0)
	}

	txn, _ := tae.StartTxn(nil)
	_, err := txn.CreateDatabase("db", "", "")
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))
	createTable := func(schema *catalog.Schema, wg *sync.WaitGroup, forceCkp bool) func() {
		return func() {
			defer wg.Done()
			txn, _ := tae.StartTxn(nil)
			db, err := txn.GetDatabase("db")
			assert.Nil(t, err)
			_, err = db.CreateRelation(schema)
			assert.Nil(t, err)
			assert.Nil(t, txn.Commit(context.Background()))
			txn, _ = tae.StartTxn(nil)
			db, err = txn.GetDatabase("db")
			assert.Nil(t, err)
			rel, err := db.GetRelationByName(schema.Name)
			assert.Nil(t, err)
			objCnt := rand.Intn(5) + 1
			for i := 0; i < objCnt; i++ {
				stats := objectio.NewObjectStatsWithObjectID(objectio.NewObjectid(), false, false, false)
				obj, err := rel.CreateNonAppendableObject(false, &objectio.CreateObjOpt{Stats: stats})
				testutil.MockObjectStats(t, obj)
				assert.Nil(t, err)
				objMeta := obj.GetMeta().(*catalog.ObjectEntry)
				baseNode := objMeta.GetLatestNode().ObjectMVCCNode
				err = objectio.SetObjectStatsSize(&baseNode.ObjectStats, 1)
				assert.Nil(t, err)
				err = objectio.SetObjectStatsRowCnt(&baseNode.ObjectStats, 1)
				assert.Nil(t, err)
				assert.False(t, baseNode.IsEmpty())
				assert.Nil(t, err)
			}
			assert.Nil(t, txn.Commit(context.Background()))
			if forceCkp || rand.Intn(100) > 80 {
				err := tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.Now(), false)
				assert.NoError(t, err)
			}
		}
	}

	var wg sync.WaitGroup
	pool, err := ants.NewPool(1)
	assert.Nil(t, err)
	defer pool.Release()
	for i, schema := range schemas {
		wg.Add(1)
		ckp := false
		if i == len(schemas)/2 {
			ckp = true
		}
		err := pool.Submit(createTable(schema, &wg, ckp))
		assert.Nil(t, err)
	}
	wg.Wait()
	logutil.Info(tae.Catalog.SimplePPString(common.PPL1))
	t.Logf("GetPenddingLSNCnt: %d", tae.Runtime.Scheduler.GetPenddingLSNCnt())
	t.Logf("GetCheckpointed: %d", tae.Runtime.Scheduler.GetCheckpointedLSN())
	tae.Close()

	tae2, err := db.Open(ctx, tae.Dir, nil)
	assert.Nil(t, err)
	defer tae2.Close()

	c := tae2.Catalog
	defer c.Close()

	// logutil.Info(c.SimplePPString(common.PPL1))
	// t.Logf("GetCatalogCheckpointed: %v", tae.Catalog.GetCheckpointed())
	// t.Logf("GetCatalogCheckpointed2: %v", c.GetCheckpointed())
	// assert.Equal(t, tae.Catalog.GetCheckpointed(), c.GetCheckpointed())
}

func TestReplayCatalog2(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	schema := catalog.MockSchema(2, 0)
	schema2 := catalog.MockSchema(2, 0)
	txn, _ := tae.StartTxn(nil)
	_, err := txn.CreateDatabase("db2", "", "")
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	txn, _ = tae.StartTxn(nil)
	e, err := txn.CreateDatabase("db", "", "")
	assert.Nil(t, err)
	rel, err := e.CreateRelation(schema)
	assert.Nil(t, err)
	obj, err := rel.CreateNonAppendableObject(false, nil)
	testutil.MockObjectStats(t, obj)
	assert.Nil(t, err)
	objMeta := obj.GetMeta().(*catalog.ObjectEntry)
	baseNode := objMeta.GetLatestNode().ObjectMVCCNode
	err = objectio.SetObjectStatsSize(&baseNode.ObjectStats, 1)
	assert.Nil(t, err)
	err = objectio.SetObjectStatsRowCnt(&baseNode.ObjectStats, 1)
	assert.Nil(t, err)
	assert.False(t, baseNode.IsEmpty())
	assert.Nil(t, err)
	_, err = e.CreateRelation(schema2)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	txn, _ = tae.StartTxn(nil)
	_, err = txn.DropDatabase("db2")
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	txn, _ = tae.StartTxn(nil)
	e, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	_, err = e.DropRelationByName(schema2.Name)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	txn, _ = tae.StartTxn(nil)
	e, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	_, err = e.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	txn, _ = tae.StartTxn(nil)
	e, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = e.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	_, err = rel.CreateObject(false)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	err = tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.Now(), false)
	assert.NoError(t, err)
	tae.Close()

	tae2, err := db.Open(ctx, tae.Dir, nil)
	assert.Nil(t, err)
	defer tae2.Close()

	c := tae2.Catalog
	defer c.Close()

	// t.Log(c.SimplePPString(common.PPL1))
	// t.Logf("GetCatalogCheckpointed: %v", tae.Catalog.GetCheckpointed())
	// t.Logf("GetCatalogCheckpointed2: %v", c.GetCheckpointed())
	// assert.Equal(t, tae.Catalog.GetCheckpointed(), c.GetCheckpointed())
}

func TestReplayCatalog3(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	schema := catalog.MockSchema(2, 0)
	schema2 := catalog.MockSchema(2, 0)
	txn, _ := tae.StartTxn(nil)
	_, err := txn.CreateDatabase("db2", "", "")
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	txn, _ = tae.StartTxn(nil)
	e, err := txn.CreateDatabase("db", "", "")
	assert.Nil(t, err)
	rel, err := e.CreateRelation(schema)
	assert.Nil(t, err)
	obj, err := rel.CreateNonAppendableObject(false, nil)
	testutil.MockObjectStats(t, obj)
	assert.Nil(t, err)
	_, err = e.CreateRelation(schema2)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	txn, _ = tae.StartTxn(nil)
	_, err = txn.DropDatabase("db2")
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	txn, _ = tae.StartTxn(nil)
	e, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	_, err = e.DropRelationByName(schema2.Name)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	txn, _ = tae.StartTxn(nil)
	e, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	_, err = e.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	txn, _ = tae.StartTxn(nil)
	e, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = e.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	obj, err = rel.CreateObject(false)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	txn, _ = tae.StartTxn(nil)
	e, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = e.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	err = rel.SoftDeleteObject(obj.GetID(), false)
	assert.NoError(t, err)
	obj, err = rel.GetObject(obj.GetID(), false)
	assert.Nil(t, err)
	testutil.MockObjectStats(t, obj)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	tae.Close()

	tae2, err := db.Open(ctx, tae.Dir, nil)
	assert.Nil(t, err)
	defer tae2.Close()

	c := tae2.Catalog
	defer c.Close()

	// t.Log(c.SimplePPString(common.PPL1))
	// t.Logf("GetCatalogCheckpointed: %v", tae.Catalog.GetCheckpointed())
	// t.Logf("GetCatalogCheckpointed2: %v", c.GetCheckpointed())
	// assert.Equal(t, tae.Catalog.GetCheckpointed(), c.GetCheckpointed())
}

// catalog and data not checkpoint
// catalog not softdelete
func TestReplay1(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	schema := catalog.MockSchema(2, 1)
	schema.BlockMaxRows = 1000
	schema.ObjectMaxBlocks = 2
	txn, _ := tae.StartTxn(nil)
	assert.Nil(t, txn.Commit(context.Background()))

	txn, _ = tae.StartTxn(nil)
	e, err := txn.CreateDatabase("db", "", "")
	assert.Nil(t, err)
	rel, err := e.CreateRelation(schema)
	assert.Nil(t, err)
	_, err = rel.CreateObject(false)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))
	logutil.Infof("%d,%d", txn.GetStartTS(), txn.GetCommitTS())

	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	tae.Close()

	tae2, err := db.Open(ctx, tae.Dir, nil)
	assert.Nil(t, err)
	c := tae2.Catalog
	t.Log(c.SimplePPString(common.PPL1))

	bat := catalog.MockBatch(schema, 10000)
	defer bat.Close()
	txn, _ = tae2.StartTxn(nil)
	e, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = e.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	err = rel.Append(context.Background(), bat)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	txn, _ = tae2.StartTxn(nil)
	e, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = e.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, uint64(10000), rel.GetMeta().(*catalog.TableEntry).GetRows())
	filter := handle.NewEQFilter(int32(5))
	err = rel.UpdateByFilter(context.Background(), filter, uint16(0), int32(33), false)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	txn, _ = tae2.StartTxn(nil)
	e, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = e.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, uint64(10000), rel.GetMeta().(*catalog.TableEntry).GetRows())
	filter.Val = int32(6)
	err = rel.DeleteByFilter(context.Background(), filter)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	t.Log(c.SimplePPString(common.PPL1))
	c.Close()
	tae2.Close()

	tae3, err := db.Open(ctx, tae.Dir, nil)
	assert.Nil(t, err)
	c3 := tae3.Catalog
	t.Log(c3.SimplePPString(common.PPL1))

	txn, _ = tae3.StartTxn(nil)
	e, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = e.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, uint64(9999), rel.GetMeta().(*catalog.TableEntry).GetRows())
	filter.Val = int32(5)
	val, _, err := rel.GetValueByFilter(context.Background(), filter, 0)
	assert.Nil(t, err)
	assert.Equal(t, int32(33), val)

	filter.Val = int32(6)
	_, _, err = rel.GetValueByFilter(context.Background(), filter, 0)
	assert.NotNil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	c3.Close()
	tae3.Close()
}

// 1. Create db and tbl, append data   [1]
// 2. Update and delete.               [2]
// 3. Delete first blk                 [3]
// replay (catalog and data not ckp, catalog softdelete)
// check 1. blk not exist, 2. id and row of data
// 1. Checkpoint catalog               [ckp 3, partial 1]
// 2. Append                           [4]
// replay (catalog ckp, data not ckp)
// check id and row of data
// 1. Checkpoint data and catalog      [ckp all]
// replay
// TODO check id and row of data
func TestReplay2(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	schema := catalog.MockSchema(2, 1)
	schema.BlockMaxRows = 1000
	schema.ObjectMaxBlocks = 2
	bat := catalog.MockBatch(schema, 10000)
	defer bat.Close()
	bats := bat.Split(2)

	txn, err := tae.StartTxn(nil)
	assert.Nil(t, err)
	e, err := txn.CreateDatabase("db", "", "")
	assert.Nil(t, err)
	rel, err := e.CreateRelation(schema)
	assert.Nil(t, err)
	err = rel.Append(context.Background(), bats[0])
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	txn, err = tae.StartTxn(nil)
	assert.Nil(t, err)
	e, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = e.GetRelationByName(schema.Name)
	assert.Nil(t, err)

	filter := handle.NewEQFilter(int32(1500))
	id, row, err := rel.GetByFilter(context.Background(), filter)
	assert.Nil(t, err)
	err = rel.UpdateByFilter(context.Background(), filter, uint16(0), int32(33), false)
	assert.Nil(t, err)

	err = rel.RangeDelete(id, row+1, row+100, handle.DT_Normal)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	txn, err = tae.StartTxn(nil)
	assert.Nil(t, err)
	e, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = e.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	blk := testutil.GetOneBlockMeta(rel)
	obj, err := rel.GetObject(blk.ID(), false)
	assert.Nil(t, err)
	err = rel.SoftDeleteObject(obj.GetID(), false)
	assert.NoError(t, err)
	obj, err = rel.GetObject(obj.GetID(), false)
	assert.Nil(t, err)
	testutil.MockObjectStats(t, obj)
	assert.Nil(t, err)
	objMeta := obj.GetMeta().(*catalog.ObjectEntry)
	baseNode := objMeta.GetLatestNode().ObjectMVCCNode
	err = objectio.SetObjectStatsSize(&baseNode.ObjectStats, 1)
	assert.Nil(t, err)
	err = objectio.SetObjectStatsRowCnt(&baseNode.ObjectStats, 1)
	assert.Nil(t, err)
	assert.False(t, baseNode.IsEmpty())
	assert.Nil(t, txn.Commit(context.Background()))

	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	tae.Close()
	//prevTs := tae.TxnMgr.TsAlloc.Get()

	tae2, err := db.Open(ctx, tae.Dir, nil)
	assert.Nil(t, err)
	t.Log(tae2.Catalog.SimplePPString(common.PPL1))

	//currTs := tae2.TxnMgr.TsAlloc.Get()
	//assert.True(t, currTs.GreaterEq(prevTs))

	txn, err = tae2.StartTxn(nil)
	assert.Nil(t, err)
	e, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = e.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	objEntry, err := rel.GetMeta().(*catalog.TableEntry).GetObjectByID(obj.GetID(), false)
	assert.Nil(t, err)
	assert.True(t, objEntry.HasDropCommitted())

	val, _, err := rel.GetValueByFilter(context.Background(), filter, 0)
	assert.Nil(t, err)
	assert.Equal(t, int32(33), val)
	_, _, err = rel.GetValue(id, row, 0, false)
	assert.NotNil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	err = tae2.BGCheckpointRunner.ForceFlush(tae2.TxnMgr.Now(), context.Background(), time.Second*10)
	assert.NoError(t, err)
	err = tae2.BGCheckpointRunner.ForceIncrementalCheckpoint(tae2.TxnMgr.Now(), false)
	assert.NoError(t, err)

	txn, err = tae2.StartTxn(nil)
	assert.Nil(t, err)
	e, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = e.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	err = rel.Append(context.Background(), bats[1])
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	t.Log(tae2.Catalog.SimplePPString(common.PPL1))
	tae2.Close()

	tae3, err := db.Open(ctx, tae.Dir, nil)
	assert.Nil(t, err)
	t.Log(tae3.Catalog.SimplePPString(common.PPL1))

	txn, err = tae3.StartTxn(nil)
	assert.Nil(t, err)
	e, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = e.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	_, err = rel.GetMeta().(*catalog.TableEntry).GetObjectByID(obj.GetID(), false)
	assert.Nil(t, err)
	val, _, err = rel.GetValueByFilter(context.Background(), filter, 0)
	assert.Nil(t, err)
	assert.Equal(t, int32(33), val)
	_, _, err = rel.GetValue(id, row, 0, false)
	assert.NotNil(t, err)
	assert.Nil(t, txn.Commit(context.Background()))

	tae3.Close()

	tae4, err := db.Open(ctx, tae.Dir, nil)
	assert.NoError(t, err)
	tae4.Close()
}

// update, delete and append in one txn
// 1. Create db and tbl and {Append, Update, Delete} for 100 times in a same txn.
// 2. Append, Update, Delete each in one txn
// replay
// check rows
// 1. Ckp
// TODO check rows
func TestReplay3(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.NewTestEngine(ctx, ModuleName, t, nil)
	defer tae.Close()
	schema := catalog.MockSchema(2, 1)
	schema.BlockMaxRows = 1000
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, 1)
	defer bat.Close()
	v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(0)
	filter := handle.NewEQFilter(v)

	txn, err := tae.StartTxn(nil)
	assert.Nil(t, err)
	db, err := txn.CreateDatabase("db", "", "")
	assert.Nil(t, err)
	tbl, err := db.CreateRelation(schema)
	assert.Nil(t, err)
	err = tbl.Append(context.Background(), bat)
	assert.Nil(t, err)
	for i := 0; i < 100; i++ {
		err = tbl.UpdateByFilter(context.Background(), filter, 0, int32(33), false)
		assert.NoError(t, err)
		err = tbl.Append(context.Background(), bat)
		assert.Error(t, err)
		err = tbl.DeleteByFilter(context.Background(), filter)
		assert.NoError(t, err)
		err = tbl.Append(context.Background(), bat)
		assert.NoError(t, err)
	}
	assert.NoError(t, txn.Commit(context.Background()))

	for i := 0; i < 10; i++ {
		txn, rel := tae.GetRelation()
		err = rel.UpdateByFilter(context.Background(), filter, 0, int32(33), false)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))

		txn, rel = tae.GetRelation()
		blkID, row, err := rel.GetByFilter(context.Background(), filter)
		assert.NoError(t, err)
		err = rel.RangeDelete(blkID, row, row, handle.DT_Normal)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))

		txn, rel = tae.GetRelation()
		err = rel.Append(context.Background(), bat)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}

	tae.Restart(ctx)

	txn, rel := tae.GetRelation()
	assert.Equal(t, uint64(1), rel.GetMeta().(*catalog.TableEntry).GetRows())
	assert.NoError(t, txn.Commit(context.Background()))

	txn, _ = tae.GetRelation()
	err = tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.Now(), false)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
}

// append, delete, compact, mergeblocks, ckp
// 1. Create db and tbl and Append and Delete
// 2. Append and Delete
// replay
// check rows
/* TODO
   1. Ckp
   replay and check rows
   1. compact
   replay and check rows */
func TestReplayTableRows(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	tae := testutil.InitTestDB(ctx, ModuleName, t, nil)
	schema := catalog.MockSchema(2, 1)
	schema.BlockMaxRows = 1000
	schema.ObjectMaxBlocks = 2
	bat := catalog.MockBatch(schema, 4800)
	defer bat.Close()
	bats := bat.Split(3)
	rows := uint64(0)

	txn, err := tae.StartTxn(nil)
	assert.Nil(t, err)
	e, err := txn.CreateDatabase("db", "", "")
	assert.Nil(t, err)
	tbl, err := e.CreateRelation(schema)
	assert.Nil(t, err)
	err = tbl.Append(context.Background(), bats[0])
	assert.Nil(t, err)
	rows += 1600
	blkID := testutil.GetOneObject(tbl).Fingerprint()
	err = tbl.RangeDelete(blkID, 0, 99, handle.DT_Normal)
	assert.Nil(t, err)
	rows -= 100
	assert.Nil(t, txn.Commit(context.Background()))

	txn, err = tae.StartTxn(nil)
	assert.Nil(t, err)
	e, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	tbl, err = e.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	err = tbl.Append(context.Background(), bats[1])
	assert.Nil(t, err)
	rows += 1600
	blkID = testutil.GetOneObject(tbl).Fingerprint()
	err = tbl.RangeDelete(blkID, 0, 99, handle.DT_Normal)
	assert.Nil(t, err)
	rows -= 100
	assert.Nil(t, txn.Commit(context.Background()))

	tae.Close()

	tae2, err := db.Open(ctx, tae.Dir, nil)
	assert.Nil(t, err)

	txn, err = tae2.StartTxn(nil)
	assert.Nil(t, err)
	e, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	tbl, err = e.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, rows, tbl.GetMeta().(*catalog.TableEntry).GetRows())
	assert.Nil(t, txn.Commit(context.Background()))

	err = tae2.Close()
	assert.Nil(t, err)

	tae3, err := db.Open(ctx, tae.Dir, nil)
	assert.Nil(t, err)

	txn, err = tae3.StartTxn(nil)
	assert.Nil(t, err)
	assert.Nil(t, err)
	e, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	tbl, err = e.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, rows, tbl.GetMeta().(*catalog.TableEntry).GetRows())
	assert.Nil(t, txn.Commit(context.Background()))

	// worker := ops.NewOpWorker("xx")
	// worker.Start()
	// txn, err = tae3.StartTxn(nil)
	// assert.Nil(t, err)
	// db, err = txn.GetDatabase("db")
	// assert.Nil(t, err)
	// tbl, err = db.GetRelationByName(schema.Name)
	// assert.Nil(t, err)
	// blkIterator := tbl.MakeBlockIt()
	// blks := make([]*catalog.BlockEntry, 0)
	// for blkIterator.Valid() {
	// 	blk := blkIterator.GetBlock().GetMeta().(*catalog.BlockEntry)
	// 	blks = append(blks, blk)
	// 	blkIterator.Next()
	// }
	// for _, blk := range blks {
	// 	ctx := &tasks.Context{Waitable: true}
	// 	task, err := jobs.NewCompactBlockTask(ctx, txn, blk, tae3.Scheduler)
	// 	assert.Nil(t, err)
	// 	worker.SendOp(task)
	// 	err = task.WaitDone()
	// 	assert.Nil(t, err)
	// }
	// assert.Nil(t, txn.Commit(context.Background()))
	// worker.Stop()

	err = tae3.Close()
	assert.Nil(t, err)

	// tae4, err := Open(tae.Dir, nil)
	// assert.Nil(t, err)

	// txn, err = tae4.StartTxn(nil)
	// assert.Nil(t, err)
	// assert.Nil(t, err)
	// db, err = txn.GetDatabase("db")
	// assert.Nil(t, err)
	// tbl, err = db.GetRelationByName(schema.Name)
	// assert.Nil(t, err)
	// assert.Equal(t, rows, tbl.GetMeta().(*catalog.TableEntry).GetRows())
	// assert.Nil(t, txn.Commit(context.Background()))

	// err = tae4.Close()
	// assert.Nil(t, err)
}

// Testing Steps
func TestReplay4(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.InitTestDB(ctx, ModuleName, t, opts)

	schema := catalog.MockSchemaAll(18, 16)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*uint32(schema.ObjectMaxBlocks+1)+1))
	defer bat.Close()
	bats := bat.Split(4)

	testutil.CreateRelationAndAppend(t, 0, tae, testutil.DefaultTestDB, schema, bats[0], true)
	txn, rel := testutil.GetDefaultRelation(t, tae, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, bats[0].Length(), false)
	assert.NoError(t, txn.Commit(context.Background()))

	_ = tae.Close()

	tae2, err := db.Open(ctx, tae.Dir, nil)
	assert.NoError(t, err)

	txn, rel = testutil.GetDefaultRelation(t, tae2, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, bats[0].Length(), false)
	err = rel.Append(context.Background(), bats[1])
	assert.NoError(t, err)
	testutil.CheckAllColRowsByScan(t, rel, bats[0].Length()+bats[1].Length(), false)
	assert.NoError(t, txn.Commit(context.Background()))

	testutil.CompactBlocks(t, 0, tae2, testutil.DefaultTestDB, schema, false)
	txn, rel = testutil.GetDefaultRelation(t, tae2, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, bats[0].Length()+bats[1].Length(), false)
	err = rel.Append(context.Background(), bats[2])
	assert.NoError(t, err)
	testutil.CheckAllColRowsByScan(t, rel,
		bats[0].Length()+bats[1].Length()+bats[2].Length(), false)
	assert.NoError(t, txn.Commit(context.Background()))

	testutil.CompactBlocks(t, 0, tae2, testutil.DefaultTestDB, schema, false)

	txn, rel = testutil.GetDefaultRelation(t, tae2, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, testutil.LenOfBats(bats[0:3]), false)
	assert.NoError(t, txn.Commit(context.Background()))

	testutil.MergeBlocks(t, 0, tae2, testutil.DefaultTestDB, schema, false)

	txn, rel = testutil.GetDefaultRelation(t, tae2, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, testutil.LenOfBats(bats[0:3]), false)
	err = rel.Append(context.Background(), bats[3])
	assert.NoError(t, err)
	testutil.CheckAllColRowsByScan(t, rel, bat.Length(), false)
	assert.NoError(t, txn.Commit(context.Background()))
	t.Log(tae2.Catalog.SimplePPString(common.PPL1))

	tae2.Close()

	tae3, err := db.Open(ctx, tae.Dir, nil)
	assert.NoError(t, err)
	defer tae3.Close()
}

// Testing Steps
func TestReplay5(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.InitTestDB(ctx, ModuleName, t, opts)

	schema := catalog.MockSchemaAll(18, 16)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*uint32(schema.ObjectMaxBlocks+1)+1))
	defer bat.Close()
	bats := bat.Split(8)

	testutil.CreateRelationAndAppend(t, 0, tae, testutil.DefaultTestDB, schema, bats[0], true)
	txn, rel := testutil.GetDefaultRelation(t, tae, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, bats[0].Length(), false)
	assert.NoError(t, txn.Commit(context.Background()))

	_ = tae.Close()
	tae, err := db.Open(ctx, tae.Dir, nil)
	assert.NoError(t, err)

	txn, rel = testutil.GetDefaultRelation(t, tae, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, bats[0].Length(), false)
	err = rel.Append(context.Background(), bats[0])
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	err = rel.Append(context.Background(), bats[1])
	assert.NoError(t, err)
	testutil.CheckAllColRowsByScan(t, rel, testutil.LenOfBats(bats[0:2]), false)
	assert.NoError(t, txn.Commit(context.Background()))
	t.Logf("LSN=%d", txn.GetLSN())

	_ = tae.Close()
	tae, err = db.Open(ctx, tae.Dir, nil)
	assert.NoError(t, err)

	txn, rel = testutil.GetDefaultRelation(t, tae, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, testutil.LenOfBats(bats[0:2]), false)
	err = rel.Append(context.Background(), bats[0])
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	err = rel.Append(context.Background(), bats[1])
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	err = rel.Append(context.Background(), bats[2])
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bats[3])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	testutil.CompactBlocks(t, 0, tae, testutil.DefaultTestDB, schema, false)
	err = tae.BGCheckpointRunner.ForceFlushWithInterval(tae.TxnMgr.Now(), context.Background(), time.Second*2, time.Millisecond*10)
	assert.NoError(t, err)
	err = tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.Now(), false)
	assert.NoError(t, err)
	lsn := tae.BGCheckpointRunner.MaxLSNInRange(tae.TxnMgr.Now())
	entry, err := tae.Wal.RangeCheckpoint(1, lsn)
	assert.NoError(t, err)
	err = entry.WaitDone()
	assert.NoError(t, err)
	txn, rel = testutil.GetDefaultRelation(t, tae, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, testutil.LenOfBats(bats[:4]), false)
	assert.NoError(t, txn.Commit(context.Background()))

	_ = tae.Close()
	tae, err = db.Open(ctx, tae.Dir, nil)
	assert.NoError(t, err)

	txn, rel = testutil.GetDefaultRelation(t, tae, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, testutil.LenOfBats(bats[:4]), false)
	err = rel.Append(context.Background(), bats[3])
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	for _, b := range bats[4:8] {
		err = rel.Append(context.Background(), b)
		assert.NoError(t, err)
	}
	assert.NoError(t, txn.Commit(context.Background()))
	testutil.CompactBlocks(t, 0, tae, testutil.DefaultTestDB, schema, false)
	err = tae.BGCheckpointRunner.ForceFlushWithInterval(tae.TxnMgr.Now(), context.Background(), time.Second*2, time.Millisecond*10)
	assert.NoError(t, err)
	err = tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.Now(), false)
	assert.NoError(t, err)
	lsn = tae.BGCheckpointRunner.MaxLSNInRange(tae.TxnMgr.Now())
	entry, err = tae.Wal.RangeCheckpoint(1, lsn)
	assert.NoError(t, err)
	assert.NoError(t, entry.WaitDone())

	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	testutil.PrintCheckpointStats(t, tae)
	_ = tae.Close()
	tae, err = db.Open(ctx, tae.Dir, nil)
	assert.NoError(t, err)

	txn, rel = testutil.GetDefaultRelation(t, tae, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, testutil.LenOfBats(bats[:8]), false)
	err = rel.Append(context.Background(), bats[0])
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	assert.NoError(t, txn.Commit(context.Background()))
	// testutils.WaitExpect(3000, func() bool {
	// 	return tae.Wal.GetCheckpointed() == tae.Wal.GetCurrSeqNum()/2
	// })
	// testutil.PrintCheckpointStats(t, tae)
	// assert.Equal(t, tae.Wal.GetCurrSeqNum()/2, tae.Wal.GetCheckpointed())
	testutil.MergeBlocks(t, 0, tae, testutil.DefaultTestDB, schema, false)

	_ = tae.Close()
	tae, err = db.Open(ctx, tae.Dir, nil)
	assert.NoError(t, err)
	defer tae.Close()
	txn, rel = testutil.GetDefaultRelation(t, tae, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, testutil.LenOfBats(bats[:8]), false)
	err = rel.Append(context.Background(), bats[0])
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	assert.NoError(t, txn.Commit(context.Background()))

	err = tae.BGCheckpointRunner.ForceFlushWithInterval(tae.TxnMgr.Now(), context.Background(), time.Second*2, time.Millisecond*10)
	assert.NoError(t, err)
	err = tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.Now(), false)
	assert.NoError(t, err)
	lsn = tae.BGCheckpointRunner.MaxLSNInRange(tae.TxnMgr.Now())
	entry, err = tae.Wal.RangeCheckpoint(1, lsn)
	assert.NoError(t, err)
	assert.NoError(t, entry.WaitDone())
	testutils.WaitExpect(1000, func() bool {
		return tae.Runtime.Scheduler.GetPenddingLSNCnt() == 0
	})
	testutil.PrintCheckpointStats(t, tae)
	assert.Equal(t, tae.Wal.GetCurrSeqNum(), tae.Wal.GetCheckpointed())
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}

func TestReplay6(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.InitTestDB(ctx, ModuleName, t, opts)
	schema := catalog.MockSchemaAll(18, 15)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*10-1))
	defer bat.Close()
	bats := bat.Split(4)

	testutil.CreateRelationAndAppend(t, 0, tae, testutil.DefaultTestDB, schema, bats[0], true)

	_ = tae.Close()
	tae, err := db.Open(ctx, tae.Dir, opts)
	assert.NoError(t, err)

	txn, rel := testutil.GetDefaultRelation(t, tae, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, testutil.LenOfBats(bats[0:1]), false)
	err = rel.Append(context.Background(), bats[0])
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	err = rel.Append(context.Background(), bats[1])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	testutil.CompactBlocks(t, 0, tae, testutil.DefaultTestDB, schema, false)

	_ = tae.Close()
	tae, err = db.Open(ctx, tae.Dir, opts)
	assert.NoError(t, err)

	txn, rel = testutil.GetDefaultRelation(t, tae, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, testutil.LenOfBats(bats[0:2]), false)
	err = rel.Append(context.Background(), bats[0])
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	err = rel.Append(context.Background(), bats[2])
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bats[3])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
	testutil.CompactBlocks(t, 0, tae, testutil.DefaultTestDB, schema, false)
	testutil.MergeBlocks(t, 0, tae, testutil.DefaultTestDB, schema, false)
	err = tae.BGCheckpointRunner.ForceFlushWithInterval(tae.TxnMgr.Now(), context.Background(), time.Second*2, time.Millisecond*10)
	assert.NoError(t, err)
	err = tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.Now(), false)
	assert.NoError(t, err)

	_ = tae.Close()
	tae, err = db.Open(ctx, tae.Dir, opts)
	assert.NoError(t, err)

	txn, rel = testutil.GetDefaultRelation(t, tae, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, testutil.LenOfBats(bats[0:4]), false)
	assert.NoError(t, txn.Commit(context.Background()))
	testutil.PrintCheckpointStats(t, tae)
	_ = tae.Close()
}

func TestReplay7(t *testing.T) {
	defer testutils.AfterTest(t)()
	t.Skip(any("This case crashes occasionally, is being fixed, skip it for now"))
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := testutil.InitTestDB(ctx, ModuleName, t, opts)
	schema := catalog.MockSchemaAll(18, 14)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 5

	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*15+1))
	defer bat.Close()
	testutil.CreateRelationAndAppend(t, 0, tae, testutil.DefaultTestDB, schema, bat, true)
	testutil.CompactBlocks(t, 0, tae, testutil.DefaultTestDB, schema, true)
	testutil.MergeBlocks(t, 0, tae, testutil.DefaultTestDB, schema, true)
	time.Sleep(time.Millisecond * 100)

	_ = tae.Close()
	tae, err := db.Open(ctx, tae.Dir, opts)
	assert.NoError(t, err)
	defer tae.Close()
	// t.Log(tae.Catalog.SimplePPString(common.PPL1))
	txn, rel := testutil.GetDefaultRelation(t, tae, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, bat.Length(), false)
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestReplay8(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(18, 13)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)

	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*3+1))
	defer bat.Close()
	bats := bat.Split(4)

	tae.CreateRelAndAppend(bats[0], true)
	txn, rel := tae.GetRelation()
	v := testutil.GetSingleSortKeyValue(bats[0], schema, 2)
	filter := handle.NewEQFilter(v)
	err := rel.DeleteByFilter(context.Background(), filter)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = tae.GetRelation()
	window := bat.CloneWindow(2, 1)
	defer window.Close()
	err = rel.Append(context.Background(), window)
	assert.NoError(t, err)
	_ = txn.Rollback(context.Background())

	tae.Restart(ctx)

	// Check the total rows by scan
	txn, rel = tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel, bats[0].Length()-1, true)
	err = rel.Append(context.Background(), bats[0])
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	assert.NoError(t, txn.Commit(context.Background()))

	// Try to append the delete row and then rollback
	txn, rel = tae.GetRelation()
	err = rel.Append(context.Background(), window)
	assert.NoError(t, err)
	_ = txn.Rollback(context.Background())

	txn, rel = testutil.GetDefaultRelation(t, tae.DB, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, bats[0].Length()-1, true)
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = testutil.GetDefaultRelation(t, tae.DB, schema.Name)
	err = rel.Append(context.Background(), window)
	assert.NoError(t, err)
	_ = txn.Rollback(context.Background())

	tae.Restart(ctx)

	txn, rel = testutil.GetDefaultRelation(t, tae.DB, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, bats[0].Length()-1, true)
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = testutil.GetDefaultRelation(t, tae.DB, schema.Name)
	err = rel.Append(context.Background(), window)
	assert.NoError(t, err)
	tuple3 := bat.Window(3, 1)
	err = rel.Append(context.Background(), tuple3)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	assert.NoError(t, txn.Commit(context.Background()))

	txn, rel = testutil.GetDefaultRelation(t, tae.DB, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, bats[0].Length(), true)
	assert.NoError(t, txn.Commit(context.Background()))

	tae.Restart(ctx)

	txn, rel = testutil.GetDefaultRelation(t, tae.DB, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, bats[0].Length(), true)
	err = rel.Append(context.Background(), window)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	err = rel.Append(context.Background(), bats[1])
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bats[2])
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bats[3])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	tae.CompactBlocks(false)
	tae.Restart(ctx)

	txn, rel = tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel, bat.Length(), true)
	err = rel.Append(context.Background(), window)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))

	v0_5 := testutil.GetSingleSortKeyValue(bats[0], schema, 5)
	filter = handle.NewEQFilter(v0_5)
	err = rel.DeleteByFilter(context.Background(), filter)
	assert.NoError(t, err)
	v1_5 := testutil.GetSingleSortKeyValue(bats[1], schema, 5)
	filter = handle.NewEQFilter(v1_5)
	err = rel.DeleteByFilter(context.Background(), filter)
	assert.NoError(t, err)
	v2_5 := testutil.GetSingleSortKeyValue(bats[2], schema, 5)
	filter = handle.NewEQFilter(v2_5)
	err = rel.DeleteByFilter(context.Background(), filter)
	assert.NoError(t, err)
	v3_2 := testutil.GetSingleSortKeyValue(bats[3], schema, 2)
	filter = handle.NewEQFilter(v3_2)
	err = rel.DeleteByFilter(context.Background(), filter)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
	// t.Log(tae.Catalog.SimplePPString(common.PPL1))
	err = tae.BGCheckpointRunner.ForceFlushWithInterval(tae.TxnMgr.Now(), context.Background(), time.Second*2, time.Millisecond*10)
	assert.NoError(t, err)

	tae.Restart(ctx)

	txn, rel = tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel, bat.Length()-4, true)
	tuple0_5 := bat.Window(5, 1)
	err = rel.Append(context.Background(), tuple0_5)
	assert.NoError(t, err)
	tuple1_5 := bats[1].Window(5, 1)
	err = rel.Append(context.Background(), tuple1_5)
	assert.NoError(t, err)
	tuple2_5 := bats[2].Window(5, 1)
	err = rel.Append(context.Background(), tuple2_5)
	assert.NoError(t, err)
	tuple3_2 := bats[3].Window(2, 1)
	err = rel.Append(context.Background(), tuple3_2)
	assert.NoError(t, err)
	testutil.CheckAllColRowsByScan(t, rel, bat.Length(), true)
	_ = txn.Rollback(context.Background())

	tae.CompactBlocks(false)
	err = tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.Now(), false)
	assert.NoError(t, err)
	tae.Restart(ctx)

	txn, rel = tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel, bat.Length()-4, true)
	err = rel.Append(context.Background(), tuple0_5)
	assert.NoError(t, err)
	err = rel.Append(context.Background(), tuple1_5)
	assert.NoError(t, err)
	err = rel.Append(context.Background(), tuple2_5)
	assert.NoError(t, err)
	err = rel.Append(context.Background(), tuple3_2)
	assert.NoError(t, err)
	testutil.CheckAllColRowsByScan(t, rel, bat.Length(), true)
	_ = txn.Rollback(context.Background())
}

func TestReplay9(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(18, 3)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 2
	tae.BindSchema(schema)
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*3+2))
	defer bat.Close()
	bats := bat.Split(4)

	tae.CreateRelAndAppend(bats[0], true)
	txn, rel := tae.GetRelation()
	testutil.CheckAllColRowsByScan(t, rel, bats[0].Length(), false)
	v := testutil.GetSingleSortKeyValue(bats[0], schema, 2)
	filter := handle.NewEQFilter(v)
	err := rel.UpdateByFilter(context.Background(), filter, 2, int32(999), false)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	tae.Restart(ctx)
	txn, rel = tae.GetRelation()
	actv, _, err := rel.GetValueByFilter(context.Background(), filter, 2)
	assert.NoError(t, err)
	assert.Equal(t, int32(999), actv)
	testutil.CheckAllColRowsByScan(t, rel, bats[0].Length(), true)
	assert.NoError(t, txn.Commit(context.Background()))

	tae.CompactBlocks(false)

	tae.Restart(ctx)
	txn, rel = tae.GetRelation()
	_, _, err = rel.GetByFilter(context.Background(), filter)
	assert.NoError(t, err)
	actv, _, err = rel.GetValueByFilter(context.Background(), filter, 2)
	assert.NoError(t, err)
	assert.Equal(t, int32(999), actv)
	testutil.CheckAllColRowsByScan(t, rel, bats[0].Length(), true)
	err = rel.Append(context.Background(), bats[1])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
	tae.CompactBlocks(false)

	txn, rel = tae.GetRelation()
	v2 := testutil.GetSingleSortKeyValue(bats[0], schema, 4)
	filter2 := handle.NewEQFilter(v2)
	err = rel.UpdateByFilter(context.Background(), filter2, 1, int16(199), false)
	assert.NoError(t, err)
	actv, _, err = rel.GetValueByFilter(context.Background(), filter2, 1)
	assert.NoError(t, err)
	assert.Equal(t, int16(199), actv)
	assert.NoError(t, txn.Commit(context.Background()))

	tae.Restart(ctx)

	txn, rel = tae.GetRelation()
	actv, _, err = rel.GetValueByFilter(context.Background(), filter2, 1)
	assert.NoError(t, err)
	assert.Equal(t, int16(199), actv)
	err = rel.Append(context.Background(), bats[2])
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bats[3])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	tae.CompactBlocks(false)
	tae.MergeBlocks(false)

	txn, rel = tae.GetRelation()
	v3 := testutil.GetSingleSortKeyValue(bats[1], schema, 3)
	filter3 := handle.NewEQFilter(v3)
	err = rel.UpdateByFilter(context.Background(), filter3, 5, uint16(88), false)
	assert.NoError(t, err)
	actv, _, err = rel.GetValueByFilter(context.Background(), filter3, 5)
	assert.NoError(t, err)
	assert.Equal(t, uint16(88), actv)
	assert.NoError(t, txn.Commit(context.Background()))

	tae.Restart(ctx)

	txn, rel = tae.GetRelation()
	actv, _, err = rel.GetValueByFilter(context.Background(), filter, 2)
	assert.NoError(t, err)
	assert.Equal(t, int32(999), actv)
	actv, _, err = rel.GetValueByFilter(context.Background(), filter2, 1)
	assert.NoError(t, err)
	assert.Equal(t, int16(199), actv)
	actv, _, err = rel.GetValueByFilter(context.Background(), filter3, 5)
	assert.NoError(t, err)
	assert.Equal(t, uint16(88), actv)
	assert.NoError(t, txn.Commit(context.Background()))
}

func TestReplay10(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := testutil.InitTestDB(ctx, ModuleName, t, opts)
	schema := catalog.MockSchemaAll(3, 2)
	schema.BlockMaxRows = 10
	schema.ObjectMaxBlocks = 5
	expr := &plan.Expr{}
	exprbuf, err := expr.Marshal()
	assert.NoError(t, err)

	schema.ColDefs[1].Default, _ = types.Encode(&plan.Default{
		NullAbility: false,
		Expr:        &plan.Expr{},
	})
	schema.ColDefs[1].OnUpdate, _ = types.Encode(&plan.OnUpdate{
		Expr: &plan.Expr{},
	})
	schema.ColDefs[2].Default, _ = types.Encode(&plan.Default{
		NullAbility: true,
		Expr:        nil,
	})
	schema.ColDefs[2].OnUpdate, _ = types.Encode(&plan.OnUpdate{
		Expr: nil,
	})

	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows))
	defer bat.Close()
	testutil.CreateRelationAndAppend(t, 0, tae, testutil.DefaultTestDB, schema, bat, true)
	time.Sleep(time.Millisecond * 100)

	_ = tae.Close()
	tae, err = db.Open(ctx, tae.Dir, opts)
	assert.NoError(t, err)
	defer tae.Close()
	// t.Log(tae.Catalog.SimplePPString(common.PPL1))
	txn, rel := testutil.GetDefaultRelation(t, tae, schema.Name)
	testutil.CheckAllColRowsByScan(t, rel, bat.Length(), false)
	assert.NoError(t, txn.Commit(context.Background()))
	schema1 := rel.Schema(false).(*catalog.Schema)

	d1 := &plan.Default{}
	assert.NoError(t, types.Decode(schema1.ColDefs[1].Default, d1))
	buf, _ := d1.Expr.Marshal()
	assert.Equal(t, exprbuf, buf)
	u1 := &plan.OnUpdate{}
	assert.NoError(t, types.Decode(schema1.ColDefs[1].OnUpdate, u1))
	buf, _ = u1.Expr.Marshal()
	assert.Equal(t, exprbuf, buf)

	d2 := &plan.Default{}
	assert.NoError(t, types.Decode(schema1.ColDefs[2].Default, d2))
	assert.Nil(t, d2.Expr)
	u2 := &plan.OnUpdate{}
	assert.NoError(t, types.Decode(schema1.ColDefs[2].OnUpdate, u2))
	assert.Nil(t, u2.Expr)
	assert.True(t, d2.NullAbility)
}

// create db,tbl,obj,blk
// checkpoint
// softdelete obj
// checkpoint
// Restart
func TestReplaySnapshots(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	schema := catalog.MockSchemaAll(1, -1)

	txn, err := tae.StartTxn(nil)
	assert.NoError(t, err)
	db, err := txn.CreateDatabase("db", "", "")
	assert.NoError(t, err)
	rel, err := db.CreateRelation(schema)
	assert.NoError(t, err)
	obj, err := rel.CreateObject(false)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))

	err = tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.Now(), false)
	assert.NoError(t, err)

	txn, err = tae.StartTxn(nil)
	assert.NoError(t, err)
	db, err = txn.GetDatabase("db")
	assert.NoError(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.NoError(t, err)
	err = rel.SoftDeleteObject(obj.GetID(), false)
	assert.NoError(t, err)
	obj, err = rel.GetObject(obj.GetID(), false)
	assert.Nil(t, err)
	testutil.MockObjectStats(t, obj)
	assert.NoError(t, err)
	objMeta := obj.GetMeta().(*catalog.ObjectEntry)
	baseNode := objMeta.GetLatestNode().ObjectMVCCNode
	err = objectio.SetObjectStatsSize(&baseNode.ObjectStats, 1)
	assert.Nil(t, err)
	err = objectio.SetObjectStatsRowCnt(&baseNode.ObjectStats, 1)
	assert.Nil(t, err)
	assert.False(t, baseNode.IsEmpty())
	assert.NoError(t, txn.Commit(context.Background()))

	err = tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.Now(), false)
	assert.NoError(t, err)
	t.Log(tae.Catalog.SimplePPString(3))

	tae.Restart(ctx)
	t.Log(tae.Catalog.SimplePPString(3))

	tae.Close()
}

func TestReplayDatabaseEntry(t *testing.T) {
	defer testutils.AfterTest(t)()
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	datypStr := "datyp"
	createSqlStr := "createSql"

	txn, err := tae.StartTxn(nil)
	assert.NoError(t, err)
	db, err := txn.CreateDatabase("db", createSqlStr, datypStr)
	assert.NoError(t, err)
	dbID := db.GetID()
	assert.NoError(t, txn.Commit(context.Background()))

	tae.Restart(ctx)
	t.Log(tae.Catalog.SimplePPString(3))
	dbEntry, err := tae.Catalog.GetDatabaseByID(dbID)
	assert.NoError(t, err)
	assert.Equal(t, datypStr, dbEntry.GetDatType())
	assert.Equal(t, createSqlStr, dbEntry.GetCreateSql())

	err = tae.BGCheckpointRunner.ForceIncrementalCheckpoint(tae.TxnMgr.Now(), false)
	assert.NoError(t, err)

	tae.Restart(ctx)
	dbEntry, err = tae.Catalog.GetDatabaseByID(dbID)
	assert.NoError(t, err)
	assert.Equal(t, datypStr, dbEntry.GetDatType())
	assert.Equal(t, createSqlStr, dbEntry.GetCreateSql())
}
