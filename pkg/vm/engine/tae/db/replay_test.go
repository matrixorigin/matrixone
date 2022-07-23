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

package db

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func TestReplayCatalog1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	schemas := make([]*catalog.Schema, 4)
	for i := range schemas {
		schemas[i] = catalog.MockSchema(2, 0)
	}

	txn, _ := tae.StartTxn(nil)
	_, err := txn.CreateDatabase("db")
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	createTable := func(schema *catalog.Schema, wg *sync.WaitGroup, forceCkp bool) func() {
		return func() {
			defer wg.Done()
			txn, _ := tae.StartTxn(nil)
			db, err := txn.GetDatabase("db")
			assert.Nil(t, err)
			_, err = db.CreateRelation(schema)
			assert.Nil(t, err)
			assert.Nil(t, txn.Commit())
			txn, _ = tae.StartTxn(nil)
			db, err = txn.GetDatabase("db")
			assert.Nil(t, err)
			rel, err := db.GetRelationByName(schema.Name)
			assert.Nil(t, err)
			segCnt := rand.Intn(5) + 1
			for i := 0; i < segCnt; i++ {
				seg, err := rel.CreateSegment()
				assert.Nil(t, err)
				blkCnt := rand.Intn(5) + 1
				for j := 0; j < blkCnt; j++ {
					_, err = seg.CreateBlock()
					assert.Nil(t, err)
				}
			}
			assert.Nil(t, txn.Commit())
			if forceCkp || rand.Intn(100) > 80 {
				err := tae.Catalog.Checkpoint(tae.Scheduler.GetSafeTS())
				assert.Nil(t, err)
			}
		}
	}

	var wg sync.WaitGroup
	pool, err := ants.NewPool(1)
	assert.Nil(t, err)
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
	t.Logf("GetPenddingLSNCnt: %d", tae.Scheduler.GetPenddingLSNCnt())
	t.Logf("GetCheckpointed: %d", tae.Scheduler.GetCheckpointedLSN())
	// ckpTs := tae.Catalog.GetCheckpointed().MaxTS
	// ckpEntry := tae.Catalog.PrepareCheckpoint(0, ckpTs)
	tae.Close()

	tae2, err := Open(tae.Dir, nil)
	assert.Nil(t, err)
	defer tae2.Close()

	c := tae2.Catalog
	defer c.Close()

	logutil.Info(c.SimplePPString(common.PPL1))
	t.Logf("GetCatalogCheckpointed: %v", tae.Catalog.GetCheckpointed())
	t.Logf("GetCatalogCheckpointed2: %v", c.GetCheckpointed())
	assert.Equal(t, tae.Catalog.GetCheckpointed(), c.GetCheckpointed())
}

func TestReplayCatalog2(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	schema := catalog.MockSchema(2, 0)
	schema2 := catalog.MockSchema(2, 0)
	txn, _ := tae.StartTxn(nil)
	_, err := txn.CreateDatabase("db2")
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, _ = tae.StartTxn(nil)
	db, err := txn.CreateDatabase("db")
	assert.Nil(t, err)
	rel, err := db.CreateRelation(schema)
	assert.Nil(t, err)
	seg, err := rel.CreateSegment()
	assert.Nil(t, err)
	blk1, err := seg.CreateBlock()
	assert.Nil(t, err)
	blk1Meta := blk1.GetMeta().(*catalog.BlockEntry)
	_, err = seg.CreateBlock()
	assert.Nil(t, err)
	_, err = db.CreateRelation(schema2)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, _ = tae.StartTxn(nil)
	_, err = txn.DropDatabase("db2")
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, _ = tae.StartTxn(nil)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	_, err = db.DropRelationByName(schema2.Name)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, _ = tae.StartTxn(nil)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	seg, err = rel.GetSegment(blk1Meta.GetSegment().ID)
	assert.Nil(t, err)
	err = seg.SoftDeleteBlock(blk1Meta.ID)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	ts := txn.GetCommitTS()

	txn, _ = tae.StartTxn(nil)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	seg, err = rel.CreateSegment()
	assert.Nil(t, err)
	_, err = seg.CreateBlock()
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	err = tae.Catalog.Checkpoint(ts)
	assert.Nil(t, err)
	err = tae.Catalog.Checkpoint(tae.Scheduler.GetSafeTS())
	assert.Nil(t, err)
	tae.Close()

	tae2, err := Open(tae.Dir, nil)
	assert.Nil(t, err)
	defer tae2.Close()

	c := tae2.Catalog
	defer c.Close()

	t.Log(c.SimplePPString(common.PPL1))
	t.Logf("GetCatalogCheckpointed: %v", tae.Catalog.GetCheckpointed())
	t.Logf("GetCatalogCheckpointed2: %v", c.GetCheckpointed())
	assert.Equal(t, tae.Catalog.GetCheckpointed(), c.GetCheckpointed())
}

func TestReplayCatalog3(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	schema := catalog.MockSchema(2, 0)
	schema2 := catalog.MockSchema(2, 0)
	txn, _ := tae.StartTxn(nil)
	_, err := txn.CreateDatabase("db2")
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, _ = tae.StartTxn(nil)
	db, err := txn.CreateDatabase("db")
	assert.Nil(t, err)
	rel, err := db.CreateRelation(schema)
	assert.Nil(t, err)
	seg, err := rel.CreateSegment()
	assert.Nil(t, err)
	blk1, err := seg.CreateBlock()
	assert.Nil(t, err)
	blk1Meta := blk1.GetMeta().(*catalog.BlockEntry)
	_, err = seg.CreateBlock()
	assert.Nil(t, err)
	_, err = db.CreateRelation(schema2)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, _ = tae.StartTxn(nil)
	_, err = txn.DropDatabase("db2")
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, _ = tae.StartTxn(nil)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	_, err = db.DropRelationByName(schema2.Name)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, _ = tae.StartTxn(nil)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	seg, err = rel.GetSegment(blk1Meta.GetSegment().ID)
	assert.Nil(t, err)
	err = seg.SoftDeleteBlock(blk1Meta.ID)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, _ = tae.StartTxn(nil)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	seg, err = rel.CreateSegment()
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, _ = tae.StartTxn(nil)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	err = rel.SoftDeleteSegment(seg.GetID())
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	tae.Close()

	tae2, err := Open(tae.Dir, nil)
	assert.Nil(t, err)
	defer tae2.Close()

	c := tae2.Catalog
	defer c.Close()

	t.Log(c.SimplePPString(common.PPL1))
	t.Logf("GetCatalogCheckpointed: %v", tae.Catalog.GetCheckpointed())
	t.Logf("GetCatalogCheckpointed2: %v", c.GetCheckpointed())
	assert.Equal(t, tae.Catalog.GetCheckpointed(), c.GetCheckpointed())
}

// catalog and data not checkpoint
// catalog not softdelete
func TestReplay1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	schema := catalog.MockSchema(2, 1)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 2
	txn, _ := tae.StartTxn(nil)
	assert.Nil(t, txn.Commit())

	txn, _ = tae.StartTxn(nil)
	db, err := txn.CreateDatabase("db")
	assert.Nil(t, err)
	rel, err := db.CreateRelation(schema)
	assert.Nil(t, err)
	seg, err := rel.CreateSegment()
	assert.Nil(t, err)
	_, err = seg.CreateBlock()
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	logutil.Infof("%d,%d", txn.GetStartTS(), txn.GetCommitTS())

	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	tae.Close()

	tae2, err := Open(tae.Dir, nil)
	assert.Nil(t, err)
	c := tae2.Catalog
	t.Log(c.SimplePPString(common.PPL1))

	bat := catalog.MockBatch(schema, 10000)
	defer bat.Close()
	txn, _ = tae2.StartTxn(nil)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	err = rel.Append(bat)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, _ = tae2.StartTxn(nil)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, uint64(10000), rel.GetMeta().(*catalog.TableEntry).GetRows())
	filter := new(handle.Filter)
	filter.Op = handle.FilterEq
	filter.Val = int32(5)
	id, row, err := rel.GetByFilter(filter)
	assert.Nil(t, err)
	err = rel.Update(id, row-1, uint16(0), int32(33))
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, _ = tae2.StartTxn(nil)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, uint64(10000), rel.GetMeta().(*catalog.TableEntry).GetRows())
	err = rel.RangeDelete(id, row+1, row+1, handle.DT_Normal)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	t.Log(c.SimplePPString(common.PPL1))
	c.Close()
	tae2.Close()

	tae3, err := Open(tae.Dir, nil)
	assert.Nil(t, err)
	c3 := tae3.Catalog
	t.Log(c3.SimplePPString(common.PPL1))

	txn, _ = tae3.StartTxn(nil)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, uint64(9999), rel.GetMeta().(*catalog.TableEntry).GetRows())
	filter = new(handle.Filter)
	filter.Op = handle.FilterEq
	filter.Val = int32(5)
	id2, row2, err := rel.GetByFilter(filter)
	assert.Nil(t, err)
	assert.Equal(t, id.BlockID, id2.BlockID)
	assert.Equal(t, row, row2)

	val, err := rel.GetValue(id, row-1, 0)
	assert.Nil(t, err)
	assert.Equal(t, int32(33), val)

	_, err = rel.GetValue(id, row+1, 0)
	assert.NotNil(t, err)
	assert.Nil(t, txn.Commit())

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
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	schema := catalog.MockSchema(2, 1)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockBatch(schema, 10000)
	defer bat.Close()
	bats := bat.Split(2)

	txn, err := tae.StartTxn(nil)
	assert.Nil(t, err)
	db, err := txn.CreateDatabase("db")
	assert.Nil(t, err)
	rel, err := db.CreateRelation(schema)
	assert.Nil(t, err)
	err = rel.Append(bats[0])
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, err = tae.StartTxn(nil)
	assert.Nil(t, err)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	filter := new(handle.Filter)
	filter.Op = handle.FilterEq
	filter.Val = int32(1500)
	id, row, err := rel.GetByFilter(filter)
	assert.Nil(t, err)
	err = rel.Update(id, row-1, uint16(0), int32(33))
	assert.Nil(t, err)
	err = rel.RangeDelete(id, row+1, row+100, handle.DT_Normal)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, err = tae.StartTxn(nil)
	assert.Nil(t, err)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	blkIterator := rel.MakeBlockIt()
	blk := blkIterator.GetBlock().GetMeta().(*catalog.BlockEntry)
	seg, err := rel.GetSegment(blk.GetSegment().ID)
	assert.Nil(t, err)
	err = seg.SoftDeleteBlock(blk.ID)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	ts := txn.GetCommitTS()

	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	tae.Close()
	prevTs := tae.TxnMgr.TsAlloc.Get()

	tae2, err := Open(tae.Dir, nil)
	assert.Nil(t, err)
	t.Log(tae2.Catalog.SimplePPString(common.PPL1))
	currTs := tae2.TxnMgr.TsAlloc.Get()
	assert.Equal(t, prevTs, currTs)

	txn, err = tae2.StartTxn(nil)
	assert.Nil(t, err)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	seg, err = rel.GetSegment(seg.GetID())
	assert.Nil(t, err)
	blkh, err := seg.GetBlock(blk.ID)
	assert.Nil(t, err)
	assert.True(t, blkh.GetMeta().(*catalog.BlockEntry).IsDroppedCommitted())
	filter = new(handle.Filter)
	filter.Op = handle.FilterEq
	filter.Val = int32(1500)
	id2, row2, err := rel.GetByFilter(filter)
	assert.Nil(t, err)
	assert.Equal(t, id.BlockID, id2.BlockID)
	assert.Equal(t, row, row2)
	val, err := rel.GetValue(id, row-1, 0)
	assert.Nil(t, err)
	assert.Equal(t, int32(33), val)
	_, err = rel.GetValue(id, row+1, 0)
	assert.NotNil(t, err)
	assert.Nil(t, txn.Commit())

	err = tae2.Catalog.Checkpoint(ts)
	assert.Nil(t, err)

	txn, err = tae2.StartTxn(nil)
	assert.Nil(t, err)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	err = rel.Append(bats[1])
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	t.Log(tae2.Catalog.SimplePPString(common.PPL1))
	tae2.Close()

	tae3, err := Open(tae.Dir, nil)
	assert.Nil(t, err)
	t.Log(tae3.Catalog.SimplePPString(common.PPL1))

	txn, err = tae3.StartTxn(nil)
	assert.Nil(t, err)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	seg, err = rel.GetSegment(seg.GetID())
	assert.Nil(t, err)
	_, err = seg.GetBlock(blk.ID)
	assert.Nil(t, err)
	filter = new(handle.Filter)
	filter.Op = handle.FilterEq
	filter.Val = int32(1500)
	id2, row2, err = rel.GetByFilter(filter)
	assert.Nil(t, err)
	assert.Equal(t, id.BlockID, id2.BlockID)
	assert.Equal(t, row, row2)
	val, err = rel.GetValue(id, row-1, 0)
	assert.Nil(t, err)
	assert.Equal(t, int32(33), val)
	_, err = rel.GetValue(id, row+1, 0)
	assert.NotNil(t, err)
	assert.Nil(t, txn.Commit())

	txn, err = tae3.StartTxn(nil)
	assert.Nil(t, err)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	blkIterator = rel.MakeBlockIt()
	for blkIterator.Valid() {
		blk := blkIterator.GetBlock()
		blkdata := blk.GetMeta().(*catalog.BlockEntry).GetBlockData()
		blkdata.Flush()
		blkIterator.Next()
	}
	err = tae3.Catalog.Checkpoint(txn.GetStartTS())
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	testutils.WaitExpect(4000, func() bool {
		return tae3.Wal.GetPenddingCnt() == 0
	})
	assert.Equal(t, uint64(0), tae3.Wal.GetPenddingCnt())

	tae3.Close()

	tae4, err := Open(tae.Dir, nil)
	assert.Nil(t, err)

	// txn, err = tae4.StartTxn(nil)
	// assert.Nil(t, err)
	// db, err = txn.GetDatabase("db")
	// assert.Nil(t, err)
	// rel, err = db.GetRelationByName(schema.Name)
	// assert.Nil(t, err)
	// seg, err = rel.GetSegment(seg.GetID())
	// assert.Nil(t, err)
	// _, err = seg.GetBlock(blk.ID)
	// assert.Nil(t, err)
	// filter = new(handle.Filter)
	// filter.Op = handle.FilterEq
	// filter.Val = int32(1500)
	// id2, row2, err = rel.GetByFilter(filter)
	// assert.Nil(t, err)
	// assert.Equal(t, id.BlockID, id2.BlockID)
	// assert.Equal(t, row, row2)
	// val, err = rel.GetValue(id, row-1, 0)
	// assert.Nil(t, err)
	// assert.Equal(t, int32(33), val)
	// _, err = rel.GetValue(id, row+1, 0)
	// assert.NotNil(t, err)
	// assert.Nil(t, txn.Commit())

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
	testutils.EnsureNoLeak(t)
	tae := newTestEngine(t, nil)
	defer tae.Close()
	schema := catalog.MockSchema(2, 1)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 2
	tae.bindSchema(schema)
	bat := catalog.MockBatch(schema, 1)
	defer bat.Close()
	v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(0)
	filter := handle.NewEQFilter(v)

	txn, err := tae.StartTxn(nil)
	assert.Nil(t, err)
	db, err := txn.CreateDatabase("db")
	assert.Nil(t, err)
	tbl, err := db.CreateRelation(schema)
	assert.Nil(t, err)
	err = tbl.Append(bat)
	assert.Nil(t, err)
	for i := 0; i < 100; i++ {
		blkID, row, err := tbl.GetByFilter(filter)
		assert.Nil(t, err)
		err = tbl.Update(blkID, row, 0, int32(33))
		assert.Nil(t, err)
		blkID, row, err = tbl.GetByFilter(filter)
		assert.Nil(t, err)
		err = tbl.RangeDelete(blkID, row, row, handle.DT_Normal)
		assert.NoError(t, err)
		err = tbl.Append(bat)
		assert.NoError(t, err)
	}
	assert.NoError(t, txn.Commit())

	for i := 0; i < 10; i++ {
		txn, rel := tae.getRelation()
		blkID, row, err := rel.GetByFilter(filter)
		assert.Nil(t, err)
		err = rel.Update(blkID, row, 0, int32(33))
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())

		txn, rel = tae.getRelation()
		blkID, row, err = rel.GetByFilter(filter)
		assert.Nil(t, err)
		err = rel.RangeDelete(blkID, row, row, handle.DT_Normal)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())

		txn, rel = tae.getRelation()
		err = rel.Append(bat)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
		// {
		// 	txn, rel := tae.getRelation()
		// 	blk := getOneBlock(rel)
		// 	t.Log(blk.String())
		// 	assert.Nil(t, txn.Rollback())
		// }
	}

	tae.restart()

	txn, rel := tae.getRelation()
	assert.Equal(t, uint64(1), rel.GetMeta().(*catalog.TableEntry).GetRows())
	assert.NoError(t, txn.Commit())

	txn, rel = tae.getRelation()
	blkIterator := rel.MakeBlockIt()
	for blkIterator.Valid() {
		blk := blkIterator.GetBlock()
		blkdata := blk.GetMeta().(*catalog.BlockEntry).GetBlockData()
		blkdata.Flush()
		blkIterator.Next()
	}
	err = tae.Catalog.Checkpoint(txn.GetStartTS())
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

	testutils.WaitExpect(4000, func() bool {
		return tae.Wal.GetPenddingCnt() == 0
	})
	assert.Equal(t, uint64(0), tae.Wal.GetPenddingCnt())
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
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	schema := catalog.MockSchema(2, 1)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockBatch(schema, 4800)
	defer bat.Close()
	bats := bat.Split(3)
	rows := uint64(0)

	txn, err := tae.StartTxn(nil)
	assert.Nil(t, err)
	db, err := txn.CreateDatabase("db")
	assert.Nil(t, err)
	tbl, err := db.CreateRelation(schema)
	assert.Nil(t, err)
	err = tbl.Append(bats[0])
	assert.Nil(t, err)
	rows += 1600
	blkIterator := tbl.MakeBlockIt()
	blkID := blkIterator.GetBlock().Fingerprint()
	err = tbl.RangeDelete(blkID, 0, 99, handle.DT_Normal)
	assert.Nil(t, err)
	rows -= 100
	assert.Nil(t, txn.Commit())

	txn, err = tae.StartTxn(nil)
	assert.Nil(t, err)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	tbl, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	err = tbl.Append(bats[1])
	assert.Nil(t, err)
	rows += 1600
	err = tbl.RangeDelete(blkID, 0, 99, handle.DT_Normal)
	assert.Nil(t, err)
	rows -= 100
	assert.Nil(t, txn.Commit())

	tae.Close()

	tae2, err := Open(tae.Dir, nil)
	assert.Nil(t, err)

	txn, err = tae2.StartTxn(nil)
	assert.Nil(t, err)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	tbl, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, rows, tbl.GetMeta().(*catalog.TableEntry).GetRows())
	assert.Nil(t, txn.Commit())

	txn, err = tae2.StartTxn(nil)
	assert.Nil(t, err)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	tbl, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	blkIterator = tbl.MakeBlockIt()
	for blkIterator.Valid() {
		blk := blkIterator.GetBlock()
		blkdata := blk.GetMeta().(*catalog.BlockEntry).GetBlockData()
		blkdata.Flush()
		blkIterator.Next()
	}
	err = tae2.Catalog.Checkpoint(txn.GetStartTS())
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	testutils.WaitExpect(4000, func() bool {
		return tae2.Wal.GetPenddingCnt() == 0
	})
	assert.Equal(t, uint64(0), tae2.Wal.GetPenddingCnt())

	err = tae2.Close()
	assert.Nil(t, err)

	tae3, err := Open(tae.Dir, nil)
	assert.Nil(t, err)

	txn, err = tae3.StartTxn(nil)
	assert.Nil(t, err)
	assert.Nil(t, err)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	tbl, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, rows, tbl.GetMeta().(*catalog.TableEntry).GetRows())
	assert.Nil(t, txn.Commit())

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
	// assert.Nil(t, txn.Commit())
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
	// assert.Nil(t, txn.Commit())

	// err = tae4.Close()
	// assert.Nil(t, err)
}

// Testing Steps
func TestReplay4(t *testing.T) {
	testutils.EnsureNoLeak(t)
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := initDB(t, opts)

	schema := catalog.MockSchemaAll(18, 16)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*uint32(schema.SegmentMaxBlocks+1)+1))
	defer bat.Close()
	bats := bat.Split(4)

	createRelationAndAppend(t, tae, defaultTestDB, schema, bats[0], true)
	txn, rel := getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, bats[0].Length(), false)
	assert.NoError(t, txn.Commit())

	_ = tae.Close()

	tae2, err := Open(tae.Dir, nil)
	assert.NoError(t, err)

	txn, rel = getDefaultRelation(t, tae2, schema.Name)
	checkAllColRowsByScan(t, rel, bats[0].Length(), false)
	err = rel.Append(bats[1])
	assert.NoError(t, err)
	checkAllColRowsByScan(t, rel, bats[0].Length()+bats[1].Length(), false)
	assert.NoError(t, txn.Commit())

	compactBlocks(t, tae2, defaultTestDB, schema, false)
	txn, rel = getDefaultRelation(t, tae2, schema.Name)
	checkAllColRowsByScan(t, rel, bats[0].Length()+bats[1].Length(), false)
	err = rel.Append(bats[2])
	assert.NoError(t, err)
	checkAllColRowsByScan(t, rel,
		bats[0].Length()+bats[1].Length()+bats[2].Length(), false)
	assert.NoError(t, txn.Commit())

	compactBlocks(t, tae2, defaultTestDB, schema, false)

	txn, rel = getDefaultRelation(t, tae2, schema.Name)
	checkAllColRowsByScan(t, rel, lenOfBats(bats[0:3]), false)
	assert.NoError(t, txn.Commit())

	mergeBlocks(t, tae2, defaultTestDB, schema, false)

	txn, rel = getDefaultRelation(t, tae2, schema.Name)
	checkAllColRowsByScan(t, rel, lenOfBats(bats[0:3]), false)
	err = rel.Append(bats[3])
	assert.NoError(t, err)
	checkAllColRowsByScan(t, rel, bat.Length(), false)
	assert.NoError(t, txn.Commit())
	t.Log(tae2.Catalog.SimplePPString(common.PPL1))

	tae2.Close()

	tae3, err := Open(tae.Dir, nil)
	assert.NoError(t, err)
	defer tae3.Close()
}

// Testing Steps
func TestReplay5(t *testing.T) {
	testutils.EnsureNoLeak(t)
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := initDB(t, opts)

	schema := catalog.MockSchemaAll(18, 16)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*uint32(schema.SegmentMaxBlocks+1)+1))
	defer bat.Close()
	bats := bat.Split(8)

	createRelationAndAppend(t, tae, defaultTestDB, schema, bats[0], true)
	txn, rel := getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, bats[0].Length(), false)
	assert.NoError(t, txn.Commit())
	forceCompactABlocks(t, tae, defaultTestDB, schema, false)

	_ = tae.Close()
	tae, err := Open(tae.Dir, nil)
	assert.NoError(t, err)

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, bats[0].Length(), false)
	err = rel.Append(bats[0])
	assert.ErrorIs(t, err, data.ErrDuplicate)
	err = rel.Append(bats[1])
	assert.NoError(t, err)
	checkAllColRowsByScan(t, rel, lenOfBats(bats[0:2]), false)
	assert.NoError(t, txn.Commit())
	t.Logf("LSN=%d", txn.GetLSN())

	forceCompactABlocks(t, tae, defaultTestDB, schema, false)

	_ = tae.Close()
	tae, err = Open(tae.Dir, nil)
	assert.NoError(t, err)

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, lenOfBats(bats[0:2]), false)
	err = rel.Append(bats[0])
	assert.ErrorIs(t, err, data.ErrDuplicate)
	err = rel.Append(bats[1])
	assert.ErrorIs(t, err, data.ErrDuplicate)
	err = rel.Append(bats[2])
	assert.NoError(t, err)
	err = rel.Append(bats[3])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

	compactBlocks(t, tae, defaultTestDB, schema, false)
	err = tae.Catalog.Checkpoint(tae.TxnMgr.StatSafeTS())
	assert.NoError(t, err)
	txn, rel = getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, lenOfBats(bats[:4]), false)
	assert.NoError(t, txn.Commit())

	_ = tae.Close()
	tae, err = Open(tae.Dir, nil)
	assert.NoError(t, err)

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, lenOfBats(bats[:4]), false)
	err = rel.Append(bats[3])
	assert.ErrorIs(t, err, data.ErrDuplicate)
	for _, b := range bats[4:8] {
		err = rel.Append(b)
		assert.NoError(t, err)
	}
	assert.NoError(t, txn.Commit())
	compactBlocks(t, tae, defaultTestDB, schema, false)
	err = tae.Catalog.Checkpoint(tae.TxnMgr.StatSafeTS())
	assert.NoError(t, err)

	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	printCheckpointStats(t, tae)
	_ = tae.Close()
	tae, err = Open(tae.Dir, nil)
	assert.NoError(t, err)

	forceCompactABlocks(t, tae, defaultTestDB, schema, false)
	txn, rel = getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, lenOfBats(bats[:8]), false)
	err = rel.Append(bats[0])
	assert.ErrorIs(t, err, data.ErrDuplicate)
	assert.NoError(t, txn.Commit())
	testutils.WaitExpect(3000, func() bool {
		return tae.Wal.GetCheckpointed() == tae.Wal.GetCurrSeqNum()
	})
	printCheckpointStats(t, tae)
	assert.Equal(t, tae.Wal.GetCurrSeqNum(), tae.Wal.GetCheckpointed())
	mergeBlocks(t, tae, defaultTestDB, schema, false)

	_ = tae.Close()
	tae, err = Open(tae.Dir, nil)
	assert.NoError(t, err)
	defer tae.Close()
	txn, rel = getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, lenOfBats(bats[:8]), false)
	err = rel.Append(bats[0])
	assert.ErrorIs(t, err, data.ErrDuplicate)
	assert.NoError(t, txn.Commit())

	err = tae.Catalog.Checkpoint(tae.TxnMgr.StatSafeTS())
	assert.NoError(t, err)
	testutils.WaitExpect(3000, func() bool {
		return tae.Wal.GetCheckpointed() == tae.Wal.GetCurrSeqNum()
	})
	printCheckpointStats(t, tae)
	assert.Equal(t, tae.Wal.GetCurrSeqNum(), tae.Wal.GetCheckpointed())
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}

func TestReplay6(t *testing.T) {
	testutils.EnsureNoLeak(t)
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := initDB(t, opts)
	schema := catalog.MockSchemaAll(18, 15)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*10-1))
	defer bat.Close()
	bats := bat.Split(4)

	createRelationAndAppend(t, tae, defaultTestDB, schema, bats[0], true)

	_ = tae.Close()
	tae, err := Open(tae.Dir, opts)
	assert.NoError(t, err)

	txn, rel := getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, lenOfBats(bats[0:1]), false)
	err = rel.Append(bats[0])
	assert.ErrorIs(t, err, data.ErrDuplicate)
	err = rel.Append(bats[1])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

	compactBlocks(t, tae, defaultTestDB, schema, false)

	_ = tae.Close()
	tae, err = Open(tae.Dir, opts)
	assert.NoError(t, err)

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, lenOfBats(bats[0:2]), false)
	err = rel.Append(bats[0])
	assert.ErrorIs(t, err, data.ErrDuplicate)
	err = rel.Append(bats[2])
	assert.NoError(t, err)
	err = rel.Append(bats[3])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())
	compactBlocks(t, tae, defaultTestDB, schema, false)
	mergeBlocks(t, tae, defaultTestDB, schema, false)
	err = tae.Catalog.Checkpoint(tae.TxnMgr.StatSafeTS())
	assert.NoError(t, err)

	_ = tae.Close()
	tae, err = Open(tae.Dir, opts)
	assert.NoError(t, err)

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, lenOfBats(bats[0:4]), false)
	assert.NoError(t, txn.Commit())
	printCheckpointStats(t, tae)
	_ = tae.Close()
}

func TestReplay7(t *testing.T) {
	t.Skip(any("This case crashes occasionally, is being fixed, skip it for now"))
	testutils.EnsureNoLeak(t)
	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := initDB(t, opts)
	schema := catalog.MockSchemaAll(18, 14)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 5

	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*15+1))
	defer bat.Close()
	createRelationAndAppend(t, tae, defaultTestDB, schema, bat, true)
	compactBlocks(t, tae, defaultTestDB, schema, true)
	mergeBlocks(t, tae, defaultTestDB, schema, true)
	time.Sleep(time.Millisecond * 100)

	_ = tae.Close()
	tae, err := Open(tae.Dir, opts)
	assert.NoError(t, err)
	defer tae.Close()
	// t.Log(tae.Catalog.SimplePPString(common.PPL1))
	txn, rel := getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, bat.Length(), false)
	assert.NoError(t, txn.Commit())
}

func TestReplay8(t *testing.T) {
	testutils.EnsureNoLeak(t)
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := newTestEngine(t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(18, 13)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	tae.bindSchema(schema)

	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*3+1))
	defer bat.Close()
	bats := bat.Split(4)

	tae.createRelAndAppend(bats[0], true)
	txn, rel := tae.getRelation()
	v := getSingleSortKeyValue(bats[0], schema, 2)
	filter := handle.NewEQFilter(v)
	err := rel.DeleteByFilter(filter)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

	txn, rel = tae.getRelation()
	window := bat.CloneWindow(2, 1)
	defer window.Close()
	err = rel.Append(window)
	assert.NoError(t, err)
	_ = txn.Rollback()

	tae.restart()

	// Check the total rows by scan
	txn, rel = tae.getRelation()
	checkAllColRowsByScan(t, rel, bats[0].Length()-1, true)
	err = rel.Append(bats[0])
	assert.ErrorIs(t, err, data.ErrDuplicate)
	assert.NoError(t, txn.Commit())

	// Try to append the delete row and then rollback
	txn, rel = tae.getRelation()
	err = rel.Append(window)
	assert.NoError(t, err)
	_ = txn.Rollback()

	// Flush the appendable block
	forceCompactABlocks(t, tae.DB, defaultTestDB, schema, false)

	txn, rel = getDefaultRelation(t, tae.DB, schema.Name)
	checkAllColRowsByScan(t, rel, bats[0].Length()-1, true)
	assert.NoError(t, txn.Commit())

	txn, rel = getDefaultRelation(t, tae.DB, schema.Name)
	err = rel.Append(window)
	assert.NoError(t, err)
	_ = txn.Rollback()

	tae.restart()

	txn, rel = getDefaultRelation(t, tae.DB, schema.Name)
	checkAllColRowsByScan(t, rel, bats[0].Length()-1, true)
	assert.NoError(t, txn.Commit())

	txn, rel = getDefaultRelation(t, tae.DB, schema.Name)
	err = rel.Append(window)
	assert.NoError(t, err)
	tuple3 := bat.Window(3, 1)
	err = rel.Append(tuple3)
	assert.ErrorIs(t, err, data.ErrDuplicate)
	assert.NoError(t, txn.Commit())

	txn, rel = getDefaultRelation(t, tae.DB, schema.Name)
	checkAllColRowsByScan(t, rel, bats[0].Length(), true)
	assert.NoError(t, txn.Commit())

	tae.restart()

	txn, rel = getDefaultRelation(t, tae.DB, schema.Name)
	checkAllColRowsByScan(t, rel, bats[0].Length(), true)
	err = rel.Append(window)
	assert.ErrorIs(t, err, data.ErrDuplicate)
	err = rel.Append(bats[1])
	assert.NoError(t, err)
	err = rel.Append(bats[2])
	assert.NoError(t, err)
	err = rel.Append(bats[3])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

	tae.compactBlocks(false)
	tae.restart()

	txn, rel = tae.getRelation()
	checkAllColRowsByScan(t, rel, bat.Length(), true)
	err = rel.Append(window)
	assert.ErrorIs(t, err, data.ErrDuplicate)

	v0_5 := getSingleSortKeyValue(bats[0], schema, 5)
	filter = handle.NewEQFilter(v0_5)
	err = rel.DeleteByFilter(filter)
	assert.NoError(t, err)
	v1_5 := getSingleSortKeyValue(bats[1], schema, 5)
	filter = handle.NewEQFilter(v1_5)
	err = rel.DeleteByFilter(filter)
	assert.NoError(t, err)
	v2_5 := getSingleSortKeyValue(bats[2], schema, 5)
	filter = handle.NewEQFilter(v2_5)
	err = rel.DeleteByFilter(filter)
	assert.NoError(t, err)
	v3_2 := getSingleSortKeyValue(bats[3], schema, 2)
	filter = handle.NewEQFilter(v3_2)
	err = rel.DeleteByFilter(filter)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())
	// t.Log(tae.Catalog.SimplePPString(common.PPL1))

	tae.restart()

	txn, rel = tae.getRelation()
	checkAllColRowsByScan(t, rel, bat.Length()-4, true)
	tuple0_5 := bat.Window(5, 1)
	err = rel.Append(tuple0_5)
	assert.NoError(t, err)
	tuple1_5 := bats[1].Window(5, 1)
	err = rel.Append(tuple1_5)
	assert.NoError(t, err)
	tuple2_5 := bats[2].Window(5, 1)
	err = rel.Append(tuple2_5)
	assert.NoError(t, err)
	tuple3_2 := bats[3].Window(2, 1)
	err = rel.Append(tuple3_2)
	assert.NoError(t, err)
	checkAllColRowsByScan(t, rel, bat.Length(), true)
	_ = txn.Rollback()

	tae.compactABlocks(false)
	tae.checkpointCatalog()
	tae.restart()

	txn, rel = tae.getRelation()
	checkAllColRowsByScan(t, rel, bat.Length()-4, true)
	err = rel.Append(tuple0_5)
	assert.NoError(t, err)
	err = rel.Append(tuple1_5)
	assert.NoError(t, err)
	err = rel.Append(tuple2_5)
	assert.NoError(t, err)
	err = rel.Append(tuple3_2)
	assert.NoError(t, err)
	checkAllColRowsByScan(t, rel, bat.Length(), true)
	_ = txn.Rollback()
}

func TestReplay9(t *testing.T) {
	testutils.EnsureNoLeak(t)
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := newTestEngine(t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(18, 3)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	tae.bindSchema(schema)
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*3+2))
	defer bat.Close()
	bats := bat.Split(4)

	tae.createRelAndAppend(bats[0], true)
	txn, rel := tae.getRelation()
	checkAllColRowsByScan(t, rel, bats[0].Length(), false)
	v := getSingleSortKeyValue(bats[0], schema, 2)
	filter := handle.NewEQFilter(v)
	err := rel.UpdateByFilter(filter, 2, int32(999))
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

	tae.restart()
	txn, rel = tae.getRelation()
	actv, err := rel.GetValueByFilter(filter, 2)
	assert.NoError(t, err)
	assert.Equal(t, int32(999), actv)
	checkAllColRowsByScan(t, rel, bats[0].Length(), false)
	assert.NoError(t, txn.Commit())

	tae.compactABlocks(false)

	tae.restart()
	txn, rel = tae.getRelation()
	_, _, err = rel.GetByFilter(filter)
	assert.NoError(t, err)
	actv, err = rel.GetValueByFilter(filter, 2)
	assert.NoError(t, err)
	assert.Equal(t, int32(999), actv)
	checkAllColRowsByScan(t, rel, bats[0].Length(), false)
	err = rel.Append(bats[1])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())
	tae.compactBlocks(false)

	txn, rel = tae.getRelation()
	v2 := getSingleSortKeyValue(bats[0], schema, 4)
	filter2 := handle.NewEQFilter(v2)
	err = rel.UpdateByFilter(filter2, 1, int16(199))
	assert.NoError(t, err)
	actv, err = rel.GetValueByFilter(filter2, 1)
	assert.NoError(t, err)
	assert.Equal(t, int16(199), actv)
	assert.NoError(t, txn.Commit())

	tae.restart()

	txn, rel = tae.getRelation()
	actv, err = rel.GetValueByFilter(filter2, 1)
	assert.NoError(t, err)
	assert.Equal(t, int16(199), actv)
	err = rel.Append(bats[2])
	assert.NoError(t, err)
	err = rel.Append(bats[3])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

	tae.compactBlocks(false)
	tae.mergeBlocks(false)

	txn, rel = tae.getRelation()
	v3 := getSingleSortKeyValue(bats[1], schema, 3)
	filter3 := handle.NewEQFilter(v3)
	err = rel.UpdateByFilter(filter3, 5, uint16(88))
	assert.NoError(t, err)
	actv, err = rel.GetValueByFilter(filter3, 5)
	assert.NoError(t, err)
	assert.Equal(t, uint16(88), actv)
	assert.NoError(t, txn.Commit())

	tae.restart()

	txn, rel = tae.getRelation()
	actv, err = rel.GetValueByFilter(filter, 2)
	assert.NoError(t, err)
	assert.Equal(t, int32(999), actv)
	actv, err = rel.GetValueByFilter(filter2, 1)
	assert.NoError(t, err)
	assert.Equal(t, int16(199), actv)
	actv, err = rel.GetValueByFilter(filter3, 5)
	assert.NoError(t, err)
	assert.Equal(t, uint16(88), actv)
	assert.NoError(t, txn.Commit())
}

func TestReplay10(t *testing.T) {
	testutils.EnsureNoLeak(t)
	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := initDB(t, opts)
	schema := catalog.MockSchemaAll(3, 2)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 5
	schema.ColDefs[1].Default = catalog.Default{
		Set:   true,
		Null:  false,
		Value: int16(3)}
	schema.ColDefs[2].Default = catalog.Default{
		Set:  true,
		Null: true,
	}

	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows))
	defer bat.Close()
	createRelationAndAppend(t, tae, defaultTestDB, schema, bat, true)
	time.Sleep(time.Millisecond * 100)

	_ = tae.Close()
	tae, err := Open(tae.Dir, opts)
	assert.NoError(t, err)
	defer tae.Close()
	// t.Log(tae.Catalog.SimplePPString(common.PPL1))
	txn, rel := getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, bat.Length(), false)
	assert.NoError(t, txn.Commit())
	schema1 := rel.GetMeta().(*catalog.TableEntry).GetSchema()
	assert.Equal(t, int16(3), schema1.ColDefs[1].Default.Value.(int16))
	assert.Equal(t, true, schema1.ColDefs[2].Default.Null)
}
