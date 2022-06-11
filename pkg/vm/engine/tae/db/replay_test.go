package db

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func TestReplayCatalog1(t *testing.T) {
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

	bat := catalog.MockData(schema, 10000)
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
	err = rel.RangeDelete(id, row+1, row+1)
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
	tae := initDB(t, nil)
	schema := catalog.MockSchema(2, 1)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockData(schema, 10000)
	bats := compute.SplitBatch(bat, 2)

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
	err = rel.RangeDelete(id, row+1, row+100)
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
	tae := initDB(t, nil)
	schema := catalog.MockSchema(2, 1)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockData(schema, 1)
	v := compute.GetValue(bat.Vecs[schema.GetSingleSortKeyIdx()], 0)
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
		err = tbl.RangeDelete(blkID, row, row)
		assert.Nil(t, err)
		err = tbl.Append(bat)
		assert.Nil(t, err)
	}
	assert.Nil(t, txn.Commit())

	for i := 0; i < 10; i++ {
		txn, err := tae.StartTxn(nil)
		assert.Nil(t, err)
		db, err = txn.GetDatabase("db")
		assert.Nil(t, err)
		tbl, err = db.GetRelationByName(schema.Name)
		assert.Nil(t, err)
		blkID, row, err := tbl.GetByFilter(filter)
		assert.Nil(t, err)
		err = tbl.Update(blkID, row, 0, int32(33))
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())

		txn, err = tae.StartTxn(nil)
		assert.Nil(t, err)
		db, err = txn.GetDatabase("db")
		assert.Nil(t, err)
		tbl, err = db.GetRelationByName(schema.Name)
		assert.Nil(t, err)
		blkID, row, err = tbl.GetByFilter(filter)
		assert.Nil(t, err)
		err = tbl.RangeDelete(blkID, row, row)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())

		txn, err = tae.StartTxn(nil)
		assert.Nil(t, err)
		db, err = txn.GetDatabase("db")
		assert.Nil(t, err)
		tbl, err = db.GetRelationByName(schema.Name)
		assert.Nil(t, err)
		err = tbl.Append(bat)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}

	tae.Close()

	tae2, err := Open(tae.Dir, nil)
	assert.Nil(t, err)

	txn, err = tae2.StartTxn(nil)
	assert.Nil(t, err)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	tbl, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), tbl.GetMeta().(*catalog.TableEntry).GetRows())
	assert.Nil(t, txn.Commit())

	txn, err = tae2.StartTxn(nil)
	assert.Nil(t, err)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	tbl, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	blkIterator := tbl.MakeBlockIt()
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

	tae2.Close()
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
	tae := initDB(t, nil)
	schema := catalog.MockSchema(2, 1)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockData(schema, 4800)
	bats := compute.SplitBatch(bat, 3)
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
	err = tbl.RangeDelete(blkID, 0, 99)
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
	err = tbl.RangeDelete(blkID, 0, 99)
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
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := initDB(t, opts)

	schema := catalog.MockSchemaAll(18, 16)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockData(schema, schema.BlockMaxRows*uint32(schema.SegmentMaxBlocks+1)+1)
	bats := compute.SplitBatch(bat, 4)

	createRelationAndAppend(t, tae, defaultTestDB, schema, bats[0], true)
	txn, rel := getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bats[0]), false)
	assert.NoError(t, txn.Commit())

	_ = tae.Close()

	tae2, err := Open(tae.Dir, nil)
	assert.NoError(t, err)

	txn, rel = getDefaultRelation(t, tae2, schema.Name)
	checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bats[0]), false)
	err = rel.Append(bats[1])
	checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bats[0])+compute.LengthOfBatch(bats[1]), false)
	assert.NoError(t, txn.Commit())

	compactBlocks(t, tae2, defaultTestDB, schema, false)
	txn, rel = getDefaultRelation(t, tae2, schema.Name)
	checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bats[0])+compute.LengthOfBatch(bats[1]), false)
	err = rel.Append(bats[2])
	checkAllColRowsByScan(t, rel,
		compute.LengthOfBatch(bats[0])+compute.LengthOfBatch(bats[1])+compute.LengthOfBatch(bats[2]), false)
	assert.NoError(t, txn.Commit())

	compactBlocks(t, tae2, defaultTestDB, schema, false)

	txn, rel = getDefaultRelation(t, tae2, schema.Name)
	checkAllColRowsByScan(t, rel,
		compute.LengthOfBatch(bats[0])+compute.LengthOfBatch(bats[1])+compute.LengthOfBatch(bats[2]), false)
	assert.NoError(t, txn.Commit())

	mergeBlocks(t, tae2, defaultTestDB, schema, false)

	txn, rel = getDefaultRelation(t, tae2, schema.Name)
	checkAllColRowsByScan(t, rel,
		compute.LengthOfBatch(bats[0])+compute.LengthOfBatch(bats[1])+compute.LengthOfBatch(bats[2]), false)
	err = rel.Append(bats[3])
	assert.NoError(t, err)
	checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bat), false)
	assert.NoError(t, txn.Commit())
	t.Log(tae2.Catalog.SimplePPString(common.PPL1))

	tae2.Close()

	tae3, err := Open(tae.Dir, nil)
	assert.NoError(t, err)
	defer tae3.Close()
}

// Testing Steps
func TestReplay5(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := initDB(t, opts)

	schema := catalog.MockSchemaAll(18, 16)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockData(schema, schema.BlockMaxRows*uint32(schema.SegmentMaxBlocks+1)+1)
	bats := compute.SplitBatch(bat, 8)

	createRelationAndAppend(t, tae, defaultTestDB, schema, bats[0], true)
	txn, rel := getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bats[0]), false)
	assert.NoError(t, txn.Commit())
	forceCompactABlocks(t, tae, defaultTestDB, schema, false)

	_ = tae.Close()
	tae, err := Open(tae.Dir, nil)
	assert.NoError(t, err)

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bats[0]), false)
	err = rel.Append(bats[0])
	assert.ErrorIs(t, err, data.ErrDuplicate)
	err = rel.Append(bats[1])
	checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bats[0])+compute.LengthOfBatch(bats[1]), false)
	assert.NoError(t, txn.Commit())
	t.Logf("LSN=%d", txn.GetLSN())

	forceCompactABlocks(t, tae, defaultTestDB, schema, false)

	_ = tae.Close()
	tae, err = Open(tae.Dir, nil)
	assert.NoError(t, err)

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bats[0])+compute.LengthOfBatch(bats[1]), false)
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
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := initDB(t, opts)
	schema := catalog.MockSchemaAll(18, 15)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockData(schema, schema.BlockMaxRows*10-1)
	bats := compute.SplitBatch(bat, 4)

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
	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := initDB(t, opts)
	schema := catalog.MockSchemaAll(18, 14)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 5

	bat := catalog.MockData(schema, schema.BlockMaxRows*15+1)
	createRelationAndAppend(t, tae, defaultTestDB, schema, bat, true)
	compactBlocks(t, tae, defaultTestDB, schema, true)
	mergeBlocks(t, tae, defaultTestDB, schema, true)
	time.Sleep(time.Millisecond * 100)
	// txn, rel := getDefaultRelation(t, tae, schema.Name)
	// checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bat), false)
	// assert.NoError(t, txn.Commit())

	_ = tae.Close()
	tae, err := Open(tae.Dir, opts)
	assert.NoError(t, err)
	defer tae.Close()
	// t.Log(tae.Catalog.SimplePPString(common.PPL1))
	txn, rel := getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bat), false)
	assert.NoError(t, txn.Commit())
}

func TestReplay8(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := newTestEngine(t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(18, 13)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	tae.bindSchema(schema)

	bat := catalog.MockData(schema, schema.BlockMaxRows*3+1)
	bats := compute.SplitBatch(bat, 4)

	tae.createRelAndAppend(bats[0], true)
	txn, rel := tae.getRelation()
	v := getSingleSortKeyValue(bats[0], schema, 2)
	filter := handle.NewEQFilter(v)
	err := rel.DeleteByFilter(filter)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

	txn, rel = tae.getRelation()
	window := compute.BatchWindow(bats[0], 2, 3)
	err = rel.Append(window)
	assert.NoError(t, err)
	_ = txn.Rollback()

	tae.restart()

	// Check the total rows by scan
	txn, rel = tae.getRelation()
	checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bats[0])-1, true)
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
	checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bats[0])-1, true)
	assert.NoError(t, txn.Commit())

	tae.restart()

	txn, rel = getDefaultRelation(t, tae.DB, schema.Name)
	checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bats[0])-1, true)
	assert.NoError(t, txn.Commit())
}
