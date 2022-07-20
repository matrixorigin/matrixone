// Copyright 2021 Matrix Origin
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

package db

import (
	"bytes"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/txnentries"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	ops "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func TestAppend(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := newTestEngine(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(14, 3)
	schema.BlockMaxRows = options.DefaultBlockMaxRows
	schema.SegmentMaxBlocks = options.DefaultBlocksPerSegment
	tae.bindSchema(schema)
	data := catalog.MockBatch(schema, int(schema.BlockMaxRows*2))
	defer data.Close()
	bats := data.Split(4)
	now := time.Now()
	tae.createRelAndAppend(bats[0], true)
	t.Log(time.Since(now))
	tae.checkRowsByScan(bats[0].Length(), false)

	txn, rel := tae.getRelation()
	err := rel.Append(bats[1])
	assert.NoError(t, err)
	// FIXME
	// checkAllColRowsByScan(t, rel, bats[0].Length()+bats[1].Length(), false)
	err = rel.Append(bats[2])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())
	tae.checkRowsByScan(bats[0].Length()+bats[1].Length()+bats[2].Length(), false)
}

func TestAppend2(t *testing.T) {
	testutils.EnsureNoLeak(t)
	opts := config.WithQuickScanAndCKPOpts(nil)
	db := initDB(t, opts)
	defer db.Close()
	schema := catalog.MockSchemaAll(13, 3)
	schema.BlockMaxRows = 400
	schema.SegmentMaxBlocks = 10
	createRelation(t, db, "db", schema, true)

	totalRows := uint64(schema.BlockMaxRows * 30)
	bat := catalog.MockBatch(schema, int(totalRows))
	defer bat.Close()
	bats := bat.Split(100)

	var wg sync.WaitGroup
	pool, _ := ants.NewPool(80)

	start := time.Now()
	for _, data := range bats {
		wg.Add(1)
		err := pool.Submit(appendClosure(t, data, schema.Name, db, &wg))
		assert.Nil(t, err)
	}
	wg.Wait()
	t.Logf("Append %d rows takes: %s", totalRows, time.Since(start))
	{
		txn, rel := getDefaultRelation(t, db, schema.Name)
		checkAllColRowsByScan(t, rel, int(totalRows), false)
		assert.NoError(t, txn.Commit())
	}
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))

	now := time.Now()
	testutils.WaitExpect(8000, func() bool {
		return db.Scheduler.GetPenddingLSNCnt() == 0
	})
	t.Log(time.Since(now))
	t.Logf("Checkpointed: %d", db.Scheduler.GetCheckpointedLSN())
	t.Logf("GetPenddingLSNCnt: %d", db.Scheduler.GetPenddingLSNCnt())
	assert.Equal(t, uint64(0), db.Scheduler.GetPenddingLSNCnt())
	t.Log(db.Catalog.SimplePPString(common.PPL1))
	wg.Add(1)
	appendFailClosure(t, bats[0], schema.Name, db, &wg)()
	wg.Wait()
}

func TestAppend3(t *testing.T) {
	testutils.EnsureNoLeak(t)
	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := initDB(t, opts)
	defer tae.Close()
	schema := catalog.MockSchema(2, 0)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	createRelation(t, tae, "db", schema, true)
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows))
	defer bat.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	appendClosure(t, bat, schema.Name, tae, &wg)()
	wg.Wait()
	testutils.WaitExpect(2000, func() bool {
		return tae.Scheduler.GetPenddingLSNCnt() == 0
	})
	// t.Log(tae.Catalog.SimplePPString(common.PPL1))
	wg.Add(1)
	appendFailClosure(t, bat, schema.Name, tae, &wg)()
	wg.Wait()
}

func TestAppend4(t *testing.T) {
	testutils.EnsureNoLeak(t)
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := initDB(t, opts)
	defer tae.Close()
	schema1 := catalog.MockSchemaAll(18, 14)
	schema2 := catalog.MockSchemaAll(18, 15)
	schema3 := catalog.MockSchemaAll(18, 16)
	schema4 := catalog.MockSchemaAll(18, 11)
	schema1.BlockMaxRows = 10
	schema2.BlockMaxRows = 10
	schema3.BlockMaxRows = 10
	schema4.BlockMaxRows = 10
	schema1.SegmentMaxBlocks = 2
	schema2.SegmentMaxBlocks = 2
	schema3.SegmentMaxBlocks = 2
	schema4.SegmentMaxBlocks = 2
	schemas := []*catalog.Schema{schema1, schema2, schema3, schema4}
	createDB(t, tae, defaultTestDB)
	for _, schema := range schemas {
		bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*3-1))
		defer bat.Close()
		bats := bat.Split(1)
		createRelation(t, tae, defaultTestDB, schema, false)
		for i := range bats {
			txn, rel := getDefaultRelation(t, tae, schema.Name)
			err := rel.Append(bats[i])
			assert.NoError(t, err)
			err = txn.Commit()
			assert.NoError(t, err)
		}
		txn, rel := getDefaultRelation(t, tae, schema.Name)
		checkAllColRowsByScan(t, rel, bat.Length(), false)

		v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(3)
		filter := handle.NewEQFilter(v)
		err := rel.DeleteByFilter(filter)
		assert.NoError(t, err)
		err = txn.Commit()
		assert.NoError(t, err)

		txn, rel = getDefaultRelation(t, tae, schema.Name)
		checkAllColRowsByScan(t, rel, bat.Length()-1, true)
		err = txn.Commit()
		assert.NoError(t, err)
		compactBlocks(t, tae, defaultTestDB, schema, false)
		txn, rel = getDefaultRelation(t, tae, schema.Name)
		checkAllColRowsByScan(t, rel, bat.Length()-1, false)
		err = txn.Commit()
		assert.NoError(t, err)
	}
}

func testCRUD(t *testing.T, tae *DB, schema *catalog.Schema) {
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*(uint32(schema.SegmentMaxBlocks)+1)-1))
	defer bat.Close()
	bats := bat.Split(4)

	var updateColIdx int
	if schema.GetSingleSortKeyIdx() >= 17 {
		updateColIdx = 0
	} else {
		updateColIdx = schema.GetSingleSortKeyIdx() + 1
	}

	createRelationAndAppend(t, tae, defaultTestDB, schema, bats[0], false)

	txn, rel := getDefaultRelation(t, tae, schema.Name)
	err := rel.Append(bats[0])
	assert.ErrorIs(t, err, data.ErrDuplicate)
	checkAllColRowsByScan(t, rel, bats[0].Length(), false)
	v := bats[0].Vecs[schema.GetSingleSortKeyIdx()].Get(2)
	filter := handle.NewEQFilter(v)
	err = rel.DeleteByFilter(filter)
	assert.NoError(t, err)

	oldv := bats[0].Vecs[updateColIdx].Get(5)
	v = bats[0].Vecs[schema.GetSingleSortKeyIdx()].Get(5)
	ufilter := handle.NewEQFilter(v)
	{
		ot := reflect.ValueOf(&oldv).Elem()
		nv := reflect.ValueOf(int8(99))
		if nv.CanConvert(reflect.TypeOf(oldv)) {
			ot.Set(nv.Convert(reflect.TypeOf(oldv)))
		}
	}
	err = rel.UpdateByFilter(ufilter, uint16(updateColIdx), oldv)
	assert.NoError(t, err)

	checkAllColRowsByScan(t, rel, bats[0].Length()-1, true)
	assert.NoError(t, txn.Commit())

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, bats[0].Length()-1, true)
	for _, b := range bats[1:] {
		err = rel.Append(b)
		assert.NoError(t, err)
	}
	checkAllColRowsByScan(t, rel, bat.Length()-1, true)
	assert.NoError(t, txn.Commit())

	compactBlocks(t, tae, defaultTestDB, schema, false)

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, bat.Length()-1, false)
	v = bats[0].Vecs[schema.GetSingleSortKeyIdx()].Get(3)
	filter = handle.NewEQFilter(v)
	err = rel.DeleteByFilter(filter)
	assert.NoError(t, err)
	checkAllColRowsByScan(t, rel, bat.Length()-2, true)
	assert.NoError(t, txn.Commit())

	compactSegs(t, tae, schema)

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, bat.Length()-2, false)
	assert.NoError(t, txn.Commit())

	// t.Log(rel.GetMeta().(*catalog.TableEntry).PPString(common.PPL1, 0, ""))
	dropRelation(t, tae, defaultTestDB, schema.Name)
}

func TestCRUD(t *testing.T) {
	testutils.EnsureNoLeak(t)
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := initDB(t, opts)
	defer tae.Close()
	createDB(t, tae, defaultTestDB)
	withTestAllPKType(t, tae, testCRUD)
}

func TestTableHandle(t *testing.T) {
	testutils.EnsureNoLeak(t)
	db := initDB(t, nil)
	defer db.Close()

	schema := catalog.MockSchema(2, 0)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 2

	txn, _ := db.StartTxn(nil)
	database, _ := txn.CreateDatabase("db")
	rel, _ := database.CreateRelation(schema)

	tableMeta := rel.GetMeta().(*catalog.TableEntry)
	t.Log(tableMeta.String())
	table := tableMeta.GetTableData()

	handle := table.GetHandle()
	appender, err := handle.GetAppender()
	assert.Nil(t, appender)
	assert.Equal(t, data.ErrAppendableSegmentNotFound, err)
}

func TestCreateBlock(t *testing.T) {
	testutils.EnsureNoLeak(t)
	db := initDB(t, nil)
	defer db.Close()

	txn, _ := db.StartTxn(nil)
	database, _ := txn.CreateDatabase("db")
	schema := catalog.MockSchemaAll(13, 12)
	rel, err := database.CreateRelation(schema)
	assert.Nil(t, err)
	seg, err := rel.CreateSegment()
	assert.Nil(t, err)
	blk1, err := seg.CreateBlock()
	assert.Nil(t, err)
	blk2, err := seg.CreateNonAppendableBlock()
	assert.Nil(t, err)
	lastAppendable := seg.GetMeta().(*catalog.SegmentEntry).LastAppendableBlock()
	assert.Equal(t, blk1.Fingerprint().BlockID, lastAppendable.GetID())
	assert.True(t, lastAppendable.IsAppendable())
	blk2Meta := blk2.GetMeta().(*catalog.BlockEntry)
	assert.False(t, blk2Meta.IsAppendable())

	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
	assert.Nil(t, txn.Commit())
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
}

func TestNonAppendableBlock(t *testing.T) {
	testutils.EnsureNoLeak(t)
	db := initDB(t, nil)
	defer db.Close()
	schema := catalog.MockSchemaAll(13, 1)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2

	bat := catalog.MockBatch(schema, 8)
	defer bat.Close()

	createRelation(t, db, "db", schema, true)

	{
		txn, _ := db.StartTxn(nil)
		database, err := txn.GetDatabase("db")
		assert.Nil(t, err)
		rel, err := database.GetRelationByName(schema.Name)
		assert.Nil(t, err)
		seg, err := rel.CreateSegment()
		assert.Nil(t, err)
		blk, err := seg.CreateNonAppendableBlock()
		assert.Nil(t, err)
		dataBlk := blk.GetMeta().(*catalog.BlockEntry).GetBlockData()
		blockFile := dataBlk.GetBlockFile()
		err = blockFile.WriteBatch(bat, txn.GetStartTS())
		assert.Nil(t, err)

		v, err := dataBlk.GetValue(txn, 4, 2)
		assert.Nil(t, err)
		expectVal := bat.Vecs[2].Get(4)
		assert.Equal(t, expectVal, v)
		assert.Equal(t, bat.Vecs[0].Length(), blk.Rows())

		view, err := dataBlk.GetColumnDataById(txn, 2, nil)
		assert.Nil(t, err)
		defer view.Close()
		assert.Nil(t, view.DeleteMask)
		assert.Equal(t, bat.Vecs[2].Length(), view.Length())

		_, err = dataBlk.RangeDelete(txn, 1, 2, handle.DT_Normal)
		assert.Nil(t, err)

		view, err = dataBlk.GetColumnDataById(txn, 2, nil)
		assert.Nil(t, err)
		defer view.Close()
		assert.True(t, view.DeleteMask.Contains(1))
		assert.True(t, view.DeleteMask.Contains(2))
		assert.Equal(t, bat.Vecs[2].Length(), view.Length())

		_, err = dataBlk.Update(txn, 3, 2, int32(999))
		assert.Nil(t, err)

		view, err = dataBlk.GetColumnDataById(txn, 2, nil)
		assert.Nil(t, err)
		defer view.Close()
		assert.True(t, view.DeleteMask.Contains(1))
		assert.True(t, view.DeleteMask.Contains(2))
		assert.Equal(t, bat.Vecs[2].Length(), view.Length())
		v = view.GetData().Get(3)
		assert.Equal(t, int32(999), v)

		assert.Nil(t, txn.Commit())
	}
}

func TestCreateSegment(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(1, 0)
	txn, _ := tae.StartTxn(nil)
	db, err := txn.CreateDatabase("db")
	assert.Nil(t, err)
	rel, err := db.CreateRelation(schema)
	assert.Nil(t, err)
	_, err = rel.CreateNonAppendableSegment()
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	bat := catalog.MockBatch(schema, 5)
	defer bat.Close()

	appendClosure(t, bat, schema.Name, tae, nil)()

	segCnt := 0
	processor := new(catalog.LoopProcessor)
	processor.SegmentFn = func(segment *catalog.SegmentEntry) error {
		segCnt++
		return nil
	}
	err = tae.Opts.Catalog.RecurLoop(processor)
	assert.Nil(t, err)
	assert.Equal(t, 2+3, segCnt)
	t.Log(tae.Opts.Catalog.SimplePPString(common.PPL1))
}

func TestCompactBlock1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	opts := config.WithLongScanAndCKPOpts(nil)
	db := initDB(t, opts)
	defer db.Close()
	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 4
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows))
	defer bat.Close()
	createRelationAndAppend(t, db, "db", schema, bat, true)
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))

	v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(2)
	filter := handle.NewEQFilter(v)
	// 1. No updates and deletes
	{
		txn, rel := getDefaultRelation(t, db, schema.Name)
		blkMeta := getOneBlockMeta(rel)
		task, err := jobs.NewCompactBlockTask(tasks.WaitableCtx, txn, blkMeta, db.Scheduler)
		assert.Nil(t, err)
		preparer, err := task.PrepareData(blkMeta.MakeKey())
		assert.Nil(t, err)
		assert.NotNil(t, preparer.Columns)
		defer preparer.Close()
		for col := 0; col < len(bat.Vecs); col++ {
			for row := 0; row < bat.Vecs[0].Length(); row++ {
				exp := bat.Vecs[col].Get(row)
				act := preparer.Columns.Vecs[col].Get(row)
				assert.Equal(t, exp, act)
			}
		}
		err = rel.DeleteByFilter(filter)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit())
	}
	{
		txn, _ := db.StartTxn(nil)
		database, err := txn.GetDatabase("db")
		assert.Nil(t, err)
		rel, err := database.GetRelationByName(schema.Name)
		assert.Nil(t, err)
		v = bat.Vecs[schema.GetSingleSortKeyIdx()].Get(3)
		filter.Val = v
		id, _, err := rel.GetByFilter(filter)
		assert.Nil(t, err)
		seg, _ := rel.GetSegment(id.SegmentID)
		block, err := seg.GetBlock(id.BlockID)
		assert.Nil(t, err)
		blkMeta := block.GetMeta().(*catalog.BlockEntry)
		task, err := jobs.NewCompactBlockTask(tasks.WaitableCtx, txn, blkMeta, nil)
		assert.Nil(t, err)
		preparer, err := task.PrepareData(blkMeta.MakeKey())
		assert.Nil(t, err)
		defer preparer.Close()
		assert.Equal(t, bat.Vecs[0].Length()-1, preparer.Columns.Vecs[0].Length())
		{
			txn, _ := db.StartTxn(nil)
			database, err := txn.GetDatabase("db")
			assert.Nil(t, err)
			rel, err := database.GetRelationByName(schema.Name)
			assert.Nil(t, err)
			v = bat.Vecs[schema.GetSingleSortKeyIdx()].Get(4)
			filter.Val = v
			id, offset, err := rel.GetByFilter(filter)
			assert.Nil(t, err)
			err = rel.RangeDelete(id, offset, offset, handle.DT_Normal)
			assert.Nil(t, err)
			err = rel.Update(id, offset+1, 3, int64(99))
			assert.Nil(t, err)
			assert.Nil(t, txn.Commit())
		}
		task, err = jobs.NewCompactBlockTask(tasks.WaitableCtx, txn, blkMeta, nil)
		assert.Nil(t, err)
		preparer, err = task.PrepareData(blkMeta.MakeKey())
		assert.Nil(t, err)
		defer preparer.Close()
		assert.Equal(t, bat.Vecs[0].Length()-1, preparer.Columns.Vecs[0].Length())
		var maxTs uint64
		{
			txn, rel := getDefaultRelation(t, db, schema.Name)
			seg, err := rel.GetSegment(id.SegmentID)
			assert.Nil(t, err)
			blk, err := seg.GetBlock(id.BlockID)
			assert.Nil(t, err)
			blkMeta := blk.GetMeta().(*catalog.BlockEntry)
			task, err = jobs.NewCompactBlockTask(tasks.WaitableCtx, txn, blkMeta, nil)
			assert.Nil(t, err)
			preparer, err := task.PrepareData(blkMeta.MakeKey())
			assert.Nil(t, err)
			defer preparer.Close()
			assert.Equal(t, bat.Vecs[0].Length()-2, preparer.Columns.Vecs[0].Length())
			maxTs = txn.GetStartTS()
		}

		dataBlock := block.GetMeta().(*catalog.BlockEntry).GetBlockData()
		changes, err := dataBlock.CollectChangesInRange(txn.GetStartTS(), maxTs+1)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), changes.DeleteMask.GetCardinality())

		destBlock, err := seg.CreateNonAppendableBlock()
		assert.Nil(t, err)
		m := destBlock.GetMeta().(*catalog.BlockEntry)
		txnEntry := txnentries.NewCompactBlockEntry(txn, block, destBlock, db.Scheduler, nil, nil)
		err = txn.LogTxnEntry(m.GetSegment().GetTable().GetDB().ID, destBlock.Fingerprint().TableID, txnEntry, []*common.ID{block.Fingerprint()})
		assert.Nil(t, err)
		// err = rel.PrepareCompactBlock(block.Fingerprint(), destBlock.Fingerprint())
		destBlockData := destBlock.GetMeta().(*catalog.BlockEntry).GetBlockData()
		assert.Nil(t, err)
		err = txn.Commit()
		assert.Nil(t, err)
		t.Log(destBlockData.PPString(common.PPL1, 0, ""))

		view, err := destBlockData.CollectChangesInRange(0, math.MaxUint64)
		assert.NoError(t, err)
		assert.True(t, view.DeleteMask.Equals(changes.DeleteMask))
	}
}

func TestCompactBlock2(t *testing.T) {
	testutils.EnsureNoLeak(t)
	db := initDB(t, nil)
	defer db.Close()

	worker := ops.NewOpWorker("xx")
	worker.Start()
	defer worker.Stop()
	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 20
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows))
	defer bat.Close()
	createRelationAndAppend(t, db, "db", schema, bat, true)
	var newBlockFp *common.ID
	{
		txn, rel := getDefaultRelation(t, db, schema.Name)
		blkMeta := getOneBlockMeta(rel)
		task, err := jobs.NewCompactBlockTask(tasks.WaitableCtx, txn, blkMeta, db.Scheduler)
		assert.NoError(t, err)
		worker.SendOp(task)
		err = task.WaitDone()
		assert.NoError(t, err)
		newBlockFp = task.GetNewBlock().Fingerprint()
		assert.NoError(t, txn.Commit())
	}
	{
		t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
		txn, rel := getDefaultRelation(t, db, schema.Name)
		t.Log(rel.SimplePPString(common.PPL1))
		seg, err := rel.GetSegment(newBlockFp.SegmentID)
		assert.Nil(t, err)
		blk, err := seg.GetBlock(newBlockFp.BlockID)
		assert.Nil(t, err)
		view, err := blk.GetColumnDataById(3, nil)
		assert.NoError(t, err)
		defer view.Close()
		assert.True(t, view.GetData().Equals(bat.Vecs[3]))
		err = blk.RangeDelete(1, 2, handle.DT_Normal)
		assert.Nil(t, err)
		err = blk.Update(3, 3, int64(999))
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	{
		t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
		txn, rel := getDefaultRelation(t, db, schema.Name)
		t.Log(rel.SimplePPString(common.PPL1))
		seg, err := rel.GetSegment(newBlockFp.SegmentID)
		assert.Nil(t, err)
		blk, err := seg.GetBlock(newBlockFp.BlockID)
		assert.Nil(t, err)
		task, err := jobs.NewCompactBlockTask(tasks.WaitableCtx, txn, blk.GetMeta().(*catalog.BlockEntry), db.Scheduler)
		assert.Nil(t, err)
		worker.SendOp(task)
		err = task.WaitDone()
		assert.Nil(t, err)
		newBlockFp = task.GetNewBlock().Fingerprint()
		assert.Nil(t, txn.Commit())
	}
	{
		t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
		txn, rel := getDefaultRelation(t, db, schema.Name)
		seg, err := rel.GetSegment(newBlockFp.SegmentID)
		assert.Nil(t, err)
		blk, err := seg.GetBlock(newBlockFp.BlockID)
		assert.Nil(t, err)
		view, err := blk.GetColumnDataById(3, nil)
		assert.Nil(t, err)
		defer view.Close()
		assert.Nil(t, view.DeleteMask)
		v := view.GetData().Get(1)
		// t.Logf("view: %v", view.GetData().String())
		// t.Logf("raw : %v", bat.Vecs[3].String())
		assert.Equal(t, int64(999), v)
		assert.Equal(t, bat.Vecs[0].Length()-2, view.Length())

		cnt := 0
		forEachBlock(rel, func(blk handle.Block) (err error) {
			cnt++
			return
		})
		assert.Equal(t, 1, cnt)

		task, err := jobs.NewCompactBlockTask(tasks.WaitableCtx, txn, blk.GetMeta().(*catalog.BlockEntry), db.Scheduler)
		assert.Nil(t, err)
		worker.SendOp(task)
		err = task.WaitDone()
		assert.Nil(t, err)
		newBlockFp = task.GetNewBlock().Fingerprint()
		{
			txn, rel := getDefaultRelation(t, db, schema.Name)
			seg, err := rel.GetSegment(newBlockFp.SegmentID)
			assert.NoError(t, err)
			blk, err := seg.GetBlock(newBlockFp.BlockID)
			assert.NoError(t, err)
			err = blk.RangeDelete(4, 5, handle.DT_Normal)
			assert.NoError(t, err)
			err = blk.Update(3, 3, int64(1999))
			assert.NoError(t, err)
			assert.NoError(t, txn.Commit())
		}
		assert.NoError(t, txn.Commit())
	}
	{
		txn, rel := getDefaultRelation(t, db, schema.Name)
		t.Log(rel.SimplePPString(common.PPL1))
		t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
		seg, err := rel.GetSegment(newBlockFp.SegmentID)
		assert.Nil(t, err)
		blk, err := seg.GetBlock(newBlockFp.BlockID)
		assert.Nil(t, err)
		view, err := blk.GetColumnDataById(3, nil)
		assert.Nil(t, err)
		defer view.Close()
		assert.True(t, view.DeleteMask.Contains(4))
		assert.True(t, view.DeleteMask.Contains(5))
		v := view.GetData().Get(3)
		assert.Equal(t, int64(1999), v)
		assert.Equal(t, bat.Vecs[0].Length()-2, view.Length())

		txn2, rel2 := getDefaultRelation(t, db, schema.Name)
		seg2, err := rel2.GetSegment(newBlockFp.SegmentID)
		assert.NoError(t, err)
		blk2, err := seg2.GetBlock(newBlockFp.BlockID)
		assert.NoError(t, err)
		err = blk2.RangeDelete(7, 7, handle.DT_Normal)
		assert.NoError(t, err)

		task, err := jobs.NewCompactBlockTask(tasks.WaitableCtx, txn, blk.GetMeta().(*catalog.BlockEntry), db.Scheduler)
		assert.NoError(t, err)
		worker.SendOp(task)
		err = task.WaitDone()
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit())

		err = txn2.Commit()
		assert.Error(t, err)
	}
}

func TestAutoCompactABlk1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := initDB(t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 3)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 10

	totalRows := schema.BlockMaxRows / 5
	bat := catalog.MockBatch(schema, int(totalRows))
	defer bat.Close()
	createRelationAndAppend(t, tae, "db", schema, bat, true)
	err := tae.Catalog.Checkpoint(tae.Scheduler.GetSafeTS())
	assert.Nil(t, err)
	testutils.WaitExpect(1000, func() bool {
		return tae.Scheduler.GetPenddingLSNCnt() == 0
	})
	assert.Equal(t, uint64(0), tae.Scheduler.GetPenddingLSNCnt())
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	{
		txn, rel := getDefaultRelation(t, tae, schema.Name)
		blk := getOneBlock(rel)
		blkData := blk.GetMeta().(*catalog.BlockEntry).GetBlockData()
		factory, taskType, scopes, err := blkData.BuildCompactionTaskFactory()
		assert.Nil(t, err)
		task, err := tae.Scheduler.ScheduleMultiScopedTxnTask(tasks.WaitableCtx, taskType, scopes, factory)
		assert.Nil(t, err)
		err = task.WaitDone()
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
}

func TestAutoCompactABlk2(t *testing.T) {
	testutils.EnsureNoLeak(t)
	opts := new(options.Options)
	opts.CacheCfg = new(options.CacheCfg)
	opts.CacheCfg.InsertCapacity = common.K * 5
	opts.CacheCfg.TxnCapacity = common.M
	opts = config.WithQuickScanAndCKPOpts(opts)
	db := initDB(t, opts)
	defer db.Close()

	schema1 := catalog.MockSchemaAll(13, 2)
	schema1.BlockMaxRows = 20
	schema1.SegmentMaxBlocks = 2

	schema2 := catalog.MockSchemaAll(13, 2)
	schema2.BlockMaxRows = 20
	schema2.SegmentMaxBlocks = 2
	{
		txn, _ := db.StartTxn(nil)
		database, err := txn.CreateDatabase("db")
		assert.Nil(t, err)
		_, err = database.CreateRelation(schema1)
		assert.Nil(t, err)
		_, err = database.CreateRelation(schema2)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	bat := catalog.MockBatch(schema1, int(schema1.BlockMaxRows*3-1))
	defer bat.Close()
	bats := bat.Split(bat.Length())

	pool, err := ants.NewPool(20)
	assert.Nil(t, err)
	var wg sync.WaitGroup
	doSearch := func(name string) func() {
		return func() {
			defer wg.Done()
			txn, rel := getDefaultRelation(t, db, name)
			it := rel.MakeBlockIt()
			for it.Valid() {
				blk := it.GetBlock()
				view, err := blk.GetColumnDataById(schema1.GetSingleSortKeyIdx(), nil)
				assert.Nil(t, err)
				view.Close()
				it.Next()
			}
			err := txn.Commit()
			assert.NoError(t, err)
		}
	}

	for _, data := range bats {
		wg.Add(4)
		err := pool.Submit(doSearch(schema1.Name))
		assert.Nil(t, err)
		err = pool.Submit(doSearch(schema2.Name))
		assert.Nil(t, err)
		err = pool.Submit(appendClosure(t, data, schema1.Name, db, &wg))
		assert.Nil(t, err)
		err = pool.Submit(appendClosure(t, data, schema2.Name, db, &wg))
		assert.Nil(t, err)
	}
	wg.Wait()
	testutils.WaitExpect(2000, func() bool {
		return db.Scheduler.GetPenddingLSNCnt() == 0
	})
	assert.Equal(t, uint64(0), db.Scheduler.GetPenddingLSNCnt())
	t.Log(db.MTBufMgr.String())
	t.Log(db.Catalog.SimplePPString(common.PPL1))
	t.Logf("GetPenddingLSNCnt: %d", db.Scheduler.GetPenddingLSNCnt())
	t.Logf("GetCheckpointed: %d", db.Scheduler.GetCheckpointedLSN())
}

func TestCompactABlk(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 3)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 10

	totalRows := schema.BlockMaxRows / 5
	bat := catalog.MockBatch(schema, int(totalRows))
	defer bat.Close()
	createRelationAndAppend(t, tae, "db", schema, bat, true)
	{
		txn, rel := getDefaultRelation(t, tae, schema.Name)
		blk := getOneBlock(rel)
		blkData := blk.GetMeta().(*catalog.BlockEntry).GetBlockData()
		factory, taskType, scopes, err := blkData.BuildCompactionTaskFactory()
		assert.NoError(t, err)
		task, err := tae.Scheduler.ScheduleMultiScopedTxnTask(tasks.WaitableCtx, taskType, scopes, factory)
		assert.NoError(t, err)
		err = task.WaitDone()
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit())
	}
	err := tae.Catalog.Checkpoint(tae.Scheduler.GetSafeTS())
	assert.Nil(t, err)
	testutils.WaitExpect(1000, func() bool {
		return tae.Scheduler.GetPenddingLSNCnt() == 0
	})
	assert.Equal(t, uint64(0), tae.Scheduler.GetPenddingLSNCnt())
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}

func TestRollback1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	db := initDB(t, nil)
	defer db.Close()
	schema := catalog.MockSchema(2, 0)

	createRelation(t, db, "db", schema, true)

	segCnt := 0
	onSegFn := func(segment *catalog.SegmentEntry) error {
		segCnt++
		return nil
	}
	blkCnt := 0
	onBlkFn := func(block *catalog.BlockEntry) error {
		blkCnt++
		return nil
	}
	processor := new(catalog.LoopProcessor)
	processor.SegmentFn = onSegFn
	processor.BlockFn = onBlkFn
	txn, rel := getDefaultRelation(t, db, schema.Name)
	_, err := rel.CreateSegment()
	assert.Nil(t, err)

	tableMeta := rel.GetMeta().(*catalog.TableEntry)
	err = tableMeta.RecurLoop(processor)
	assert.Nil(t, err)
	assert.Equal(t, segCnt, 1)

	assert.Nil(t, txn.Rollback())
	segCnt = 0
	err = tableMeta.RecurLoop(processor)
	assert.Nil(t, err)
	assert.Equal(t, segCnt, 0)

	txn, rel = getDefaultRelation(t, db, schema.Name)
	seg, err := rel.CreateSegment()
	assert.Nil(t, err)
	segMeta := seg.GetMeta().(*catalog.SegmentEntry)
	assert.Nil(t, txn.Commit())
	segCnt = 0
	err = tableMeta.RecurLoop(processor)
	assert.Nil(t, err)
	assert.Equal(t, segCnt, 1)

	txn, rel = getDefaultRelation(t, db, schema.Name)
	seg, err = rel.GetSegment(segMeta.GetID())
	assert.Nil(t, err)
	_, err = seg.CreateBlock()
	assert.Nil(t, err)
	blkCnt = 0
	err = tableMeta.RecurLoop(processor)
	assert.Nil(t, err)
	assert.Equal(t, blkCnt, 1)

	err = txn.Rollback()
	assert.Nil(t, err)
	blkCnt = 0
	err = tableMeta.RecurLoop(processor)
	assert.Nil(t, err)
	assert.Equal(t, blkCnt, 0)

	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
}

func TestMVCC1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	db := initDB(t, nil)
	defer db.Close()
	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 40
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*10))
	defer bat.Close()
	bats := bat.Split(40)

	txn, _, rel := createRelationNoCommit(t, db, "db", schema, true)
	err := rel.Append(bats[0])
	assert.NoError(t, err)

	row := 5
	expectVal := bats[0].Vecs[schema.GetSingleSortKeyIdx()].Get(row)
	filter := handle.NewEQFilter(expectVal)
	actualVal, err := rel.GetValueByFilter(filter, schema.GetSingleSortKeyIdx())
	assert.NoError(t, err)
	assert.Equal(t, expectVal, actualVal)
	assert.NoError(t, txn.Commit())

	_, rel = getDefaultRelation(t, db, schema.Name)
	actualVal, err = rel.GetValueByFilter(filter, schema.GetSingleSortKeyIdx())
	assert.NoError(t, err)
	assert.Equal(t, expectVal, actualVal)

	txn2, rel2 := getDefaultRelation(t, db, schema.Name)
	err = rel2.Append(bats[1])
	assert.NoError(t, err)

	val2 := bats[1].Vecs[schema.GetSingleSortKeyIdx()].Get(row)
	filter.Val = val2
	actualVal, err = rel2.GetValueByFilter(filter, schema.GetSingleSortKeyIdx())
	assert.NoError(t, err)
	assert.Equal(t, val2, actualVal)

	assert.NoError(t, txn2.Commit())

	_, _, err = rel.GetByFilter(filter)
	assert.Error(t, err)
	var id *common.ID

	{
		txn, rel := getDefaultRelation(t, db, schema.Name)
		id, _, err = rel.GetByFilter(filter)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit())
	}

	it := rel.MakeBlockIt()
	for it.Valid() {
		block := it.GetBlock()
		bid := block.Fingerprint()
		if bid.BlockID == id.BlockID {
			var buffer bytes.Buffer
			view, err := block.GetColumnDataById(schema.GetSingleSortKeyIdx(), &buffer)
			assert.Nil(t, err)
			defer view.Close()
			assert.Nil(t, view.DeleteMask)
			assert.NotNil(t, view.GetData())
			t.Log(view.GetData().String())
			assert.Equal(t, bats[0].Vecs[0].Length(), view.Length())
		}
		it.Next()
	}
}

// 1. Txn1 create db, relation and append 10 rows. committed -- PASS
// 2. Txn2 append 10 rows. Get the 5th append row value -- PASS
// 3. Txn2 delete the 5th row value in uncommitted state -- PASS
// 4. Txn2 get the 5th row value -- NotFound
func TestMVCC2(t *testing.T) {
	testutils.EnsureNoLeak(t)
	db := initDB(t, nil)
	defer db.Close()
	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 100
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows))
	defer bat.Close()
	bats := bat.Split(10)
	{
		txn, _, rel := createRelationNoCommit(t, db, "db", schema, true)
		err := rel.Append(bats[0])
		assert.NoError(t, err)
		val := bats[0].Vecs[schema.GetSingleSortKeyIdx()].Get(5)
		filter := handle.NewEQFilter(val)
		_, _, err = rel.GetByFilter(filter)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit())
	}
	{
		txn, rel := getDefaultRelation(t, db, schema.Name)
		err := rel.Append(bats[1])
		assert.NoError(t, err)
		val := bats[1].Vecs[schema.GetSingleSortKeyIdx()].Get(5)
		filter := handle.NewEQFilter(val)
		err = rel.DeleteByFilter(filter)
		assert.NoError(t, err)

		_, _, err = rel.GetByFilter(filter)
		assert.Error(t, err)
		t.Log(err)
		assert.NoError(t, txn.Commit())
	}
	{
		txn, rel := getDefaultRelation(t, db, schema.Name)
		it := rel.MakeBlockIt()
		var buffer bytes.Buffer
		for it.Valid() {
			block := it.GetBlock()
			view, err := block.GetColumnDataByName(schema.GetSingleSortKey().Name, &buffer)
			assert.Nil(t, err)
			defer view.Close()
			assert.Nil(t, view.DeleteMask)
			// TODO: exclude deleted rows when apply appends
			assert.Equal(t, bats[1].Vecs[0].Length()*2-1, view.Length())
			it.Next()
		}
		assert.NoError(t, txn.Commit())
	}
}

func TestUnload1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	opts := new(options.Options)
	opts.CacheCfg = new(options.CacheCfg)
	opts.CacheCfg.InsertCapacity = common.K
	opts.CacheCfg.TxnCapacity = common.M
	db := initDB(t, opts)
	defer db.Close()

	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2

	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*2))
	defer bat.Close()
	bats := bat.Split(int(schema.BlockMaxRows))
	createRelation(t, db, "db", schema, true)
	var wg sync.WaitGroup
	pool, err := ants.NewPool(1)
	assert.Nil(t, err)
	for _, data := range bats {
		wg.Add(1)
		err := pool.Submit(appendClosure(t, data, schema.Name, db, &wg))
		assert.Nil(t, err)
	}
	wg.Wait()
	{
		txn, rel := getDefaultRelation(t, db, schema.Name)
		for i := 0; i < 10; i++ {
			it := rel.MakeBlockIt()
			for it.Valid() {
				blk := it.GetBlock()
				view, err := blk.GetColumnDataByName(schema.GetSingleSortKey().Name, nil)
				assert.Nil(t, err)
				defer view.Close()
				assert.Equal(t, int(schema.BlockMaxRows), view.Length())
				it.Next()
			}
		}
		_ = txn.Commit()
	}
	t.Log(common.GPool.String())
}

func TestUnload2(t *testing.T) {
	testutils.EnsureNoLeak(t)
	opts := new(options.Options)
	opts.CacheCfg = new(options.CacheCfg)
	opts.CacheCfg.InsertCapacity = common.K*4 - common.K/2
	opts.CacheCfg.TxnCapacity = common.M
	db := initDB(t, opts)
	defer db.Close()

	schema1 := catalog.MockSchemaAll(13, 2)
	schema1.BlockMaxRows = 10
	schema1.SegmentMaxBlocks = 2

	schema2 := catalog.MockSchemaAll(13, 2)
	schema2.BlockMaxRows = 10
	schema2.SegmentMaxBlocks = 2
	{
		txn, _ := db.StartTxn(nil)
		database, err := txn.CreateDatabase("db")
		assert.Nil(t, err)
		_, err = database.CreateRelation(schema1)
		assert.Nil(t, err)
		_, err = database.CreateRelation(schema2)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}

	bat := catalog.MockBatch(schema1, int(schema1.BlockMaxRows*5+5))
	defer bat.Close()
	bats := bat.Split(bat.Length())

	p, err := ants.NewPool(10)
	assert.Nil(t, err)
	var wg sync.WaitGroup
	for i, data := range bats {
		wg.Add(1)
		name := schema1.Name
		if i%2 == 1 {
			name = schema2.Name
		}
		err := p.Submit(appendClosure(t, data, name, db, &wg))
		assert.Nil(t, err)
	}
	wg.Wait()

	{
		txn, rel := getDefaultRelation(t, db, schema1.Name)
		for i := 0; i < len(bats); i += 2 {
			data := bats[i]
			v := data.Vecs[schema1.GetSingleSortKeyIdx()].Get(0)
			filter := handle.NewEQFilter(v)
			_, _, err := rel.GetByFilter(filter)
			assert.NoError(t, err)
		}
		database, _ := txn.GetDatabase("db")
		rel, err = database.GetRelationByName(schema2.Name)
		assert.Nil(t, err)
		for i := 1; i < len(bats); i += 2 {
			data := bats[i]
			v := data.Vecs[schema1.GetSingleSortKeyIdx()].Get(0)
			filter := handle.NewEQFilter(v)
			_, _, err := rel.GetByFilter(filter)
			assert.NoError(t, err)
		}
		_ = txn.Commit()
	}

	t.Log(db.MTBufMgr.String())
	// t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
}

func TestDelete1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()

	schema := catalog.MockSchemaAll(3, 2)
	schema.BlockMaxRows = 10
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows))
	defer bat.Close()
	createRelationAndAppend(t, tae, "db", schema, bat, true)
	var id *common.ID
	var row uint32
	{
		txn, rel := getDefaultRelation(t, tae, schema.Name)
		assert.Equal(t, bat.Length(), int(rel.Rows()))
		pkCol := bat.Vecs[schema.GetSingleSortKeyIdx()]
		pkVal := pkCol.Get(5)
		filter := handle.NewEQFilter(pkVal)
		var err error
		id, row, err = rel.GetByFilter(filter)
		assert.NoError(t, err)
		err = rel.RangeDelete(id, row, row, handle.DT_Normal)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit())
	}
	{
		txn, rel := getDefaultRelation(t, tae, schema.Name)
		assert.Equal(t, bat.Length()-1, int(rel.Rows()))
		pkCol := bat.Vecs[schema.GetSingleSortKeyIdx()]
		pkVal := pkCol.Get(5)
		filter := handle.NewEQFilter(pkVal)
		_, _, err := rel.GetByFilter(filter)
		assert.Error(t, err)
		assert.NoError(t, txn.Commit())
	}
	{
		txn, rel := getDefaultRelation(t, tae, schema.Name)
		blkMeta := getOneBlockMeta(rel)
		blkData := blkMeta.GetBlockData()
		factory, taskType, scopes, err := blkData.BuildCompactionTaskFactory()
		assert.NoError(t, err)
		task, err := tae.Scheduler.ScheduleMultiScopedTxnTask(tasks.WaitableCtx, taskType, scopes, factory)
		assert.NoError(t, err)
		err = task.WaitDone()
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit())
	}
	{
		txn, rel := getDefaultRelation(t, tae, schema.Name)
		blk := getOneBlock(rel)
		view, err := blk.GetColumnDataById(schema.GetSingleSortKeyIdx(), nil)
		assert.NoError(t, err)
		defer view.Close()
		assert.Nil(t, view.DeleteMask)
		assert.Equal(t, bat.Vecs[0].Length()-1, view.Length())

		err = blk.RangeDelete(0, 0, handle.DT_Normal)
		assert.NoError(t, err)
		view, err = blk.GetColumnDataById(schema.GetSingleSortKeyIdx(), nil)
		assert.NoError(t, err)
		defer view.Close()
		assert.True(t, view.DeleteMask.Contains(0))
		v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(0)
		filter := handle.NewEQFilter(v)
		_, _, err = rel.GetByFilter(filter)
		assert.Equal(t, data.ErrNotFound, err)
		assert.NoError(t, txn.Commit())
	}
	{
		txn, rel := getDefaultRelation(t, tae, schema.Name)
		assert.Equal(t, bat.Length()-2, int(rel.Rows()))
		blk := getOneBlock(rel)
		view, err := blk.GetColumnDataById(schema.GetSingleSortKeyIdx(), nil)
		assert.NoError(t, err)
		defer view.Close()
		assert.True(t, view.DeleteMask.Contains(0))
		assert.Equal(t, bat.Vecs[0].Length()-1, view.Length())
		v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(0)
		filter := handle.NewEQFilter(v)
		_, _, err = rel.GetByFilter(filter)
		assert.ErrorIs(t, err, data.ErrNotFound)
		_ = txn.Rollback()
	}
	t.Log(tae.Opts.Catalog.SimplePPString(common.PPL1))
}

func TestLogIndex1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 0)
	schema.BlockMaxRows = 10
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows))
	defer bat.Close()
	bats := bat.Split(int(schema.BlockMaxRows))
	createRelation(t, tae, "db", schema, true)
	txns := make([]txnif.AsyncTxn, 0)
	doAppend := func(data *containers.Batch) func() {
		return func() {
			txn, rel := getDefaultRelation(t, tae, schema.Name)
			err := rel.Append(data)
			assert.NoError(t, err)
			assert.NoError(t, txn.Commit())
			txns = append(txns, txn)
		}
	}
	for _, data := range bats {
		doAppend(data)()
	}
	var id *common.ID
	var offset uint32
	var err error
	{
		txn, rel := getDefaultRelation(t, tae, schema.Name)
		v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(3)
		filter := handle.NewEQFilter(v)
		id, offset, err = rel.GetByFilter(filter)
		assert.Nil(t, err)
		err = rel.RangeDelete(id, offset, offset, handle.DT_Normal)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	{
		txn, rel := getDefaultRelation(t, tae, schema.Name)
		meta := getOneBlockMeta(rel)
		indexes, err := meta.GetBlockData().CollectAppendLogIndexes(txns[0].GetStartTS(), txns[len(txns)-1].GetCommitTS())
		assert.NoError(t, err)
		assert.Equal(t, len(txns), len(indexes))
		indexes, err = meta.GetBlockData().CollectAppendLogIndexes(txns[1].GetStartTS(), txns[len(txns)-1].GetCommitTS())
		assert.NoError(t, err)
		assert.Equal(t, len(txns)-1, len(indexes))
		indexes, err = meta.GetBlockData().CollectAppendLogIndexes(txns[2].GetCommitTS(), txns[len(txns)-1].GetCommitTS())
		assert.NoError(t, err)
		assert.Equal(t, len(txns)-2, len(indexes))
		indexes, err = meta.GetBlockData().CollectAppendLogIndexes(txns[3].GetCommitTS(), txns[len(txns)-1].GetCommitTS())
		assert.NoError(t, err)
		assert.Equal(t, len(txns)-3, len(indexes))
		assert.NoError(t, txn.Commit())
	}
	{
		txn, rel := getDefaultRelation(t, tae, schema.Name)
		blk := getOneBlock(rel)
		meta := blk.GetMeta().(*catalog.BlockEntry)
		indexes, err := meta.GetBlockData().CollectAppendLogIndexes(1, txn.GetStartTS())
		assert.NoError(t, err)
		for i, index := range indexes {
			t.Logf("%d: %s", i, index.String())
		}

		view, err := blk.GetColumnDataById(schema.GetSingleSortKeyIdx(), nil)
		assert.Nil(t, err)
		defer view.Close()
		assert.True(t, view.DeleteMask.Contains(offset))
		task, err := jobs.NewCompactBlockTask(nil, txn, meta, tae.Scheduler)
		assert.Nil(t, err)
		err = task.OnExec()
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
}

func TestCrossDBTxn(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()

	txn, _ := tae.StartTxn(nil)
	db1, err := txn.CreateDatabase("db1")
	assert.Nil(t, err)
	db2, err := txn.CreateDatabase("db2")
	assert.Nil(t, err)
	assert.NotNil(t, db1)
	assert.NotNil(t, db2)
	assert.Nil(t, txn.Commit())

	schema1 := catalog.MockSchema(2, 0)
	schema1.BlockMaxRows = 10
	schema1.SegmentMaxBlocks = 2
	schema2 := catalog.MockSchema(4, 0)
	schema2.BlockMaxRows = 10
	schema2.SegmentMaxBlocks = 2

	rows1 := schema1.BlockMaxRows * 5 / 2
	rows2 := schema1.BlockMaxRows * 3 / 2
	bat1 := catalog.MockBatch(schema1, int(rows1))
	bat2 := catalog.MockBatch(schema2, int(rows2))
	defer bat1.Close()
	defer bat2.Close()

	txn, _ = tae.StartTxn(nil)
	db1, err = txn.GetDatabase("db1")
	assert.Nil(t, err)
	db2, err = txn.GetDatabase("db2")
	assert.Nil(t, err)
	rel1, err := db1.CreateRelation(schema1)
	assert.Nil(t, err)
	rel2, err := db2.CreateRelation(schema2)
	assert.Nil(t, err)
	err = rel1.Append(bat1)
	assert.Nil(t, err)
	err = rel2.Append(bat2)
	assert.Nil(t, err)

	assert.Nil(t, txn.Commit())

	txn, _ = tae.StartTxn(nil)
	db1, err = txn.GetDatabase("db1")
	assert.NoError(t, err)
	db2, err = txn.GetDatabase("db2")
	assert.NoError(t, err)
	rel1, err = db1.GetRelationByName(schema1.Name)
	assert.NoError(t, err)
	rel2, err = db2.GetRelationByName(schema2.Name)
	assert.NoError(t, err)

	checkAllColRowsByScan(t, rel1, int(rows1), false)
	checkAllColRowsByScan(t, rel2, int(rows2), false)

	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}

func TestSystemDB1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchema(2, 0)
	txn, _ := tae.StartTxn(nil)
	_, err := txn.CreateDatabase(catalog.SystemDBName)
	assert.NotNil(t, err)
	_, err = txn.DropDatabase(catalog.SystemDBName)
	assert.NotNil(t, err)

	db1, err := txn.CreateDatabase("db1")
	assert.Nil(t, err)
	_, err = db1.CreateRelation(schema)
	assert.Nil(t, err)

	_, err = txn.CreateDatabase("db2")
	assert.Nil(t, err)

	db, _ := txn.GetDatabase(catalog.SystemDBName)
	table, err := db.GetRelationByName(catalog.SystemTable_DB_Name)
	assert.Nil(t, err)
	it := table.MakeBlockIt()
	rows := 0
	for it.Valid() {
		blk := it.GetBlock()
		rows += blk.Rows()
		view, err := blk.GetColumnDataByName(catalog.SystemDBAttr_Name, nil)
		assert.Nil(t, err)
		defer view.Close()
		assert.Equal(t, 3, view.Length())
		view, err = blk.GetColumnDataByName(catalog.SystemDBAttr_CatalogName, nil)
		assert.Nil(t, err)
		defer view.Close()
		assert.Equal(t, 3, view.Length())
		view, err = blk.GetColumnDataByName(catalog.SystemDBAttr_CreateSQL, nil)
		assert.Nil(t, err)
		defer view.Close()
		assert.Equal(t, 3, view.Length())
		it.Next()
	}
	assert.Equal(t, 3, rows)

	table, err = db.GetRelationByName(catalog.SystemTable_Table_Name)
	assert.Nil(t, err)
	it = table.MakeBlockIt()
	rows = 0
	for it.Valid() {
		blk := it.GetBlock()
		rows += blk.Rows()
		view, err := blk.GetColumnDataByName(catalog.SystemRelAttr_Name, nil)
		assert.Nil(t, err)
		defer view.Close()
		assert.Equal(t, 4, view.Length())
		view, err = blk.GetColumnDataByName(catalog.SystemRelAttr_Persistence, nil)
		assert.NoError(t, err)
		defer view.Close()
		view, err = blk.GetColumnDataByName(catalog.SystemRelAttr_Kind, nil)
		assert.NoError(t, err)
		defer view.Close()
		it.Next()
	}
	assert.Equal(t, 4, rows)

	table, err = db.GetRelationByName(catalog.SystemTable_Columns_Name)
	assert.Nil(t, err)

	bat := containers.NewBatch()
	defer bat.Close()
	// schema2 := table.GetMeta().(*catalog.TableEntry).GetSchema()
	// bat := containers.BuildBatch(schema2.AllNames(), schema2.AllTypes(), schema2.AllNullables(), 0)
	it = table.MakeBlockIt()
	rows = 0
	for it.Valid() {
		blk := it.GetBlock()
		rows += blk.Rows()
		view, err := blk.GetColumnDataByName(catalog.SystemColAttr_DBName, nil)
		assert.NoError(t, err)
		defer view.Close()
		bat.AddVector(catalog.SystemColAttr_DBName, view.Orphan())

		view, err = blk.GetColumnDataByName(catalog.SystemColAttr_RelName, nil)
		assert.Nil(t, err)
		defer view.Close()
		bat.AddVector(catalog.SystemColAttr_RelName, view.Orphan())

		view, err = blk.GetColumnDataByName(catalog.SystemColAttr_Name, nil)
		assert.Nil(t, err)
		defer view.Close()
		bat.AddVector(catalog.SystemColAttr_Name, view.Orphan())

		view, err = blk.GetColumnDataByName(catalog.SystemColAttr_ConstraintType, nil)
		assert.Nil(t, err)
		defer view.Close()
		t.Log(view.GetData().String())
		bat.AddVector(catalog.SystemColAttr_ConstraintType, view.Orphan())

		view, err = blk.GetColumnDataByName(catalog.SystemColAttr_Type, nil)
		assert.Nil(t, err)
		defer view.Close()
		t.Log(view.GetData().String())
		view, err = blk.GetColumnDataByName(catalog.SystemColAttr_Num, nil)
		assert.Nil(t, err)
		defer view.Close()
		t.Log(view.GetData().String())
		it.Next()
	}
	t.Log(rows)

	for i := 0; i < bat.Vecs[0].Length(); i++ {
		dbName := bat.Vecs[0].Get(i)
		relName := bat.Vecs[1].Get(i)
		attrName := bat.Vecs[2].Get(i)
		ct := bat.Vecs[3].Get(i)
		t.Logf("%s,%s,%s,%s", dbName, relName, attrName, ct)
		if dbName == catalog.SystemDBName {
			if relName == catalog.SystemTable_DB_Name {
				if attrName == catalog.SystemDBAttr_Name {
					assert.Equal(t, catalog.SystemColPKConstraint, ct)
				} else {
					assert.Equal(t, catalog.SystemColNoConstraint, ct)
				}
			} else if relName == catalog.SystemTable_Table_Name {
				if attrName == catalog.SystemRelAttr_DBName || attrName == catalog.SystemRelAttr_Name {
					assert.Equal(t, catalog.SystemColPKConstraint, ct)
				} else {
					assert.Equal(t, catalog.SystemColNoConstraint, ct)
				}
			} else if relName == catalog.SystemTable_Columns_Name {
				if attrName == catalog.SystemColAttr_DBName || attrName == catalog.SystemColAttr_RelName || attrName == catalog.SystemColAttr_Name {
					assert.Equal(t, catalog.SystemColPKConstraint, ct)
				} else {
					assert.Equal(t, catalog.SystemColNoConstraint, ct)
				}
			}
		}
	}

	err = txn.Rollback()
	assert.Nil(t, err)
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
}

func TestSystemDB2(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()

	txn, _ := tae.StartTxn(nil)
	sysDB, err := txn.GetDatabase(catalog.SystemDBName)
	assert.NoError(t, err)
	_, err = sysDB.DropRelationByName(catalog.SystemTable_DB_Name)
	assert.Error(t, err)
	_, err = sysDB.DropRelationByName(catalog.SystemTable_Table_Name)
	assert.Error(t, err)
	_, err = sysDB.DropRelationByName(catalog.SystemTable_Columns_Name)
	assert.Error(t, err)

	schema := catalog.MockSchema(2, 0)
	schema.BlockMaxRows = 100
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockBatch(schema, 1000)
	defer bat.Close()

	rel, err := sysDB.CreateRelation(schema)
	assert.NoError(t, err)
	assert.NotNil(t, rel)
	err = rel.Append(bat)
	assert.Nil(t, err)
	assert.NoError(t, txn.Commit())

	txn, _ = tae.StartTxn(nil)
	sysDB, err = txn.GetDatabase(catalog.SystemDBName)
	assert.NoError(t, err)
	rel, err = sysDB.GetRelationByName(schema.Name)
	assert.NoError(t, err)
	checkAllColRowsByScan(t, rel, 1000, false)
	assert.NoError(t, txn.Commit())
}

func TestSystemDB3(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	txn, _ := tae.StartTxn(nil)
	schema := catalog.MockSchemaAll(13, 12)
	schema.BlockMaxRows = 100
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockBatch(schema, 20)
	defer bat.Close()
	db, err := txn.GetDatabase(catalog.SystemDBName)
	assert.NoError(t, err)
	rel, err := db.CreateRelation(schema)
	assert.NoError(t, err)
	err = rel.Append(bat)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())
}

func TestScan1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()

	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 100
	schema.SegmentMaxBlocks = 2

	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows-1))
	defer bat.Close()
	txn, _, rel := createRelationNoCommit(t, tae, defaultTestDB, schema, true)
	err := rel.Append(bat)
	assert.NoError(t, err)
	checkAllColRowsByScan(t, rel, bat.Length(), false)
	assert.NoError(t, txn.Commit())
}

func TestDedup(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()

	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 100
	schema.SegmentMaxBlocks = 2

	bat := catalog.MockBatch(schema, 10)
	defer bat.Close()
	txn, _, rel := createRelationNoCommit(t, tae, defaultTestDB, schema, true)
	err := rel.Append(bat)
	assert.NoError(t, err)
	err = rel.Append(bat)
	t.Log(err)
	assert.ErrorIs(t, err, data.ErrDuplicate)
	checkAllColRowsByScan(t, rel, 10, false)
	err = txn.Rollback()
	assert.NoError(t, err)
}

func TestScan2(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 10
	rows := schema.BlockMaxRows * 5 / 2
	bat := catalog.MockBatch(schema, int(rows))
	defer bat.Close()
	bats := bat.Split(2)

	txn, _, rel := createRelationNoCommit(t, tae, defaultTestDB, schema, true)
	err := rel.Append(bats[0])
	assert.NoError(t, err)
	checkAllColRowsByScan(t, rel, bats[0].Length(), false)

	err = rel.Append(bats[0])
	assert.Error(t, err)
	err = rel.Append(bats[1])
	assert.NoError(t, err)
	checkAllColRowsByScan(t, rel, int(rows), false)

	pkv := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(5)
	filter := handle.NewEQFilter(pkv)
	err = rel.DeleteByFilter(filter)
	assert.NoError(t, err)
	checkAllColRowsByScan(t, rel, int(rows)-1, true)

	pkv = bat.Vecs[schema.GetSingleSortKeyIdx()].Get(8)
	filter = handle.NewEQFilter(pkv)
	updateV := int64(999)
	err = rel.UpdateByFilter(filter, 3, updateV)
	assert.NoError(t, err)

	v, err := rel.GetValueByFilter(filter, 3)
	assert.NoError(t, err)
	assert.Equal(t, updateV, v.(int64))
	checkAllColRowsByScan(t, rel, int(rows)-1, true)
	assert.NoError(t, txn.Commit())
}

func TestUpdatePrimaryKey(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	bat := catalog.MockBatch(schema, 100)
	defer bat.Close()
	createRelationAndAppend(t, tae, "db", schema, bat, true)

	txn, rel := getDefaultRelation(t, tae, schema.Name)
	v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(2)
	filter := handle.NewEQFilter(v)
	id, row, err := rel.GetByFilter(filter)
	assert.NoError(t, err)
	err = rel.Update(id, row, uint16(schema.GetSingleSortKeyIdx()), v)
	assert.Error(t, err)
	assert.NoError(t, txn.Commit())
}

func TestADA(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 3)
	schema.BlockMaxRows = 1000
	bat := catalog.MockBatch(schema, 1)
	defer bat.Close()

	// Append to a block
	createRelationAndAppend(t, tae, "db", schema, bat, true)

	// Delete a row from the block
	txn, rel := getDefaultRelation(t, tae, schema.Name)
	v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(0)
	filter := handle.NewEQFilter(v)
	id, row, err := rel.GetByFilter(filter)
	assert.NoError(t, err)
	err = rel.RangeDelete(id, row, row, handle.DT_Normal)
	assert.NoError(t, err)
	_, _, err = rel.GetByFilter(filter)
	assert.Error(t, err)
	assert.NoError(t, txn.Commit())

	// Append a row with the same primary key
	txn, rel = getDefaultRelation(t, tae, schema.Name)
	_, _, err = rel.GetByFilter(filter)
	assert.Error(t, err)
	err = rel.Append(bat)
	assert.NoError(t, err)
	id, row, err = rel.GetByFilter(filter)
	assert.NoError(t, err)
	checkAllColRowsByScan(t, rel, 1, true)

	err = rel.RangeDelete(id, row, row, handle.DT_Normal)
	assert.NoError(t, err)
	_, _, err = rel.GetByFilter(filter)
	assert.Error(t, err)

	err = rel.Append(bat)
	assert.NoError(t, err)
	_, _, err = rel.GetByFilter(filter)
	assert.NoError(t, err)
	checkAllColRowsByScan(t, rel, 1, true)
	assert.NoError(t, txn.Commit())

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	err = rel.Append(bat)
	assert.Error(t, err)
	id, row, err = rel.GetByFilter(filter)
	assert.NoError(t, err)
	err = rel.RangeDelete(id, row, row, handle.DT_Normal)
	assert.NoError(t, err)
	_, _, err = rel.GetByFilter(filter)
	assert.Error(t, err)

	err = rel.Append(bat)
	assert.NoError(t, err)

	id, row, err = rel.GetByFilter(filter)
	assert.NoError(t, err)

	err = rel.Append(bat)
	assert.Error(t, err)

	err = rel.RangeDelete(id, row, row, handle.DT_Normal)
	assert.NoError(t, err)
	_, _, err = rel.GetByFilter(filter)
	assert.Error(t, err)
	err = rel.Append(bat)
	assert.NoError(t, err)

	assert.NoError(t, txn.Commit())

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	err = rel.Append(bat)
	assert.Error(t, err)
	id, row, err = rel.GetByFilter(filter)
	assert.NoError(t, err)
	err = rel.RangeDelete(id, row, row, handle.DT_Normal)
	assert.NoError(t, err)
	_, _, err = rel.GetByFilter(filter)
	assert.Error(t, err)

	err = rel.Append(bat)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	it := rel.MakeBlockIt()
	for it.Valid() {
		blk := it.GetBlock()
		view, err := blk.GetColumnDataById(schema.GetSingleSortKeyIdx(), nil)
		assert.NoError(t, err)
		defer view.Close()
		assert.Equal(t, 4, view.Length())
		assert.Equal(t, uint64(3), view.DeleteMask.GetCardinality())
		it.Next()
	}
	assert.NoError(t, txn.Commit())
}

func TestUpdateByFilter(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 3)
	bat := catalog.MockBatch(schema, 100)
	defer bat.Close()

	createRelationAndAppend(t, tae, "db", schema, bat, true)

	txn, rel := getDefaultRelation(t, tae, schema.Name)
	v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(2)
	filter := handle.NewEQFilter(v)
	err := rel.UpdateByFilter(filter, 2, int32(2222))
	assert.NoError(t, err)

	id, row, err := rel.GetByFilter(filter)
	assert.NoError(t, err)
	cv, err := rel.GetValue(id, row, 2)
	assert.NoError(t, err)
	assert.Equal(t, int32(2222), cv.(int32))

	v = bat.Vecs[schema.GetSingleSortKeyIdx()].Get(3)
	filter = handle.NewEQFilter(v)

	err = rel.UpdateByFilter(filter, uint16(schema.GetSingleSortKeyIdx()), int64(333333))
	assert.NoError(t, err)

	assert.NoError(t, txn.Commit())
}

// Test Steps
// 1. Create DB|Relation and append 10 rows. Commit
// 2. Make a equal filter with value of the pk of the second inserted row
// 3. Start Txn1. GetByFilter return PASS
// 4. Start Txn2. Delete row 2. Commit.
// 5. Txn1 call GetByFilter and should return PASS
func TestGetByFilter(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	bat := catalog.MockBatch(schema, 10)
	defer bat.Close()

	// Step 1
	createRelationAndAppend(t, tae, "db", schema, bat, true)

	// Step 2
	v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(2)
	filter := handle.NewEQFilter(v)

	// Step 3
	txn1, rel := getDefaultRelation(t, tae, schema.Name)
	id, row, err := rel.GetByFilter(filter)
	assert.NoError(t, err)

	// Step 4
	{
		txn2, rel := getDefaultRelation(t, tae, schema.Name)
		err := rel.RangeDelete(id, row, row, handle.DT_Normal)
		assert.NoError(t, err)
		assert.NoError(t, txn2.Commit())
	}

	// Step 5
	_, _, err = rel.GetByFilter(filter)
	assert.NoError(t, err)
	assert.NoError(t, txn1.Commit())
}

// 1. Set a big BlockMaxRows
// 2. Mock one row batch
// 3. Start tones of workers. Each work execute below routines:
//    3.1 GetByFilter a pk val
//        3.1.1 If found, go to 3.5
//    3.2 Append a row
//    3.3 err should not be duplicated(TODO: now is duplicated, should be W-W conflict)
//        (why not duplicated: previous GetByFilter had checked that there was no duplicate key)
//    3.4 If no error. try commit. If commit ok, inc appendedcnt. If error, rollback
//    3.5 Delete the row
//        3.5.1 If no error. try commit. commit should always pass
//        3.5.2 If error, should always be w-w conflict
// 4. Wait done all workers. Check the raw row count of table, should be same with appendedcnt.
func TestChaos1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	schema.BlockMaxRows = 100000
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockBatch(schema, 1)
	defer bat.Close()

	createRelation(t, tae, "db", schema, true)

	v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(0)
	filter := handle.NewEQFilter(v)
	var wg sync.WaitGroup
	appendCnt := uint32(0)
	deleteCnt := uint32(0)
	worker := func() {
		defer wg.Done()
		txn, rel := getDefaultRelation(t, tae, schema.Name)
		id, row, err := rel.GetByFilter(filter)
		// logutil.Infof("id=%v,row=%d,err=%v", id, row, err)
		if err == nil {
			err = rel.RangeDelete(id, row, row, handle.DT_Normal)
			if err != nil {
				t.Logf("delete: %v", err)
				// assert.Equal(t, txnif.ErrTxnWWConflict, err)
				assert.NoError(t, txn.Rollback())
				return
			}
			assert.NoError(t, txn.Commit())
			atomic.AddUint32(&deleteCnt, uint32(1))
			return
		}
		assert.Equal(t, data.ErrNotFound, err)
		err = rel.Append(bat)
		// TODO: enable below check later
		// assert.NotEqual(t, data.ErrDuplicate, err)
		if err == nil {
			err = txn.Commit()
			// TODO: enable below check later
			// assert.NotEqual(t, data.ErrDuplicate, err)
			if err == nil {
				atomic.AddUint32(&appendCnt, uint32(1))
			} else {
				t.Logf("commit: %v", err)
			}
			return
		}
		_ = txn.Rollback()
	}
	pool, _ := ants.NewPool(10)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		err := pool.Submit(worker)
		assert.Nil(t, err)
	}
	wg.Wait()
	t.Logf("AppendCnt: %d", appendCnt)
	t.Logf("DeleteCnt: %d", deleteCnt)
	assert.True(t, appendCnt-deleteCnt <= 1)
	_, rel := getDefaultRelation(t, tae, schema.Name)
	assert.Equal(t, int64(appendCnt-deleteCnt), rel.Rows())
	blk := getOneBlock(rel)
	view, err := blk.GetColumnDataById(schema.GetSingleSortKeyIdx(), nil)
	assert.NoError(t, err)
	defer view.Close()
	assert.Equal(t, int(appendCnt), view.Length())
	mask := view.DeleteMask
	view.ApplyDeletes()
	t.Log(view.String())
	assert.Equal(t, uint64(deleteCnt), mask.GetCardinality())
}

// Testing Steps
// 1. Append 10 rows
// 2. Start txn1
// 3. Start txn2. Update the 3rd row 3rd col to int64(2222) and commit. -- PASS
// 4. Txn1 try to update the 3rd row 3rd col to int64(1111). -- W-W Conflict.
// 5. Txn1 try to delete the 3rd row. W-W Conflict. Rollback
// 6. Start txn3 and try to update th3 3rd row 3rd col to int64(3333). -- PASS
func TestSnapshotIsolation1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	schema.BlockMaxRows = 100
	bat := catalog.MockBatch(schema, 10)
	defer bat.Close()
	v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(3)
	filter := handle.NewEQFilter(v)

	// Step 1
	createRelationAndAppend(t, tae, "db", schema, bat, true)

	// Step 2
	txn1, rel1 := getDefaultRelation(t, tae, schema.Name)

	// Step 3
	txn2, rel2 := getDefaultRelation(t, tae, schema.Name)
	err := rel2.UpdateByFilter(filter, 3, int64(2222))
	assert.NoError(t, err)
	assert.NoError(t, txn2.Commit())

	// Step 4
	err = rel1.UpdateByFilter(filter, 3, int64(1111))
	t.Log(err)
	assert.ErrorIs(t, err, txnif.ErrTxnWWConflict)

	// Step 5
	id, row, err := rel1.GetByFilter(filter)
	assert.NoError(t, err)
	err = rel1.RangeDelete(id, row, row, handle.DT_Normal)
	t.Log(err)
	assert.ErrorIs(t, err, txnif.ErrTxnWWConflict)
	_ = txn1.Rollback()

	// Step 6
	txn3, rel3 := getDefaultRelation(t, tae, schema.Name)
	err = rel3.UpdateByFilter(filter, 3, int64(3333))
	assert.NoError(t, err)
	assert.NoError(t, txn3.Commit())

	txn, rel := getDefaultRelation(t, tae, schema.Name)
	v, err = rel.GetValueByFilter(filter, 3)
	assert.NoError(t, err)
	assert.Equal(t, int64(3333), v.(int64))
	err = rel.RangeDelete(id, row, row, handle.DT_Normal)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())
}

// Testing Steps
// 1. Start txn1
// 2. Start txn2 and append one row and commit
// 3. Start txn3 and delete the row and commit
// 4. Txn1 try to append the row. (W-W). Rollback
// 5. Start txn4 and append the row.
func TestSnapshotIsolation2(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	schema.BlockMaxRows = 100
	bat := catalog.MockBatch(schema, 1)
	defer bat.Close()
	v := bat.Vecs[schema.GetSingleSortKeyIdx()].Get(0)
	filter := handle.NewEQFilter(v)

	createRelation(t, tae, "db", schema, true)

	// Step 1
	txn1, rel1 := getDefaultRelation(t, tae, schema.Name)

	// Step 2
	txn2, rel2 := getDefaultRelation(t, tae, schema.Name)
	err := rel2.Append(bat)
	assert.NoError(t, err)
	assert.NoError(t, txn2.Commit())

	// Step 3
	txn3, rel3 := getDefaultRelation(t, tae, schema.Name)
	err = rel3.DeleteByFilter(filter)
	assert.NoError(t, err)
	assert.NoError(t, txn3.Commit())

	// Step 4
	err = rel1.Append(bat)
	t.Log(err)
	assert.ErrorIs(t, err, txnif.ErrTxnWWConflict)
	_ = txn1.Rollback()
}

// 1. Append 3 blocks and delete last 5 rows of the 1st block
// 2. Merge blocks
// 3. Check rows and col[0]
func TestMergeBlocks(t *testing.T) {
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, -1)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 3
	bat := catalog.MockBatch(schema, 30)
	defer bat.Close()

	createRelationAndAppend(t, tae, "db", schema, bat, true)

	txn, err := tae.StartTxn(nil)
	assert.Nil(t, err)
	db, err := txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err := db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	it := rel.MakeBlockIt()
	blkID := it.GetBlock().Fingerprint()
	err = rel.RangeDelete(blkID, 5, 9, handle.DT_Normal)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, err = tae.StartTxn(nil)
	assert.Nil(t, err)
	for it.Valid() {
		checkAllColRowsByScan(t, rel, bat.Length(), false)
		col, err := it.GetBlock().GetMeta().(*catalog.BlockEntry).GetBlockData().GetColumnDataById(txn, 0, nil)
		assert.NoError(t, err)
		defer col.Close()
		t.Log(col)
		it.Next()
	}
	assert.Nil(t, txn.Commit())

	mergeBlocks(t, tae, "db", schema, false)

	txn, err = tae.StartTxn(nil)
	assert.Nil(t, err)
	db, err = txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err = db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	assert.Equal(t, uint64(25), rel.GetMeta().(*catalog.TableEntry).GetRows())
	it = rel.MakeBlockIt()
	for it.Valid() {
		checkAllColRowsByScan(t, rel, bat.Length()-5, false)
		col, err := it.GetBlock().GetMeta().(*catalog.BlockEntry).GetBlockData().GetColumnDataById(txn, 0, nil)
		assert.Nil(t, err)
		t.Log(col)
		defer col.Close()
		it.Next()
	}
	assert.Nil(t, txn.Commit())
}

func TestDelete2(t *testing.T) {
	testutils.EnsureNoLeak(t)
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := newTestEngine(t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(18, 11)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	tae.bindSchema(schema)
	bat := catalog.MockBatch(schema, 5)
	defer bat.Close()
	tae.createRelAndAppend(bat, true)

	txn, rel := tae.getRelation()
	v := getSingleSortKeyValue(bat, schema, 2)
	filter := handle.NewEQFilter(v)
	err := rel.DeleteByFilter(filter)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

	tae.compactABlocks(false)
}

func TestNull1(t *testing.T) {
	testutils.EnsureNoLeak(t)
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := newTestEngine(t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(18, 9)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	tae.bindSchema(schema)

	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*3+1))
	defer bat.Close()
	bats := bat.Split(4)
	bat.Vecs[3].Update(2, types.Null{})
	tae.createRelAndAppend(bats[0], true)

	txn, rel := tae.getRelation()
	blk := getOneBlock(rel)
	view, err := blk.GetColumnDataById(3, nil)
	assert.NoError(t, err)
	defer view.Close()
	v := view.GetData().Get(2)
	assert.True(t, types.IsNull(v))
	checkAllColRowsByScan(t, rel, bats[0].Length(), false)
	assert.NoError(t, txn.Commit())

	tae.restart()
	txn, rel = tae.getRelation()
	blk = getOneBlock(rel)
	view, err = blk.GetColumnDataById(3, nil)
	assert.NoError(t, err)
	defer view.Close()
	v = view.GetData().Get(2)
	assert.True(t, types.IsNull(v))
	checkAllColRowsByScan(t, rel, bats[0].Length(), false)

	v = getSingleSortKeyValue(bats[0], schema, 2)
	filter_2 := handle.NewEQFilter(v)
	uv0_2, err := rel.GetValueByFilter(filter_2, 3)
	assert.NoError(t, err)
	assert.True(t, types.IsNull(uv0_2))

	v0_4 := getSingleSortKeyValue(bats[0], schema, 4)
	filter_4 := handle.NewEQFilter(v0_4)
	err = rel.UpdateByFilter(filter_4, 3, types.Null{})
	assert.NoError(t, err)
	uv, err := rel.GetValueByFilter(filter_4, 3)
	assert.NoError(t, err)
	assert.True(t, types.IsNull(uv))
	assert.NoError(t, txn.Commit())

	txn, rel = tae.getRelation()
	checkAllColRowsByScan(t, rel, bats[0].Length(), false)
	uv, err = rel.GetValueByFilter(filter_4, 3)
	assert.NoError(t, err)
	assert.True(t, types.IsNull(uv))

	err = rel.Append(bats[1])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

	tae.compactBlocks(false)
	txn, rel = tae.getRelation()
	checkAllColRowsByScan(t, rel, lenOfBats(bats[:2]), false)
	uv, err = rel.GetValueByFilter(filter_4, 3)
	assert.NoError(t, err)
	assert.True(t, types.IsNull(uv))
	assert.NoError(t, txn.Commit())

	tae.restart()
	txn, rel = tae.getRelation()
	checkAllColRowsByScan(t, rel, lenOfBats(bats[:2]), false)
	uv, err = rel.GetValueByFilter(filter_4, 3)
	assert.NoError(t, err)
	assert.True(t, types.IsNull(uv))

	v0_1 := getSingleSortKeyValue(bats[0], schema, 1)
	filter0_1 := handle.NewEQFilter(v0_1)
	err = rel.UpdateByFilter(filter0_1, 12, types.Null{})
	assert.NoError(t, err)
	uv0_1, err := rel.GetValueByFilter(filter0_1, 12)
	assert.NoError(t, err)
	assert.True(t, types.IsNull(uv0_1))
	assert.NoError(t, txn.Commit())

	txn, rel = tae.getRelation()
	uv0_1, err = rel.GetValueByFilter(filter0_1, 12)
	assert.NoError(t, err)
	assert.True(t, types.IsNull(uv0_1))
	err = rel.Append(bats[2])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

	tae.compactBlocks(false)
	tae.mergeBlocks(false)

	txn, rel = tae.getRelation()
	uv0_1, err = rel.GetValueByFilter(filter0_1, 12)
	assert.NoError(t, err)
	assert.True(t, types.IsNull(uv0_1))
	uv0_2, err = rel.GetValueByFilter(filter_2, 3)
	assert.NoError(t, err)
	assert.True(t, types.IsNull(uv0_2))
	assert.NoError(t, txn.Commit())

	tae.restart()

	txn, rel = tae.getRelation()
	uv0_1, err = rel.GetValueByFilter(filter0_1, 12)
	assert.NoError(t, err)
	assert.True(t, types.IsNull(uv0_1))
	uv0_2, err = rel.GetValueByFilter(filter_2, 3)
	assert.NoError(t, err)
	assert.True(t, types.IsNull(uv0_2))
	assert.NoError(t, txn.Commit())
}

func TestNull2(t *testing.T) {
	testutils.EnsureNoLeak(t)
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := newTestEngine(t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(18, 6)
	schema.BlockMaxRows = 10
	tae.bindSchema(schema)
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows-1))
	defer bat.Close()
	tae.createRelAndAppend(bat, true)

	txn, rel := tae.getRelation()
	v1 := getSingleSortKeyValue(bat, schema, 1)
	filter1 := handle.NewEQFilter(v1)
	err := rel.UpdateByFilter(filter1, 2, int32(99))
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())
	tae.compactABlocks(false)
	tae.restart()

	txn, rel = tae.getRelation()
	uv1, err := rel.GetValueByFilter(filter1, 2)
	assert.NoError(t, err)
	assert.Equal(t, int32(99), uv1.(int32))
	err = rel.UpdateByFilter(filter1, 2, types.Null{})
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())
	tae.compactABlocks(false)
	tae.restart()

	txn, rel = tae.getRelation()
	uv1, err = rel.GetValueByFilter(filter1, 2)
	assert.NoError(t, err)
	assert.True(t, types.IsNull(uv1))
	err = rel.UpdateByFilter(filter1, 2, int32(2))
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())
	tae.compactABlocks(false)
	tae.restart()

	txn, rel = tae.getRelation()
	uv1, err = rel.GetValueByFilter(filter1, 2)
	assert.NoError(t, err)
	assert.Equal(t, int32(2), uv1.(int32))
	err = rel.UpdateByFilter(filter1, 2, int32(2))
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

	tae.compactABlocks(false)
	tae.restart()

	txn, rel = tae.getRelation()
	uv1, err = rel.GetValueByFilter(filter1, 2)
	assert.NoError(t, err)
	assert.Equal(t, int32(2), uv1.(int32))
	err = rel.UpdateByFilter(filter1, 2, int32(2))
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())
}

func TestTruncate(t *testing.T) {
	testutils.EnsureNoLeak(t)
	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := newTestEngine(t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(18, 15)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	tae.bindSchema(schema)
	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*5+1))
	defer bat.Close()
	bats := bat.Split(20)
	tae.createRelAndAppend(bats[0], true)

	var wg sync.WaitGroup
	p, _ := ants.NewPool(10)
	tryAppend := func(i int) func() {
		return func() {
			defer wg.Done()
			tae.tryAppend(bats[1+i])
		}
	}

	for i := range bats[1:] {
		if i == 10 {
			wg.Add(1)
			_ = p.Submit(func() {
				defer wg.Done()
				tae.truncate()
				t.Log(tae.Catalog.SimplePPString(common.PPL1))
			})
		}
		wg.Add(1)
		_ = p.Submit(tryAppend(i))
		time.Sleep(time.Millisecond * 2)
	}
	wg.Wait()
	txn, rel := tae.getRelation()
	t.Logf("Rows: %d", rel.Rows())
	assert.NoError(t, txn.Commit())
	tae.truncate()
	txn, rel = tae.getRelation()
	assert.Zero(t, 0, rel.Rows())
	assert.NoError(t, txn.Commit())
}

func TestGetColumnData(t *testing.T) {
	testutils.EnsureNoLeak(t)
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := newTestEngine(t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(18, 13)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	tae.bindSchema(schema)
	bat := catalog.MockBatch(schema, 39)
	bats := bat.Split(4)
	defer bat.Close()
	tae.createRelAndAppend(bats[0], true)
	txn, rel := tae.getRelation()
	blk := getOneBlock(rel)
	view, _ := blk.GetColumnDataById(2, nil)
	defer view.Close()
	assert.Equal(t, bats[0].Length(), view.Length())
	assert.NotZero(t, view.GetData().Allocated())

	buffer := new(bytes.Buffer)
	view, _ = blk.GetColumnDataById(2, buffer)
	defer view.Close()
	assert.Equal(t, bats[0].Length(), view.Length())
	assert.Zero(t, view.GetData().Allocated())
	assert.NoError(t, txn.Commit())

	tae.compactBlocks(false)
	txn, rel = tae.getRelation()
	blk = getOneBlock(rel)
	view, _ = blk.GetColumnDataById(2, nil)
	defer view.Close()
	assert.Equal(t, bats[0].Length(), view.Length())
	assert.NotZero(t, view.GetData().Allocated())

	buffer.Reset()
	view, _ = blk.GetColumnDataById(2, buffer)
	defer view.Close()
	assert.Equal(t, bats[0].Length(), view.Length())
	assert.Zero(t, view.GetData().Allocated())
	assert.NoError(t, txn.Commit())

	txn, rel = tae.getRelation()
	err := rel.Append(bats[1])
	assert.NoError(t, err)
	blk = getOneBlock(rel)
	view, err = blk.GetColumnDataById(2, nil)
	assert.NoError(t, err)
	defer view.Close()
	assert.True(t, view.GetData().Equals(bats[1].Vecs[2]))
	assert.NotZero(t, view.GetData().Allocated())
	buffer.Reset()
	view, err = blk.GetColumnDataById(2, buffer)
	assert.NoError(t, err)
	defer view.Close()
	assert.True(t, view.GetData().Equals(bats[1].Vecs[2]))
	assert.Zero(t, view.GetData().Allocated())

	assert.NoError(t, txn.Commit())
}

func TestCompactBlk(t *testing.T) {
	testutils.EnsureNoLeak(t)
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := newTestEngine(t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(3, 1)
	schema.BlockMaxRows = 5
	schema.SegmentMaxBlocks = 2
	tae.bindSchema(schema)
	bat := catalog.MockBatch(schema, 5)
	bats := bat.Split(5)
	defer bat.Close()

	tae.createRelAndAppend(bats[2], true)

	txn, rel := tae.getRelation()
	_ = rel.Append(bats[1])
	assert.Nil(t, txn.Commit())

	txn, rel = tae.getRelation()
	_ = rel.Append(bats[3])
	assert.Nil(t, txn.Commit())

	txn, rel = tae.getRelation()
	_ = rel.Append(bats[4])
	assert.Nil(t, txn.Commit())

	txn, rel = tae.getRelation()
	_ = rel.Append(bats[0])
	assert.Nil(t, txn.Commit())

	{
		v := getSingleSortKeyValue(bat, schema, 1)
		t.Logf("v is %v**********", v)
		filter := handle.NewEQFilter(v)
		txn2, rel := tae.getRelation()
		t.Log("********before delete******************")
		checkAllColRowsByScan(t, rel, 5, true)
		_ = rel.DeleteByFilter(filter)
		assert.Nil(t, txn2.Commit())
	}

	_, rel = tae.getRelation()
	checkAllColRowsByScan(t, rel, 4, true)

	{
		t.Log("************compact************")
		txn, rel = tae.getRelation()
		it := rel.MakeBlockIt()
		blk := it.GetBlock()
		meta := blk.GetMeta().(*catalog.BlockEntry)
		task, err := jobs.NewCompactBlockTask(nil, txn, meta, tae.DB.Scheduler)
		assert.NoError(t, err)
		err = task.OnExec()
		assert.NoError(t, err)

		{
			v := getSingleSortKeyValue(bat, schema, 2)
			t.Logf("v is %v**********", v)
			filter := handle.NewEQFilter(v)
			txn2, rel := tae.getRelation()
			t.Log("********before delete******************")
			checkAllColRowsByScan(t, rel, 4, true)
			_ = rel.DeleteByFilter(filter)
			assert.Nil(t, txn2.Commit())
		}
		{
			v := getSingleSortKeyValue(bat, schema, 4)
			t.Logf("v is %v**********", v)
			filter := handle.NewEQFilter(v)
			txn2, rel := tae.getRelation()
			t.Log("********before delete******************")
			checkAllColRowsByScan(t, rel, 3, true)
			_ = rel.DeleteByFilter(filter)
			assert.Nil(t, txn2.Commit())
		}
		{
			v := getSingleSortKeyValue(bat, schema, 3)
			t.Logf("v is %v**********", v)
			filter := handle.NewEQFilter(v)
			txn2, rel := tae.getRelation()
			t.Log("********before delete******************")
			checkAllColRowsByScan(t, rel, 2, true)
			_ = rel.UpdateByFilter(filter, 0, int8(111))
			assert.Nil(t, txn2.Commit())
		}

		err = txn.Commit()
		assert.NoError(t, err)
	}

	_, rel = tae.getRelation()
	checkAllColRowsByScan(t, rel, 2, true)
	assert.Equal(t, int64(2), rel.Rows())

	v := getSingleSortKeyValue(bat, schema, 3)
	filter := handle.NewEQFilter(v)
	val, err := rel.GetValueByFilter(filter, 0)
	assert.Nil(t, err)
	assert.Equal(t, int8(111), val)

	v = getSingleSortKeyValue(bat, schema, 2)
	filter = handle.NewEQFilter(v)
	_, _, err = rel.GetByFilter(filter)
	assert.NotNil(t, err)

	v = getSingleSortKeyValue(bat, schema, 4)
	filter = handle.NewEQFilter(v)
	_, _, err = rel.GetByFilter(filter)
	assert.NotNil(t, err)

	tae.restart()
	assert.Equal(t, int64(2), rel.Rows())
}

func TestDelete3(t *testing.T) {
	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := newTestEngine(t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(1, -1)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	tae.bindSchema(schema)
	// rows := int(schema.BlockMaxRows * 1)
	rows := int(schema.BlockMaxRows*3) + 1
	bat := catalog.MockBatch(schema, rows)

	tae.createRelAndAppend(bat, true)
	tae.checkRowsByScan(rows, false)
	deleted := false
	for i := 0; i < 10; i++ {
		if deleted {
			tae.checkRowsByScan(0, true)
			tae.doAppend(bat)
			deleted = false
			tae.checkRowsByScan(rows, true)
		} else {
			tae.checkRowsByScan(rows, true)
			err := tae.deleteAll(true)
			if err == nil {
				deleted = true
				tae.checkRowsByScan(0, true)
				// assert.Zero(t, tae.getRows())
			} else {
				tae.checkRowsByScan(rows, true)
				// assert.Equal(t, tae.getRows(), rows)
			}
		}
	}
	t.Logf(tae.Catalog.SimplePPString(common.PPL1))
}

func TestDropCreated1(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := newTestEngine(t, opts)
	defer tae.Close()

	txn, err := tae.StartTxn(nil)
	assert.Nil(t, err)
	_, err = txn.CreateDatabase("db")
	assert.Nil(t, err)
	db, err := txn.DropDatabase("db")
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	assert.Equal(t, txn.GetCommitTS(), db.GetMeta().(*catalog.DBEntry).CreateAt)
	assert.Equal(t, txn.GetCommitTS(), db.GetMeta().(*catalog.DBEntry).DeleteAt)

	tae.restart()
}

func TestDropCreated2(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := newTestEngine(t, opts)
	schema := catalog.MockSchemaAll(1, -1)
	defer tae.Close()

	txn, err := tae.StartTxn(nil)
	assert.Nil(t, err)
	db, err := txn.CreateDatabase("db")
	assert.Nil(t, err)
	rel, err := db.CreateRelation(schema)
	assert.Nil(t, err)
	_, err = db.DropRelationByName(schema.Name)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	assert.Equal(t, txn.GetCommitTS(), rel.GetMeta().(*catalog.TableEntry).CreateAt)
	assert.Equal(t, txn.GetCommitTS(), rel.GetMeta().(*catalog.TableEntry).DeleteAt)

	tae.restart()
}
