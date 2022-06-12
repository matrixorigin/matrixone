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

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"

	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
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
	db := initDB(t, nil)
	defer db.Close()
	schema := catalog.MockSchemaAll(14, 3)
	schema.BlockMaxRows = options.DefaultBlockMaxRows
	schema.SegmentMaxBlocks = options.DefaultBlocksPerSegment
	data := catalog.MockData(schema, schema.BlockMaxRows*2)
	bats := compute.SplitBatch(data, 4)
	now := time.Now()
	createRelationAndAppend(t, db, "db", schema, bats[0], true)
	t.Log(time.Since(now))
	t.Log(vector.Length(bats[0].Vecs[0]))

	{
		txn, _ := db.StartTxn(nil)
		database, err := txn.GetDatabase("db")
		assert.Nil(t, err)
		rel, err := database.GetRelationByName(schema.Name)
		assert.Nil(t, err)
		appendClosure(t, bats[1], schema.Name, db, nil)()
		err = rel.Append(bats[2])
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
}

func TestAppend2(t *testing.T) {
	opts := config.WithQuickScanAndCKPOpts(nil)
	db := initDB(t, opts)
	defer db.Close()
	schema := catalog.MockSchemaAll(13, 3)
	schema.BlockMaxRows = 400
	schema.SegmentMaxBlocks = 10
	createRelation(t, db, "db", schema, true)

	totalRows := uint64(schema.BlockMaxRows * 30)
	bat := catalog.MockData(schema, uint32(totalRows))
	bats := compute.SplitBatch(bat, 100)

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
	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := initDB(t, opts)
	defer tae.Close()
	schema := catalog.MockSchema(2, 0)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	createRelation(t, tae, "db", schema, true)
	bat := catalog.MockData(schema, schema.BlockMaxRows)
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
		bat := catalog.MockData(schema, schema.BlockMaxRows*3-1)
		bats := compute.SplitBatch(bat, 1)
		createRelation(t, tae, defaultTestDB, schema, false)
		for i := range bats {
			txn, rel := getDefaultRelation(t, tae, schema.Name)
			err := rel.Append(bats[i])
			assert.NoError(t, err)
			err = txn.Commit()
			assert.NoError(t, err)
		}
		txn, rel := getDefaultRelation(t, tae, schema.Name)
		checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bat), false)

		v := compute.GetValue(bat.Vecs[schema.GetSingleSortKeyIdx()], 3)
		filter := handle.NewEQFilter(v)
		err := rel.DeleteByFilter(filter)
		assert.NoError(t, err)
		err = txn.Commit()
		assert.NoError(t, err)

		txn, rel = getDefaultRelation(t, tae, schema.Name)
		checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bat)-1, true)
		err = txn.Commit()
		assert.NoError(t, err)
		compactBlocks(t, tae, defaultTestDB, schema, false)
		txn, rel = getDefaultRelation(t, tae, schema.Name)
		checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bat)-1, false)
		err = txn.Commit()
		assert.NoError(t, err)
	}
}

func testCRUD(t *testing.T, tae *DB, schema *catalog.Schema) {
	bat := catalog.MockData(schema, schema.BlockMaxRows*(uint32(schema.SegmentMaxBlocks)+1)-1)
	bats := compute.SplitBatch(bat, 4)

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
	checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bats[0]), false)
	v := compute.GetValue(bats[0].Vecs[schema.GetSingleSortKeyIdx()], 2)
	filter := handle.NewEQFilter(v)
	err = rel.DeleteByFilter(filter)
	assert.NoError(t, err)

	oldv := compute.GetValue(bats[0].Vecs[updateColIdx], 5)
	v = compute.GetValue(bats[0].Vecs[schema.GetSingleSortKeyIdx()], 5)
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

	checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bats[0])-1, true)
	assert.NoError(t, txn.Commit())

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bats[0])-1, true)
	for _, b := range bats[1:] {
		err = rel.Append(b)
		assert.NoError(t, err)
	}
	checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bat)-1, true)
	assert.NoError(t, txn.Commit())

	compactBlocks(t, tae, defaultTestDB, schema, false)

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bat)-1, false)
	v = compute.GetValue(bats[0].Vecs[schema.GetSingleSortKeyIdx()], 3)
	filter = handle.NewEQFilter(v)
	err = rel.DeleteByFilter(filter)
	checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bat)-2, true)
	assert.NoError(t, txn.Commit())

	compactSegs(t, tae, schema)

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bat)-2, false)
	assert.NoError(t, txn.Commit())

	// t.Log(rel.GetMeta().(*catalog.TableEntry).PPString(common.PPL1, 0, ""))
	dropRelation(t, tae, defaultTestDB, schema.Name)
}

func TestCRUD(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := initDB(t, opts)
	defer tae.Close()
	createDB(t, tae, defaultTestDB)
	withTestAllPKType(t, tae, testCRUD)
}

func TestTableHandle(t *testing.T) {
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
	db := initDB(t, nil)
	defer db.Close()
	schema := catalog.MockSchemaAll(13, 1)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2

	bat := catalog.MockData(schema, 8)

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
		expectVal := compute.GetValue(bat.Vecs[2], 4)
		assert.Equal(t, expectVal, v)
		assert.Equal(t, vector.Length(bat.Vecs[0]), blk.Rows())

		view, err := dataBlk.GetColumnDataById(txn, 2, nil, nil)
		assert.Nil(t, err)
		assert.Nil(t, view.DeleteMask)
		t.Log(view.AppliedVec.String())
		assert.Equal(t, vector.Length(bat.Vecs[2]), vector.Length(view.AppliedVec))

		_, err = dataBlk.RangeDelete(txn, 1, 2)
		assert.Nil(t, err)

		view, err = dataBlk.GetColumnDataById(txn, 2, nil, nil)
		assert.Nil(t, err)
		assert.True(t, view.DeleteMask.Contains(1))
		assert.True(t, view.DeleteMask.Contains(2))
		assert.Equal(t, vector.Length(bat.Vecs[2]), vector.Length(view.AppliedVec))

		_, err = dataBlk.Update(txn, 3, 2, int32(999))
		assert.Nil(t, err)
		t.Log(view.AppliedVec.String())

		view, err = dataBlk.GetColumnDataById(txn, 2, nil, nil)
		assert.Nil(t, err)
		assert.True(t, view.DeleteMask.Contains(1))
		assert.True(t, view.DeleteMask.Contains(2))
		assert.Equal(t, vector.Length(bat.Vecs[2]), vector.Length(view.AppliedVec))
		v = compute.GetValue(view.AppliedVec, 3)
		assert.Equal(t, int32(999), v)
		t.Log(view.AppliedVec.String())

		// assert.Nil(t, txn.Commit())
	}
}

func TestCreateSegment(t *testing.T) {
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

	bat := catalog.MockData(schema, 5)

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
	opts := config.WithLongScanAndCKPOpts(nil)
	db := initDB(t, opts)
	defer db.Close()
	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 4
	bat := catalog.MockData(schema, schema.BlockMaxRows)
	createRelationAndAppend(t, db, "db", schema, bat, true)
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))

	v := compute.GetValue(bat.Vecs[schema.GetSingleSortKeyIdx()], 2)
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
			for row := 0; row < vector.Length(bat.Vecs[0]); row++ {
				exp := compute.GetValue(bat.Vecs[col], uint32(row))
				act := compute.GetValue(preparer.Columns.Vecs[col], uint32(row))
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
		v = compute.GetValue(bat.Vecs[schema.GetSingleSortKeyIdx()], 3)
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
		assert.Equal(t, vector.Length(bat.Vecs[0])-1, vector.Length(preparer.Columns.Vecs[0]))
		{
			txn, _ := db.StartTxn(nil)
			database, err := txn.GetDatabase("db")
			assert.Nil(t, err)
			rel, err := database.GetRelationByName(schema.Name)
			assert.Nil(t, err)
			v = compute.GetValue(bat.Vecs[schema.GetSingleSortKeyIdx()], 4)
			filter.Val = v
			id, offset, err := rel.GetByFilter(filter)
			assert.Nil(t, err)
			err = rel.RangeDelete(id, offset, offset)
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
		assert.Equal(t, vector.Length(bat.Vecs[0])-1, vector.Length(preparer.Columns.Vecs[0]))
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
			assert.Equal(t, vector.Length(bat.Vecs[0])-2, vector.Length(preparer.Columns.Vecs[0]))
			maxTs = txn.GetStartTS()
		}

		dataBlock := block.GetMeta().(*catalog.BlockEntry).GetBlockData()
		changes, err := dataBlock.CollectChangesInRange(txn.GetStartTS(), maxTs+1)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), changes.DeleteMask.GetCardinality())

		destBlock, err := seg.CreateNonAppendableBlock()
		assert.Nil(t, err)
		m := destBlock.GetMeta().(*catalog.BlockEntry)
		txnEntry := txnentries.NewCompactBlockEntry(txn, block, destBlock, db.Scheduler)
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
	db := initDB(t, nil)
	defer db.Close()

	worker := ops.NewOpWorker("xx")
	worker.Start()
	defer worker.Stop()
	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 20
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockData(schema, schema.BlockMaxRows)
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
		err = blk.RangeDelete(1, 2)
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
		t.Log(rel.SimplePPString(common.PPL1))
		seg, err := rel.GetSegment(newBlockFp.SegmentID)
		assert.Nil(t, err)
		blk, err := seg.GetBlock(newBlockFp.BlockID)
		assert.Nil(t, err)
		view, err := blk.GetColumnDataById(3, nil, nil)
		assert.Nil(t, err)
		assert.Nil(t, view.DeleteMask)
		v := compute.GetValue(view.AppliedVec, 1)
		assert.Equal(t, int64(999), v)
		assert.Equal(t, vector.Length(bat.Vecs[0])-2, vector.Length(view.AppliedVec))

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
			err = blk.RangeDelete(4, 5)
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
		view, err := blk.GetColumnDataById(3, nil, nil)
		assert.Nil(t, err)
		assert.True(t, view.DeleteMask.Contains(4))
		assert.True(t, view.DeleteMask.Contains(5))
		v := compute.GetValue(view.AppliedVec, 3)
		assert.Equal(t, int64(1999), v)
		assert.Equal(t, vector.Length(bat.Vecs[0])-2, vector.Length(view.AppliedVec))

		txn2, rel2 := getDefaultRelation(t, db, schema.Name)
		seg2, err := rel2.GetSegment(newBlockFp.SegmentID)
		assert.NoError(t, err)
		blk2, err := seg2.GetBlock(newBlockFp.BlockID)
		assert.NoError(t, err)
		err = blk2.RangeDelete(7, 7)
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
	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := initDB(t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 3)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 10

	totalRows := schema.BlockMaxRows / 5
	bat := catalog.MockData(schema, totalRows)
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
	bat := catalog.MockData(schema1, schema1.BlockMaxRows*3-1)
	bats := compute.SplitBatch(bat, vector.Length(bat.Vecs[0]))

	pool, err := ants.NewPool(20)
	assert.Nil(t, err)
	var wg sync.WaitGroup
	doSearch := func(name string) func() {
		return func() {
			defer wg.Done()
			txn, _ := db.StartTxn(nil)
			database, err := txn.GetDatabase("db")
			assert.Nil(t, err)
			rel, err := database.GetRelationByName(name)
			assert.Nil(t, err)
			it := rel.MakeBlockIt()
			for it.Valid() {
				blk := it.GetBlock()
				_, err := blk.GetColumnDataById(schema1.GetSingleSortKeyIdx(), nil, nil)
				assert.Nil(t, err)
				it.Next()
			}
			err = txn.Commit()
			assert.Nil(t, err)
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
	testutils.WaitExpect(1000, func() bool {
		return db.Scheduler.GetPenddingLSNCnt() == 0
	})
	assert.Equal(t, uint64(0), db.Scheduler.GetPenddingLSNCnt())
	t.Log(db.MTBufMgr.String())
	t.Log(db.Catalog.SimplePPString(common.PPL1))
	t.Logf("GetPenddingLSNCnt: %d", db.Scheduler.GetPenddingLSNCnt())
	t.Logf("GetCheckpointed: %d", db.Scheduler.GetCheckpointedLSN())
}

func TestCompactABlk(t *testing.T) {
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 3)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 10

	totalRows := schema.BlockMaxRows / 5
	bat := catalog.MockData(schema, totalRows)
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
	db := initDB(t, nil)
	defer db.Close()
	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 40
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockData(schema, schema.BlockMaxRows*10)
	bats := compute.SplitBatch(bat, 40)

	txn, _, rel := createRelationNoCommit(t, db, "db", schema, true)
	err := rel.Append(bats[0])
	assert.NoError(t, err)

	row := uint32(5)
	expectVal := compute.GetValue(bats[0].Vecs[schema.GetSingleSortKeyIdx()], row)
	filter := handle.NewEQFilter(expectVal)
	actualVal, err := rel.GetValueByFilter(filter, schema.GetSingleSortKeyIdx())
	assert.NoError(t, err)
	assert.Equal(t, expectVal, actualVal)
	assert.NoError(t, txn.Commit())

	txn, rel = getDefaultRelation(t, db, schema.Name)
	actualVal, err = rel.GetValueByFilter(filter, schema.GetSingleSortKeyIdx())
	assert.NoError(t, err)
	assert.Equal(t, expectVal, actualVal)

	txn2, rel2 := getDefaultRelation(t, db, schema.Name)
	err = rel2.Append(bats[1])
	assert.NoError(t, err)

	val2 := compute.GetValue(bats[1].Vecs[schema.GetSingleSortKeyIdx()], row)
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
			var comp bytes.Buffer
			var decomp bytes.Buffer
			view, err := block.GetColumnDataById(schema.GetSingleSortKeyIdx(), &comp, &decomp)
			assert.Nil(t, err)
			assert.Nil(t, view.DeleteMask)
			assert.NotNil(t, view.GetColumnData())
			t.Log(view.GetColumnData().String())
			t.Log(vector.Length(bats[0].Vecs[0]))
			assert.Equal(t, vector.Length(bats[0].Vecs[0]), vector.Length(view.AppliedVec))
		}
		it.Next()
	}
}

// 1. Txn1 create db, relation and append 10 rows. committed -- PASS
// 2. Txn2 append 10 rows. Get the 5th append row value -- PASS
// 3. Txn2 delete the 5th row value in uncommitted state -- PASS
// 4. Txn2 get the 5th row value -- NotFound
func TestMVCC2(t *testing.T) {
	db := initDB(t, nil)
	defer db.Close()
	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 100
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockData(schema, schema.BlockMaxRows)
	bats := compute.SplitBatch(bat, 10)
	{
		txn, _, rel := createRelationNoCommit(t, db, "db", schema, true)
		err := rel.Append(bats[0])
		assert.NoError(t, err)
		val := compute.GetValue(bats[0].Vecs[schema.GetSingleSortKeyIdx()], 5)
		filter := handle.NewEQFilter(val)
		_, _, err = rel.GetByFilter(filter)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit())
	}
	{
		txn, rel := getDefaultRelation(t, db, schema.Name)
		err := rel.Append(bats[1])
		assert.NoError(t, err)
		val := compute.GetValue(bats[1].Vecs[schema.GetSingleSortKeyIdx()], 5)
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
		var comp bytes.Buffer
		var decomp bytes.Buffer
		for it.Valid() {
			block := it.GetBlock()
			view, err := block.GetColumnDataByName(schema.GetSingleSortKey().Name, &comp, &decomp)
			assert.Nil(t, err)
			assert.Nil(t, view.DeleteMask)
			t.Log(view.AppliedVec.String())
			// TODO: exclude deleted rows when apply appends
			assert.Equal(t, vector.Length(bats[1].Vecs[0])*2-1, int(vector.Length(view.AppliedVec)))
			it.Next()
		}
		assert.NoError(t, txn.Commit())
	}
}

func TestUnload1(t *testing.T) {
	opts := new(options.Options)
	opts.CacheCfg = new(options.CacheCfg)
	opts.CacheCfg.InsertCapacity = common.K
	opts.CacheCfg.TxnCapacity = common.M
	db := initDB(t, opts)
	defer db.Close()

	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2

	bat := catalog.MockData(schema, schema.BlockMaxRows*2)
	bats := compute.SplitBatch(bat, int(schema.BlockMaxRows))
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
				view, err := blk.GetColumnDataByName(schema.GetSingleSortKey().Name, nil, nil)
				assert.Nil(t, err)
				assert.Equal(t, int(schema.BlockMaxRows), vector.Length(view.AppliedVec))
				it.Next()
			}
		}
		_ = txn.Commit()
	}
	t.Log(common.GPool.String())
}

func TestUnload2(t *testing.T) {
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

	bat := catalog.MockData(schema1, schema1.BlockMaxRows*5+5)
	bats := compute.SplitBatch(bat, vector.Length(bat.Vecs[0]))

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
			v := compute.GetValue(data.Vecs[schema1.GetSingleSortKeyIdx()], 0)
			filter := handle.NewEQFilter(v)
			_, _, err := rel.GetByFilter(filter)
			assert.NoError(t, err)
		}
		database, _ := txn.GetDatabase("db")
		rel, err = database.GetRelationByName(schema2.Name)
		assert.Nil(t, err)
		for i := 1; i < len(bats); i += 2 {
			data := bats[i]
			v := compute.GetValue(data.Vecs[schema1.GetSingleSortKeyIdx()], 0)
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
	tae := initDB(t, nil)
	defer tae.Close()

	schema := catalog.MockSchemaAll(3, 2)
	schema.BlockMaxRows = 10
	bat := catalog.MockData(schema, schema.BlockMaxRows)
	createRelationAndAppend(t, tae, "db", schema, bat, true)
	var id *common.ID
	var row uint32
	{
		txn, rel := getDefaultRelation(t, tae, schema.Name)
		assert.Equal(t, compute.LengthOfBatch(bat), int(rel.Rows()))
		pkCol := bat.Vecs[schema.GetSingleSortKeyIdx()]
		pkVal := compute.GetValue(pkCol, 5)
		filter := handle.NewEQFilter(pkVal)
		var err error
		id, row, err = rel.GetByFilter(filter)
		assert.NoError(t, err)
		err = rel.RangeDelete(id, row, row)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit())
	}
	{
		txn, rel := getDefaultRelation(t, tae, schema.Name)
		assert.Equal(t, compute.LengthOfBatch(bat)-1, int(rel.Rows()))
		pkCol := bat.Vecs[schema.GetSingleSortKeyIdx()]
		pkVal := compute.GetValue(pkCol, 5)
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
		view, err := blk.GetColumnDataById(schema.GetSingleSortKeyIdx(), nil, nil)
		assert.NoError(t, err)
		assert.Nil(t, view.DeleteMask)
		assert.Equal(t, vector.Length(bat.Vecs[0])-1, vector.Length(view.AppliedVec))

		err = blk.RangeDelete(0, 0)
		assert.NoError(t, err)
		view, err = blk.GetColumnDataById(schema.GetSingleSortKeyIdx(), nil, nil)
		assert.NoError(t, err)
		assert.True(t, view.DeleteMask.Contains(0))
		v := compute.GetValue(bat.Vecs[schema.GetSingleSortKeyIdx()], 0)
		filter := handle.NewEQFilter(v)
		_, _, err = rel.GetByFilter(filter)
		assert.Equal(t, data.ErrNotFound, err)
		assert.NoError(t, txn.Commit())
	}
	{
		_, rel := getDefaultRelation(t, tae, schema.Name)
		assert.Equal(t, compute.LengthOfBatch(bat)-2, int(rel.Rows()))
		blk := getOneBlock(rel)
		view, err := blk.GetColumnDataById(schema.GetSingleSortKeyIdx(), nil, nil)
		assert.NoError(t, err)
		assert.True(t, view.DeleteMask.Contains(0))
		assert.Equal(t, vector.Length(bat.Vecs[0])-1, vector.Length(view.AppliedVec))
		v := compute.GetValue(bat.Vecs[schema.GetSingleSortKeyIdx()], 0)
		filter := handle.NewEQFilter(v)
		_, _, err = rel.GetByFilter(filter)
		assert.ErrorIs(t, err, data.ErrNotFound)
	}
	t.Log(tae.Opts.Catalog.SimplePPString(common.PPL1))
}

func TestLogIndex1(t *testing.T) {
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 0)
	schema.BlockMaxRows = 10
	bat := catalog.MockData(schema, schema.BlockMaxRows)
	bats := compute.SplitBatch(bat, int(schema.BlockMaxRows))
	createRelation(t, tae, "db", schema, true)
	txns := make([]txnif.AsyncTxn, 0)
	doAppend := func(data *gbat.Batch) func() {
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
		v := compute.GetValue(bat.Vecs[schema.GetSingleSortKeyIdx()], 3)
		filter := handle.NewEQFilter(v)
		id, offset, err = rel.GetByFilter(filter)
		assert.Nil(t, err)
		err = rel.RangeDelete(id, offset, offset)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	{
		_, rel := getDefaultRelation(t, tae, schema.Name)
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
	}
	{
		txn, rel := getDefaultRelation(t, tae, schema.Name)
		blk := getOneBlock(rel)
		meta := blk.GetMeta().(*catalog.BlockEntry)
		indexes, err := meta.GetBlockData().CollectAppendLogIndexes(0, txn.GetStartTS())
		assert.NoError(t, err)
		for i, index := range indexes {
			t.Logf("%d: %s", i, index.String())
		}

		view, err := blk.GetColumnDataById(schema.GetSingleSortKeyIdx(), nil, nil)
		assert.Nil(t, err)
		assert.True(t, view.DeleteMask.Contains(offset))
		t.Log(view.AppliedVec.String())
		task, err := jobs.NewCompactBlockTask(nil, txn, meta, tae.Scheduler)
		assert.Nil(t, err)
		err = task.OnExec()
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
}

func TestCrossDBTxn(t *testing.T) {
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
	bat1 := catalog.MockData(schema1, rows1)
	bat2 := catalog.MockData(schema2, rows2)

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
		view, err := blk.GetColumnDataByName(catalog.SystemDBAttr_Name, nil, nil)
		assert.Nil(t, err)
		assert.Equal(t, 3, vector.Length(view.GetColumnData()))
		view, err = blk.GetColumnDataByName(catalog.SystemDBAttr_CatalogName, nil, nil)
		assert.Nil(t, err)
		assert.Equal(t, 3, vector.Length(view.GetColumnData()))
		view, err = blk.GetColumnDataByName(catalog.SystemDBAttr_CreateSQL, nil, nil)
		assert.Nil(t, err)
		assert.Equal(t, 3, vector.Length(view.GetColumnData()))
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
		view, err := blk.GetColumnDataByName(catalog.SystemRelAttr_Name, nil, nil)
		assert.Nil(t, err)
		assert.Equal(t, 4, vector.Length(view.GetColumnData()))
		view, err = blk.GetColumnDataByName(catalog.SystemRelAttr_Persistence, nil, nil)
		assert.NoError(t, err)
		t.Log(view.GetColumnData().String())
		view, err = blk.GetColumnDataByName(catalog.SystemRelAttr_Kind, nil, nil)
		assert.NoError(t, err)
		t.Log(view.GetColumnData().String())
		it.Next()
	}
	assert.Equal(t, 4, rows)

	bat := gbat.New(true, []string{catalog.SystemColAttr_DBName, catalog.SystemColAttr_RelName, catalog.SystemColAttr_Name, catalog.SystemColAttr_ConstraintType})
	table, err = db.GetRelationByName(catalog.SystemTable_Columns_Name)
	assert.Nil(t, err)
	it = table.MakeBlockIt()
	rows = 0
	for it.Valid() {
		blk := it.GetBlock()
		rows += blk.Rows()
		view, err := blk.GetColumnDataByName(catalog.SystemColAttr_DBName, nil, nil)
		assert.NoError(t, err)
		bat.Vecs[0] = view.GetColumnData()

		view, err = blk.GetColumnDataByName(catalog.SystemColAttr_RelName, nil, nil)
		assert.Nil(t, err)
		bat.Vecs[1] = view.GetColumnData()

		view, err = blk.GetColumnDataByName(catalog.SystemColAttr_Name, nil, nil)
		assert.Nil(t, err)
		t.Log(view.GetColumnData().String())
		bat.Vecs[2] = view.GetColumnData()

		view, err = blk.GetColumnDataByName(catalog.SystemColAttr_ConstraintType, nil, nil)
		assert.Nil(t, err)
		t.Log(view.GetColumnData().String())
		bat.Vecs[3] = view.GetColumnData()

		view, err = blk.GetColumnDataByName(catalog.SystemColAttr_Type, nil, nil)
		assert.Nil(t, err)
		t.Log(view.GetColumnData().String())
		view, err = blk.GetColumnDataByName(catalog.SystemColAttr_Num, nil, nil)
		assert.Nil(t, err)
		t.Log(view.GetColumnData().String())
		it.Next()
	}
	t.Log(rows)

	for i := 0; i < vector.Length(bat.Vecs[0]); i++ {
		dbName := compute.GetValue(bat.Vecs[0], uint32(i))
		relName := compute.GetValue(bat.Vecs[1], uint32(i))
		attrName := compute.GetValue(bat.Vecs[2], uint32(i))
		ct := compute.GetValue(bat.Vecs[3], uint32(i))
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
	bat := catalog.MockData(schema, 1000)

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
	tae := initDB(t, nil)
	defer tae.Close()
	txn, _ := tae.StartTxn(nil)
	schema := catalog.MockSchemaAll(13, 12)
	schema.BlockMaxRows = 100
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockData(schema, 20)
	db, err := txn.GetDatabase(catalog.SystemDBName)
	assert.NoError(t, err)
	rel, err := db.CreateRelation(schema)
	assert.NoError(t, err)
	err = rel.Append(bat)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())
}

func TestScan1(t *testing.T) {
	tae := initDB(t, nil)
	defer tae.Close()

	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 100
	schema.SegmentMaxBlocks = 2

	bat := catalog.MockData(schema, schema.BlockMaxRows-1)
	txn, _, rel := createRelationNoCommit(t, tae, defaultTestDB, schema, true)
	err := rel.Append(bat)
	assert.NoError(t, err)
	checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bat), false)
	assert.NoError(t, txn.Commit())
}

func TestDedup(t *testing.T) {
	tae := initDB(t, nil)
	defer tae.Close()

	schema := catalog.MockSchemaAll(13, 2)
	schema.BlockMaxRows = 100
	schema.SegmentMaxBlocks = 2

	bat := catalog.MockData(schema, 10)
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
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	schema.BlockMaxRows = 20
	schema.SegmentMaxBlocks = 10
	rows := schema.BlockMaxRows * 5 / 2
	bat := catalog.MockData(schema, rows)
	bats := compute.SplitBatch(bat, 2)

	txn, _, rel := createRelationNoCommit(t, tae, defaultTestDB, schema, true)
	err := rel.Append(bats[0])
	assert.NoError(t, err)
	checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bats[0]), false)

	err = rel.Append(bats[0])
	assert.Error(t, err)
	err = rel.Append(bats[1])
	assert.NoError(t, err)
	checkAllColRowsByScan(t, rel, int(rows), false)

	pkv := compute.GetValue(bat.Vecs[schema.GetSingleSortKeyIdx()], 5)
	filter := handle.NewEQFilter(pkv)
	err = rel.DeleteByFilter(filter)
	assert.NoError(t, err)
	checkAllColRowsByScan(t, rel, int(rows)-1, true)

	pkv = compute.GetValue(bat.Vecs[schema.GetSingleSortKeyIdx()], 8)
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
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	bat := catalog.MockData(schema, 100)
	createRelationAndAppend(t, tae, "db", schema, bat, true)

	txn, rel := getDefaultRelation(t, tae, schema.Name)
	v := compute.GetValue(bat.Vecs[schema.GetSingleSortKeyIdx()], 2)
	filter := handle.NewEQFilter(v)
	id, row, err := rel.GetByFilter(filter)
	assert.NoError(t, err)
	err = rel.Update(id, row, uint16(schema.GetSingleSortKeyIdx()), v)
	assert.Error(t, err)
	assert.NoError(t, txn.Commit())
}

func TestADA(t *testing.T) {
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 3)
	schema.BlockMaxRows = 1000
	bat := catalog.MockData(schema, 1)

	// Append to a block
	createRelationAndAppend(t, tae, "db", schema, bat, true)

	// Delete a row from the block
	txn, rel := getDefaultRelation(t, tae, schema.Name)
	v := compute.GetValue(bat.Vecs[schema.GetSingleSortKeyIdx()], 0)
	filter := handle.NewEQFilter(v)
	id, row, err := rel.GetByFilter(filter)
	assert.NoError(t, err)
	err = rel.RangeDelete(id, row, row)
	assert.NoError(t, err)
	id, row, err = rel.GetByFilter(filter)
	assert.Error(t, err)
	assert.NoError(t, txn.Commit())

	// Append a row with the same primary key
	txn, rel = getDefaultRelation(t, tae, schema.Name)
	id, row, err = rel.GetByFilter(filter)
	assert.Error(t, err)
	err = rel.Append(bat)
	assert.NoError(t, err)
	id, row, err = rel.GetByFilter(filter)
	assert.NoError(t, err)
	checkAllColRowsByScan(t, rel, 1, true)

	err = rel.RangeDelete(id, row, row)
	assert.NoError(t, err)
	id, row, err = rel.GetByFilter(filter)
	assert.Error(t, err)

	err = rel.Append(bat)
	assert.NoError(t, err)
	id, row, err = rel.GetByFilter(filter)
	assert.NoError(t, err)
	checkAllColRowsByScan(t, rel, 1, true)
	assert.NoError(t, txn.Commit())

	txn, rel = getDefaultRelation(t, tae, schema.Name)
	err = rel.Append(bat)
	assert.Error(t, err)
	id, row, err = rel.GetByFilter(filter)
	assert.NoError(t, err)
	err = rel.RangeDelete(id, row, row)
	assert.NoError(t, err)
	_, _, err = rel.GetByFilter(filter)
	assert.Error(t, err)

	err = rel.Append(bat)
	assert.NoError(t, err)

	id, row, err = rel.GetByFilter(filter)
	assert.NoError(t, err)

	err = rel.Append(bat)
	assert.Error(t, err)

	err = rel.RangeDelete(id, row, row)
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
	err = rel.RangeDelete(id, row, row)
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
		view, err := blk.GetColumnDataById(schema.GetSingleSortKeyIdx(), nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, 4, view.Length())
		assert.Equal(t, uint64(3), view.DeleteMask.GetCardinality())
		it.Next()
	}
	assert.NoError(t, txn.Commit())
}

func TestUpdateByFilter(t *testing.T) {
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 3)
	bat := catalog.MockData(schema, 100)

	createRelationAndAppend(t, tae, "db", schema, bat, true)

	txn, rel := getDefaultRelation(t, tae, schema.Name)
	v := compute.GetValue(bat.Vecs[schema.GetSingleSortKeyIdx()], 2)
	filter := handle.NewEQFilter(v)
	err := rel.UpdateByFilter(filter, 2, int32(2222))
	assert.NoError(t, err)

	id, row, err := rel.GetByFilter(filter)
	assert.NoError(t, err)
	cv, err := rel.GetValue(id, row, 2)
	assert.NoError(t, err)
	assert.Equal(t, int32(2222), cv.(int32))

	v = compute.GetValue(bat.Vecs[schema.GetSingleSortKeyIdx()], 3)
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
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	bat := catalog.MockData(schema, 10)

	// Step 1
	createRelationAndAppend(t, tae, "db", schema, bat, true)

	// Step 2
	v := compute.GetValue(bat.Vecs[schema.GetSingleSortKeyIdx()], 2)
	filter := handle.NewEQFilter(v)

	// Step 3
	txn1, rel := getDefaultRelation(t, tae, schema.Name)
	id, row, err := rel.GetByFilter(filter)
	assert.NoError(t, err)

	// Step 4
	{
		txn2, rel := getDefaultRelation(t, tae, schema.Name)
		err := rel.RangeDelete(id, row, row)
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
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	schema.BlockMaxRows = 100000
	schema.SegmentMaxBlocks = 2
	bat := catalog.MockData(schema, 1)

	createRelation(t, tae, "db", schema, true)

	v := compute.GetValue(bat.Vecs[schema.GetSingleSortKeyIdx()], 0)
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
			err = rel.RangeDelete(id, row, row)
			if err != nil {
				t.Logf("delete: %v", err)
				// assert.Equal(t, txnif.TxnWWConflictErr, err)
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
	view, err := blk.GetColumnDataById(schema.GetSingleSortKeyIdx(), nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, int(appendCnt), view.Length())
	view.ApplyDeletes()
	t.Log(view.DeleteMask.String())
	t.Log(view.String())
	assert.Equal(t, uint64(deleteCnt), view.DeleteMask.GetCardinality())
}

// Testing Steps
// 1. Append 10 rows
// 2. Start txn1
// 3. Start txn2. Update the 3rd row 3rd col to int64(2222) and commit. -- PASS
// 4. Txn1 try to update the 3rd row 3rd col to int64(1111). -- W-W Conflict.
// 5. Txn1 try to delete the 3rd row. W-W Conflict. Rollback
// 6. Start txn3 and try to update th3 3rd row 3rd col to int64(3333). -- PASS
func TestSnapshotIsolation1(t *testing.T) {
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	schema.BlockMaxRows = 100
	bat := catalog.MockData(schema, 10)
	v := compute.GetValue(bat.Vecs[schema.GetSingleSortKeyIdx()], 3)
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
	assert.ErrorIs(t, err, txnif.TxnWWConflictErr)

	// Step 5
	id, row, err := rel1.GetByFilter(filter)
	assert.NoError(t, err)
	err = rel1.RangeDelete(id, row, row)
	t.Log(err)
	assert.ErrorIs(t, err, txnif.TxnWWConflictErr)
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
	err = rel.RangeDelete(id, row, row)
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
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	schema.BlockMaxRows = 100
	bat := catalog.MockData(schema, 1)
	v := compute.GetValue(bat.Vecs[schema.GetSingleSortKeyIdx()], 0)
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
	assert.ErrorIs(t, err, txnif.TxnWWConflictErr)
	_ = txn1.Rollback()
}

// 1. Append 3 blocks and delete last 5 rows of the 1st block
// 2. Merge blocks
// 3. Check rows and col[0]
func TestMergeBlockes(t *testing.T) {
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, -1)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 3
	bat := catalog.MockData(schema, 30)

	createRelationAndAppend(t, tae, "db", schema, bat, true)

	txn, err := tae.StartTxn(nil)
	assert.Nil(t, err)
	db, err := txn.GetDatabase("db")
	assert.Nil(t, err)
	rel, err := db.GetRelationByName(schema.Name)
	assert.Nil(t, err)
	it := rel.MakeBlockIt()
	blkID := it.GetBlock().Fingerprint()
	err = rel.RangeDelete(blkID, 5, 9)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, err = tae.StartTxn(nil)
	assert.Nil(t, err)
	for it.Valid() {
		col, err := it.GetBlock().GetMeta().(*catalog.BlockEntry).GetBlockData().GetColumnDataById(txn, 0, nil, nil)
		t.Log(col)
		assert.Nil(t, err)
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
		col, err := it.GetBlock().GetMeta().(*catalog.BlockEntry).GetBlockData().GetColumnDataById(txn, 0, nil, nil)
		t.Log(col)
		assert.Nil(t, err)
		it.Next()
	}
	assert.Nil(t, txn.Commit())
}

func TestDelete2(t *testing.T) {
	opts := config.WithLongScanAndCKPOpts(nil)
	tae := newTestEngine(t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(18, 11)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	tae.bindSchema(schema)
	bat := catalog.MockData(schema, 5)
	tae.createRelAndAppend(bat, true)

	txn, rel := tae.getRelation()
	v := getSingleSortKeyValue(bat, schema, 2)
	filter := handle.NewEQFilter(v)
	err := rel.DeleteByFilter(filter)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

	tae.compactABlocks(false)

	// txn, rel = tae.getRelation()
	// checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bat)-1, true)
	// assert.NoError(t, txn.Commit())

	// tae.restart()
	// txn, rel = tae.getRelation()
	// checkAllColRowsByScan(t, rel, compute.LengthOfBatch(bat)-1, true)
	// assert.NoError(t, txn.Commit())
}
