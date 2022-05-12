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
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/mockio"
	idxCommon "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"

	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
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

const (
	ModuleName = "TAEDB"
)

func initDB(t *testing.T, opts *options.Options) *DB {
	mockio.ResetFS()
	dir := testutils.InitTestEnv(ModuleName, t)
	db, _ := Open(dir, opts)
	idxCommon.MockIndexBufferManager = buffer.NewNodeManager(1024*1024*150, nil)
	return db
}

func TestAppend(t *testing.T) {
	db := initDB(t, nil)
	defer db.Close()
	txn := db.StartTxn(nil)
	schema := catalog.MockSchemaAll(14)
	schema.BlockMaxRows = options.DefaultBlockMaxRows
	schema.SegmentMaxBlocks = options.DefaultBlocksPerSegment
	schema.PrimaryKey = 3
	data := compute.MockBatch(schema.Types(), uint64(schema.BlockMaxRows)*2, int(schema.PrimaryKey), nil)
	now := time.Now()
	bats := compute.SplitBatch(data, 4)
	database, err := txn.CreateDatabase("db")
	assert.Nil(t, err)
	rel, err := database.CreateRelation(schema)
	assert.Nil(t, err)
	err = rel.Append(bats[0])
	assert.Nil(t, err)
	t.Log(vector.Length(bats[0].Vecs[0]))
	assert.Nil(t, txn.Commit())
	t.Log(time.Since(now))
	t.Log(vector.Length(bats[0].Vecs[0]))

	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		{
			txn := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(schema.Name)
			err = rel.Append(bats[1])
			assert.Nil(t, err)
			assert.Nil(t, txn.Commit())
		}
		err = rel.Append(bats[2])
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
}

func TestAppend2(t *testing.T) {
	opts := new(options.Options)
	opts.CheckpointCfg = new(options.CheckpointCfg)
	opts.CheckpointCfg.ScannerInterval = 10
	opts.CheckpointCfg.ExecutionLevels = 2
	opts.CheckpointCfg.ExecutionInterval = 10
	db := initDB(t, opts)
	defer db.Close()
	schema := catalog.MockSchemaAll(13)
	schema.BlockMaxRows = 400
	schema.SegmentMaxBlocks = 10
	schema.PrimaryKey = 3
	{
		txn := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		database.CreateRelation(schema)
		assert.Nil(t, txn.Commit())
	}

	totalRows := uint64(schema.BlockMaxRows * 30)
	bat := compute.MockBatch(schema.Types(), totalRows, int(schema.PrimaryKey), nil)
	bats := compute.SplitBatch(bat, 100)

	var wg sync.WaitGroup
	pool, _ := ants.NewPool(80)

	doAppend := func(data *gbat.Batch) func() {
		return func() {
			defer wg.Done()
			txn := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(schema.Name)
			err := rel.Append(data)
			assert.Nil(t, err)
			assert.Nil(t, txn.Commit())
		}
	}

	start := time.Now()
	for _, data := range bats {
		wg.Add(1)
		pool.Submit(doAppend(data))
	}
	wg.Wait()
	t.Logf("Append %d rows takes: %s", totalRows, time.Since(start))
	rows := 0
	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		for it.Valid() {
			blk := it.GetBlock()
			rows += blk.Rows()
			it.Next()
		}
		assert.Nil(t, txn.Commit())
	}
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
	t.Logf("Rows: %d", rows)
	assert.Equal(t, int(totalRows), rows)

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
	opts := new(options.Options)
	opts.CheckpointCfg = new(options.CheckpointCfg)
	opts.CheckpointCfg.ScannerInterval = 10
	opts.CheckpointCfg.ExecutionLevels = 2
	opts.CheckpointCfg.ExecutionInterval = 10
	opts.CheckpointCfg.CatalogCkpInterval = 10
	opts.CheckpointCfg.CatalogUnCkpLimit = 1
	tae := initDB(t, opts)
	defer tae.Close()
	schema := catalog.MockSchema(2)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	{
		txn := tae.StartTxn(nil)
		db, _ := txn.CreateDatabase("db")
		db.CreateRelation(schema)
		assert.Nil(t, txn.Commit())
	}
	bat := compute.MockBatch(schema.Types(), uint64(schema.BlockMaxRows), int(schema.PrimaryKey), nil)
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

func TestTableHandle(t *testing.T) {
	db := initDB(t, nil)
	defer db.Close()

	txn := db.StartTxn(nil)
	database, _ := txn.CreateDatabase("db")
	schema := catalog.MockSchema(2)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 2
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

	txn := db.StartTxn(nil)
	database, _ := txn.CreateDatabase("db")
	schema := catalog.MockSchemaAll(13)
	rel, _ := database.CreateRelation(schema)
	seg, _ := rel.CreateSegment()
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
	schema := catalog.MockSchemaAll(13)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	schema.PrimaryKey = 1

	bat := compute.MockBatch(schema.Types(), 8, int(schema.PrimaryKey), nil)

	{
		txn := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		database.CreateRelation(schema)
		assert.Nil(t, txn.Commit())
	}
	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		seg, _ := rel.CreateSegment()
		blk, err := seg.CreateNonAppendableBlock()
		assert.Nil(t, err)
		dataBlk := blk.GetMeta().(*catalog.BlockEntry).GetBlockData()
		blockFile := dataBlk.GetBlockFile()
		blockFile.WriteBatch(bat, txn.GetStartTS())

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
	schema := catalog.MockSchemaAll(1)
	txn := tae.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	rel, _ := db.CreateRelation(schema)
	_, err := rel.CreateNonAppendableSegment()
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	bat := compute.MockBatch(schema.Types(), 5, int(schema.PrimaryKey), nil)
	txn = tae.StartTxn(nil)
	db, _ = txn.GetDatabase("db")
	rel, _ = db.GetRelationByName(schema.Name)
	err = rel.Append(bat)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	segCnt := 0
	processor := new(catalog.LoopProcessor)
	processor.SegmentFn = func(segment *catalog.SegmentEntry) error {
		segCnt++
		return nil
	}
	tae.Opts.Catalog.RecurLoop(processor)
	assert.Equal(t, 2, segCnt)
	t.Log(tae.Opts.Catalog.SimplePPString(common.PPL1))
}

func TestCompactBlock1(t *testing.T) {
	opts := new(options.Options)
	opts.CheckpointCfg = new(options.CheckpointCfg)
	opts.CheckpointCfg.ScannerInterval = 10000
	opts.CheckpointCfg.ExecutionLevels = 20
	opts.CheckpointCfg.ExecutionInterval = 20000
	db := initDB(t, opts)
	defer db.Close()
	schema := catalog.MockSchemaAll(13)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 4
	schema.PrimaryKey = 2
	bat := compute.MockBatch(schema.Types(), uint64(schema.BlockMaxRows), int(schema.PrimaryKey), nil)
	{
		txn := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		rel, _ := database.CreateRelation(schema)
		err := rel.Append(bat)
		assert.Nil(t, err)
		txn.Commit()
		t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
	}

	v := compute.GetValue(bat.Vecs[schema.PrimaryKey], 2)
	filter := handle.Filter{
		Op:  handle.FilterEq,
		Val: v,
	}
	ctx := tasks.Context{Waitable: true}
	// 1. No updates and deletes
	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		var block handle.Block
		for it.Valid() {
			block = it.GetBlock()
			break
		}
		blkMeta := block.GetMeta().(*catalog.BlockEntry)
		t.Log(blkMeta.String())
		task, err := jobs.NewCompactBlockTask(&ctx, txn, blkMeta, db.Scheduler)
		assert.Nil(t, err)
		data, err := task.PrepareData()
		assert.Nil(t, err)
		assert.NotNil(t, data)
		for col := 0; col < len(data.Vecs); col++ {
			for row := 0; row < vector.Length(bat.Vecs[0]); row++ {
				exp := compute.GetValue(bat.Vecs[col], uint32(row))
				act := compute.GetValue(data.Vecs[col], uint32(row))
				assert.Equal(t, exp, act)
			}
		}
		id, offset, err := rel.GetByFilter(&filter)
		assert.Nil(t, err)

		err = rel.RangeDelete(id, offset, offset)
		assert.Nil(t, err)

		assert.Nil(t, txn.Commit())
	}
	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		v = compute.GetValue(bat.Vecs[schema.PrimaryKey], 3)
		filter.Val = v
		id, _, err := rel.GetByFilter(&filter)
		assert.Nil(t, err)
		seg, _ := rel.GetSegment(id.SegmentID)
		block, err := seg.GetBlock(id.BlockID)
		assert.Nil(t, err)
		blkMeta := block.GetMeta().(*catalog.BlockEntry)
		task, err := jobs.NewCompactBlockTask(&ctx, txn, blkMeta, nil)
		assert.Nil(t, err)
		data, err := task.PrepareData()
		assert.Nil(t, err)
		assert.Equal(t, vector.Length(bat.Vecs[0])-1, vector.Length(data.Vecs[0]))
		{
			txn := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(schema.Name)
			v = compute.GetValue(bat.Vecs[schema.PrimaryKey], 4)
			filter.Val = v
			id, offset, err := rel.GetByFilter(&filter)
			assert.Nil(t, err)
			err = rel.RangeDelete(id, offset, offset)
			assert.Nil(t, err)
			err = rel.Update(id, offset+1, uint16(schema.PrimaryKey), int32(99))
			assert.Nil(t, err)
			assert.Nil(t, txn.Commit())
		}
		task, err = jobs.NewCompactBlockTask(&ctx, txn, blkMeta, nil)
		assert.Nil(t, err)
		data, err = task.PrepareData()
		assert.Nil(t, err)
		assert.Equal(t, vector.Length(bat.Vecs[0])-1, vector.Length(data.Vecs[0]))
		var maxTs uint64
		{
			txn := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(schema.Name)
			seg, _ := rel.GetSegment(id.SegmentID)
			blk, _ := seg.GetBlock(id.BlockID)
			blkMeta := blk.GetMeta().(*catalog.BlockEntry)
			task, err = jobs.NewCompactBlockTask(&ctx, txn, blkMeta, nil)
			assert.Nil(t, err)
			data, err := task.PrepareData()
			assert.Nil(t, err)
			assert.Equal(t, vector.Length(bat.Vecs[0])-2, vector.Length(data.Vecs[0]))
			t.Log(blk.String())
			maxTs = txn.GetStartTS()
		}

		dataBlock := block.GetMeta().(*catalog.BlockEntry).GetBlockData()
		changes := dataBlock.CollectChangesInRange(txn.GetStartTS(), maxTs+1)
		assert.Equal(t, uint64(1), changes.DeleteMask.GetCardinality())

		destBlock, err := seg.CreateNonAppendableBlock()
		assert.Nil(t, err)
		txnEntry := txnentries.NewCompactBlockEntry(txn, block, destBlock, db.Scheduler)
		txn.LogTxnEntry(destBlock.Fingerprint().TableID, txnEntry, []*common.ID{block.Fingerprint()})
		// err = rel.PrepareCompactBlock(block.Fingerprint(), destBlock.Fingerprint())
		destBlockData := destBlock.GetMeta().(*catalog.BlockEntry).GetBlockData()
		assert.Nil(t, err)
		err = txn.Commit()
		assert.Nil(t, err)
		t.Log(destBlockData.PPString(common.PPL1, 0, ""))

		view := destBlockData.CollectChangesInRange(0, math.MaxUint64)
		assert.True(t, view.DeleteMask.Equals(changes.DeleteMask))
	}
}

func TestCompactBlock2(t *testing.T) {
	db := initDB(t, nil)
	defer db.Close()

	worker := ops.NewOpWorker("xx")
	worker.Start()
	defer worker.Stop()
	schema := catalog.MockSchemaAll(13)
	schema.BlockMaxRows = 20
	schema.SegmentMaxBlocks = 2
	schema.PrimaryKey = 2
	bat := compute.MockBatch(schema.Types(), uint64(schema.BlockMaxRows), int(schema.PrimaryKey), nil)
	{
		txn := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		rel, _ := database.CreateRelation(schema)
		err := rel.Append(bat)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	ctx := &tasks.Context{Waitable: true}
	var newBlockFp *common.ID
	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		var block handle.Block
		for it.Valid() {
			block = it.GetBlock()
			break
		}
		task, err := jobs.NewCompactBlockTask(ctx, txn, block.GetMeta().(*catalog.BlockEntry), db.Scheduler)
		assert.Nil(t, err)
		worker.SendOp(task)
		err = task.WaitDone()
		assert.Nil(t, err)
		newBlockFp = task.GetNewBlock().Fingerprint()
		assert.Nil(t, txn.Commit())
	}
	{
		t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		t.Log(rel.SimplePPString(common.PPL1))
		seg, _ := rel.GetSegment(newBlockFp.SegmentID)
		blk, _ := seg.GetBlock(newBlockFp.BlockID)
		err := blk.RangeDelete(1, 2)
		assert.Nil(t, err)
		err = blk.Update(3, 3, int64(999))
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	{
		t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		t.Log(rel.SimplePPString(common.PPL1))
		seg, _ := rel.GetSegment(newBlockFp.SegmentID)
		blk, _ := seg.GetBlock(newBlockFp.BlockID)
		task, err := jobs.NewCompactBlockTask(ctx, txn, blk.GetMeta().(*catalog.BlockEntry), db.Scheduler)
		assert.Nil(t, err)
		worker.SendOp(task)
		err = task.WaitDone()
		assert.Nil(t, err)
		newBlockFp = task.GetNewBlock().Fingerprint()
		assert.Nil(t, txn.Commit())
	}
	{
		t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		t.Log(rel.SimplePPString(common.PPL1))
		seg, _ := rel.GetSegment(newBlockFp.SegmentID)
		blk, _ := seg.GetBlock(newBlockFp.BlockID)
		view, err := blk.GetColumnDataById(3, nil, nil)
		assert.Nil(t, err)
		assert.Nil(t, view.DeleteMask)
		v := compute.GetValue(view.AppliedVec, 1)
		assert.Equal(t, int64(999), v)
		assert.Equal(t, vector.Length(bat.Vecs[0])-2, vector.Length(view.AppliedVec))

		cnt := 0
		it := rel.MakeBlockIt()
		for it.Valid() {
			cnt++
			it.Next()
		}
		assert.Equal(t, 1, cnt)

		task, err := jobs.NewCompactBlockTask(ctx, txn, blk.GetMeta().(*catalog.BlockEntry), db.Scheduler)
		assert.Nil(t, err)
		worker.SendOp(task)
		err = task.WaitDone()
		assert.Nil(t, err)
		newBlockFp = task.GetNewBlock().Fingerprint()
		{
			txn := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(schema.Name)
			seg, _ := rel.GetSegment(newBlockFp.SegmentID)
			blk, _ := seg.GetBlock(newBlockFp.BlockID)
			err := blk.RangeDelete(4, 5)
			assert.Nil(t, err)
			err = blk.Update(3, 3, int64(1999))
			assert.Nil(t, err)
			assert.Nil(t, txn.Commit())
		}
		assert.Nil(t, txn.Commit())
	}
	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		t.Log(rel.SimplePPString(common.PPL1))
		t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
		seg, _ := rel.GetSegment(newBlockFp.SegmentID)
		blk, _ := seg.GetBlock(newBlockFp.BlockID)
		view, err := blk.GetColumnDataById(3, nil, nil)
		assert.Nil(t, err)
		assert.True(t, view.DeleteMask.Contains(4))
		assert.True(t, view.DeleteMask.Contains(5))
		v := compute.GetValue(view.AppliedVec, 3)
		assert.Equal(t, int64(1999), v)
		assert.Equal(t, vector.Length(bat.Vecs[0])-2, vector.Length(view.AppliedVec))

		txn2 := db.StartTxn(nil)
		database2, _ := txn2.GetDatabase("db")
		rel2, _ := database2.GetRelationByName(schema.Name)
		seg2, _ := rel2.GetSegment(newBlockFp.SegmentID)
		blk2, _ := seg2.GetBlock(newBlockFp.BlockID)
		err = blk2.RangeDelete(7, 7)
		assert.Nil(t, err)

		task, err := jobs.NewCompactBlockTask(ctx, txn, blk.GetMeta().(*catalog.BlockEntry), db.Scheduler)
		assert.Nil(t, err)
		worker.SendOp(task)
		err = task.WaitDone()
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())

		err = txn2.Commit()
		assert.NotNil(t, err)
	}
}

func TestAutoCompactABlk1(t *testing.T) {
	opts := new(options.Options)
	opts.CheckpointCfg = new(options.CheckpointCfg)
	opts.CheckpointCfg.ScannerInterval = 10
	opts.CheckpointCfg.ExecutionLevels = 5
	opts.CheckpointCfg.ExecutionInterval = 1
	opts.CheckpointCfg.CatalogCkpInterval = 10
	opts.CheckpointCfg.CatalogUnCkpLimit = 1
	tae := initDB(t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 10
	schema.PrimaryKey = 3

	totalRows := uint64(schema.BlockMaxRows) / 5
	bat := compute.MockBatch(schema.Types(), totalRows, int(schema.PrimaryKey), nil)
	{
		txn := tae.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		rel, _ := database.CreateRelation(schema)
		rel.Append(bat)
		assert.Nil(t, txn.Commit())
	}
	testutils.WaitExpect(1000, func() bool {
		return tae.Scheduler.GetPenddingLSNCnt() == 0
	})
	err := tae.Catalog.Checkpoint(tae.Scheduler.GetSafeTS())
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), tae.Scheduler.GetPenddingLSNCnt())
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	{
		txn := tae.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		blk := it.GetBlock()
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
	opts.CheckpointCfg = new(options.CheckpointCfg)
	opts.CheckpointCfg.ScannerInterval = 2
	opts.CheckpointCfg.ExecutionLevels = 2
	opts.CheckpointCfg.ExecutionInterval = 2
	opts.CheckpointCfg.CatalogCkpInterval = 1
	opts.CheckpointCfg.CatalogUnCkpLimit = 1
	db := initDB(t, opts)
	defer db.Close()

	schema1 := catalog.MockSchemaAll(13)
	schema1.BlockMaxRows = 20
	schema1.SegmentMaxBlocks = 2
	schema1.PrimaryKey = 2

	schema2 := catalog.MockSchemaAll(13)
	schema2.BlockMaxRows = 20
	schema2.SegmentMaxBlocks = 2
	schema2.PrimaryKey = 2
	{
		txn := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		database.CreateRelation(schema1)
		database.CreateRelation(schema2)
		assert.Nil(t, txn.Commit())
	}
	bat := compute.MockBatch(schema1.Types(), uint64(schema1.BlockMaxRows)*3-1, int(schema1.PrimaryKey), nil)
	bats := compute.SplitBatch(bat, vector.Length(bat.Vecs[0]))

	pool, _ := ants.NewPool(20)
	var wg sync.WaitGroup
	doFn := func(name string, data *gbat.Batch) func() {
		return func() {
			defer wg.Done()
			txn := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(name)
			err := rel.Append(data)
			assert.Nil(t, err)
			assert.Nil(t, txn.Commit())
		}
	}
	doSearch := func(name string) func() {
		return func() {
			defer wg.Done()
			txn := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(name)
			it := rel.MakeBlockIt()
			for it.Valid() {
				blk := it.GetBlock()
				blk.GetColumnDataById(int(schema1.PrimaryKey), nil, nil)
				it.Next()
			}
			txn.Commit()
		}
	}

	for _, data := range bats {
		wg.Add(4)
		pool.Submit(doSearch(schema1.Name))
		pool.Submit(doSearch(schema2.Name))
		pool.Submit(doFn(schema1.Name, data))
		pool.Submit(doFn(schema2.Name, data))
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
	schema := catalog.MockSchemaAll(13)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 10
	schema.PrimaryKey = 3

	totalRows := uint64(schema.BlockMaxRows) / 5
	bat := compute.MockBatch(schema.Types(), totalRows, int(schema.PrimaryKey), nil)
	{
		txn := tae.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		rel, _ := database.CreateRelation(schema)
		rel.Append(bat)
		assert.Nil(t, txn.Commit())
	}
	{
		txn := tae.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		blk := it.GetBlock()
		blkData := blk.GetMeta().(*catalog.BlockEntry).GetBlockData()
		factory, taskType, scopes, err := blkData.BuildCompactionTaskFactory()
		assert.Nil(t, err)
		task, err := tae.Scheduler.ScheduleMultiScopedTxnTask(tasks.WaitableCtx, taskType, scopes, factory)
		assert.Nil(t, err)
		err = task.WaitDone()
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
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
	schema := catalog.MockSchema(2)

	txn := db.StartTxn(nil)
	database, _ := txn.CreateDatabase("db")
	database.CreateRelation(schema)
	assert.Nil(t, txn.Commit())

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
	txn = db.StartTxn(nil)
	database, _ = txn.GetDatabase("db")
	rel, _ := database.GetRelationByName(schema.Name)
	rel.CreateSegment()

	tableMeta := rel.GetMeta().(*catalog.TableEntry)
	tableMeta.RecurLoop(processor)
	assert.Equal(t, segCnt, 1)

	assert.Nil(t, txn.Rollback())
	segCnt = 0
	tableMeta.RecurLoop(processor)
	assert.Equal(t, segCnt, 0)

	txn = db.StartTxn(nil)
	database, _ = txn.GetDatabase("db")
	rel, _ = database.GetRelationByName(schema.Name)
	seg, _ := rel.CreateSegment()
	segMeta := seg.GetMeta().(*catalog.SegmentEntry)
	assert.Nil(t, txn.Commit())
	segCnt = 0
	tableMeta.RecurLoop(processor)
	assert.Equal(t, segCnt, 1)

	txn = db.StartTxn(nil)
	database, _ = txn.GetDatabase("db")
	rel, _ = database.GetRelationByName(schema.Name)
	seg, _ = rel.GetSegment(segMeta.GetID())
	_, err := seg.CreateBlock()
	assert.Nil(t, err)
	blkCnt = 0
	tableMeta.RecurLoop(processor)
	assert.Equal(t, blkCnt, 1)

	txn.Rollback()
	blkCnt = 0
	tableMeta.RecurLoop(processor)
	assert.Equal(t, blkCnt, 0)

	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
}

func TestMVCC1(t *testing.T) {
	db := initDB(t, nil)
	defer db.Close()
	schema := catalog.MockSchemaAll(13)
	schema.BlockMaxRows = 40
	schema.SegmentMaxBlocks = 2
	schema.PrimaryKey = 2
	bat := compute.MockBatch(schema.Types(), uint64(schema.BlockMaxRows)*10, int(schema.PrimaryKey), nil)
	bats := compute.SplitBatch(bat, 40)

	txn := db.StartTxn(nil)
	database, _ := txn.CreateDatabase("db")
	rel, _ := database.CreateRelation(schema)
	err := rel.Append(bats[0])
	assert.Nil(t, err)

	row := uint32(5)
	expectVal := compute.GetValue(bats[0].Vecs[schema.PrimaryKey], row)
	filter := &handle.Filter{
		Op:  handle.FilterEq,
		Val: expectVal,
	}
	id, offset, err := rel.GetByFilter(filter)
	assert.Nil(t, err)
	t.Logf("id=%s,offset=%d", id, offset)
	// Read uncommitted value
	actualVal, err := rel.GetValue(id, offset, uint16(schema.PrimaryKey))
	assert.Nil(t, err)
	assert.Equal(t, expectVal, actualVal)
	assert.Nil(t, txn.Commit())

	txn = db.StartTxn(nil)
	database, _ = txn.GetDatabase("db")
	rel, _ = database.GetRelationByName(schema.Name)
	id, offset, err = rel.GetByFilter(filter)
	assert.Nil(t, err)
	t.Logf("id=%s,offset=%d", id, offset)
	// Read committed value
	actualVal, err = rel.GetValue(id, offset, uint16(schema.PrimaryKey))
	assert.Nil(t, err)
	assert.Equal(t, expectVal, actualVal)

	txn2 := db.StartTxn(nil)
	database2, _ := txn2.GetDatabase("db")
	rel2, _ := database2.GetRelationByName(schema.Name)

	err = rel2.Append(bats[1])
	assert.Nil(t, err)

	val2 := compute.GetValue(bats[1].Vecs[schema.PrimaryKey], row)
	filter.Val = val2
	id, offset, err = rel2.GetByFilter(filter)
	assert.Nil(t, err)
	actualVal, err = rel2.GetValue(id, offset, uint16(schema.PrimaryKey))
	assert.Nil(t, err)
	assert.Equal(t, val2, actualVal)

	assert.Nil(t, txn2.Commit())

	_, _, err = rel.GetByFilter(filter)
	assert.NotNil(t, err)

	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		id, offset, err = rel.GetByFilter(filter)
		t.Log(err)
		assert.Nil(t, err)
	}

	it := rel.MakeBlockIt()
	for it.Valid() {
		block := it.GetBlock()
		bid := block.Fingerprint()
		if bid.BlockID == id.BlockID {
			var comp bytes.Buffer
			var decomp bytes.Buffer
			view, err := block.GetColumnDataById(int(schema.PrimaryKey), &comp, &decomp)
			assert.Nil(t, err)
			assert.Nil(t, view.DeleteMask)
			assert.NotNil(t, view.GetColumnData())
			t.Log(view.GetColumnData().String())
			t.Log(offset)
			t.Log(val2)
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
	schema := catalog.MockSchemaAll(13)
	schema.BlockMaxRows = 100
	schema.SegmentMaxBlocks = 2
	schema.PrimaryKey = 2
	bat := compute.MockBatch(schema.Types(), uint64(schema.BlockMaxRows), int(schema.PrimaryKey), nil)
	bats := compute.SplitBatch(bat, 10)
	{
		txn := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		rel, _ := database.CreateRelation(schema)
		rel.Append(bats[0])
		val := compute.GetValue(bats[0].Vecs[schema.PrimaryKey], 5)
		filter := handle.Filter{
			Op:  handle.FilterEq,
			Val: val,
		}
		_, _, err := rel.GetByFilter(&filter)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		rel.Append(bats[1])
		val := compute.GetValue(bats[1].Vecs[schema.PrimaryKey], 5)
		filter := handle.Filter{
			Op:  handle.FilterEq,
			Val: val,
		}
		id, offset, err := rel.GetByFilter(&filter)
		assert.Nil(t, err)
		err = rel.RangeDelete(id, offset, offset)
		assert.Nil(t, err)

		_, _, err = rel.GetByFilter(&filter)
		assert.NotNil(t, err)

		t.Log(err)
		assert.Nil(t, txn.Commit())
	}
	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		var comp bytes.Buffer
		var decomp bytes.Buffer
		for it.Valid() {
			block := it.GetBlock()
			view, err := block.GetColumnDataByName(schema.ColDefs[schema.PrimaryKey].Name, &comp, &decomp)
			assert.Nil(t, err)
			assert.Nil(t, view.DeleteMask)
			t.Log(view.AppliedVec.String())
			// TODO: exclude deleted rows when apply appends
			assert.Equal(t, vector.Length(bats[1].Vecs[0])*2-1, int(vector.Length(view.AppliedVec)))
			it.Next()
		}
	}
}

func TestUnload1(t *testing.T) {
	opts := new(options.Options)
	opts.CacheCfg = new(options.CacheCfg)
	opts.CacheCfg.InsertCapacity = common.K
	opts.CacheCfg.TxnCapacity = common.M
	db := initDB(t, opts)
	defer db.Close()

	schema := catalog.MockSchemaAll(13)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2
	schema.PrimaryKey = 2

	bat := compute.MockBatch(schema.Types(), uint64(schema.BlockMaxRows*2), int(schema.PrimaryKey), nil)
	bats := compute.SplitBatch(bat, int(schema.BlockMaxRows))

	{
		txn := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		database.CreateRelation(schema)
		// rel.Append(bat)
		assert.Nil(t, txn.Commit())
	}
	var wg sync.WaitGroup
	doAppend := func(data *gbat.Batch) func() {
		return func() {
			defer wg.Done()
			txn := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(schema.Name)
			rel.Append(data)
			assert.Nil(t, txn.Commit())
		}
	}
	pool, _ := ants.NewPool(1)
	for _, data := range bats {
		wg.Add(1)
		pool.Submit(doAppend(data))
	}
	wg.Wait()
	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		for i := 0; i < 10; i++ {
			it := rel.MakeBlockIt()
			for it.Valid() {
				blk := it.GetBlock()
				view, _ := blk.GetColumnDataByName(schema.ColDefs[schema.PrimaryKey].Name, nil, nil)
				assert.Equal(t, int(schema.BlockMaxRows), vector.Length(view.AppliedVec))
				it.Next()
			}
		}
	}
	t.Log(common.GPool.String())
}

func TestUnload2(t *testing.T) {
	opts := new(options.Options)
	opts.CacheCfg = new(options.CacheCfg)
	opts.CacheCfg.InsertCapacity = common.K * 3
	opts.CacheCfg.TxnCapacity = common.M
	db := initDB(t, opts)
	defer db.Close()

	schema1 := catalog.MockSchemaAll(13)
	schema1.BlockMaxRows = 10
	schema1.SegmentMaxBlocks = 2
	schema1.PrimaryKey = 2

	schema2 := catalog.MockSchemaAll(13)
	schema2.BlockMaxRows = 10
	schema2.SegmentMaxBlocks = 2
	schema2.PrimaryKey = 2
	{
		txn := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		database.CreateRelation(schema1)
		database.CreateRelation(schema2)
		assert.Nil(t, txn.Commit())
	}

	bat := compute.MockBatch(schema1.Types(), uint64(schema1.BlockMaxRows*5)+5, int(schema1.PrimaryKey), nil)
	bats := compute.SplitBatch(bat, vector.Length(bat.Vecs[0]))

	p, _ := ants.NewPool(10)
	var wg sync.WaitGroup
	doFn := func(name string, data *gbat.Batch) func() {
		return func() {
			defer wg.Done()
			txn := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(name)
			err := rel.Append(data)
			assert.Nil(t, err)
			assert.Nil(t, txn.Commit())
		}
	}

	for i, data := range bats {
		wg.Add(1)
		name := schema1.Name
		if i%2 == 1 {
			name = schema2.Name
		}
		p.Submit(doFn(name, data))
	}
	wg.Wait()

	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema1.Name)
		filter := handle.Filter{
			Op: handle.FilterEq,
		}
		for i := 0; i < len(bats); i += 2 {
			data := bats[i]
			filter.Val = compute.GetValue(data.Vecs[schema1.PrimaryKey], 0)
			_, _, err := rel.GetByFilter(&filter)
			assert.Nil(t, err)
		}
		rel, _ = database.GetRelationByName(schema2.Name)
		for i := 1; i < len(bats); i += 2 {
			data := bats[i]
			filter.Val = compute.GetValue(data.Vecs[schema1.PrimaryKey], 0)
			_, _, err := rel.GetByFilter(&filter)
			assert.Nil(t, err)
		}
	}

	t.Log(db.MTBufMgr.String())
	// t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
}

func TestDelete1(t *testing.T) {
	tae := initDB(t, nil)
	defer tae.Close()

	schema := catalog.MockSchemaAll(3)
	schema.PrimaryKey = 2
	schema.BlockMaxRows = 10
	bat := compute.MockBatch(schema.Types(), uint64(schema.BlockMaxRows), int(schema.PrimaryKey), nil)

	{
		txn := tae.StartTxn(nil)
		db, _ := txn.CreateDatabase("db")
		rel, _ := db.CreateRelation(schema)
		rel.Append(bat)
		assert.Nil(t, txn.Commit())
	}
	var id *common.ID
	var row uint32
	var err error
	{
		txn := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		pkCol := bat.Vecs[schema.PrimaryKey]
		pkVal := compute.GetValue(pkCol, 5)
		filter := handle.NewEQFilter(pkVal)
		id, row, err = rel.GetByFilter(filter)
		assert.Nil(t, err)
		err = rel.RangeDelete(id, row, row)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	{
		txn := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		pkCol := bat.Vecs[schema.PrimaryKey]
		pkVal := compute.GetValue(pkCol, 5)
		filter := handle.NewEQFilter(pkVal)
		_, _, err = rel.GetByFilter(filter)
		assert.Equal(t, txnbase.ErrNotFound, err)
		assert.Nil(t, txn.Commit())
	}
	{
		txn := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		blk := it.GetBlock()
		blkData := blk.GetMeta().(*catalog.BlockEntry).GetBlockData()
		factory, taskType, scopes, err := blkData.BuildCompactionTaskFactory()
		assert.Nil(t, err)
		task, err := tae.Scheduler.ScheduleMultiScopedTxnTask(tasks.WaitableCtx, taskType, scopes, factory)
		assert.Nil(t, err)
		err = task.WaitDone()
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	{
		txn := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		blk := it.GetBlock()
		view, err := blk.GetColumnDataById(int(schema.PrimaryKey), nil, nil)
		assert.Nil(t, err)
		assert.Nil(t, view.DeleteMask)
		assert.Equal(t, vector.Length(bat.Vecs[0])-1, vector.Length(view.AppliedVec))

		err = blk.RangeDelete(0, 0)
		assert.Nil(t, err)
		view, err = blk.GetColumnDataById(int(schema.PrimaryKey), nil, nil)
		assert.Nil(t, err)
		assert.True(t, view.DeleteMask.Contains(0))
		v := compute.GetValue(bat.Vecs[schema.PrimaryKey], 0)
		filter := handle.NewEQFilter(v)
		_, _, err = rel.GetByFilter(filter)
		assert.Equal(t, txnbase.ErrNotFound, err)
		assert.Nil(t, txn.Commit())
	}
	{
		txn := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		blk := it.GetBlock()
		view, err := blk.GetColumnDataById(int(schema.PrimaryKey), nil, nil)
		assert.Nil(t, err)
		assert.True(t, view.DeleteMask.Contains(0))
		assert.Equal(t, vector.Length(bat.Vecs[0])-1, vector.Length(view.AppliedVec))
		v := compute.GetValue(bat.Vecs[schema.PrimaryKey], 0)
		filter := handle.NewEQFilter(v)
		_, _, err = rel.GetByFilter(filter)
		assert.Equal(t, txnbase.ErrNotFound, err)
	}
	t.Log(tae.Opts.Catalog.SimplePPString(common.PPL1))
}

func TestLogIndex1(t *testing.T) {
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13)
	schema.BlockMaxRows = 10
	bat := compute.MockBatch(schema.Types(), uint64(schema.BlockMaxRows), int(schema.PrimaryKey), nil)
	bats := compute.SplitBatch(bat, int(schema.BlockMaxRows))
	{
		txn := tae.StartTxn(nil)
		db, _ := txn.CreateDatabase("db")
		db.CreateRelation(schema)
		// err := rel.Append(bat)
		// assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	txns := make([]txnif.AsyncTxn, 0)
	doAppend := func(data *gbat.Batch) func() {
		return func() {
			txn := tae.StartTxn(nil)
			db, _ := txn.GetDatabase("db")
			rel, _ := db.GetRelationByName(schema.Name)
			rel.Append(data)
			assert.Nil(t, txn.Commit())
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
		txn := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		v := compute.GetValue(bat.Vecs[schema.PrimaryKey], 3)
		filter := handle.NewEQFilter(v)
		id, offset, err = rel.GetByFilter(filter)
		assert.Nil(t, err)
		err = rel.RangeDelete(id, offset, offset)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	{
		txn := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		blk := it.GetBlock()
		meta := blk.GetMeta().(*catalog.BlockEntry)
		indexes := meta.GetBlockData().CollectAppendLogIndexes(txns[0].GetStartTS(), txns[len(txns)-1].GetCommitTS())
		assert.Equal(t, len(txns), len(indexes))
		indexes = meta.GetBlockData().CollectAppendLogIndexes(txns[1].GetStartTS(), txns[len(txns)-1].GetCommitTS())
		assert.Equal(t, len(txns)-1, len(indexes))
		indexes = meta.GetBlockData().CollectAppendLogIndexes(txns[2].GetCommitTS(), txns[len(txns)-1].GetCommitTS())
		assert.Equal(t, len(txns)-2, len(indexes))
		indexes = meta.GetBlockData().CollectAppendLogIndexes(txns[3].GetCommitTS(), txns[len(txns)-1].GetCommitTS())
		assert.Equal(t, len(txns)-3, len(indexes))
	}
	{
		txn := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		rel, _ := db.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		blk := it.GetBlock()
		meta := blk.GetMeta().(*catalog.BlockEntry)
		indexes := meta.GetBlockData().CollectAppendLogIndexes(0, txn.GetStartTS())
		for i, index := range indexes {
			t.Logf("%d: %s", i, index.String())
		}

		view, err := blk.GetColumnDataById(int(schema.PrimaryKey), nil, nil)
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
