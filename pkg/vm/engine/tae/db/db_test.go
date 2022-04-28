package db

import (
	"bytes"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	idxCommon "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common"

	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/txnentries"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	ops "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/worker"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

const (
	ModuleName = "TAEDB"
)

func initDB(t *testing.T, opts *options.Options) *DB {
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
	opts.CheckpointCfg.CalibrationInterval = 1
	db := initDB(t, opts)
	defer db.Close()
	schema := catalog.MockSchemaAll(13)
	schema.BlockMaxRows = 50
	schema.SegmentMaxBlocks = 2
	schema.PrimaryKey = 3
	{
		txn := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		database.CreateRelation(schema)
		assert.Nil(t, txn.Commit())
	}

	bat := compute.MockBatch(schema.Types(), 8000, int(schema.PrimaryKey), nil)
	bats := compute.SplitBatch(bat, 800)

	var wg sync.WaitGroup
	pool, _ := ants.NewPool(40)

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

	for _, data := range bats {
		wg.Add(1)
		pool.Submit(doAppend(data))
	}
	wg.Wait()
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
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
		assert.Equal(t, gvec.Length(bat.Vecs[0]), blk.Rows())

		vec, mask, err := dataBlk.GetVectorCopy(txn, schema.ColDefs[2].Name, nil, nil)
		assert.Nil(t, err)
		assert.Nil(t, mask)
		t.Log(vec.String())
		assert.Equal(t, gvec.Length(bat.Vecs[2]), gvec.Length(vec))

		// filter := handle.Filter{
		// 	Op:handle.FilterEq,
		// 	Val:
		// }
		_, err = dataBlk.RangeDelete(txn, 1, 2)
		assert.Nil(t, err)

		vec, mask, err = dataBlk.GetVectorCopy(txn, schema.ColDefs[2].Name, nil, nil)
		assert.Nil(t, err)
		assert.True(t, mask.Contains(1))
		assert.True(t, mask.Contains(2))
		assert.Equal(t, gvec.Length(bat.Vecs[2]), gvec.Length(vec))

		_, err = dataBlk.Update(txn, 3, 2, int32(999))
		assert.Nil(t, err)
		t.Log(vec.String())

		vec, mask, err = dataBlk.GetVectorCopy(txn, schema.ColDefs[2].Name, nil, nil)
		assert.Nil(t, err)
		assert.True(t, mask.Contains(1))
		assert.True(t, mask.Contains(2))
		assert.Equal(t, gvec.Length(bat.Vecs[2]), gvec.Length(vec))
		v = compute.GetValue(vec, 3)
		assert.Equal(t, int32(999), v)
		t.Log(vec.String())

		// assert.Nil(t, txn.Commit())
	}
}

func TestCompactBlock1(t *testing.T) {
	opts := new(options.Options)
	opts.CheckpointCfg = new(options.CheckpointCfg)
	opts.CheckpointCfg.CalibrationInterval = 10000
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
		task, err := jobs.NewCompactBlockTask(&ctx, txn, blkMeta)
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
		seg, err := rel.GetSegment(id.SegmentID)
		block, err := seg.GetBlock(id.BlockID)
		assert.Nil(t, err)
		blkMeta := block.GetMeta().(*catalog.BlockEntry)
		task, err := jobs.NewCompactBlockTask(&ctx, txn, blkMeta)
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
		task, err = jobs.NewCompactBlockTask(&ctx, txn, blkMeta)
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
			task, err = jobs.NewCompactBlockTask(&ctx, txn, blkMeta)
			assert.Nil(t, err)
			data, err := task.PrepareData()
			assert.Nil(t, err)
			assert.Equal(t, vector.Length(bat.Vecs[0])-2, vector.Length(data.Vecs[0]))
			t.Log(blk.String())
			maxTs = txn.GetStartTS()
		}

		dataBlock := block.GetMeta().(*catalog.BlockEntry).GetBlockData()
		changes := dataBlock.CollectChangesInRange(txn.GetStartTS(), maxTs+1).(*updates.BlockView)
		assert.Equal(t, uint64(1), changes.DeleteMask.GetCardinality())

		destBlock, err := seg.CreateNonAppendableBlock()
		assert.Nil(t, err)
		txnEntry := txnentries.NewCompactBlockEntry(txn, block, destBlock)
		txn.LogTxnEntry(destBlock.Fingerprint().TableID, txnEntry, []*common.ID{block.Fingerprint()})
		// err = rel.PrepareCompactBlock(block.Fingerprint(), destBlock.Fingerprint())
		destBlockData := destBlock.GetMeta().(*catalog.BlockEntry).GetBlockData()
		assert.Nil(t, err)
		err = txn.Commit()
		assert.Nil(t, err)
		t.Log(destBlockData.PPString(common.PPL1, 0, ""))

		view := destBlockData.CollectChangesInRange(0, math.MaxUint64).(*updates.BlockView)
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
		task, err := jobs.NewCompactBlockTask(ctx, txn, block.GetMeta().(*catalog.BlockEntry))
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
		task, err := jobs.NewCompactBlockTask(ctx, txn, blk.GetMeta().(*catalog.BlockEntry))
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
		// blkData := blk.GetMeta().(*catalog.BlockEntry).GetBlockData()
		// blkData.GetVectorCopyById()
		vec, mask, err := blk.GetVectorCopyById(3, nil, nil)
		assert.Nil(t, err)
		assert.Nil(t, mask)
		v := compute.GetValue(vec, 1)
		assert.Equal(t, int64(999), v)
		assert.Equal(t, gvec.Length(bat.Vecs[0])-2, gvec.Length(vec))

		cnt := 0
		it := rel.MakeBlockIt()
		for it.Valid() {
			cnt++
			it.Next()
		}
		assert.Equal(t, 1, cnt)

		task, err := jobs.NewCompactBlockTask(ctx, txn, blk.GetMeta().(*catalog.BlockEntry))
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
		vec, mask, err := blk.GetVectorCopyById(3, nil, nil)
		assert.Nil(t, err)
		assert.True(t, mask.Contains(4))
		assert.True(t, mask.Contains(5))
		v := compute.GetValue(vec, 3)
		assert.Equal(t, int64(1999), v)
		assert.Equal(t, gvec.Length(bat.Vecs[0])-2, gvec.Length(vec))

		txn2 := db.StartTxn(nil)
		database2, _ := txn2.GetDatabase("db")
		rel2, _ := database2.GetRelationByName(schema.Name)
		seg2, _ := rel2.GetSegment(newBlockFp.SegmentID)
		blk2, _ := seg2.GetBlock(newBlockFp.BlockID)
		err = blk2.RangeDelete(7, 7)
		assert.Nil(t, err)

		task, err := jobs.NewCompactBlockTask(ctx, txn, blk.GetMeta().(*catalog.BlockEntry))
		assert.Nil(t, err)
		worker.SendOp(task)
		err = task.WaitDone()
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())

		err = txn2.Commit()
		assert.NotNil(t, err)
	}
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
			vec, mask, err := block.GetVectorCopy(schema.ColDefs[schema.PrimaryKey].Name, &comp, &decomp)
			assert.Nil(t, err)
			assert.Nil(t, mask)
			assert.NotNil(t, vec)
			t.Log(vec.String())
			t.Log(offset)
			t.Log(val2)
			t.Log(vector.Length(bats[0].Vecs[0]))
			assert.Equal(t, vector.Length(bats[0].Vecs[0]), vector.Length(vec))
		}
		it.Next()
	}
}

// 1. Txn1 create db, relation and append 10 rows. committed -- PASS
// 2. Txn2 append 10 rows. Get the 5th append row value -- PASS
// 3. Txn2 delete the 5th row value in uncommited state -- PASS
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
			vec, mask, err := block.GetVectorCopy(schema.ColDefs[schema.PrimaryKey].Name, &comp, &decomp)
			assert.Nil(t, err)
			assert.Nil(t, mask)
			t.Log(vec.String())
			// TODO: exclude deleted rows when apply appends
			assert.Equal(t, vector.Length(bats[1].Vecs[0])*2-1, int(vector.Length(vec)))
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

	{
		txn := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		rel, _ := database.CreateRelation(schema)
		rel.Append(bat)
		assert.Nil(t, txn.Commit())
	}
	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		for i := 0; i < 10; i++ {
			it := rel.MakeBlockIt()
			for it.Valid() {
				blk := it.GetBlock()
				vec, _, _ := blk.GetVectorCopy(schema.ColDefs[schema.PrimaryKey].Name, nil, nil)
				assert.Equal(t, int(schema.BlockMaxRows), gvec.Length(vec))
				it.Next()
			}
		}
	}
	t.Log(common.GPool.String())
}

func TestUnload2(t *testing.T) {
	opts := new(options.Options)
	opts.CacheCfg = new(options.CacheCfg)
	opts.CacheCfg.InsertCapacity = common.K * 2
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
	bats := compute.SplitBatch(bat, gvec.Length(bat.Vecs[0]))

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
