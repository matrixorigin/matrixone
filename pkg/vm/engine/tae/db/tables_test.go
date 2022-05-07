package db

import (
	"bytes"
	"sync"
	"testing"
	"time"

	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/mockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func TestTables1(t *testing.T) {
	db := initDB(t, nil)
	defer db.Close()
	txn := db.StartTxn(nil)
	database, _ := txn.CreateDatabase("db")
	schema := catalog.MockSchema(1)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 2
	rel, _ := database.CreateRelation(schema)
	tableMeta := rel.GetMeta().(*catalog.TableEntry)

	dataFactory := tables.NewDataFactory(mockio.SegmentFileMockFactory, db.MTBufMgr, db.Scheduler)
	tableFactory := dataFactory.MakeTableFactory()
	table := tableFactory(tableMeta)
	handle := table.GetHandle()
	_, err := handle.GetAppender()
	assert.Equal(t, data.ErrAppendableSegmentNotFound, err)
	seg, _ := rel.CreateSegment()
	blk, _ := seg.CreateBlock()
	id := blk.GetMeta().(*catalog.BlockEntry).AsCommonID()
	appender := handle.SetAppender(id)
	assert.NotNil(t, appender)

	blkCnt := 3
	rows := schema.BlockMaxRows * uint32(blkCnt)
	toAppend, err := appender.PrepareAppend(rows)
	assert.Equal(t, schema.BlockMaxRows, toAppend)
	assert.Nil(t, err)
	t.Log(toAppend)

	toAppend, err = appender.PrepareAppend(rows - toAppend)
	assert.Equal(t, uint32(0), toAppend)
	appender.Close()

	appender, err = handle.GetAppender()
	assert.Equal(t, data.ErrAppendableBlockNotFound, err)

	blk, _ = seg.CreateBlock()
	id = blk.GetMeta().(*catalog.BlockEntry).AsCommonID()
	appender = handle.SetAppender(id)

	toAppend, err = appender.PrepareAppend(rows - toAppend)
	assert.Equal(t, schema.BlockMaxRows, toAppend)
	appender.Close()

	appender, err = handle.GetAppender()
	assert.Equal(t, data.ErrAppendableSegmentNotFound, err)

	seg, _ = rel.CreateSegment()
	blk, _ = seg.CreateBlock()

	id = blk.GetMeta().(*catalog.BlockEntry).AsCommonID()
	appender = handle.SetAppender(id)
	toAppend, err = appender.PrepareAppend(rows - 2*toAppend)
	assert.Equal(t, schema.BlockMaxRows, toAppend)
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
	txn.Rollback()
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
}

func TestTxn1(t *testing.T) {
	db := initDB(t, nil)
	defer db.Close()

	schema := catalog.MockSchema(3)
	schema.BlockMaxRows = 10000
	schema.SegmentMaxBlocks = 4
	schema.PrimaryKey = 2
	batchRows := uint64(schema.BlockMaxRows) * 2 / 5
	cnt := uint64(20)
	bat := compute.MockBatch(schema.Types(), batchRows*cnt, int(schema.PrimaryKey), nil)
	bats := compute.SplitBatch(bat, 20)
	{
		txn := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		database.CreateRelation(schema)
		err := txn.Commit()
		assert.Nil(t, err)
	}
	var wg sync.WaitGroup
	now := time.Now()
	doAppend := func(b *gbat.Batch) func() {
		return func() {
			defer wg.Done()
			txn := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, err := database.GetRelationByName(schema.Name)
			assert.Nil(t, err)
			err = rel.Append(b)
			assert.Nil(t, err)
			err = txn.Commit()
			assert.Nil(t, err)
		}
	}
	p, _ := ants.NewPool(4)
	for _, toAppend := range bats {
		wg.Add(1)
		p.Submit(doAppend(toAppend))
	}

	wg.Wait()

	t.Logf("Append takes: %s", time.Since(now))
	// expectBlkCnt := (uint32(batchRows)*uint32(batchCnt)*uint32(loopCnt)-1)/schema.BlockMaxRows + 1
	expectBlkCnt := (uint32(batchRows)*uint32(cnt)-1)/schema.BlockMaxRows + 1
	expectSegCnt := (expectBlkCnt-1)/uint32(schema.SegmentMaxBlocks) + 1
	// t.Log(expectBlkCnt)
	// t.Log(expectSegCnt)
	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		seg, err := rel.CreateSegment()
		assert.Nil(t, err)
		_, err = seg.CreateBlock()
		assert.Nil(t, err)
	}
	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		segIt := rel.MakeSegmentIt()
		segCnt := uint32(0)
		blkCnt := uint32(0)
		for segIt.Valid() {
			segCnt++
			blkIt := segIt.GetSegment().MakeBlockIt()
			for blkIt.Valid() {
				blkCnt++
				blkIt.Next()
			}
			segIt.Next()
		}
		assert.Equal(t, expectSegCnt, segCnt)
		assert.Equal(t, expectBlkCnt, blkCnt)
	}
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
}

func TestTxn2(t *testing.T) {
	db := initDB(t, nil)
	defer db.Close()

	var wg sync.WaitGroup
	run := func() {
		defer wg.Done()
		txn := db.StartTxn(nil)
		if _, err := txn.CreateDatabase("db"); err != nil {
			assert.Nil(t, txn.Rollback())
		} else {
			assert.Nil(t, txn.Commit())
		}
		t.Log(txn.String())
	}
	wg.Add(2)
	go run()
	go run()
	wg.Wait()
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
}

func TestTxn3(t *testing.T) {
	db := initDB(t, nil)
	defer db.Close()

	schema := catalog.MockSchemaAll(4)
	schema.BlockMaxRows = 40000
	schema.SegmentMaxBlocks = 8
	rows := uint64(30)
	colIdx := uint16(0)
	{
		txn := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		rel, _ := database.CreateRelation(schema)
		bat := compute.MockBatch(schema.Types(), rows, int(schema.PrimaryKey), nil)
		for i := 0; i < 1; i++ {
			err := rel.Append(bat)
			assert.Nil(t, err)
		}
		err := txn.Commit()
		assert.Nil(t, err)
		t.Log(bat.Vecs[colIdx].String())
	}
	{
		// 1. Update ROW=5, VAL=99 -- PASS
		// 2. Update ROW=5, VAL=100 -- PASS
		// 3. Delete ROWS=[0,2] -- PASS
		// 4. Update ROW=1 -- FAIL
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		assert.True(t, it.Valid())
		blk := it.GetBlock()
		err := blk.Update(5, colIdx, int8(99))
		// err := blk.Update(5, colIdx, uint32(99))
		assert.Nil(t, err)

		// Txn can update a resource many times in a txn
		err = blk.Update(5, colIdx, int8(100))
		// err = blk.Update(5, colIdx, int32(100))
		assert.Nil(t, err)

		err = blk.RangeDelete(0, 2)
		assert.Nil(t, err)

		// err = blk.Update(1, 0, int32(11))
		err = blk.Update(1, colIdx, int8(11))
		assert.NotNil(t, err)

		var comp bytes.Buffer
		var decomp bytes.Buffer
		vec, dels, err := blk.GetColumnDataById(0, &comp, &decomp)
		assert.Nil(t, err)
		assert.Equal(t, int(rows), vector.Length(vec))
		assert.Equal(t, 3, int(dels.GetCardinality()))
		assert.Equal(t, int8(100), compute.GetValue(vec, 5))
		// assert.Equal(t, int32(100), compute.GetValue(vec, 2))
		// Check w-w with uncommitted col update
		{
			txn := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(schema.Name)
			it := rel.MakeBlockIt()
			assert.True(t, it.Valid())
			blk := it.GetBlock()
			err := blk.Update(5, colIdx, int8(99))
			// err := blk.Update(5, colIdx, int32(99))
			assert.NotNil(t, err)
			err = blk.Update(8, colIdx, int8(88))
			// err = blk.Update(8, colIdx, int32(88))
			assert.Nil(t, err)

			err = blk.RangeDelete(2, 2)
			assert.NotNil(t, err)
			err = blk.Update(0, colIdx, int8(50))
			// err = blk.Update(0, colIdx, int32(200))
			assert.NotNil(t, err)

			txn.Rollback()
		}
		err = txn.Commit()
		assert.Nil(t, err)
	}
	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		assert.True(t, it.Valid())
		blk := it.GetBlock()
		err := blk.Update(5, colIdx, int8(99))
		// err := blk.Update(5, colIdx, int32(99))
		assert.Nil(t, err)
		err = blk.Update(8, colIdx, int8(88))
		// err = blk.Update(8, colIdx, int32(88))
		assert.Nil(t, err)

		txn2 := db.StartTxn(nil)
		db2, _ := txn2.GetDatabase("db")
		rel2, _ := db2.GetRelationByName(schema.Name)
		it2 := rel2.MakeBlockIt()
		assert.True(t, it.Valid())
		blk2 := it2.GetBlock()
		err = blk2.Update(20, colIdx, int8(40))
		// err = blk2.Update(20, colIdx, int32(2000))
		assert.Nil(t, err)
		// chain := it2.GetBlock().GetMeta().(*catalog.BlockEntry).GetBlockData().GetUpdateChain().(*updates.BlockUpdateChain)
		// t.Log(chain.StringLocked())
		var comp bytes.Buffer
		var decomp bytes.Buffer
		vec, dels, err := it2.GetBlock().GetColumnDataByName(schema.ColDefs[0].Name, &comp, &decomp)
		assert.Nil(t, err)
		t.Log(vec.String())
		assert.Equal(t, int(rows), vector.Length(vec))
		assert.Equal(t, 3, int(dels.GetCardinality()))
		assert.Equal(t, int8(100), compute.GetValue(vec, 5))
		// assert.Equal(t, int32(100), compute.GetValue(vec, 2))
		assert.Equal(t, int8(40), compute.GetValue(vec, 20))
		// assert.Equal(t, int32(50), compute.GetValue(vec, 17))

		assert.Nil(t, txn.Commit())
		vec, _, err = it2.GetBlock().GetColumnDataByName(schema.ColDefs[colIdx].Name, &comp, &decomp)
		assert.Nil(t, err)
		t.Log(vec.Typ.String())
		// chain = it2.GetBlock().GetMeta().(*catalog.BlockEntry).GetBlockData().GetUpdateChain().(*updates.BlockUpdateChain)
		// t.Log(chain.StringLocked())
	}
}

func TestTxn4(t *testing.T) {
	db := initDB(t, nil)
	defer db.Close()

	schema := catalog.MockSchemaAll(4)
	schema.BlockMaxRows = 40000
	schema.SegmentMaxBlocks = 8
	schema.PrimaryKey = 2
	{
		txn := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		rel, _ := database.CreateRelation(schema)
		pk := gvec.New(schema.ColDefs[schema.PrimaryKey].Type)
		compute.AppendValue(pk, int32(1))
		compute.AppendValue(pk, int32(2))
		compute.AppendValue(pk, int32(1))
		provider := compute.NewMockDataProvider()
		provider.AddColumnProvider(int(schema.PrimaryKey), pk)
		bat := compute.MockBatch(schema.Types(), 3, int(schema.PrimaryKey), provider)
		err := rel.Append(bat)
		t.Log(err)
		assert.NotNil(t, err)
		assert.Nil(t, txn.Commit())
	}
}

func TestTxn5(t *testing.T) {
	db := initDB(t, nil)
	defer db.Close()

	schema := catalog.MockSchemaAll(4)
	schema.BlockMaxRows = 20
	schema.SegmentMaxBlocks = 4
	schema.PrimaryKey = 2
	cnt := uint64(10)
	rows := uint64(schema.BlockMaxRows) / 2 * cnt
	bat := compute.MockBatch(schema.Types(), rows, int(schema.PrimaryKey), nil)
	bats := compute.SplitBatch(bat, int(cnt))
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
		err := rel.Append(bats[0])
		assert.Nil(t, err)
		err = rel.Append(bats[0])
		assert.NotNil(t, err)
		assert.Nil(t, txn.Rollback())
	}

	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		err := rel.Append(bats[0])
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		err := rel.Append(bats[0])
		assert.NotNil(t, err)
		assert.Nil(t, txn.Rollback())
	}
	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		err := rel.Append(bats[1])
		assert.Nil(t, err)

		txn2 := db.StartTxn(nil)
		db2, _ := txn2.GetDatabase("db")
		rel2, _ := db2.GetRelationByName(schema.Name)
		err = rel2.Append(bats[1])
		assert.Nil(t, err)
		err = rel2.Append(bats[2])
		assert.Nil(t, err)

		assert.Nil(t, txn2.Commit())
		assert.NotNil(t, txn.Commit())
		t.Log(txn2.String())
		t.Log(txn.String())
	}
	t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
}

func TestTxn6(t *testing.T) {
	db := initDB(t, nil)
	defer db.Close()

	schema := catalog.MockSchemaAll(4)
	schema.BlockMaxRows = 20
	schema.SegmentMaxBlocks = 4
	schema.PrimaryKey = 2
	cnt := uint64(10)
	rows := uint64(schema.BlockMaxRows) / 2 * cnt
	bat := compute.MockBatch(schema.Types(), rows, int(schema.PrimaryKey), nil)
	bats := compute.SplitBatch(bat, int(cnt))
	{
		txn := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		rel, _ := database.CreateRelation(schema)
		err := rel.Append(bats[0])
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		filter := new(handle.Filter)
		filter.Op = handle.FilterEq
		filter.Val = int32(5)
		id, row, err := rel.GetByFilter(filter)
		assert.Nil(t, err)

		err = rel.Update(id, row, uint16(3), int64(33))
		assert.Nil(t, err)

		err = rel.Update(id, row, uint16(3), int64(44))
		assert.Nil(t, err)
		v, err := rel.GetValue(id, row, uint16(3))
		assert.Nil(t, err)
		assert.Equal(t, int64(44), v)

		err = rel.Update(id, row+1, uint16(3), int64(77))
		assert.Nil(t, err)

		err = rel.RangeDelete(id, row+1, row+1)
		assert.Nil(t, err)

		// Double delete in a same txn -- FAIL
		err = rel.RangeDelete(id, row+1, row+1)
		assert.NotNil(t, err)

		{
			txn := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(schema.Name)

			v, err := rel.GetValue(id, row, uint16(3))
			assert.Nil(t, err)
			assert.NotEqual(t, int64(44), v)

			err = rel.Update(id, row, uint16(3), int64(55))
			assert.NotNil(t, err)

			err = rel.Update(id, row+2, uint16(3), int64(88))
			assert.Nil(t, err)

			// Update row that has uncommitted delete -- FAIL
			err = rel.Update(id, row+1, uint16(3), int64(55))
			assert.NotNil(t, err)
			v, err = rel.GetValue(id, row+1, uint16(3))
			assert.Nil(t, err)
			txn.Rollback()
		}
		err = rel.Update(id, row+2, uint16(3), int64(99))
		assert.Nil(t, err)

		assert.Nil(t, txn.Commit())

		{
			txn := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(schema.Name)

			v, err := rel.GetValue(id, row, uint16(3))
			assert.Nil(t, err)
			assert.Equal(t, int64(44), v)

			v, err = rel.GetValue(id, row+2, uint16(3))
			assert.Nil(t, err)
			assert.Equal(t, int64(99), v)

			_, err = rel.GetValue(id, row+1, uint16(3))
			assert.NotNil(t, err)

			var comp bytes.Buffer
			var decomp bytes.Buffer
			it := rel.MakeBlockIt()
			for it.Valid() {
				comp.Reset()
				decomp.Reset()
				blk := it.GetBlock()
				vec, dels, err := blk.GetColumnDataByName(schema.ColDefs[3].Name, &comp, &decomp)
				assert.Nil(t, err)
				assert.Equal(t, gvec.Length(bats[0].Vecs[0]), gvec.Length(vec))
				assert.True(t, dels.Contains(row+1))
				t.Log(dels.String())
				it.Next()
			}

			t.Log(rel.SimplePPString(common.PPL1))
		}
	}
}

func TestMergeBlocks1(t *testing.T) {
	opts := new(options.Options)
	// opts.CheckpointCfg = new(options.CheckpointCfg)
	// opts.CheckpointCfg.ScannerInterval = 5
	// opts.CheckpointCfg.ExecutionLevels = 2
	// opts.CheckpointCfg.ExecutionInterval = 1
	// opts.CheckpointCfg.CatalogCkpInterval = 5
	// opts.CheckpointCfg.CatalogUnCkpLimit = 1
	db := initDB(t, opts)
	defer db.Close()
	schema := catalog.MockSchemaAll(13)
	schema.BlockMaxRows = 5
	schema.SegmentMaxBlocks = 8
	schema.PrimaryKey = 2
	col3Data := []int64{10, 8, 1, 6, 15, 7, 3, 12, 11, 4, 9, 5, 14, 13, 2}
	// col3Data := []int64{2, 9, 11, 13, 15, 1, 4, 7, 10, 14, 3, 5, 6, 8, 12}
	pkData := []int32{2, 9, 11, 13, 15, 1, 4, 7, 10, 14, 3, 5, 6, 8, 12}
	pk := gvec.New(schema.GetPKType())
	col3 := gvec.New(schema.ColDefs[3].Type)
	mapping := make(map[int32]int64)
	for i, v := range pkData {
		compute.AppendValue(pk, v)
		compute.AppendValue(col3, col3Data[i])
		mapping[v] = col3Data[i]
	}

	provider := compute.NewMockDataProvider()
	provider.AddColumnProvider(int(schema.PrimaryKey), pk)
	provider.AddColumnProvider(3, col3)
	bat := compute.MockBatch(schema.Types(), uint64(schema.BlockMaxRows*3), int(schema.PrimaryKey), provider)
	{
		txn := db.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		rel, _ := database.CreateRelation(schema)
		err := rel.Append(bat)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}

	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		blks := make([]*catalog.BlockEntry, 0)
		it := rel.MakeBlockIt()
		for it.Valid() {
			blk := it.GetBlock()
			meta := blk.GetMeta().(*catalog.BlockEntry)
			blks = append(blks, meta)
			// vec, _, _ := blk.GetColumnDataById(int(schema.PrimaryKey), nil, nil)
			it.Next()
		}
		{
			txn := db.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(schema.Name)
			it := rel.MakeBlockIt()
			blk := it.GetBlock()
			err := blk.Update(2, 3, int64(22))
			assert.Nil(t, err)
			pkv, err := rel.GetValue(blk.Fingerprint(), 2, uint16(schema.PrimaryKey))
			mapping[pkv.(int32)] = int64(22)
			assert.Nil(t, err)
			err = blk.RangeDelete(4, 4)
			assert.Nil(t, err)
			assert.Nil(t, txn.Commit())
		}
		start := time.Now()
		factory := jobs.MergeBlocksTaskFactory(blks, blks[0].GetSegment(), db.Scheduler)
		// task, err := db.Scheduler.ScheduleTxnTask(tasks.WaitableCtx, factory)
		// err = task.WaitDone()
		// assert.Nil(t, err)
		{
			task, err := factory(nil, txn)
			assert.Nil(t, err)
			err = task.OnExec()
			assert.Nil(t, err)
		}
		assert.Nil(t, txn.Commit())
		t.Logf("MergeSort takes: %s", time.Since(start))
		t.Log(db.Opts.Catalog.SimplePPString(common.PPL1))
	}
	{
		txn := db.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(schema.Name)
		it := rel.MakeBlockIt()
		for it.Valid() {
			blk := it.GetBlock()
			vec, mask, _ := blk.GetColumnDataById(3, nil, nil)
			assert.NotNil(t, vec)
			if mask != nil {
				t.Log(mask.String())
			}
			var pkVec *gvec.Vector
			pkVec, _, _ = blk.GetColumnDataById(int(schema.PrimaryKey), nil, nil)
			for i := 0; i < gvec.Length(pkVec); i++ {
				pkv := compute.GetValue(pkVec, uint32(i))
				colv := compute.GetValue(vec, uint32(i))
				assert.Equal(t, mapping[pkv.(int32)], colv)
			}
			it.Next()
		}
	}
	// testutils.WaitExpect(1000, func() bool {
	// 	return db.Wal.GetPenddingCnt() == 0
	// })
	// assert.Equal(t, uint64(0), db.Wal.GetPenddingCnt())
	t.Logf("Checkpointed: %d", db.Wal.GetCheckpointed())
	t.Logf("PendingCnt: %d", db.Wal.GetPenddingCnt())
}

func TestMergeBlocks2(t *testing.T) {
	opts := new(options.Options)
	opts.CheckpointCfg = new(options.CheckpointCfg)
	opts.CheckpointCfg.ScannerInterval = 3
	opts.CheckpointCfg.ExecutionLevels = 2
	opts.CheckpointCfg.ExecutionInterval = 1
	opts.CheckpointCfg.CatalogCkpInterval = 2
	opts.CheckpointCfg.CatalogUnCkpLimit = 1
	tae := initDB(t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13)
	schema.BlockMaxRows = 5
	schema.SegmentMaxBlocks = 2
	schema.PrimaryKey = 2
	col3Data := []int64{10, 8, 1, 6, 15, 7, 3, 12, 11, 4, 9, 5, 14, 13, 2}
	// col3Data := []int64{2, 9, 11, 13, 15, 1, 4, 7, 10, 14, 3, 5, 6, 8, 12}
	pkData := []int32{2, 9, 11, 13, 15, 1, 4, 7, 10, 14, 3, 5, 6, 8, 12}
	pk := gvec.New(schema.GetPKType())
	col3 := gvec.New(schema.ColDefs[3].Type)
	mapping := make(map[int32]int64)
	for i, v := range pkData {
		compute.AppendValue(pk, v)
		compute.AppendValue(col3, col3Data[i])
		mapping[v] = col3Data[i]
	}

	provider := compute.NewMockDataProvider()
	provider.AddColumnProvider(int(schema.PrimaryKey), pk)
	provider.AddColumnProvider(3, col3)
	bat := compute.MockBatch(schema.Types(), uint64(schema.BlockMaxRows*3), int(schema.PrimaryKey), provider)
	{
		txn := tae.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		rel, _ := database.CreateRelation(schema)
		err := rel.Append(bat)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}

	// {
	// 	txn := db.StartTxn(nil)
	// 	database, _ := txn.GetDatabase("db")
	// 	rel, _ := database.GetRelationByName(schema.Name)
	// 	blks := make([]*catalog.BlockEntry, 0)
	// 	it := rel.MakeBlockIt()
	// }
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	start := time.Now()
	testutils.WaitExpect(2000, func() bool {
		return tae.Wal.GetPenddingCnt() == 0
	})
	t.Logf("Wait %s", time.Since(start))
	assert.Equal(t, uint64(0), tae.Wal.GetPenddingCnt())
	t.Logf("Checkpointed: %d", tae.Wal.GetCheckpointed())
	t.Logf("PendingCnt: %d", tae.Wal.GetPenddingCnt())
}
