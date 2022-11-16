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
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func TestGCBlock1(t *testing.T) {
	defer testutils.AfterTest(t)()
	t.Skip(any("GC is not working, refactor needed"))
	testutils.EnsureNoLeak(t)
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	schema.BlockMaxRows = 100
	schema.SegmentMaxBlocks = 2

	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows))
	defer bat.Close()
	createRelationAndAppend(t, 0, tae, "db", schema, bat, true)

	txn, _ := tae.StartTxn(nil)
	db, _ := txn.GetDatabase("db")
	rel, _ := db.GetRelationByName(schema.Name)
	it := rel.MakeBlockIt()
	blk := it.GetBlock()
	meta := blk.GetMeta().(*catalog.BlockEntry)
	task, err := jobs.NewCompactBlockTask(nil, txn, meta, tae.Scheduler)
	assert.Nil(t, err)
	err = task.OnExec()
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	t.Log(tae.Opts.Catalog.SimplePPString(common.PPL1))

	err = meta.GetSegment().RemoveEntry(meta)
	assert.Nil(t, err)
	blkData := meta.GetBlockData()
	// 13 zonemap + 1 bloomfilter
	assert.Equal(t, 0, tae.MTBufMgr.Count())
	err = blkData.Destroy()
	assert.Nil(t, err)
	assert.Equal(t, 0, tae.MTBufMgr.Count())

	err = task.GetNewBlock().GetMeta().(*catalog.BlockEntry).GetBlockData().Destroy()
	assert.Nil(t, err)
	assert.Equal(t, 0, tae.MTBufMgr.Count())
	t.Log(tae.MTBufMgr.String())
}

func TestAutoGC1(t *testing.T) {
	defer testutils.AfterTest(t)()
	t.Skip(any("GC is not working, refactor needed"))
	testutils.EnsureNoLeak(t)
	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := initDB(t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 3)
	schema.BlockMaxRows = 20
	schema.SegmentMaxBlocks = 4

	totalRows := schema.BlockMaxRows * 21 / 2
	bat := catalog.MockBatch(schema, int(totalRows))
	defer bat.Close()
	bats := bat.Split(100)
	createRelation(t, tae, "db", schema, true)
	pool, _ := ants.NewPool(50)
	defer pool.Release()
	var wg sync.WaitGroup
	for _, data := range bats {
		wg.Add(1)
		err := pool.Submit(appendClosure(t, data, schema.Name, tae, &wg))
		assert.Nil(t, err)
	}
	cnt := 0
	processor := new(catalog.LoopProcessor)
	processor.BlockFn = func(block *catalog.BlockEntry) error {
		cnt++
		return nil
	}

	testutils.WaitExpect(2000, func() bool {
		cnt = 0
		err := tae.Catalog.RecurLoop(processor)
		assert.Nil(t, err)
		return tae.Scheduler.GetPenddingLSNCnt() == 0 && cnt == 12
	})
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	t.Logf("GetPenddingLSNCnt: %d", tae.Scheduler.GetPenddingLSNCnt())
	t.Logf("GetCheckpointed: %d", tae.Scheduler.GetCheckpointedLSN())
	// assert.Equal(t, 12, cnt)
	t.Logf("BlockCnt %d, Expect 12", cnt)
	assert.Equal(t, uint64(0), tae.Scheduler.GetPenddingLSNCnt())
}

// Test Steps
// 1. Create a table w/o data and commit
// 2. Drop the table and commit
// 3. Create a table w one appendable block data and commit
// 4. Drop the table and commit
func TestGCTable(t *testing.T) {
	defer testutils.AfterTest(t)()
	t.Skip(any("GC is not working, refactor needed"))
	testutils.EnsureNoLeak(t)
	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := initDB(t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2

	// 1. Create a table without data
	db, _ := createRelation(t, tae, "db", schema, true)

	// 2. Drop the table
	dropRelation(t, tae, "db", schema.Name)

	dbEntry, _ := tae.Catalog.GetDatabaseByID(db.GetID())
	now := time.Now()
	testutils.WaitExpect(2000, func() bool {
		return dbEntry.CoarseTableCnt() == 0
	})
	assert.Equal(t, 0, dbEntry.CoarseTableCnt())
	t.Logf("Takes: %s", time.Since(now))
	printCheckpointStats(t, tae)

	bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*uint32(schema.SegmentMaxBlocks+1)-1))
	defer bat.Close()
	bats := bat.Split(4)

	// 3. Create a table and append 7 rows
	db, _ = createRelationAndAppend(t, 0, tae, "db", schema, bats[0], false)

	testutils.WaitExpect(2000, func() bool {
		blocksnames := getBlockFileNames(tae)
		return len(blocksnames) == 2
	})
	blocksnames := getBlockFileNames(tae)
	assert.Equal(t, 2, len(blocksnames))

	// 4. Drop the table
	dropRelation(t, tae, "db", schema.Name)

	dbEntry, _ = tae.Catalog.GetDatabaseByID(db.GetID())
	now = time.Now()
	testutils.WaitExpect(2000, func() bool {
		return dbEntry.CoarseTableCnt() == 0
	})
	assert.Equal(t, 0, dbEntry.CoarseTableCnt())
	t.Logf("Takes: %s", time.Since(now))
	printCheckpointStats(t, tae)
	names := getSegmentFileNames(tae)
	assert.Equal(t, 0, len(names))

	// 5. Create a table and append 3 block
	createRelationAndAppend(t, 0, tae, "db", schema, bat, false)
	testutils.WaitExpect(2000, func() bool {
		names = getSegmentFileNames(tae)
		return len(names) == 1
	})
	names = getSegmentFileNames(tae)
	t.Log(names)
	assert.Equal(t, 1, len(names))
	printCheckpointStats(t, tae)

	compactBlocks(t, 0, tae, "db", schema, true)

	// 6. Drop the table
	dropRelation(t, tae, "db", schema.Name)
	testutils.WaitExpect(2000, func() bool {
		return dbEntry.CoarseTableCnt() == 0
	})
	names = getSegmentFileNames(tae)
	printCheckpointStats(t, tae)
	t.Log(names)
	assert.Equal(t, 0, dbEntry.CoarseTableCnt())
	names = getSegmentFileNames(tae)
	assert.Equal(t, 0, len(names))

	// 7. Create a table
	createRelation(t, tae, "db", schema, false)

	// 8. Append blocks and drop
	var wg sync.WaitGroup
	pool, _ := ants.NewPool(5)
	defer pool.Release()
	bat = catalog.MockBatch(schema, int(schema.BlockMaxRows*10))
	defer bat.Close()
	bats = bat.Split(20)
	for i := range bats[:10] {
		wg.Add(1)
		_ = pool.Submit(tryAppendClosure(t, bats[i], schema.Name, tae, &wg))
	}
	wg.Add(1)
	_ = pool.Submit(func() {
		defer wg.Done()
		dropRelation(t, tae, "db", schema.Name)
	})
	for i := range bats[10:] {
		wg.Add(1)
		_ = pool.Submit(tryAppendClosure(t, bats[i+10], schema.Name, tae, &wg))
	}
	wg.Wait()
	printCheckpointStats(t, tae)
	testutils.WaitExpect(2000, func() bool {
		return dbEntry.CoarseTableCnt() == 0
	})
	printCheckpointStats(t, tae)
	assert.Equal(t, 0, dbEntry.CoarseTableCnt())
	names = getSegmentFileNames(tae)
	assert.Equal(t, 0, len(names))
	// t.Log(common.DefaultAllocator.String())
}

// Test Steps
// 1. Create a db with 2 tables w/o data
// 2. Drop the db
func TestGCDB(t *testing.T) {
	defer testutils.AfterTest(t)()
	t.Skip(any("GC is not working, refactor needed"))
	testutils.EnsureNoLeak(t)
	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := initDB(t, opts)
	defer tae.Close()
	dbCnt := tae.Catalog.CoarseDBCnt()
	tableCnt := tae.Catalog.CoarseTableCnt()
	columnCnt := tae.Catalog.CoarseColumnCnt()

	schema1 := catalog.MockSchema(13, 12)
	schema1.BlockMaxRows = 10
	schema1.SegmentMaxBlocks = 2
	schema2 := catalog.MockSchema(13, 12)
	schema2.BlockMaxRows = 10
	schema2.SegmentMaxBlocks = 2

	createRelation(t, tae, "db", schema1, true)
	createRelation(t, tae, "db", schema2, false)
	dropDB(t, tae, "db")
	testutils.WaitExpect(2000, func() bool {
		return tae.Catalog.CoarseDBCnt() == 1
	})
	printCheckpointStats(t, tae)
	assert.Equal(t, 1, tae.Catalog.CoarseDBCnt())

	bat1 := catalog.MockBatch(schema1, int(schema1.BlockMaxRows*3-1))
	bat2 := catalog.MockBatch(schema2, int(schema2.BlockMaxRows*3-1))
	defer bat1.Close()
	defer bat2.Close()

	createRelation(t, tae, "db", schema1, true)
	createRelation(t, tae, "db", schema2, false)
	appendClosure(t, bat1, schema1.Name, tae, nil)()
	appendClosure(t, bat2, schema2.Name, tae, nil)()
	dropDB(t, tae, "db")

	testutils.WaitExpect(2000, func() bool {
		return tae.Catalog.CoarseDBCnt() == 1
	})
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	assert.Equal(t, 1, tae.Catalog.CoarseDBCnt())
	names := getSegmentFileNames(tae)
	assert.Equal(t, 0, len(names))

	createRelation(t, tae, "db", schema1, true)
	createRelation(t, tae, "db", schema2, false)
	appendClosure(t, bat1, schema1.Name, tae, nil)()
	appendClosure(t, bat2, schema2.Name, tae, nil)()
	compactBlocks(t, 0, tae, "db", schema1, true)
	compactBlocks(t, 0, tae, "db", schema2, true)
	dropDB(t, tae, "db")

	testutils.WaitExpect(4000, func() bool {
		return tae.Catalog.CoarseDBCnt() == 1
	})
	assert.Equal(t, 1, tae.Catalog.CoarseDBCnt())
	names = getSegmentFileNames(tae)
	assert.Equal(t, 0, len(names))

	createDB(t, tae, "db")

	var wg sync.WaitGroup
	pool, _ := ants.NewPool(4)
	defer pool.Release()
	routine := func() {
		defer wg.Done()
		schema := catalog.MockSchema(3, 2)
		schema.BlockMaxRows = 10
		schema.SegmentMaxBlocks = 2
		bat := catalog.MockBatch(schema, int(schema.BlockMaxRows*uint32(rand.Intn(4)+1)-1))
		defer bat.Close()
		txn, _ := tae.StartTxn(nil)
		db, err := txn.GetDatabase("db")
		if err != nil {
			_ = txn.Rollback()
			return
		}
		_, err = db.CreateRelation(schema)
		assert.NoError(t, err)
		err = txn.Commit()
		if err != nil {
			return
		}

		txn, _ = tae.StartTxn(nil)
		db, err = txn.GetDatabase("db")
		if err != nil {
			_ = txn.Rollback()
			return
		}
		rel, err := db.GetRelationByName(schema.Name)
		assert.NoError(t, err)
		err = rel.Append(bat)
		assert.NoError(t, err)
		_ = txn.Commit()
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		_ = pool.Submit(routine)
	}
	dropDB(t, tae, "db")
	wg.Wait()

	testutils.WaitExpect(5000, func() bool {
		return tae.Catalog.CoarseDBCnt() == 1
	})
	assert.Equal(t, 1, tae.Catalog.CoarseDBCnt())
	names = getSegmentFileNames(tae)
	assert.Equal(t, 0, len(names))
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	printCheckpointStats(t, tae)
	assert.Equal(t, dbCnt, tae.Catalog.CoarseDBCnt())
	assert.Equal(t, tableCnt, tae.Catalog.CoarseTableCnt())
	assert.Equal(t, columnCnt, tae.Catalog.CoarseColumnCnt())
}
