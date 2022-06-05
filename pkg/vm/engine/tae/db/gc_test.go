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
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func TestGCBlock1(t *testing.T) {
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	schema.BlockMaxRows = 100
	schema.SegmentMaxBlocks = 2

	bat := catalog.MockData(schema, schema.BlockMaxRows)
	createRelationAndAppend(t, tae, "db", schema, bat, true)

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
	assert.Equal(t, 3, tae.MTBufMgr.Count())
	err = blkData.Destroy()
	assert.Nil(t, err)
	assert.Equal(t, 2, tae.MTBufMgr.Count())

	err = task.GetNewBlock().GetMeta().(*catalog.BlockEntry).GetBlockData().Destroy()
	assert.Nil(t, err)
	assert.Equal(t, 0, tae.MTBufMgr.Count())
	t.Log(tae.MTBufMgr.String())
}

func TestAutoGC1(t *testing.T) {
	opts := config.WithQuickScanAndCKPOpts(nil)
	tae := initDB(t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 3)
	schema.BlockMaxRows = 20
	schema.SegmentMaxBlocks = 4

	totalRows := schema.BlockMaxRows * 21 / 2
	bat := catalog.MockData(schema, totalRows)
	bats := compute.SplitBatch(bat, 100)
	createRelation(t, tae, "db", schema, true)
	pool, _ := ants.NewPool(50)
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
	testutils.WaitExpect(1000, func() bool {
		return dbEntry.CoarseTableCnt() == 0
	})
	assert.Equal(t, 0, dbEntry.CoarseTableCnt())
	t.Logf("Takes: %s", time.Since(now))
	printCheckpointStats(t, tae)

	bat := catalog.MockData(schema, schema.BlockMaxRows*uint32(schema.SegmentMaxBlocks+1)-1)
	bats := compute.SplitBatch(bat, 4)

	// 3. Create a table and append 7 rows
	db, _ = createRelationAndAppend(t, tae, "db", schema, bats[0], false)

	names := getSegmentFileNames(tae.Dir)
	assert.Equal(t, 1, len(names))

	// 4. Drop the table
	dropRelation(t, tae, "db", schema.Name)

	dbEntry, _ = tae.Catalog.GetDatabaseByID(db.GetID())
	now = time.Now()
	testutils.WaitExpect(1000, func() bool {
		return dbEntry.CoarseTableCnt() == 0
	})
	assert.Equal(t, 0, dbEntry.CoarseTableCnt())
	t.Logf("Takes: %s", time.Since(now))
	printCheckpointStats(t, tae)
	names = getSegmentFileNames(tae.Dir)
	assert.Equal(t, 0, len(names))

	// 5. Create a table and append 3 block
	db, _ = createRelationAndAppend(t, tae, "db", schema, bat, false)
	names = getSegmentFileNames(tae.Dir)
	t.Log(names)
	assert.Equal(t, 2, len(names))
	printCheckpointStats(t, tae)

	compactBlocks(t, tae, "db", schema, true)

	// 6. Drop the table
	dropRelation(t, tae, "db", schema.Name)
	testutils.WaitExpect(200, func() bool {
		return dbEntry.CoarseTableCnt() == 0
	})
	names = getSegmentFileNames(tae.Dir)
	printCheckpointStats(t, tae)
	t.Log(names)
	assert.Equal(t, 0, dbEntry.CoarseTableCnt())
	names = getSegmentFileNames(tae.Dir)
	assert.Equal(t, 0, len(names))

	// 7. Create a table
	db, _ = createRelation(t, tae, "db", schema, false)

	// 8. Append blocks and drop
	var wg sync.WaitGroup
	pool, _ := ants.NewPool(5)
	bat = catalog.MockData(schema, schema.BlockMaxRows*10)
	bats = compute.SplitBatch(bat, 20)
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
	names = getSegmentFileNames(tae.Dir)
	assert.Equal(t, 0, len(names))
	// t.Log(common.GPool.String())
}
