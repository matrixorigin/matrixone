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
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func printCheckpointStats(t *testing.T, tae *DB) {
	t.Logf("GetCheckpointedLSN: %d", tae.Wal.GetCheckpointed())
	t.Logf("GetPenddingLSNCnt: %d", tae.Wal.GetPenddingCnt())
}

func getSegmentFileNames(dir string) (names map[uint64]string) {
	names = make(map[uint64]string)
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		name := f.Name()
		segName := strings.TrimSuffix(name, ".seg")
		if segName == name {
			continue
		}
		id, err := strconv.ParseUint(segName, 10, 64)
		if err != nil {
			panic(err)
		}
		names[id] = name
	}
	return
}

func enableCheckpoint(in *options.Options) (opts *options.Options) {
	if in == nil {
		opts = new(options.Options)
	} else {
		opts = in
	}
	opts.CheckpointCfg = new(options.CheckpointCfg)
	opts.CheckpointCfg.ScannerInterval = 10
	opts.CheckpointCfg.ExecutionLevels = 5
	opts.CheckpointCfg.ExecutionInterval = 1
	opts.CheckpointCfg.CatalogCkpInterval = 5
	opts.CheckpointCfg.CatalogUnCkpLimit = 1
	return opts
}

func appendFailClosure(t *testing.T, data *gbat.Batch, name string, e *DB, wg *sync.WaitGroup) func() {
	return func() {
		if wg != nil {
			defer wg.Done()
		}
		txn, _ := e.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(name)
		err := rel.Append(data)
		assert.NotNil(t, err)
		assert.Nil(t, txn.Rollback())
	}
}

func appendClosure(t *testing.T, data *gbat.Batch, name string, e *DB, wg *sync.WaitGroup) func() {
	return func() {
		if wg != nil {
			defer wg.Done()
		}
		txn, _ := e.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(name)
		err := rel.Append(data)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
}

func tryAppendClosure(t *testing.T, data *gbat.Batch, name string, e *DB, wg *sync.WaitGroup) func() {
	return func() {
		if wg != nil {
			defer wg.Done()
		}
		txn, _ := e.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, err := database.GetRelationByName(name)
		if err != nil {
			_ = txn.Rollback()
			return
		}
		if err = rel.Append(data); err != nil {
			_ = txn.Rollback()
			return
		}
		_ = txn.Commit()
	}
}

func TestGCBlock1(t *testing.T) {
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	schema.BlockMaxRows = 100
	schema.SegmentMaxBlocks = 2

	bat := catalog.MockData(schema, schema.BlockMaxRows)
	txn, _ := tae.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	rel, _ := db.CreateRelation(schema)
	err := rel.Append(bat)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn, _ = tae.StartTxn(nil)
	db, _ = txn.GetDatabase("db")
	rel, _ = db.GetRelationByName(schema.Name)
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
	opts := enableCheckpoint(nil)
	tae := initDB(t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 3)
	schema.BlockMaxRows = 20
	schema.SegmentMaxBlocks = 4

	totalRows := schema.BlockMaxRows * 21 / 2
	bat := catalog.MockData(schema, totalRows)
	bats := compute.SplitBatch(bat, 100)
	pool, _ := ants.NewPool(50)
	{
		txn, _ := tae.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		_, err := database.CreateRelation(schema)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit())
	}
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
	opts := enableCheckpoint(nil)
	tae := initDB(t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13, 12)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 2

	// 1. Create a table without data
	txn, _ := tae.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	_, _ = db.CreateRelation(schema)
	assert.NoError(t, txn.Commit())

	// 2. Drop the table
	txn, _ = tae.StartTxn(nil)
	db, _ = txn.GetDatabase("db")
	_, err := db.DropRelationByName(schema.Name)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

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
	txn, _ = tae.StartTxn(nil)
	db, _ = txn.GetDatabase("db")
	rel, _ := db.CreateRelation(schema)
	err = rel.Append(bats[0])
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

	names := getSegmentFileNames(tae.Dir)
	assert.Equal(t, 1, len(names))

	// 4. Drop the table
	txn, _ = tae.StartTxn(nil)
	db, _ = txn.GetDatabase("db")
	_, err = db.DropRelationByName(schema.Name)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

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
	txn, _ = tae.StartTxn(nil)
	db, _ = txn.GetDatabase("db")
	rel, err = db.CreateRelation(schema)
	assert.NoError(t, err)
	err = rel.Append(bat)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())
	names = getSegmentFileNames(tae.Dir)
	t.Log(names)
	assert.Equal(t, 2, len(names))
	printCheckpointStats(t, tae)

	compactBlocks(t, tae, "db", schema, true)

	// 6. Drop the table
	txn, _ = tae.StartTxn(nil)
	db, _ = txn.GetDatabase("db")
	_, err = db.DropRelationByName(schema.Name)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())
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
	txn, _ = tae.StartTxn(nil)
	db, _ = txn.GetDatabase("db")
	_, err = db.CreateRelation(schema)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit())

	// 8. Append blocks and drop
	var wg sync.WaitGroup
	pool, _ := ants.NewPool(5)
	bat = catalog.MockData(schema, schema.BlockMaxRows*10)
	bats = compute.SplitBatch(bat, 20)
	for i := range bats[:10] {
		wg.Add(1)
		pool.Submit(tryAppendClosure(t, bats[i], schema.Name, tae, &wg))
	}
	wg.Add(1)
	pool.Submit(func() {
		defer wg.Done()
		txn, _ := tae.StartTxn(nil)
		db, _ := txn.GetDatabase("db")
		_, err := db.DropRelationByName(schema.Name)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit())
	})
	for i := range bats[10:] {
		wg.Add(1)
		pool.Submit(tryAppendClosure(t, bats[i+10], schema.Name, tae, &wg))
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
