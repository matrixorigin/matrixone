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

	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	idxCommon "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func TestGCBlock1(t *testing.T) {
	tae := initDB(t, nil)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13)
	schema.BlockMaxRows = 100
	schema.SegmentMaxBlocks = 2

	bat := compute.MockBatch(schema.Types(), uint64(schema.BlockMaxRows), int(schema.PrimaryKey), nil)
	txn := tae.StartTxn(nil)
	db, _ := txn.CreateDatabase("db")
	rel, _ := db.CreateRelation(schema)
	err := rel.Append(bat)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())

	txn = tae.StartTxn(nil)
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
	assert.Equal(t, 1, tae.MTBufMgr.Count())
	blkData.Destroy()
	assert.Equal(t, 0, tae.MTBufMgr.Count())

	assert.Equal(t, 2, idxCommon.MockIndexBufferManager.Count())
	err = task.GetNewBlock().GetMeta().(*catalog.BlockEntry).GetBlockData().Destroy()
	assert.Nil(t, err)
	assert.Equal(t, 0, idxCommon.MockIndexBufferManager.Count())
}

func TestAutoGC1(t *testing.T) {
	opts := new(options.Options)
	opts.CheckpointCfg = new(options.CheckpointCfg)
	opts.CheckpointCfg.ScannerInterval = 5
	opts.CheckpointCfg.ExecutionLevels = 5
	opts.CheckpointCfg.ExecutionInterval = 1
	opts.CheckpointCfg.CatalogCkpInterval = 5
	opts.CheckpointCfg.CatalogUnCkpLimit = 1
	tae := initDB(t, opts)
	defer tae.Close()
	schema := catalog.MockSchemaAll(13)
	schema.PrimaryKey = 3
	schema.BlockMaxRows = 20
	schema.SegmentMaxBlocks = 4

	totalRows := uint64(schema.BlockMaxRows * 21 / 2)
	bat := compute.MockBatch(schema.Types(), totalRows, int(schema.PrimaryKey), nil)
	bats := compute.SplitBatch(bat, 100)
	pool, _ := ants.NewPool(50)
	{
		txn := tae.StartTxn(nil)
		database, _ := txn.CreateDatabase("db")
		database.CreateRelation(schema)
		assert.Nil(t, txn.Commit())
	}
	var wg sync.WaitGroup
	doAppend := func(data *gbat.Batch, name string, e *DB, wg *sync.WaitGroup) func() {
		return func() {
			defer wg.Done()
			txn := e.StartTxn(nil)
			database, _ := txn.GetDatabase("db")
			rel, _ := database.GetRelationByName(name)
			err := rel.Append(data)
			assert.Nil(t, err)
			assert.Nil(t, txn.Commit())
		}
	}
	for _, data := range bats {
		wg.Add(1)
		pool.Submit(doAppend(data, schema.Name, tae, &wg))
	}
	cnt := 0
	processor := new(catalog.LoopProcessor)
	processor.BlockFn = func(block *catalog.BlockEntry) error {
		cnt++
		return nil
	}

	testutils.WaitExpect(1000, func() bool {
		cnt = 0
		tae.Catalog.RecurLoop(processor)
		return tae.Scheduler.GetPenddingLSNCnt() == 0 && cnt == 12
	})
	t.Log(tae.Catalog.SimplePPString(common.PPL1))
	t.Logf("GetPenddingLSNCnt: %d", tae.Scheduler.GetPenddingLSNCnt())
	t.Logf("GetCheckpointed: %d", tae.Scheduler.GetCheckpointedLSN())
	assert.Equal(t, 12, cnt)
	assert.Equal(t, uint64(0), tae.Scheduler.GetPenddingLSNCnt())
}
