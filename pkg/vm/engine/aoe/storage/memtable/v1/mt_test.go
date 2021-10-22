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

package memtable

import (
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	dbsched "matrixone/pkg/vm/engine/aoe/storage/db/sched"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock"
	"matrixone/pkg/vm/engine/aoe/storage/testutils/config"
	"matrixone/pkg/vm/engine/aoe/storage/wal/shard"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var WORK_DIR = "/tmp/memtable/mt_test"

func init() {
	os.RemoveAll(WORK_DIR)
}

func TestManager(t *testing.T) {
	dir := "/tmp/testmanager"
	os.RemoveAll(dir)
	opts := config.NewOptions(dir, config.CST_Customize, config.BST_S, config.SST_S)
	defer opts.Meta.Catalog.Close()
	manager := NewManager(opts, nil)
	assert.Equal(t, len(manager.CollectionIDs()), 0)
	capacity := uint64(4096)
	fsMgr := ldio.NewManager(WORK_DIR, false)
	indexBufMgr := bmgr.NewBufferManager(WORK_DIR, capacity)
	mtBufMgr := bmgr.NewBufferManager(WORK_DIR, capacity)
	sstBufMgr := bmgr.NewBufferManager(WORK_DIR, capacity)
	tables := table.NewTables(new(sync.RWMutex), fsMgr, mtBufMgr, sstBufMgr, indexBufMgr)
	tableMeta := metadata.MockTable(opts.Meta.Catalog, nil, 10, nil)
	t0_data, err := tables.RegisterTable(tableMeta)
	assert.Nil(t, err)

	c0, err := manager.RegisterCollection(t0_data)
	assert.Nil(t, err)
	assert.NotNil(t, c0)
	assert.Equal(t, len(manager.CollectionIDs()), 1)
	assert.Equal(t, int64(2), c0.RefCount())
	c0.Unref()
	assert.Equal(t, int64(1), c0.RefCount())
	c00, err := manager.RegisterCollection(t0_data)
	assert.NotNil(t, err)
	assert.Nil(t, c00)
	assert.Equal(t, len(manager.CollectionIDs()), 1)
	c00, err = manager.UnregisterCollection(tableMeta.Id + 1)
	assert.NotNil(t, err)
	assert.Nil(t, c00)
	assert.Equal(t, len(manager.CollectionIDs()), 1)
	t.Log(manager.String())
	c00, err = manager.UnregisterCollection(tableMeta.Id)
	assert.Nil(t, err)
	assert.NotNil(t, c00)
	assert.Equal(t, len(manager.CollectionIDs()), 0)
	assert.Equal(t, int64(1), c0.RefCount())
	assert.Equal(t, int64(1), c00.RefCount())
	c00.Unref()
	assert.Equal(t, int64(0), c0.RefCount())
}

func TestCollection(t *testing.T) {
	os.RemoveAll(WORK_DIR)
	blockRows := uint64(1024)
	cols := 2
	capacity := blockRows * 4 * uint64(cols) * 2 * 2 * 4
	blockCnt := uint64(4)
	opts := config.NewCustomizedMetaOptions(WORK_DIR, config.CST_Customize, blockRows, blockCnt, nil)
	defer opts.Meta.Catalog.Close()

	manager := NewManager(opts, nil)
	fsMgr := ldio.NewManager(WORK_DIR, false)
	indexBufMgr := bmgr.NewBufferManager(WORK_DIR, capacity)
	mtBufMgr := bmgr.NewBufferManager(WORK_DIR, capacity)
	sstBufMgr := bmgr.NewBufferManager(WORK_DIR, capacity)
	tables := table.NewTables(new(sync.RWMutex), fsMgr, mtBufMgr, sstBufMgr, indexBufMgr)
	opts.Scheduler = dbsched.NewScheduler(opts, tables)

	schema := metadata.MockSchema(2)
	tbl, err := opts.Meta.Catalog.SimpleCreateTable(schema, nil)
	assert.Nil(t, err)

	tableMeta := tbl
	t0_data, err := tables.RegisterTable(tableMeta)
	assert.Nil(t, err)
	c0, _ := manager.RegisterCollection(t0_data)
	blks := uint64(20)
	expect_blks := blks
	batch_size := uint64(8)
	step := expect_blks / batch_size
	var waitgroup sync.WaitGroup
	seq := uint64(0)
	for expect_blks > 0 {
		thisStep := step
		if expect_blks < step {
			thisStep = expect_blks
			expect_blks = 0
		} else {
			expect_blks -= step
		}
		waitgroup.Add(1)
		logid := seq
		seq++
		go func(id uint64, wg *sync.WaitGroup) {
			defer wg.Done()
			insert := mock.MockBatch(tbl.Schema.Types(), thisStep*opts.Meta.Conf.BlockMaxRows)
			index := &shard.Index{
				Id:       shard.SimpleIndexId(id),
				Capacity: uint64(insert.Vecs[0].Length()),
			}
			err := c0.Append(insert, index)
			assert.Nil(t, err)
			// t.Log(mtBufMgr.String())
		}(logid, &waitgroup)
	}
	waitgroup.Wait()
	assert.Equal(t, len(tbl.SimpleGetSegmentIds()), int(blks/(opts.Meta.Catalog.Cfg.SegmentMaxBlocks)))
	time.Sleep(time.Duration(40) * time.Millisecond)

	// for _, column := range t0_data.GetCollumns() {
	// 	loopSeg := column.GetSegmentRoot()
	// 	for loopSeg != nil {
	// 		cursor := col.ScanCursor{}
	// 		for {
	// 			loopSeg.InitScanCursor(&cursor)
	// 			err := cursor.Init()
	// 			assert.Nil(t, err)
	// 			cursor.Next()
	// 			if cursor.Current == nil {
	// 				break
	// 			}
	// 		}
	// 		cursor.Close()
	// 		loopSeg.UnRef()
	// 		loopSeg = loopSeg.GetNext()
	// 	}
	// }
	t.Log(mtBufMgr.String())
	t.Log(sstBufMgr.String())
	t.Log(indexBufMgr.String())
	t.Log(fsMgr.String())
	t.Log(manager)
	// t.Log(common.GPool.String())
	t.Log(t0_data.String())

	opts.Scheduler.Stop()
}
