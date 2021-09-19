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

package memtable

import (
	bm "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"matrixone/pkg/vm/engine/aoe/storage/db/factories"
	"matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/events/meta"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock"
	"matrixone/pkg/vm/engine/aoe/storage/mutation/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/testutils/config"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMutCollection(t *testing.T) {
	dir := "/tmp/memtable/mc"
	os.RemoveAll(dir)
	colcnt := 4
	blockRows, blockCnt := uint64(64), uint64(4)

	opts := config.NewCustomizedMetaOptions(dir, config.CST_Customize, blockRows, blockCnt)

	capacity := blockRows * 4 * uint64(colcnt) * 2 * 2 * 4
	// capacity := blockRows * 4 * uint64(colcnt) * 2 * 2 * 4
	manager := NewManager(opts, nil)
	fsMgr := ldio.NewManager(dir, false)
	indexBufMgr := bmgr.NewBufferManager(dir, capacity)
	mtBufMgr := bmgr.NewBufferManager(dir, capacity)
	sstBufMgr := bmgr.NewBufferManager(dir, capacity)
	tables := table.NewTables(new(sync.RWMutex), fsMgr, mtBufMgr, sstBufMgr, indexBufMgr)
	opts.Scheduler = sched.NewScheduler(opts, tables)

	tabletInfo := metadata.MockTableInfo(colcnt)
	ctx := &sched.Context{Opts: opts, Waitable: true}
	event := meta.NewCreateTableEvent(ctx, dbi.TableOpCtx{TableName: tabletInfo.Name}, tabletInfo)
	assert.NotNil(t, event)
	opts.Scheduler.Schedule(event)
	err := event.WaitDone()
	assert.Nil(t, err)
	tbl := event.GetTable()

	maxsize := uint64(capacity)
	evicter := bm.NewSimpleEvictHolder()
	mgr := buffer.NewNodeManager(maxsize, evicter)
	factory := factories.NewMutFactory(mgr, nil)
	tables.MutFactory = factory

	t0, err := tables.RegisterTable(tbl)
	assert.Nil(t, err)

	t0.Ref()
	c0 := newMutableCollection(manager, t0)
	blks := uint64(20)
	expectBlks := blks
	batchSize := uint64(4)
	step := expectBlks / batchSize
	var wg sync.WaitGroup
	seq := uint64(0)
	for expectBlks > 0 {
		thisStep := step
		if expectBlks < step {
			thisStep = expectBlks
			expectBlks = 0
		} else {
			expectBlks -= step
		}
		wg.Add(1)
		logId := seq
		seq++
		go func(id uint64, wgp *sync.WaitGroup) {
			defer wgp.Done()
			insert := mock.MockBatch(tbl.Schema.Types(), thisStep*opts.Meta.Conf.BlockMaxRows)
			index := &metadata.LogIndex{
				ID:       id,
				Capacity: uint64(insert.Vecs[0].Length()),
			}
			err := c0.Append(insert, index)
			assert.Nil(t, err)
		}(logId, &wg)
	}
	wg.Wait()
	t.Log(mgr.String())
	time.Sleep(time.Duration(50) * time.Millisecond)
	c0.Unref()
	time.Sleep(time.Duration(100) * time.Millisecond)
	t0.Unref()
	assert.Equal(t, int64(0), t0.RefCount())
	t.Log(mgr.String())
	t.Log(sstBufMgr.String())
	assert.Equal(t, 0, mgr.Count())
}
