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
	"os"
	"sync"
	"testing"
	"time"

	bm "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	bmgr "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/factories"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/events/memdata"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/events/meta"
	ldio "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"

	"github.com/stretchr/testify/assert"
)

func TestMutCollection(t *testing.T) {
	dir := "/tmp/memtable/mc"
	os.RemoveAll(dir)
	colcnt := 4
	blockRows, blockCnt := uint64(64), uint64(4)

	opts := config.NewCustomizedMetaOptions(dir, config.CST_Customize, blockRows, blockCnt, nil)
	opts.Meta.Catalog, _ = opts.CreateCatalog(dir)
	opts.Meta.Catalog.Start()
	opts.Wal = shard.NewNoopWal()

	capacity := blockRows * 4 * uint64(colcnt) * 1 * 1 * 2
	// capacity := blockRows * 4 * uint64(colcnt) * 2 * 2 * 4
	manager := NewManager(opts, nil)
	fsMgr := ldio.NewManager(dir, false)
	indexBufMgr := bmgr.NewBufferManager(dir, capacity)
	mtBufMgr := bmgr.NewBufferManager(dir, capacity)
	sstBufMgr := bmgr.NewBufferManager(dir, capacity)
	tables := table.NewTables(new(sync.RWMutex), fsMgr, mtBufMgr, sstBufMgr, indexBufMgr)
	opts.Scheduler = sched.NewScheduler(opts, tables)

	schema := metadata.MockSchema(2)
	tbl, err := opts.Meta.Catalog.SimpleCreateTable(schema, nil)
	assert.Nil(t, err)
	assert.NotNil(t, tbl)

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
	for expectBlks > 0 {
		thisStep := step
		if expectBlks < step {
			thisStep = expectBlks
			expectBlks = 0
		} else {
			expectBlks -= step
		}
		wg.Add(1)
		go func(id uint64, wgp *sync.WaitGroup) {
			defer wgp.Done()
			insert := mock.MockBatch(tbl.Schema.Types(), thisStep*opts.Meta.Conf.BlockMaxRows)
			index := &shard.Index{
				Id:       shard.SimpleIndexId(id),
				Capacity: uint64(insert.Vecs[0].Length()),
			}
			err := c0.Append(insert, index)
			assert.Nil(t, err)
		}(common.NextGlobalSeqNum(), &wg)
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

	ctx := &sched.Context{
		Waitable: true,
		Opts:     opts,
	}
	dropBlkE := meta.NewDropTableEvent(ctx, dbi.DropTableCtx{TableName: tbl.Schema.Name, OpIndex: common.NextGlobalSeqNum()},
		manager, tables)
	opts.Scheduler.Schedule(dropBlkE)
	err = dropBlkE.WaitDone()
	assert.Nil(t, err)

	eCtx := &memdata.Context{
		Opts:      opts,
		MTMgr:     manager,
		Tables:    tables,
		TableMeta: tbl,
		Waitable:  true,
	}
	createTblE := memdata.NewCreateTableEvent(eCtx)
	opts.Scheduler.Schedule(createTblE)
	err = createTblE.WaitDone()
	assert.Nil(t, err)

	dropTblE := memdata.NewDropTableEvent(eCtx, tbl.Id)
	opts.Scheduler.Schedule(dropTblE)
	err = dropTblE.WaitDone()
	assert.Nil(t, err)
	opts.Meta.Catalog.Close()
}
