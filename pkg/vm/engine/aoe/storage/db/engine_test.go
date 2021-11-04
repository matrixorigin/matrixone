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
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/adaptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/panjf2000/ants/v2"

	"github.com/stretchr/testify/assert"
)

func TestEngine(t *testing.T) {
	initDBTest()
	inst := initDB(wal.HolderRole)
	tableInfo := adaptor.MockTableInfo(2)
	tid, err := inst.CreateTable(tableInfo, dbi.TableOpCtx{TableName: "mockcon", OpIndex: common.NextGlobalSeqNum()})
	assert.Nil(t, err)
	tblMeta := inst.Store.Catalog.SimpleGetTable(tid)
	assert.NotNil(t, tblMeta)
	blkCnt := inst.Store.Catalog.Cfg.SegmentMaxBlocks
	rows := inst.Store.Catalog.Cfg.BlockMaxRows * blkCnt
	baseCk := mock.MockBatch(tblMeta.Schema.Types(), rows)
	insertCh := make(chan dbi.AppendCtx)
	searchCh := make(chan *dbi.GetSnapshotCtx)

	p, _ := ants.NewPool(40)
	attrs := []string{}
	cols := make([]int, 0)
	for i, colDef := range tblMeta.Schema.ColDefs {
		attrs = append(attrs, colDef.Name)
		cols = append(cols, i)
	}
	hm := host.New(1 << 40)
	gm := guest.New(1<<40, hm)
	proc := process.New(gm)

	tableCnt := 100
	var twg sync.WaitGroup
	for i := 0; i < tableCnt; i++ {
		twg.Add(1)
		f := func(idx int) func() {
			return func() {
				tInfo := *tableInfo
				_, err := inst.CreateTable(&tInfo, dbi.TableOpCtx{TableName: fmt.Sprintf("%dxxxxxx%d", idx, idx),
					OpIndex: common.NextGlobalSeqNum(),
					ShardId: common.NextGlobalSeqNum(),
				})
				assert.Nil(t, err)
				twg.Done()
			}
		}
		p.Submit(f(i))
	}

	reqCtx, cancel := context.WithCancel(context.Background())
	var (
		loopWg   sync.WaitGroup
		searchWg sync.WaitGroup
		loadCnt  uint32
	)
	assert.Nil(t, err)
	task := func(ctx *dbi.GetSnapshotCtx) func() {
		return func() {
			defer searchWg.Done()
			rel, err := inst.Relation(tblMeta.Schema.Name)
			assert.Nil(t, err)
			for _, segId := range rel.SegmentIds().Ids {
				seg := rel.Segment(segId, proc)
				for _, id := range seg.Blocks() {
					blk := seg.Block(id, proc)
					blk.Prefetch(attrs)
					//assert.Nil(t, err)
					//for _, attr := range attrs {
					//	bat.GetVector(attr, proc)
					//	atomic.AddUint32(&loadCnt, uint32(1))
					//}
				}
			}
			rel.Close()
		}
	}
	assert.NotNil(t, task)
	task2 := func(ctx *dbi.GetSnapshotCtx) func() {
		return func() {
			defer searchWg.Done()
			ss, err := inst.GetSnapshot(ctx)
			assert.Nil(t, err)
			segIt := ss.NewIt()
			assert.Nil(t, err)
			if segIt == nil {
				return
			}
			for segIt.Valid() {
				sh := segIt.GetHandle()
				blkIt := sh.NewIt()
				for blkIt.Valid() {
					blkHandle := blkIt.GetHandle()
					hh := blkHandle.Prefetch()
					for idx, _ := range attrs {
						hh.GetReaderByAttr(idx)
						atomic.AddUint32(&loadCnt, uint32(1))
					}
					hh.Close()
					// blkHandle.Close()
					blkIt.Next()
				}
				blkIt.Close()
				segIt.Next()
			}
			segIt.Close()
		}
	}
	assert.NotNil(t, task2)
	loopWg.Add(1)
	loop := func(ctx context.Context) {
		defer loopWg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case req := <-searchCh:
				p.Submit(task2(req))
			case req := <-insertCh:
				loopWg.Add(1)
				t := func() {
					rel, err := inst.Relation(req.TableName)
					assert.Nil(t, err)
					err = rel.Write(req)
					assert.Nil(t, err)
					loopWg.Done()
				}
				go t()
			}
		}
	}
	go loop(reqCtx)

	insertCnt := 8
	var driverWg sync.WaitGroup
	driverWg.Add(1)
	go func() {
		defer driverWg.Done()
		for i := 0; i < insertCnt; i++ {
			req := dbi.AppendCtx{
				TableName: tableInfo.Name,
				Data:      baseCk,
				OpIndex:   common.NextGlobalSeqNum(),
				OpSize:    1,
			}
			insertCh <- req
		}
	}()

	time.Sleep(time.Duration(500) * time.Millisecond)
	searchCnt := 10000
	driverWg.Add(1)
	go func() {
		defer driverWg.Done()
		for i := 0; i < searchCnt; i++ {
			req := &dbi.GetSnapshotCtx{
				TableName: tableInfo.Name,
				ScanAll:   true,
				Cols:      cols,
			}
			searchWg.Add(1)
			searchCh <- req
		}
	}()
	driverWg.Wait()
	searchWg.Wait()
	cancel()
	loopWg.Wait()
	twg.Wait()
	t.Log(inst.MTBufMgr.String())
	t.Log(inst.SSTBufMgr.String())
	t.Log(inst.MemTableMgr.String())
	t.Logf("Load: %d", loadCnt)
	tbl, err := inst.Store.DataTables.WeakRefTable(tid)
	t.Logf("tbl %v, tid %d, err %v", tbl, tid, err)
	assert.Equal(t, tbl.GetRowCount(), rows*uint64(insertCnt))
	t.Log(tbl.GetRowCount())
	attr := tblMeta.Schema.ColDefs[0].Name
	t.Log(tbl.Size(attr))
	attr = tblMeta.Schema.ColDefs[1].Name
	t.Log(tbl.Size(attr))
	t.Log(tbl.String())
	rel, err := inst.Relation(tblMeta.Schema.Name)
	assert.Nil(t, err)
	t.Logf("Rows: %d, Size: %d", rel.Rows(), rel.Size(tblMeta.Schema.ColDefs[0].Name))
	t.Log(inst.GetSegmentIds(dbi.GetSegmentsCtx{TableName: tblMeta.Schema.Name}))
	t.Log(common.GPool.String())
	inst.Close()
}

func TestLogIndex(t *testing.T) {
	initDBTest()
	inst := initDB(wal.HolderRole)
	tableInfo := adaptor.MockTableInfo(2)
	idAlloc := common.IdAlloctor{}
	tid, err := inst.CreateTable(tableInfo, dbi.TableOpCtx{TableName: "mockcon", OpIndex: idAlloc.Alloc()})
	assert.Nil(t, err)
	tblMeta := inst.Store.Catalog.SimpleGetTable(tid)
	assert.NotNil(t, tblMeta)
	rows := inst.Store.Catalog.Cfg.BlockMaxRows * 2 / 5
	baseCk := mock.MockBatch(tblMeta.Schema.Types(), rows)

	// p, _ := ants.NewPool(40)

	for i := 0; i < 50; i++ {
		rel, err := inst.Relation(tblMeta.Schema.Name)
		assert.Nil(t, err)
		err = rel.Write(dbi.AppendCtx{
			OpIndex:   idAlloc.Alloc(),
			OpOffset:  0,
			OpSize:    1,
			Data:      baseCk,
			TableName: tblMeta.Schema.Name,
		})
		assert.Nil(t, err)
		rel.Close()
	}

	_, err = inst.DropTable(dbi.DropTableCtx{TableName: tblMeta.Schema.Name, OpIndex: idAlloc.Alloc()})
	assert.Nil(t, err)
	testutils.WaitExpect(100, func() bool {
		return inst.GetShardCheckpointId(0) == inst.Wal.GetShardCurrSeqNum(0)
	})
	assert.Equal(t, idAlloc.Get(), inst.Wal.GetShardCurrSeqNum(0))
	assert.Equal(t, inst.Wal.GetShardCurrSeqNum(0), inst.GetShardCheckpointId(0))

	inst.Close()
}
