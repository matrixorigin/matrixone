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

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
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
	inst, gen, database := initDB2(wal.HolderRole, "db1", uint64(100))
	schema := metadata.MockSchema(2)
	schema.Name = "mockcon"
	shardId := database.GetShardId()
	tid, err := inst.CreateTable(database.Name, schema, gen.Next(shardId))
	assert.Nil(t, err)
	tblMeta := database.SimpleGetTable(tid)
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

	tableCnt := 20
	var twg sync.WaitGroup
	for i := 0; i < tableCnt; i++ {
		twg.Add(1)
		f := func(idx int) func() {
			return func() {
				schema := metadata.MockSchema(2)
				schema.Name = fmt.Sprintf("%dxxxxxx%d", idx, idx)
				_, err := inst.CreateTable(database.Name, schema, gen.Next(shardId))
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
			rel, err := inst.Relation(database.Name, tblMeta.Schema.Name)
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
					rel, err := inst.Relation(database.Name, req.TableName)
					assert.Nil(t, err)
					assert.NotNil(t, rel)
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
				DBName:    database.Name,
				ShardId:   shardId,
				TableName: schema.Name,
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
				ShardId:   shardId,
				DBName:    database.Name,
				TableName: schema.Name,
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
	rel, err := inst.Relation(database.Name, tblMeta.Schema.Name)
	assert.Nil(t, err)
	t.Logf("Rows: %d, Size: %d", rel.Rows(), rel.Size(tblMeta.Schema.ColDefs[0].Name))
	t.Log(inst.GetSegmentIds(database.Name, tblMeta.Schema.Name))
	t.Log(common.GPool.String())
	inst.Close()
}

func TestLogIndex(t *testing.T) {
	initDBTest()
	inst, gen, database := initDB2(wal.HolderRole, "db1", uint64(100))
	schema := metadata.MockSchema(2)
	schema.Name = "t1"
	shardId := database.GetShardId()
	tid, err := inst.CreateTable(database.Name, schema, gen.Next(shardId))
	assert.Nil(t, err)
	tblMeta := database.SimpleGetTable(tid)
	assert.NotNil(t, tblMeta)
	rows := inst.Store.Catalog.Cfg.BlockMaxRows * 2 / 5
	baseCk := mock.MockBatch(tblMeta.Schema.Types(), rows)

	for i := 0; i < 50; i++ {
		rel, err := inst.Relation(database.Name, tblMeta.Schema.Name)
		assert.Nil(t, err)
		err = rel.Write(dbi.AppendCtx{
			ShardId:   database.GetShardId(),
			OpIndex:   gen.Alloc(shardId),
			OpOffset:  0,
			OpSize:    1,
			Data:      baseCk,
			TableName: tblMeta.Schema.Name,
			DBName:    database.Name,
		})
		assert.Nil(t, err)
		rel.Close()
	}

	_, err = inst.DropTable(dbi.DropTableCtx{ShardId: shardId, DBName: database.Name, TableName: tblMeta.Schema.Name, OpIndex: gen.Alloc(shardId)})
	assert.Nil(t, err)
	testutils.WaitExpect(100, func() bool {
		return inst.GetShardCheckpointId(shardId) == inst.Wal.GetShardCurrSeqNum(shardId)
	})
	assert.Equal(t, gen.Get(shardId), inst.Wal.GetShardCurrSeqNum(shardId))
	assert.Equal(t, inst.Wal.GetShardCurrSeqNum(shardId), inst.GetShardCheckpointId(shardId))

	inst.Close()
}
