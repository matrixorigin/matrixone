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
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/internal/invariants"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"

	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

var (
	TEST_DB_DIR = "/tmp/db_test"
)

func initDBTest() {
	os.RemoveAll(TEST_DB_DIR)
}

func initDB(walRole wal.Role) (*DB, *shard.MockIndexAllocator) {
	rand.Seed(time.Now().UnixNano())
	opts := new(storage.Options)
	opts.WalRole = walRole
	config.NewCustomizedMetaOptions(TEST_DB_DIR, config.CST_Customize, uint64(2000), uint64(2), opts)
	inst, _ := Open(TEST_DB_DIR, opts)
	gen := shard.NewMockIndexAllocator()
	return inst, gen
}

func initDB2(walRole wal.Role, dbName string, shardId uint64) (*DB, *shard.MockIndexAllocator, *metadata.Database) {
	inst, gen := initDB(walRole)
	database, _ := inst.CreateDatabase(dbName, shardId)
	return inst, gen, database
}

func TestCreateTable(t *testing.T) {
	initDBTest()
	inst, gen := initDB(wal.HolderRole)
	assert.NotNil(t, inst)
	defer inst.Close()
	tblCnt := rand.Intn(5) + 3
	prefix := "mocktbl_"
	var wg sync.WaitGroup
	names := make([]string, 0)

	for i := 0; i < tblCnt; i++ {
		schema := metadata.MockSchema(2)
		name := fmt.Sprintf("%s%d", prefix, i)
		schema.Name = name
		names = append(names, name)
		wg.Add(1)
		go func(w *sync.WaitGroup, shardId uint64) {
			dbName := strconv.FormatUint(shardId, 10)
			_, err := inst.CreateDatabase(dbName, shardId)
			assert.Nil(t, err)

			_, err = inst.CreateTable(dbName, schema, gen.Next(shardId))
			assert.Nil(t, err)
			w.Done()
		}(&wg, uint64(i)+1)
	}
	wg.Wait()
	dbNames := inst.Store.Catalog.SimpleGetDatabaseNames()
	assert.Equal(t, tblCnt, len(dbNames))
	t.Log(inst.Store.Catalog.PString(metadata.PPL0))
}

func TestCreateDuplicateTable(t *testing.T) {
	initDBTest()
	inst, gen, database := initDB2(wal.BrokerRole, "db1", uint64(100))
	defer inst.Close()

	schema := metadata.MockSchema(2)
	_, err := inst.CreateTable(database.Name, schema, gen.Next(database.GetShardId()))
	assert.Nil(t, err)
	_, err = inst.CreateTable(database.Name, schema, gen.Next(database.GetShardId()))
	assert.NotNil(t, err)
}

func TestDropEmptyTable(t *testing.T) {
	initDBTest()
	inst, gen, database := initDB2(wal.BrokerRole, "db1", uint64(100))
	defer inst.Close()

	schema := metadata.MockSchema(2)
	schema.Name = "t1"

	_, err := inst.CreateTable(database.Name, schema, gen.Next(database.GetShardId()))
	assert.Nil(t, err)
	_, err = inst.DropTable(dbi.DropTableCtx{ShardId: database.GetShardId(), DBName: database.Name,
		TableName: schema.Name, OpIndex: gen.Alloc(database.GetShardId())})
	assert.Nil(t, err)
}

func TestDropTable(t *testing.T) {
	initDBTest()
	inst, gen, database := initDB2(wal.BrokerRole, "db1", uint64(100))
	defer inst.Close()

	name := "t1"
	schema := metadata.MockSchema(2)
	schema.Name = name

	tid, err := inst.CreateTable(database.Name, schema, gen.Next(database.GetShardId()))
	assert.Nil(t, err)

	ssCtx := &dbi.GetSnapshotCtx{
		DBName:    database.Name,
		TableName: name,
		Cols:      []int{0},
		ScanAll:   true,
	}

	ss, err := inst.GetSnapshot(ssCtx)
	assert.Nil(t, err)
	ss.Close()

	dropTid, err := inst.DropTable(dbi.DropTableCtx{
		ShardId:   database.GetShardId(),
		OpIndex:   gen.Alloc(database.GetShardId()),
		TableName: name,
		DBName:    database.Name,
	})
	assert.Nil(t, err)
	assert.Equal(t, tid, dropTid)

	ss, err = inst.GetSnapshot(ssCtx)
	assert.NotNil(t, err)
	assert.Nil(t, ss)

	tid2, err := inst.CreateTable(database.Name, schema, gen.Next(database.GetShardId()))
	assert.Nil(t, err)
	assert.NotEqual(t, tid, tid2)

	ss, err = inst.GetSnapshot(ssCtx)
	assert.Nil(t, err)
	ss.Close()
	t.Log(inst.Wal.String())
}

func TestAppend(t *testing.T) {
	initDBTest()
	inst, gen, database := initDB2(wal.BrokerRole, "db1", uint64(100))

	schema := metadata.MockSchema(2)
	schema.Name = "mocktbl"

	tid, err := inst.CreateTable(database.Name, schema, gen.Next(database.GetShardId()))
	assert.Nil(t, err)
	tblMeta := database.SimpleGetTable(tid)
	assert.NotNil(t, tblMeta)
	blkCnt := 2
	rows := inst.Store.Catalog.Cfg.BlockMaxRows * uint64(blkCnt)
	ck := mock.MockBatch(tblMeta.Schema.Types(), rows)
	assert.Equal(t, int(rows), ck.Vecs[0].Length())
	invalidName := "xxx"
	appendCtx := dbi.AppendCtx{
		OpIndex:   gen.Alloc(database.GetShardId()),
		OpSize:    1,
		TableName: invalidName,
		DBName:    database.Name,
		Data:      ck,
		ShardId:   database.GetShardId(),
	}
	err = inst.Append(appendCtx)
	assert.NotNil(t, err)
	insertCnt := 8
	appendCtx.TableName = tblMeta.Schema.Name
	for i := 0; i < insertCnt; i++ {
		appendCtx.OpIndex = gen.Alloc(database.GetShardId())
		err = inst.Append(appendCtx)
		assert.Nil(t, err)
	}

	cols := []int{0, 1}
	tbl, _ := inst.Store.DataTables.WeakRefTable(tid)
	segIds := tbl.SegmentIds()
	ssCtx := &dbi.GetSnapshotCtx{
		DBName:     database.Name,
		TableName:  schema.Name,
		SegmentIds: segIds,
		Cols:       cols,
	}

	blkCount := 0
	segCount := 0
	ss, err := inst.GetSnapshot(ssCtx)
	assert.Nil(t, err)
	segIt := ss.NewIt()
	assert.NotNil(t, segIt)

	for segIt.Valid() {
		segCount++
		segH := segIt.GetHandle()
		assert.NotNil(t, segH)
		blkIt := segH.NewIt()
		// segH.Close()
		assert.NotNil(t, blkIt)
		for blkIt.Valid() {
			blkCount++
			blkIt.Next()
		}
		blkIt.Close()
		segIt.Next()
	}
	segIt.Close()
	assert.Equal(t, insertCnt, segCount)
	assert.Equal(t, blkCnt*insertCnt, blkCount)
	ss.Close()

	time.Sleep(time.Duration(50) * time.Millisecond)
	t.Log(inst.MTBufMgr.String())
	t.Log(inst.SSTBufMgr.String())
	t.Log(inst.IndexBufMgr.String())
	// t.Log(inst.FsMgr.String())
	// t.Log(tbl.GetIndexHolder().String())
	t.Log(inst.Wal.String())
	inst.Close()
}

func TestConcurrency(t *testing.T) {
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

	reqCtx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	var searchWg sync.WaitGroup
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case req := <-searchCh:
				f := func() {
					defer searchWg.Done()
					{
						ss, err := inst.GetSnapshot(req)
						assert.Nil(t, err)
						segIt := ss.NewIt()
						assert.Nil(t, err)
						if segIt == nil {
							return
						}
						segCnt := 0
						blkCnt := 0
						for segIt.Valid() {
							segCnt++
							sh := segIt.GetHandle()
							blkIt := sh.NewIt()
							for blkIt.Valid() {
								blkCnt++
								blkHandle := blkIt.GetHandle()
								hh := blkHandle.Prefetch()
								hh.Close()
								// blkHandle.Close()
								blkIt.Next()
							}
							blkIt.Close()
							segIt.Next()
						}
						segIt.Close()
						ss.Close()
					}
				}
				p.Submit(f)
			case req := <-insertCh:
				wg.Add(1)
				go func() {
					err := inst.Append(req)
					assert.Nil(t, err)
					wg.Done()
				}()
			}
		}
	}(reqCtx)

	insertCnt := 8
	var wg2 sync.WaitGroup

	wg2.Add(1)
	go func() {
		defer wg2.Done()
		for i := 0; i < insertCnt; i++ {
			insertReq := dbi.AppendCtx{
				ShardId:   shardId,
				DBName:    database.Name,
				TableName: schema.Name,
				Data:      baseCk,
				OpIndex:   gen.Alloc(shardId),
				OpSize:    1,
			}
			insertCh <- insertReq
		}
	}()

	cols := make([]int, 0)
	for i := 0; i < len(tblMeta.Schema.ColDefs); i++ {
		cols = append(cols, i)
	}
	wg2.Add(1)
	go func() {
		defer wg2.Done()
		reqCnt := 10000
		for i := 0; i < reqCnt; i++ {
			tbl, _ := inst.Store.DataTables.WeakRefTable(tid)
			for tbl == nil {
				time.Sleep(time.Duration(100) * time.Microsecond)
				tbl, _ = inst.Store.DataTables.WeakRefTable(tid)
			}
			segIds := tbl.SegmentIds()
			searchReq := &dbi.GetSnapshotCtx{
				ShardId:    shardId,
				DBName:     database.Name,
				TableName:  schema.Name,
				SegmentIds: segIds,
				Cols:       cols,
			}
			searchWg.Add(1)
			searchCh <- searchReq
		}
	}()

	wg2.Wait()
	searchWg.Wait()
	cancel()
	wg.Wait()
	tbl, _ := inst.Store.DataTables.WeakRefTable(tid)
	root := tbl.WeakRefRoot()
	testutils.WaitExpect(300, func() bool {
		return int64(1) == root.RefCount()
	})

	assert.Equal(t, int64(1), root.RefCount())
	opts := &dbi.GetSnapshotCtx{
		ShardId:   shardId,
		DBName:    database.Name,
		TableName: schema.Name,
		Cols:      cols,
		ScanAll:   true,
	}
	now := time.Now()
	ss, err := inst.GetSnapshot(opts)
	assert.Nil(t, err)
	segIt := ss.NewIt()
	segCnt := 0
	tblkCnt := 0
	for segIt.Valid() {
		segCnt++
		h := segIt.GetHandle()
		blkIt := h.NewIt()
		for blkIt.Valid() {
			tblkCnt++
			blkHandle := blkIt.GetHandle()
			// col0 := blkHandle.GetColumn(0)
			// ctx := index.NewFilterCtx(index.OpEq)
			// ctx.Val = int32(0 + col0.GetColIdx()*100)
			// err = col0.EvalFilter(ctx)
			// assert.Nil(t, err)
			// if col0.GetBlockType() > base.PERSISTENT_BLK {
			// 	assert.False(t, ctx.BoolRes)
			// }
			// ctx.Reset()
			// ctx.Code = index.OpEq
			// ctx.Val = int32(1 + col0.GetColIdx()*100)
			// err = col0.EvalFilter(ctx)
			// assert.Nil(t, err)
			// if col0.GetBlockType() > base.PERSISTENT_BLK {
			// 	assert.True(t, ctx.BoolRes)
			// }
			hh := blkHandle.Prefetch()
			vec0, err := hh.GetReaderByAttr(1)
			assert.Nil(t, err)
			val, err := vec0.GetValue(22)
			assert.Nil(t, err)
			t.Logf("vec0[22]=%s, type=%d", val, vec0.GetType())
			hh.Close()
			// blkHandle.Close()
			blkIt.Next()
		}
		blkIt.Close()
		// h.Close()
		segIt.Next()
	}
	segIt.Close()
	ss.Close()
	assert.Equal(t, insertCnt*int(blkCnt), tblkCnt)
	assert.Equal(t, insertCnt, segCnt)
	assert.Equal(t, int64(1), root.RefCount())

	t.Logf("Takes %v", time.Since(now))
	t.Log(tbl.String())
	time.Sleep(time.Duration(100) * time.Millisecond)
	if invariants.RaceEnabled {
		time.Sleep(time.Duration(400) * time.Millisecond)
	}

	t.Log(inst.MTBufMgr.String())
	t.Log(inst.SSTBufMgr.String())
	t.Log(inst.MemTableMgr.String())
	// t.Log(inst.IndexBufMgr.String())
	// t.Log(tbl.GetIndexHolder().String())
	// t.Log(common.GPool.String())
	inst.Close()
}

func TestMultiTables(t *testing.T) {
	initDBTest()
	inst, gen, database := initDB2(wal.HolderRole, "db1", uint64(100))
	prefix := "mtable"
	tblCnt := 8
	var names []string
	for i := 0; i < tblCnt; i++ {
		name := fmt.Sprintf("%s_%d", prefix, i)
		schema := metadata.MockSchema(2)
		schema.Name = name
		_, err := inst.CreateTable(database.Name, schema, gen.Next(database.GetShardId()))
		assert.Nil(t, err)
		names = append(names, name)
	}
	tblMeta := database.SimpleGetTableByName(names[0])
	assert.NotNil(t, tblMeta)
	rows := uint64(tblMeta.Database.Catalog.Cfg.BlockMaxRows / 2)
	baseCk := mock.MockBatch(tblMeta.Schema.Types(), rows)
	p1, _ := ants.NewPool(4)
	p2, _ := ants.NewPool(4)
	attrs := []string{}
	for _, colDef := range tblMeta.Schema.ColDefs {
		attrs = append(attrs, colDef.Name)
	}
	refs := make([]uint64, len(attrs))

	tnames := database.SimpleGetTableNames()
	assert.Equal(t, len(names), len(tnames))
	insertCnt := 7
	searchCnt := 100
	var wg sync.WaitGroup
	for i := 0; i < insertCnt; i++ {
		for _, name := range tnames {
			task := func(opIdx uint64, tname string) func() {
				return func() {
					defer wg.Done()
					inst.Append(dbi.AppendCtx{
						ShardId:   database.GetShardId(),
						DBName:    database.Name,
						TableName: tname,
						OpIndex:   opIdx,
						OpSize:    1,
						Data:      baseCk,
					})
				}
			}
			wg.Add(1)
			p1.Submit(task(uint64(i), name))
		}
	}

	doneCB := func() {
		wg.Done()
	}
	for i := 0; i < searchCnt; i++ {
		for _, name := range tnames {
			task := func(opIdx uint64, tname string, donecb func()) func() {
				return func() {
					if donecb != nil {
						defer donecb()
					}
					rel, err := inst.Relation(database.Name, tname)
					assert.Nil(t, err)
					for _, segId := range rel.SegmentIds().Ids {
						seg := rel.Segment(segId, nil)
						for _, id := range seg.Blocks() {
							blk := seg.Block(id, nil)
							cds := make([]*bytes.Buffer, len(attrs))
							dds := make([]*bytes.Buffer, len(attrs))
							for i := range cds {
								cds[i] = bytes.NewBuffer(make([]byte, 0))
								dds[i] = bytes.NewBuffer(make([]byte, 0))
							}
							bat, err := blk.Read(refs, attrs, cds, dds)
							//{
							//	for i, attr := range bat.Attrs {
							//		if bat.Vecs[i], err = bat.Is[i].R.Read(bat.Is[i].Len, bat.Is[i].Ref, attr, proc); err != nil {
							//			log.Fatal(err)
							//		}
							//	}
							//}
							assert.Nil(t, err)
							for attri, attr := range attrs {
								v := bat.GetVector(attr)
								if attri == 0 && v.Length() > 5000 {
									// edata := baseCk.Vecs[attri].Col.([]int32)

									// odata := v.Col.([]int32)

									// assert.Equal(t, edata[4999], data[4999])
									// assert.Equal(t, edata[5000], data[5000])

									// t.Logf("data[4998]=%d", data[4998])
									// t.Logf("data[4999]=%d", data[4999])

									// t.Logf("data[5000]=%d", data[5000])
									// t.Logf("data[5001]=%d", data[5001])
								}
								assert.True(t, v.Length() <= int(tblMeta.Database.Catalog.Cfg.BlockMaxRows))
								// t.Logf("%s, seg=%v, blk=%v, attr=%s, len=%d", tname, segId, id, attr, v.Length())
							}
						}
					}
					rel.Close()
				}
			}
			wg.Add(1)
			p2.Submit(task(uint64(i), name, doneCB))
		}
	}
	wg.Wait()
	time.Sleep(time.Duration(100) * time.Millisecond)
	{
		for _, name := range names {
			rel, err := inst.Relation(database.Name, name)
			assert.Nil(t, err)
			sids := rel.SegmentIds().Ids
			segId := sids[len(sids)-1]
			seg := rel.Segment(segId, nil)
			blks := seg.Blocks()
			blk := seg.Block(blks[len(blks)-1], nil)
			cds := make([]*bytes.Buffer, len(attrs))
			dds := make([]*bytes.Buffer, len(attrs))
			for i := range cds {
				cds[i] = bytes.NewBuffer(make([]byte, 0))
				dds[i] = bytes.NewBuffer(make([]byte, 0))
			}
			bat, err := blk.Read(refs, attrs, cds, dds)
			//{
			//	for i, attr := range bat.Attrs {
			//		if bat.Vecs[i], err = bat.Is[i].R.Read(bat.Is[i].Len, bat.Is[i].Ref, attr, proc); err != nil {
			//			log.Fatal(err)
			//		}
			//	}
			//}
			assert.Nil(t, err)
			for _, attr := range attrs {
				v := bat.GetVector(attr)
				assert.Equal(t, int(rows), v.Length())
				// t.Log(v.Length())
				// t.Logf("%s, seg=%v, attr=%s, len=%d", name, segId, attr, v.Length())
			}
		}
	}
	t.Log(inst.MTBufMgr.String())
	t.Log(inst.SSTBufMgr.String())
	inst.Close()
}

func TestDropTable2(t *testing.T) {
	initDBTest()
	inst, gen, database := initDB2(wal.HolderRole, "db1", uint64(100))
	schema := metadata.MockSchema(2)
	schema.Name = "mockt"
	tid, err := inst.CreateTable(database.Name, schema, gen.Next(database.GetShardId()))
	assert.Nil(t, err)
	blkCnt := inst.Store.Catalog.Cfg.SegmentMaxBlocks
	rows := inst.Store.Catalog.Cfg.BlockMaxRows * blkCnt
	tblMeta := database.SimpleGetTable(tid)
	assert.NotNil(t, tblMeta)
	baseCk := mock.MockBatch(tblMeta.Schema.Types(), rows)

	insertCnt := uint64(1)

	var wg sync.WaitGroup
	{
		for i := uint64(0); i < insertCnt; i++ {
			wg.Add(1)
			go func() {
				inst.Append(dbi.AppendCtx{
					ShardId:   database.GetShardId(),
					DBName:    database.Name,
					TableName: schema.Name,
					Data:      baseCk,
					OpIndex:   gen.Alloc(database.GetShardId()),
					OpSize:    1,
				})
				wg.Done()
			}()
		}
	}
	wg.Wait()
	testutils.WaitExpect(100, func() bool {
		return int(blkCnt*insertCnt) == inst.SSTBufMgr.NodeCount()+inst.MTBufMgr.NodeCount()
	})

	t.Log(inst.MTBufMgr.String())
	t.Log(inst.SSTBufMgr.String())
	assert.Equal(t, int(blkCnt*insertCnt), inst.SSTBufMgr.NodeCount()+inst.MTBufMgr.NodeCount())
	cols := make([]int, 0)
	for i := 0; i < len(tblMeta.Schema.ColDefs); i++ {
		cols = append(cols, i)
	}
	opts := &dbi.GetSnapshotCtx{
		DBName:    database.Name,
		TableName: schema.Name,
		Cols:      cols,
		ScanAll:   true,
	}
	ss, err := inst.GetSnapshot(opts)
	assert.Nil(t, err)

	doneCh := make(chan error)
	expectErr := errors.New("mock error")
	dropCB := func(err error) {
		doneCh <- expectErr
	}

	assert.True(t, database.GetSize() > 0)

	inst.DropTable(dbi.DropTableCtx{ShardId: database.GetShardId(), DBName: database.Name, TableName: schema.Name,
		OnFinishCB: dropCB, OpIndex: gen.Alloc(database.GetShardId())})

	testutils.WaitExpect(50, func() bool {
		return int(blkCnt*insertCnt) == inst.SSTBufMgr.NodeCount()+inst.MTBufMgr.NodeCount()
	})
	assert.Equal(t, int(blkCnt*insertCnt), inst.SSTBufMgr.NodeCount()+inst.MTBufMgr.NodeCount())
	ss.Close()

	testutils.WaitExpect(50, func() bool {
		return inst.SSTBufMgr.NodeCount()+inst.MTBufMgr.NodeCount() == 0
	})
	t.Log(inst.MTBufMgr.String())
	t.Log(inst.SSTBufMgr.String())
	t.Log(inst.IndexBufMgr.String())
	t.Log(inst.MemTableMgr.String())
	assert.Equal(t, 0, inst.SSTBufMgr.NodeCount()+inst.MTBufMgr.NodeCount())
	err = <-doneCh
	assert.Equal(t, expectErr, err)
	assert.Equal(t, int64(0), database.GetSize())
	inst.Close()
}

func TestE2E(t *testing.T) {
	if !dataio.FlushIndex {
		dataio.FlushIndex = true
		defer func() {
			dataio.FlushIndex = false
		}()
	}
	waitTime := time.Duration(100) * time.Millisecond
	if invariants.RaceEnabled {
		waitTime *= 2
	}
	initDBTest()
	inst, gen, database := initDB2(wal.HolderRole, "db1", uint64(100))
	schema := metadata.MockSchema(2)
	schema.Name = "mockcon"
	tid, err := inst.CreateTable(database.Name, schema, gen.Next(database.GetShardId()))
	assert.Nil(t, err)
	tblMeta := database.SimpleGetTable(tid)
	assert.NotNil(t, tblMeta)
	blkCnt := inst.Store.Catalog.Cfg.SegmentMaxBlocks
	rows := inst.Store.Catalog.Cfg.BlockMaxRows * blkCnt
	baseCk := mock.MockBatch(tblMeta.Schema.Types(), rows)

	insertCnt := uint64(10)

	var wg sync.WaitGroup
	{
		for i := uint64(0); i < insertCnt; i++ {
			wg.Add(1)
			go func() {
				inst.Append(dbi.AppendCtx{
					ShardId:   database.GetShardId(),
					DBName:    database.Name,
					TableName: schema.Name,
					Data:      baseCk,
					OpIndex:   common.NextGlobalSeqNum(),
					OpSize:    1,
				})
				wg.Done()
			}()
		}
	}
	wg.Wait()
	time.Sleep(waitTime)
	tblData, err := inst.Store.DataTables.WeakRefTable(tid)
	assert.Nil(t, err)
	t.Log(tblData.String())
	// t.Log(tblData.GetIndexHolder().String())

	segs := tblData.SegmentIds()
	for _, segId := range segs {
		seg := tblData.WeakRefSegment(segId)
		seg.GetIndexHolder().Init(seg.GetSegmentFile())
		//t.Log(seg.GetIndexHolder().Inited)
		segment := &Segment{
			Data: seg,
			Ids:  new(atomic.Value),
		}
		spf := segment.NewSparseFilter()
		f := segment.NewFilter()
		sumr := segment.NewSummarizer()
		t.Log(spf.Eq("mock_0", int32(1)))
		t.Log(f == nil)
		t.Log(sumr.Count("mock_0", nil))
		//t.Log(spf.Eq("mock_0", int32(1)))
		//t.Log(f.Eq("mock_0", int32(-1)))
		//t.Log(sumr.Count("mock_0", nil))
		t.Log(inst.IndexBufMgr.String())
	}

	time.Sleep(waitTime)
	t.Log(inst.IndexBufMgr.String())

	_, err = inst.DropTable(dbi.DropTableCtx{ShardId: database.GetShardId(), DBName: database.Name, TableName: schema.Name, OpIndex: gen.Alloc(database.GetShardId())})
	assert.Nil(t, err)
	time.Sleep(waitTime / 2)

	t.Log(inst.FsMgr.String())
	t.Log(inst.MTBufMgr.String())
	t.Log(inst.SSTBufMgr.String())
	t.Log(inst.IndexBufMgr.String())
	inst.Close()
}

func TestCreateSnapshot(t *testing.T) {
	initDBTest()
	inst, gen := initDB(wal.BrokerRole)
	//inst, gen, database := initDB2(wal.BrokerRole, "db1", uint64(0))
	schema1 := metadata.MockSchema(20)
	schema1.Name = "t1"
	schema2 := metadata.MockSchema(10)
	schema2.Name = "t2"
	db1, err := inst.CreateDatabase("0", uint64(0))
	assert.Nil(t, err)
	db2, err := inst.CreateDatabase("1", uint64(1))
	assert.Nil(t, err)
	tid1, err := inst.CreateTable(db1.Name, schema1, gen.Next(db1.GetShardId()))
	assert.Nil(t, err)
	tid2, err := inst.CreateTable(db2.Name, schema2, gen.Next(db2.GetShardId()))
	assert.Nil(t, err)
	tbl1 := db1.SimpleGetTable(tid1)
	if tbl1 == nil {
		t.Error("table not found")
	}
	tbl2 := db2.SimpleGetTable(tid2)
	if tbl2 == nil {
		t.Error("table not found")
	}
	insertCnt := uint64(20)
	tableName1 := tbl1.Schema.Name
	tableName2 := tbl2.Schema.Name
	rowCnt := uint64(2000)
	bat1 := mock.MockBatch(tbl1.Schema.Types(), rowCnt)
	bat2 := mock.MockBatch(tbl2.Schema.Types(), rowCnt)

	assert.Nil(t, os.RemoveAll("/tmp/test_ss/s0"))
	assert.Nil(t, os.RemoveAll("/tmp/test_ss/s1"))

	for i := uint64(0); i < insertCnt; i++ {
		if err := inst.Append(dbi.AppendCtx{ShardId: db1.GetShardId(), TableName: tableName1, DBName: db1.Name, Data: bat1, OpIndex: gen.Alloc(db1.GetShardId()), OpSize: 1}); err != nil {
			t.Error(err)
		}
		pid, err := inst.CreateSnapshot(db1.Name, fmt.Sprintf("/tmp/test_ss/s0/ss-%d", i))
		if err != nil {
			t.Error(err)
		}
		t.Logf("Shard[%d] PersistentLogIndex: %d", 0, pid)

		if err := inst.Append(dbi.AppendCtx{DBName: db2.Name, ShardId: db2.GetShardId(), TableName: tableName2, Data: bat2, OpIndex: gen.Alloc(db2.GetShardId()), OpSize: 1}); err != nil {
			t.Error(err)
		}
		pid, err = inst.CreateSnapshot(db2.Name, fmt.Sprintf("/tmp/test_ss/s1/ss-%d", i))
		if err != nil {
			t.Error(err)
		}
		t.Logf("Shard[%d] PersistentLogIndex: %d", 1, pid)
	}

	time.Sleep(200 * time.Millisecond)

	st := time.Now()
	pid, err := inst.CreateSnapshot(db1.Name, fmt.Sprintf("/tmp/test_ss/s0/ss-final"))
	assert.Nil(t, err)
	t.Log(time.Since(st).Milliseconds(), " ms")
	assert.Equal(t, pid, inst.GetShardCheckpointId(db1.GetShardId()))

	files, err := ioutil.ReadDir("/tmp/test_ss/s0/ss-final")
	assert.Nil(t, err)
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".meta") {
			continue
		}
		reader := metadata.NewDBSSLoader(inst.Store.Catalog, filepath.Join("/tmp/test_ss/s0/ss-final", file.Name()))
		assert.Nil(t, reader.PrepareLoad())
		//t.Log(reader.View().Range)
		//t.Log(reader.View().LogRange)
		//t.Log(reader.View().Catalog.PString(2))
		// TODO
		// assert.Equal(t, uint64(19), reader.View().LogRange.Range.Right)
		// assert.Equal(t, uint64(0), reader.View().LogRange.Range.Left)
		// TODO
		// tbl1 := reader.View().Database.TableSet[uint64(1)]
		// assert.Equal(t, uint64(3), tbl1.Id)
		// for i, seg := range tbl1.SegmentSet {
		// 	assert.Equal(t, uint64(i+21), seg.Id)
		// }
	}

	assert.Nil(t, inst.ApplySnapshot(db1.Name, "/tmp/test_ss/s0/ss-15"))
	t.Log(inst.GetShardCheckpointId(db1.GetShardId())) // TODO: should be reset automatically
	db1, err = inst.Store.Catalog.SimpleGetDatabaseByName(db1.Name)
	assert.Nil(t, err)

	data, err := inst.getTableData(db1.TableSet[uint64(4)])
	assert.Nil(t, err)
	t.Log(data.GetSegmentCount())

	//for _, seg := range data.GetMeta().SegmentSet {
	//	t.Log("Seg ", seg.Id, "\n")
	//	for _, blk := range seg.BlockSet {
	//		t.Log(" Blk ", blk.Id)
	//	}
	//}

	seg31 := data.StrongRefSegment(uint64(31))
	assert.NotNil(t, seg31)
	blk60 := seg31.WeakRefBlock(uint64(60))
	assert.NotNil(t, blk60)
	t.Log(blk60.Size("mock_9"))
	t.Log(blk60.GetRowCount())

	t.Log(db1.TableSet[uint64(4)].PString(2))

	assert.Nil(t, inst.Close())

	//seg13 := data.StrongRefSegment(uint64(13))
	//assert.NotNil(t, seg13)
	//t.Log(seg13.Size("mock_9"))
	//t.Log(seg13.BlockIds())
	//assert.NotNil(t, seg13.WeakRefBlock(uint64(0)))

	//inst.Close()
	//
	//inst = initDB(wal.BrokerRole)
	//t.Log(inst.GetShardCheckpointId(uint64(0)))

	//t.Log(inst.Wal.String())
	//t.Log(inst.Store.Catalog.PString(2))
	//t.Log(inst.GetShardCheckpointId(uint64(0)))
	//
	//for i, tbl := range inst.Store.DataTables.Data {
	//	t.Log(i, ": ", tbl.String())
	//}
	//for i, tbl := range inst.Store.Catalog.TableSet {
	//	t.Log(i, "  ", tbl.PString(2))
	//}
	//tids, err := inst.TableIDs()
	//assert.Nil(t, err)
	//tnames := inst.TableNames()
	//t.Log(tids)
	//t.Log(tnames)
	//tbl1 = inst.Store.Catalog.SimpleGetTableByName("tbl_1")
	//t.Log(tbl1.PString(2))
	//t.Log(tbl1.GetReplayIndex().String())
}

// Snapshot View: SortedSeg_1<[Block_1][Block_2]>,UnsortedSeg_2<[Block_3]>
//                1_1.seg  1_2_1.blk
// Crete View: SortedSeg_1<[Block_1][Block_2]>,SortedSeg_2<[Block_3][Block_4]>
//                1_1.seg  1_2.seg
// May triggers this case and aoe would retry automatically
func TestCreateSnapshotCase1(t *testing.T) {
	initDBTest()
	inst, gen, database := initDB2(wal.BrokerRole, "0", uint64(0))
	schema1 := metadata.MockSchema(20)
	schema1.Name = "tbl_1"
	shardId := database.GetShardId()
	tid1, err := inst.CreateTable(database.Name, schema1, gen.Next(shardId))
	assert.Nil(t, err)
	tbl1 := database.SimpleGetTable(tid1)
	if tbl1 == nil {
		t.Error("table not found")
	}
	insertCnt := uint64(20)
	tableName1 := tbl1.Schema.Name
	rowCnt := uint64(2000)
	bat1 := mock.MockBatch(tbl1.Schema.Types(), rowCnt)
	assert.Nil(t, os.RemoveAll("/tmp/test_ss/case_1/ss-1"))

	for i := 0; i < int(insertCnt)/2; i++ {
		if err := inst.Append(dbi.AppendCtx{ShardId: shardId, TableName: tableName1, DBName: database.Name, Data: bat1, OpIndex: gen.Alloc(shardId), OpSize: 1}); err != nil {
			t.Error(err)
		}
	}

	var newId uint64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		newId, err = inst.CreateSnapshot(database.Name, "/tmp/test_ss/case_1/ss-1")
		if err != nil {
			t.Error(err)
		}
		t.Logf("Snapshot created at %d", newId)
		wg.Done()
	}()

	for i := int(insertCnt) / 2; i < int(insertCnt); i++ {
		if err := inst.Append(dbi.AppendCtx{ShardId: shardId, DBName: database.Name, TableName: tableName1, Data: bat1, OpIndex: gen.Alloc(shardId), OpSize: 1}); err != nil {
			t.Error(err)
		}
	}
	wg.Wait()
	assert.Nil(t, inst.Close())

	initDBTest()
	inst, _ = initDB(wal.BrokerRole)

	// TODO: now onCreateTable is not implemented yet, remove later
	inst.CreateDatabase("0", uint64(0))

	assert.Nil(t, inst.ApplySnapshot(database.Name, "/tmp/test_ss/case_1/ss-1"))

	t.Log(inst.Store.Catalog.PString(2))
	t.Log(inst.Store.DataTables.String())
	//for sid, db := range inst.Store.Catalog.Databases {
	//	t.Logf("Shard[%d] => DB[%s]", sid, db.Name)
	//}
	t.Log(inst.Store.Catalog.SimpleGetDatabaseNames())
	db, err := inst.Store.Catalog.SimpleGetDatabaseByName("0")
	assert.Nil(t, err)
	t.Log(db.Id)

	inst.Wal.InitShard(uint64(0), newId)
	t.Log(inst.GetShardCheckpointId(uint64(0)))

	for i := int(newId); i < int(insertCnt); i++ {
		if err := inst.Append(dbi.AppendCtx{ShardId: shardId, TableName: tableName1, DBName: database.Name, Data: bat1, OpIndex: gen.Alloc(shardId), OpSize: 1}); err != nil {
			t.Error(err)
		}
	}

	t.Log(inst.Store.Catalog.PString(2))
	t.Log(inst.Store.DataTables.String())

	assert.Nil(t, inst.Close())
}

func TestCreateSnapshotCase2(t *testing.T) {
	sched.DisableFlushSegment = true
	initDBTest()
	inst, gen, database := initDB2(wal.BrokerRole, "0", uint64(0))
	//inst.Opts.Meta.Conf.BlockMaxRows = 1
	//inst.Opts.Meta.Conf.SegmentMaxBlocks = 1
	schema1 := metadata.MockSchema(20)
	schema1.Name = "tbl_1"
	shardId := database.GetShardId()
	tid1, err := inst.CreateTable(database.Name, schema1, gen.Next(shardId))
	assert.Nil(t, err)
	tbl1 := database.SimpleGetTable(tid1)
	if tbl1 == nil {
		t.Error("table not found")
	}
	insertCnt := uint64(20)
	tableName1 := tbl1.Schema.Name
	rowCnt := uint64(2000)
	bat1 := mock.MockBatch(tbl1.Schema.Types(), rowCnt)
	assert.Nil(t, os.RemoveAll("/tmp/test_ss/case_2/ss-1"))

	for i := 0; i < int(insertCnt)/2; i++ {
		if err := inst.Append(dbi.AppendCtx{ShardId: shardId, TableName: tableName1, DBName: database.Name, Data: bat1, OpIndex: gen.Alloc(shardId), OpSize: 1}); err != nil {
			t.Error(err)
		}
	}

	var newId uint64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		newId, err = inst.CreateSnapshot(database.Name, "/tmp/test_ss/case_2/ss-1")
		if err != nil {
			t.Error(err)
		}
		t.Logf("Snapshot created at %d", newId)
		wg.Done()
	}()

	for i := int(insertCnt) / 2; i < int(insertCnt); i++ {
		if err := inst.Append(dbi.AppendCtx{ShardId: shardId, DBName: database.Name, TableName: tableName1, Data: bat1, OpIndex: gen.Alloc(shardId), OpSize: 1}); err != nil {
			t.Error(err)
		}
	}
	wg.Wait()
	t.Log(inst.Store.Catalog.PString(1))
	assert.Nil(t, inst.Close())

	initDBTest()
	inst, _ = initDB(wal.BrokerRole)

	sched.DisableFlushSegment = false

	// TODO: now onCreateTable is not implemented yet, remove later
	inst.CreateDatabase("0", uint64(0))

	assert.Nil(t, inst.ApplySnapshot(database.Name, "/tmp/test_ss/case_2/ss-1"))

	time.Sleep(100*time.Millisecond)

	t.Log(inst.Store.Catalog.PString(2))
	t.Log(inst.Store.DataTables.String())
	//for sid, db := range inst.Store.Catalog.Databases {
	//	t.Logf("Shard[%d] => DB[%s]", sid, db.Name)
	//}
	t.Log(inst.Store.Catalog.SimpleGetDatabaseNames())
	db, err := inst.Store.Catalog.SimpleGetDatabaseByName("0")
	assert.Nil(t, err)
	t.Log(db.Id)

	inst.Wal.InitShard(uint64(0), newId)
	t.Log(inst.GetShardCheckpointId(uint64(0)))

	for i := int(newId); i < int(insertCnt); i++ {
		if err := inst.Append(dbi.AppendCtx{ShardId: shardId, TableName: tableName1, DBName: database.Name, Data: bat1, OpIndex: gen.Alloc(shardId), OpSize: 1}); err != nil {
			t.Error(err)
		}
	}

	t.Log(inst.Store.Catalog.PString(2))
	t.Log(inst.Store.DataTables.String())

	assert.Nil(t, inst.Close())
}
