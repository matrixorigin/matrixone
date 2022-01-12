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

package aoedb

import (
	"bytes"
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/base"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/internal/invariants"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

func TestCreateTable(t *testing.T) {
	initTestEnv(t)
	inst, gen, _ := initTestDB2(t)
	assert.NotNil(t, inst)
	defer inst.Close()
	tblCnt := rand.Intn(5) + 3
	var wg sync.WaitGroup
	names := make([]string, 0)

	for i := 0; i < tblCnt; i++ {
		schema := metadata.MockSchema(2)
		names = append(names, schema.Name)
		wg.Add(1)
		go func(w *sync.WaitGroup, id uint64) {
			ctx := &CreateDBCtx{
				DB: strconv.FormatUint(id, 10),
			}
			database, err := inst.CreateDatabase(ctx)
			assert.Nil(t, err)

			createCtx := &CreateTableCtx{
				DBMutationCtx: *CreateDBMutationCtx(database, gen),
				Schema:        schema,
			}
			_, err = inst.CreateTable(createCtx)
			assert.Nil(t, err)
			w.Done()
		}(&wg, uint64(i)+1)
	}
	wg.Wait()
	dbNames := inst.Store.Catalog.SimpleGetDatabaseNames()
	assert.Equal(t, tblCnt, len(dbNames))
	t.Log(inst.Store.Catalog.PString(metadata.PPL0, 0))
}

func TestCreateDuplicateTable(t *testing.T) {
	initTestEnv(t)
	inst, gen, database := initTestDB1(t)
	defer inst.Close()

	schema := metadata.MockSchema(2)
	createCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema,
	}
	_, err := inst.CreateTable(createCtx)
	assert.Nil(t, err)
	createCtx.Id = gen.Alloc(database.GetShardId())
	_, err = inst.CreateTable(createCtx)
	assert.Equal(t, metadata.DuplicateErr, err)
}

func TestDropEmptyTable(t *testing.T) {
	initTestEnv(t)
	inst, gen, database := initTestDB1(t)
	defer inst.Close()

	schema := metadata.MockSchema(2)
	createCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema,
	}
	_, err := inst.CreateTable(createCtx)
	assert.Nil(t, err)

	dropCtx := CreateTableMutationCtx(database, gen, schema.Name)
	_, err = inst.DropTable(dropCtx)
	assert.Nil(t, err)
}

func TestDropTable(t *testing.T) {
	initTestEnv(t)
	inst, gen, database := initTestDB1(t)
	defer inst.Close()

	name := "t1"
	schema := metadata.MockSchema(2)
	schema.Name = name

	createCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema,
	}
	createMeta, err := inst.CreateTable(createCtx)
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

	dropCtx := CreateTableMutationCtx(database, gen, schema.Name)
	dropMeta, err := inst.DropTable(dropCtx)
	assert.Nil(t, err)
	assert.Equal(t, createMeta.Id, dropMeta.Id)

	ss, err = inst.GetSnapshot(ssCtx)
	assert.NotNil(t, err)
	assert.Nil(t, ss)

	createCtx.Id = gen.Alloc(database.GetShardId())
	createMeta2, err := inst.CreateTable(createCtx)
	assert.Nil(t, err)
	assert.NotEqual(t, createMeta.Id, createMeta2.Id)

	ss, err = inst.GetSnapshot(ssCtx)
	assert.Nil(t, err)
	ss.Close()
	t.Log(inst.Wal.String())
}

func TestSSOnMutation(t *testing.T) {
	initTestEnv(t)
	inst, gen, database := initTestDB1(t)
	defer inst.Close()
	schema := metadata.MockSchema(2)
	createCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema,
	}
	_, err := inst.CreateTable(createCtx)
	assert.Nil(t, err)
	rows := inst.Store.Catalog.Cfg.BlockMaxRows / 10
	ck := mock.MockBatch(schema.Types(), rows)
	appendCtx := CreateAppendCtx(database, gen, schema.Name, ck)
	err = inst.Append(appendCtx)
	assert.Nil(t, err)
	err = inst.FlushTable(database.Name, schema.Name)
	assert.Nil(t, err)
	testutils.WaitExpect(200, func() bool {
		return database.GetCheckpointId() == gen.Get(database.GetShardId())
	})
	assert.Equal(t, database.GetCheckpointId(), gen.Get(database.GetShardId()))

	idx := database.GetCheckpointId()
	appendCtx.Id = gen.Alloc(database.GetShardId())
	err = inst.Append(appendCtx)

	view := database.View(idx)
	t.Log(view.Database.PString(metadata.PPL1, 0))
}

func TestAppend(t *testing.T) {
	initTestEnv(t)
	inst, gen, database := initTestDB1(t)

	schema := metadata.MockSchema(2)
	createCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema,
	}

	tblMeta, err := inst.CreateTable(createCtx)
	assert.Nil(t, err)
	assert.NotNil(t, tblMeta)
	blkCnt := 2
	rows := inst.Store.Catalog.Cfg.BlockMaxRows * uint64(blkCnt)
	ck := mock.MockBatch(tblMeta.Schema.Types(), rows)
	assert.Equal(t, int(rows), vector.Length(ck.Vecs[0]))
	invalidName := "xxx"
	appendCtx := CreateAppendCtx(database, gen, invalidName, ck)
	err = inst.Append(appendCtx)
	assert.NotNil(t, err)
	insertCnt := 8
	appendCtx.Table = tblMeta.Schema.Name
	for i := 0; i < insertCnt; i++ {
		appendCtx.Id = gen.Alloc(database.GetShardId())
		err = inst.Append(appendCtx)
		assert.Nil(t, err)
	}

	cols := []int{0, 1}
	tbl, _ := inst.Store.DataTables.WeakRefTable(tblMeta.Id)
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

	ssPath := prepareSnapshotPath(defaultSnapshotPath, t)

	createSSCtx := &CreateSnapshotCtx{
		DB:   database.Name,
		Path: ssPath,
		Sync: true,
	}
	idx, err := inst.CreateSnapshot(createSSCtx)
	assert.Nil(t, err)
	assert.Equal(t, database.GetCheckpointId(), idx)

	applySSCtx := &ApplySnapshotCtx{
		DB:   database.Name,
		Path: ssPath,
	}
	err = inst.ApplySnapshot(applySSCtx)
	assert.Nil(t, err)
	assert.True(t, database.IsDeleted())

	database2, err := database.Catalog.SimpleGetDatabaseByName(database.Name)
	assert.Nil(t, err)

	tMeta, err := inst.Store.Catalog.SimpleGetTableByName(database.Name, schema.Name)
	assert.Nil(t, err)
	t.Log(tMeta.PString(metadata.PPL0, 0))

	err = inst.Append(appendCtx)
	assert.Nil(t, err)
	err = inst.FlushTable(database2.Name, schema.Name)
	t.Log(inst.Wal.String())
	gen.Reset(database2.GetShardId(), appendCtx.Id)

	schema2 := metadata.MockSchema(3)
	createCtx.Id = gen.Alloc(database2.GetShardId())
	createCtx.Schema = schema2
	_, err = inst.CreateTable(createCtx)
	assert.Nil(t, err)
	testutils.WaitExpect(200, func() bool {
		return database2.GetCheckpointId() == gen.Get(database2.GetShardId())
	})
	assert.Equal(t, database2.GetCheckpointId(), gen.Get(database2.GetShardId()))
	inst.Store.Catalog.Compact(nil, nil)

	// t.Log(inst.MTBufMgr.String())
	// t.Log(inst.SSTBufMgr.String())
	// t.Log(inst.IndexBufMgr.String())
	// t.Log(inst.FsMgr.String())
	// t.Log(tbl.GetIndexHolder().String())
	t.Log(inst.Wal.String())
	inst.Close()
}

func TestConcurrency(t *testing.T) {
	initTestEnv(t)
	inst, gen, database := initTestDB3(t)
	schema := metadata.MockSchema(2)

	shardId := database.GetShardId()
	createCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema,
	}
	tblMeta, err := inst.CreateTable(createCtx)
	assert.Nil(t, err)
	assert.NotNil(t, tblMeta)
	blkCnt := inst.Store.Catalog.Cfg.SegmentMaxBlocks
	rows := inst.Store.Catalog.Cfg.BlockMaxRows * blkCnt
	baseCk := mock.MockBatch(tblMeta.Schema.Types(), rows)
	insertCh := make(chan *AppendCtx)
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
			insertReq := CreateAppendCtx(database, gen, schema.Name, baseCk)
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
			tbl, _ := inst.Store.DataTables.WeakRefTable(tblMeta.Id)
			for tbl == nil {
				time.Sleep(time.Duration(100) * time.Microsecond)
				tbl, _ = inst.Store.DataTables.WeakRefTable(tblMeta.Id)
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
	tbl, _ := inst.Store.DataTables.WeakRefTable(tblMeta.Id)
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
	// t.Log(inst.IndexBufMgr.String())
	// t.Log(tbl.GetIndexHolder().String())
	// t.Log(common.GPool.String())
	inst.Close()
}

func TestMultiTables(t *testing.T) {
	initTestEnv(t)
	inst, gen, database := initTestDB3(t)
	prefix := "mtable"
	tblCnt := 8
	var names []string
	for i := 0; i < tblCnt; i++ {
		name := fmt.Sprintf("%s_%d", prefix, i)
		schema := metadata.MockSchema(2)
		schema.Name = name
		createCtx := &CreateTableCtx{
			DBMutationCtx: *CreateDBMutationCtx(database, gen),
			Schema:        schema,
		}
		_, err := inst.CreateTable(createCtx)
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
			task := func(tname string) func() {
				return func() {
					defer wg.Done()
					appendCtx := CreateAppendCtx(database, gen, tname, baseCk)
					err := inst.Append(appendCtx)
					assert.Nil(t, err)
				}
			}
			wg.Add(1)
			p1.Submit(task(name))
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
						seg := rel.Segment(segId)
						for _, id := range seg.Blocks() {
							blk := seg.Block(id)
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
								v := batch.GetVector(bat, attr)
								if attri == 0 && vector.Length(v) > 5000 {
									// edata := baseCk.Vecs[attri].Col.([]int32)

									// odata := v.Col.([]int32)

									// assert.Equal(t, edata[4999], data[4999])
									// assert.Equal(t, edata[5000], data[5000])

									// t.Logf("data[4998]=%d", data[4998])
									// t.Logf("data[4999]=%d", data[4999])

									// t.Logf("data[5000]=%d", data[5000])
									// t.Logf("data[5001]=%d", data[5001])
								}
								assert.True(t, vector.Length(v) <= int(tblMeta.Database.Catalog.Cfg.BlockMaxRows))
								// t.Logf("%s, seg=%v, blk=%v, attr=%s, len=%d", tname, segId, id, attr, vector.Length(v))
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
			seg := rel.Segment(segId)
			blks := seg.Blocks()
			blk := seg.Block(blks[len(blks)-1])
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
				v := batch.GetVector(bat, attr)
				assert.Equal(t, int(rows), vector.Length(v))
				// t.Log(vector.Length(v))
				// t.Logf("%s, seg=%v, attr=%s, len=%d", name, segId, attr, vector.Length(v))
			}
		}
	}
	t.Log(inst.MTBufMgr.String())
	t.Log(inst.SSTBufMgr.String())
	inst.Close()
}

func TestDropTable2(t *testing.T) {
	initTestEnv(t)
	inst, gen, database := initTestDB3(t)
	schema := metadata.MockSchema(2)
	createCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema,
	}
	tblMeta, err := inst.CreateTable(createCtx)
	assert.Nil(t, err)
	assert.NotNil(t, tblMeta)
	blkCnt := inst.Store.Catalog.Cfg.SegmentMaxBlocks
	rows := inst.Store.Catalog.Cfg.BlockMaxRows * blkCnt
	baseCk := mock.MockBatch(tblMeta.Schema.Types(), rows)

	insertCnt := uint64(1)

	var wg sync.WaitGroup
	{
		for i := uint64(0); i < insertCnt; i++ {
			wg.Add(1)
			go func() {
				appendCtx := CreateAppendCtx(database, gen, schema.Name, baseCk)
				inst.Append(appendCtx)
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

	assert.True(t, database.GetSize() > 0)

	dropCtx := CreateTableMutationCtx(database, gen, schema.Name)
	_, err = inst.DropTable(dropCtx)
	assert.Nil(t, err)
	dropDBCtx := &DropDBCtx{
		DB: database.Name,
		Id: gen.Alloc(database.GetShardId()),
	}
	_, err = inst.DropDatabase(dropDBCtx)
	assert.Nil(t, err)

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
	t.Log(inst.MutationBufMgr.String())
	// t.Log(inst.IndexBufMgr.String())
	// t.Log(inst.MemTableMgr.String())
	assert.Equal(t, 0, inst.SSTBufMgr.NodeCount()+inst.MTBufMgr.NodeCount())
	testutils.WaitExpect(100, func() bool {
		return database.GetSize() == int64(0)
	})
	assert.Equal(t, int64(0), database.GetSize())
	inst.Close()
}

// Test bsi file flushed with segment file when metadata contains bsi.
func TestBuildIndex(t *testing.T) {
	waitTime := time.Duration(100) * time.Millisecond
	if invariants.RaceEnabled {
		waitTime *= 2
	}
	initTestEnv(t)
	inst, gen, database := initTestDB3(t)
	schema := metadata.MockSchema(2)
	indice := metadata.NewIndexSchema()
	indice.MakeIndex("idx-1", metadata.NumBsi, 1)
	createCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema,
		Indice:        indice,
	}
	tblMeta, err := inst.CreateTable(createCtx)
	assert.Nil(t, err)
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
				appendCtx := CreateAppendCtx(database, gen, schema.Name, baseCk)
				inst.Append(appendCtx)
				wg.Done()
			}()
		}
	}
	wg.Wait()
	time.Sleep(waitTime)
	tblData, err := inst.Store.DataTables.WeakRefTable(tblMeta.Id)
	assert.Nil(t, err)
	t.Log(tblData.String())
	// t.Log(tblData.GetIndexHolder().String())

	segs := tblData.SegmentIds()
	for _, segId := range segs {
		seg := tblData.WeakRefSegment(segId)
		//seg.GetIndexHolder().Init(seg.GetSegmentFile())
		//t.Log(seg.GetIndexHolder().Inited)
		segment := &db.Segment{
			Data: seg,
			Ids:  new(atomic.Value),
		}
		spf := segment.NewSparseFilter()
		f := segment.NewFilter()
		sumr := segment.NewSummarizer()
		t.Log(spf.Eq("mock_0", int32(1)))
		t.Log(f == nil)
		t.Log(sumr.Count("mock_1", nil))
		//t.Log(spf.Eq("mock_0", int32(1)))
		//t.Log(f.Eq("mock_0", int32(-1)))
		//t.Log(sumr.Count("mock_0", nil))
		t.Log(inst.IndexBufMgr.String())
	}

	time.Sleep(waitTime)
	t.Log(inst.IndexBufMgr.String())

	dropCtx := CreateTableMutationCtx(database, gen, schema.Name)
	_, err = inst.DropTable(dropCtx)
	assert.Nil(t, err)
	time.Sleep(waitTime / 2)

	t.Log(inst.FsMgr.String())
	t.Log(inst.MTBufMgr.String())
	t.Log(inst.SSTBufMgr.String())
	t.Log(inst.IndexBufMgr.String())
	inst.Close()
}

func TestRebuildIndices(t *testing.T) {
	waitTime := time.Duration(100) * time.Millisecond
	if invariants.RaceEnabled {
		waitTime *= 2
	}
	initTestEnv(t)
	inst, gen, database := initTestDB3(t)
	schema := metadata.MockSchema(2)
	indice := metadata.NewIndexSchema()
	_, err := indice.MakeIndex("idx-1", metadata.NumBsi, 1)
	assert.Nil(t, err)
	createCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema,
		Indice:        indice,
	}
	tblMeta, err := inst.CreateTable(createCtx)
	assert.Nil(t, err)
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
				appendCtx := CreateAppendCtx(database, gen, schema.Name, baseCk)
				inst.Append(appendCtx)
				wg.Done()
			}()
		}
	}
	wg.Wait()
	time.Sleep(waitTime)
	tblData, err := inst.Store.DataTables.WeakRefTable(tblMeta.Id)
	assert.Nil(t, err)
	//t.Log(tblData.String())
	//t.Log(tblData.GetIndexHolder().String())

	segs := tblData.SegmentIds()
	for _, segId := range segs {
		seg := tblData.WeakRefSegment(segId)
		if seg.GetType() != base.SORTED_SEG {
			continue
		}
		//seg.GetIndexHolder().Init(seg.GetSegmentFile())
		//t.Log(seg.GetIndexHolder().Inited)
		segment := &db.Segment{
			Data: seg,
			Ids:  new(atomic.Value),
		}
		spf := segment.NewSparseFilter()
		f := segment.NewFilter()
		sumr := segment.NewSummarizer()
		t.Log(spf.Eq("mock_0", int32(1)))
		t.Log(f == nil)
		t.Log(sumr.Count("mock_1", nil))
		//t.Log(spf.Eq("mock_0", int32(1)))
		//t.Log(f.Eq("mock_0", int32(-1)))
		//t.Log(sumr.Count("mock_0", nil))
		t.Log(inst.IndexBufMgr.String())
		e := sched.NewFlushSegIndexEvent(&sched.Context{}, seg)
		inst.Scheduler.Schedule(e)
		time.Sleep(10 * time.Millisecond)
		t.Log(inst.IndexBufMgr.String())
	}
	time.Sleep(10 * time.Millisecond)
	t.Log(tblData.GetIndexHolder().StringIndicesRefs())
	inst.Close()
	// manually delete some index files, and during replaying the file could
	// be flushed automatically.
	os.Remove(filepath.Join(inst.Dir, "data/1_1_7_1.bsi"))

	// manually create useless index files, and during replaying the file
	// would be GCed automatically.
	os.Create(filepath.Join(inst.Dir, "data/1_1_11_1.bsi"))

	// manually rename some index files to higher version, create a stale version
	// to simulate upgrade scenario. And during replaying, stale index file woould
	// be GCed as well.
	os.Rename(filepath.Join(inst.Dir, "data/1_1_6_1.bsi"), filepath.Join(inst.Dir, "data/2_1_6_1.bsi"))
	os.Create(filepath.Join(inst.Dir, "data/1_1_6_1.bsi"))

	inst, gen, database = initTestDB3(t)
	tblData, err = inst.Store.DataTables.WeakRefTable(tblMeta.Id)
	assert.Nil(t, err)
	//t.Log(tblData.String())
	//t.Log(tblData.GetIndexHolder().String())
	time.Sleep(100 * time.Millisecond)
	// for _, segId := range segs {
	// 	seg := tblData.StrongRefSegment(segId)
	// 	t.Log(seg.GetIndexHolder().StringIndicesRefsNoLock())
	// }
	inst.Close()
}

func TestManyLoadAndDrop(t *testing.T) {
	waitTime := time.Duration(100) * time.Millisecond
	if invariants.RaceEnabled {
		waitTime *= 2
	}
	initTestEnv(t)
	inst, gen, database := initTestDB3(t)
	schema := metadata.MockSchema(2)
	indice := metadata.NewIndexSchema()
	_, err := indice.MakeIndex("idx-1", metadata.NumBsi, 1)
	assert.Nil(t, err)
	createCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema,
		Indice:        indice,
	}
	tblMeta, err := inst.CreateTable(createCtx)
	assert.Nil(t, err)
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
				appendCtx := CreateAppendCtx(database, gen, schema.Name, baseCk)
				inst.Append(appendCtx)
				wg.Done()
			}()
		}
	}
	wg.Wait()
	time.Sleep(waitTime)
	tblData, err := inst.Store.DataTables.WeakRefTable(tblMeta.Id)
	assert.Nil(t, err)

	segId := tblData.SegmentIds()[0]
	seg := tblData.StrongRefSegment(segId)
	holder := seg.GetIndexHolder()
	f1 := func() {
		holder.Count(1, nil)
		wg.Done()
	}
	f2 := func() {
		holder.Min(1, nil)
		wg.Done()
	}
	f3 := func() {
		holder.Max(1, nil)
		wg.Done()
	}
	cnt := 100
	for i := 0; i < cnt; i++ {
		wg.Add(3)
		go f1()
		go f2()
		go f3()
	}
	// dropped explicitly, but some indices are still in use, so when those
	// query finished, they would unref index and once the ref count reaches
	// 0 the index file would be removed.
	holder.DropIndex(filepath.Join(inst.Dir, "data/1_1_1_1.bsi"))
	wg.Wait()

	// test continuously load new index, check if stale versions are GCed correctly
	segId = tblData.SegmentIds()[1]
	seg = tblData.StrongRefSegment(segId)
	holder = seg.GetIndexHolder()

	loader := func(i uint64) {
		e := sched.NewFlushSegIndexEvent(&sched.Context{}, seg)
		e.Cols = []uint16{1}
		inst.Scheduler.Schedule(e)
		wg.Done()
	}
	for i := 1; i < 10; i++ {
		wg.Add(1)
		go loader(uint64(i))
	}
	wg.Wait()

	time.Sleep(300 * time.Millisecond)

	infos, err := ioutil.ReadDir(filepath.Join(inst.Dir, "data"))
	assert.Nil(t, err)
	versions := make([]uint64, 0)
	for _, info := range infos {
		if name, ok := common.ParseBitSlicedIndexFileName(filepath.Base(info.Name())); ok {
			if v, _, sid, _, ok := common.ParseBitSlicedIndexFileNameToInfo(name); ok {
				if sid == uint64(2) {
					versions = append(versions, v)
				}
			}
		}
	}
	t.Log(versions)
	assert.Equal(t, 1, len(versions))
	assert.Equal(t, uint64(10), versions[0])

	// test continuous load and dropping, speculative dropping could be
	// handled correctly.
	segId = tblData.SegmentIds()[2]
	seg = tblData.StrongRefSegment(segId)
	holder = seg.GetIndexHolder()

	loader = func(i uint64) {
		e := sched.NewFlushSegIndexEvent(&sched.Context{}, seg)
		e.Cols = []uint16{1}
		inst.Scheduler.Schedule(e)
		wg.Done()
	}

	dropper := func(i uint64) {
		filename := filepath.Join(inst.Dir, fmt.Sprintf("data/%d_1_3_1.bsi", i))
		holder.DropIndex(filename)
		wg.Done()
	}
	for i := uint64(1); i < uint64(10); i++ {
		wg.Add(1)
		go loader(i)
		if i%4 == 0 {
			wg.Add(1)
			go dropper(i)
		}
	}
	wg.Wait()
	inst.Close()
}

func TestEngine(t *testing.T) {
	initTestEnv(t)
	inst, gen, database := initTestDB3(t)
	schema := metadata.MockSchema(2)
	shardId := database.GetShardId()
	createCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema,
	}
	tblMeta, err := inst.CreateTable(createCtx)
	assert.Nil(t, err)
	assert.NotNil(t, tblMeta)
	blkCnt := inst.Store.Catalog.Cfg.SegmentMaxBlocks
	rows := inst.Store.Catalog.Cfg.BlockMaxRows * blkCnt
	baseCk := mock.MockBatch(tblMeta.Schema.Types(), rows)
	insertCh := make(chan *AppendCtx)
	searchCh := make(chan *dbi.GetSnapshotCtx)

	p, _ := ants.NewPool(40)
	attrs := []string{}
	cols := make([]int, 0)
	for i, colDef := range tblMeta.Schema.ColDefs {
		attrs = append(attrs, colDef.Name)
		cols = append(cols, i)
	}

	tableCnt := 20
	var twg sync.WaitGroup
	for i := 0; i < tableCnt; i++ {
		twg.Add(1)
		f := func(idx int) func() {
			return func() {
				schema := metadata.MockSchema(2)
				schema.Name = fmt.Sprintf("%s-%d", schema.Name, idx)
				createCtx := &CreateTableCtx{
					DBMutationCtx: *CreateDBMutationCtx(database, gen),
					Schema:        schema,
				}
				_, err := inst.CreateTable(createCtx)
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
				seg := rel.Segment(segId)
				for _, id := range seg.Blocks() {
					blk := seg.Block(id)
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
					err := inst.Append(req)
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
			req := CreateAppendCtx(database, gen, schema.Name, baseCk)
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
	t.Logf("Load: %d", loadCnt)
	tbl, err := inst.Store.DataTables.WeakRefTable(tblMeta.Id)
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
	initTestEnv(t)
	inst, gen, database := initTestDB3(t)
	schema := metadata.MockSchema(2)
	shardId := database.GetShardId()
	createCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema,
	}
	tblMeta, err := inst.CreateTable(createCtx)
	assert.Nil(t, err)
	assert.NotNil(t, tblMeta)
	rows := inst.Store.Catalog.Cfg.BlockMaxRows * 2 / 5
	baseCk := mock.MockBatch(tblMeta.Schema.Types(), rows)

	appendCtx := new(AppendCtx)
	for i := 0; i < 50; i++ {
		appendCtx = CreateAppendCtx(database, gen, schema.Name, baseCk)
		err = inst.Append(appendCtx)
		assert.Nil(t, err)
	}

	dropCtx := CreateTableMutationCtx(database, gen, schema.Name)
	_, err = inst.DropTable(dropCtx)
	assert.Nil(t, err)
	testutils.WaitExpect(500, func() bool {
		return inst.GetShardCheckpointId(shardId) == inst.Wal.GetShardCurrSeqNum(shardId)
	})
	// assert.Equal(t, gen.Get(shardId), inst.Wal.GetShardCurrSeqNum(shardId))
	assert.Equal(t, inst.Wal.GetShardCurrSeqNum(shardId), inst.GetShardCheckpointId(shardId))

	inst.Close()
}

func TestMultiInstance(t *testing.T) {
	dir := initTestEnv(t)
	var dirs []string
	for i := 0; i < 10; i++ {
		dirs = append(dirs, path.Join(dir, fmt.Sprintf("wd%d", i)))
	}
	var insts []*DB
	for _, d := range dirs {
		opts := storage.Options{}
		inst, _ := Open(d, &opts)
		insts = append(insts, inst)
		defer inst.Close()
	}

	gen := shard.NewMockIndexAllocator()
	shardId := uint64(100)

	var schema *metadata.Schema
	for _, inst := range insts {
		db, err := inst.Store.Catalog.SimpleCreateDatabase("db1", gen.Next(shardId))
		assert.Nil(t, err)
		schema = metadata.MockSchema(2)
		schema.Name = "xxx"
		createCtx := &CreateTableCtx{
			DBMutationCtx: *CreateDBMutationCtx(db, gen),
			Schema:        schema,
		}
		_, err = inst.CreateTable(createCtx)
		assert.Nil(t, err)
	}
	meta, err := insts[0].Store.Catalog.SimpleGetTableByName("db1", schema.Name)
	assert.Nil(t, err)
	bat := mock.MockBatch(meta.Schema.Types(), 100)
	for _, inst := range insts {
		database, err := inst.Store.Catalog.SimpleGetDatabaseByName("db1")
		assert.Nil(t, err)
		appendCtx := CreateAppendCtx(database, gen, schema.Name, bat)
		err = inst.Append(appendCtx)
		assert.Nil(t, err)
	}

	time.Sleep(time.Duration(50) * time.Millisecond)
}

func decodeBlockIds(ids []string) []string {
	res := make([]string, 0)
	for _, id := range ids {
		res = append(res, strconv.Itoa(int(encoding.DecodeUint64([]byte(id)))))
	}
	return res
}

func TestFilterUnclosedSegment(t *testing.T) {
	initTestEnv(t)
	inst, gen, database := initTestDB3(t)
	inst.Store.Catalog.Cfg.BlockMaxRows = uint64(10)
	inst.Store.Catalog.Cfg.SegmentMaxBlocks = uint64(4)

	schema := metadata.MockSchemaAll(14)
	indice := metadata.NewIndexSchema()
	indice.MakeIndex("idx-0", metadata.NumBsi, 0)
	indice.MakeIndex("idx-1", metadata.FixStrBsi, 13)
	createCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema,
		Indice:        indice,
	}
	tblMeta, err := inst.CreateTable(createCtx)
	assert.Nil(t, err)
	assert.NotNil(t, tblMeta)
	blkCnt := inst.Store.Catalog.Cfg.SegmentMaxBlocks
	rows := inst.Store.Catalog.Cfg.BlockMaxRows
	baseCk := mock.MockBatch(tblMeta.Schema.Types(), rows)
	nulls.Add(baseCk.Vecs[0].Nsp, 3)
	nulls.Add(baseCk.Vecs[0].Nsp, 4)
	nulls.Add(baseCk.Vecs[13].Nsp, 3)
	nulls.Add(baseCk.Vecs[13].Nsp, 4)
	appendCtx := CreateAppendCtx(database, gen, schema.Name, baseCk)
	for i := 0; i < int(blkCnt-1); i++ {
		assert.Nil(t, inst.Append(appendCtx))
	}
	baseCk = mock.MockBatch(tblMeta.Schema.Types(), rows/2+1)
	nulls.Add(baseCk.Vecs[0].Nsp, 3)
	nulls.Add(baseCk.Vecs[0].Nsp, 4)
	nulls.Add(baseCk.Vecs[13].Nsp, 3)
	nulls.Add(baseCk.Vecs[13].Nsp, 4)
	appendCtx = CreateAppendCtx(database, gen, schema.Name, baseCk)
	assert.Nil(t, inst.Append(appendCtx))
	time.Sleep(100 * time.Millisecond)

	tblData, err := inst.Store.DataTables.WeakRefTable(tblMeta.Id)
	assert.Nil(t, err)
	segId := inst.GetSegmentIds(database.Name, tblMeta.Schema.Name).Ids[0]
	seg := tblData.WeakRefSegment(segId)
	//t.Logf("%+v", seg.GetIndexHolder())
	segment := &db.Segment{
		Data: seg,
		Ids:  new(atomic.Value),
	}
	sparseFilter := segment.NewSparseFilter()
	summarizer := segment.NewSummarizer()
	filter := segment.NewFilter()

	// test sparse filter upon unclosed segment
	res, _ := sparseFilter.Eq("mock_0", int8(-1))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"4"}))
	res, _ = sparseFilter.Ne("mock_0", int8(-1))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"1", "2", "3", "4"}))
	res, _ = sparseFilter.Btw("mock_0", int8(1), int8(8))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"1", "2", "3", "4"}))
	res, _ = sparseFilter.Btw("mock_0", int8(1), int8(10))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"4"}))
	res, _ = sparseFilter.Lt("mock_0", int8(0))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"4"}))
	res, _ = sparseFilter.Gt("mock_0", int8(5))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"1", "2", "3", "4"}))
	res, _ = sparseFilter.Le("mock_0", int8(0))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"1", "2", "3", "4"}))
	res, _ = sparseFilter.Ge("mock_0", int8(9))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"1", "2", "3", "4"}))
	res, _ = sparseFilter.Eq("mock_0", int8(1))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"1", "2", "3", "4"}))
	res, _ = sparseFilter.Lt("mock_0", int8(1))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"1", "2", "3", "4"}))
	res, _ = sparseFilter.Gt("mock_0", int8(9))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"4"}))
	res, _ = sparseFilter.Le("mock_0", int8(-1))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"4"}))
	res, _ = sparseFilter.Ge("mock_0", int8(10))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"4"}))

	res, _ = sparseFilter.Eq("mock_13", []byte("str/"))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"4"}))
	res, _ = sparseFilter.Ne("mock_13", []byte("str/"))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"1", "2", "3", "4"}))
	res, _ = sparseFilter.Btw("mock_13", []byte("str1"), []byte("str8"))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"1", "2", "3", "4"}))
	res, _ = sparseFilter.Btw("mock_13", []byte("str1"), []byte("str:"))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"4"}))
	res, _ = sparseFilter.Lt("mock_13", []byte("str0"))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"4"}))
	res, _ = sparseFilter.Gt("mock_13", []byte("str5"))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"1", "2", "3", "4"}))
	res, _ = sparseFilter.Le("mock_13", []byte("str0"))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"1", "2", "3", "4"}))
	res, _ = sparseFilter.Ge("mock_13", []byte("str9"))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"1", "2", "3", "4"}))
	res, _ = sparseFilter.Eq("mock_13", []byte("str1"))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"1", "2", "3", "4"}))
	res, _ = sparseFilter.Lt("mock_13", []byte("str1"))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"1", "2", "3", "4"}))
	res, _ = sparseFilter.Gt("mock_13", []byte("str9"))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"4"}))
	res, _ = sparseFilter.Le("mock_13", []byte("str/"))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"4"}))
	res, _ = sparseFilter.Ge("mock_13", []byte("str:"))
	assert.True(t, matchStringArray(decodeBlockIds(res), []string{"4"}))

	// test summarizer upon unclosed segment

	mockBM := roaring.NewBitmap()
	mockBM.AddRange(1, 7)
	mockBM.AddRange(11, 17)
	mockBM.AddRange(21, 27)
	mockBM.AddRange(31, 37)
	sum, cnt, err := summarizer.Sum("mock_0", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, sum, int64(14*3 + 8))
	assert.Equal(t, cnt, uint64(4*3 + 3))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 10)
	mockBM.AddRange(13, 20)
	mockBM.AddRange(23, 30)
	mockBM.AddRange(33, 40)
	min, err := summarizer.Min("mock_0", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, min, int8(5))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 10)
	mockBM.AddRange(13, 20)
	mockBM.AddRange(23, 30)
	mockBM.AddRange(33, 40)
	max, err := summarizer.Max("mock_0", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, max, int8(9))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 10)
	mockBM.AddRange(13, 20)
	mockBM.AddRange(23, 30)
	mockBM.AddRange(33, 40)
	nullCnt, err := summarizer.NullCount("mock_0", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, nullCnt, uint64(2*4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 10)
	mockBM.AddRange(13, 20)
	mockBM.AddRange(23, 30)
	mockBM.AddRange(33, 40)
	cnt, err = summarizer.Count("mock_0", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(5*3 + 1))
	cnt, err = summarizer.Count("mock_0", nil)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(8*3 + 4))

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 10)
	mockBM.AddRange(13, 20)
	mockBM.AddRange(23, 30)
	mockBM.AddRange(33, 40)
	min, err = summarizer.Min("mock_13", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, min, []byte("str5"))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 10)
	mockBM.AddRange(13, 20)
	mockBM.AddRange(23, 30)
	mockBM.AddRange(33, 40)
	max, err = summarizer.Max("mock_13", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, max, []byte("str9"))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 10)
	mockBM.AddRange(13, 20)
	mockBM.AddRange(23, 30)
	mockBM.AddRange(33, 40)
	nullCnt, err = summarizer.NullCount("mock_13", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, nullCnt, uint64(2*4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 10)
	mockBM.AddRange(13, 20)
	mockBM.AddRange(23, 30)
	mockBM.AddRange(33, 40)
	cnt, err = summarizer.Count("mock_13", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(5*3 + 1))
	cnt, err = summarizer.Count("mock_13", nil)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(8*3 + 4))

	// test filter upon unclosed segment
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	res_, err := filter.Ne("mock_0", int8(-1))
	assert.Nil(t, err)
	assert.NotNil(t, res_)
	mockBM.Clear()
	mockBM.AddRange(0, 36)
	mockBM.Remove(3)
	mockBM.Remove(4)
	mockBM.Remove(13)
	mockBM.Remove(14)
	mockBM.Remove(23)
	mockBM.Remove(24)
	mockBM.Remove(33)
	mockBM.Remove(34)
	assert.True(t, mockBM.Equals(res_))
	res_, _ = filter.Eq("mock_0", int8(-1))
	assert.Equal(t, true, res_.IsEmpty())
	res_, _ = filter.Eq("mock_0", int8(3))
	assert.Equal(t, true, res_.IsEmpty())
	res_, _ = filter.Eq("mock_0", int8(5))
	mockBM = roaring.NewBitmap()
	mockBM.Add(5)
	mockBM.Add(15)
	mockBM.Add(25)
	mockBM.Add(35)
	mockBM.Xor(res_)
	assert.True(t, mockBM.IsEmpty())
	res_, _ = filter.Gt("mock_0", int8(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(5, 10)
	mockBM.AddRange(15, 20)
	mockBM.AddRange(25, 30)
	mockBM.AddRange(35, 36)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Ge("mock_0", int8(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(5, 10)
	mockBM.AddRange(15, 20)
	mockBM.AddRange(25, 30)
	mockBM.AddRange(35, 36)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Le("mock_0", int8(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 3)
	mockBM.AddRange(10, 13)
	mockBM.AddRange(20, 23)
	mockBM.AddRange(30, 33)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Lt("mock_0", int8(6))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 6)
	mockBM.AddRange(10, 16)
	mockBM.AddRange(20, 26)
	mockBM.AddRange(30, 36)
	mockBM.Remove(3)
	mockBM.Remove(4)
	mockBM.Remove(13)
	mockBM.Remove(14)
	mockBM.Remove(23)
	mockBM.Remove(24)
	mockBM.Remove(33)
	mockBM.Remove(34)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Btw("mock_0", int8(3), int8(8))
	mockBM.AddRange(5, 9)
	mockBM.AddRange(15, 19)
	mockBM.AddRange(25, 29)
	mockBM.AddRange(35, 36)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	res_, err = filter.Ne("mock_13", []byte("str/"))
	assert.Nil(t, err)
	assert.NotNil(t, res_)
	mockBM.Clear()
	mockBM.AddRange(0, 36)
	mockBM.Remove(3)
	mockBM.Remove(4)
	mockBM.Remove(13)
	mockBM.Remove(14)
	mockBM.Remove(23)
	mockBM.Remove(24)
	mockBM.Remove(33)
	mockBM.Remove(34)
	assert.True(t, mockBM.Equals(res_))
	res_, _ = filter.Eq("mock_13", []byte("str/"))
	assert.Equal(t, true, res_.IsEmpty())
	res_, _ = filter.Eq("mock_13", []byte("str3"))
	assert.Equal(t, true, res_.IsEmpty())
	res_, _ = filter.Eq("mock_13", []byte("str5"))
	mockBM = roaring.NewBitmap()
	mockBM.Add(5)
	mockBM.Add(15)
	mockBM.Add(25)
	mockBM.Add(35)
	mockBM.Xor(res_)
	assert.True(t, mockBM.IsEmpty())
	res_, _ = filter.Gt("mock_13", []byte("str4"))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(5, 10)
	mockBM.AddRange(15, 20)
	mockBM.AddRange(25, 30)
	mockBM.AddRange(35, 36)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Ge("mock_13", []byte("str4"))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(5, 10)
	mockBM.AddRange(15, 20)
	mockBM.AddRange(25, 30)
	mockBM.AddRange(35, 36)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Le("mock_13", []byte("str4"))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 3)
	mockBM.AddRange(10, 13)
	mockBM.AddRange(20, 23)
	mockBM.AddRange(30, 33)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Lt("mock_13", []byte("str6"))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 6)
	mockBM.AddRange(10, 16)
	mockBM.AddRange(20, 26)
	mockBM.AddRange(30, 36)
	mockBM.Remove(3)
	mockBM.Remove(4)
	mockBM.Remove(13)
	mockBM.Remove(14)
	mockBM.Remove(23)
	mockBM.Remove(24)
	mockBM.Remove(33)
	mockBM.Remove(34)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Btw("mock_13", []byte("str3"), []byte("str8"))
	mockBM.AddRange(5, 9)
	mockBM.AddRange(15, 19)
	mockBM.AddRange(25, 29)
	mockBM.AddRange(35, 36)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())

	inst.Close()
}

func TestFilter(t *testing.T) {
	waitTime := time.Duration(100) * time.Millisecond
	if invariants.RaceEnabled {
		waitTime *= 2
	}
	initTestEnv(t)
	inst, gen, database := initTestDB3(t)
	inst.Store.Catalog.Cfg.BlockMaxRows = uint64(10)
	inst.Store.Catalog.Cfg.SegmentMaxBlocks = uint64(4)
	colIdx := make([]int, 0)
	for i := 0; i < 12; i++ {
		colIdx = append(colIdx, i)
	}

	schema := metadata.MockSchemaAll(14)
	indice := metadata.NewIndexSchema()
	_, err := indice.MakeIndex("idx-1", metadata.NumBsi, colIdx...)
	assert.Nil(t, err)
	_, err = indice.MakeIndex("idx-2", metadata.FixStrBsi, 13)
	assert.Nil(t, err)
	createCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema,
		Indice:        indice,
	}
	tblMeta, err := inst.CreateTable(createCtx)
	assert.Nil(t, err)
	assert.NotNil(t, tblMeta)
	blkCnt := inst.Store.Catalog.Cfg.SegmentMaxBlocks
	rows := inst.Store.Catalog.Cfg.BlockMaxRows
	baseCk := mock.MockBatch(tblMeta.Schema.Types(), rows)

	appendCtx := CreateAppendCtx(database, gen, schema.Name, baseCk)
	for i := 0; i < int(blkCnt+1); i++ {
		assert.Nil(t, inst.Append(appendCtx))
	}
	time.Sleep(waitTime)

	tblData, err := inst.Store.DataTables.WeakRefTable(tblMeta.Id)
	assert.Nil(t, err)
	segId := inst.GetSegmentIds(database.Name, tblMeta.Schema.Name).Ids[0]
	seg := tblData.WeakRefSegment(segId)
	//seg.GetIndexHolder().Init(seg.GetSegmentFile())
	segment := &db.Segment{
		Data: seg,
		Ids:  new(atomic.Value),
	}
	summarizer := segment.NewSummarizer()
	sparseFilter := segment.NewSparseFilter()
	filter := segment.NewFilter()

	// test sparse filter
	res, _ := sparseFilter.Eq("mock_0", int8(-1))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Ne("mock_0", int8(-1))
	assert.Equal(t, decodeBlockIds(res), []string{"1", "2", "3", "4"})
	res, _ = sparseFilter.Btw("mock_0", int8(1), int8(7))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Btw("mock_0", int8(-1), int8(8))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Lt("mock_0", int8(0))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Gt("mock_0", int8(8))
	assert.Equal(t, decodeBlockIds(res), []string{"4"})
	res, _ = sparseFilter.Le("mock_0", int8(0))
	assert.Equal(t, decodeBlockIds(res), []string{"1"})
	res, _ = sparseFilter.Ge("mock_0", int8(9))
	assert.Equal(t, decodeBlockIds(res), []string{"4"})
	res, _ = sparseFilter.Eq("mock_0", int8(1))
	assert.Equal(t, decodeBlockIds(res), []string{"1"})
	res, _ = sparseFilter.Lt("mock_0", int8(1))
	assert.Equal(t, decodeBlockIds(res), []string{"1"})
	res, _ = sparseFilter.Gt("mock_0", int8(9))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Le("mock_0", int8(-1))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Ge("mock_0", int8(10))
	assert.Equal(t, decodeBlockIds(res), []string{})

	res, _ = sparseFilter.Eq("mock_1", int16(-1))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Ne("mock_1", int16(-1))
	assert.Equal(t, decodeBlockIds(res), []string{"1", "2", "3", "4"})
	res, _ = sparseFilter.Btw("mock_1", int16(1), int16(7))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Btw("mock_1", int16(-1), int16(8))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Lt("mock_1", int16(0))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Gt("mock_1", int16(8))
	assert.Equal(t, decodeBlockIds(res), []string{"4"})
	res, _ = sparseFilter.Le("mock_1", int16(0))
	assert.Equal(t, decodeBlockIds(res), []string{"1"})
	res, _ = sparseFilter.Ge("mock_1", int16(9))
	assert.Equal(t, decodeBlockIds(res), []string{"4"})

	res, _ = sparseFilter.Eq("mock_2", int32(-1))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Ne("mock_2", int32(-1))
	assert.Equal(t, decodeBlockIds(res), []string{"1", "2", "3", "4"})
	res, _ = sparseFilter.Btw("mock_2", int32(1), int32(7))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Btw("mock_2", int32(-1), int32(8))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Lt("mock_2", int32(0))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Gt("mock_2", int32(8))
	assert.Equal(t, decodeBlockIds(res), []string{"4"})
	res, _ = sparseFilter.Le("mock_2", int32(0))
	assert.Equal(t, decodeBlockIds(res), []string{"1"})
	res, _ = sparseFilter.Ge("mock_2", int32(9))
	assert.Equal(t, decodeBlockIds(res), []string{"4"})

	res, _ = sparseFilter.Eq("mock_3", int64(-1))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Ne("mock_3", int64(-1))
	assert.Equal(t, decodeBlockIds(res), []string{"1", "2", "3", "4"})
	res, _ = sparseFilter.Btw("mock_3", int64(1), int64(7))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Btw("mock_3", int64(-1), int64(8))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Lt("mock_3", int64(0))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Gt("mock_3", int64(4))
	assert.Equal(t, decodeBlockIds(res), []string{"3", "4"})
	res, _ = sparseFilter.Le("mock_3", int64(2))
	assert.Equal(t, decodeBlockIds(res), []string{"1", "2"})
	res, _ = sparseFilter.Ge("mock_3", int64(3))
	assert.Equal(t, decodeBlockIds(res), []string{"2", "3", "4"})

	res, _ = sparseFilter.Eq("mock_4", uint8(10))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Ne("mock_4", uint8(10))
	assert.Equal(t, decodeBlockIds(res), []string{"1", "2", "3", "4"})
	res, _ = sparseFilter.Btw("mock_4", uint8(2), uint8(7))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Btw("mock_4", uint8(1), uint8(10))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Lt("mock_4", uint8(0))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Gt("mock_4", uint8(8))
	assert.Equal(t, decodeBlockIds(res), []string{"4"})
	res, _ = sparseFilter.Le("mock_4", uint8(0))
	assert.Equal(t, decodeBlockIds(res), []string{"1"})
	res, _ = sparseFilter.Ge("mock_4", uint8(9))
	assert.Equal(t, decodeBlockIds(res), []string{"4"})

	res, _ = sparseFilter.Eq("mock_5", uint16(10))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Ne("mock_5", uint16(10))
	assert.Equal(t, decodeBlockIds(res), []string{"1", "2", "3", "4"})
	res, _ = sparseFilter.Btw("mock_5", uint16(1), uint16(7))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Btw("mock_5", uint16(1), uint16(10))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Lt("mock_5", uint16(0))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Gt("mock_5", uint16(8))
	assert.Equal(t, decodeBlockIds(res), []string{"4"})
	res, _ = sparseFilter.Le("mock_5", uint16(0))
	assert.Equal(t, decodeBlockIds(res), []string{"1"})
	res, _ = sparseFilter.Ge("mock_5", uint16(9))
	assert.Equal(t, decodeBlockIds(res), []string{"4"})

	res, _ = sparseFilter.Eq("mock_6", uint32(10))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Ne("mock_6", uint32(10))
	assert.Equal(t, decodeBlockIds(res), []string{"1", "2", "3", "4"})
	res, _ = sparseFilter.Btw("mock_6", uint32(1), uint32(7))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Btw("mock_6", uint32(1), uint32(10))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Lt("mock_6", uint32(0))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Gt("mock_6", uint32(8))
	assert.Equal(t, decodeBlockIds(res), []string{"4"})
	res, _ = sparseFilter.Le("mock_6", uint32(0))
	assert.Equal(t, decodeBlockIds(res), []string{"1"})
	res, _ = sparseFilter.Ge("mock_6", uint32(9))
	assert.Equal(t, decodeBlockIds(res), []string{"4"})

	res, _ = sparseFilter.Eq("mock_7", uint64(10))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Ne("mock_7", uint64(10))
	assert.Equal(t, decodeBlockIds(res), []string{"1", "2", "3", "4"})
	res, _ = sparseFilter.Btw("mock_7", uint64(1), uint64(7))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Btw("mock_7", uint64(1), uint64(10))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Lt("mock_7", uint64(0))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Gt("mock_7", uint64(8))
	assert.Equal(t, decodeBlockIds(res), []string{"4"})
	res, _ = sparseFilter.Le("mock_7", uint64(0))
	assert.Equal(t, decodeBlockIds(res), []string{"1"})
	res, _ = sparseFilter.Ge("mock_7", uint64(9))
	assert.Equal(t, decodeBlockIds(res), []string{"4"})

	res, _ = sparseFilter.Eq("mock_8", float32(10))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Ne("mock_8", float32(10))
	assert.Equal(t, decodeBlockIds(res), []string{"1", "2", "3", "4"})
	res, _ = sparseFilter.Btw("mock_8", float32(1), float32(7))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Btw("mock_8", float32(1), float32(10))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Lt("mock_8", float32(0))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Gt("mock_8", float32(8))
	assert.Equal(t, decodeBlockIds(res), []string{"4"})
	res, _ = sparseFilter.Le("mock_8", float32(0))
	assert.Equal(t, decodeBlockIds(res), []string{"1"})
	res, _ = sparseFilter.Ge("mock_8", float32(9))
	assert.Equal(t, decodeBlockIds(res), []string{"4"})

	res, _ = sparseFilter.Eq("mock_9", float64(10))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Ne("mock_9", float64(10))
	assert.Equal(t, decodeBlockIds(res), []string{"1", "2", "3", "4"})
	res, _ = sparseFilter.Btw("mock_9", float64(1), float64(7))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Btw("mock_9", float64(1), float64(10))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Lt("mock_9", float64(0))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Gt("mock_9", float64(8))
	assert.Equal(t, decodeBlockIds(res), []string{"4"})
	res, _ = sparseFilter.Le("mock_9", float64(0))
	assert.Equal(t, decodeBlockIds(res), []string{"1"})
	res, _ = sparseFilter.Ge("mock_9", float64(9))
	assert.Equal(t, decodeBlockIds(res), []string{"4"})

	res, _ = sparseFilter.Eq("mock_10", types.FromCalendar(0, 1, 1))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Ne("mock_10", types.FromCalendar(0, 1, 1))
	assert.Equal(t, decodeBlockIds(res), []string{"1", "2", "3", "4"})
	res, _ = sparseFilter.Btw("mock_10", types.FromCalendar(100, 1, 1), types.FromCalendar(300, 1, 1))
	assert.Equal(t, decodeBlockIds(res), []string{"1", "2", "3", "4"})
	res, _ = sparseFilter.Btw("mock_10", types.FromCalendar(0, 1, 1), types.FromCalendar(100, 1, 1))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Ge("mock_10", types.FromCalendar(1000, 1, 1))
	assert.Equal(t, decodeBlockIds(res), []string{"1", "2", "3", "4"})
	res, _ = sparseFilter.Gt("mock_10", types.FromCalendar(1000, 1, 1))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Le("mock_10", types.FromCalendar(100, 1, 1))
	assert.Equal(t, decodeBlockIds(res), []string{"1", "2", "3", "4"})
	res, _ = sparseFilter.Lt("mock_10", types.FromCalendar(100, 1, 1))
	assert.Equal(t, decodeBlockIds(res), []string{})

	res, _ = sparseFilter.Lt("mock_11", types.FromClock(100, 1, 1, 1, 1, 1, 1))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Gt("mock_11", types.FromClock(1000, 1, 1, 1, 1, 1, 1))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Le("mock_11", types.FromClock(300, 1, 1, 1, 1, 1, 1))
	assert.Equal(t, decodeBlockIds(res), []string{"1", "2", "3", "4"})
	res, _ = sparseFilter.Ge("mock_11", types.FromClock(1000, 1, 1, 1, 1, 1, 1))
	assert.Equal(t, decodeBlockIds(res), []string{"1", "2", "3", "4"})
	res, _ = sparseFilter.Eq("mock_11", types.FromClock(1000, 1, 1, 1, 1, 1, 1))
	assert.Equal(t, decodeBlockIds(res), []string{"1", "2", "3", "4"})
	res, _ = sparseFilter.Ne("mock_11", types.FromClock(1000, 1, 1, 1, 1, 1, 1))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Btw("mock_11", types.FromClock(100, 1, 1, 1, 1, 1, 1), types.FromClock(1000, 1, 1, 1, 1, 1, 1))
	assert.Equal(t, decodeBlockIds(res), []string{"1", "2", "3", "4"})

	res, _ = sparseFilter.Eq("mock_12", []byte("str/"))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Ne("mock_12", []byte("str/"))
	assert.Equal(t, decodeBlockIds(res), []string{"1", "2", "3", "4"})
	res, _ = sparseFilter.Btw("mock_12", []byte("str1"), []byte("str8"))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Btw("mock_12", []byte("str/"), []byte("str8"))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Lt("mock_12", []byte("str0"))
	assert.Equal(t, decodeBlockIds(res), []string{})
	res, _ = sparseFilter.Gt("mock_12", []byte("str8"))
	assert.Equal(t, decodeBlockIds(res), []string{"4"})

	_, err = sparseFilter.Eq("xxxx", 0)
	assert.NotNil(t, err)
	_, err = sparseFilter.Ne("xxxx", 0)
	assert.NotNil(t, err)
	_, err = sparseFilter.Gt("xxxx", 0)
	assert.NotNil(t, err)
	_, err = sparseFilter.Lt("xxxx", 0)
	assert.NotNil(t, err)
	_, err = sparseFilter.Ge("xxxx", 0)
	assert.NotNil(t, err)
	_, err = sparseFilter.Le("xxxx", 0)
	assert.NotNil(t, err)
	_, err = sparseFilter.Btw("xxxx", 0, 0)
	assert.NotNil(t, err)

	// test filter
	mockBM := roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	res_, err := filter.Ne("mock_0", int8(-1))
	assert.Nil(t, err)
	assert.NotNil(t, res_)
	assert.True(t, mockBM.Equals(res_))
	res_, _ = filter.Eq("mock_0", int8(-1))
	assert.Equal(t, true, roaring.NewBitmap().Equals(res_))
	res_, _ = filter.Eq("mock_0", int8(3))
	mockBM = roaring.NewBitmap()
	mockBM.Add(12)
	mockBM.Add(13)
	mockBM.Add(14)
	mockBM.Add(15)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Gt("mock_0", int8(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(20, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Ge("mock_0", int8(5))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(20, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Le("mock_0", int8(3))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Lt("mock_0", int8(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Btw("mock_0", int8(3), int8(8))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(12, 36)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	res_, _ = filter.Ne("mock_1", int16(-1))
	assert.Equal(t, true, mockBM.Equals(res_))
	res_, _ = filter.Eq("mock_1", int16(-1))
	assert.Equal(t, true, roaring.NewBitmap().Equals(res_))
	res_, _ = filter.Eq("mock_1", int16(3))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(12, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Gt("mock_1", int16(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(20, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Ge("mock_1", int16(5))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(20, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Le("mock_1", int16(3))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Lt("mock_1", int16(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Btw("mock_1", int16(3), int16(8))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(12, 36)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	res_, _ = filter.Ne("mock_2", int32(-1))
	assert.Equal(t, true, mockBM.Equals(res_))
	res_, _ = filter.Eq("mock_2", int32(-1))
	assert.Equal(t, true, roaring.NewBitmap().Equals(res_))
	res_, _ = filter.Eq("mock_2", int32(3))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(12, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Gt("mock_2", int32(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(20, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Ge("mock_2", int32(5))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(20, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Le("mock_2", int32(3))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Lt("mock_2", int32(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Btw("mock_2", int32(3), int32(8))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(12, 36)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	res_, _ = filter.Ne("mock_3", int64(-1))
	assert.Equal(t, true, mockBM.Equals(res_))
	res_, _ = filter.Eq("mock_3", int64(-1))
	assert.Equal(t, true, roaring.NewBitmap().Equals(res_))
	res_, _ = filter.Eq("mock_3", int64(3))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(12, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Gt("mock_3", int64(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(20, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Ge("mock_3", int64(5))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(20, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Le("mock_3", int64(3))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Lt("mock_3", int64(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Btw("mock_3", int64(3), int64(8))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(12, 36)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	res_, _ = filter.Ne("mock_4", uint8(100))
	assert.Equal(t, true, mockBM.Equals(res_))
	res_, _ = filter.Eq("mock_4", uint8(100))
	assert.Equal(t, true, roaring.NewBitmap().Equals(res_))
	res_, _ = filter.Eq("mock_4", uint8(3))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(12, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Gt("mock_4", uint8(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(20, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Ge("mock_4", uint8(5))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(20, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Le("mock_4", uint8(3))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Lt("mock_4", uint8(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Btw("mock_4", uint8(3), uint8(8))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(12, 36)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	res_, _ = filter.Ne("mock_5", uint16(100))
	assert.Equal(t, true, mockBM.Equals(res_))
	res_, _ = filter.Eq("mock_5", uint16(100))
	assert.Equal(t, true, roaring.NewBitmap().Equals(res_))
	res_, _ = filter.Eq("mock_5", uint16(3))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(12, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Gt("mock_5", uint16(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(20, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Ge("mock_5", uint16(5))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(20, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Le("mock_5", uint16(3))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Lt("mock_5", uint16(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Btw("mock_5", uint16(3), uint16(8))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(12, 36)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	res_, _ = filter.Ne("mock_6", uint32(100))
	assert.Equal(t, true, mockBM.Equals(res_))
	res_, _ = filter.Eq("mock_6", uint32(100))
	assert.Equal(t, true, roaring.NewBitmap().Equals(res_))
	res_, _ = filter.Eq("mock_6", uint32(3))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(12, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Gt("mock_6", uint32(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(20, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Ge("mock_6", uint32(5))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(20, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Le("mock_6", uint32(3))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Lt("mock_6", uint32(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Btw("mock_6", uint32(3), uint32(8))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(12, 36)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	res_, _ = filter.Ne("mock_7", uint64(100))
	assert.Equal(t, true, mockBM.Equals(res_))
	res_, _ = filter.Eq("mock_7", uint64(100))
	assert.Equal(t, true, roaring.NewBitmap().Equals(res_))
	res_, _ = filter.Eq("mock_7", uint64(3))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(12, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Gt("mock_7", uint64(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(20, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Ge("mock_7", uint64(5))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(20, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Le("mock_7", uint64(3))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Lt("mock_7", uint64(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Btw("mock_7", uint64(3), uint64(8))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(12, 36)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	res_, _ = filter.Ne("mock_8", float32(100))
	assert.Equal(t, true, mockBM.Equals(res_))
	res_, _ = filter.Eq("mock_8", float32(100))
	assert.Equal(t, true, roaring.NewBitmap().Equals(res_))
	res_, _ = filter.Eq("mock_8", float32(3))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(12, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Gt("mock_8", float32(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(20, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Ge("mock_8", float32(5))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(20, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Le("mock_8", float32(3))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Lt("mock_8", float32(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Btw("mock_8", float32(3), float32(8))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(12, 36)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	res_, _ = filter.Ne("mock_9", float64(100))
	assert.Equal(t, true, mockBM.Equals(res_))
	res_, _ = filter.Eq("mock_9", float64(100))
	assert.Equal(t, true, roaring.NewBitmap().Equals(res_))
	res_, _ = filter.Eq("mock_9", float64(3))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(12, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Gt("mock_9", float64(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(20, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Ge("mock_9", float64(5))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(20, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Le("mock_9", float64(3))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Lt("mock_9", float64(4))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Btw("mock_9", float64(3), float64(8))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(12, 36)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	res_, _ = filter.Ne("mock_10", types.FromCalendar(0, 1, 1))
	assert.Equal(t, true, mockBM.Equals(res_))
	res_, _ = filter.Eq("mock_10", types.FromCalendar(0, 1, 1))
	assert.Equal(t, true, roaring.NewBitmap().Equals(res_))
	res_, _ = filter.Eq("mock_10", types.FromCalendar(400, 1, 1))
	mockBM = roaring.NewBitmap()
	mockBM.Add(3)
	mockBM.Add(13)
	mockBM.Add(23)
	mockBM.Add(33)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Gt("mock_10", types.FromCalendar(500, 1, 1))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(5, 10)
	mockBM.AddRange(15, 20)
	mockBM.AddRange(25, 30)
	mockBM.AddRange(35, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Ge("mock_10", types.FromCalendar(600, 1, 1))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(5, 10)
	mockBM.AddRange(15, 20)
	mockBM.AddRange(25, 30)
	mockBM.AddRange(35, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Le("mock_10", types.FromCalendar(400, 1, 1))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 4)
	mockBM.AddRange(10, 14)
	mockBM.AddRange(20, 24)
	mockBM.AddRange(30, 34)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Lt("mock_10", types.FromCalendar(500, 1, 1))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 4)
	mockBM.AddRange(10, 14)
	mockBM.AddRange(20, 24)
	mockBM.AddRange(30, 34)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Btw("mock_10", types.FromCalendar(400, 1, 1), types.FromCalendar(900, 1, 1))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 9)
	mockBM.AddRange(13, 19)
	mockBM.AddRange(23, 29)
	mockBM.AddRange(33, 39)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	res_, _ = filter.Ne("mock_11", types.FromClock(0, 1, 1, 1, 1, 1, 1))
	assert.Equal(t, true, mockBM.Equals(res_))
	res_, _ = filter.Eq("mock_11", types.FromClock(0, 1, 1, 1, 1, 1, 1))
	assert.Equal(t, true, roaring.NewBitmap().Equals(res_))
	res_, _ = filter.Eq("mock_11", types.FromClock(400, 1, 1, 1, 1, 1, 1))
	mockBM = roaring.NewBitmap()
	mockBM.Add(3)
	mockBM.Add(13)
	mockBM.Add(23)
	mockBM.Add(33)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Gt("mock_11", types.FromClock(500, 1, 1, 1, 1, 1, 1))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(5, 10)
	mockBM.AddRange(15, 20)
	mockBM.AddRange(25, 30)
	mockBM.AddRange(35, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Ge("mock_11", types.FromClock(600, 1, 1, 1, 1, 1, 1))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(5, 10)
	mockBM.AddRange(15, 20)
	mockBM.AddRange(25, 30)
	mockBM.AddRange(35, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Le("mock_11", types.FromClock(400, 1, 1, 1, 1, 1, 1))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 4)
	mockBM.AddRange(10, 14)
	mockBM.AddRange(20, 24)
	mockBM.AddRange(30, 34)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Lt("mock_11", types.FromClock(500, 1, 1, 1, 1, 1, 1))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 4)
	mockBM.AddRange(10, 14)
	mockBM.AddRange(20, 24)
	mockBM.AddRange(30, 34)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Btw("mock_11", types.FromClock(400, 1, 1, 1, 1, 1, 1), types.FromClock(900, 1, 1, 1, 1, 1, 1))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 9)
	mockBM.AddRange(13, 19)
	mockBM.AddRange(23, 29)
	mockBM.AddRange(33, 39)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	res_, err = filter.Ne("mock_13", []byte("str/"))
 	assert.Nil(t, err)
	assert.NotNil(t, res_)
	assert.True(t, mockBM.Equals(res_))
	res_, err = filter.Eq("mock_13", []byte("str/"))
	assert.Equal(t, true, roaring.NewBitmap().Equals(res_))
	res_, _ = filter.Eq("mock_13", []byte("str3"))
	mockBM = roaring.NewBitmap()
	mockBM.Add(12)
	mockBM.Add(13)
	mockBM.Add(14)
	mockBM.Add(15)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Gt("mock_13", []byte("str4"))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(20, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Ge("mock_13", []byte("str5"))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(20, 40)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Le("mock_13", []byte("str3"))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Lt("mock_13", []byte("str4"))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 16)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())
	res_, _ = filter.Btw("mock_13", []byte("str3"), []byte("str8"))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(12, 36)
	mockBM.Xor(res_)
	assert.Equal(t, true, mockBM.IsEmpty())

	_, err = filter.Eq("xxxx", 0)
	assert.NotNil(t, err)
	_, err = filter.Ne("xxxx", 0)
	assert.NotNil(t, err)
	_, err = filter.Gt("xxxx", 0)
	assert.NotNil(t, err)
	_, err = filter.Lt("xxxx", 0)
	assert.NotNil(t, err)
	_, err = filter.Ge("xxxx", 0)
	assert.NotNil(t, err)
	_, err = filter.Le("xxxx", 0)
	assert.NotNil(t, err)
	_, err = filter.Btw("xxxx", 0, 0)
	assert.NotNil(t, err)

	// test summarizer
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	sum, cnt, err := summarizer.Sum("mock_0", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, sum, int64(45*4))
	assert.Equal(t, cnt, uint64(40))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 7)
	min, err := summarizer.Min("mock_0", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, min, int8(0))
	max, err := summarizer.Max("mock_0", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, max, int8(1))
	nullCnt, err := summarizer.NullCount("mock_0", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, nullCnt, uint64(0))
	cnt, err = summarizer.Count("mock_0", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(4))
	cnt, err = summarizer.Count("mock_0", nil)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(40))

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	sum, cnt, err = summarizer.Sum("mock_1", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, sum, int64(45*4))
	assert.Equal(t, cnt, uint64(40))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 7)
	min, err = summarizer.Min("mock_1", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, min, int16(0))
	max, err = summarizer.Max("mock_1", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, max, int16(1))
	nullCnt, err = summarizer.NullCount("mock_1", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, nullCnt, uint64(0))
	cnt, err = summarizer.Count("mock_1", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(4))
	cnt, err = summarizer.Count("mock_1", nil)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(40))

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	sum, cnt, err = summarizer.Sum("mock_2", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, sum, int64(45*4))
	assert.Equal(t, cnt, uint64(40))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 7)
	min, err = summarizer.Min("mock_2", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, min, int32(0))
	max, err = summarizer.Max("mock_2", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, max, int32(1))
	nullCnt, err = summarizer.NullCount("mock_2", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, nullCnt, uint64(0))
	cnt, err = summarizer.Count("mock_2", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(4))
	cnt, err = summarizer.Count("mock_2", nil)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(40))

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	sum, cnt, err = summarizer.Sum("mock_3", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, sum, int64(45*4))
	assert.Equal(t, cnt, uint64(40))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 7)
	min, err = summarizer.Min("mock_3", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, min, int64(0))
	max, err = summarizer.Max("mock_3", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, max, int64(1))
	nullCnt, err = summarizer.NullCount("mock_3", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, nullCnt, uint64(0))
	cnt, err = summarizer.Count("mock_3", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(4))
	cnt, err = summarizer.Count("mock_3", nil)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(40))

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	sum, cnt, err = summarizer.Sum("mock_4", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, sum, int64(45*4))
	assert.Equal(t, cnt, uint64(40))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 7)
	min, err = summarizer.Min("mock_4", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, min, uint8(0))
	max, err = summarizer.Max("mock_4", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, max, uint8(1))
	nullCnt, err = summarizer.NullCount("mock_4", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, nullCnt, uint64(0))
	cnt, err = summarizer.Count("mock_4", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(4))
	cnt, err = summarizer.Count("mock_4", nil)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(40))

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	sum, cnt, err = summarizer.Sum("mock_5", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, sum, int64(45*4))
	assert.Equal(t, cnt, uint64(40))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 7)
	min, err = summarizer.Min("mock_5", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, min, uint16(0))
	max, err = summarizer.Max("mock_5", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, max, uint16(1))
	nullCnt, err = summarizer.NullCount("mock_5", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, nullCnt, uint64(0))
	cnt, err = summarizer.Count("mock_5", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(4))
	cnt, err = summarizer.Count("mock_5", nil)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(40))

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	sum, cnt, err = summarizer.Sum("mock_6", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, sum, int64(45*4))
	assert.Equal(t, cnt, uint64(40))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 7)
	min, err = summarizer.Min("mock_6", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, min, uint32(0))
	max, err = summarizer.Max("mock_6", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, max, uint32(1))
	nullCnt, err = summarizer.NullCount("mock_6", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, nullCnt, uint64(0))
	cnt, err = summarizer.Count("mock_6", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(4))
	cnt, err = summarizer.Count("mock_6", nil)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(40))

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	sum, cnt, err = summarizer.Sum("mock_7", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, sum, int64(45*4))
	assert.Equal(t, cnt, uint64(40))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 7)
	min, err = summarizer.Min("mock_7", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, min, uint64(0))
	max, err = summarizer.Max("mock_7", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, max, uint64(1))
	nullCnt, err = summarizer.NullCount("mock_7", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, nullCnt, uint64(0))
	cnt, err = summarizer.Count("mock_7", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(4))
	cnt, err = summarizer.Count("mock_7", nil)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(40))

	// todo: float sum support
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 7)
	min, err = summarizer.Min("mock_8", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, min, float32(0))
	max, err = summarizer.Max("mock_8", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, max, float32(1))
	nullCnt, err = summarizer.NullCount("mock_8", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, nullCnt, uint64(0))
	cnt, err = summarizer.Count("mock_8", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(4))
	cnt, err = summarizer.Count("mock_8", nil)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(40))

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 7)
	min, err = summarizer.Min("mock_9", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, min, float64(0))
	max, err = summarizer.Max("mock_9", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, max, float64(1))
	nullCnt, err = summarizer.NullCount("mock_9", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, nullCnt, uint64(0))
	cnt, err = summarizer.Count("mock_9", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(4))
	cnt, err = summarizer.Count("mock_9", nil)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(40))

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	sum, cnt, err = summarizer.Sum("mock_10", mockBM)
	assert.Nil(t, err)
	_sum := int32(0)
	for i := 1; i <= 10; i++ {
		_sum += int32(types.FromCalendar(int32(i*100), 1, 1))
	}
	assert.Equal(t, sum, int64(_sum*4))
	assert.Equal(t, cnt, uint64(40))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 7)
	min, err = summarizer.Min("mock_10", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, min, int32(types.FromCalendar(400, 1, 1)))
	max, err = summarizer.Max("mock_10", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, max, int32(types.FromCalendar(700, 1, 1)))
	nullCnt, err = summarizer.NullCount("mock_10", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, nullCnt, uint64(0))
	cnt, err = summarizer.Count("mock_10", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(4))
	cnt, err = summarizer.Count("mock_10", nil)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(40))

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(0, 40)
	sum, cnt, err = summarizer.Sum("mock_11", mockBM)
	assert.Nil(t, err)
	sum_ := int64(0)
	for i := 1; i <= 10; i++ {
		sum_ += int64(types.FromClock(int32(i*100), 1, 1, 1, 1, 1, 1))
	}
	assert.Equal(t, sum, int64(sum_*4))
	assert.Equal(t, cnt, uint64(40))
	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 7)
	min, err = summarizer.Min("mock_11", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, min, int64(types.FromClock(400, 1, 1, 1, 1, 1, 1)))
	max, err = summarizer.Max("mock_11", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, max, int64(types.FromClock(700, 1, 1, 1, 1, 1, 1)))
	nullCnt, err = summarizer.NullCount("mock_11", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, nullCnt, uint64(0))
	cnt, err = summarizer.Count("mock_11", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(4))
	cnt, err = summarizer.Count("mock_11", nil)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(40))

	mockBM = roaring.NewBitmap()
	mockBM.AddRange(3, 7)
	min, err = summarizer.Min("mock_13", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, min, []byte("str0"))
	max, err = summarizer.Max("mock_13", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, max, []byte("str1"))
	nullCnt, err = summarizer.NullCount("mock_13", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, nullCnt, uint64(0))
	cnt, err = summarizer.Count("mock_13", mockBM)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(4))
	cnt, err = summarizer.Count("mock_13", nil)
	assert.Nil(t, err)
	assert.Equal(t, cnt, uint64(40))

	_, err = summarizer.Min("xxxx", nil)
	assert.NotNil(t, err)
	_, err = summarizer.Max("xxxx", nil)
	assert.NotNil(t, err)
	_, _, err = summarizer.Sum("xxxx", nil)
	assert.NotNil(t, err)
	_, err = summarizer.Count("xxxx", nil)
	assert.NotNil(t, err)
	_, err = summarizer.NullCount("xxxx", nil)
	assert.NotNil(t, err)

	inst.Close()
}

func TestCreateAndDropIndex(t *testing.T) {
	waitTime := time.Duration(100) * time.Millisecond
	if invariants.RaceEnabled {
		waitTime *= 2
	}
	initTestEnv(t)
	inst, gen, database := initTestDB3(t)
	schema := metadata.MockSchema(4)
	createCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema,
	}
	tblMeta, err := inst.CreateTable(createCtx)
	assert.Nil(t, err)
	assert.NotNil(t, tblMeta)
	tblName := schema.Name
	blkCnt := inst.Store.Catalog.Cfg.SegmentMaxBlocks
	rows := inst.Store.Catalog.Cfg.BlockMaxRows * blkCnt
	baseCk := mock.MockBatch(tblMeta.Schema.Types(), rows)

	insertCnt := uint64(2)

	var wg sync.WaitGroup
	{
		for i := uint64(0); i < insertCnt; i++ {
			wg.Add(1)
			go func() {
				appendCtx := CreateAppendCtx(database, gen, schema.Name, baseCk)
				inst.Append(appendCtx)
				wg.Done()
			}()
		}
	}
	wg.Wait()
	time.Sleep(waitTime)

	creater := func(i int) func() {
		return func() {
			indice := metadata.NewIndexSchema()
			indice.MakeIndex(fmt.Sprintf("idx-%d", i), metadata.NumBsi, i)
			ctx := &CreateIndexCtx{
				DBMutationCtx: *CreateDBMutationCtx(database, gen),
				Table:         tblName,
				Indices:       indice,
			}
			assert.Nil(t, inst.CreateIndex(ctx))
		}
	}

	dropper := func(s, i int) func() {
		return func() {
			indexNames := make([]string, 0)
			for j := s; j < i; j += 1 {
				indexNames = append(indexNames, fmt.Sprintf("idx-%d", j))
			}
			ctx := &DropIndexCtx{
				DBMutationCtx: *CreateDBMutationCtx(database, gen),
				Table:         tblName,
				IndexNames:    indexNames,
			}
			assert.Nil(t, inst.DropIndex(ctx))
		}
	}

	for i := 0; i < 4; i++ {
		creater(i)()
		if i == 2 {
			dropper(0, i)()
		}
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 2, tblMeta.GetIndexSchema().IndiceNum())
	for i := 0; i < 2; i++ {
		creater(i)()
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 4, tblMeta.GetIndexSchema().IndiceNum())
	dropper(0, 2)()
	assert.Equal(t, 2, tblMeta.GetIndexSchema().IndiceNum())
	for i := 0; i < 2; i++ {
		creater(i)()
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 4, tblMeta.GetIndexSchema().IndiceNum())
	dropper(0, 1)()
	assert.Equal(t, 3, tblMeta.GetIndexSchema().IndiceNum())
	creater(0)()
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 4, tblMeta.GetIndexSchema().IndiceNum())

	dataPath := filepath.Join(inst.Dir, "data")
	infos, err := ioutil.ReadDir(dataPath)
	assert.Nil(t, err)
	for _, info := range infos {
		bn := filepath.Base(info.Name())
		if fn, ok := common.ParseBitSlicedIndexFileName(bn); ok {
			if v, _, _, col, ok := common.ParseBitSlicedIndexFileNameToInfo(fn); ok {
				if col == 0 {
					assert.Equal(t, uint64(4), v)
				} else if col == 1 {
					assert.Equal(t, uint64(3), v)
				} else if col == 3 || col == 2 {
					assert.Equal(t, uint64(1), v)
				} else {
					t.Error("invalid column")
				}
			}
		}
	}

	inst.Close()

	inst, _, _ = initTestDB3(t)
	tblData, err := inst.Store.DataTables.WeakRefTable(tblMeta.Id)
	assert.Nil(t, err)
	tblMeta = tblData.GetMeta()
	//t.Log(tblData.GetIndexHolder().StringIndicesRefs())
	for _, segId := range tblMeta.SimpleGetSegmentIds() {
		segMeta := tblMeta.SimpleGetSegment(segId)
		segMeta.RLock()
		if !segMeta.IsSortedLocked() {
			segMeta.RUnlock()
			continue
		}
		segMeta.RUnlock()
		seg := tblData.StrongRefSegment(segId)
		holder := seg.GetIndexHolder()
		// 4 columns * (1 bsi + 1 zone map)
		assert.Equal(t, 8, holder.IndicesCount())
		seg.Unref()
	}

	for i := 0; i < 2; i++ {
		appendCtx := CreateAppendCtx(database, gen, schema.Name, baseCk)
		inst.Append(appendCtx)
		time.Sleep(waitTime)
	}

	time.Sleep(waitTime)

	for _, segId := range tblMeta.SimpleGetSegmentIds() {
		segMeta := tblMeta.SimpleGetSegment(segId)
		segMeta.RLock()
		if !segMeta.IsSortedLocked() {
			segMeta.RUnlock()
			continue
		}
		segMeta.RUnlock()
		seg := tblData.StrongRefSegment(segId)
		holder := seg.GetIndexHolder()
		assert.Equal(t, 8, holder.IndicesCount())
		seg.Unref()
	}

	inst.Close()
}

func TestRepeatCreateAndDropIndex(t *testing.T) {
	waitTime := time.Duration(100) * time.Millisecond
	if invariants.RaceEnabled {
		waitTime *= 2
	}
	initTestEnv(t)
	inst, gen, database := initTestDB1(t)
	schema := metadata.MockSchema(4)
	indice := metadata.NewIndexSchema()
	indice.MakeIndex("idx-0", metadata.NumBsi, 1)
	createCtx := &CreateTableCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Schema:        schema,
		Indice:        indice,
	}
	tblMeta, err := inst.CreateTable(createCtx)
	assert.Nil(t, err)
	assert.NotNil(t, tblMeta)
	tblName := schema.Name
	blkCnt := inst.Store.Catalog.Cfg.SegmentMaxBlocks
	rows := inst.Store.Catalog.Cfg.BlockMaxRows * blkCnt
	baseCk := mock.MockBatch(tblMeta.Schema.Types(), rows)

	insertCnt := uint64(3)

	var wg sync.WaitGroup
	{
		for i := uint64(0); i < insertCnt; i++ {
			wg.Add(1)
			appendCtx := CreateAppendCtx(database, gen, schema.Name, baseCk)
			inst.Append(appendCtx)
			wg.Done()
		}
	}
	wg.Wait()
	time.Sleep(waitTime)

	creater := func(i int) func() {
		return func() {
			indice := metadata.NewIndexSchema()
			indice.MakeIndex(fmt.Sprintf("idx-%d", i), metadata.NumBsi, i)
			ctx := &CreateIndexCtx{
				DBMutationCtx: *CreateDBMutationCtx(database, gen),
				Table:         tblName,
				Indices:       indice,
			}
			assert.Nil(t, inst.CreateIndex(ctx))
		}
	}

	dropper := func(s, i int) func() {
		return func() {
			indexNames := make([]string, 0)
			for j := s; j < i; j += 1 {
				indexNames = append(indexNames, fmt.Sprintf("idx-%d", j))
			}
			ctx := &DropIndexCtx{
				DBMutationCtx: *CreateDBMutationCtx(database, gen),
				Table:         tblName,
				IndexNames:    indexNames,
			}
			assert.Nil(t, inst.DropIndex(ctx))
		}
	}

	for i := 0; i < 10; i++ {
		creater(1)()
		dropper(1, 2)()
	}

	tblData, err := inst.GetTableData(tblMeta)
	assert.Nil(t, err)
	seg := tblData.StrongRefSegment(uint64(1))
	holder := seg.GetIndexHolder()
	//t.Log(holder.StringIndicesRefsNoLock())

	inst.Close()

	inst, _, _ = initTestDB2(t)
	tblData, err = inst.GetTableData(tblMeta)
	assert.Nil(t, err)
	seg = tblData.StrongRefSegment(uint64(1))
	holder = seg.GetIndexHolder()
	assert.Equal(t, 5, holder.IndicesCount())

	s := &db.Segment{
		Data: seg,
		Ids:  new(atomic.Value),
	}

	indice = metadata.NewIndexSchema()
	indice.MakeIndex("idx-1", metadata.NumBsi, 0)
	indice.MakeIndex("idx-2", metadata.NumBsi, 2)
	indice.MakeIndex("idx-3", metadata.NumBsi, 3)
	createIdxCtx := &CreateIndexCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Table:         tblMeta.Schema.Name,
		Indices:       indice,
	}
	assert.Nil(t, inst.CreateIndex(createIdxCtx))
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 4; i++ {
		column := fmt.Sprintf("mock_%d", i)
		filter := s.NewFilter()
		res, err := filter.Eq(column, int32(100))
		assert.Nil(t, err)
		assert.Equal(t, res.ToArray()[0], uint64(100))
		sparse := s.NewSparseFilter()
		ret, err := sparse.Eq(column, int32(100))
		assert.Nil(t, err)
		rets := decodeBlockIds(ret)
		assert.Equal(t, strconv.Itoa(1), rets[0])
		summ := s.NewSummarizer()
		count, err := summ.Count(column, nil)
		assert.Equal(t, uint64(4000), count)
	}

	dropIdxCtx := &DropIndexCtx{
		DBMutationCtx: *CreateDBMutationCtx(database, gen),
		Table:         tblMeta.Schema.Name,
		IndexNames:    []string{"idx-3"},
	}
	assert.Nil(t, inst.DropIndex(dropIdxCtx))
	time.Sleep(50 * time.Millisecond)

	filter := s.NewFilter()
	_, err = filter.Eq("mock_3", int32(1))
	assert.NotNil(t, err)
	_, err = filter.Ge("mock_2", int32(2))
	assert.Nil(t, err)

	inst.Close()
}

func matchStringArray(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 {
		return true
	}
	mapper := make(map[string]int)
	for _, e := range a {
		if _, ok := mapper[e]; ok {
			mapper[e] += 1
		} else {
			mapper[e] = 1
		}
	}
	for _, e := range b {
		if _, ok := mapper[e]; !ok {
			return false
		} else {
			mapper[e] -= 1
		}
	}
	return true
}
