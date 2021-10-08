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
	"math/rand"
	"matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/adaptor"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/internal/invariants"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v2"
	"matrixone/pkg/vm/engine/aoe/storage/mock"
	"matrixone/pkg/vm/engine/aoe/storage/testutils/config"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

var (
	TEST_DB_DIR = "/tmp/db_test"
)

func initDBTest() {
	os.RemoveAll(TEST_DB_DIR)
}

func initDB(ft storage.FactoryType) *DB {
	rand.Seed(time.Now().UnixNano())
	opts := config.NewCustomizedMetaOptions(TEST_DB_DIR, config.CST_Customize, uint64(2000), uint64(2))
	opts.FactoryType = ft
	inst, _ := Open(TEST_DB_DIR, opts)
	return inst
}

func TestCreateTable(t *testing.T) {
	initDBTest()
	inst := initDB(storage.NORMAL_FT)
	assert.NotNil(t, inst)
	defer inst.Close()
	tblCnt := rand.Intn(5) + 3
	prefix := "mocktbl_"
	var wg sync.WaitGroup
	names := make([]string, 0)
	for i := 0; i < tblCnt; i++ {
		tableInfo := adaptor.MockTableInfo(2)
		name := fmt.Sprintf("%s%d", prefix, i)
		names = append(names, name)
		wg.Add(1)
		go func(w *sync.WaitGroup) {
			_, err := inst.CreateTable(tableInfo, dbi.TableOpCtx{TableName: name})
			assert.Nil(t, err)
			w.Done()
		}(&wg)
	}
	wg.Wait()
	ids, err := inst.TableIDs()
	assert.Nil(t, err)
	assert.Equal(t, tblCnt, len(ids))
	for _, name := range names {
		assert.True(t, inst.HasTable(name))
	}
	t.Log(inst.Store.Catalog.PString(metadata.PPL0))
}

func TestCreateDuplicateTable(t *testing.T) {
	initDBTest()
	inst := initDB(storage.NORMAL_FT)
	defer inst.Close()

	tableInfo := adaptor.MockTableInfo(2)
	_, err := inst.CreateTable(tableInfo, dbi.TableOpCtx{TableName: "t1"})
	assert.Nil(t, err)
	_, err = inst.CreateTable(tableInfo, dbi.TableOpCtx{TableName: "t1"})
	assert.NotNil(t, err)
}

func TestDropEmptyTable(t *testing.T) {
	initDBTest()
	inst := initDB(storage.NORMAL_FT)
	defer inst.Close()
	tableInfo := adaptor.MockTableInfo(2)
	_, err := inst.CreateTable(tableInfo, dbi.TableOpCtx{TableName: tableInfo.Name})
	assert.Nil(t, err)
	_, err = inst.DropTable(dbi.DropTableCtx{TableName: tableInfo.Name})
	assert.Nil(t, err)
	time.Sleep(time.Duration(200) * time.Millisecond)
}

func TestDropTable(t *testing.T) {
	initDBTest()
	inst := initDB(storage.NORMAL_FT)
	defer inst.Close()

	name := "t1"
	tableInfo := adaptor.MockTableInfo(2)
	tid, err := inst.CreateTable(tableInfo, dbi.TableOpCtx{TableName: name})
	assert.Nil(t, err)

	ssCtx := &dbi.GetSnapshotCtx{
		TableName: name,
		Cols:      []int{0},
		ScanAll:   true,
	}

	ss, err := inst.GetSnapshot(ssCtx)
	assert.Nil(t, err)
	ss.Close()

	dropTid, err := inst.DropTable(dbi.DropTableCtx{TableName: name})
	assert.Nil(t, err)
	assert.Equal(t, tid, dropTid)

	ss, err = inst.GetSnapshot(ssCtx)
	assert.NotNil(t, err)
	assert.Nil(t, ss)

	tid2, err := inst.CreateTable(tableInfo, dbi.TableOpCtx{TableName: name})
	assert.Nil(t, err)
	assert.NotEqual(t, tid, tid2)

	ss, err = inst.GetSnapshot(ssCtx)
	assert.Nil(t, err)
	ss.Close()
}

func TestAppend(t *testing.T) {
	initDBTest()
	inst := initDB(storage.NORMAL_FT)
	tableInfo := adaptor.MockTableInfo(2)
	tid, err := inst.CreateTable(tableInfo, dbi.TableOpCtx{TableName: "mocktbl"})
	assert.Nil(t, err)
	tblMeta := inst.Store.Catalog.SimpleGetTable(tid)
	assert.NotNil(t, tblMeta)
	blkCnt := 2
	rows := inst.Store.Catalog.Cfg.BlockMaxRows * uint64(blkCnt)
	ck := mock.MockBatch(tblMeta.Schema.Types(), rows)
	assert.Equal(t, int(rows), ck.Vecs[0].Length())
	invalidName := "xxx"
	// err = inst.Append(invalidName, ck, logIdx)
	appendCtx := dbi.AppendCtx{
		OpIndex:   uint64(1),
		OpSize:    1,
		TableName: invalidName,
		Data:      ck,
	}
	err = inst.Append(appendCtx)
	assert.NotNil(t, err)
	insertCnt := 8
	appendCtx.TableName = tblMeta.Schema.Name
	for i := 0; i < insertCnt; i++ {
		err = inst.Append(appendCtx)
		assert.Nil(t, err)
		// tbl, err := inst.Store.DataTables.WeakRefTable(tid)
		// assert.Nil(t, err)
		// t.Log(tbl.GetCollumn(0).ToString(1000))
	}

	cols := []int{0, 1}
	tbl, _ := inst.Store.DataTables.WeakRefTable(tid)
	segIds := tbl.SegmentIds()
	ssCtx := &dbi.GetSnapshotCtx{
		TableName:  tableInfo.Name,
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
	inst.Close()
}

// TODO: When the capacity is not very big and the query concurrency is very high,
// the db will be stuck due to no more space. Need intruduce timeout mechanism later
func TestConcurrency(t *testing.T) {
	initDBTest()
	inst := initDB(storage.NORMAL_FT)
	tableInfo := adaptor.MockTableInfo(2)
	tid, err := inst.CreateTable(tableInfo, dbi.TableOpCtx{TableName: "mockcon"})
	assert.Nil(t, err)
	tblMeta := inst.Store.Catalog.SimpleGetTable(tid)
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
				TableName: tableInfo.Name,
				Data:      baseCk,
				OpIndex:   uint64(i),
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
				TableName:  tableInfo.Name,
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
	time.Sleep(time.Duration(100) * time.Millisecond)
	if invariants.RaceEnabled {
		time.Sleep(time.Duration(250) * time.Millisecond)
	}
	tbl, _ := inst.Store.DataTables.WeakRefTable(tid)
	root := tbl.WeakRefRoot()
	assert.Equal(t, int64(1), root.RefCount())
	opts := &dbi.GetSnapshotCtx{
		TableName: tableInfo.Name,
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
	inst := initDB(storage.NORMAL_FT)
	prefix := "mtable"
	tblCnt := 40
	var names []string
	for i := 0; i < tblCnt; i++ {
		name := fmt.Sprintf("%s_%d", prefix, i)
		tInfo := adaptor.MockTableInfo(2)
		_, err := inst.CreateTable(tInfo, dbi.TableOpCtx{TableName: name})
		assert.Nil(t, err)
		names = append(names, name)
	}
	tblMeta := inst.Store.Catalog.SimpleGetTableByName(names[0])
	assert.NotNil(t, tblMeta)
	rows := uint64(tblMeta.Catalog.Cfg.BlockMaxRows / 2)
	baseCk := mock.MockBatch(tblMeta.Schema.Types(), rows)
	p1, _ := ants.NewPool(10)
	p2, _ := ants.NewPool(10)
	attrs := []string{}
	for _, colDef := range tblMeta.Schema.ColDefs {
		attrs = append(attrs, colDef.Name)
	}
	refs := make([]uint64, len(attrs))

	tnames := inst.TableNames()
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
					rel, err := inst.Relation(tname)
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
								assert.True(t, v.Length() <= int(tblMeta.Catalog.Cfg.BlockMaxRows))
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
			rel, err := inst.Relation(name)
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
	tbls := inst.Store.Catalog.SimpleGetTablesByPrefix(prefix)
	assert.Equal(t, tblCnt, len(tbls))
	inst.Close()
}

func TestDropTable2(t *testing.T) {
	initDBTest()
	inst := initDB(storage.NORMAL_FT)
	tableInfo := adaptor.MockTableInfo(2)
	tid, err := inst.CreateTable(tableInfo, dbi.TableOpCtx{TableName: "mockcon"})
	assert.Nil(t, err)
	blkCnt := inst.Store.Catalog.Cfg.SegmentMaxBlocks
	rows := inst.Store.Catalog.Cfg.BlockMaxRows * blkCnt
	tblMeta := inst.Store.Catalog.SimpleGetTable(tid)
	assert.NotNil(t, tblMeta)
	baseCk := mock.MockBatch(tblMeta.Schema.Types(), rows)

	insertCnt := uint64(1)

	var wg sync.WaitGroup
	{
		for i := uint64(0); i < insertCnt; i++ {
			wg.Add(1)
			go func() {
				inst.Append(dbi.AppendCtx{
					TableName: tableInfo.Name,
					Data:      baseCk,
					OpIndex:   uint64(1),
					OpSize:    1,
				})
				wg.Done()
			}()
		}
	}
	wg.Wait()
	time.Sleep(time.Duration(100) * time.Millisecond)
	tbl, _ := inst.Store.DataTables.WeakRefTable(tid)
	t.Log(tbl.String())

	t.Log(inst.MTBufMgr.String())
	t.Log(inst.SSTBufMgr.String())
	if inst.Opts.FactoryType == storage.NORMAL_FT {
		assert.Equal(t, int(blkCnt*insertCnt*2), inst.SSTBufMgr.NodeCount()+inst.MTBufMgr.NodeCount())
	}
	cols := make([]int, 0)
	for i := 0; i < len(tblMeta.Schema.ColDefs); i++ {
		cols = append(cols, i)
	}
	opts := &dbi.GetSnapshotCtx{
		TableName: tableInfo.Name,
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
	inst.DropTable(dbi.DropTableCtx{TableName: tableInfo.Name, OnFinishCB: dropCB})
	time.Sleep(time.Duration(50) * time.Millisecond)
	if inst.Opts.FactoryType == storage.NORMAL_FT {
		assert.Equal(t, int(blkCnt*insertCnt*2), inst.SSTBufMgr.NodeCount()+inst.MTBufMgr.NodeCount())
	}
	ss.Close()
	time.Sleep(time.Duration(50) * time.Millisecond)
	t.Log(inst.MTBufMgr.String())
	t.Log(inst.SSTBufMgr.String())
	t.Log(inst.IndexBufMgr.String())
	t.Log(inst.MemTableMgr.String())
	assert.Equal(t, 0, inst.SSTBufMgr.NodeCount()+inst.MTBufMgr.NodeCount())
	err = <-doneCh
	assert.Equal(t, expectErr, err)
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
	inst := initDB(storage.NORMAL_FT)
	tableInfo := adaptor.MockTableInfo(2)
	tid, err := inst.CreateTable(tableInfo, dbi.TableOpCtx{TableName: "mockcon"})
	assert.Nil(t, err)
	tblMeta := inst.Store.Catalog.SimpleGetTable(tid)
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
					TableName: tableInfo.Name,
					Data:      baseCk,
					OpIndex:   uint64(1),
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

	_, err = inst.DropTable(dbi.DropTableCtx{TableName: tableInfo.Name})
	assert.Nil(t, err)
	time.Sleep(waitTime / 2)

	t.Log(inst.FsMgr.String())
	t.Log(inst.MTBufMgr.String())
	t.Log(inst.SSTBufMgr.String())
	t.Log(inst.IndexBufMgr.String())
	inst.Close()
}
