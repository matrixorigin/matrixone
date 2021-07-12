package engine

import (
	"context"
	"math/rand"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine/aoe"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/db"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/process"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/panjf2000/ants/v2"

	"github.com/stretchr/testify/assert"
)

var (
	DIR = "/tmp/engine_test"
)

func initDBTest() {
	os.RemoveAll(DIR)
}

func initDB() *Engine {
	rand.Seed(time.Now().UnixNano())
	cfg := &md.Configuration{
		Dir:              DIR,
		SegmentMaxBlocks: 2,
		BlockMaxRows:     20000,
	}
	opts := &e.Options{}
	opts.Meta.Conf = cfg
	inst, _ := db.Open(DIR, opts)
	eng := &Engine{
		DBImpl: inst,
	}
	return eng
}

type InsertReq struct {
	Name     string
	Data     *batch.Batch
	LogIndex *md.LogIndex
}

func TestEngine(t *testing.T) {
	initDBTest()
	eng := initDB()
	tableInfo := md.MockTableInfo(2)
	tablet := aoe.TabletInfo{Table: *tableInfo, Name: "mockcon"}
	tid, err := eng.DBImpl.CreateTable(&tablet)
	assert.Nil(t, err)
	tblMeta, err := eng.DBImpl.Opts.Meta.Info.ReferenceTable(tid)
	assert.Nil(t, err)
	blkCnt := eng.DBImpl.Store.MetaInfo.Conf.SegmentMaxBlocks
	rows := eng.DBImpl.Store.MetaInfo.Conf.BlockMaxRows * blkCnt
	baseCk := chunk.MockBatch(tblMeta.Schema.Types(), rows)
	insertCh := make(chan *InsertReq)
	searchCh := make(chan *dbi.GetSnapshotCtx)

	p, _ := ants.NewPool(40)
	attrs := []string{}
	cols := make([]int, 0)
	for i, colDef := range tblMeta.Schema.ColDefs {
		attrs = append(attrs, colDef.Name)
		cols = append(cols, i)
	}
	refs := make([]uint64, len(attrs))
	hm := host.New(1 << 40)
	gm := guest.New(1<<40, hm)
	proc := process.New(gm, mempool.New(1<<48, 8))

	reqCtx, cancel := context.WithCancel(context.Background())
	var (
		loopWg   sync.WaitGroup
		searchWg sync.WaitGroup
		loadCnt  uint32
	)
	dbase, err := eng.Database(DefaultDatabase)
	assert.Nil(t, err)
	task := func(ctx *dbi.GetSnapshotCtx) func() {
		return func() {
			defer searchWg.Done()
			rel, err := dbase.Relation(tblMeta.Schema.Name)
			assert.Nil(t, err)
			for _, segInfo := range rel.Segments() {
				seg := rel.Segment(segInfo, proc)
				for _, id := range seg.Blocks() {
					blk := seg.Block(id, proc)
					bat, err := blk.Prefetch(refs, attrs, proc)
					assert.Nil(t, err)
					for _, attr := range attrs {
						bat.GetVector(attr, proc)
						atomic.AddUint32(&loadCnt, uint32(1))
					}
				}
			}
			rel.(*Relation).Close()
		}
	}
	assert.NotNil(t, task)
	task2 := func(ctx *dbi.GetSnapshotCtx) func() {
		return func() {
			defer searchWg.Done()
			ss, err := eng.DBImpl.GetSnapshot(ctx)
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
					rel, err := dbase.Relation(req.Name)
					assert.Nil(t, err)
					err = rel.Write(req.Data)
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
			req := &InsertReq{
				Name:     tablet.Name,
				Data:     baseCk,
				LogIndex: &md.LogIndex{ID: uint64(i), Capacity: uint64(baseCk.Vecs[0].Length())},
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
				TableName: tablet.Name,
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
	t.Log(eng.DBImpl.WorkersStatsString())
	t.Log(eng.DBImpl.MTBufMgr.String())
	t.Log(eng.DBImpl.SSTBufMgr.String())
	t.Log(eng.DBImpl.MemTableMgr.String())
	t.Logf("Load: %d", loadCnt)
	tbl, _ := eng.DBImpl.Store.DataTables.WeakRefTable(tid)
	assert.Equal(t, tbl.GetRowCount(), rows*uint64(insertCnt))
	t.Log(tbl.GetRowCount())
	attr := tblMeta.Schema.ColDefs[0].Name
	t.Log(tbl.Size(attr))
	attr = tblMeta.Schema.ColDefs[1].Name
	t.Log(tbl.Size(attr))
	t.Log(tbl.String())
	eng.DBImpl.Close()
}
