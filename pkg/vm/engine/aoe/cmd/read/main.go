package main

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/engine/aoe/local"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/db"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	w "matrixone/pkg/vm/engine/aoe/storage/worker"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/process"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/panjf2000/ants/v2"
)

const (
	blockRows          uint64 = 10000
	blockCntPerSegment uint64 = 4
	workDir            string = "/tmp/readtest"
	tableName          string = "mocktbl"

	colCnt     int    = 2000
	insertRows uint64 = blockRows * blockCntPerSegment * 10

	insertCnt       uint64 = 20
	batchInsertRows uint64 = insertRows / insertCnt
	readPoolSize    int    = 40

	cpuprofile string = "/tmp/readcpufile"
)

var (
	opts     = &e.Options{}
	table    *aoe.TableInfo
	readPool *ants.Pool
	proc     *process.Process
	hm       *host.Mmu
)

func init() {

	hm = host.New(1 << 48)
	gm := guest.New(1<<40, hm)
	proc = process.New(gm, mempool.New(1<<48, 8))
	readPool, _ = ants.NewPool(readPoolSize)
	mdCfg := &md.Configuration{
		Dir:              workDir,
		SegmentMaxBlocks: blockCntPerSegment,
		BlockMaxRows:     blockRows,
	}
	opts.CacheCfg = &e.CacheCfg{
		IndexCapacity:  blockRows * blockCntPerSegment * 80,
		InsertCapacity: blockRows * uint64(colCnt) * 400,
		DataCapacity:   blockRows * uint64(colCnt) * 400,
	}
	opts.MetaCleanerCfg = &e.MetaCleanerCfg{
		Interval: time.Duration(1) * time.Second,
	}
	opts.Meta.Conf = mdCfg
	info := md.MockTableInfo(colCnt)
	table = info
}

func getInsertBatch(meta *md.Table) *batch.Batch {
	bat := chunk.MockBatch(meta.Schema.Types(), batchInsertRows)
	return bat
}

func startProfile() {
	f, _ := os.Create(cpuprofile)
	pprof.StartCPUProfile(f)
}

func stopProfile() {
	pprof.StopCPUProfile()
}

func makeDB() *db.DB {
	impl, _ := db.Open(workDir, opts)
	return impl
}

func creatTable(impl *db.DB) {
	_, err := impl.CreateTable(table, dbi.TableOpCtx{TableName: table.Name})
	if err != nil {
		panic(err)
	}
}

func doRemove() {
	os.RemoveAll(workDir)
}

func makeFiles(impl *db.DB) {
	meta, err := impl.Opts.Meta.Info.ReferenceTableByName(tableName)
	if err != nil {
		panic(err)
	}
	ibat := getInsertBatch(meta)
	for i := uint64(0); i < insertCnt; i++ {
		if err := impl.Append(dbi.AppendCtx{TableName: tableName, Data: ibat, OpIndex: uint64(i)}); err != nil {
			panic(err)
		}
	}
	waitTime := insertCnt * uint64(ibat.Vecs[0].Length()) * uint64(colCnt) / uint64(400000000) * 6000
	time.Sleep(time.Duration(waitTime) * time.Millisecond)
}

func mockData() {
	doRemove()
	impl := makeDB()
	creatTable(impl)
	makeFiles(impl)
	// {
	// 	time.Sleep(time.Duration(4) * time.Second)
	// 	log.Info(common.GPool.String())
	// 	time.Sleep(time.Duration(100) * time.Second)
	// }
	impl.Close()
}

func readData() {
	impl := makeDB()
	localEngine := local.NewLocalRoEngine(impl)
	dbase, err := localEngine.Database("")
	if err != nil {
		panic(err)
	}
	rel, err := dbase.Relation(tableName)
	if err != nil {
		panic(err)
	}
	tblMeta, _ := impl.Opts.Meta.Info.ReferenceTableByName(tableName)
	var attrs []string
	cols := make([]int, 0)
	for i, colDef := range tblMeta.Schema.ColDefs {
		attrs = append(attrs, colDef.Name)
		cols = append(cols, i)
	}
	refs := make([]uint64, len(attrs))
	var segIds db.IDS
	{
		dbrel, _ := impl.Relation(tableName)
		segIds = dbrel.SegmentIds()
		dbrel.Close()
	}

	totalRows := uint64(0)
	startProfile()
	defer stopProfile()
	now := time.Now()
	var wg sync.WaitGroup
	for _, segId := range segIds.Ids {
		idstr := strconv.FormatUint(segId, 10)
		seg := rel.Segment(engine.SegmentInfo{Id: idstr}, proc)
		for _, id := range seg.Blocks() {
			blk := seg.Block(id, proc)
			bat, err := blk.Prefetch(refs, attrs, proc)
			if err != nil {
				panic(err)
			}
			for _, attr := range attrs {
				wg.Add(1)
				f := func(b *batch.Batch, col string) func() {
					return func() {
						gm := guest.New(1<<48, hm)
						proc2 := process.New(gm, mempool.New(1<<48, 8))
						defer wg.Done()
						v, err := bat.GetVector(col, proc2)
						if err != nil {
							panic(err)
						}
						if v != nil {
							v.Free(proc2)
						}
						atomic.AddUint64(&totalRows, uint64(v.Length()))
					}
				}
				readPool.Submit(f(bat, attr))
			}
		}
	}
	wg.Wait()
	log.Infof("Time: %s, Rows: %d", time.Since(now), totalRows)
	log.Infof("MMU usage: %d", hm.Size())
	// {
	// 	time.Sleep(time.Duration(4) * time.Second)
	// 	log.Info(common.GPool.String())
	// 	time.Sleep(time.Duration(100) * time.Second)
	// }
}

type gcHandle struct{}

// func (h *gcHandle) OnExec()    { runtime.GC() }
func (h *gcHandle) OnExec()    {}
func (h *gcHandle) OnStopped() { runtime.GC() }

func main() {
	go func() {
		http.ListenAndServe(":8080", nil)
	}()
	gc := w.NewHeartBeater(time.Duration(1)*time.Second, &gcHandle{})
	gc.Start()

	mockData()
	// readData()

	gc.Stop()
}
