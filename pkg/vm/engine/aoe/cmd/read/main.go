package main

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine/aoe"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/db"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/process"
	"os"
	"runtime/pprof"
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
)

func init() {

	hm := host.New(1 << 40)
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
		InsertCapacity: blockRows * uint64(colCnt) * 2000,
		DataCapacity:   blockRows * uint64(colCnt) * 2000,
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
		index := md.LogIndex{Capacity: uint64(ibat.Vecs[0].Length())}
		if err := impl.Append(tableName, ibat, &index); err != nil {
			panic(err)
		}
	}
	waitTime := insertCnt * uint64(ibat.Vecs[0].Length()) * uint64(colCnt) / uint64(400000000) * 5000
	time.Sleep(time.Duration(waitTime) * time.Millisecond)
}

func mockData() {
	doRemove()
	impl := makeDB()
	creatTable(impl)
	makeFiles(impl)
	impl.Close()
}

func readData() {
	impl := makeDB()
	rel, err := impl.Relation(tableName)
	if err != nil {
		panic(err)
	}
	tblMeta, _ := impl.Opts.Meta.Info.ReferenceTableByName(tableName)
	attrs := []string{}
	cols := make([]int, 0)
	for i, colDef := range tblMeta.Schema.ColDefs {
		attrs = append(attrs, colDef.Name)
		cols = append(cols, i)
	}
	refs := make([]uint64, len(attrs))
	segInfos := rel.Segments()

	totalRows := uint64(0)
	startProfile()
	defer stopProfile()
	now := time.Now()
	var wg sync.WaitGroup
	for _, segInfo := range segInfos {
		seg := rel.Segment(segInfo, proc)
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
						defer wg.Done()
						v, err := bat.GetVector(col, proc)
						if err != nil {
							panic(err)
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
	log.Info(common.GPool.String())
}

func main() {
	mockData()
	// readData()
}
