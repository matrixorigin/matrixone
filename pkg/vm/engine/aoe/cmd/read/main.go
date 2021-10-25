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

package main

import (
	"bytes"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/local"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/adaptor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/dbi"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
	w "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/worker"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

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
	opts     = &storage.Options{}
	table    *aoe.TableInfo
	readPool *ants.Pool
	proc     *process.Process
	hm       *host.Mmu
)

func init() {
	readPool, _ = ants.NewPool(readPoolSize)
	mdCfg := &storage.MetaCfg{
		SegmentMaxBlocks: blockCntPerSegment,
		BlockMaxRows:     blockRows,
	}
	opts.CacheCfg = &storage.CacheCfg{
		IndexCapacity:  blockRows * blockCntPerSegment * 80,
		InsertCapacity: blockRows * uint64(colCnt) * 400,
		DataCapacity:   blockRows * uint64(colCnt) * 400,
	}
	opts.MetaCleanerCfg = &storage.MetaCleanerCfg{
		Interval: time.Duration(1) * time.Second,
	}
	opts.Meta.Conf = mdCfg
	opts.WalRole = wal.HolderRole
	info := adaptor.MockTableInfo(colCnt)
	table = info
}

func getInsertBatch(meta *metadata.Table) *batch.Batch {
	bat := mock.MockBatch(meta.Schema.Types(), batchInsertRows)
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
	meta := impl.Opts.Meta.Catalog.SimpleGetTableByName(tableName)
	if meta == nil {
		panic(metadata.TableNotFoundErr)
	}
	ibat := getInsertBatch(meta)
	for i := uint64(0); i < insertCnt; i++ {
		if err := impl.Append(dbi.AppendCtx{TableName: tableName, Data: ibat, OpIndex: uint64(i) + 1, OpSize: 1}); err != nil {
			panic(err)
		}
	}
	waitTime := insertCnt * uint64(ibat.Vecs[0].Length()) * uint64(colCnt) / uint64(400000000) * 20000
	time.Sleep(time.Duration(waitTime) * time.Millisecond)
}

func mockData() {
	doRemove()
	impl := makeDB()
	creatTable(impl)
	makeFiles(impl)
	log.Info(common.GPool.String())
	// {
	// 	time.Sleep(time.Duration(4) * time.Second)
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
	id, _ := impl.GetSegmentedId(*dbi.NewTabletSegmentedIdCtx(tableName))
	log.Infof("segmented id: %d", id)
	rel, err := dbase.Relation(tableName)
	if err != nil {
		panic(err)
	}
	tblMeta := impl.Opts.Meta.Catalog.SimpleGetTableByName(tableName)
	var attrs []string
	cols := make([]int, 0)
	for i, colDef := range tblMeta.Schema.ColDefs {
		attrs = append(attrs, colDef.Name)
		cols = append(cols, i)
	}
	refs := make([]uint64, len(attrs))
	var segIds dbi.IDS
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
			blk.Prefetch(attrs)
			if err != nil {
				panic(err)
			}
			for coli, attr := range attrs {
				wg.Add(1)
				f := func(src engine.Block, i int, col string) func() {
					return func() {
						defer wg.Done()
						c := bytes.NewBuffer(make([]byte, 0))
						dc := bytes.NewBuffer(make([]byte, 0))
						gBat, err := src.Read(refs[i:i+1], attrs[i:i+1], []*bytes.Buffer{c}, []*bytes.Buffer{dc})
						if err != nil {
							panic(err)
						}
						atomic.AddUint64(&totalRows, uint64(gBat.Vecs[0].Length()))
					}
				}
				readPool.Submit(f(blk, coli, attr))
			}
		}
	}
	wg.Wait()
	rel.Close()
	log.Infof("Time: %s, Rows: %d", time.Since(now), totalRows)
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
