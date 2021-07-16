package main

import (
	"matrixone/pkg/vm/engine/aoe"
	e "matrixone/pkg/vm/engine/aoe/storage"
	// "matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/db"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

func main() {
	workDir := "/tmp/myDemo"
	os.RemoveAll(workDir)
	colCnt := 4
	metaConf := &md.Configuration{
		Dir:              workDir,
		BlockMaxRows:     10000,
		SegmentMaxBlocks: 10,
	}
	cacheCfg := &e.CacheCfg{
		IndexCapacity:  10000,
		InsertCapacity: metaConf.BlockMaxRows * uint64(colCnt) * 100,
		DataCapacity:   metaConf.BlockMaxRows * metaConf.SegmentMaxBlocks * uint64(colCnt) * 2,
	}
	opts := e.Options{CacheCfg: cacheCfg}
	opts.Meta.Conf = metaConf
	inst, err := db.Open(workDir, &opts)
	if err != nil {
		panic(err)
	}

	tableInfo := md.MockTableInfo(colCnt)
	tabletInfo := aoe.TabletInfo{
		Name:  tableInfo.Name,
		Table: *tableInfo,
	}
	tName := tabletInfo.Name
	_, err = inst.CreateTable(&tabletInfo)
	if err != nil {
		panic(err)
	}
	rows := metaConf.BlockMaxRows / 8
	tblMeta, err := inst.Opts.Meta.Info.ReferenceTableByName(tName)
	ck := chunk.MockBatch(tblMeta.Schema.Types(), rows)
	logIdx := &md.LogIndex{
		ID:       uint64(0),
		Capacity: uint64(ck.Vecs[0].Length()),
	}
	cols := make([]int, 0)
	for i := 0; i < len(tblMeta.Schema.ColDefs); i++ {
		cols = append(cols, i)
	}

	var (
		insertWg sync.WaitGroup
		searchWg sync.WaitGroup
	)
	insertCnt := int(float32(metaConf.SegmentMaxBlocks*10) * (float32(rows) / float32(metaConf.BlockMaxRows)))
	insertWg.Add(1)
	go func() {
		for i := 0; i < insertCnt; i++ {
			err = inst.Append(tName, ck, logIdx)
			if err != nil {
				log.Warn(err)
			}
		}
		insertWg.Done()
	}()
	ctx := dbi.GetSnapshotCtx{ScanAll: true, TableName: tName, Cols: cols}

	doScan := func() {
		ss, err := inst.GetSnapshot(&ctx)
		if err != nil {
			log.Warn(err)
			return
		}
		segIt := ss.NewIt()
		for segIt.Valid() {
			segment := segIt.GetHandle()
			blkIt := segment.NewIt()
			for blkIt.Valid() {
				block := blkIt.GetHandle()
				hh := block.Prefetch()
				vec := hh.GetReaderByAttr(1)
				if vec.Length() >= 2 {
					log.Infof("vec[1]=%s, %d", vec.GetValue(1), vec.GetType())
				}
				hh.Close()
				blkIt.Next()
			}
			blkIt.Close()
			segIt.Next()
		}
		segIt.Close()

		ss.Close()
	}
	searchWg.Add(1)
	go func() {
		for i := 0; i < 10; i++ {
			time.Sleep(time.Duration(1) * time.Millisecond)
			doScan()
		}
		searchWg.Done()
	}()
	insertWg.Wait()

	time.Sleep(time.Duration(20) * time.Millisecond)
	_, err = inst.DropTable(tName)
	log.Infof("drop err: %v", err)
	searchWg.Wait()
	// time.Sleep(time.Duration(100) * time.Millisecond)
	doScan()
	log.Info(inst.IndexBufMgr.String())
	log.Info(inst.MTBufMgr.String())
	log.Info(inst.SSTBufMgr.String())

	// time.Sleep(time.Duration(10) * time.Millisecond)
	// _, err = inst.DropTablet(tName)

	time.Sleep(time.Duration(100) * time.Millisecond)
	log.Info(inst.IndexBufMgr.String())
	log.Info(inst.MTBufMgr.String())
	log.Info(inst.SSTBufMgr.String())
	log.Infof("drop err: %v", err)
	inst.Close()
}
