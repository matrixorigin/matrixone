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
	"matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/adaptor"
	"matrixone/pkg/vm/engine/aoe/storage/db"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/mock"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

func main() {
	workDir := "/tmp/myDemo"
	os.RemoveAll(workDir)
	colCnt := 4
	metaConf := &storage.MetaCfg{
		BlockMaxRows:     10000,
		SegmentMaxBlocks: 2,
	}
	opts := storage.Options{}
	opts.Meta.Conf = metaConf
	inst, err := db.Open(workDir, &opts)
	if err != nil {
		panic(err)
	}

	tableInfo := adaptor.MockTableInfo(colCnt)
	tName := tableInfo.Name
	_, err = inst.CreateTable(tableInfo, dbi.TableOpCtx{TableName: tName})
	if err != nil {
		panic(err)
	}
	rows := metaConf.BlockMaxRows / 8
	tblMeta := inst.Opts.Meta.Catalog.SimpleGetTableByName(tName)
	ck := mock.MockBatch(tblMeta.Schema.Types(), rows)
	cols := make([]int, 0)
	for i := 0; i < len(tblMeta.Schema.ColDefs); i++ {
		cols = append(cols, i)
	}

	var (
		insertWg sync.WaitGroup
		searchWg sync.WaitGroup
	)
	insertCnt := 16 * int(float32(metaConf.SegmentMaxBlocks*10)*(float32(rows)/float32(metaConf.BlockMaxRows)))
	insertWg.Add(1)
	go func() {
		for i := 0; i < insertCnt; i++ {
			err = inst.Append(dbi.AppendCtx{TableName: tName, Data: ck, OpIndex: uint64(i), OpSize: 1})
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
				vec, err := hh.GetReaderByAttr(1)
				if err != nil {
					panic(err)
				}
				if vec.Length() >= 2 {
					val, err := vec.GetValue(1)
					if err != nil {
						panic(err)
					}
					log.Infof("vec[1]=%s, %d", val, vec.GetType())
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
	_, err = inst.DropTable(dbi.DropTableCtx{TableName: tName})
	log.Infof("drop err: %v", err)
	searchWg.Wait()
	// time.Sleep(time.Duration(100) * time.Millisecond)
	doScan()
	log.Info(inst.IndexBufMgr.String())
	log.Info(inst.MTBufMgr.String())
	log.Info(inst.SSTBufMgr.String())

	// time.Sleep(time.Duration(10) * time.Millisecond)
	// _, err = inst.DropTablet(tName)

	time.Sleep(time.Duration(200) * time.Millisecond)
	log.Info(inst.IndexBufMgr.String())
	log.Info(inst.MTBufMgr.String())
	log.Info(inst.SSTBufMgr.String())
	log.Info(inst.FsMgr.String())
	log.Infof("drop err: %v", err)
	inst.Close()
}
