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
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/common/helper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	"github.com/panjf2000/ants/v2"
)

var sampleDir = "/tmp/sample3"
var txnBufSize = common.G
var mutBufSize = common.G
var dbName = "db"
var cpuprofile = "/tmp/sample3/cpuprofile"
var memprofile = "/tmp/sample3/memprofile"

func init() {
	os.RemoveAll(sampleDir)
}

func startProfile() {
	f, _ := os.Create(cpuprofile)
	pprof.StartCPUProfile(f)
}

func stopProfile() {
	pprof.StopCPUProfile()
	memf, _ := os.Create(memprofile)
	defer memf.Close()
	pprof.Lookup("heap").WriteTo(memf, 0)
}

func main() {
	tae, _ := db.Open(sampleDir, nil)
	defer tae.Close()

	var schema *catalog.Schema
	{
		txn := tae.StartTxn(nil)
		eng := moengine.NewEngine(txn)
		err := eng.Create(0, dbName, 0)
		if err != nil {
			panic(err)
		}
		db, err := eng.Database(dbName)
		if err != nil {
			panic(err)
		}
		tblInfo := moengine.MockTableInfo(4)
		tblInfo.Columns[0].PrimaryKey = true
		_, _, _, _, defs, _ := helper.UnTransfer(*tblInfo)
		err = db.Create(0, tblInfo.Name, defs)
		{
			db, _ := txn.GetDatabase(dbName)
			rel, _ := db.GetRelationByName(tblInfo.Name)
			schema = rel.GetMeta().(*catalog.TableEntry).GetSchema()
		}
		if err := txn.Commit(); err != nil {
			panic(err)
		}
	}
	batchCnt := uint64(100)
	batchRows := uint64(10000) * 1 / 2 * batchCnt
	logutil.Info(tae.Opts.Catalog.SimplePPString(common.PPL1))
	bat := compute.MockBatch(schema.Types(), batchRows, int(schema.PrimaryKey), nil)
	bats := compute.SplitBatch(bat, int(batchCnt))
	var wg sync.WaitGroup
	doAppend := func(b *batch.Batch) func() {
		return func() {
			defer wg.Done()
			txn := tae.StartTxn(nil)
			// {
			// 	db, _ := txn.GetDatabase(dbName)
			// 	rel, _ := db.GetRelationByName(schema.Name)
			// }
			eng := moengine.NewEngine(txn)
			db, err := eng.Database(dbName)
			if err != nil {
				panic(err)
			}
			rel, err := db.Relation(schema.Name)
			if err != nil {
				panic(err)
			}
			if err := rel.Write(0, b); err != nil {
				panic(err)
			}
			if err := txn.Commit(); err != nil {
				panic(err)
			}
		}
	}
	p, _ := ants.NewPool(10)
	now := time.Now()
	startProfile()
	for _, b := range bats {
		wg.Add(1)
		p.Submit(doAppend(b))
	}
	wg.Wait()
	stopProfile()
	logutil.Infof("Append takes: %s", time.Since(now))
	{
		txn := tae.StartTxn(nil)
		eng := moengine.NewEngine(txn)
		db, err := eng.Database(dbName)
		if err != nil {
			panic(err)
		}
		rel, err := db.Relation(schema.Name)
		if err != nil {
			panic(err)
		}
		readProc := func(reader engine.Reader) {
			defer wg.Done()
			for {
				bat, err := reader.Read([]uint64{uint64(1)}, []string{schema.ColDefs[0].Name})
				if err != nil {
					panic(err)
				}
				if bat == nil {
					break
				}
				logutil.Infof("bat rows: %d", vector.Length(bat.Vecs[0]))
			}
		}

		parallel := 10
		readers := rel.NewReader(parallel, nil, nil)
		for _, reader := range readers {
			wg.Add(1)
			go readProc(reader)
		}
		wg.Wait()
		if err = txn.Commit(); err != nil {
			panic(err)
		}
	}
	logutil.Info(tae.Opts.Catalog.SimplePPString(common.PPL1))
}
