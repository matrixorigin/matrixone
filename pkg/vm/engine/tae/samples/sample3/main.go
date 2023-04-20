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
	"context"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	"github.com/panjf2000/ants/v2"
)

var sampleDir = "/tmp/sample3"
var dbName = "db"
var cpuprofile = "/tmp/sample3/cpuprofile"
var memprofile = "/tmp/sample3/memprofile"

func init() {
	os.RemoveAll(sampleDir)
}

func startProfile() {
	f, _ := os.Create(cpuprofile)
	if err := pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}
}

func stopProfile() {
	pprof.StopCPUProfile()
	memf, _ := os.Create(memprofile)
	defer memf.Close()
	_ = pprof.Lookup("heap").WriteTo(memf, 0)
}

func main() {
	tae, _ := db.Open(sampleDir, nil)
	defer tae.Close()
	eng := moengine.NewEngine(tae)

	ctx := context.TODO()
	schema := catalog.MockSchema(13, 12)
	{
		txn, _ := eng.StartTxn(nil)
		txnOperator := moengine.TxnToTxnOperator(txn)
		err := eng.Create(ctx, dbName, txnOperator)
		if err != nil {
			panic(err)
		}
		db, err := eng.Database(ctx, dbName, txnOperator)
		if err != nil {
			panic(err)
		}
		defs, _ := moengine.SchemaToDefs(schema)
		err = db.Create(ctx, schema.Name, defs)
		if err != nil {
			panic(err)
		}
		if err := txn.Commit(); err != nil {
			panic(err)
		}
	}
	batchCnt := uint32(100)
	batchRows := uint32(10000) * 1 / 2 * batchCnt
	logutil.Info(tae.Opts.Catalog.SimplePPString(common.PPL1))
	bat := catalog.MockBatch(schema, int(batchRows))
	newbat := batch.New(true, bat.Attrs)
	newbat.Vecs = containers.CopyToCNVectors(bat.Vecs)
	bats := containers.SplitBatch(newbat, int(batchCnt))
	var wg sync.WaitGroup
	doAppend := func(b *batch.Batch) func() {
		return func() {
			defer wg.Done()
			txn, _ := eng.StartTxn(nil)
			txnOperator := moengine.TxnToTxnOperator(txn)
			// {
			// 	db, _ := txn.GetDatabase(dbName)
			// 	rel, _ := db.GetRelationByName(schema.Name)
			// }
			db, err := eng.Database(ctx, dbName, txnOperator)
			if err != nil {
				panic(err)
			}
			rel, err := db.Relation(ctx, schema.Name)
			if err != nil {
				panic(err)
			}
			if err := rel.Write(ctx, b); err != nil {
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
		_ = p.Submit(doAppend(b))
	}
	wg.Wait()
	stopProfile()
	logutil.Infof("Append takes: %s", time.Since(now))
	{
		txn, _ := eng.StartTxn(nil)
		txnOperator := moengine.TxnToTxnOperator(txn)
		db, err := eng.Database(ctx, dbName, txnOperator)
		if err != nil {
			panic(err)
		}
		rel, err := db.Relation(ctx, schema.Name)
		if err != nil {
			panic(err)
		}
		m := mpool.MustNewZeroNoFixed()
		readProc := func(reader engine.Reader) {
			defer wg.Done()
			for {
				bat, err := reader.Read(ctx, []string{schema.ColDefs[0].Name}, nil, m)
				if err != nil {
					panic(err)
				}
				if bat == nil {
					break
				}
				logutil.Infof("bat rows: %d", bat.Vecs[0].Length())
			}
		}

		parallel := 10
		readers, err := rel.NewReader(ctx, parallel, nil, nil)
		if err != nil {
			panic(err)
		}
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
