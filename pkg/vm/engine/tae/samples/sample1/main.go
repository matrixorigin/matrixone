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

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/panjf2000/ants/v2"
)

var sampleDir = "/tmp/sample1"
var dbName = "db"
var cpuprofile = "/tmp/sample1/cpuprofile"
var memprofile = "/tmp/sample1/memprofile"

func init() {
	os.RemoveAll(sampleDir)
}

func startProfile() {
	f, _ := os.Create(cpuprofile)
	_ = pprof.StartCPUProfile(f)
}

func stopProfile() {
	pprof.StopCPUProfile()
	memf, _ := os.Create(memprofile)
	defer memf.Close()
	_ = pprof.Lookup("heap").WriteTo(memf, 0)
}

func main() {
	ctx := context.Background()

	opts := config.WithOpts(nil, 1)
	tae, _ := db.Open(ctx, sampleDir, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(10, 3)
	schema.BlockMaxRows = 20
	schema.SegmentMaxBlocks = 2
	batchCnt := uint32(1)
	batchRows := int(schema.BlockMaxRows * 1 / 20 * batchCnt)
	{
		txn, _ := tae.StartTxn(nil)
		db, _ := txn.CreateDatabase(dbName, "", "")
		_, _ = db.CreateRelation(schema)
		if err := txn.Commit(context.Background()); err != nil {
			panic(err)
		}
	}
	bat := catalog.MockBatch(schema, batchRows)
	bats := bat.Split(int(batchCnt))
	var wg sync.WaitGroup
	doAppend := func(b *containers.Batch) func() {
		return func() {
			defer wg.Done()
			txn, _ := tae.StartTxn(nil)
			db, err := txn.GetDatabase(dbName)
			if err != nil {
				panic(err)
			}
			rel, err := db.GetRelationByName(schema.Name)
			if err != nil {
				panic(err)
			}
			if err := rel.Append(context.Background(), b); err != nil {
				panic(err)
			}
			if err := txn.Commit(context.Background()); err != nil {
				panic(err)
			}
		}
	}
	p, _ := ants.NewPool(200)
	now := time.Now()
	startProfile()
	for _, b := range bats {
		wg.Add(1)
		_ = p.Submit(doAppend(b))
	}
	wg.Wait()
	stopProfile()
	logutil.Infof("Append takes: %s", time.Since(now))
	waitTime := time.Millisecond * time.Duration(batchCnt/200+1) * 300
	for i := 0; i < 5; i++ {
		time.Sleep(waitTime)
	}
}
