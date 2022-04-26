package main

import (
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/panjf2000/ants/v2"
	"github.com/sirupsen/logrus"
)

var sampleDir = "/tmp/sample1"
var txnBufSize = common.G
var mutBufSize = common.G
var dbName = "db"
var cpuprofile = "/tmp/sample1/cpuprofile"
var memprofile = "/tmp/sample1/memprofile"

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

	schema := catalog.MockSchemaAll(10)
	schema.BlockMaxRows = 80000
	schema.SegmentMaxBlocks = 5
	schema.PrimaryKey = 3
	batchCnt := uint64(200)
	batchRows := uint64(schema.BlockMaxRows) * 1 / 20 * batchCnt
	{
		txn := tae.StartTxn(nil)
		db, _ := txn.CreateDatabase(dbName)
		db.CreateRelation(schema)
		if err := txn.Commit(); err != nil {
			panic(err)
		}
	}
	bat := compute.MockBatch(schema.Types(), batchRows, int(schema.PrimaryKey), nil)
	bats := compute.SplitBatch(bat, int(batchCnt))
	var wg sync.WaitGroup
	doAppend := func(b *batch.Batch) func() {
		return func() {
			defer wg.Done()
			txn := tae.StartTxn(nil)
			db, err := txn.GetDatabase(dbName)
			if err != nil {
				panic(err)
			}
			rel, err := db.GetRelationByName(schema.Name)
			if err != nil {
				panic(err)
			}
			if err := rel.Append(b); err != nil {
				panic(err)
			}
			if err := txn.Commit(); err != nil {
				panic(err)
			}
		}
	}
	p, _ := ants.NewPool(200)
	now := time.Now()
	startProfile()
	for _, b := range bats {
		wg.Add(1)
		p.Submit(doAppend(b))
	}
	wg.Wait()
	// {
	// 	txn := mgr.StartTxn(nil)
	// 	db, _ := txn.GetDatabase(dbName)
	// 	rel, _ := db.GetRelationByName(schema.Name)
	// 	var compressed bytes.Buffer
	// 	var decompressed bytes.Buffer
	// 	it := rel.MakeBlockIt()
	// 	for it.Valid() {
	// 		blk := it.GetBlock()
	// 		vec, _ := blk.GetVectorCopy(schema.ColDefs[schema.PrimaryKey].Name, &compressed, &decompressed)
	// 		logutil.Info(vec.String())
	// 		it.Next()
	// 	}
	// }

	stopProfile()
	logrus.Infof("Append takes: %s", time.Since(now))
	// time.Sleep(time.Second * 100)
	logrus.Info(tae.TxnBufMgr.String())
	logrus.Info(tae.MTBufMgr.String())
}
