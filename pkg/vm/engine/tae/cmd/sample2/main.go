package main

import (
	"bytes"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
	"github.com/panjf2000/ants/v2"
	"github.com/sirupsen/logrus"
)

var sampleDir = "/tmp/sample2"
var txnBufSize = common.G
var mutBufSize = common.G
var dbName = "db"
var cpuprofile = "/tmp/sample2/cpuprofile"
var memprofile = "/tmp/sample2/memprofile"

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

func initContext() (*catalog.Catalog, *txnbase.TxnManager, txnbase.NodeDriver, base.INodeManager, base.INodeManager) {
	c := catalog.MockCatalog(sampleDir, "sample", nil)
	driver := txnbase.NewNodeDriver(sampleDir, "store", nil)
	txnBufMgr := buffer.NewNodeManager(txnBufSize, nil)
	mutBufMgr := buffer.NewNodeManager(mutBufSize, nil)
	factory := tables.NewDataFactory(dataio.SegmentFileMockFactory, mutBufMgr)
	mgr := txnbase.NewTxnManager(txnimpl.TxnStoreFactory(c, driver, txnBufMgr, factory), txnimpl.TxnFactory(c))
	mgr.Start()
	return c, mgr, driver, txnBufMgr, mutBufMgr
}

func main() {
	c, mgr, driver, txnBufMgr, mutBufMgr := initContext()
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	schema := catalog.MockSchemaAll(10)
	schema.BlockMaxRows = 1000
	schema.SegmentMaxBlocks = 10
	schema.PrimaryKey = 3
	batchCnt := uint64(10)
	batchRows := uint64(schema.BlockMaxRows) * 1 / 2 * batchCnt
	{
		txn := mgr.StartTxn(nil)
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
			txn := mgr.StartTxn(nil)
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
	p, _ := ants.NewPool(4)
	now := time.Now()
	startProfile()
	for _, b := range bats {
		wg.Add(1)
		p.Submit(doAppend(b))
	}
	wg.Wait()
	stopProfile()
	logrus.Infof("Append takes: %s", time.Since(now))
	logrus.Info(txnBufMgr.Count())
	logrus.Info(mutBufMgr.Count())

	{
		txn := mgr.StartTxn(nil)
		db, err := txn.GetDatabase(dbName)
		if err != nil {
			panic(err)
		}
		rel, err := db.GetRelationByName(schema.Name)
		if err != nil {
			panic(err)
		}
		var compressed bytes.Buffer
		var decompressed bytes.Buffer
		segIt := rel.MakeSegmentIt()
		for segIt.Valid() {
			seg := segIt.GetSegment()
			logrus.Info(seg.String())
			blkIt := seg.MakeBlockIt()
			for blkIt.Valid() {
				blk := blkIt.GetBlock()
				logrus.Info(blk.String())
				compressed.Reset()
				decompressed.Reset()
				vec, err := blk.GetVectorCopy(schema.ColDefs[0].Name, &compressed, &decompressed)
				logrus.Infof("Block %s Rows %d", blk.Fingerprint().ToBlockFileName(), vector.Length(vec))
				if err != nil {
					panic(err)
				}
				blkIt.Next()
			}
			segIt.Next()
		}
		if err = txn.Commit(); err != nil {
			panic(err)
		}
	}
	logrus.Info(c.SimplePPString(common.PPL1))
}
