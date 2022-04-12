package main

import (
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
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

	schema := catalog.MockSchema(1)
	schema.BlockMaxRows = 10000
	schema.SegmentMaxBlocks = 10
	batchRows := uint64(schema.BlockMaxRows) * 1 / 2
	{
		txn := mgr.StartTxn(nil)
		db, _ := txn.CreateDatabase(dbName)
		db.CreateRelation(schema)
		if err := txn.Commit(); err != nil {
			panic(err)
		}
	}
	bat := mock.MockBatch(schema.Types(), batchRows)
	var wg sync.WaitGroup
	doAppend := func() {
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
		if err := rel.Append(bat); err != nil {
			panic(err)
		}
		if err := txn.Commit(); err != nil {
			panic(err)
		}
	}
	p, _ := ants.NewPool(12)
	batchCnt := 1000
	now := time.Now()
	startProfile()
	for i := 0; i < batchCnt; i++ {
		wg.Add(1)
		p.Submit(doAppend)
	}
	wg.Wait()
	stopProfile()
	logrus.Infof("Append takes: %s", time.Since(now))
	// time.Sleep(time.Second * 100)
	logrus.Info(txnBufMgr.String())
	logrus.Info(mutBufMgr.String())
}
