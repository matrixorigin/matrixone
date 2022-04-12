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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var sampleDir = "/tmp/sample4"
var txnBufSize = common.G
var mutBufSize = common.G
var dbName = "db"
var cpuprofile = "/tmp/sample4/cpuprofile"
var memprofile = "/tmp/sample4/memprofile"

var querys = []string{
	"CREATE DATABASE IF NOT EXISTS test;",
	"CREATE DATABASE db;",
	"CREATE DATABASE IF NOT EXISTS db;",
	"CREATE TABLE table1(a int);",
	"CREATE TABLE IF NOT EXISTS table1(a int);",
	// "CREATE INDEX idx on table1(a);",
	// "DROP INDEX idx on table1;",
	// "SHOW DATABASES;",
	"SHOW TABLES;",
	// "SHOW COLUMNS FROM table1;",
	// "SHOW CREATE TABLE table1;",
	// "SHOW CREATE DATABASE db;",
	"INSERT INTO table1 values(12),(13),(14),(15);",
	"SELECT * FROM table1;",
	"DROP TABLE table1;",
	"DROP DATABASE IF EXISTS db;",
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

	compile.InitAddress("127.0.0.1")
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	proc := process.New(mheap.New(gm))
	for _, query := range querys {
		txn := mgr.StartTxn(nil)
		e := moengine.NewEngine(txn)
		processQuery(query, e, proc)
		if err := txn.Commit(); err != nil {
			panic(err)
		}
		logutil.Info(txn.String())
	}
	logutil.Infof("%d", txnBufMgr.Count())
	logutil.Infof("%d", mutBufMgr.Count())
	logutil.Info(c.SimplePPString(common.PPL1))
}

func sqlOutput(_ interface{}, bat *batch.Batch) error {
	logutil.Infof("%v\n", bat.Zs)
	logutil.Infof("%v\n", bat)
	return nil
}

func processQuery(query string, e engine.Engine, proc *process.Process) {
	c := compile.New("test", query, "", e, proc)
	es, err := c.Build()
	if err != nil {
		panic(err)
	}
	for _, e := range es {
		logutil.Infof("%s\n", query)
		if err := e.Compile(nil, sqlOutput); err != nil {
			panic(err)
		}
		attrs := e.Columns()
		logutil.Infof("result:\n")
		for i, attr := range attrs {
			logutil.Infof("\t[%v] = %v:%v\n", i, attr.Name, attr.Typ)
		}
		if err := e.Run(0); err != nil {
			panic(err)
		}
	}
}
