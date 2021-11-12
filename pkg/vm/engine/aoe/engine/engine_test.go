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

package engine

import (
	"bytes"
	"fmt"
	stdLog "log"
	catalog2 "matrixone/pkg/catalog"
	"matrixone/pkg/container/types"
	"matrixone/pkg/logutil"
	"matrixone/pkg/vm/engine/aoe/protocol"
	"matrixone/pkg/vm/mheap"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"

	aoe3 "matrixone/pkg/vm/driver/aoe"
	"matrixone/pkg/vm/driver/config"
	"matrixone/pkg/vm/driver/testutil"
	//"matrixone/pkg/sql/protocol"
	vengine "matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/engine/aoe/common/codec"
	"matrixone/pkg/vm/engine/aoe/common/helper"
	"matrixone/pkg/vm/engine/aoe/storage"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock"
	"sync"

	"github.com/fagongzi/log"
	putil "github.com/matrixorigin/matrixcube/components/prophet/util"
	cConfig "github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/stretchr/testify/require"

	"testing"
	"time"
)

const (
	testDBName = "db1"

	blockRows          = 10000
	blockCntPerSegment = 2
	colCnt             = 4
	segmentCnt         = 5
	blockCnt           = blockCntPerSegment * segmentCnt
	restart            = false
	tableDDL           = false
)

var (
	dbName    = "test_db1"
	tableName = "test_tb"
	cols      = []vengine.TableDef{
		&vengine.AttributeDef{
			Attr: vengine.Attribute{
				Name: "col1",
				Type: types.Type{},
				Alg:  0,
			},
		},
		&vengine.AttributeDef{
			Attr: vengine.Attribute{
				Name: "col2",
				Type: types.Type{},
				Alg:  0,
			},
		},
	}
)

func TestAOEEngine(t *testing.T) {
	putil.SetLogger(log.NewLoggerWithPrefix("prophet"))
	c := testutil.NewTestAOECluster(t,
		func(node int) *config.Config {
			c := &config.Config{}
			c.ClusterConfig.PreAllocatedGroupNum = 20
			c.ServerConfig.ExternalServer = true
			return c
		},
		testutil.WithTestAOEClusterAOEStorageFunc(func(path string) (*aoe3.Storage, error) {
			opts := &storage.Options{}
			mdCfg := &storage.MetaCfg{
				SegmentMaxBlocks: blockCntPerSegment,
				BlockMaxRows:     blockRows,
			}
			opts.CacheCfg = &storage.CacheCfg{
				IndexCapacity:  blockRows * blockCntPerSegment * 80,
				InsertCapacity: blockRows * uint64(colCnt) * 2000,
				DataCapacity:   blockRows * uint64(colCnt) * 2000,
			}
			opts.MetaCleanerCfg = &storage.MetaCleanerCfg{
				Interval: time.Duration(1) * time.Second,
			}
			opts.Meta.Conf = mdCfg
			return aoe3.NewStorageWithOptions(path, opts)
		}),
		testutil.WithTestAOEClusterUsePebble(),
		testutil.WithTestAOEClusterRaftClusterOptions(
			raftstore.WithTestClusterRecreate(true),
			raftstore.WithTestClusterLogLevel("info"),
			raftstore.WithTestClusterDataPath("./test"),
			raftstore.WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *cConfig.Config) {
				cfg.Worker.RaftEventWorkers = uint64(32)
			})))

	c.Start()
	defer func() {
		stdLog.Printf(">>>>>>>>>>>>>>>>> call stop")
		c.Stop()
	}()
	c.RaftCluster.WaitLeadersByCount(21, time.Second*30)
	stdLog.Printf("app started.")
	time.Sleep(3 * time.Second)

	var catalogs []*catalog2.Catalog
	for i := 0; i < 3; i++ {
		catalogs = append(catalogs, catalog2.NewCatalog(c.CubeDrivers[i]))
	}

	if tableDDL {
		time.Sleep(3 * time.Second)
		testTableDDL(t, catalogs)
	}

	aoeEngine := New(catalogs[0])

	//test engine
	n := aoeEngine.Node("")
	require.NotNil(t, n, "the result of Node()")

	t0 := time.Now()
	err := aoeEngine.Create(0, testDBName, 0)
	require.NoError(t, err)
	logutil.Infof("Create cost %d ms", time.Since(t0).Milliseconds())

	dbs := aoeEngine.Databases()
	require.Equal(t, 1, len(dbs))

	err = aoeEngine.Delete(1, testDBName)
	require.NoError(t, err)

	dbs = aoeEngine.Databases()
	require.Equal(t, 0, len(dbs))

	_, err = aoeEngine.Database(testDBName)
	require.NotNil(t, err)

	err = aoeEngine.Create(2, testDBName, 0)
	require.NoError(t, err)
	db, err := aoeEngine.Database(testDBName)
	require.NoError(t, err)

	//test database
	tbls := db.Relations()
	require.Equal(t, 0, len(tbls))

	mockTbl := md.MockTableInfo(colCnt)
	mockTbl.Name = fmt.Sprintf("%s%d", tableName, time.Now().Unix())
	_, _, _, _, defs , _ := helper.UnTransfer(*mockTbl)

	time.Sleep(10 * time.Second)

	err = db.Create(3, mockTbl.Name, defs)
	if err != nil {
		stdLog.Printf("create table %v failed, %v", mockTbl.Name, err)
	} else {
		stdLog.Printf("create table %v is succeeded", mockTbl.Name)
	}
	require.NoError(t, err)

	tbls = db.Relations()
	require.Equal(t, 1, len(tbls))

	tb, err := db.Relation(mockTbl.Name)
	require.NoError(t, err)
	require.Equal(t, tb.ID(), mockTbl.Name)

	attrs := helper.Attribute(*mockTbl)
	var typs []types.Type
	for _, attr := range attrs {
		typs = append(typs, attr.Type)
	}
	ibat := mock.MockBatch(typs, blockRows)
	var buf bytes.Buffer
	//err = protocol.EncodeBatch(ibat, &buf)
	err = protocol.EncodeBatch(ibat, &buf)
	require.NoError(t, err)
	stdLog.Printf("size of batch is  %d", buf.Len())

	for i := 0; i < blockCnt; i++ {
		err = tb.Write(4, ibat)
		require.NoError(t, err)
	}

	tb.Close()

	tb = &relation{}
	err = tb.Write(4, ibat)
	require.EqualError(t, err, "no tablets exists", "wrong err")

	//test relation
	tb, err = db.Relation(mockTbl.Name)
	require.NoError(t, err)
	require.Equal(t, tb.ID(), mockTbl.Name)

	var vattrs []vengine.Attribute
	tdefs := tb.TableDefs()
	for _, def := range tdefs {
		if v, ok := def.(*vengine.AttributeDef); ok {
			vattrs = append(vattrs, v.Attr)
		}
	}
	require.Equal(t, len(attrs), len(vattrs), "Attribute: wrong length")

	var index []vengine.IndexTableDef
	for _, def := range tdefs {
		if i, ok := def.(*vengine.IndexTableDef); ok {
			index = append(index, vengine.IndexTableDef{
				Typ:   int(i.Typ),
				Names: i.Names,
			})
		}
	}
	require.Equal(t, len(attrs), len(index), "Index: wrong len")

	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	mp := mheap.New(gm)
	readers := tb.NewReader(6, mp)
	for _, reader := range readers {
		_, err = reader.Read([]uint64{uint64(1)}, []string{"mock_0"}, []*bytes.Buffer{bytes.NewBuffer(nil)})
		require.NoError(t, err)
		_, err := reader.Read([]uint64{uint64(1)}, []string{"mock_1"}, []*bytes.Buffer{bytes.NewBuffer(nil)})
		require.NoError(t, err)
	}
	num := tb.NewReader(15, mp)
	require.Equal(t, 10, len(num))
	tb.Close()

	err = db.Delete(5, mockTbl.Name)
	require.NoError(t, err)

	tbls = db.Relations()
	require.Equal(t, 0, len(tbls))

	if restart {
		time.Sleep(3 * time.Second)
		doRestartEngine(t)
	}
}

func doRestartEngine(t *testing.T) {
	putil.SetLogger(log.NewLoggerWithPrefix("prophet"))
	c := testutil.NewTestAOECluster(t,
		func(node int) *config.Config {
			c := &config.Config{}
			c.ClusterConfig.PreAllocatedGroupNum = 20
			c.ServerConfig.ExternalServer = true
			return c
		},
		testutil.WithTestAOEClusterAOEStorageFunc(func(path string) (*aoe3.Storage, error) {
			opts := &storage.Options{}
			mdCfg := &storage.MetaCfg{
				SegmentMaxBlocks: blockCntPerSegment,
				BlockMaxRows:     blockRows,
			}
			opts.CacheCfg = &storage.CacheCfg{
				IndexCapacity:  blockRows * blockCntPerSegment * 80,
				InsertCapacity: blockRows * uint64(colCnt) * 2000,
				DataCapacity:   blockRows * uint64(colCnt) * 2000,
			}
			opts.MetaCleanerCfg = &storage.MetaCleanerCfg{
				Interval: time.Duration(1) * time.Second,
			}
			opts.Meta.Conf = mdCfg
			return aoe3.NewStorageWithOptions(path, opts)
		}),
		testutil.WithTestAOEClusterUsePebble(),
		testutil.WithTestAOEClusterRaftClusterOptions(
			raftstore.WithTestClusterLogLevel("info"),
			raftstore.WithTestClusterDataPath("./test"),
			raftstore.WithTestClusterRecreate(false)))
	defer func() {
		stdLog.Printf(">>>>>>>>>>>>>>>>> call stop")
		c.Stop()
	}()
	c.Start()

	c.RaftCluster.WaitLeadersByCount(21, time.Second*30)

	catalog := catalog2.NewCatalog(c.CubeDrivers[0])
	aoeEngine := New(catalog)

	dbs := aoeEngine.Databases()
	require.Equal(t, 1, len(dbs))

	db, err := aoeEngine.Database(testDBName)
	require.NoError(t, err)

	tbls := db.Relations()
	require.Equal(t, 9, len(tbls))

	for _, tName := range tbls {
		_, err := db.Relation(tName)
		require.NoError(t, err)
		//require.Equal(t, segmentCnt, len(tb.Segments()))
		//logutil.Infof("table name is %s, segment size is %d, segments is %v\n", tName, len(tb.Segments()), tb.Segments())
	}
}

func testTableDDL(t *testing.T, c []*catalog2.Catalog) {
	//Wait shard state change
	logutil.Infof("ddl test begin")

	tbs, err := c[0].ListTables(99)
	require.Error(t, catalog2.ErrDBNotExists, err)

	dbid, err := c[0].CreateDatabase(0, dbName, 1)
	require.NoError(t, err)
	require.Less(t, uint64(0), dbid)

	tbs, err = c[0].ListTables(dbid)
	require.NoError(t, err)
	require.Nil(t, tbs)

	colCnt := 4
	t1 := md.MockTableInfo(colCnt)
	t1.Name = "t1"

	tid, err := c[0].CreateTable(1, dbid, *t1)
	require.NoError(t, err)
	require.Less(t, uint64(0), tid)

	tb, err := c[0].GetTable(dbid, t1.Name)
	require.NoError(t, err)
	require.NotNil(t, tb)
	require.Equal(t, aoe.StatePublic, tb.State)

	t2 := md.MockTableInfo(colCnt)
	t2.Name = "t2"
	_, err = c[0].CreateTable(2, dbid, *t2)
	require.NoError(t, err)

	tbls, err := c[0].ListTables(dbid)
	require.NoError(t, err)
	require.Equal(t, 2, len(tbls))

	err = c[0].DropDatabase(3, dbName)
	require.NoError(t, err)

	dbs, err := c[0].ListDatabases()
	require.NoError(t, err)
	require.Nil(t, dbs)

	logutil.Infof("Scan DeletedTableQueue")
	kvs, err := c[0].Driver.Scan(codec.String2Bytes("DeletedTableQueue"), codec.String2Bytes("DeletedTableQueue10"), 0)
	require.NoError(t, err)
	for i := 0; i < len(kvs); i += 2 {
		tbl, err := helper.DecodeTable(kvs[i+1])
		require.NoError(t, err)
		require.Equal(t, uint64(3), tbl.Epoch)
	}

	cnt, err := c[0].RemoveDeletedTable(10)
	require.NoError(t, err)
	require.Equal(t, 2, cnt)

	cnt, err = c[0].RemoveDeletedTable(11)
	require.NoError(t, err)
	require.Equal(t, 0, cnt)

	dbid, err = c[0].CreateDatabase(5, dbName, 1)
	require.NoError(t, err)
	require.Less(t, uint64(0), dbid)

	wg := sync.WaitGroup{}

	for index := range c {
		wg.Add(1)

		go func(idx int) {
			defer func() {
				wg.Done()
			}()
			for j := 10; j < 200; j++ {
				if j%3 != idx {
					continue
				}
				t1.Name = fmt.Sprintf("t%d", j)
				tid, err := c[idx].CreateTable(uint64(j), dbid, *t1)
				if err != nil {
					logutil.Infof("create table failed, catalog-%d, j is %d, tid is %d", idx, j, tid)
				} else {
					logutil.Infof("create table finished, catalog-%d, j is %d, tid is %d", idx, j, tid)
				}
				require.NoError(t, err)
				require.Less(t, uint64(0), tid)
				time.Sleep(100 * time.Millisecond)
			}
		}(index)
	}
	wg.Wait()

	tbls, err = c[0].ListTables(dbid)
	require.NoError(t, err)
	require.Equal(t, 190, len(tbls))

	for i := uint64(10); i < 15; i++ {
		_, err = c[0].DropTable(20+i, dbid, fmt.Sprintf("t%d", i))
		require.NoError(t, err)
	}

	tbls, err = c[0].ListTables(dbid)
	require.NoError(t, err)
	require.Equal(t, 185, len(tbls))

	err = c[0].DropDatabase(3, dbName)
	require.NoError(t, err)

	dbs, err = c[0].ListDatabases()
	require.NoError(t, err)
	require.Nil(t, dbs)

	logutil.Infof("ddl test is finished")

}
