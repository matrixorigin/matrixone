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

package catalog

import (
	"fmt"
	stdLog "log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"

	cconfig "github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"

	// "github.com/matrixorigin/matrixone/pkg/logutil"
	aoe3 "github.com/matrixorigin/matrixone/pkg/vm/driver/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/config"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"

	// "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/aoedb/v1"

	"github.com/matrixorigin/matrixcube/raftstore"

	"github.com/fagongzi/log"
)

const (
	blockRows          = 10000
	blockCntPerSegment = 2
	colCnt             = 4
	segmentCnt         = 5
	blockCnt           = blockCntPerSegment * segmentCnt
	restart            = false
	tableCount         = 5
	databaseCount      = 10
	preAllocShardNum   = 20
)

var (
	testDatabaceName = "test_db"
	testTableName    = "test_tbl"
	testTables       []*aoe.TableInfo
)

func init() {
	for i := 0; i < tableCount; i++ {
		testTable := MockTableInfo(colCnt, i)
		testTable.Name = fmt.Sprintf("%v%v", testTableName, uint64(i))
		testTables = append(testTables, testTable)
	}
}
func MockTableInfo(colCnt int, i int) *aoe.TableInfo {
	tblInfo := &aoe.TableInfo{
		Name:    "mocktbl" + strconv.Itoa(i),
		Columns: make([]aoe.ColumnInfo, 0),
		Indices: make([]aoe.IndexInfo, 0),
	}
	prefix := "mock_"
	for i := 0; i < colCnt; i++ {
		name := fmt.Sprintf("%s%d", prefix, i)
		colInfo := aoe.ColumnInfo{
			Name: name,
		}
		if i == 1 {
			colInfo.Type = types.Type{Oid: types.T(types.T_varchar), Size: 24}
		} else {
			colInfo.Type = types.Type{Oid: types.T_int32, Size: 4, Width: 4}
		}
		tblInfo.Columns = append(tblInfo.Columns, colInfo)
	}
	return tblInfo
}
func MockTableInfoWithProperties(colCnt int, i, bucket int) *aoe.TableInfo {
	tblInfo := &aoe.TableInfo{
		Name:    "test_table" + strconv.Itoa(i),
		Columns: make([]aoe.ColumnInfo, 0),
		Indices: make([]aoe.IndexInfo, 0),
	}
	prefix := "mock_"
	for i := 0; i < colCnt; i++ {
		name := fmt.Sprintf("%s%d", prefix, i)
		colInfo := aoe.ColumnInfo{
			Name: name,
		}
		if i == 1 {
			colInfo.Type = types.Type{Oid: types.T(types.T_varchar), Size: 24}
		} else {
			colInfo.Type = types.Type{Oid: types.T_int32, Size: 4, Width: 4}
		}
		tblInfo.Columns = append(tblInfo.Columns, colInfo)
	}
	property := aoe.Property{Key: "bucket", Value: strconv.Itoa(bucket)}
	tblInfo.Properties = append(tblInfo.Properties, property)
	return tblInfo
}

// func TestProperties(t *testing.T) {
// 	nodeCount := 3
// 	bucketCount := 9
// 	tableCount := 1
// 	stdLog.SetFlags(log.Lshortfile | log.LstdFlags)
// 	c := testutil.NewTestAOECluster(t,
// 		func(node int) *config.Config {
// 			c := &config.Config{}
// 			c.ClusterConfig.PreAllocatedGroupNum = preAllocShardNum
// 			return c
// 		},
// 		testutil.WithTestAOEClusterAOEStorageFunc(func(path string) (*aoe3.Storage, error) {
// 			opts := &storage.Options{}
// 			mdCfg := &storage.MetaCfg{
// 				SegmentMaxBlocks: blockCntPerSegment,
// 				BlockMaxRows:     blockRows,
// 			}
// 			opts.CacheCfg = &storage.CacheCfg{
// 				IndexCapacity:  blockRows * blockCntPerSegment * 80,
// 				InsertCapacity: blockRows * uint64(colCnt) * 2000,
// 				DataCapacity:   blockRows * uint64(colCnt) * 2000,
// 			}
// 			opts.MetaCleanerCfg = &storage.MetaCleanerCfg{
// 				Interval: time.Duration(1) * time.Second,
// 			}
// 			opts.Meta.Conf = mdCfg
// 			return aoe3.NewStorageWithOptions(path, opts)
// 		}),
// 		testutil.WithTestAOEClusterUsePebble(),
// 		testutil.WithTestAOEClusterRaftClusterOptions(
// 			raftstore.WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *cconfig.Config) {
// 				cfg.Worker.RaftEventWorkers = 8
// 				cfg.Prophet.Replication.MaxReplicas = 1
// 			}),
// 			raftstore.WithTestClusterNodeCount(nodeCount),
// 			raftstore.WithTestClusterLogLevel(zapcore.DebugLevel),
// 			raftstore.WithTestClusterDataPath("./test")))

// 	c.Start()
// 	defer func() {
// 		stdLog.Printf(">>>>>>>>>>>>>>>>> call stop")
// 		c.Stop()
// 	}()
// 	c.RaftCluster.WaitLeadersByCount(preAllocShardNum+1, time.Second*60)
// 	logutil.Infof("wait shard finished")
// 	driver := c.CubeDrivers[0]

// 	catalog := NewCatalog(driver)
// 	dbid, _ := catalog.CreateDatabase(0, "test_label", 0)
// 	tid_sid := make(map[uint64][]uint64)
// 	for i := 0; i < tableCount; i++ {
// 		tbl := MockTableInfoWithProperties(4, i, bucketCount)
// 		logutil.Infof("before create table")
// 		tid, err := catalog.CreateTable(0, dbid, *tbl)
// 		logutil.Infof("after create table")
// 		require.Nil(t, err)
// 		sids, err := catalog.getShardids(tid)
// 		require.Nil(t, err)
// 		require.Equal(t, bucketCount, len(sids))
// 		tid_sid[tid] = sids
// 	}

// 	for k := 0; k < nodeCount; k++ {
// 		shardsFromStorage := c.AOEStorages[k].DB.DatabaseNames()
// 		logutil.Infof("storage %v, sids %v", k, shardsFromStorage)
// 		for tid, tableSids := range tid_sid {
// 			logutil.Infof("tid %v, sids %v", tid, tableSids)
// 			shardInStorageCount := 0
// 			for _, storageSidStr := range shardsFromStorage {
// 				storageSid, _ := aoedb.IdToNameFactory.Decode(storageSidStr)
// 				for _, tableSid := range tableSids {
// 					if tableSid == storageSid {
// 						shardInStorageCount++
// 						logutil.Infof("tid %v, sid %v", tid, tableSid)
// 					}
// 				}
// 			}
// 			// require.Equal(t, bucketCount/nodeCount, shardInStorageCount)
// 			logutil.Infof("node %v(total replitca count %v), table id %v, replica count %v\n******************************", k, len(shardsFromStorage), tid, shardInStorageCount)
// 		}
// 	}

// 	catalog.DropDatabase(0, "test_label")
// }
func TestCatalogWithUtil(t *testing.T) {
	os.Remove("test")
	stdLog.SetFlags(log.Lshortfile | log.LstdFlags)
	c := testutil.NewTestAOECluster(t,
		func(node int) *config.Config {
			c := &config.Config{}
			c.ClusterConfig.PreAllocatedGroupNum = preAllocShardNum
			// c.ServerConfig.ExternalServer = true
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
			raftstore.WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *cconfig.Config) {
				cfg.Worker.RaftEventWorkers = 8
			}),
			// raftstore.WithTestClusterNodeCount(1),
			raftstore.WithTestClusterLogLevel(zapcore.InfoLevel),
			raftstore.WithTestClusterDataPath("./test")))

	c.Start()
	defer func() {
		stdLog.Printf(">>>>>>>>>>>>>>>>> call stop")
		c.Stop()
	}()

	c.RaftCluster.WaitLeadersByCount(preAllocShardNum+1, time.Second*60)

	stdLog.Printf("driver all started.")

	driver := c.CubeDrivers[0]

	catalog := NewCatalog(driver)

	_, err := catalog.getAvailableShard(0)
	require.NoError(t, err, "getAvailableShard Fail")
	//Test CreateDatabase
	var dbids []uint64
	for i := 0; i < databaseCount; i++ {
		dbid, err := catalog.CreateDatabase(0, testDatabaceName+strconv.Itoa(i), 0)
		require.NoError(t, err, "CreateDatabase%v Fail", i)
		dbids = append(dbids, dbid)
	}
	_, err = catalog.CreateDatabase(0, testDatabaceName+strconv.Itoa(0), 0)
	require.Equal(t, ErrDBCreateExists, err, "CreateDatabase: wrong err")

	//Test ListDatabases
	schemas, err := catalog.ListDatabases()
	require.NoError(t, err, "ListDatabases Fail")
	require.Equal(t, len(schemas), len(dbids), "ListDatabases: Wrong len")

	//Test GetDatabase
	schema, err := catalog.GetDatabase(testDatabaceName + "0")
	require.NoError(t, err, "GetDatabase Fail")
	require.Equal(t, schema.Id, dbids[0], "GetDatabase: Wrong id")
	_, err = catalog.GetDatabase(testDatabaceName)
	require.Equal(t, ErrDBNotExists, err, "GetDatabase: wrong err")

	//Test CreateTableFailed
	_, err = catalog.CreateTable(0, dbids[0], aoe.TableInfo{})
	require.Equal(t, ErrTabletCreateFailed, err, "CreateTable: wrong err")

	//Test CreateTable
	var createIds []uint64
	for i := 0; i < tableCount; i++ {
		createId, err := catalog.CreateTable(0, dbids[0], *testTables[i])
		require.NoError(t, err, "CreateTable%v Fail", i)
		createIds = append(createIds, createId)
	}

	//Test CreateIndex
	col := testTables[0].Columns[0]
	idxTableInfo, _ := catalog.GetTable(dbids[0], testTables[0].Name)
	err = catalog.CreateIndex(0, aoe.IndexInfo{SchemaId: dbids[0], TableId: idxTableInfo.Id, Name: "mock_idx", ColumnNames: []string{col.Name}, Type: aoe.ZoneMap})
	require.NoError(t, err)
	idxTableInfo, _ = catalog.GetTable(dbids[0], testTables[0].Name)
	idxs := idxTableInfo.Indices
	require.Equal(t, 1, len(idxs))
	idx := idxs[0]
	require.Equal(t, "mock_idx", idx.Name)
	require.Equal(t, aoe.ZoneMap, idx.Type)
	require.Equal(t, 1, len(idx.Columns))
	require.Equal(t, col.Id, idx.Columns[0])
	require.Equal(t, col.Name, idx.ColumnNames[0])

	err = catalog.CreateIndex(0, aoe.IndexInfo{SchemaId: dbids[0], TableId: idxTableInfo.Id, Name: "mock_idx", ColumnNames: []string{col.Name}, Type: aoe.ZoneMap})
	require.Equal(t, ErrIndexExist, err)

	err = catalog.CreateIndex(0, aoe.IndexInfo{SchemaId: dbids[0], TableId: idxTableInfo.Id, Name: "mock_idx_invalid_type", ColumnNames: []string{col.Name}, Type: aoe.Invalid})
	require.Equal(t, ErrInvalidIndexType, err)

	//Test DropIndex
	err = catalog.DropIndex(0, idxTableInfo.Id, idxTableInfo.SchemaId, "mock_idx")
	require.NoError(t, err)
	idxTableInfo, _ = catalog.GetTable(dbids[0], testTables[0].Name)
	idxs = idxTableInfo.Indices
	require.Equal(t, 0, len(idxs))

	err = catalog.DropIndex(0, idxTableInfo.Id, idxTableInfo.SchemaId, "mock_idx")
	require.Equal(t, ErrIndexNotExist, err)

	//Test CreateTableExists
	_, err = catalog.CreateTable(0, dbids[0], *testTables[0])
	require.Equal(t, ErrTableCreateExists, err, "CreateTable: wrong err")

	//test ListTables
	tables, err := catalog.ListTables(dbids[0])
	require.NoError(t, err, "ListTables Fail")
	require.Equal(t, len(tables), tableCount, "ListTables: Wrong len")

	//test ListTablesByName
	tables, err = catalog.ListTablesByName(testDatabaceName + strconv.Itoa(0))
	require.NoError(t, err, "ListTablesByName Fail")
	require.Equal(t, len(tables), tableCount, "ListTablesByName: Wrong len")

	//test GetTable
	table, err := catalog.GetTable(dbids[0], testTables[0].Name)
	require.NoError(t, err, "GetTable Fail")
	require.Equal(t, table.Id, createIds[0], "GetTable: Wrong id")
	require.Equal(t, table.Name, testTables[0].Name, "GetTable: Wrong Name")

	_, err = catalog.GetTable(dbids[0], "wrong_name")
	require.Equal(t, ErrTableNotExists, err, "GetTable: wrong err")

	//test GetTablets
	tablets, err := catalog.GetTablets(dbids[0], testTables[0].Name)
	require.NoError(t, err, "GetTablets Fail")
	for i := range tablets {
		require.Equal(t, tablets[i].Table.Id, createIds[0], "GetTablets: Wrong id")
		require.Equal(t, tablets[i].Table.Name, testTables[0].Name, "GetTablets: Wrong Name")
	}

	//test DropTable
	dropId, err := catalog.DropTable(0, dbids[0], testTables[0].Name)
	require.NoError(t, err, "DropTable Fail")
	require.Equal(t, createIds[0], dropId, "DropTable: Wrong id")

	_, err = catalog.GetTable(dbids[0], testTables[0].Name)
	require.Equal(t, ErrTableNotExists, err, "DropTable: GetTable wrong err")

	_, err = catalog.DropTable(0, dbids[0], testTables[0].Name)
	require.Equal(t, ErrTableNotExists, err, "DropTable: DropTable wrong err")

	_, err = catalog.GetTablets(dbids[0], testTables[0].Name)
	require.Equal(t, ErrTableNotExists, err, "DropTable: GetTablets wrong err")

	_, err = catalog.checkTableExists(dbids[0], createIds[0])
	require.Equal(t, ErrTableNotExists, err, "DropTable: checkTableExists wrong err")

	//test RemoveDeletedTable
	cnt, err := catalog.RemoveDeletedTable(0)
	require.NoError(t, err, "RemoveDeletedTable Fail")
	require.Equal(t, cnt, 1, "RemoveDeletedTable: Wrong id")

	//test DropDatabase
	for i := 0; i < databaseCount; i++ {
		err = catalog.DropDatabase(0, testDatabaceName+strconv.Itoa(i))
		require.NoError(t, err, "DropDatabase%v Fail", i)
	}

	_, err = catalog.GetDatabase(testDatabaceName + "0")
	require.Equal(t, ErrDBNotExists, err, "DropDatabase: GetDatabase wrong err")

	_, err = catalog.DropTable(0, dbids[0], testTables[0].Name)
	require.Equal(t, ErrDBNotExists, err, "DropDatabase: DropTable wrong err")

	_, err = catalog.ListTables(dbids[0])
	require.Equal(t, ErrDBNotExists, err, "DropDatabase: ListTables wrong err")

	_, err = catalog.ListTablesByName(testDatabaceName + strconv.Itoa(0))
	require.Equal(t, ErrDBNotExists, err, "DropDatabase: ListTablesByName wrong err")

	_, err = catalog.GetTable(dbids[0], testTables[0].Name)
	require.Equal(t, ErrDBNotExists, err, "DropDatabase: GetTable wrong err")

	_, err = catalog.GetTablets(dbids[0], testTables[0].Name)
	require.Equal(t, ErrDBNotExists, err, "DropDatabase: GetTablets wrong err")

	_, err = catalog.CreateTable(0, dbids[0], *testTables[0])
	require.Equal(t, ErrDBNotExists, err, "CreateDatabase: wrong err")

	err = catalog.DropDatabase(0, testDatabaceName+strconv.Itoa(0))
	require.Equal(t, ErrDBNotExists, err, "DropDatabase: DropDatabase wrong err")

}
