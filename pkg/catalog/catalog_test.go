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
	"errors"
	"fmt"
	stdLog "log"
	"strconv"
	"testing"
	"time"

	"matrixone/pkg/container/types"
	aoe3 "matrixone/pkg/vm/driver/aoe"
	"matrixone/pkg/vm/driver/config"
	"matrixone/pkg/vm/driver/testutil"
	"matrixone/pkg/vm/engine/aoe"
	e "matrixone/pkg/vm/engine/aoe/storage"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"

	"github.com/matrixorigin/matrixcube/raftstore"

	"github.com/fagongzi/log"
	"github.com/stretchr/testify/assert"
)

const (
	blockRows          = 10000
	blockCntPerSegment = 2
	colCnt             = 4
	segmentCnt         = 5
	blockCnt           = blockCntPerSegment * segmentCnt
	restart            = false
	tableCount         = 50
	databaseCount      = 50
)

var testDatabaceName = "test_db"
var testTables []*aoe.TableInfo

func init() {
	for i := 0; i < tableCount; i++ {
		testTable := MockTableInfo(colCnt, i)
		testTable.Id = uint64(100 + i)
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
		indexInfo := aoe.IndexInfo{Type: uint64(md.ZoneMap), Columns: []uint64{uint64(i)}}
		tblInfo.Columns = append(tblInfo.Columns, colInfo)
		tblInfo.Indices = append(tblInfo.Indices, indexInfo)
	}
	return tblInfo
}
func TestCatalogWithUtil(t *testing.T) {
	stdLog.SetFlags(log.Lshortfile | log.LstdFlags)
	c := testutil.NewTestAOECluster(t,
		func(node int) *config.Config {
			c := &config.Config{}
			c.ClusterConfig.PreAllocatedGroupNum = 20
			c.ServerConfig.ExternalServer = true
			return c
		},
		testutil.WithTestAOEClusterAOEStorageFunc(func(path string) (*aoe3.Storage, error) {
			opts := &e.Options{}
			mdCfg := &e.MetaCfg{
				SegmentMaxBlocks: blockCntPerSegment,
				BlockMaxRows:     blockRows,
			}
			opts.CacheCfg = &e.CacheCfg{
				IndexCapacity:  blockRows * blockCntPerSegment * 80,
				InsertCapacity: blockRows * uint64(colCnt) * 2000,
				DataCapacity:   blockRows * uint64(colCnt) * 2000,
			}
			opts.MetaCleanerCfg = &e.MetaCleanerCfg{
				Interval: time.Duration(1) * time.Second,
			}
			opts.Meta.Conf = mdCfg
			return aoe3.NewStorageWithOptions(path, opts)
		}),
		testutil.WithTestAOEClusterUsePebble(),
		testutil.WithTestAOEClusterRaftClusterOptions(
			raftstore.WithTestClusterLogLevel("info"),
			raftstore.WithTestClusterDataPath("./test")))

	c.Start()
	defer func() {
		stdLog.Printf("3>>>>>>>>>>>>>>>>> call stop")
		c.Stop()
	}()

	c.RaftCluster.WaitLeadersByCount(21, time.Second*30)

	stdLog.Printf("driver all started.")

	driver := c.CubeDrivers[0]

	ctlg := NewCatalog(driver)

	_, err := ctlg.getAvailableShard(0)
	assert.NoError(t, err, "getAvailableShard Fail")
	//test CreateDatabase
	var dbids []uint64
	for i := 0; i < databaseCount; i++ {
		dbid, err := ctlg.CreateDatabase(0, testDatabaceName+strconv.Itoa(i), 0)
		assert.NoError(t, err, "CreateDatabase%v Fail", i)
		dbids = append(dbids, dbid)
	}
	_, err = ctlg.CreateDatabase(0, testDatabaceName+strconv.Itoa(0), 0)
	assert.Equal(t, ErrDBCreateExists, err, "CreateDatabase: wrong err")
	//test ListDatabases
	schemas, err := ctlg.ListDatabases()
	assert.NoError(t, err, "ListDatabases Fail")
	assert.Equal(t, len(schemas), len(dbids), "ListDatabases: Wrong len")
	// fmt.Print(schema)
	//test GetDatabase
	schema, err := ctlg.GetDatabase(testDatabaceName + "0")
	assert.NoError(t, err, "GetDatabase Fail")
	assert.Equal(t, schema.Id, dbids[0], "GetDatabase: Wrong id")
	_, err = ctlg.GetDatabase(testDatabaceName)
	assert.Equal(t, ErrDBNotExists, err, "GetDatabase: wrong err")
	//test CreateTable
	tempTable := &aoe.TableInfo{Name: "mocktbl0"}
	_, err = ctlg.CreateTable(0, dbids[0], *tempTable)
	assert.NotNil(t, err, "CreateTable: create a table with 0 column.")
	var createIds []uint64
	for i := 0; i < tableCount; i++ {
		createId, err := ctlg.CreateTable(0, dbids[0], *testTables[i])
		assert.NoError(t, err, "CreateTable%v Fail", i)
		createIds = append(createIds, createId)
	}
	_, err = ctlg.CreateTable(0, dbids[0], *testTables[0])
	assert.Equal(t, ErrTableCreateExists, err, "CreateTable: wrong err")
	_, err = ctlg.CreateTable(0, dbids[0], aoe.TableInfo{})
	assert.Equal(t, errors.New("Call CreateTable Failed,invalid schema"), err, "CreateTable: wrong err") //after adding these two lines, database 41 can be dropped
	//test ListTables
	tables, err := ctlg.ListTables(dbids[0])
	assert.NoError(t, err, "ListTables Fail")
	assert.Equal(t, len(tables), tableCount, "ListTables: Wrong len")
	//test GetTable
	table, err := ctlg.GetTable(dbids[0], "mocktbl0")
	assert.NoError(t, err, "GetTable Fail")
	assert.Equal(t, table.Id, createIds[0], "GetTable: Wrong id")
	assert.Equal(t, table.Name, "mocktbl0", "GetTable: Wrong Name")
	_, err = ctlg.GetTable(dbids[0], "mocktbl")
	assert.Equal(t, ErrTableNotExists, err, "GetTable: wrong err")
	//test GetTablets
	tablets, err := ctlg.GetTablets(dbids[0], "mocktbl0")
	assert.NoError(t, err, "GetTablets Fail")
	for i := range tablets {
		assert.Equal(t, tablets[i].Table.Id, createIds[0], "GetTablets: Wrong id")
		assert.Equal(t, tablets[i].Table.Name, "mocktbl0", "GetTablets: Wrong Name")
	}
	//test DropTable
	dropId, err := ctlg.DropTable(0, dbids[0], "mocktbl0")
	assert.NoError(t, err, "DropTable Fail")
	assert.Equal(t, createIds[0], dropId, "DropTable: Wrong id")
	_, err = ctlg.GetTable(dbids[0], "mocktbl0")
	assert.Equal(t, ErrTableNotExists, err, "DropTable: GetTable wrong err")
	_, err = ctlg.DropTable(0, dbids[0], "mocktbl0")
	assert.Equal(t, ErrTableNotExists, err, "DropTable: DropTable wrong err")
	_, err = ctlg.GetTablets(dbids[0], "mocktbl0")
	assert.Equal(t, ErrTableNotExists, err, "DropTable: GetTablets wrong err")
	_, err = ctlg.checkTableExists(dbids[0], createIds[0])
	assert.Equal(t, ErrTableNotExists, err, "DropTable: checkTableExists wrong err")
	//test RemoveDeletedTable
	cnt, err := ctlg.RemoveDeletedTable(0)
	assert.NoError(t, err, "RemoveDeletedTable Fail")
	assert.Equal(t, cnt, 1, "RemoveDeletedTable: Wrong id")
	//test DropDatabase
	for i := 0; i < databaseCount; i++ {
		err = ctlg.DropDatabase(0, testDatabaceName+strconv.Itoa(i))
		assert.NoError(t, err, "DropDatabase%v Fail", i)
	}
	_, err = ctlg.GetDatabase(testDatabaceName + "0")
	assert.Equal(t, ErrDBNotExists, err, "DropDatabase: GetDatabase wrong err")
	_, err = ctlg.DropTable(0, dbids[0], "mocktbl0")
	assert.Equal(t, ErrDBNotExists, err, "DropDatabase: DropTable wrong err")
	_, err = ctlg.ListTables(dbids[0])
	assert.Equal(t, ErrDBNotExists, err, "DropDatabase: ListTables wrong err")
	_, err = ctlg.GetTable(dbids[0], "mocktbl0")
	assert.Equal(t, ErrDBNotExists, err, "DropDatabase: GetTable wrong err")
	_, err = ctlg.GetTablets(dbids[0], "mocktbl0")
	assert.Equal(t, ErrDBNotExists, err, "DropDatabase: GetTablets wrong err")
	_, err = ctlg.CreateTable(0, dbids[0], *testTables[0])
	assert.Equal(t, ErrDBNotExists, err, "CreateDatabase: wrong err")
	err = ctlg.DropDatabase(0, testDatabaceName+strconv.Itoa(0))
	assert.Equal(t, ErrDBNotExists, err, "DropDatabase: DropDatabase wrong err")
	//test genGlobalUniqIDs
	_, err = ctlg.genGlobalUniqIDs([]byte(""))
	assert.NoError(t, err, "genGlobalUniqIDs Fail")
	//test dropTables
	err = ctlg.dropTables(0, dbids[0])
	assert.Equal(t, ErrDBNotExists, err, "dropTables: wrong err")
	//test allocId
	_, err = ctlg.allocId("unsupported")
	assert.Equal(t, errors.New("unsupported id category"), err, "allocId: wrong err")
}
