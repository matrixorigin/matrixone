package catalog

import (
	// "errors"
	"fmt"
	"testing"
	stdLog "log"
	"time"

	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/driver/testutil"
	"matrixone/pkg/vm/driver/config"
	e "matrixone/pkg/vm/engine/aoe/storage"
	aoe3 "matrixone/pkg/vm/driver/aoe"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"

	"github.com/matrixorigin/matrixcube/raftstore"

	"github.com/stretchr/testify/assert"
	"github.com/fagongzi/log"
)
const (
	blockRows          = 10000
	blockCntPerSegment = 2
	colCnt             = 4
	segmentCnt         = 5
	blockCnt           = blockCntPerSegment * segmentCnt
	restart            = false
)
var testDatabaceName="test_db"
var testTable *aoe.TableInfo
func init() {
	testTable = md.MockTableInfo(colCnt)
	testTable.Id = 100
}

func TestCatalogWithUtil(t *testing.T){
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

	shardid,err:=ctlg.getAvailableShard(0)
	fmt.Print(shardid)
	assert.NoError(t,err,"getAvailableShard Fail")
	//test CreateDatabase
	dbid,err:=ctlg.CreateDatabase(0,testDatabaceName,0)
	assert.NoError(t,err,"CreateDatabase Fail")
	// _,err=c.CreateDatabase(0,testDatabaceName,0)
	// assert.Equal(t,err,errors.New("db already exists"),"CreateExistingDatabase Fail")
	//test ListDatabases
	schemas,err:=ctlg.ListDatabases()
	assert.NoError(t,err,"ListDatabases Fail")
	assert.Equal(t,schemas[0].Name,testDatabaceName,"ListDatabases: Wrong name")
	assert.Equal(t,schemas[0].Id,dbid,"ListDatabases: Wrong id")
	// fmt.Print(schema)
	//test GetDatabase
	schema,err:=ctlg.GetDatabase(testDatabaceName)
	assert.NoError(t,err,"GetDatabase Fail")
	assert.Equal(t,schema.Id,dbid,"GetDatabase: Wrong id")
	//test CreateTable
	createId,err:=ctlg.CreateTable(0,dbid,*testTable)
	assert.NoError(t,err,"CreateTable Fail")
	//test ListTables
	tables,err:=ctlg.ListTables(dbid)
	assert.NoError(t,err,"ListTables Fail")
	assert.Equal(t,len(tables),1,"ListTables: Wrong len")
	assert.Equal(t,tables[0].Id,createId,"ListTables: Wrong id")
	assert.Equal(t,tables[0].Name,"mocktbl","ListTables: Wrong Name")
	//test GetTable
	table,err:=ctlg.GetTable(dbid,"mocktbl")
	assert.NoError(t,err,"GetTable Fail")
	assert.Equal(t,table.Id,createId,"GetTable: Wrong id")
	assert.Equal(t,table.Name,"mocktbl","GetTable: Wrong Name")
	//test GetTablets
	tablets,err:=ctlg.GetTablets(dbid,"mocktbl")
	assert.NoError(t,err,"GetTablets Fail")
	for i := range tablets{
		assert.Equal(t,tablets[i].Table.Id,createId,"GetTablets: Wrong id")
		assert.Equal(t,tablets[i].Table.Name,"mocktbl","GetTablets: Wrong Name")
	}
	//test DropTable
	dropId,err:=ctlg.DropTable(0,dbid,"mocktbl")
	assert.NoError(t,err,"DropTable Fail")
	assert.Equal(t,createId,dropId,"DropTable: Wrong id")
	//test RemoveDeletedTable
	cnt,err:=ctlg.RemoveDeletedTable(0)
	assert.NoError(t,err,"RemoveDeletedTable Fail")
	assert.Equal(t,cnt,1,"RemoveDeletedTable: Wrong id")
	// fmt.Print(schema)
	err=ctlg.DropDatabase(0,testDatabaceName)
	assert.NoError(t,err,"DropDatabase Fail")
}