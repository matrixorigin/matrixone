package catalog

import (
	// "errors"
	"fmt"
	"testing"
	stdLog "log"
	"time"

	"matrixone/pkg/vm/driver"
	aoeDriver "matrixone/pkg/vm/driver/aoe"
	dConfig "matrixone/pkg/vm/driver/config"
	aoeStorage "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/driver/testutil"
	"matrixone/pkg/vm/driver/config"
	e "matrixone/pkg/vm/engine/aoe/storage"
	aoe3 "matrixone/pkg/vm/driver/aoe"

	cPebble "github.com/matrixorigin/matrixcube/storage/pebble"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/matrixorigin/matrixcube/raftstore"

	"github.com/BurntSushi/toml"
	"github.com/cockroachdb/pebble"
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

var targetDir="."
var configDir="../../cmd/generate-config/system_vars_def.toml"
var testDatabaceName="test_db"
var testTable=aoe.TableInfo{Name: "test_tb"}
func TestCatalog(t *testing.T){
	metaStorage, _ := cPebble.NewStorage(targetDir+"/pebble/meta", &pebble.Options{
		FS: vfs.NewPebbleFS(vfs.Default),
	})
	pebbleDataStorage, _ := cPebble.NewStorage(targetDir+"/pebble/data", &pebble.Options{
		FS: vfs.NewPebbleFS(vfs.Default),
	})
	opt := aoeStorage.Options{}
	toml.DecodeFile(configDir, &opt)
	aoeDataStorage, _ := aoeDriver.NewStorageWithOptions(targetDir+"/aoe", &opt)
	cfg := dConfig.Config{}
	toml.DecodeFile(configDir, &cfg.CubeConfig)
	a, _ := driver.NewCubeDriverWithOptions(metaStorage, pebbleDataStorage, aoeDataStorage, &cfg)
	a.Start()
	c := NewCatalog(a)
	shardid,err:=c.getAvailableShard(0)
	fmt.Print(shardid)
	assert.NoError(t,err,"getAvailableShard Fail")
	//test CreateDatabase
	dbid,err:=c.CreateDatabase(0,testDatabaceName,0)
	assert.NoError(t,err,"CreateDatabase Fail")
	// _,err=c.CreateDatabase(0,testDatabaceName,0)
	// assert.Equal(t,err,errors.New("db already exists"),"CreateExistingDatabase Fail")
	//test ListDatabases
	schemas,err:=c.ListDatabases()
	assert.NoError(t,err,"ListDatabases Fail")
	assert.Equal(t,schemas[0].Name,testDatabaceName,"ListDatabases: Wrong name")
	assert.Equal(t,schemas[0].Id,dbid,"ListDatabases: Wrong id")
	// fmt.Print(schema)
	//test GetDatabase
	schema,err:=c.GetDatabase(testDatabaceName)
	assert.NoError(t,err,"GetDatabase Fail")
	assert.Equal(t,schema.Id,dbid,"GetDatabase: Wrong id")
	//test CreateTable
	_,err=c.CreateTable(0,dbid,testTable)
	assert.NoError(t,err,"CreateTable Fail")
	//test GetDatabase
	// schema,err:=c.DropTable(testDatabaceName)
	// assert.NoError(t,err,"GetDatabase Fail")
	// assert.Equal(t,schema.Id,dbid,"GetDatabase: Wrong id")
	// fmt.Print(schema)
	//test DropDatabase
	err=c.DropDatabase(0,testDatabaceName)
	assert.NoError(t,err,"DropDatabase Fail")
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
	_,err=ctlg.CreateTable(0,dbid,testTable)
	assert.NoError(t,err,"CreateTable Fail")
	//test GetDatabase
	// schema,err:=c.DropTable(testDatabaceName)
	// assert.NoError(t,err,"GetDatabase Fail")
	// assert.Equal(t,schema.Id,dbid,"GetDatabase: Wrong id")
	// fmt.Print(schema)
	//test DropDatabase
	err=ctlg.DropDatabase(0,testDatabaceName)
	assert.NoError(t,err,"DropDatabase Fail")
}