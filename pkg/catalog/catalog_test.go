package catalog

import (
	// "errors"
	"fmt"
	"testing"

	"matrixone/pkg/vm/driver"
	aoeDriver "matrixone/pkg/vm/driver/aoe"
	dConfig "matrixone/pkg/vm/driver/config"
	aoeStorage "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe"

	cPebble "github.com/matrixorigin/matrixcube/storage/pebble"
	"github.com/matrixorigin/matrixcube/vfs"

	"github.com/BurntSushi/toml"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
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