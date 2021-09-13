package catalog

import (
	// "errors"
	"testing"

	"matrixone/pkg/vm/driver"
	aoeDriver "matrixone/pkg/vm/driver/aoe"
	dConfig "matrixone/pkg/vm/driver/config"
	aoeStorage "matrixone/pkg/vm/engine/aoe/storage"

	cPebble "github.com/matrixorigin/matrixcube/storage/pebble"
	"github.com/matrixorigin/matrixcube/vfs"

	"github.com/BurntSushi/toml"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
)

var targetDir="."
var configDir="../../cmd/generate-config/system_vars_def.toml"
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
//test CreateDatabase
	_,err:=c.CreateDatabase(0,"test_db",0)
	assert.NoError(t,err,"CreateDatabase Fail")
	// _,err=c.CreateDatabase(0,"test_db",0)
	// assert.Equal(t,err,errors.New("db already exists"),"CreateExistingDatabase Fail")
//test DropDatabase
	err=c.DropDatabase(0,"test_db")
	assert.NoError(t,err,"DropDatabase Fail")
}