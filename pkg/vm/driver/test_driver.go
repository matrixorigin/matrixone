package driver

import (
	// "errors"
	// "fmt"
	"testing"

	aoeDriver "matrixone/pkg/vm/driver/aoe"
	dConfig "matrixone/pkg/vm/driver/config"
	aoeStorage "matrixone/pkg/vm/engine/aoe/storage"
	// "matrixone/pkg/vm/engine/aoe"

	cPebble "github.com/matrixorigin/matrixcube/storage/pebble"
	"github.com/matrixorigin/matrixcube/vfs"

	"github.com/BurntSushi/toml"
	"github.com/cockroachdb/pebble"
	// "github.com/stretchr/testify/assert"
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
	a, _ := NewCubeDriverWithOptions(metaStorage, pebbleDataStorage, aoeDataStorage, &cfg)
	a.Start()
}