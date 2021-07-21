package testutil

import (
	"fmt"
	pConfig "github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/server"
	"github.com/matrixorigin/matrixcube/storage/pebble"
	"matrixone/pkg/vm/engine/aoe/dist"
	daoe "matrixone/pkg/vm/engine/aoe/dist/aoe"
	"os"
	"testing"
	"time"
)

var (
	tmpDir    = "./cube-test"
)


type TestCluster struct {
	T            *testing.T
	Applications []dist.Storage
}

func NewTestClusterStore(t *testing.T) (*TestCluster, error) {
	if err := recreateTestTempDir(); err != nil {
		return nil, err
	}
	c := &TestCluster{T: t}
	for i := 0; i < 3; i++ {
		metaStorage, err := pebble.NewStorage(fmt.Sprintf("%s/pebble/meta-%d", tmpDir, i))
		if err != nil {
			return nil, err
		}
		pebbleDataStorage, err := pebble.NewStorage(fmt.Sprintf("%s/pebble/data-%d", tmpDir, i))
		if err != nil {
			return nil, err
		}
		aoeDataStorage, err := daoe.NewStorage(fmt.Sprintf("%s/aoe-%d", tmpDir, i))
		if err != nil {
			return nil, err
		}
		a, err := dist.NewStorageWithOptions(metaStorage, pebbleDataStorage, aoeDataStorage, func(cfg *config.Config) {
			cfg.DataPath = fmt.Sprintf("%s/node-%d", tmpDir, i)
			cfg.RaftAddr = fmt.Sprintf("127.0.0.1:1000%d", i)
			cfg.ClientAddr = fmt.Sprintf("127.0.0.1:2000%d", i)

			pConfig.DefaultSchedulers = nil
			cfg.Replication.ShardHeartbeatDuration = typeutil.NewDuration(time.Millisecond * 100)
			cfg.Replication.StoreHeartbeatDuration = typeutil.NewDuration(time.Second)

			cfg.Raft.TickInterval = typeutil.NewDuration(time.Millisecond * 100)

			cfg.Prophet.Name = fmt.Sprintf("node-%d", i)
			cfg.Prophet.StorageNode = true
			cfg.Prophet.RPCAddr = fmt.Sprintf("127.0.0.1:3000%d", i)
			if i != 0 {
				cfg.Prophet.EmbedEtcd.Join = "http://127.0.0.1:40000"
			}
			cfg.Prophet.EmbedEtcd.ClientUrls = fmt.Sprintf("http://127.0.0.1:4000%d", i)
			cfg.Prophet.EmbedEtcd.PeerUrls = fmt.Sprintf("http://127.0.0.1:5000%d", i)
			cfg.Prophet.Schedule.EnableJointConsensus = true

		}, server.Cfg{
			Addr: fmt.Sprintf("127.0.0.1:809%d", i),
		})
		if err != nil {
			return nil, err
		}
		c.Applications = append(c.Applications, a)
	}
	return c, nil
}

func (c *TestCluster) Stop() {
	for _, s := range c.Applications {
		s.Close()
	}
}

func recreateTestTempDir() (err error) {
	err = os.RemoveAll(tmpDir)
	if err != nil {
		return err
	}
	err = os.MkdirAll(tmpDir, os.ModeDir)
	return err
}

func cleanupTmpDir() error {
	return os.RemoveAll(tmpDir)
}