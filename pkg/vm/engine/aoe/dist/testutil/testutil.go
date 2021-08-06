package testutil

import (
	"fmt"
	pConfig "github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	cConfig "github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/server"
	"github.com/matrixorigin/matrixcube/storage/pebble"
	log "github.com/sirupsen/logrus"
	stdLog "log"
	"matrixone/pkg/vm/engine/aoe/dist"
	daoe "matrixone/pkg/vm/engine/aoe/dist/aoe"
	"matrixone/pkg/vm/engine/aoe/dist/config"
	"os"
	"sync"
	"testing"
	"time"
)

var (
	tmpDir = "/tmp/aoe-cluster-test/"
)

type TestCluster struct {
	T            *testing.T
	Applications []dist.Storage
	AOEDBs       []*daoe.Storage
}

func NewTestClusterStore(t *testing.T, reCreate bool, f func(path string) (*daoe.Storage, error)) (*TestCluster, error) {
	if reCreate {
		stdLog.Printf("clean target dir")
		if err := recreateTestTempDir(); err != nil {
			return nil, err
		}
	}
	c := &TestCluster{T: t}
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		metaStorage, err := pebble.NewStorage(fmt.Sprintf("%s/pebble/meta-%d", tmpDir, i))
		if err != nil {
			return nil, err
		}
		pebbleDataStorage, err := pebble.NewStorage(fmt.Sprintf("%s/pebble/data-%d", tmpDir, i))
		var aoeDataStorage *daoe.Storage
		if err != nil {
			return nil, err
		}
		if f == nil {
			aoeDataStorage, err = daoe.NewStorage(fmt.Sprintf("%s/aoe-%d", tmpDir, i))
		} else {
			aoeDataStorage, err = f(fmt.Sprintf("%s/aoe-%d", tmpDir, i))
		}

		if err != nil {
			return nil, err
		}
		cfg := config.Config{}
		cfg.ServerConfig = server.Cfg{
			Addr: fmt.Sprintf("127.0.0.1:809%d", i),
		}
		cfg.ClusterConfig = config.ClusterConfig{
			PreAllocatedGroupNum: 20,
		}
		cfg.CubeConfig = cConfig.Config{
			DataPath:   fmt.Sprintf("%s/node-%d", tmpDir, i),
			RaftAddr:   fmt.Sprintf("127.0.0.1:1000%d", i),
			ClientAddr: fmt.Sprintf("127.0.0.1:2000%d", i),
			Replication: cConfig.ReplicationConfig{
				ShardHeartbeatDuration: typeutil.NewDuration(time.Millisecond * 100),
				StoreHeartbeatDuration: typeutil.NewDuration(time.Second),
			},
			Raft: cConfig.RaftConfig{
				TickInterval:  typeutil.NewDuration(time.Millisecond * 600),
				MaxEntryBytes: 300 * 1024 * 1024,
			},
			Prophet: pConfig.Config{
				Name:        fmt.Sprintf("node-%d", i),
				StorageNode: true,
				RPCAddr:     fmt.Sprintf("127.0.0.1:3000%d", i),
				EmbedEtcd: pConfig.EmbedEtcdConfig{
					ClientUrls: fmt.Sprintf("http://127.0.0.1:4000%d", i),
					PeerUrls:   fmt.Sprintf("http://127.0.0.1:5000%d", i),
				},
				Schedule: pConfig.ScheduleConfig{
					EnableJointConsensus: true,
				},
			},
		}
		if i != 0 {
			cfg.CubeConfig.Prophet.EmbedEtcd.Join = "http://127.0.0.1:40000"
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			a, err := dist.NewStorageWithOptions(metaStorage, pebbleDataStorage, aoeDataStorage, cfg)
			if err != nil {
				log.Fatal("create failed with %+v", err)
			}
			c.AOEDBs = append(c.AOEDBs, aoeDataStorage)
			c.Applications = append(c.Applications, a)
		}()
		if i == 0 {
			time.Sleep(3 * time.Second)
		}
		time.Sleep(2 * time.Second)
	}
	wg.Wait()
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
