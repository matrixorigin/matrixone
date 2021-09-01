package frontend

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	pConfig "github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	cConfig "github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/server"
	cPebble "github.com/matrixorigin/matrixcube/storage/pebble"
	"github.com/matrixorigin/matrixcube/vfs"
	stdLog "log"
	dist2 "matrixone/pkg/vm/engine/dist"
	"matrixone/pkg/vm/engine/dist/aoe"
	config2 "matrixone/pkg/vm/engine/dist/config"
	"os"
	"sync"
	"testing"
	"time"
)

var (
	tmpDir = "./cube-test"
)

type TestCluster struct {
	T            *testing.T
	Applications []dist2.CubeDriver
	AOEDBs       []*aoe.Storage
}

func NewTestClusterStore(t *testing.T, reCreate bool,
	f func(path string) (*aoe.Storage, error),
	pcis []*PDCallbackImpl, nodeCnt int) (*TestCluster, error) {
	if reCreate {
		stdLog.Printf("clean target dir")
		if err := recreateTestTempDir(); err != nil {
			return nil, err
		}
	}
	c := &TestCluster{T: t}
	var wg sync.WaitGroup
	for i := 0; i < nodeCnt; i++ {
		metaStorage, err := cPebble.NewStorage(fmt.Sprintf("%s/pebble/meta-%d", tmpDir, i), &pebble.Options{
			FS: vfs.NewPebbleFS(vfs.Default),
		})
		if err != nil {
			return nil, err
		}
		pebbleDataStorage, err := cPebble.NewStorage(fmt.Sprintf("%s/pebble/data-%d", tmpDir, i), &pebble.Options{
			FS: vfs.NewPebbleFS(vfs.Default),
		})
		var aoeDataStorage *aoe.Storage
		if err != nil {
			return nil, err
		}
		if f == nil {
			aoeDataStorage, err = aoe.NewStorage(fmt.Sprintf("%s/aoe-%d", tmpDir, i))
		} else {
			aoeDataStorage, err = f(fmt.Sprintf("%s/aoe-%d", tmpDir, i))
		}

		if err != nil {
			return nil, err
		}
		cfg := config2.Config{}
		cfg.ServerConfig = server.Cfg{
			Addr: fmt.Sprintf("127.0.0.1:809%d", i),
		}
		cfg.ClusterConfig = config2.ClusterConfig{
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

		if i < len(pcis) {
			cfg.CubeConfig.Customize.CustomStoreHeartbeatDataProcessor = pcis[i]
		}

		if i != 0 {
			cfg.CubeConfig.Prophet.EmbedEtcd.Join = "http://127.0.0.1:40000"
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			a, err := dist2.NewCubeDriverWithOptions(metaStorage, pebbleDataStorage, aoeDataStorage, &cfg)
			if err != nil {
				fmt.Printf("create failed with %v", err)
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
