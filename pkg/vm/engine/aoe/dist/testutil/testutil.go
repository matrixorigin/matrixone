package testutil

import (
	"fmt"
	stdLog "log"
	"matrixone/pkg/vm/engine/aoe/dist"
	daoe "matrixone/pkg/vm/engine/aoe/dist/aoe"
	"matrixone/pkg/vm/engine/aoe/dist/config"
	"os"
	"sync"
	"testing"
	"time"

	log "github.com/fagongzi/log"
	pConfig "github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	cConfig "github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/server"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/mem"
	"github.com/matrixorigin/matrixcube/storage/pebble"
	"github.com/stretchr/testify/assert"
)

var (
	tmpDir = "/tmp/aoe-cluster-test/"
)

// TestAOEClusterOption the option for create TestAOECluster
type TestAOEClusterOption func(opts *testAOEClusterOptions)

type testAOEClusterOptions struct {
	raftOptions       []raftstore.TestClusterOption
	aoeFactoryFunc    func(path string) (*daoe.Storage, error)
	metaFactoryFunc   func(path string) (storage.MetadataStorage, error)
	kvDataFactoryFunc func(path string) (storage.DataStorage, error)
	usePebble         bool
}

// WithTestAOEClusterUsePebble set use pebble
func WithTestAOEClusterUsePebble() TestAOEClusterOption {
	return func(opts *testAOEClusterOptions) {
		opts.usePebble = true
	}
}

// WithTestAOEClusterRaftClusterOptions set raftstore test cluster options
func WithTestAOEClusterRaftClusterOptions(values ...raftstore.TestClusterOption) TestAOEClusterOption {
	return func(opts *testAOEClusterOptions) {
		opts.raftOptions = values
	}
}

// WithTestAOEClusterAOEStorageFunc set aoe storage func
func WithTestAOEClusterAOEStorageFunc(value func(path string) (*daoe.Storage, error)) TestAOEClusterOption {
	return func(opts *testAOEClusterOptions) {
		opts.aoeFactoryFunc = value
	}
}

// WithTestAOEClusterMetaStorageFunc set metadata storage func
func WithTestAOEClusterMetaStorageFunc(value func(path string) (storage.MetadataStorage, error)) TestAOEClusterOption {
	return func(opts *testAOEClusterOptions) {
		opts.metaFactoryFunc = value
	}
}

// WithTestAOEClusterKVDataStorageFunc set kv data storage func
func WithTestAOEClusterKVDataStorageFunc(value func(path string) (storage.DataStorage, error)) TestAOEClusterOption {
	return func(opts *testAOEClusterOptions) {
		opts.kvDataFactoryFunc = value
	}
}

func newTestAOEClusterOptions() *testAOEClusterOptions {
	return &testAOEClusterOptions{}
}

func (opts *testAOEClusterOptions) adjust() {
	if opts.aoeFactoryFunc == nil {
		opts.aoeFactoryFunc = daoe.NewStorage
	}

	if opts.metaFactoryFunc == nil {
		opts.metaFactoryFunc = func(path string) (storage.MetadataStorage, error) {
			if opts.usePebble {
				return pebble.NewStorage(path)
			}

			return mem.NewStorage(), nil
		}
	}

	if opts.kvDataFactoryFunc == nil {
		opts.kvDataFactoryFunc = func(path string) (storage.DataStorage, error) {
			if opts.usePebble {
				return pebble.NewStorage(path)
			}

			return mem.NewStorage(), nil
		}
	}

	if opts.aoeFactoryFunc == nil {
		opts.aoeFactoryFunc = daoe.NewStorage
	}
}

// TestAOECluster is a test cluster for testing.
type TestAOECluster struct {
	t                *testing.T
	opts             *testAOEClusterOptions
	RaftCluster      *raftstore.TestRaftCluster
	CubeDrivers      []dist.CubeDriver
	AOEStorages      []*daoe.Storage
	MetadataStorages []storage.MetadataStorage
	DataStorages     []storage.DataStorage
}

func NewTestAOECluster(t *testing.T, cfgCreator func(node int) *config.Config, opts ...TestAOEClusterOption) *TestAOECluster {
	c := &TestAOECluster{t: t, opts: newTestAOEClusterOptions()}
	for _, opt := range opts {
		opt(c.opts)
	}
	c.opts.adjust()

	c.opts.raftOptions = append(c.opts.raftOptions, raftstore.WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *cConfig.Config) {
		meta, err := c.opts.metaFactoryFunc(fmt.Sprintf("%s/meta", cfg.DataPath))
		assert.NoError(t, err)
		c.MetadataStorages = append(c.MetadataStorages, meta)

		data, err := c.opts.kvDataFactoryFunc(fmt.Sprintf("%s/data", cfg.DataPath))
		assert.NoError(t, err)
		c.DataStorages = append(c.DataStorages, data)

		aoe, err := c.opts.aoeFactoryFunc(fmt.Sprintf("%s/aoe", cfg.DataPath))
		assert.NoError(t, err)
		c.AOEStorages = append(c.AOEStorages, aoe)

		cfg.Replication.ShardHeartbeatDuration = typeutil.NewDuration(time.Millisecond * 100)
		cfg.Replication.StoreHeartbeatDuration = typeutil.NewDuration(time.Second)
		cfg.Raft.TickInterval = typeutil.NewDuration(time.Millisecond * 600)
		cfg.Raft.MaxEntryBytes = 300 * 1024 * 1024
		cfg.Replication.ShardCapacityBytes = 100
		cfg.Replication.ShardSplitCheckBytes = 80
	}), raftstore.WithTestClusterStoreFactory(func(node int, cfg *cConfig.Config) raftstore.Store {
		dCfg := cfgCreator(node)
		dCfg.CubeConfig = *cfg
		d, err := dist.NewCubeDriverWithFactory(c.MetadataStorages[node], c.DataStorages[node], c.AOEStorages[node], dCfg, func(c *cConfig.Config) (raftstore.Store, error) {
			return raftstore.NewStore(c), nil
		})
		assert.NoError(t, err)
		c.CubeDrivers = append(c.CubeDrivers, d)
		return d.RaftStore()
	}), raftstore.WithTestClusterNodeStartFunc(func(node int, store raftstore.Store) {
		assert.NoError(t, c.CubeDrivers[node].Start())
	}))

	c.RaftCluster = raftstore.NewTestClusterStore(t, c.opts.raftOptions...)
	return c
}

func (c *TestAOECluster) Start() {
	c.RaftCluster.Start()
}

func (c *TestAOECluster) Stop() {
	c.RaftCluster.Stop()
	for _, d := range c.CubeDrivers {
		d.Close()
	}

	for _, s := range c.MetadataStorages {
		assert.NoError(c.t, s.Close())
	}

	for _, s := range c.DataStorages {
		assert.NoError(c.t, s.Close())
	}

	for _, s := range c.AOEStorages {
		assert.NoError(c.t, s.Close())
	}
}

type TestCluster struct {
	T            *testing.T
	Applications []dist.CubeDriver
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
		cfg := &config.Config{}
		cfg.ServerConfig = server.Cfg{
			ExternalServer: true,
		}
		cfg.ClusterConfig = config.ClusterConfig{
			PreAllocatedGroupNum: 5,
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
			a, err := dist.NewCubeDriverWithOptions(metaStorage, pebbleDataStorage, aoeDataStorage, cfg)
			if err != nil {
				log.Fatal("create failed with %+v", err)
			}
			err = a.Start()
			if err != nil {
				log.Fatal("cube driver start failed with %+v", err)
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
