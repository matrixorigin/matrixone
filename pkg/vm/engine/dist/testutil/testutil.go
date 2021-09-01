package testutil

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/matrixorigin/matrixcube/vfs"
	stdLog "log"
	dist2 "matrixone/pkg/vm/engine/dist"
	"matrixone/pkg/vm/engine/dist/aoe"
	config2 "matrixone/pkg/vm/engine/dist/config"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	cConfig "github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/mem"
	cPebble "github.com/matrixorigin/matrixcube/storage/pebble"
	"github.com/stretchr/testify/assert"
)

// TestAOEClusterOption the option for create TestAOECluster
type TestAOEClusterOption func(opts *testAOEClusterOptions)

type testAOEClusterOptions struct {
	raftOptions       []raftstore.TestClusterOption
	aoeFactoryFunc    func(path string) (*aoe.Storage, error)
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
func WithTestAOEClusterAOEStorageFunc(value func(path string) (*aoe.Storage, error)) TestAOEClusterOption {
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
		opts.aoeFactoryFunc = aoe.NewStorage
	}

	if opts.metaFactoryFunc == nil {
		opts.metaFactoryFunc = func(path string) (storage.MetadataStorage, error) {
			if opts.usePebble {
				return cPebble.NewStorage(path, &pebble.Options{
					FS: vfs.NewPebbleFS(vfs.Default),
				})
			}

			return mem.NewStorage(vfs.Default), nil
		}
	}

	if opts.kvDataFactoryFunc == nil {
		opts.kvDataFactoryFunc = func(path string) (storage.DataStorage, error) {
			if opts.usePebble {
				return cPebble.NewStorage(path, &pebble.Options{
					FS: vfs.NewPebbleFS(vfs.Default),
				})
			}

			return mem.NewStorage(vfs.Default), nil
		}
	}

	if opts.aoeFactoryFunc == nil {
		opts.aoeFactoryFunc = aoe.NewStorage
	}
}

// TestAOECluster is a test cluster for testing.
type TestAOECluster struct {
	// init fields
	t              *testing.T
	initOpts       []TestAOEClusterOption
	initCfgCreator func(node int) *config2.Config

	// reset fields
	opts             *testAOEClusterOptions
	RaftCluster      *raftstore.TestRaftCluster
	CubeDrivers      []dist2.CubeDriver
	AOEStorages      []*aoe.Storage
	MetadataStorages []storage.MetadataStorage
	DataStorages     []storage.DataStorage
}

func NewTestAOECluster(t *testing.T, cfgCreator func(node int) *config2.Config, opts ...TestAOEClusterOption) *TestAOECluster {
	c := &TestAOECluster{t: t, initOpts: opts, initCfgCreator: cfgCreator}
	c.reset()
	return c
}

func (c *TestAOECluster) reset(opts ...raftstore.TestClusterOption) {
	c.RaftCluster = nil
	c.CubeDrivers = nil
	c.AOEStorages = nil
	c.MetadataStorages = nil
	c.DataStorages = nil
	c.opts = newTestAOEClusterOptions()

	for _, opt := range c.initOpts {
		opt(c.opts)
	}
	c.opts.adjust()
	c.opts.raftOptions = append(c.opts.raftOptions, opts...)

	c.opts.raftOptions = append(c.opts.raftOptions, raftstore.WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *cConfig.Config) {
		meta, err := c.opts.metaFactoryFunc(fmt.Sprintf("%s/meta", cfg.DataPath))
		assert.NoError(c.t, err)
		c.MetadataStorages = append(c.MetadataStorages, meta)

		data, err := c.opts.kvDataFactoryFunc(fmt.Sprintf("%s/data", cfg.DataPath))
		assert.NoError(c.t, err)
		c.DataStorages = append(c.DataStorages, data)

		aoe, err := c.opts.aoeFactoryFunc(fmt.Sprintf("%s/aoe", cfg.DataPath))
		assert.NoError(c.t, err)
		c.AOEStorages = append(c.AOEStorages, aoe)

		cfg.Replication.ShardHeartbeatDuration = typeutil.NewDuration(time.Millisecond * 100)
		cfg.Replication.StoreHeartbeatDuration = typeutil.NewDuration(time.Second)
		cfg.Raft.TickInterval = typeutil.NewDuration(time.Millisecond * 100)
		cfg.Raft.MaxEntryBytes = 300 * 1024 * 1024
		//cfg.Replication.ShardCapacityBytes = 100
		//cfg.Replication.ShardSplitCheckBytes = 80

		//ppu := client.NewPDCallbackParameterUnit(5, 20, 20, 20)
		//pci := client.NewPDCallbackImpl(ppu)
		//pci.Id = node
		//cfg.Customize.CustomStoreHeartbeatDataProcessor = pci
		//c.PCIs = append(c.PCIs, pci)
	}), raftstore.WithTestClusterStoreFactory(func(node int, cfg *cConfig.Config) raftstore.Store {
		dCfg := c.initCfgCreator(node)
		dCfg.CubeConfig = *cfg
		dCfg.ServerConfig.ExternalServer = true
		d, err := dist2.NewCubeDriverWithFactory(c.MetadataStorages[node], c.DataStorages[node], c.AOEStorages[node], dCfg, func(c *cConfig.Config) (raftstore.Store, error) {
			return raftstore.NewStore(c), nil
		})
		assert.NoError(c.t, err)
		c.CubeDrivers = append(c.CubeDrivers, d)
		return d.RaftStore()
	}), raftstore.WithTestClusterNodeStartFunc(func(node int, store raftstore.Store) {
		err := c.CubeDrivers[node].Start()
		if err != nil {
			stdLog.Printf("Node-%d start failed, %v", node, err)
		}
		assert.NoError(c.t, err)
	}))

	c.RaftCluster = raftstore.NewTestClusterStore(c.t, c.opts.raftOptions...)
}

func (c *TestAOECluster) Start() {
	c.RaftCluster.Start()
}

func (c *TestAOECluster) Restart() {
	c.Stop()
	c.reset(raftstore.WithTestClusterRecreate(false))
	c.Start()
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
	Applications []dist2.CubeDriver
	AOEDBs       []*aoe.Storage
}

func (c *TestCluster) Stop() {
	for _, s := range c.Applications {
		s.Close()
	}
}
