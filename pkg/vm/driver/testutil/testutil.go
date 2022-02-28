// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testutil

import (
	"fmt"
	stdLog "log"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/matrixorigin/matrixone/pkg/vm/driver"
	aoe2 "github.com/matrixorigin/matrixone/pkg/vm/driver/aoe"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/config"
	kvDriver "github.com/matrixorigin/matrixone/pkg/vm/driver/kv"

	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	cConfig "github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	cPebble "github.com/matrixorigin/matrixcube/storage/kv/pebble"
	"github.com/stretchr/testify/assert"
)

// TestAOEClusterOption the option for create TestAOECluster
type TestAOEClusterOption func(opts *testAOEClusterOptions)

type testAOEClusterOptions struct {
	raftOptions    []raftstore.TestClusterOption
	aoeFactoryFunc func(path string) (*aoe2.Storage, error)
	// metaFactoryFunc   func(path string) (storage.MetadataStorage, error)
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
func WithTestAOEClusterAOEStorageFunc(value func(path string) (*aoe2.Storage, error)) TestAOEClusterOption {
	return func(opts *testAOEClusterOptions) {
		opts.aoeFactoryFunc = value
	}
}

// // WithTestAOEClusterMetaStorageFunc set metadata storage func
// func WithTestAOEClusterMetaStorageFunc(value func(path string) (storage.MetadataStorage, error)) TestAOEClusterOption {
// 	return func(opts *testAOEClusterOptions) {
// 		opts.metaFactoryFunc = value
// 	}
// }

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
		opts.aoeFactoryFunc = aoe2.NewStorage
	}

	// if opts.metaFactoryFunc == nil {
	// 	opts.metaFactoryFunc = func(path string) (storage.MetadataStorage, error) {
	// 		if opts.usePebble {
	// 			return cPebble.NewStorage(path, &pebble.Options{
	// 				FS: vfs.NewPebbleFS(vfs.Default),
	// 			})
	// 		}

	// 		return mem.NewStorage(vfs.Default), nil
	// 	}
	// }

	if opts.kvDataFactoryFunc == nil {
		opts.kvDataFactoryFunc = func(path string) (storage.DataStorage, error) {
			if opts.usePebble {
				kvs, err := cPebble.NewStorage(path, nil, &pebble.Options{
					FS: vfs.NewPebbleFS(vfs.Default),
				})
				kvBase := kv.NewBaseStorage(kvs, vfs.Default)
				dataStorage := kv.NewKVDataStorage(kvBase, kvDriver.NewkvExecutor(kvs))
				return dataStorage, err
			}

			kvs := mem.NewStorage()
			kvBase := kv.NewBaseStorage(kvs, vfs.Default)
			dataStorage := kv.NewKVDataStorage(kvBase, kvDriver.NewkvExecutor(kvs))
			return dataStorage, nil
		}
	}

	if opts.aoeFactoryFunc == nil {
		opts.aoeFactoryFunc = aoe2.NewStorage
	}
}

// TestAOECluster is a test cluster for testing.
type TestAOECluster struct {
	// init fields
	t              *testing.T
	initOpts       []TestAOEClusterOption
	initCfgCreator func(node int) *config.Config
	nodes          int

	// reset fields
	opts         *testAOEClusterOptions
	RaftCluster  raftstore.TestRaftCluster
	CubeDrivers  []driver.CubeDriver
	AOEStorages  []*aoe2.Storage
	DataStorages []storage.DataStorage
}

func NewTestAOECluster(t *testing.T, cfgCreator func(node int) *config.Config, opts ...TestAOEClusterOption) *TestAOECluster {
	c := &TestAOECluster{t: t, initOpts: opts, initCfgCreator: cfgCreator}
	c.reset()
	c.RaftCluster.EveryStore(func(i int, store raftstore.Store) {
		c.nodes++
	})
	return c
}

func (c *TestAOECluster) reset(opts ...raftstore.TestClusterOption) {
	c.RaftCluster = nil
	c.CubeDrivers = nil
	c.AOEStorages = nil
	c.DataStorages = nil

	c.opts = newTestAOEClusterOptions()
	for _, opt := range c.initOpts {
		opt(c.opts)
	}
	c.opts.adjust()

	c.opts.raftOptions = append(c.opts.raftOptions, opts...)
	c.opts.raftOptions = append(c.opts.raftOptions, raftstore.WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *cConfig.Config) {
		data, err := c.opts.kvDataFactoryFunc(fmt.Sprintf("%s/data", cfg.DataPath))
		assert.NoError(c.t, err)
		if len(c.DataStorages) > node {
			c.DataStorages[node] = data
		} else {
			c.DataStorages = append(c.DataStorages, data)
		}

		aoe, err := c.opts.aoeFactoryFunc(fmt.Sprintf("%s/aoe", cfg.DataPath))
		assert.NoError(c.t, err)
		if len(c.AOEStorages) > node {
			c.AOEStorages[node] = aoe
		} else {
			c.AOEStorages = append(c.AOEStorages, aoe)
		}

		// avoid create raftstore kv storage, this filed will reset in cube driver
		cfg.Storage.DataStorageFactory = func(group uint64) storage.DataStorage { return nil }

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
		// dCfg.ServerConfig.ExternalServer = true
		d, err := driver.NewCubeDriverWithFactory(c.DataStorages[node], c.AOEStorages[node], dCfg, func(c *cConfig.Config) (raftstore.Store, error) {
			s := raftstore.NewStore(c)
			types := []metapb.JobType{metapb.JobType_RemoveResource, metapb.JobType_CreateResourcePool, metapb.JobType_CustomStartAt}
			for _, t := range types {
				if v := c.Prophet.GetJobProcessor(t); v != nil {
					dCfg.CubeConfig.Prophet.RegisterJobProcessor(t, v)
				}
			}
			return s, nil
		})
		assert.NoError(c.t, err)
		if len(c.CubeDrivers) > node {
			c.CubeDrivers[node] = d
		} else {
			c.CubeDrivers = append(c.CubeDrivers, d)
		}
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

func (c *TestAOECluster) StopNode(n int) {
	c.RaftCluster.StopNode(n)
	c.CubeDrivers[n].Close()
	c.DataStorages[n].Close()
	c.AOEStorages[n].Close()
}

func (c *TestAOECluster) RestartNode(n int) {
	c.RaftCluster.RestartNode(n)
}

func (c *TestAOECluster) Stop() {
	c.RaftCluster.Stop()
	for _, d := range c.CubeDrivers {
		d.Close()
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
	Applications []driver.CubeDriver
	AOEDBs       []*aoe2.Storage
}

func (c *TestCluster) Stop() {
	for _, s := range c.Applications {
		s.Close()
	}
}
