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

package main

import (
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	crdbpebble "github.com/cockroachdb/pebble"
	"github.com/matrixorigin/matrixcube/aware"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/server"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/executor/simple"
	"github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixcube/storage/kv/pebble"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/matrixorigin/matrixone/pkg/chaostesting"
	"github.com/reusee/e4"
	"go.uber.org/zap"
)

func TestNewCubeCluster(t *testing.T) {
	defer he(nil, e4.TestingFatal(t))

	NewScope().Call(func(
		scope Scope,
		cleanup fz.Cleanup,
		tempDir fz.TempDir,
		defaultConfig DefaultCubeConfig,
		randomize RandomizeCubeConfig,
	) {
		defer cleanup()

		// port generator
		port := int64(54321)
		nextPort := func() (ret string) {
			ret = fmt.Sprintf("%d", port)
			port++
			return
		}

		// number of nodes in cluster
		numNodes := 3

		var prophetEtcdEndpoints []string
		for i := 0; i < numNodes; i++ {
			prophetEtcdEndpoints = append(prophetEtcdEndpoints, "http://"+net.JoinHostPort("localhost", nextPort()))
		}

		cond := sync.NewCond(new(sync.Mutex))
		numCreated := 0
		numLeaderReady := 0

		for i := 0; i < numNodes; i++ {
			i := i

			conf := defaultConfig(i)
			randomize(conf)

			loggerConfigStr := `{
        "level": "debug",
        "encoding": "json"
      }`
			var loggerConfig zap.Config
			ce(json.NewDecoder(strings.NewReader(loggerConfigStr)).Decode(&loggerConfig))
			logger, err := loggerConfig.Build()
			ce(err)
			defer logger.Sync()

			fs := vfs.Default
			conf.FS = fs

			conf.RaftAddr = net.JoinHostPort("127.0.0.1", nextPort())
			conf.ClientAddr = net.JoinHostPort("127.0.0.1", nextPort())
			conf.DataPath = filepath.Join(string(tempDir), fmt.Sprintf("data-%d", i))
			conf.Logger = logger

			conf.Prophet.DataDir = filepath.Join(string(tempDir), fmt.Sprintf("prophet-%d", i))
			conf.Prophet.RPCAddr = net.JoinHostPort("127.0.0.1", nextPort())
			if i > 0 {
				conf.Prophet.EmbedEtcd.Join = prophetEtcdEndpoints[0]
			}
			conf.Prophet.EmbedEtcd.ClientUrls = "http://" + net.JoinHostPort("localhost", nextPort())
			conf.Prophet.EmbedEtcd.PeerUrls = prophetEtcdEndpoints[i]

			conf.Storage = func() config.StorageConfig {
				kvStorage, err := pebble.NewStorage(
					fs.PathJoin(string(tempDir), fmt.Sprintf("storage-%d", i)),
					logger,
					&crdbpebble.Options{},
				)
				ce(err)
				base := kv.NewBaseStorage(kvStorage, fs)
				dataStorage := kv.NewKVDataStorage(base, simple.NewSimpleKVExecutor(kvStorage))
				return config.StorageConfig{
					DataStorageFactory: func(group uint64) storage.DataStorage {
						return dataStorage
					},
					ForeachDataStorageFunc: func(fn func(storage.DataStorage)) {
						fn(dataStorage)
					},
				}
			}()

			conf.Customize = config.CustomizeConfig{
				CustomShardStateAwareFactory: func() aware.ShardStateAware {
					return &cubeShardStateAware{
						created: func(_ meta.Shard) {
							cond.L.Lock()
							numCreated++
							cond.L.Unlock()
							cond.Signal()
						},
						becomeLeader: func(_ meta.Shard) {
							cond.L.Lock()
							numLeaderReady++
							cond.L.Unlock()
							cond.Signal()
						},
					}
				},
			}

			store := raftstore.NewStore(conf)

			app := server.NewApplication(server.Cfg{
				Store: store,
			})

			ce(app.Start())

		}

		cond.L.Lock()
		for numLeaderReady == 0 {
			cond.Wait()
		}
		cond.L.Unlock()

	})
}

type cubeShardStateAware struct {
	created         func(meta.Shard)
	updated         func(meta.Shard)
	splited         func(meta.Shard)
	destroyed       func(meta.Shard)
	becomeLeader    func(meta.Shard)
	becomeFollower  func(meta.Shard)
	snapshotApplied func(meta.Shard)
}

var _ aware.ShardStateAware = new(cubeShardStateAware)

func (c *cubeShardStateAware) Created(shard meta.Shard) {
	if c.created != nil {
		c.created(shard)
	}
}

func (c *cubeShardStateAware) Updated(shard meta.Shard) {
	if c.updated != nil {
		c.updated(shard)
	}
}

func (c *cubeShardStateAware) Splited(shard meta.Shard) {
	if c.splited != nil {
		c.splited(shard)
	}
}

func (c *cubeShardStateAware) Destroyed(shard meta.Shard) {
	if c.destroyed != nil {
		c.destroyed(shard)
	}
}

func (c *cubeShardStateAware) BecomeLeader(shard meta.Shard) {
	if c.becomeLeader != nil {
		c.becomeLeader(shard)
	}
}

func (c *cubeShardStateAware) BecomeFollower(shard meta.Shard) {
	if c.becomeFollower != nil {
		c.becomeFollower(shard)
	}
}

func (c *cubeShardStateAware) SnapshotApplied(shard meta.Shard) {
	if c.snapshotApplied != nil {
		c.snapshotApplied(shard)
	}
}
