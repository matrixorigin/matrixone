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
	"fmt"
	"net"
	"path/filepath"
	"sync"

	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixcube/aware"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/executor/simple"
	"github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
)

func (_ Def2) Nodes(
	configs NodeConfigs,
	tempDir fz.TempDir,
	numNodes fz.NumNodes,
	logger fz.Logger,
	listenHost ListenHost,
) (nodes fz.Nodes) {

	host := string(listenHost)

	var endpoints []string
	for i := fz.NumNodes(0); i < numNodes; i++ {
		endpoints = append(endpoints, "http://"+net.JoinHostPort(host, randPort()))
	}

	for nodeID, conf := range configs {

		node := &Node{}

		node.Logger = logger

		fs := vfs.Default
		conf.FS = fs

		conf.RaftAddr = net.JoinHostPort(host, randPort())
		conf.ClientAddr = net.JoinHostPort(host, randPort())
		conf.DataPath = filepath.Join(string(tempDir), fmt.Sprintf("data-%d", nodeID))
		conf.Logger = logger

		conf.Prophet.DataDir = filepath.Join(string(tempDir), fmt.Sprintf("prophet-%d", nodeID))
		conf.Prophet.RPCAddr = net.JoinHostPort(host, randPort())
		if nodeID > 0 {
			conf.Prophet.EmbedEtcd.Join = endpoints[0]
		}
		conf.Prophet.EmbedEtcd.PeerUrls = endpoints[nodeID]
		conf.Prophet.EmbedEtcd.ClientUrls = "http://" + net.JoinHostPort(host, randPort())

		conf.Storage = func() config.StorageConfig {

			// pebble
			//kvStorage, err := pebble.NewStorage(
			//	fs.PathJoin(string(tempDir), fmt.Sprintf("storage-%d", nodeID)),
			//	logger,
			//	&crdbpebble.Options{},
			//)
			//ce(err)

			// memory
			kvStorage := mem.NewStorage()

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

		cond := sync.NewCond(new(sync.Mutex))
		var created, isLeader, isFollower bool
		conf.Customize.CustomShardStateAwareFactory = func() aware.ShardStateAware {
			return &shardStateAware{
				created: func(shard meta.Shard) {
					cond.L.Lock()
					created = true
					cond.L.Unlock()
					cond.Broadcast()
				},
				updated: func(shard meta.Shard) {
				},
				splited: func(shard meta.Shard) {
				},
				destroyed: func(shard meta.Shard) {
				},
				becomeLeader: func(shard meta.Shard) {
					cond.L.Lock()
					isLeader = true
					isFollower = false
					cond.L.Unlock()
					cond.Broadcast()
				},
				becomeFollower: func(shard meta.Shard) {
					cond.L.Lock()
					isLeader = false
					isFollower = true
					cond.L.Unlock()
					cond.Broadcast()
				},
				snapshotApplied: func(shard meta.Shard) {
				},
			}
		}

		store := raftstore.NewStore(conf)
		store.Start()
		if nodeID == 0 {
			cond.L.Lock()
			for !isLeader || !created {
				cond.Wait()
			}
			cond.L.Unlock()
		} else {
			cond.L.Lock()
			for !isFollower {
				cond.Wait()
			}
			cond.L.Unlock()
		}
		node.RaftStore = store

		nodes = append(nodes, node)
	}

	return
}
