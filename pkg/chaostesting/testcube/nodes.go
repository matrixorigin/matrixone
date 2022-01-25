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

	"github.com/matrixorigin/matrixcube/aware"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/executor/simple"
	"github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/vfs"
	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
)

func (_ Def2) Nodes(
	configs NodeConfigs,
	tempDir fz.TempDir,
	numNodes fz.NumNodes,
	logger fz.Logger,
	netHost fz.NetworkHost,
	getPort fz.GetPortStr,
) (nodes fz.Nodes) {

	host := string(netHost)

	var endpoints []string
	for i := fz.NumNodes(0); i < numNodes; i++ {
		endpoints = append(endpoints, "http://"+net.JoinHostPort(host, getPort()))
	}

	for nodeID, conf := range configs {

		node := &Node{
			Cond: sync.NewCond(new(sync.Mutex)),
		}

		node.Logger = logger

		fs := vfs.NewMemFS()
		conf.FS = fs

		conf.RaftAddr = net.JoinHostPort(host, getPort())
		conf.ClientAddr = net.JoinHostPort(host, getPort())
		conf.DataPath = filepath.Join(string(tempDir), fmt.Sprintf("data-%d", nodeID))
		conf.Logger = logger

		conf.Prophet.DataDir = filepath.Join(string(tempDir), fmt.Sprintf("prophet-%d", nodeID))
		conf.Prophet.RPCAddr = net.JoinHostPort(host, getPort())
		if nodeID > 0 {
			conf.Prophet.EmbedEtcd.Join = endpoints[0]
		}
		conf.Prophet.EmbedEtcd.PeerUrls = endpoints[nodeID]
		conf.Prophet.EmbedEtcd.ClientUrls = "http://" + net.JoinHostPort(host, getPort())

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
		node.DataStorage = dataStorage

		conf.Storage = func() config.StorageConfig {
			return config.StorageConfig{
				DataStorageFactory: func(group uint64) storage.DataStorage {
					return dataStorage
				},
				ForeachDataStorageFunc: func(fn func(storage.DataStorage)) {
					fn(dataStorage)
				},
			}
		}()

		conf.Customize.CustomShardStateAwareFactory = func() aware.ShardStateAware {
			return &shardStateAware{
				created: func(shard meta.Shard) {
					node.Cond.L.Lock()
					node.Created = true
					node.Cond.L.Unlock()
					node.Cond.Broadcast()
				},
				updated: func(shard meta.Shard) {
				},
				splited: func(shard meta.Shard) {
				},
				destroyed: func(shard meta.Shard) {
				},
				becomeLeader: func(shard meta.Shard) {
					node.Cond.L.Lock()
					node.IsLeader = true
					node.IsFollower = false
					node.Cond.L.Unlock()
					node.Cond.Broadcast()
				},
				becomeFollower: func(shard meta.Shard) {
					node.Cond.L.Lock()
					node.IsLeader = false
					node.IsFollower = true
					node.Cond.L.Unlock()
					node.Cond.Broadcast()
				},
				snapshotApplied: func(shard meta.Shard) {
				},
			}
		}

		node.Config = conf

		store := raftstore.NewStore(conf)
		store.Start()
		if nodeID == 0 {
			node.Cond.L.Lock()
			for !node.IsLeader || !node.Created {
				node.Cond.Wait()
			}
			node.Cond.L.Unlock()
		} else {
			node.Cond.L.Lock()
			for !node.IsFollower {
				node.Cond.Wait()
			}
			node.Cond.L.Unlock()
		}
		node.RaftStore = store

		nodes = append(nodes, node)
	}

	return
}
