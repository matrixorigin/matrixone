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
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/executor/simple"
	"github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/transport"
	"github.com/matrixorigin/matrixcube/vfs"
	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
)

func (_ Def2) Nodes(
	configs NodeConfigs,
	getTempDir fz.GetTempDir,
	numNodes fz.NumNodes,
	logger fz.Logger,
	getHost fz.GetNetworkHost,
	getPort fz.GetPortStr,
	newInterceptableTransport NewInterceptableTransport,
) (nodes fz.Nodes) {

	host := getHost()

	var bootEndpoint string

	for nodeID, conf := range configs {

		node := &Node{
			Cond: sync.NewCond(new(sync.Mutex)),
		}

		node.Logger = logger

		fs := vfs.NewMemFS()
		conf.FS = fs
		node.FS = fs

		conf.RaftAddr = net.JoinHostPort(host, getPort(fz.NodeID(nodeID), host))
		conf.ClientAddr = net.JoinHostPort(host, getPort(fz.NodeID(nodeID), host))
		conf.DataPath = filepath.Join(getTempDir(), fmt.Sprintf("data-%d", nodeID))
		conf.Logger = logger

		conf.Prophet.DataDir = filepath.Join(getTempDir(), fmt.Sprintf("prophet-%d", nodeID))
		conf.Prophet.RPCAddr = net.JoinHostPort(host, getPort(fz.NodeID(nodeID), host))
		if nodeID == 0 {
			bootEndpoint = "http://" + net.JoinHostPort(host, getPort(0, host))
			conf.Prophet.EmbedEtcd.PeerUrls = bootEndpoint
		} else {
			conf.Prophet.EmbedEtcd.Join = bootEndpoint
			conf.Prophet.EmbedEtcd.PeerUrls =
				"http://" + net.JoinHostPort(host, getPort(fz.NodeID(nodeID), host))
		}
		conf.Prophet.EmbedEtcd.ClientUrls = "http://" + net.JoinHostPort(host, getPort(fz.NodeID(nodeID), host))

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
				ForeachDataStorageFunc: func(fn func(n uint64, _ storage.DataStorage)) {
					fn(0, dataStorage)
				},
			}
		}()

		conf.Customize.CustomShardStateAwareFactory = func() aware.ShardStateAware {
			return &shardStateAware{
				created: func(shard metapb.Shard) {
					node.Cond.L.Lock()
					node.Created = true
					node.Cond.L.Unlock()
					node.Cond.Broadcast()
				},
				updated: func(shard metapb.Shard) {
				},
				splited: func(shard metapb.Shard) {
				},
				destroyed: func(shard metapb.Shard) {
				},
				becomeLeader: func(shard metapb.Shard) {
					node.Cond.L.Lock()
					node.IsLeader = true
					node.IsFollower = false
					node.Cond.L.Unlock()
					node.Cond.Broadcast()
				},
				becomeFollower: func(shard metapb.Shard) {
					node.Cond.L.Lock()
					node.IsLeader = false
					node.IsFollower = true
					node.Cond.L.Unlock()
					node.Cond.Broadcast()
				},
				snapshotApplied: func(shard metapb.Shard) {
				},
			}
		}

		conf.Customize.CustomWrapNewTransport = func(t transport.Trans) transport.Trans {
			return newInterceptableTransport(t, func() fz.Nodes {
				return nodes
			})
		}

		node.Config = conf

		store := raftstore.NewStore(conf)
		if nodeID == 0 {
			// first node
			store.Start()
			node.Cond.L.Lock()
			for !node.IsLeader || !node.Created {
				node.Cond.Wait()
			}
			node.Cond.L.Unlock()
		} else {
			go func() {
				store.Start()
			}()
		}
		node.RaftStore = store

		nodes = append(nodes, node)
	}

	// wait ready
	for i, fzNode := range nodes {
		if i > 0 {
			node := fzNode.(*Node)
			node.Cond.L.Lock()
			for !node.IsFollower {
				node.Cond.Wait()
			}
			node.Cond.L.Unlock()
		}
	}

	return
}
