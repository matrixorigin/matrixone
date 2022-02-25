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
	"sync"
	"time"

	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/vfs"
	fz "github.com/matrixorigin/matrixone/pkg/chaostesting"
	"go.uber.org/zap"
)

type Node struct {
	Logger      *zap.Logger
	Config      *config.Config
	RaftStore   raftstore.Store
	DataStorage storage.DataStorage
	FS          vfs.FS

	Cond       *sync.Cond
	Created    bool
	IsLeader   bool
	IsFollower bool
}

func (_ Def2) CloseNode(
	nodes fz.Nodes,
	logger fz.Logger,
) fz.CloseNode {
	return func(id fz.NodeID) error {
		node := nodes[id].(*Node)
		if node.RaftStore != nil {
			node.RaftStore.Stop()
			logger.Info("node stopped", zap.Int("node-id", int(id)))
		}
		return nil
	}
}

type RestartNode func(id fz.NodeID) error

func (_ Def) RestartNode(
	closeNode fz.CloseNode,
	nodes fz.Nodes,
	logger fz.Logger,
) RestartNode {
	return func(id fz.NodeID) (err error) {
		defer he(&err)

		ce(closeNode(id))

		n := nodes[id].(*Node)
		n.Created = false
		n.IsLeader = false
		n.IsFollower = false
		n.RaftStore = raftstore.NewStore(n.Config)
		n.RaftStore.Start()
		done := make(chan struct{})
		go func() {
			n.Cond.L.Lock()
			for !(n.Created && (n.IsLeader || n.IsFollower)) {
				n.Cond.Wait()
			}
			n.Cond.L.Unlock()
			close(done)
		}()
		select {
		case <-done:
			logger.Info("node restarted", zap.Int("node-id", int(id)))
		case <-time.After(time.Minute):
			return we(fmt.Errorf("restart timeout"))
		}

		return
	}
}
