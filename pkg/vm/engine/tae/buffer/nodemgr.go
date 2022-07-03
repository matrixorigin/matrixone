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

package buffer

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type nodeManager struct {
	sync.RWMutex
	sizeLimiter
	nodes           map[common.ID]base.INode
	evicter         IEvictHolder
	unregistertimes int64
	loadtimes       int64
	evicttimes      int64
}

func NewNodeManager(maxsize uint64, evicter IEvictHolder) *nodeManager {
	if evicter == nil {
		evicter = NewSimpleEvictHolder()
	}
	mgr := &nodeManager{
		sizeLimiter: *newSizeLimiter(maxsize),
		nodes:       make(map[common.ID]base.INode),
		evicter:     evicter,
	}
	return mgr
}

func (mgr *nodeManager) String() string {
	var w bytes.Buffer
	mgr.RLock()
	defer mgr.RUnlock()
	loaded := 0
	_, _ = w.WriteString(fmt.Sprintf("<nodeManager>[%s][Nodes:%d,LoadTimes:%d,EvictTimes:%d,UnregisterTimes:%d]:",
		mgr.sizeLimiter.String(),
		len(mgr.nodes),
		atomic.LoadInt64(&mgr.loadtimes),
		atomic.LoadInt64(&mgr.evicttimes),
		atomic.LoadInt64(&mgr.unregistertimes)))
	for _, node := range mgr.nodes {
		id := node.GetID()
		_ = w.WriteByte('\n')
		node.RLock()
		_, _ = w.WriteString(fmt.Sprintf("\t%s | %s | Size: %d ",
			id.String(),
			base.NodeStateString(mgr.nodes[node.GetID()].GetState()),
			mgr.nodes[node.GetID()].Size()))
		if node.GetState() == base.NODE_LOADED {
			loaded++
		}
		node.RUnlock()
	}
	_ = w.WriteByte('\n')
	_, _ = w.WriteString(fmt.Sprintf("[Load Status: (%d/%d)]", loaded, len(mgr.nodes)))

	return w.String()
}

func (mgr *nodeManager) Count() int {
	mgr.RLock()
	defer mgr.RUnlock()
	return len(mgr.nodes)
}

func (mgr *nodeManager) RegisterNode(node base.INode) {
	id := node.GetID()
	mgr.Lock()
	defer mgr.Unlock()
	_, ok := mgr.nodes[id]
	if ok {
		panic(fmt.Sprintf("Duplicate node: %s", id.BlockString()))
	}
	mgr.nodes[id] = node
}

func (mgr *nodeManager) UnregisterNode(node base.INode) {
	mgr.Lock()
	defer mgr.Unlock()
	atomic.AddInt64(&mgr.unregistertimes, int64(1))
	delete(mgr.nodes, node.GetID())
	node.Destroy()
}

func (mgr *nodeManager) MakeRoom(size uint64) bool {
	ok := mgr.sizeLimiter.ApplyQuota(size)
	for !ok {
		evicted := mgr.evicter.Dequeue()
		if evicted == nil {
			return false
		}
		if evicted.Handle.IsClosed() {
			continue
		}

		if !evicted.Unloadable(evicted.Handle) {
			continue
		}

		{
			evicted.Handle.Lock()
			if !evicted.Unloadable(evicted.Handle) {
				evicted.Handle.Unlock()
				continue
			}
			if !evicted.Handle.Unloadable() {
				evicted.Handle.Unlock()
				continue
			}
			evicted.Handle.Unload()
			evicted.Handle.Unlock()
		}
		ok = mgr.sizeLimiter.ApplyQuota(size)
	}

	return ok
}

func (mgr *nodeManager) TryPin(node base.INode, timeout time.Duration) (h base.INodeHandle, err error) {
	h = mgr.Pin(node)
	if h == nil {
		times := 0
		var ctx context.Context
		var cancel context.CancelFunc
		if timeout > 0 {
			ctx, cancel = context.WithTimeout(context.Background(), timeout)
			defer cancel()
		}
		err = common.DoRetry(func() (err error) {
			times++
			h = mgr.Pin(node)
			if h == nil {
				err = base.ErrNoSpace
			}
			return
		}, ctx)
		id := node.GetID()
		logutil.Warnf("DoRetry Pin Node %s Times %d: %v", id.String(), times, err)
	}
	return
}

func (mgr *nodeManager) Pin(node base.INode) base.INodeHandle {
	node.RLock()
	if node.IsLoaded() {
		node.Ref()
		node.RUnlock()
		return node.MakeHandle()
	}
	node.RUnlock()

	node.Lock()
	defer node.Unlock()
	if node.IsLoaded() {
		node.Ref()
		return node.MakeHandle()
	}
	ok := mgr.MakeRoom(node.Size())
	if !ok {
		return nil
	}
	node.Load()
	atomic.AddInt64(&mgr.loadtimes, int64(1))
	node.Ref()
	return node.MakeHandle()
}

func (mgr *nodeManager) Unpin(node base.INode) {
	node.Lock()
	defer node.Unlock()
	node.Unref()
	if node.RefCount() == 0 {
		toevict := &EvictNode{Handle: node, Iter: node.IncIteration()}
		mgr.evicter.Enqueue(toevict)
		atomic.AddInt64(&mgr.evicttimes, int64(1))
	}
}
