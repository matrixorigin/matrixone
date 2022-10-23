// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	nodes           map[any]base.INode
	evicter         IEvictHolder
	unregistertimes atomic.Int64
	loadtimes       atomic.Int64
	evicttimes      atomic.Int64
}

func NewNodeManager(maxsize uint64, evicter IEvictHolder) *nodeManager {
	if evicter == nil {
		evicter = NewSimpleEvictHolder()
	}
	mgr := &nodeManager{
		sizeLimiter: *newSizeLimiter(maxsize),
		nodes:       make(map[any]base.INode),
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
		mgr.loadtimes.Load(),
		mgr.evicttimes.Load(),
		mgr.unregistertimes.Load()))
	for _, node := range mgr.nodes {
		key := node.Key()
		_ = w.WriteByte('\n')
		node.RLock()
		_, _ = w.WriteString(fmt.Sprintf("\t%v | %s | Size: %d ",
			key,
			base.NodeStateString(mgr.nodes[key].GetState()),
			mgr.nodes[key].Size()))
		if node.GetState() == base.NodeLoaded {
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

func (mgr *nodeManager) Add(node base.INode) (err error) {
	if !node.IsLoaded() {
		return mgr.RegisterNode(node)
	}
	ok := mgr.MakeRoom(node.Size())
	if !ok {
		err = base.ErrNoSpace
		return
	}
	return mgr.RegisterNode(node)
}

func (mgr *nodeManager) RegisterNode(node base.INode) error {
	key := node.Key()
	mgr.Lock()
	defer mgr.Unlock()
	if _, ok := mgr.nodes[key]; ok {
		return base.ErrDuplicataNode
	}
	mgr.nodes[key] = node
	return nil
}

func (mgr *nodeManager) UnregisterNode(node base.INode) {
	mgr.Lock()
	defer mgr.Unlock()
	mgr.unregistertimes.Add(1)
	delete(mgr.nodes, node.Key())
	node.Destroy()
}

func (mgr *nodeManager) MakeRoom(size uint64) bool {
	ok := mgr.sizeLimiter.ApplyQuota(size)
	nodes := make([]base.IEvictHandle, 0)
	for !ok {
		evicted := mgr.evicter.Dequeue()
		if evicted == nil {
			return false
		}

		if !evicted.Unloadable(evicted.Handle) {
			continue
		}

		{
			evicted.Handle.Lock()
			if evicted.Handle.IsClosed() {
				evicted.Handle.Unlock()
				continue
			}
			if !evicted.Unloadable(evicted.Handle) {
				evicted.Handle.Unlock()
				continue
			}
			if !evicted.Handle.Unloadable() {
				evicted.Handle.Unlock()
				continue
			}
			evicted.Handle.Unload()
			if evicted.Handle.HardEvictable() {
				nodes = append(nodes, evicted.Handle)
			}
			evicted.Handle.Unlock()
		}
		ok = mgr.sizeLimiter.ApplyQuota(size)
	}
	for _, enode := range nodes {
		enode.TryClose()
	}

	return ok
}

func (mgr *nodeManager) GetNodeByKey(key any) (n base.INode, err error) {
	mgr.RLock()
	defer mgr.RUnlock()
	n = mgr.nodes[key]
	if n == nil {
		err = base.ErrNotFound
	}
	return
}

func (mgr *nodeManager) PinByKey(key any) (h base.INodeHandle, err error) {
	n, err := mgr.GetNodeByKey(key)
	if err != nil {
		return
	}
	h = mgr.Pin(n)
	return
}

func (mgr *nodeManager) TryPinByKey(key any, timeout time.Duration) (h base.INodeHandle, err error) {
	n, err := mgr.GetNodeByKey(key)
	if err != nil {
		return
	}
	return mgr.TryPin(n, timeout)
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
		key := node.Key()
		logutil.Warnf("DoRetry Pin Node %v Times %d: %v", key, times, err)
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
	if node.IsClosed() {
		return nil
	}
	if node.IsLoaded() {
		node.Ref()
		return node.MakeHandle()
	}
	ok := mgr.MakeRoom(node.Size())
	if !ok {
		return nil
	}
	node.Load()
	mgr.loadtimes.Add(1)
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
		mgr.evicttimes.Add(1)
	}
}
