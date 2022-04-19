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

package flusher

import (
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/shard"
)

type shardCreatedCtx struct {
	id uint64
}

type shardDeletedCtx struct {
	id uint64
}

type shardNodeCreatedCtx struct {
	shardId uint64
	nodeId  uint64
}

type shardNodeDeletedCtx struct {
	shardId uint64
	nodeId  uint64
}

type driver struct {
	common.ClosedState
	sm.StateMachine
	mu        sync.RWMutex
	shards    map[uint64]*shardFlusher
	flushmask *roaring64.Bitmap
	factory   DriverFactory
}

func NewDriver() *driver {
	f := &driver{
		flushmask: roaring64.New(),
		shards:    make(map[uint64](*shardFlusher)),
	}
	wg := new(sync.WaitGroup)
	// rqueue := sm.NewWaitableQueue(10000, 100, f, wg, nil, nil, f.onMessages)
	rqueue := sm.NewSafeQueue(10000, 100, f.onMessages)
	// TODO: flushQueue should be non-blocking
	// wqueue := sm.NewWaitableQueue(20000, 1000, f, wg, nil, nil, f.onFlushes)
	wqueue := sm.NewSafeQueue(20000, 1000, f.onFlushes)
	f.StateMachine = sm.NewStateMachine(wg, f, rqueue, wqueue)
	return f
}

func (f *driver) InitFactory(factory DriverFactory) {
	f.factory = factory
}

func (f *driver) String() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	str := fmt.Sprintf("driver<cnt=%d>{", len(f.shards))
	for _, sf := range f.shards {
		str = fmt.Sprintf("%s\n%s", str, sf.String())
	}
	if len(f.shards) > 0 {
		str = fmt.Sprintf("%s\n}", str)
	} else {
		str = fmt.Sprintf("%s}", str)
	}
	return str
}

func (f *driver) onFlushes(items ...interface{}) {
	defer f.flushmask.Clear()
	for _, item := range items {
		mask := item.(*roaring64.Bitmap)
		f.flushmask.Or(mask)
	}
	it := f.flushmask.Iterator()
	for it.HasNext() {
		shardId := it.Next()
		s := f.getShard(shardId)
		if s == nil {
			continue
		}
		s.doFlush()
	}
}

func (f *driver) onMessage(msg interface{}) {
	switch ctx := msg.(type) {
	case *shardCreatedCtx:
		f.addShard(ctx)
	case *shardDeletedCtx:
		f.deleteShard(ctx)
	case *shardNodeCreatedCtx:
		f.addShardNode(ctx)
	case *shardNodeDeletedCtx:
		f.deleteShardNode(ctx)
	case []*shard.ItemsToCheckpointStat:
		f.onPengdingItems(ctx)
	}
}

func (f *driver) onMessages(items ...interface{}) {
	for _, item := range items {
		f.onMessage(item)
	}
}

func (f *driver) OnStats(stats interface{}) {
	err, _ := f.EnqueueRecevied(stats)
	if err != nil {
		logutil.Warnf("%v", err)
	}
}

func (f *driver) ShardCnt() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.shards)
}

func (f *driver) ShardCreated(id uint64) {
	ctx := &shardCreatedCtx{id: id}
	_, err := f.EnqueueRecevied(ctx)
	if err != nil {
		logutil.Warnf("%v", err)
	}
}

func (f *driver) ShardDeleted(id uint64) {
	ctx := &shardDeletedCtx{id: id}
	err, _ := f.EnqueueRecevied(ctx)
	if err != nil {
		logutil.Warnf("%v", err)
	}
}

func (f *driver) ShardNodeCreated(shardId, nodeId uint64) {
	ctx := &shardNodeCreatedCtx{
		shardId: shardId,
		nodeId:  nodeId,
	}
	_, err := f.EnqueueRecevied(ctx)
	if err != nil {
		logutil.Warnf("%v", err)
	}
}

func (f *driver) ShardNodeDeleted(shardId, nodeId uint64) {
	ctx := &shardNodeDeletedCtx{
		shardId: shardId,
		nodeId:  nodeId,
	}
	_, err := f.EnqueueRecevied(ctx)
	if err != nil {
		logutil.Warnf("%v", err)
	}
}

func (f *driver) getShard(id uint64) *shardFlusher {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.shards[id]
}

func (f *driver) addShard(ctx *shardCreatedCtx) *shardFlusher {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.shards[ctx.id] = newShardFlusher(ctx.id, f.factory(ctx.id))
	return f.shards[ctx.id]
}

func (f *driver) deleteShard(ctx *shardDeletedCtx) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.shards, ctx.id)
}

func (f *driver) addShardNode(ctx *shardNodeCreatedCtx) {
	s := f.getShard(ctx.shardId)
	if s == nil {
		addCtx := &shardCreatedCtx{
			id: ctx.shardId,
		}
		s = f.addShard(addCtx)
	}
	if err := s.addNode(ctx.nodeId); err != nil {
		logutil.Warn(err.Error())
	}
}

func (f *driver) deleteShardNode(ctx *shardNodeDeletedCtx) {
	s := f.getShard(ctx.shardId)
	if s == nil {
		logutil.Warnf("Specified shard %d not found", ctx.shardId)
		return
	}
	if left, err := s.deleteNode(ctx.nodeId); err != nil {
		logutil.Warn(err.Error())
	} else if left == 0 {
		deleteCtx := &shardDeletedCtx{
			id: ctx.shardId,
		}
		f.deleteShard(deleteCtx)
	}
}

func (f *driver) onPengdingItems(items []*shard.ItemsToCheckpointStat) {
	toFlush := roaring64.New()
	for _, stat := range items {
		if stat.Count == 0 {
			continue
		}
		s := f.getShard(stat.ShardId)
		if s == nil {
			logutil.Warnf("Specified shard %d not found", stat.ShardId)
			continue
		}
		toFlush.Add(stat.ShardId)
	}
	if toFlush.GetCardinality() > 0 {
		err, _ := f.EnqueueCheckpoint(toFlush)
		if err != nil {
			logutil.Warnf("%v", err)
		}
	}
}
