package flusher

import (
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/shard"
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

type flusher struct {
	sm.Closable
	sm.StateMachine
	mu        sync.RWMutex
	shards    map[uint64]*shardFlusher
	flushmask *roaring64.Bitmap
	factory   DriverFactory
}

func NewFlusher(factory DriverFactory) *flusher {
	f := &flusher{
		factory:   factory,
		flushmask: roaring64.New(),
		shards:    make(map[uint64](*shardFlusher)),
	}
	wg := new(sync.WaitGroup)
	rqueue := sm.NewWaitableQueue(10000, 100, f, nil, nil, nil, f.onMessages)
	// TODO: flushQueue should be non-blocking
	wqueue := sm.NewWaitableQueue(20000, 1000, f, nil, nil, nil, f.onFlushes)
	f.StateMachine = sm.NewStateMachine(wg, f, rqueue, wqueue)
	return f
}

func (f *flusher) onFlushes(items ...interface{}) {
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
	}
}

func (f *flusher) onMessage(msg interface{}) {
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

func (f *flusher) onMessages(items ...interface{}) {
	for _, item := range items {
		f.onMessage(item)
	}
}

func (f *flusher) ShardCreated(id uint64) {
	ctx := &shardCreatedCtx{id: id}
	f.EnqueueRecevied(ctx)
}

func (f *flusher) ShardDeleted(id uint64) {
	ctx := &shardDeletedCtx{id: id}
	f.EnqueueRecevied(ctx)
}

func (f *flusher) ShardNodeCreated(shardId, nodeId uint64) {
	ctx := &shardNodeCreatedCtx{
		shardId: shardId,
		nodeId:  nodeId,
	}
	f.EnqueueRecevied(ctx)
}

func (f *flusher) ShardNodeDeleted(shardId, nodeId uint64) {
	ctx := &shardNodeDeletedCtx{
		shardId: shardId,
		nodeId:  nodeId,
	}
	f.EnqueueRecevied(ctx)
}

func (f *flusher) getShard(id uint64) *shardFlusher {
	f.mu.RLock()
	defer f.mu.RUnlock()
	sf, _ := f.shards[id]
	return sf
}

func (f *flusher) addShard(ctx *shardCreatedCtx) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.shards[ctx.id] = newShardFlusher(ctx.id, f.factory(ctx.id))
}

func (f *flusher) deleteShard(ctx *shardDeletedCtx) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.shards, ctx.id)
}

func (f *flusher) addShardNode(ctx *shardNodeCreatedCtx) {
	s := f.getShard(ctx.shardId)
	if s == nil {
		logutil.Warnf("Specified shard %d not found", ctx.shardId)
		return
	}
	if err := s.addNode(ctx.nodeId); err != nil {
		logutil.Warn(err.Error())
	}
	return
}

func (f *flusher) deleteShardNode(ctx *shardNodeDeletedCtx) {
	s := f.getShard(ctx.shardId)
	if s == nil {
		logutil.Warnf("Specified shard %d not found", ctx.shardId)
		return
	}
	if err := s.deleteNode(ctx.nodeId); err != nil {
		logutil.Warn(err.Error())
	}
	return
}

func (f *flusher) onPengdingItems(items []*shard.ItemsToCheckpointStat) {
	toFlush := roaring64.New()
	for _, stat := range items {
		s := f.getShard(stat.ShardId)
		if s == nil {
			logutil.Warnf("Specified shard %d not found", stat.ShardId)
			continue
		}
		toFlush.Add(stat.ShardId)
	}
	if toFlush.GetCardinality() > 0 {
		f.EnqueueCheckpoint(toFlush)
	}
}
