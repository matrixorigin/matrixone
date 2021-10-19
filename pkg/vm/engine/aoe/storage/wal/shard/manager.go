package shard

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var (
	QueueSize = 500000
	BatchSize = 1000
)

var (
	DuplicateShardErr = errors.New("aoe: duplicate shard")
	ShardNotFoundErr  = errors.New("aoe: shard not found")
)

type manager struct {
	mu            sync.RWMutex
	shards        map[uint64]*proxy
	requestQueue  chan *Entry
	requestCtx    context.Context
	requestCancel context.CancelFunc
	requestWg     sync.WaitGroup

	ckpQueue  chan *snippet
	ckpCtx    context.Context
	ckpCancel context.CancelFunc
	ckpWg     sync.WaitGroup

	wg     sync.WaitGroup
	closed int32
}

func NewManager() *manager {
	mgr := &manager{
		shards:       make(map[uint64]*proxy),
		requestQueue: make(chan *Entry, QueueSize),
		ckpQueue:     make(chan *snippet, QueueSize),
	}
	mgr.requestCtx, mgr.requestCancel = context.WithCancel(context.Background())
	mgr.ckpCtx, mgr.ckpCancel = context.WithCancel(context.Background())
	mgr.wg.Add(2)
	go mgr.requestLoop()
	go mgr.checkpointLoop()
	return mgr
}

func (mgr *manager) checkpointLoop() {
	defer mgr.wg.Done()
	entries := make([]*snippet, 0, BatchSize)
	for {
		select {
		case <-mgr.ckpCtx.Done():
			return
		case entry := <-mgr.ckpQueue:
			entries = append(entries, entry)
		Left:
			for i := 0; i < BatchSize-1; i++ {
				select {
				case entry = <-mgr.ckpQueue:
					entries = append(entries, entry)
				default:
					break Left
				}
			}
			cnt := len(entries)
			mgr.onSnippets(entries)
			entries = entries[:0]
			mgr.ckpWg.Add(-1 * cnt)
		}
	}
}

func (mgr *manager) requestLoop() {
	defer mgr.wg.Done()
	entries := make([]*Entry, 0, BatchSize)
	for {
		select {
		case <-mgr.requestCtx.Done():
			return
		case entry := <-mgr.requestQueue:
			entries = append(entries, entry)
		Left:
			for i := 0; i < BatchSize-1; i++ {
				select {
				case entry = <-mgr.requestQueue:
					entries = append(entries, entry)
				default:
					break Left
				}
			}
			cnt := len(entries)
			mgr.onEntries(entries)
			entries = entries[:0]
			mgr.requestWg.Add(-1 * cnt)
		}
	}
}

func (mgr *manager) EnqueueEntry(entry *Entry) error {
	if atomic.LoadInt32(&mgr.closed) == int32(1) {
		return errors.New("closed")
	}
	mgr.requestWg.Add(1)
	if atomic.LoadInt32(&mgr.closed) == int32(1) {
		mgr.requestWg.Done()
		return errors.New("closed")
	}
	mgr.requestQueue <- entry
	return nil
}

func (mgr *manager) EnqueueSnippet(snip *snippet) error {
	if atomic.LoadInt32(&mgr.closed) == int32(1) {
		return errors.New("closed")
	}
	mgr.ckpWg.Add(1)
	if atomic.LoadInt32(&mgr.closed) == int32(1) {
		mgr.ckpWg.Done()
		return errors.New("closed")
	}
	mgr.ckpQueue <- snip
	return nil
}

func (mgr *manager) GetShard(id uint64) (*proxy, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	s, ok := mgr.shards[id]
	if !ok {
		return nil, ShardNotFoundErr
	}
	return s, nil
}

func (mgr *manager) AddShardLocked(id uint64) (*proxy, error) {
	_, ok := mgr.shards[id]
	if ok {
		return nil, DuplicateShardErr
	}
	mgr.shards[id] = newProxy(id, mgr)
	return mgr.shards[id], nil
}

func (mgr *manager) getOrAddShard(id uint64) (*proxy, error) {
	shard, err := mgr.GetShard(id)
	if err == nil {
		return shard, err
	}
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if shard, err = mgr.AddShardLocked(id); err == DuplicateShardErr {
		shard = mgr.shards[id]
		err = nil
	}
	return shard, err
}

func (mgr *manager) onEntries(entries []*Entry) {
	for _, entry := range entries {
		mgr.logEntry(entry)
		entry.SetDone()
	}
}

func (mgr *manager) logEntry(entry *Entry) {
	index := entry.Payload.(*LogIndex)
	shard, _ := mgr.getOrAddShard(index.ShardId)
	shard.LogIndex(index)
}

func (mgr *manager) onSnippets(snips []*snippet) {
	shards := make(map[uint64]*proxy)
	for _, snip := range snips {
		shardId := snip.GetShardId()
		shard, err := mgr.GetShard(shardId)
		if err != nil {
			panic(fmt.Sprintf("%d: %s", shardId, err))
		}
		shard.AppendSnippet(snip)
		shards[shardId] = shard
	}
	for _, shard := range shards {
		shard.Checkpoint()
	}
}

func (mgr *manager) Close() error {
	if !atomic.CompareAndSwapInt32(&mgr.closed, int32(0), int32(1)) {
		return nil
	}
	mgr.requestWg.Wait()
	mgr.requestCancel()
	mgr.ckpWg.Wait()
	mgr.ckpCancel()
	mgr.wg.Wait()
	return nil
}
