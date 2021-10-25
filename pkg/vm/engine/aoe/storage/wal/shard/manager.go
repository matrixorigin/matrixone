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

package shard

import (
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
	"sync"
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
	sm.ClosedState
	sm.StateMachine
	mu     sync.RWMutex
	shards map[uint64]*proxy
}

func NewManager() *manager {
	mgr := &manager{
		shards: make(map[uint64]*proxy),
	}
	wg := new(sync.WaitGroup)
	rQueue := sm.NewWaitableQueue(QueueSize, BatchSize, mgr, wg, nil, nil, mgr.onReceived)
	ckpQueue := sm.NewWaitableQueue(QueueSize, BatchSize, mgr, wg, nil, nil, mgr.onSnippets)
	mgr.StateMachine = sm.NewStateMachine(wg, mgr, rQueue, ckpQueue)
	mgr.Start()
	return mgr
}

func (mgr *manager) Log(payload wal.Payload) (*Entry, error) {
	entry := wal.GetEntry(0)
	entry.Payload = payload
	if _, err := mgr.EnqueueRecevied(entry); err != nil {
		entry.Free()
		return nil, err
	}
	return entry, nil
}

func (mgr *manager) EnqueueEntry(entry *Entry) error {
	_, err := mgr.EnqueueRecevied(entry)
	return err
}

func (mgr *manager) Checkpoint(v interface{}) {
	switch vv := v.(type) {
	case *LogIndex:
		snip := NewSimpleSnippet(vv)
		mgr.EnqueueCheckpoint(snip)
		return
	case *Snippet:
		mgr.EnqueueCheckpoint(vv)
		return
	}
	panic("not supported")
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

func (mgr *manager) onReceived(items ...interface{}) {
	for _, item := range items {
		entry := item.(*Entry)
		mgr.logEntry(entry)
		entry.SetDone()
	}
}

func (mgr *manager) logEntry(entry *Entry) {
	index := entry.Payload.(*LogIndex)
	shard, _ := mgr.getOrAddShard(index.ShardId)
	shard.LogIndex(index)
}

func (mgr *manager) onSnippets(items ...interface{}) {
	shards := make(map[uint64]*proxy)
	for _, item := range items {
		snip := item.(*Snippet)
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

func (mgr *manager) String() string {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	s := fmt.Sprintf("ShardMgr(Cnt=%d){", len(mgr.shards))
	for _, shard := range mgr.shards {
		s = fmt.Sprintf("%s\n%s", s, shard.String())
	}
	if len(mgr.shards) > 0 {
		s = fmt.Sprintf("%s\n}", s)
	} else {
		s = fmt.Sprintf("%s}", s)
	}
	return s
}

func (mgr *manager) Close() error {
	mgr.Stop()
	return nil
}
