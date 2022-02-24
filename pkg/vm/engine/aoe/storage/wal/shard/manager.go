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
	"sync"

	"github.com/jiangxinmeng1/logstore/pkg/common"
	"github.com/jiangxinmeng1/logstore/pkg/store"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/shard"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
)

var (
	QueueSize = 500000
	BatchSize = 1000
)

var (
	DuplicateShardErr = errors.New("aoe: duplicate shard")
	ShardNotFoundErr  = errors.New("aoe: shard not found")
)

type noopWal struct{}

func NewNoopWal() *noopWal {
	return new(noopWal)
}
func (noop *noopWal) GetRole() wal.Role                 { return wal.HolderRole }
func (noop *noopWal) GetShardId(uint64) (uint64, error) { return uint64(0), nil }
func (noop *noopWal) String() string                    { return "<noop>" }
func (noop *noopWal) Checkpoint(interface{})            {}
func (noop *noopWal) Close() error                      { return nil }
func (noop *noopWal) SyncLog(wal.Payload) error         { return nil }
func (noop *noopWal) Log(wal.Payload) (*wal.Entry, error) {
	entry := wal.GetEntry(uint64(0))
	entry.SetDone()
	return entry, nil
}
func (noop *noopWal) SetLogstoreCommitId(uint64)                           {}
func (noop *noopWal) GetShardCurrSeqNum(shardId uint64) (id uint64)        { return }
func (noop *noopWal) GetShardCheckpointId(shardId uint64) uint64           { return 0 }
func (noop *noopWal) InitShard(shardId, safeId uint64) error               { return nil }
func (noop *noopWal) GetAllShardCheckpointId() map[uint64]uint64           { return nil }
func (noop *noopWal) GetAllPendingEntries() []*shard.ItemsToCheckpointStat { return nil }
func (noop *noopWal) GetShardPendingCnt(shardId uint64) int                { return 0 }

type manager struct {
	sm.ClosedState
	sm.StateMachine
	mu                        sync.RWMutex
	shards                    map[uint64]*proxy
	driver                    store.Store
	own                       bool
	safemu                    sync.RWMutex
	safeids                   map[uint64]uint64
	role                      wal.Role
	logStoreCommitIdAllocator *common.IdAllocator
}

func NewManager(role wal.Role) *manager {
	return NewManagerWithDriver(nil, false, role)
}

func NewManagerWithDriver(driver store.Store, own bool, role wal.Role) *manager {
	mgr := &manager{
		own:                       own,
		role:                      role,
		driver:                    driver,
		shards:                    make(map[uint64]*proxy),
		safeids:                   make(map[uint64]uint64),
		logStoreCommitIdAllocator: &common.IdAllocator{},
	}
	wg := new(sync.WaitGroup)
	// rQueue := sm.NewWaitableQueue(QueueSize, BatchSize, mgr, wg, nil, nil, mgr.onReceived)
	// ckpQueue := sm.NewWaitableQueue(QueueSize, BatchSize, mgr, wg, nil, nil, mgr.onSnippets)
	rQueue := sm.NewSafeQueue(QueueSize, BatchSize, mgr.onReceived)
	ckpQueue := sm.NewSafeQueue(QueueSize, BatchSize, mgr.onSnippets)
	mgr.StateMachine = sm.NewStateMachine(wg, mgr, rQueue, ckpQueue)
	// if own && driver != nil {
	// mgr.driver.Start()
	// }
	mgr.Start()
	return mgr
}

func (mgr *manager) UpdateSafeId(shardId, id uint64) {
	mgr.safemu.Lock()
	defer mgr.safemu.Unlock()
	old, ok := mgr.safeids[shardId]
	if !ok {
		mgr.safeids[shardId] = id
	} else {
		if old > id {
			panic(fmt.Sprintf("logic error: %d, %d", old, id))
		}
		mgr.safeids[shardId] = id
	}
}

func (mgr *manager) GetRole() wal.Role {
	return mgr.role
}

// Log payload, blocking until the payload is handled
func (mgr *manager) SyncLog(payload wal.Payload) error {
	if entry, err := mgr.Log(payload); err != nil {
		return err
	} else {
		entry.WaitDone()
		entry.Free()
		return nil
	}
}

// async Log payload. Caller is responsible to wait and free the returned entry
func (mgr *manager) Log(payload wal.Payload) (*Entry, error) {
	entry := wal.GetEntry(0)
	entry.Payload = payload
	if _, err := mgr.EnqueueRecevied(entry); err != nil {
		entry.Free()
		return nil, err
	}
	return entry, nil
}

func (mgr *manager) GetShardCurrSeqNum(shardId uint64) (id uint64) {
	s, err := mgr.GetShard(shardId)
	if err != nil {
		return
	}
	if s.idAlloctor != nil {
		id = s.idAlloctor.Get()
	} else {
		id = s.GetLastId()
	}
	return
}
func (mgr *manager) SetLogstoreCommitId(id uint64) {
	mgr.logStoreCommitIdAllocator.Set(id)
}
func (mgr *manager) GetShardCheckpointId(shardId uint64) uint64 {
	s, err := mgr.GetShard(shardId)
	if err != nil {
		logutil.Warnf("shard %d not found", shardId)
		return 0
	}
	return s.GetSafeId()
}

func (mgr *manager) EnqueueEntry(entry *Entry) error {
	_, err := mgr.EnqueueRecevied(entry)
	return err
}

func (mgr *manager) Checkpoint(v interface{}) {
	switch vv := v.(type) {
	case *Index:
		bat := NewSimpleBatchIndice(vv)
		mgr.EnqueueCheckpoint(bat)
		return
	case *SliceIndice:
		if vv == nil {
			return
		}
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

func (mgr *manager) InitShard(shardId, safeId uint64) error {
	mgr.mu.Lock()
	s, err := mgr.AddShardLocked(shardId)
	mgr.mu.Unlock()
	if err != nil {
		return err
	}
	s.InitSafeId(safeId)
	return nil
}

func (mgr *manager) GetSafeIds() SafeIds {
	ids := SafeIds{
		Ids: make([]SafeId, 0, 100),
	}
	mgr.safemu.RLock()
	defer mgr.safemu.RUnlock()
	for shardId, id := range mgr.safeids {
		ids.Append(shardId, id)
	}
	return ids
}

func (mgr *manager) GetAllShardCheckpointId() map[uint64]uint64 {
	ids := make(map[uint64]uint64)
	mgr.safemu.RLock()
	defer mgr.safemu.RUnlock()
	for shardId, id := range mgr.safeids {
		ids[shardId] = id
	}
	return ids
}

func (mgr *manager) logEntry(entry *Entry) {
	index := entry.Payload.(*Index)
	shard, _ := mgr.getOrAddShard(index.ShardId)
	shard.LogIndex(index)
}

func (mgr *manager) onSnippets(items ...interface{}) {
	shards := make(map[uint64]*proxy)
	for _, item := range items {
		bat := item.(*SliceIndice)
		shardId := bat.GetShardId()
		shard, err := mgr.GetShard(shardId)
		if err != nil {
			panic(fmt.Sprintf("%d: %s", shardId, err))
		}
		shard.AppendBatchIndice(bat)
		shards[shardId] = shard
	}
	for _, shard := range shards {
		shard.Checkpoint()
	}
}

func (mgr *manager) GetShardPendingCnt(shardId uint64) int {
	s, err := mgr.GetShard(shardId)
	if err != nil {
		return 0
	}
	return int(s.GetPendingEntries())
}

func (mgr *manager) GetAllPendingEntries() []*shard.ItemsToCheckpointStat {
	stats := make([]*shard.ItemsToCheckpointStat, 0, 100)
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	for _, s := range mgr.shards {
		stat := &shard.ItemsToCheckpointStat{
			ShardId: s.id,
			Count:   int(s.GetPendingEntries()),
		}
		stats = append(stats, stat)
	}
	return stats
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
	if mgr.own && mgr.driver != nil {
		mgr.driver.Close()
	}
	for _, s := range mgr.shards {
		logutil.Infof("[AOE]: Shard-%d SafeId-%d | Closed", s.id, s.GetSafeId())
	}
	return nil
}
