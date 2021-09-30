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

package logstore

import (
	"errors"
	"matrixone/pkg/logutil"
	ops "matrixone/pkg/vm/engine/aoe/storage/worker"
	"matrixone/pkg/vm/engine/aoe/storage/worker/base"
	"sync/atomic"
	"time"
)

type state = uint32

const (
	stInited state = iota
	stPreCheckpoint
)

type SyncerCfg struct {
	Factory  HBHandleFactory
	Interval time.Duration
}

type HBHandleFactory = func(BufferedStore) base.IHBHandle

var (
	DefaultHBInterval = time.Duration(100) * time.Millisecond
)

type BufferedStore interface {
	Store
	Start()
	AppendEntryWithCommitId(Entry, uint64) error
	Checkpoint(Entry, uint64) error
	GetSyncedId() uint64
	SetSyncedId(uint64)
	GetCheckpointId() uint64
}

type syncHandler struct {
	store *bufferedStore
}

func (h *syncHandler) OnExec() { h.store.Sync() }
func (h *syncHandler) OnStopped() {
	h.store.Sync()
	logutil.Infof("syncHandler Stoped at: %d", h.store.GetSyncedId())
}

type bufferedStore struct {
	store
	committed    uint64
	uncommitted  uint64
	checkpointed uint64
	state        state
	syncer       base.IHeartbeater
}

func NewBufferedStore(dir, name string, syncerCfg *SyncerCfg) (*bufferedStore, error) {
	ss, err := New(dir, name)
	if err != nil {
		return nil, err
	}
	s := &bufferedStore{
		store: *ss,
	}
	var handle base.IHBHandle
	if syncerCfg == nil {
		syncerCfg = &SyncerCfg{
			Interval: DefaultHBInterval,
		}
		handle = &syncHandler{store: s}
	} else {
		handle = syncerCfg.Factory(s)
	}
	s.syncer = ops.NewHeartBeater(syncerCfg.Interval, handle)
	return s, nil
}

func (s *bufferedStore) Start() {
	s.syncer.Start()
}

func (s *bufferedStore) Close() error {
	s.syncer.Stop()
	return s.store.Close()
}

func (s *bufferedStore) Checkpoint(entry Entry, id uint64) error {
	if !atomic.CompareAndSwapUint32(&s.state, stInited, stPreCheckpoint) {
		return errors.New("Another checkpoint job is running")
	}
	defer atomic.StoreUint32(&s.state, stInited)
	curr := atomic.LoadUint64(&s.checkpointed)
	if id <= curr {
		return nil
	}
	if err := s.store.AppendEntry(entry); err != nil {
		return err
	}
	if err := s.Sync(); err != nil {
		return err
	}
	atomic.StoreUint64(&s.checkpointed, id)
	return nil
}

func (s *bufferedStore) GetCheckpointId() uint64 {
	return atomic.LoadUint64(&s.checkpointed)
}

func (s *bufferedStore) GetSyncedId() uint64 {
	return atomic.LoadUint64(&s.committed)
}

func (s *bufferedStore) SetSyncedId(id uint64) {
	atomic.StoreUint64(&s.committed, id)
}

func (s *bufferedStore) AppendEntry(entry Entry) error {
	panic("not supported")
}

func (s *bufferedStore) AppendEntryWithCommitId(entry Entry, commitId uint64) error {
	err := s.store.AppendEntry(entry)
	if err != nil {
		return err
	}
	atomic.StoreUint64(&s.uncommitted, commitId)
	if entry.GetMeta().IsFlush() {
		return s.Sync()
	}
	return nil
}

func (s *bufferedStore) Sync() error {
	uncommitted := atomic.LoadUint64(&s.uncommitted)
	// if uncommitted == s.GetSyncedId() {
	// 	return nil
	// }
	// if err := s.writer.Flush(); err != nil {
	// 	return err
	// }
	if err := s.file.Sync(); err != nil {
		return err
	}
	s.SetSyncedId(uncommitted)
	return nil
}
