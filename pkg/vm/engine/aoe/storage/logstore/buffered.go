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
	"matrixone/pkg/vm/engine/aoe/storage/common"
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
	SetCheckpointId(uint64)
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
	synced       uint64
	unsynced     uint64
	checkpointed uint64
	state        state
	syncer       base.IHeartbeater
}

func NewBufferedStore(dir, name string, rotationCfg *RotationCfg, syncerCfg *SyncerCfg) (*bufferedStore, error) {
	s := &bufferedStore{}
	if rotationCfg.Observer == nil {
		rotationCfg.Observer = s
	} else {
		rotationCfg.Observer = NewObservers(rotationCfg.Observer, s)
	}
	ss, err := New(dir, name, rotationCfg)
	s.store = *ss
	if err != nil {
		return nil, err
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
	entry.SetAuxilaryInfo(&common.Range{Right: id})
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

func (s *bufferedStore) SetCheckpointId(id uint64) {
	atomic.StoreUint64(&s.checkpointed, id)
}

func (s *bufferedStore) GetSyncedId() uint64 {
	return atomic.LoadUint64(&s.synced)
}

func (s *bufferedStore) SetSyncedId(id uint64) {
	atomic.StoreUint64(&s.synced, id)
}

func (s *bufferedStore) AppendEntry(entry Entry) error {
	panic("not supported")
}

func (s *bufferedStore) AppendEntryWithCommitId(entry Entry, commitId uint64) error {
	isFlush := entry.GetMeta().IsFlush()
	entry.SetAuxilaryInfo(commitId)
	err := s.store.AppendEntry(entry)
	if err != nil {
		return err
	}
	atomic.StoreUint64(&s.unsynced, commitId)
	if isFlush {
		return s.Sync()
	}
	return nil
}

func (s *bufferedStore) OnSynced() {
	unsynced := atomic.LoadUint64(&s.unsynced)
	s.SetSyncedId(unsynced)
}

func (s *bufferedStore) OnRotated(vf *VersionFile) {
	// logutil.Infof("%s rotated", vf.Name())
}

func (s *bufferedStore) Sync() error {
	if err := s.file.Sync(); err != nil {
		return err
	}
	return nil
}
