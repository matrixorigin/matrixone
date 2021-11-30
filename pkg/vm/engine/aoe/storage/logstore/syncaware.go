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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	ops "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/worker"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/worker/base"
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

type HBHandleFactory = func(AwareStore) base.IHBHandle

var (
	DefaultHBInterval = time.Duration(100) * time.Millisecond
)

type AwareStore interface {
	Store
	Start()
	Checkpoint(Entry) error
	GetSyncedId() uint64
	SetSyncedId(uint64)
	GetCheckpointId() uint64
	SetCheckpointId(uint64)
}

type syncHandler struct {
	store *syncAwareStore
}

func (h *syncHandler) OnExec() { h.store.Sync() }
func (h *syncHandler) OnStopped() {
	h.store.Sync()
	logutil.Infof("syncHandler Stoped at: %d", h.store.GetSyncedId())
}

type syncAwareStore struct {
	store
	synced       uint64
	unsynced     uint64
	checkpointed uint64
	state        state
	syncer       base.IHeartbeater
}

func NewSyncAwareStore(dir, name string, rotationCfg *RotationCfg, syncerCfg *SyncerCfg) (*syncAwareStore, error) {
	s := &syncAwareStore{}
	if rotationCfg.Observer == nil {
		rotationCfg.Observer = s
	} else {
		rotationCfg.Observer = NewObservers(rotationCfg.Observer, s)
	}
	ss, err := New(dir, name, rotationCfg)
	if err != nil {
		return nil, err
	}
	s.store = *ss
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

func (s *syncAwareStore) Start() {
	s.syncer.Start()
}

func (s *syncAwareStore) Close() error {
	s.syncer.Stop()
	return s.store.Close()
}

func (s *syncAwareStore) Checkpoint(entry Entry) error {
	if !atomic.CompareAndSwapUint32(&s.state, stInited, stPreCheckpoint) {
		return errors.New("Another checkpoint job is running")
	}
	defer atomic.StoreUint32(&s.state, stInited)
	curr := atomic.LoadUint64(&s.checkpointed)
	r := entry.GetAuxilaryInfo().(*common.Range)
	if r.Right <= curr {
		return nil
	}
	if err := s.store.AppendEntry(entry); err != nil {
		return err
	}
	if err := s.Sync(); err != nil {
		return err
	}
	s.SetCheckpointId(r.Right)
	return nil
}

func (s *syncAwareStore) GetCheckpointId() uint64 {
	return atomic.LoadUint64(&s.checkpointed)
}

func (s *syncAwareStore) SetCheckpointId(id uint64) {
	atomic.StoreUint64(&s.checkpointed, id)
}

func (s *syncAwareStore) GetSyncedId() uint64 {
	return atomic.LoadUint64(&s.synced)
}

func (s *syncAwareStore) SetSyncedId(id uint64) {
	atomic.StoreUint64(&s.synced, id)
}

func (s *syncAwareStore) AppendEntry(entry Entry) error {
	var commitId uint64
	isFlush := entry.GetMeta().IsFlush()
	if !isFlush {
		commitId = entry.GetAuxilaryInfo().(uint64)
	}
	err := s.store.AppendEntry(entry)
	if err != nil {
		return err
	}
	if isFlush {
		return s.Sync()
	}
	atomic.StoreUint64(&s.unsynced, commitId)
	return nil
}

func (s *syncAwareStore) OnSynced() {
	unsynced := atomic.LoadUint64(&s.unsynced)
	s.SetSyncedId(unsynced)
}

func (s *syncAwareStore) OnRotated(vf *VersionFile) {
	// logutil.Infof("%s rotated", vf.Name())
}

func (s *syncAwareStore) Sync() error {
	if err := s.file.Sync(); err != nil {
		return err
	}
	return nil
}
