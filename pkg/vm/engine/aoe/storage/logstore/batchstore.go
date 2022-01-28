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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore/sm"
)

var (
	DefaultMaxBatchSize = 500
)

type batchStore struct {
	syncBase
	store
	sm.ClosedState
	flushQueue sm.Queue
	wg         *sync.WaitGroup
}

func NewBatchStore(dir, name string, rotationCfg *RotationCfg) (*batchStore, error) {
	bs, err := New(dir, name, rotationCfg)
	if err != nil {
		return nil, err
	}
	wg := new(sync.WaitGroup)
	s := &batchStore{
		store: *bs,
		wg:    wg,
	}
	s.flushQueue = sm.NewSafeQueue(DefaultMaxBatchSize*100, DefaultMaxBatchSize, s.onEntries)
	// s.flushQueue = sm.NewWaitableQueue(DefaultMaxBatchSize*100, DefaultMaxBatchSize, s, wg,
	// 	nil, nil, s.onEntries)
	return s, nil
}

func (s *batchStore) Checkpoint(entry Entry) error {
	_, err := s.flushQueue.Enqueue(entry)
	s.SetCheckpointId(entry.GetAuxilaryInfo().(*common.Range).Right)
	return err
}

func (s *batchStore) AppendEntry(entry Entry) error {
	_, err := s.flushQueue.Enqueue(entry)
	return err
}

func (s *batchStore) onEntries(items ...interface{}) {
	var err error
	for _, item := range items {
		entry := item.(Entry)
		if err = s.store.AppendEntry(entry); err != nil {
			panic(err)
		}
		s.OnEntryReceived(entry)
	}
	if err = s.store.Sync(); err != nil {
		panic(err)
	}
	s.OnCommit()
	for _, item := range items {
		item.(AsyncEntry).DoneWithErr(nil)
	}
}

func (s *batchStore) Start() {
	s.flushQueue.Start()
}

func (s *batchStore) Close() error {
	if !s.TryClose() {
		return nil
	}
	s.flushQueue.Stop()
	s.wg.Wait()
	return nil
}
