package logstore

import (
	"matrixone/pkg/vm/engine/aoe/storage/logstore/sm"
	"sync"
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
	s.flushQueue = sm.NewWaitableQueue(DefaultMaxBatchSize*100, DefaultMaxBatchSize, s, wg,
		nil, nil, s.onEntries)
	return s, nil
}

func (s *batchStore) Checkpoint(entry Entry) error {
	_, err := s.flushQueue.Enqueue(entry)
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
