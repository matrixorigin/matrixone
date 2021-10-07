package logstore

import (
	"context"
	"errors"
	"matrixone/pkg/logutil"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"sync"
	"sync/atomic"
)

var (
	DefaultMaxBatchSize = 500
)

type batchStore struct {
	store
	synced            uint64
	syncpending       uint64
	checkpointpending uint64
	checkpointed      uint64
	flushWg           sync.WaitGroup
	flushCtx          context.Context
	flushCancel       context.CancelFunc
	flushQueue        chan Entry
	wg                sync.WaitGroup
	closed            int32
}

func newBatchStore(dir, name string, rotationCfg *RotationCfg) (*batchStore, error) {
	bs, err := New(dir, name, rotationCfg)
	if err != nil {
		return nil, err
	}
	s := &batchStore{
		store:      *bs,
		flushQueue: make(chan Entry, DefaultMaxBatchSize*100),
	}
	s.flushCtx, s.flushCancel = context.WithCancel(context.Background())
	s.wg.Add(1)
	go s.flushLoop()
	return s, nil
}

func (s *batchStore) IsClosed() bool {
	return atomic.LoadInt32(&s.closed) == int32(1)
}

func (s *batchStore) Checkpoint(entry Entry) error {
	return s.AppendEntry(entry)
}

func (s *batchStore) AppendEntry(entry Entry) error {
	if s.IsClosed() {
		return errors.New("is closed")
	}
	s.flushWg.Add(1)
	if s.IsClosed() {
		s.flushWg.Done()
		return errors.New("is closed")
	}
	s.flushQueue <- entry
	return nil
}

func (s *batchStore) flushLoop() {
	defer s.wg.Done()
	entries := make([]Entry, 0, DefaultMaxBatchSize)
	indice := make([]int, 0, DefaultMaxBatchSize)
	for {
		select {
		case <-s.flushCtx.Done():
			logutil.Infof("flush loop done")
			return
		case e := <-s.flushQueue:
			entries = append(entries, e)
			if e.IsAsync() {
				indice = append(indice, len(entries)-1)
			}
		Left:
			for i := 0; i < DefaultMaxBatchSize-1; i++ {
				select {
				case e = <-s.flushQueue:
					entries = append(entries, e)
					if e.IsAsync() {
						indice = append(indice, len(entries)-1)
					}
				default:
					break Left
				}
			}
			cnt := len(entries)
			s.onEntries(entries, indice)
			entries = entries[:0]
			indice = indice[:0]
			s.flushWg.Add(-1 * cnt)
		}
	}
}

func (s *batchStore) onEntries(entries []Entry, indice []int) {
	var err error
	for _, entry := range entries {
		err = s.store.AppendEntry(entry)
		if err != nil {
			panic(err)
		}
		if info := entry.GetAuxilaryInfo(); info != nil {
			switch v := info.(type) {
			case uint64:
				s.syncpending = v
			case *common.Range:
				s.checkpointpending = v.Right
			default:
				panic("not supported")
			}
		}
	}
	err = s.store.Sync()
	if err != nil {
		panic(err)
	}
	if s.checkpointpending > s.checkpointed {
		s.SetCheckpointId(s.checkpointpending)
	}
	if s.syncpending > s.synced {
		s.SetSyncedId(s.syncpending)
	}
	for _, idx := range indice {
		entries[idx].(AsyncEntry).DoneWithErr(nil)
	}
}

func (s *batchStore) GetCheckpointId() uint64 {
	return atomic.LoadUint64(&s.checkpointed)
}

func (s *batchStore) SetCheckpointId(id uint64) {
	atomic.StoreUint64(&s.checkpointed, id)
}

func (s *batchStore) GetSyncedId() uint64 {
	return atomic.LoadUint64(&s.synced)
}

func (s *batchStore) SetSyncedId(id uint64) {
	atomic.StoreUint64(&s.synced, id)
}

func (s *batchStore) Close() error {
	atomic.StoreInt32(&s.closed, int32(1))
	s.flushWg.Wait()
	s.flushCancel()
	s.wg.Wait()
	return nil
}
