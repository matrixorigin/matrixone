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

package batchstoredriver

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
)

var (
	DefaultMaxBatchSize  = 500
	DefaultMaxSyncSize   = 10
	DefaultMaxCommitSize = 10
	DefaultBatchPerSync  = 100
	DefaultSyncDuration  = time.Millisecond * 2
	FlushEntry           entry.Entry
)

type StoreCfg struct {
	RotateChecker  RotateChecker
	HistoryFactory HistoryFactory
}

type baseStore struct {
	syncBase
	sm.ClosedState
	dir, name       string
	flushWg         sync.WaitGroup
	flushWgMu       *sync.RWMutex
	flushCtx        context.Context
	flushCancel     context.CancelFunc
	flushQueue      sm.Queue
	syncQueue       sm.Queue
	commitQueue     sm.Queue
	postCommitQueue sm.Queue
	truncateQueue   sm.Queue
	file            File
	mu              *sync.RWMutex
}

func NewBaseStore(dir, name string, cfg *StoreCfg) (*baseStore, error) {
	var err error
	bs := &baseStore{
		syncBase:  *newSyncBase(),
		dir:       dir,
		name:      name,
		mu:        &sync.RWMutex{},
		flushWgMu: &sync.RWMutex{},
	}
	bs.flushQueue = sm.NewSafeQueue(DefaultMaxBatchSize*100, DefaultMaxBatchSize, bs.onEntries)
	bs.syncQueue = sm.NewSafeQueue(DefaultMaxBatchSize*100, DefaultMaxBatchSize, bs.onSyncs)
	bs.commitQueue = sm.NewSafeQueue(DefaultMaxBatchSize*100, DefaultMaxBatchSize, bs.onCommits)
	bs.postCommitQueue = sm.NewSafeQueue(DefaultMaxBatchSize*100, DefaultMaxBatchSize, bs.onPostCommits)
	bs.truncateQueue = sm.NewSafeQueue(DefaultMaxBatchSize, DefaultMaxBatchSize, bs.onTruncate)
	if cfg == nil {
		cfg = &StoreCfg{}
	}
	bs.file, err = OpenRotateFile(dir, name, nil, cfg.RotateChecker, cfg.HistoryFactory, bs)
	if err != nil {
		return nil, err
	}
	bs.flushCtx, bs.flushCancel = context.WithCancel(context.Background())
	bs.start()
	return bs, nil
}

func (bs *baseStore) start() {
	bs.flushQueue.Start()
	bs.syncQueue.Start()
	bs.commitQueue.Start()
	bs.postCommitQueue.Start()
	bs.truncateQueue.Start()
}

func (bs *baseStore) onTruncate(batch ...any) {
	lsn := bs.checkpointing.Load()
	if lsn == 0 {
		return
	}
	version, err := bs.retryGetVersionByGLSN(lsn)
	if err != nil {
		logutil.Debugf("get %d", lsn)
		panic(err)
	}
	if version-1 <= bs.truncatedVersion {
		return
	}
	for i := bs.truncatedVersion + 1; i < version; i++ {
		_, err := bs.file.GetHistory().DropEntry(i)
		if err != nil {
			panic(err)
		}
		bs.addrmu.Lock()
		delete(bs.addrs, i)
		bs.addrmu.Unlock()
	}
	bs.truncatedVersion = version - 1
}

func (bs *baseStore) onPostCommits(batches ...any) {
	for _, item := range batches {
		e := item.(*entry.Entry)
		err := bs.syncBase.OnEntryReceived(e)
		if err != nil {
			panic(err)
		}
	}
	bs.syncBase.OnCommit()
}

func (bs *baseStore) onCommits(batches ...any) {
	for _, v := range batches {
		bat := v.([]any)
		for _, item := range bat {
			e := item.(*entry.Entry)
			e.SetInfo()
			// if e.IsPrintTime() {
			// 	logutil.Infof("sync and queues takes %dms", e.Duration().Milliseconds())
			// 	e.StartTime()
			// }
			e.DoneWithErr(nil)
			_, err := bs.postCommitQueue.Enqueue(e)
			if err != nil {
				panic(err)
			}
		}
		cnt := len(bat)
		bs.flushWg.Add(-1 * cnt)
	}
}

func (bs *baseStore) onSyncs(batches ...any) {
	var err error
	if err = bs.file.Sync(); err != nil {
		panic(err)
	}
	for _, item := range batches {
		_, err := bs.commitQueue.Enqueue(item)
		if err != nil {
			panic(err)
		}
	}
}

func (bs *baseStore) onEntries(entries ...any) {
	for _, item := range entries {
		e := item.(*entry.Entry)
		// if e.IsPrintTime() {
		// 	logutil.Infof("flush queue takes %dms", e.Duration().Milliseconds())
		// 	e.StartTime()
		// }
		var err error
		appender := bs.file.GetAppender()
		e.Ctx, err = appender.Prepare(e.GetSize(), e.Lsn)
		if err != nil {
			panic(err)
		}
		if _, err = e.WriteTo(appender); err != nil {
			panic(err)
		}
		// if e.IsPrintTime() {
		// 	logutil.Infof("onentry1 takes %dms", e.Duration().Milliseconds())
		// 	e.StartTime()
		// }
		if err = appender.Commit(); err != nil {
			panic(err)
		}
		// if e.IsPrintTime() {
		// 	logutil.Infof("onEntries2 takes %dms", e.Duration().Milliseconds())
		// 	e.StartTime()
		// }
	}
	bat := make([]any, len(entries))
	copy(bat, entries)
	_, err := bs.syncQueue.Enqueue(bat)
	if err != nil {
		panic(err)
	}
}

//TODO: commented due to static check
//type prepareCommit struct {
//	checkpointing, syncing map[uint32]uint64
//}

func (bs *baseStore) Close() error {
	if !bs.TryClose() {
		return nil
	}
	bs.flushWgMu.RLock()
	bs.flushWg.Wait()
	bs.flushWgMu.RUnlock()
	bs.flushCancel()
	bs.flushQueue.Stop()
	bs.syncQueue.Stop()
	bs.commitQueue.Stop()
	bs.postCommitQueue.Stop()
	bs.truncateQueue.Stop()
	return bs.file.Close()
}

func (bs *baseStore) Truncate(lsn uint64) (err error) {
	checkpointing := bs.checkpointing.Load()
	if lsn <= checkpointing {
		return nil
	}
	bs.ckpmu.Lock()
	checkpointing = bs.checkpointing.Load()
	if lsn <= checkpointing {
		bs.ckpmu.Unlock()
		return nil
	}
	bs.checkpointing.Store(lsn)
	bs.ckpmu.Unlock()
	_, err = bs.truncateQueue.Enqueue(lsn)
	if err != nil && err != sm.ErrClose {
		panic(err)
	}
	return nil
}

func (bs *baseStore) Append(e *entry.Entry) error {
	// if e.IsPrintTime() {
	// 	e.StartTime()
	// }
	if bs.IsClosed() {
		return sm.ErrClose
	}
	bs.flushWgMu.Lock()
	bs.flushWg.Add(1)
	if bs.IsClosed() {
		bs.flushWg.Done()
		return sm.ErrClose
	}
	bs.flushWgMu.Unlock()
	bs.mu.Lock()
	lsn := bs.AllocateLsn()
	e.Lsn = lsn
	// if e.IsPrintTime() {
	// 	logutil.Infof("append entry takes %dms", e.Duration().Milliseconds())
	// 	e.StartTime()
	// }
	if err := e.Entry.ExecutePreCallbacks(); err != nil {
		return err
	}
	_, err := bs.flushQueue.Enqueue(e)
	if err != nil {
		panic(err)
	}
	bs.mu.Unlock()
	return nil
}

func (bs *baseStore) Replay(h driver.ApplyHandle) error {
	r := newReplayer(h)
	bs.addrs = r.addrs
	err := bs.file.Replay(r)
	if err != nil {
		return err
	}
	bs.onReplay(r)
	logutil.Info("open-tae", common.OperationField("replay"),
		common.OperandField("wal"),
		common.AnyField("backend", "batchstore"),
		common.AnyField("apply cost", r.applyDuration),
		common.AnyField("read cost", r.readDuration))
	return nil
}

func (bs *baseStore) Read(lsn uint64) (*entry.Entry, error) {
	ver, err := bs.retryGetVersionByGLSN(lsn)
	if err != nil {
		return nil, err
	}
	vf, err := bs.file.GetEntryByVersion(ver)
	if err != nil {
		return nil, err
	}
	e, err := vf.Load(lsn)
	return e, err
}

func (bs *baseStore) retryGetVersionByGLSN(lsn uint64) (int, error) {
	ver, err := bs.GetVersionByGLSN(lsn)
	if err == ErrGroupNotExist || err == ErrLsnNotExist {
		syncedLsn := bs.GetCurrSeqNum()
		if lsn <= syncedLsn {
			for i := 0; i < 10; i++ {
				bs.syncBase.commitCond.L.Lock()
				ver, err = bs.GetVersionByGLSN(lsn)
				if err == nil {
					bs.syncBase.commitCond.L.Unlock()
					break
				}
				bs.syncBase.commitCond.Wait()
				bs.syncBase.commitCond.L.Unlock()
				if err != ErrGroupNotExist && err != ErrLsnNotExist {
					break
				}
			}
			if err != nil {
				return 0, err
			}
		}
	}
	return ver, err
}
