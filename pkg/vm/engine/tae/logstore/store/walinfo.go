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

package store

import (
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	driverEntry "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

var (
	ErrGroupNotFount = moerr.NewInternalErrorNoCtx("group not found")
	ErrLsnNotFount   = moerr.NewInternalErrorNoCtx("lsn not found")
	ErrTimeOut       = moerr.NewInternalErrorNoCtx("retry timeout")
	ErrLsnTooSmall   = moerr.NewInternalErrorNoCtx("lsn is too small")
)

type StoreInfo struct {
	ckpMu sync.RWMutex

	// lsn: monotonically increasing in each group
	// dsn: monotonically increasing in the whole store
	// dsn:    1, 2, 3, 4, 5
	// group:  1, 2, 1, 1, 2
	// lsn:    1, 1, 2, 3, 2
	lsn2dsn struct {
		mu sync.RWMutex
		// group -> lsn -> dsn
		mapping map[uint32]map[uint64]uint64
	}

	watermark struct {
		mu              sync.Mutex
		nextLSN         map[uint32]uint64
		dsnCheckpointed atomic.Uint64
		lsnCheckpointed atomic.Uint64
	}

	syncing map[uint32]uint64 //todo

	synced     map[uint32]uint64 //todo
	syncedMu   sync.RWMutex
	commitCond sync.Cond

	ckpcnt   map[uint32]uint64
	ckpcntMu sync.RWMutex
}

func newWalInfo() *StoreInfo {
	s := StoreInfo{
		syncing:    make(map[uint32]uint64),
		commitCond: *sync.NewCond(new(sync.Mutex)),
		synced:     make(map[uint32]uint64),
	}
	s.watermark.nextLSN = make(map[uint32]uint64)
	s.lsn2dsn.mapping = make(map[uint32]map[uint64]uint64)
	return &s
}

func (w *StoreInfo) GetCurrSeqNum() (lsn uint64) {
	w.watermark.mu.Lock()
	defer w.watermark.mu.Unlock()
	return w.watermark.nextLSN[entry.GTCustomized]
}

// only for test
func (w *StoreInfo) GetPendding() (cnt uint64) {
	lsnCheckpointed := w.watermark.lsnCheckpointed.Load()
	lsn := w.GetCurrSeqNum()
	return lsn - lsnCheckpointed
}

func (w *StoreInfo) GetCheckpointed() (lsn uint64) {
	return w.watermark.lsnCheckpointed.Load()
}

func (w *StoreInfo) nextLSN(gid uint32) uint64 {
	w.watermark.mu.Lock()
	defer w.watermark.mu.Unlock()
	if lsn, ok := w.watermark.nextLSN[gid]; ok {
		lsn++
		w.watermark.nextLSN[gid] = lsn
		return lsn
	}
	w.watermark.nextLSN[gid] = 1
	return 1
}

func (w *StoreInfo) logDSN(driverEntry *driverEntry.Entry) {
	info := driverEntry.Info

	if w.syncing[info.Group] < info.GroupLSN {
		w.syncing[info.Group] = info.GroupLSN
	}

	w.lsn2dsn.mu.Lock()
	defer w.lsn2dsn.mu.Unlock()
	groupMapping, ok := w.lsn2dsn.mapping[info.Group]
	if !ok {
		groupMapping = make(map[uint64]uint64)
		w.lsn2dsn.mapping[info.Group] = groupMapping
	}
	groupMapping[info.GroupLSN] = driverEntry.DSN
}

func (w *StoreInfo) onAppend() {
	w.commitCond.L.Lock()
	w.commitCond.Broadcast()
	w.commitCond.L.Unlock()
	w.syncedMu.Lock()
	for gid, lsn := range w.syncing {
		w.synced[gid] = lsn
	}
	w.syncedMu.Unlock()
}

func (w *StoreInfo) gcDSNMapping(dsnIntent uint64) {
	lsns := make([]uint64, 0, 100)
	w.lsn2dsn.mu.Lock()
	defer w.lsn2dsn.mu.Unlock()
	for _, groupMapping := range w.lsn2dsn.mapping {
		lsns = lsns[:0]
		for lsn, dsn := range groupMapping {
			if dsn < dsnIntent {
				lsns = append(lsns, lsn)
			}
		}
		for _, lsn := range lsns {
			delete(groupMapping, lsn)
		}
	}
}

func (w *StoreInfo) updateLSNCheckpointed(info *entry.Info) {
	switch info.Group {
	case GroupCKP:
		for _, intervals := range info.Checkpoints {
			maxLSN := intervals.GetMax()
			if maxLSN > w.watermark.lsnCheckpointed.Load() {
				w.watermark.lsnCheckpointed.Store(maxLSN)
			}
		}
	}
}

func (w *StoreInfo) GetTruncated() uint64 {
	return w.watermark.dsnCheckpointed.Load()
}

func (w *StoreInfo) getCheckpointedDSNIntent() (dsn uint64, found bool) {
	lsn := w.watermark.lsnCheckpointed.Load()
	if lsn == 0 {
		return
	}
	w.lsn2dsn.mu.RLock()
	defer w.lsn2dsn.mu.RUnlock()
	mapping, ok := w.lsn2dsn.mapping[entry.GTCustomized]
	if !ok {
		return
	}
	dsn, found = mapping[lsn]
	return
}
