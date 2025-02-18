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
	"github.com/matrixorigin/matrixone/pkg/logutil"
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
	checkpointInfo map[uint32]*checkpointInfo
	ckpMu          sync.RWMutex

	// lsn: monotonically increasing in each group
	// dsn: monotonically increasing in the whole store
	// dsn:    1, 2, 3, 4, 5
	// group:  1, 2, 1, 1, 2
	// lsn:    1, 1, 2, 3, 2
	lsn2dsn struct {
		mu sync.RWMutex
		// group -> lsn -> dsn
		mapping map[uint32]map[uint64]uint64
		// minLSN: the minimum lsn logged in the lsn2DSN for each group
		minLSN map[uint32]uint64
	}

	watermark struct {
		mu              sync.Mutex
		nextLSN         map[uint32]uint64
		dsnCheckpointed atomic.Uint64
	}

	syncing map[uint32]uint64 //todo

	synced     map[uint32]uint64 //todo
	syncedMu   sync.RWMutex
	commitCond sync.Cond

	checkpointed   map[uint32]uint64
	checkpointedMu sync.RWMutex
	ckpcnt         map[uint32]uint64
	ckpcntMu       sync.RWMutex
}

func newWalInfo() *StoreInfo {
	s := StoreInfo{
		checkpointInfo: make(map[uint32]*checkpointInfo),
		syncing:        make(map[uint32]uint64),
		commitCond:     *sync.NewCond(new(sync.Mutex)),
		checkpointed:   make(map[uint32]uint64),
		synced:         make(map[uint32]uint64),
		ckpcnt:         make(map[uint32]uint64),
	}
	s.watermark.nextLSN = make(map[uint32]uint64)
	s.lsn2dsn.mapping = make(map[uint32]map[uint64]uint64)
	s.lsn2dsn.minLSN = make(map[uint32]uint64)
	return &s
}

func (w *StoreInfo) GetCurrSeqNum() (lsn uint64) {
	w.watermark.mu.Lock()
	defer w.watermark.mu.Unlock()
	return w.watermark.nextLSN[entry.GTCustomized]
}

func (w *StoreInfo) GetSynced(gid uint32) (lsn uint64) {
	w.syncedMu.RLock()
	defer w.syncedMu.RUnlock()
	lsn = w.synced[gid]
	return
}

func (w *StoreInfo) GetPendding() (cnt uint64) {
	lsn := w.GetCurrSeqNum()
	w.ckpcntMu.RLock()
	ckpcnt := w.ckpcnt[entry.GTCustomized]
	w.ckpcntMu.RUnlock()
	cnt = lsn - ckpcnt
	return
}
func (w *StoreInfo) GetCheckpointed(gid uint32) (lsn uint64) {
	w.checkpointedMu.RLock()
	lsn = w.checkpointed[gid]
	w.checkpointedMu.RUnlock()
	return
}

func (w *StoreInfo) SetCheckpointed(gid uint32, lsn uint64) {
	w.checkpointedMu.Lock()
	w.checkpointed[gid] = lsn
	w.checkpointedMu.Unlock()
}

func (w *StoreInfo) allocateLsn(gid uint32) uint64 {
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

func (w *StoreInfo) logDriverLsn(driverEntry *driverEntry.Entry) {
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

func (w *StoreInfo) retryGetDriverLsn(lsn uint64) (dsn uint64, err error) {
	gid := entry.GTCustomized
	dsn, err = w.getDriverLsn(gid, lsn)
	if err == ErrGroupNotFount || err == ErrLsnNotFount {
		currLsn := w.GetCurrSeqNum()
		if lsn <= currLsn {
			for i := 0; i < 10; i++ {
				logutil.Debugf("retry %d-%d", gid, lsn)
				w.commitCond.L.Lock()
				dsn, err = w.getDriverLsn(gid, lsn)
				if err != ErrGroupNotFount && err != ErrLsnNotFount {
					w.commitCond.L.Unlock()
					return
				}
				w.commitCond.Wait()
				w.commitCond.L.Unlock()
				dsn, err = w.getDriverLsn(gid, lsn)
				if err != ErrGroupNotFount && err != ErrLsnNotFount {
					return
				}
			}
			return 0, ErrTimeOut
		}
		return
	}
	return
}

func (w *StoreInfo) getDriverLsn(gid uint32, lsn uint64) (dsn uint64, err error) {
	w.lsn2dsn.mu.RLock()
	defer w.lsn2dsn.mu.RUnlock()
	minLsn := w.lsn2dsn.minLSN[gid]
	if lsn < minLsn {
		return 0, ErrLsnTooSmall
	}
	groupMapping, ok := w.lsn2dsn.mapping[gid]
	if !ok {
		return 0, ErrGroupNotFount
	}
	if dsn, ok = groupMapping[lsn]; !ok {
		return 0, ErrLsnNotFount
	}
	return
}

func (w *StoreInfo) gcDSNMapping(dsnIntent uint64) {
	w.lsn2dsn.mu.Lock()
	defer w.lsn2dsn.mu.Unlock()
	for gid, groupMapping := range w.lsn2dsn.mapping {
		minLsn := w.lsn2dsn.minLSN[gid]
		lsns := make([]uint64, 0)
		for lsn, dsn := range groupMapping {
			if dsn < dsnIntent {
				lsns = append(lsns, lsn)
				if lsn > minLsn {
					minLsn = lsn
				}
			}
		}
		for _, lsn := range lsns {
			delete(groupMapping, lsn)
		}
		w.lsn2dsn.minLSN[gid] = minLsn + 1
	}
}

func (w *StoreInfo) logCheckpointInfo(info *entry.Info) {
	switch info.Group {
	case GroupCKP:
		for _, intervals := range info.Checkpoints {
			w.ckpMu.Lock()
			ckpInfo, ok := w.checkpointInfo[intervals.Group]
			if !ok {
				ckpInfo = newCheckpointInfo()
				w.checkpointInfo[intervals.Group] = ckpInfo
			}
			if intervals.Ranges != nil && len(intervals.Ranges.Intervals) > 0 {
				ckpInfo.UpdateWtihRanges(intervals.Ranges)
			}
			if intervals.Command != nil {
				ckpInfo.MergeCommandMap(intervals.Command)
			}
			w.ckpMu.Unlock()
		}
	}
}

func (w *StoreInfo) onCheckpoint() {
	w.checkpointedMu.Lock()
	for gid, ckp := range w.checkpointInfo {
		ckped := ckp.GetCheckpointed()
		if ckped == 0 {
			continue
		}
		w.checkpointed[gid] = ckped
	}
	w.checkpointedMu.Unlock()
	w.ckpcntMu.Lock()
	for gid, ckp := range w.checkpointInfo {
		w.ckpcnt[gid] = ckp.GetCkpCnt()
	}
	w.ckpcntMu.Unlock()
}
func (w *StoreInfo) GetTruncated() uint64 {
	return w.watermark.dsnCheckpointed.Load()
}
func (w *StoreInfo) getDriverCheckpointed() (gid uint32, dsn uint64) {
	// deep copy watermark
	watermark := make(map[uint32]uint64, 0)
	w.watermark.mu.Lock()
	for g, lsn := range w.watermark.nextLSN {
		watermark[g] = lsn
	}
	w.watermark.mu.Unlock()

	w.checkpointedMu.RLock()
	defer w.checkpointedMu.RUnlock()
	if len(w.checkpointed) == 0 {
		return
	}
	maxLsn := watermark[entry.GTCustomized]
	lsn := w.checkpointed[entry.GTCustomized]
	if lsn < maxLsn {
		drLsn, err := w.retryGetDriverLsn(lsn + 1)
		if err != nil {
			if err == ErrLsnTooSmall {
				return entry.GTCustomized, 0
			}
			panic(err)
		}
		drLsn--
		return entry.GTCustomized, drLsn
	} else {
		return entry.GTCustomized, maxLsn
	}
}
