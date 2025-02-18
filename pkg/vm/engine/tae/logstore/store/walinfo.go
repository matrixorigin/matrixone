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
	// group -> lsn -> dsn
	lsn2DSNMapping map[uint32]map[uint64]uint64

	lsnMu              sync.RWMutex
	driverCheckpointed atomic.Uint64

	watermark struct {
		mu  sync.Mutex
		lsn map[uint32]uint64
	}

	syncing map[uint32]uint64 //todo

	synced     map[uint32]uint64 //todo
	syncedMu   sync.RWMutex
	commitCond sync.Cond

	checkpointed   map[uint32]uint64
	checkpointedMu sync.RWMutex
	ckpcnt         map[uint32]uint64
	ckpcntMu       sync.RWMutex

	minLsn map[uint32]uint64
}

func newWalInfo() *StoreInfo {
	s := StoreInfo{
		checkpointInfo: make(map[uint32]*checkpointInfo),
		lsn2DSNMapping: make(map[uint32]map[uint64]uint64),
		syncing:        make(map[uint32]uint64),
		commitCond:     *sync.NewCond(new(sync.Mutex)),
		checkpointed:   make(map[uint32]uint64),
		synced:         make(map[uint32]uint64),
		ckpcnt:         make(map[uint32]uint64),
		minLsn:         make(map[uint32]uint64),
	}
	s.watermark.lsn = make(map[uint32]uint64)
	return &s
}

func (w *StoreInfo) GetCurrSeqNum(gid uint32) (lsn uint64) {
	w.watermark.mu.Lock()
	defer w.watermark.mu.Unlock()
	return w.watermark.lsn[gid]
}

func (w *StoreInfo) GetSynced(gid uint32) (lsn uint64) {
	w.syncedMu.RLock()
	defer w.syncedMu.RUnlock()
	lsn = w.synced[gid]
	return
}

func (w *StoreInfo) GetPendding(gid uint32) (cnt uint64) {
	lsn := w.GetCurrSeqNum(gid)
	w.ckpcntMu.RLock()
	ckpcnt := w.ckpcnt[gid]
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
	if lsn, ok := w.watermark.lsn[gid]; ok {
		lsn++
		w.watermark.lsn[gid] = lsn
		return lsn
	}
	w.watermark.lsn[gid] = 1
	return 1
}

func (w *StoreInfo) logDriverLsn(driverEntry *driverEntry.Entry) {
	info := driverEntry.Info

	if w.syncing[info.Group] < info.GroupLSN {
		w.syncing[info.Group] = info.GroupLSN
	}

	w.lsnMu.Lock()
	lsnMap, ok := w.lsn2DSNMapping[info.Group]
	if !ok {
		lsnMap = make(map[uint64]uint64)
		w.lsn2DSNMapping[info.Group] = lsnMap
	}
	lsnMap[info.GroupLSN] = driverEntry.DSN
	w.lsnMu.Unlock()
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

func (w *StoreInfo) retryGetDriverLsn(gid uint32, lsn uint64) (driverLsn uint64, err error) {
	driverLsn, err = w.getDriverLsn(gid, lsn)
	if err == ErrGroupNotFount || err == ErrLsnNotFount {
		currLsn := w.GetCurrSeqNum(gid)
		if lsn <= currLsn {
			for i := 0; i < 10; i++ {
				logutil.Debugf("retry %d-%d", gid, lsn)
				w.commitCond.L.Lock()
				driverLsn, err = w.getDriverLsn(gid, lsn)
				if err != ErrGroupNotFount && err != ErrLsnNotFount {
					w.commitCond.L.Unlock()
					return
				}
				w.commitCond.Wait()
				w.commitCond.L.Unlock()
				driverLsn, err = w.getDriverLsn(gid, lsn)
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

func (w *StoreInfo) getDriverLsn(gid uint32, lsn uint64) (driverLsn uint64, err error) {
	w.lsnMu.RLock()
	defer w.lsnMu.RUnlock()
	minLsn := w.minLsn[gid]
	if lsn < minLsn {
		return 0, ErrLsnTooSmall
	}
	lsnMap, ok := w.lsn2DSNMapping[gid]
	if !ok {
		return 0, ErrGroupNotFount
	}
	driverLsn, ok = lsnMap[lsn]
	if !ok {
		return 0, ErrLsnNotFount
	}
	return
}

func (w *StoreInfo) gcDSNMapping(dsnIntent uint64) {
	w.lsnMu.Lock()
	defer w.lsnMu.Unlock()
	for gid, lsn2DSNMapping := range w.lsn2DSNMapping {
		minLsn := w.minLsn[gid]
		lsnsToDelete := make([]uint64, 0)
		for storeLSN, dsn := range lsn2DSNMapping {
			if dsn < dsnIntent {
				lsnsToDelete = append(lsnsToDelete, storeLSN)
				if storeLSN > minLsn {
					minLsn = storeLSN
				}
			}
		}
		for _, lsn := range lsnsToDelete {
			delete(lsn2DSNMapping, lsn)
		}
		w.minLsn[gid] = minLsn + 1
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
		// logutil.Infof("%d-%v", gid, ckp)
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
	return w.driverCheckpointed.Load()
}
func (w *StoreInfo) getDriverCheckpointed() (gid uint32, driverLsn uint64) {
	// deep copy watermark
	watermark := make(map[uint32]uint64, 0)
	w.watermark.mu.Lock()
	for g, lsn := range w.watermark.lsn {
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
		drLsn, err := w.retryGetDriverLsn(entry.GTCustomized, lsn+1)
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
