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
	"bytes"
	"io"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	checkpointInfo      map[uint32]*checkpointInfo
	ckpMu               sync.RWMutex
	walDriverLsnMap     map[uint32]map[uint64]uint64
	lsnMu               sync.RWMutex
	driverCheckpointing atomic.Uint64
	driverCheckpointed  atomic.Uint64
	walCurrentLsn       map[uint32]uint64 //todo
	lsnmu               sync.RWMutex
	syncing             map[uint32]uint64 //todo

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
	return &StoreInfo{
		checkpointInfo:  make(map[uint32]*checkpointInfo),
		ckpMu:           sync.RWMutex{},
		walDriverLsnMap: make(map[uint32]map[uint64]uint64),
		lsnMu:           sync.RWMutex{},
		walCurrentLsn:   make(map[uint32]uint64),
		lsnmu:           sync.RWMutex{},
		syncing:         make(map[uint32]uint64),
		commitCond:      *sync.NewCond(&sync.Mutex{}),

		checkpointed:   make(map[uint32]uint64),
		checkpointedMu: sync.RWMutex{},
		synced:         make(map[uint32]uint64),
		syncedMu:       sync.RWMutex{},
		ckpcnt:         make(map[uint32]uint64),
		ckpcntMu:       sync.RWMutex{},

		minLsn: make(map[uint32]uint64),
	}
}

func (w *StoreInfo) GetCurrSeqNum(gid uint32) (lsn uint64) {
	w.lsnmu.RLock()
	defer w.lsnmu.RUnlock()
	lsn = w.walCurrentLsn[gid]
	return
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
	w.lsnmu.Lock()
	defer w.lsnmu.Unlock()
	lsn, ok := w.walCurrentLsn[gid]
	if !ok {
		w.walCurrentLsn[gid] = 1
		return 1
	}
	lsn++
	w.walCurrentLsn[gid] = lsn
	return lsn
}

func (w *StoreInfo) logDriverLsn(driverEntry *driverEntry.Entry) {
	info := driverEntry.Info

	if w.syncing[info.Group] < info.GroupLSN {
		w.syncing[info.Group] = info.GroupLSN
	}

	w.lsnMu.Lock()
	lsnMap, ok := w.walDriverLsnMap[info.Group]
	if !ok {
		lsnMap = make(map[uint64]uint64)
		w.walDriverLsnMap[info.Group] = lsnMap
	}
	lsnMap[info.GroupLSN] = driverEntry.Lsn
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
	lsnMap, ok := w.walDriverLsnMap[gid]
	if !ok {
		return 0, ErrGroupNotFount
	}
	driverLsn, ok = lsnMap[lsn]
	if !ok {
		return 0, ErrLsnNotFount
	}
	return
}

func (w *StoreInfo) gcWalDriverLsnMap(drlsn uint64) {
	w.lsnMu.Lock()
	defer w.lsnMu.Unlock()
	for gid, walDriverLsnMap := range w.walDriverLsnMap {
		minLsn := w.minLsn[gid]
		lsnsToDelete := make([]uint64, 0)
		for storeLSN, driverLSN := range walDriverLsnMap {
			if driverLSN < drlsn {
				lsnsToDelete = append(lsnsToDelete, storeLSN)
				if storeLSN > minLsn {
					minLsn = storeLSN
				}
			}
		}
		for _, lsn := range lsnsToDelete {
			delete(walDriverLsnMap, lsn)
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
	case GroupInternal:
		w.checkpointedMu.Lock()
		w.checkpointed[GroupCKP] = info.TargetLsn
		w.checkpointed[GroupInternal] = info.GroupLSN - 1
		w.checkpointedMu.Unlock()
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
	groups := make(map[uint32]uint64, 0)
	w.lsnmu.Lock()
	for g, lsn := range w.walCurrentLsn {
		groups[g] = lsn
	}
	w.lsnmu.Unlock()

	w.checkpointedMu.RLock()
	defer w.checkpointedMu.RUnlock()
	if len(w.checkpointed) == 0 {
		return
	}
	maxLsn := groups[entry.GTCustomized]
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

func (w *StoreInfo) makeInternalCheckpointEntry() (e entry.Entry) {
	e = entry.GetBase()
	lsn := w.GetSynced(GroupCKP)
	e.SetType(entry.IOET_WALEntry_PostCommit)
	buf, err := w.marshalPostCommitEntry()
	if err != nil {
		panic(err)
	}
	err = e.SetPayload(buf)
	if err != nil {
		panic(err)
	}
	info := &entry.Info{}
	info.TargetLsn = lsn
	info.Group = GroupInternal
	e.SetInfo(info)
	return
}

func (w *StoreInfo) marshalPostCommitEntry() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = w.writePostCommitEntry(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}

func (w *StoreInfo) unmarshalPostCommitEntry(buf []byte) (err error) {
	bbuf := bytes.NewBuffer(buf)
	_, err = w.readPostCommitEntry(bbuf)
	return
}

func (w *StoreInfo) writePostCommitEntry(writer io.Writer) (n int64, err error) {
	w.ckpMu.RLock()
	defer w.ckpMu.RUnlock()
	//checkpointing
	length := uint32(len(w.checkpointInfo))
	if _, err = writer.Write(types.EncodeUint32(&length)); err != nil {
		return
	}
	n += 4
	for groupID, ckpInfo := range w.checkpointInfo {
		if _, err = writer.Write(types.EncodeUint32(&groupID)); err != nil {
			return
		}
		n += 4
		sn, err := ckpInfo.WriteTo(writer)
		n += sn
		if err != nil {
			return n, err
		}
	}
	return
}

func (w *StoreInfo) readPostCommitEntry(reader io.Reader) (n int64, err error) {
	w.ckpMu.Lock()
	defer w.ckpMu.Unlock()
	//checkpointing
	length := uint32(0)
	if _, err = reader.Read(types.EncodeUint32(&length)); err != nil {
		return
	}
	n += 4
	for i := 0; i < int(length); i++ {
		groupID := uint32(0)
		if _, err = reader.Read(types.EncodeUint32(&groupID)); err != nil {
			return
		}
		n += 4
		ckpInfo := newCheckpointInfo()
		sn, err := ckpInfo.ReadFrom(reader)
		n += sn
		if err != nil {
			return n, err
		}
		ckp, ok := w.checkpointInfo[groupID]
		if ok {
			ckp.MergeCheckpointInfo(ckpInfo)
		} else {
			w.checkpointInfo[groupID] = ckpInfo
		}
	}
	return
}
