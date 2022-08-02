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
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	driverEntry "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

var (
	ErrGroupNotFount = errors.New("group not found")
	ErrLsnNotFount   = errors.New("lsn not found")
	ErrTimeOut       = errors.New("retry time our")
)

type StoreInfo struct {
	checkpointInfo      map[uint32]*checkpointInfo
	ckpMu               sync.RWMutex
	walDriverLsnMap     map[uint32]map[uint64]uint64
	lsnMu               sync.RWMutex
	driverCheckpointing uint64
	driverCheckpointed  uint64
	walCurrentLsn       map[uint32]uint64
	lsnmu               sync.RWMutex
	syncing             map[uint32]uint64

	synced     map[uint32]uint64
	syncedMu   sync.RWMutex
	commitCond sync.Cond

	checkpointed   map[uint32]uint64
	checkpointedMu sync.RWMutex
	ckpcnt         map[uint32]uint64
	ckpcntMu       sync.RWMutex
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
	lsn := w.GetSynced(gid)
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

	if info.Group == GroupInternal {
		w.checkpointedMu.Lock()
		w.checkpointed[GroupCKP] = info.TargetLsn
		w.checkpointedMu.Unlock()
	}

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
				logutil.Infof("retry %d-%d", gid, lsn)
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

func (w *StoreInfo) logCheckpointInfo(info *entry.Info) any {
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
	return nil
}

func (w *StoreInfo) onCheckpoint() {
	w.checkpointedMu.Lock()
	for gid, ckp := range w.checkpointInfo {
		ckped := ckp.GetCheckpointed()
		if ckped == 0 {
			continue
		}
		w.checkpointed[gid] = ckped
		logutil.Infof("%d-%v", gid, ckp)
	}
	w.checkpointedMu.Unlock()
	w.ckpcntMu.Lock()
	for gid, ckp := range w.checkpointInfo {
		w.ckpcnt[gid] = ckp.GetCkpCnt()
	}
	w.ckpcntMu.Unlock()
}

func (w *StoreInfo) getDriverCheckpointed() (gid uint32, driverLsn uint64) {
	w.checkpointedMu.RLock()
	if len(w.checkpointed) == 0 {
		w.checkpointedMu.RUnlock()
		return
	}
	driverLsn = math.MaxInt64
	for g, lsn := range w.checkpointed {
		drLsn, err := w.retryGetDriverLsn(g, lsn)
		if err != nil {
			logutil.Infof("%d-%d", g, lsn)
			panic(err)
		}
		if drLsn < driverLsn {
			gid = g
			driverLsn = drLsn
		}
	}
	w.checkpointedMu.RUnlock()
	return
}

func (w *StoreInfo) makeInternalCheckpointEntry() (e entry.Entry) {
	e = entry.GetBase()
	lsn := w.GetSynced(GroupCKP)
	e.SetType(entry.ETPostCommit)
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

func (w *StoreInfo) writePostCommitEntry(writer io.Writer) (n int64, err error) {
	w.ckpMu.RLock()
	defer w.ckpMu.RUnlock()
	//checkpointing
	length := uint32(len(w.checkpointInfo))
	if err = binary.Write(writer, binary.BigEndian, length); err != nil {
		return
	}
	n += 4
	for groupID, ckpInfo := range w.checkpointInfo {
		if err = binary.Write(writer, binary.BigEndian, groupID); err != nil {
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
