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
	"errors"
	"sync"
	"sync/atomic"

	// "github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
)

var (
	ErrGroupNotExist       = errors.New("group not existed")
	ErrLsnNotExist         = errors.New("lsn not existed")
	ErrVFileVersionTimeOut = errors.New("get vfile version timeout")
	ErrLsnCheckpointed     = errors.New("lsn has been checkpointed")
)

type syncBase struct {
	*sync.RWMutex
	Lsn              uint64 //r
	lsnmu            sync.RWMutex
	checkpointing    uint64 //r/(i first entry)
	ckpmu            *sync.RWMutex
	truncatedVersion int //i
	syncing          uint64 //r
	synced           uint64//r
	addrs            map[int]*common.ClosedIntervals //group-version-glsn range //r
	addrmu           sync.RWMutex
	commitCond       sync.Cond

	// calculatedCond sync.Cond
}

func newSyncBase() *syncBase {
	return &syncBase{
		lsnmu:          sync.RWMutex{},
		addrs:          make(map[int]*common.ClosedIntervals),
		addrmu:         sync.RWMutex{},
		ckpmu:          &sync.RWMutex{},
		commitCond:     *sync.NewCond(new(sync.Mutex)),
		// calculatedCond: *sync.NewCond(new(sync.Mutex)),
	}
}

// func (base *syncBase) OnReplay(r *replayer) {
// 	base.addrs = r.addrs
// 	base.groupLSN = r.groupLSN
// 	for k, v := range r.groupLSN {
// 		base.synced.ids[k] = v
// 	}
// 	for k, v := range r.groupLSN {
// 		base.syncing[k] = v
// 	}
// 	base.checkpointing = r.checkpointrange
// 	for groupId := range base.checkpointing {
// 		base.checkpointed.ids[groupId] = base.checkpointing[groupId].GetCheckpointed()
// 		base.ckpCnt.ids[groupId] = base.checkpointing[groupId].GetCkpCnt()
// 	}
// 	r.checkpointrange = base.checkpointing
// 	base.syncedVersion = uint64(r.ckpVersion)
// 	base.tidLsnMaps = r.tidlsnMap
// }

func (base *syncBase) GetVersionByGLSN(lsn uint64) (int, error) {
	base.addrmu.RLock()
	defer base.addrmu.RUnlock()
	for ver, interval := range base.addrs {
		if interval.Contains(*common.NewClosedIntervalsByInt(lsn)) {
			return ver, nil
		}
	}
	// for ver, lsns := range base.addrs {
	// 	logutil.Infof("versionsMap %d %v", ver, lsns)
	// }
	return 0, ErrLsnNotExist
}

func (base *syncBase) OnEntryReceived(v *entry.Entry) error {
	base.syncing = v.Lsn
	base.addrmu.Lock()
	defer base.addrmu.Unlock()
	addr := v.Ctx.(*VFileAddress)
	interval, ok := base.addrs[addr.Version]
	if !ok {
		interval = common.NewClosedIntervals()
		base.addrs[addr.Version] = interval
	}
	interval.TryMerge(*common.NewClosedIntervalsByInt(v.Lsn))
	return nil
}

func (base *syncBase) GetPenddingCnt() uint64 {
	// ckp := base.GetCKpCnt(groupId)
	// commit := base.GetSynced(groupId)
	return 0
}

func (base *syncBase) GetTruncated() (uint64,error) {
	lsn := atomic.LoadUint64(&base.checkpointing)
	return lsn,nil
}

func (base *syncBase) SetCheckpointed(groupId uint32, id uint64) {
	// base.checkpointed.Lock()
	// base.checkpointed.ids[groupId] = id
	// base.checkpointed.Unlock()
}

// func (base *syncBase) GetSynced(groupId uint32) uint64 {
// 	base.synced.RLock()
// 	defer base.synced.RUnlock()
// 	return base.synced.ids[groupId]
// }

// func (base *syncBase) SetSynced(groupId uint32, id uint64) {
// 	base.synced.Lock()
// 	base.synced.ids[groupId] = id
// 	base.synced.Unlock()
// }

// func (base *syncBase) SetCKpCnt(groupId uint32, id uint64) {
// 	base.ckpCnt.Lock()
// 	base.ckpCnt.ids[groupId] = id
// 	base.ckpCnt.Unlock()
// }

//TODO do checkpoint
func (base *syncBase) OnCommit() {
	base.commitCond.L.Lock()
	base.commitCond.Broadcast()
	base.commitCond.L.Unlock()
	// for group, checkpointing := range base.checkpointing {
	// 	checkpointingId := checkpointing.GetCheckpointed()
	// 	ckpcnt := checkpointing.GetCkpCnt()
	// 	// logutil.Infof("G%d-%v",group,checkpointing)
	// 	checkpointedId := base.GetCheckpointed(group)
	// 	if checkpointingId > checkpointedId {
	// 		base.SetCheckpointed(group, checkpointingId)
	// 	}
	// 	preCnt := base.GetCKpCnt(group)
	// 	if ckpcnt > preCnt {
	// 		base.SetCKpCnt(group, ckpcnt)
	// 	}
	// }

	if base.syncing > base.synced {
		base.synced = base.syncing
	}
}

func (base *syncBase) AllocateLsn() uint64 {
	base.lsnmu.Lock()
	defer base.lsnmu.Unlock()
	base.Lsn++
	return base.Lsn
}

func (base *syncBase) GetCurrSeqNum() uint64 {
	base.lsnmu.RLock()
	defer base.lsnmu.RUnlock()
	return base.Lsn
}
