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
	"errors"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

type syncBase struct {
	*sync.RWMutex
	groupLSN                     map[uint32]uint64 // for alloc
	lsnmu                        sync.RWMutex
	checkpointing                map[uint32]*checkpointInfo
	syncing                      map[uint32]uint64
	checkpointed, synced, ckpCnt *syncMap
	uncommits                    map[uint32][]uint64
	addrs                        map[uint32]map[int]common.ClosedInterval //group-version-glsn range
	addrmu                       sync.RWMutex
}

type checkpointInfo struct {
	ranges  *common.ClosedIntervals
	partial map[uint64]*partialCkpInfo
}

func newCheckpointInfo() *checkpointInfo {
	return &checkpointInfo{
		ranges:  common.NewClosedIntervals(),
		partial: make(map[uint64]*partialCkpInfo),
	}
}

func (info *checkpointInfo) UpdateWtihRanges(intervals *common.ClosedIntervals) {
	info.ranges.TryMerge(*intervals)
}

func (info *checkpointInfo) UpdateWtihPartialCheckpoint(lsn uint64, ckps *partialCkpInfo) {
	if info.ranges.Contains(*common.NewClosedIntervalsByInt(lsn)) {
		return
	}
	partialInfo, ok := info.partial[lsn]
	if !ok {
		partialInfo = newPartialCkpInfo(ckps.size)
		info.partial[lsn] = partialInfo
	}
	partialInfo.MergePartialCkpInfo(ckps)
	if partialInfo.IsAllCheckpointed() {
		info.ranges.TryMerge(*common.NewClosedIntervalsByInt(lsn))
		delete(info.partial, lsn)
	}
}

func (info *checkpointInfo) UpdateWithCommandInfo(lsn uint64, cmds *entry.CommandInfo) {
	if info.ranges.Contains(*common.NewClosedIntervalsByInt(lsn)) {
		return
	}
	partialInfo, ok := info.partial[lsn]
	if !ok {
		partialInfo = newPartialCkpInfo(cmds.Size)
		info.partial[lsn] = partialInfo
	}
	partialInfo.MergeCommandInfos(cmds)
	if partialInfo.IsAllCheckpointed() {
		info.ranges.TryMerge(*common.NewClosedIntervalsByInt(lsn))
		delete(info.partial, lsn)
	}
}

func (info *checkpointInfo) GetCheckpointed() uint64 {
	if info.ranges == nil || len(info.ranges.Intervals) == 0 {
		return 0
	}
	if info.ranges.Intervals[0].Start != 1 {
		return 0
	}
	return info.ranges.Intervals[0].End
}

func (info *checkpointInfo) GetCkpCnt() uint64 {
	cnt := uint64(0)
	cnt += uint64(info.ranges.GetCardinality())
	// cnt += uint64(len(info.partial))
	return cnt
}

type syncMap struct {
	*sync.RWMutex
	ids map[uint32]uint64
}

func newSyncMap() *syncMap {
	return &syncMap{
		RWMutex: new(sync.RWMutex),
		ids:     make(map[uint32]uint64),
	}
}
func newSyncBase() *syncBase {
	return &syncBase{
		groupLSN:      make(map[uint32]uint64),
		lsnmu:         sync.RWMutex{},
		checkpointing: make(map[uint32]*checkpointInfo),
		syncing:       make(map[uint32]uint64),
		checkpointed:  newSyncMap(),
		synced:        newSyncMap(),
		ckpCnt:        newSyncMap(),
		uncommits:     make(map[uint32][]uint64),
		addrs:         make(map[uint32]map[int]common.ClosedInterval),
		addrmu:        sync.RWMutex{},
	}
}
func (base *syncBase) OnReplay(r *replayer) {
	base.addrs = r.addrs
	base.groupLSN = r.groupLSN
	for k, v := range r.groupLSN {
		base.synced.ids[k] = v
	}
	for groupId, ckps := range r.checkpointrange {
		base.checkpointed.ids[groupId] = ckps.GetCheckpointed()
	}
}
func (base *syncBase) GetVersionByGLSN(groupId uint32, lsn uint64) (int, error) {
	base.addrmu.RLock()
	defer base.addrmu.RUnlock()
	versionsMap, ok := base.addrs[groupId]
	if !ok {
		return 0, errors.New("group not existed")
	}
	for ver, interval := range versionsMap {
		if interval.Contains(common.ClosedInterval{Start: lsn, End: lsn}) {
			return ver, nil
		}
	}
	fmt.Printf("versionsMap is %v\n", versionsMap)
	return 0, errors.New("lsn not existed")
}

//TODO
func (base *syncBase) GetLastAddr(groupName uint32, tid uint64) *VFileAddress {
	// tidMap, ok := base.uncommits[groupName]
	// if !ok {
	// 	return nil
	// }
	return nil
}

func (base *syncBase) OnEntryReceived(v *entry.Info) error {
	switch v.Group {
	case entry.GTCKp:
		for _, intervals := range v.Checkpoints {
			ckpInfo, ok := base.checkpointing[intervals.Group]
			if !ok {
				ckpInfo = newCheckpointInfo()
				base.checkpointing[intervals.Group] = ckpInfo
			}
			if intervals.Ranges != nil && len(intervals.Ranges.Intervals) > 0 {
				ckpInfo.UpdateWtihRanges(intervals.Ranges)
			}
			if intervals.Command != nil {
				for lsn, cmds := range intervals.Command {
					ckpInfo.UpdateWithCommandInfo(lsn, &cmds)
				}
			}
		}
	case entry.GTUncommit:
		// addr := v.Addr.(*VFileAddress)
		for _, tid := range v.Uncommits {
			tids, ok := base.uncommits[tid.Group]
			if !ok {
				tids = make([]uint64, 0)
			}
			existed := false
			for _, id := range tids {
				if id == tid.Tid {
					existed = true
					break
				}
			}
			if !existed {
				tids = append(tids, tid.Tid)
			}
			base.uncommits[tid.Group] = tids
		}
		// fmt.Printf("receive uncommit %d-%d\n", v.Group, v.GroupLSN)
	default:
		base.syncing[v.Group] = v.GroupLSN
	}
	base.addrmu.Lock()
	defer base.addrmu.Unlock()
	addr := v.Info.(*VFileAddress)
	versionRanges, ok := base.addrs[addr.Group]
	if !ok {
		versionRanges = make(map[int]common.ClosedInterval)
	}
	interval, ok := versionRanges[addr.Version]
	if !ok {
		interval = common.ClosedInterval{}
	}
	interval.TryMerge(common.ClosedInterval{Start: 0, End: addr.LSN})
	versionRanges[addr.Version] = interval
	base.addrs[addr.Group] = versionRanges
	// fmt.Printf("versionsMap is %v\n", base.addrs)
	return nil
}

func (base *syncBase) GetPenddingCnt(groupId uint32) uint64 {
	ckp := base.GetCKpCnt(groupId)
	commit := base.GetSynced(groupId)
	return commit - ckp
}

func (base *syncBase) GetCheckpointed(groupId uint32) uint64 {
	base.checkpointed.RLock()
	defer base.checkpointed.RUnlock()
	return base.checkpointed.ids[groupId]
}

func (base *syncBase) SetCheckpointed(groupId uint32, id uint64) {
	base.checkpointed.Lock()
	base.checkpointed.ids[groupId] = id
	base.checkpointed.Unlock()
}

func (base *syncBase) GetSynced(groupId uint32) uint64 {
	base.synced.RLock()
	defer base.synced.RUnlock()
	return base.synced.ids[groupId]
}

func (base *syncBase) SetSynced(groupId uint32, id uint64) {
	base.synced.Lock()
	base.synced.ids[groupId] = id
	base.synced.Unlock()
}

func (base *syncBase) GetCKpCnt(groupId uint32) uint64 {
	base.ckpCnt.RLock()
	defer base.ckpCnt.RUnlock()
	return base.ckpCnt.ids[groupId]
}

func (base *syncBase) SetCKpCnt(groupId uint32, id uint64) {
	base.ckpCnt.Lock()
	base.ckpCnt.ids[groupId] = id
	base.ckpCnt.Unlock()
}

func (base *syncBase) OnCommit() {
	for group, checkpointing := range base.checkpointing {
		checkpointingId := checkpointing.GetCheckpointed()
		ckpcnt := checkpointing.GetCkpCnt()
		checkpointedId := base.GetCheckpointed(group)
		if checkpointingId > checkpointedId {
			base.SetCheckpointed(group, checkpointingId)
		}
		preCnt := base.GetCKpCnt(group)
		if ckpcnt > preCnt {
			base.SetCKpCnt(group, ckpcnt)
		}
	}

	for group, syncingId := range base.syncing {
		syncedId := base.GetSynced(group)
		if syncingId > syncedId {
			base.SetSynced(group, syncingId)
		}
	}
}

func (base *syncBase) AllocateLsn(groupID uint32) uint64 {
	base.lsnmu.Lock()
	defer base.lsnmu.Unlock()
	lsn, ok := base.groupLSN[groupID]
	if !ok {
		base.groupLSN[groupID] = 1
		return 1
	}
	lsn++
	base.groupLSN[groupID] = lsn
	return lsn
}

func (base *syncBase) GetCurrSeqNum(groupID uint32) uint64 {
	base.lsnmu.RLock()
	defer base.lsnmu.RUnlock()
	lsn, ok := base.groupLSN[groupID]
	if !ok {
		return 0
	}
	return lsn
}
