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
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

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
	for lsn := range info.partial {
		if intervals.ContainsInt(lsn) {
			delete(info.partial, lsn)
		}
	}
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

func (info *checkpointInfo) MergeCommandMap(cmdMap map[uint64]entry.CommandInfo) {
	ckpedLsn := make([]uint64, 0)
	for lsn, cmds := range cmdMap {
		if info.ranges.Contains(*common.NewClosedIntervalsByInt(lsn)) {
			continue
		}
		partialInfo, ok := info.partial[lsn]
		if !ok {
			partialInfo = newPartialCkpInfo(cmds.Size)
			info.partial[lsn] = partialInfo
		}
		partialInfo.MergeCommandInfos(&cmds)
		if partialInfo.IsAllCheckpointed() {
			ckpedLsn = append(ckpedLsn, lsn)
			delete(info.partial, lsn)
		}
	}
	if len(ckpedLsn) == 0 {
		return
	}
	intervals := common.NewClosedIntervalsBySlice(ckpedLsn)
	info.ranges.TryMerge(*intervals)
}

func (info *checkpointInfo) MergeCheckpointInfo(ockp *checkpointInfo) {
	info.ranges.TryMerge(*ockp.ranges)
	for lsn, ockpinfo := range ockp.partial {
		ckpinfo, ok := info.partial[lsn]
		if !ok {
			info.partial[lsn] = ockpinfo
		} else {
			if ckpinfo.size != ockpinfo.size {
				panic("logic err")
			}
			ckpinfo.ckps.Or(ockpinfo.ckps)
			if ckpinfo.IsAllCheckpointed() {
				info.ranges.TryMerge(*common.NewClosedIntervalsByInt(lsn))
				delete(info.partial, lsn)
			}
		}
	}
}

func (info *checkpointInfo) GetCheckpointed() uint64 {
	if info.ranges == nil || len(info.ranges.Intervals) == 0 {
		return 0
	}
	if info.ranges.Intervals[0].Start > 1 {
		return 0
	}
	return info.ranges.Intervals[0].End
}

func (info *checkpointInfo) String() string {
	s := fmt.Sprintf("range %v, partial ", info.ranges)
	for lsn, partial := range info.partial {
		s = fmt.Sprintf("%s[%d-%v]", s, lsn, partial)
	}
	return s
}

func (info *checkpointInfo) GetCkpCnt() uint64 {
	cnt := uint64(0)
	cnt += uint64(info.ranges.GetCardinality())
	// cnt += uint64(len(info.partial))
	return cnt
}

func (info *checkpointInfo) WriteTo(w io.Writer) (n int64, err error) {
	sn, err := info.ranges.WriteTo(w)
	n += sn
	if err != nil {
		return
	}
	length := uint64(len(info.partial))
	if _, err = w.Write(types.EncodeUint64(&length)); err != nil {
		return
	}
	n += 8
	for lsn, partialInfo := range info.partial {
		if _, err = w.Write(types.EncodeUint64(&lsn)); err != nil {
			return
		}
		n += 8
		sn, err = partialInfo.WriteTo(w)
		n += sn
		if err != nil {
			return
		}
	}
	return
}

func (info *checkpointInfo) ReadFrom(r io.Reader) (n int64, err error) {
	info.ranges = common.NewClosedIntervals()
	sn, err := info.ranges.ReadFrom(r)
	n += sn
	if err != nil {
		return
	}
	length := uint64(0)
	if _, err = r.Read(types.EncodeUint64(&length)); err != nil {
		return
	}
	n += 8
	for i := 0; i < int(length); i++ {
		lsn := uint64(0)
		if _, err = r.Read(types.EncodeUint64(&lsn)); err != nil {
			return
		}
		n += 8
		partial := newPartialCkpInfo(0)
		sn, err = partial.ReadFrom(r)
		n += sn
		if err != nil {
			return
		}
		info.partial[lsn] = partial
	}
	return
}
