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
	"io"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

type noopObserver struct{}

func (o *noopObserver) OnNewEntry(_ int) {
}

func (o *noopObserver) OnLogInfo(*entry.Info)               {}
func (o *noopObserver) OnNewCheckpoint(*entry.Info)         {}
func (o *noopObserver) OnNewTxn(*entry.Info)                {}
func (o *noopObserver) OnNewUncommit(addrs []*VFileAddress) {}

type replayer struct {
	version         int
	state           vFileState
	uncommit        map[uint32]map[uint64][]*replayEntry
	entrys          []*replayEntry
	checkpointrange map[uint32]*checkpointInfo
	checkpoints     []*replayEntry
	mergeFuncs      map[uint32]func(pre, curr []byte) []byte
	applyEntry      ApplyHandle

	//syncbase
	addrs     map[uint32]map[int]*common.ClosedIntervals
	groupLSN  map[uint32]uint64
	tidlsnMap map[uint32]map[uint64]uint64

	//internal entry
	ckpVersion int
	ckpEntry   *replayEntry
}

func (r *replayer) updateaddrs(groupId uint32, version int, lsn uint64) {
	m, ok := r.addrs[groupId]
	if !ok {
		m = make(map[int]*common.ClosedIntervals)
	}
	interval, ok := m[version]
	if !ok {
		interval = common.NewClosedIntervals()
	}
	interval.TryMerge(*common.NewClosedIntervalsByInt(lsn))
	m[version] = interval
	r.addrs[groupId] = m
}
func (r *replayer) updateGroupLSN(groupId uint32, lsn uint64) {
	curr := r.groupLSN[groupId]
	if lsn > curr {
		r.groupLSN[groupId] = lsn
	}
}

func newReplayer(h ApplyHandle) *replayer {
	return &replayer{
		uncommit:        make(map[uint32]map[uint64][]*replayEntry),
		entrys:          make([]*replayEntry, 0),
		checkpointrange: make(map[uint32]*checkpointInfo),
		checkpoints:     make([]*replayEntry, 0),
		mergeFuncs:      make(map[uint32]func(pre []byte, curr []byte) []byte),
		applyEntry:      h,
		addrs:           make(map[uint32]map[int]*common.ClosedIntervals),
		groupLSN:        make(map[uint32]uint64),
		tidlsnMap:       make(map[uint32]map[uint64]uint64),
	}
}

func (r *replayer) Apply() {
	for _, e := range r.checkpoints {
		r.applyEntry(e.group, e.commitId, e.payload, e.entryType, e.info)
	}

	sort.Slice(r.entrys, func(i, j int) (ret bool) {
		// defer logutil.Infof("swap %d, %d less %v", i, j, ret)
		if r.entrys[i].group != r.entrys[j].group {
			ret = true
			return
		}
		return r.entrys[i].commitId < r.entrys[j].commitId
	})

	for _, e := range r.entrys {
		// logutil.Infof("offset %d, glsn %d-%d", i, e.group, e.commitId)
		ckpinfo, ok := r.checkpointrange[e.group]
		if ok {
			if ckpinfo.ranges.ContainsInterval(
				common.ClosedInterval{Start: e.commitId, End: e.commitId}) {
				continue
			}
		}
		r.applyEntry(e.group, e.commitId, e.payload, e.entryType, nil)
	}
}

type replayEntry struct {
	entryType uint16
	group     uint32
	commitId  uint64
	payload   []byte
	info      any
}

func (r *replayEntry) String() string {
	return fmt.Sprintf("%v\n", r.info)
}
func (r *replayer) onReplayEntry(e entry.Entry, vf ReplayObserver, addrInMeta bool) error {
	v := e.GetInfo()
	if v == nil {
		return nil
	}
	info := v.(*entry.Info)
	if addrInMeta {
		r.version = vf.(*vFile).Id()
	}
	r.updateaddrs(info.Group, r.version, info.GroupLSN)
	if e.GetType() == entry.ETFlush {
		return nil
	}
	r.updateGroupLSN(info.Group, info.GroupLSN)
	if !addrInMeta {
		info.Info = &VFileAddress{
			Group:   info.Group,
			LSN:     info.GroupLSN,
			Version: r.version,
			Offset:  r.state.pos,
		}
	}
	switch info.Group {
	case entry.GTCKp:
		replayEty := &replayEntry{
			entryType: e.GetType(),
			payload:   make([]byte, e.GetPayloadSize()),
			info:      info,
		}
		copy(replayEty.payload, e.GetPayload())
		r.checkpoints = append(r.checkpoints, replayEty)

		for _, ckp := range info.Checkpoints {
			ckpInfo, ok := r.checkpointrange[ckp.Group]
			if !ok {
				ckpInfo = newCheckpointInfo()
				r.checkpointrange[ckp.Group] = ckpInfo
			}
			if ckp.Ranges != nil && len(ckp.Ranges.Intervals) > 0 {
				ckpInfo.UpdateWtihRanges(ckp.Ranges)
			}
			if ckp.Command != nil {
				ckpInfo.MergeCommandMap(ckp.Command)
			}
		}
	case entry.GTUncommit:
		for _, tinfo := range info.Uncommits {
			tidMap, ok := r.uncommit[tinfo.Group]
			if !ok {
				tidMap = make(map[uint64][]*replayEntry)
			}
			entries, ok := tidMap[info.TxnId]
			if !ok {
				entries = make([]*replayEntry, 0)
			}
			replayEty := &replayEntry{
				payload: make([]byte, e.GetPayloadSize()),
			}
			copy(replayEty.payload, e.GetPayload())
			entries = append(entries, replayEty)
			tidMap[info.TxnId] = entries
			r.uncommit[tinfo.Group] = tidMap
		}
	case entry.GTInternal:
		if info.PostCommitVersion >= r.ckpVersion {
			r.ckpVersion = info.PostCommitVersion
			replayEty := &replayEntry{
				payload: make([]byte, e.GetPayloadSize()),
			}
			copy(replayEty.payload, e.GetPayload())
			r.ckpEntry = replayEty
		}
	default:
		replayEty := &replayEntry{
			entryType: e.GetType(),
			group:     info.Group,
			commitId:  info.GroupLSN,
			payload:   make([]byte, e.GetPayloadSize()),
		}
		tidlsnMap, ok := r.tidlsnMap[info.Group]
		if !ok {
			tidlsnMap = make(map[uint64]uint64)
			r.tidlsnMap[info.Group] = tidlsnMap
		}
		tidlsnMap[info.TxnId] = info.GroupLSN
		copy(replayEty.payload, e.GetPayload())
		r.entrys = append(r.entrys, replayEty)
	}
	if addrInMeta && info.Group == entry.GTUncommit {
		return nil
	}
	vf.OnLogInfo(info)
	return nil
}

func (r *replayer) replayHandler(v VFile, o ReplayObserver) error {
	vfile := v.(*vFile)
	if vfile.version != r.version {
		r.state.pos = 0
		r.version = vfile.version
	}
	current := vfile.GetState()
	e := entry.GetBase()
	defer e.Free()

	metaBuf := e.GetMetaBuf()
	_, err := vfile.Read(metaBuf)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return err
		}
		err2 := vfile.Truncate(int64(r.state.pos))
		if err2 != nil {
			panic(err2)
		}
		return err
	}
	if e.GetType() == entry.ETMeta {
		return io.EOF
	}

	n, err := e.ReadFrom(vfile)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return err
		}
		panic("wrong wal")
		// err2 := vfile.Truncate(int64(r.state.pos))
		// if err2 != nil {
		// 	panic(err2)
		// }
		// return err
	}
	if int(n) != e.TotalSizeExpectMeta() {
		if current.pos == r.state.pos+int(n) {
			panic("wrong wal")
			// err2 := vfile.Truncate(int64(current.pos))
			// if err2 != nil {
			// 	logutil.Infof("lalala")
			// 	return err
			// }
			// return io.EOF
		} else {
			return fmt.Errorf("payload mismatch: %d != %d", n, e.GetPayloadSize())
		}
	}
	if err = r.onReplayEntry(e, v.(*vFile), false); err != nil {
		return err
	}
	r.state.pos += e.TotalSize()
	return nil
}

func (r *replayer) MergeCkps(vfiles []VFile) {
	for _, vf := range vfiles {
		addrs, addrmu := vf.GetAddrs()
		addrmu.RLock()
		for group, lsnOffset := range addrs {
			verLsn, ok := r.addrs[group]
			if !ok {
				verLsn = make(map[int]*common.ClosedIntervals)
				r.addrs[group] = verLsn
			}
			lsn, ok := verLsn[vf.Id()]
			if !ok {
				lsn = common.NewClosedIntervals()
				verLsn[vf.Id()] = lsn
			}
			for vinfoLsn := range lsnOffset {
				lsn.TryMerge(*common.NewClosedIntervalsByInt(vinfoLsn))
			}
		}
		for lsn, offset := range addrs[entry.GTInternal] {
			e, err := vf.LoadByOffset(offset)
			r.updateGroupLSN(entry.GTInternal, lsn)
			if err != nil {
				panic(err)
			}
			err = r.onReplayEntry(e, vf.(*vFile), true)
			e.Free()
			if err != nil {
				panic(err)
			}
		}
		for lsn, offset := range addrs[entry.GTCKp] {
			e, err := vf.LoadByOffset(offset)
			r.updateGroupLSN(entry.GTCKp, lsn)
			if err != nil {
				panic(err)
			}
			err = r.onReplayEntry(e, vf.(*vFile), true)
			e.Free()
			if err != nil {
				panic(err)
			}
		}
		addrmu.RUnlock()
	}
	if r.ckpEntry != nil {
		cmd := newEmptyInternalCmd()
		err := cmd.Unarshal(r.ckpEntry.payload)
		if err != nil {
			panic(err)
		}
		for groupId, ckps := range cmd.checkpointing {
			ckpInfo, ok := r.checkpointrange[groupId]
			if !ok {
				r.checkpointrange[groupId] = ckps
			} else {
				ckpInfo.MergeCheckpointInfo(ckps)
			}
		}
	}
}

func (r *replayer) replayHandlerWithCkpForCommitGroups(vf VFile, o ReplayObserver) error {
	addrMap, addrmu := vf.GetAddrs()
	addrmu.RLock()
	for gid, addrs := range addrMap {
		if gid == entry.GTUncommit {
			continue
		}
		for lsn, offset := range addrs {
			r.updateGroupLSN(gid, lsn)
			if IsCheckpointed(gid, lsn, r.checkpointrange) {
				continue
			}
			e, err := vf.LoadByOffset(offset)
			if err != nil {
				panic(err)
			}
			err = r.onReplayEntry(e, vf.(*vFile), true)
			e.Free()
			if err != nil {
				panic(err)
			}
		}
	}
	addrmu.RUnlock()
	return nil
}

func (r *replayer) replayHandlerWithCkpForUCGroups(vf VFile, o ReplayObserver) error {
	addrMap, addrmu := vf.GetAddrs()
	addrmu.RLock()
	defer addrmu.RUnlock()
	gid := entry.GTUncommit
	addrs, ok := addrMap[gid]
	if !ok {
		return nil
	}
	for lsn, offset := range addrs {
		r.updateGroupLSN(gid, lsn)
		gidtid := vf.GetUncommitGidTid(lsn)
		tidLsn, ok := r.tidlsnMap[gidtid.Group]
		if ok {
			commitLsn, ok := tidLsn[gidtid.Tid]
			if ok {
				if IsCheckpointed(gidtid.Group, commitLsn, r.checkpointrange) {
					continue
				}
			}
		}
		e, err := vf.LoadByOffset(offset)
		if err != nil {
			panic(err)
		}
		err = r.onReplayEntry(e, vf.(*vFile), true)
		e.Free()
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func IsCheckpointed(gid uint32, lsn uint64, ckps map[uint32]*checkpointInfo) bool {
	if gid == entry.GTCKp {
		return true
	}
	ckp, ok := ckps[gid]
	if !ok {
		return false
	}
	return ckp.ranges.ContainsInterval(common.ClosedInterval{Start: lsn, End: lsn})
}
