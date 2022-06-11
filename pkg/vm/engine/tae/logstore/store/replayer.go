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
	addrs      map[uint32]map[int]common.ClosedIntervals
	groupLSN   map[uint32]uint64
	ckpVersion int
	ckpEntry   *replayEntry

	//vinfo
	// Commits     map[uint32]*common.ClosedInterval
	// Checkpoints map[uint32]*common.ClosedIntervals
	vinfoAddrs map[uint32]map[uint64]int
}

func (r *replayer) updateVinfoAddrs(groupId uint32, lsn uint64, offset int) {
	m, ok := r.vinfoAddrs[groupId]
	if !ok {
		m = make(map[uint64]int)
	}
	m[lsn] = offset
	r.vinfoAddrs[groupId] = m
}
func (r *replayer) updateaddrs(groupId uint32, version int, lsn uint64) {
	if groupId == entry.GTNoop {
	}
	m, ok := r.addrs[groupId]
	if !ok {
		m = make(map[int]common.ClosedIntervals)
	}
	interval, ok := m[version]
	if !ok {
		interval = *common.NewClosedIntervals()
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
		addrs:           make(map[uint32]map[int]common.ClosedIntervals),
		groupLSN:        make(map[uint32]uint64),
		vinfoAddrs:      make(map[uint32]map[uint64]int),
	}
}

func defaultMergePayload(pre, curr []byte) []byte {
	return append(pre, curr...)
}
func (r *replayer) mergeUncommittedEntries(pre, curr *replayEntry) *replayEntry {
	if pre == nil {
		return curr
	}
	mergePayload, ok := r.mergeFuncs[curr.group]
	if !ok {
		mergePayload = defaultMergePayload
	}
	curr.payload = mergePayload(pre.payload, curr.payload)
	return curr
}

func (r *replayer) Apply() {
	for _, e := range r.checkpoints {
		r.applyEntry(e.group, e.commitId, e.payload, e.entryType, e.info)
	}

	for _, e := range r.entrys {
		ckpinfo, ok := r.checkpointrange[e.group]
		if ok {
			if ckpinfo.ranges.ContainsInterval(
				common.ClosedInterval{Start: e.commitId, End: e.commitId}) {
				continue
			}
		}
		if e.entryType == entry.ETTxn {
			// var pre *replayEntry
			tidMap, ok := r.uncommit[e.group]
			if ok {
				entries, ok := tidMap[e.tid]
				if ok {
					for _, entry := range entries {
						r.applyEntry(entry.group, entry.commitId, entry.payload, entry.entryType, nil)
					}
				}
			}
			r.applyEntry(e.group, e.commitId, e.payload, e.entryType, nil)
		} else {
			r.applyEntry(e.group, e.commitId, e.payload, e.entryType, nil)
		}
	}
}

type replayEntry struct {
	entryType uint16
	group     uint32
	commitId  uint64
	// isTxn     bool
	tid uint64
	// checkpointRange *common.ClosedInterval
	payload []byte
	info    any
}

func (r *replayEntry) String() string {
	return fmt.Sprintf("%v\n", r.info)
}
func (r *replayer) onReplayEntry(e entry.Entry, vf ReplayObserver) error {
	infobuf := e.GetInfoBuf()
	info := entry.Unmarshal(infobuf)
	r.updateVinfoAddrs(info.Group, info.GroupLSN, r.state.pos)
	r.updateaddrs(info.Group, r.version, info.GroupLSN)
	if e.GetType() == entry.ETFlush {
		return nil
	}
	r.updateGroupLSN(info.Group, info.GroupLSN)
	info.Info = &VFileAddress{
		Group:   info.Group,
		LSN:     info.GroupLSN,
		Version: r.version,
		Offset:  r.state.pos,
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
				for lsn, cmds := range ckp.Command {
					ckpInfo.UpdateWithCommandInfo(lsn, &cmds)
				}
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
				// info:    addr,
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
		copy(replayEty.payload, e.GetPayload())
		r.entrys = append(r.entrys, replayEty)
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
	entry := entry.GetBase()
	defer entry.Free()

	metaBuf := entry.GetMetaBuf()
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

	n, err := entry.ReadFrom(vfile)
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
	if int(n) != entry.TotalSizeExpectMeta() {
		if current.pos == r.state.pos+int(n) {
			panic("wrong wal")
			// err2 := vfile.Truncate(int64(current.pos))
			// if err2 != nil {
			// 	logutil.Infof("lalala")
			// 	return err
			// }
			// return io.EOF
		} else {
			return fmt.Errorf("payload mismatch: %d != %d", n, entry.GetPayloadSize())
		}
	}
	if err = r.onReplayEntry(entry, o); err != nil {
		return err
	}
	r.state.pos += entry.TotalSize()
	return nil
}
