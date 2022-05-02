package store

import (
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"io"
)

type noopObserver struct {
}

func (o *noopObserver) OnNewEntry(_ int) {
}

func (o *noopObserver) OnNewCommit(*entry.Info)             {}
func (o *noopObserver) OnNewCheckpoint(*entry.Info)         {}
func (o *noopObserver) OnNewTxn(*entry.Info)                {}
func (o *noopObserver) OnNewUncommit(addrs []*VFileAddress) {}

type replayer struct {
	version         int
	state           vFileState
	uncommit        map[uint32]map[uint64][]*replayEntry
	entrys          []*replayEntry
	checkpointrange map[uint32]*common.ClosedIntervals
	checkpoints     []*replayEntry
	mergeFuncs      map[uint32]func(pre, curr []byte) []byte
	applyEntry      ApplyHandle

	//syncbase
	addrs    map[uint32]map[int]common.ClosedInterval
	groupLSN map[uint32]uint64

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
	if groupId==entry.GTNoop{
	}
	m, ok := r.addrs[groupId]
	if !ok {
		m = make(map[int]common.ClosedInterval)
	}
	interval, ok := m[version]
	if !ok {
		interval = common.ClosedInterval{}
	}
	interval.TryMerge(common.ClosedInterval{Start: lsn, End: lsn})
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
		checkpointrange: make(map[uint32]*common.ClosedIntervals),
		checkpoints:     make([]*replayEntry, 0),
		mergeFuncs:      make(map[uint32]func(pre []byte, curr []byte) []byte),
		applyEntry:      h,
		addrs:           make(map[uint32]map[int]common.ClosedInterval),
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
		interval, ok := r.checkpointrange[e.group]
		if ok {
			if interval.ContainsInterval(
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
						// pre = r.mergeUncommittedEntries(
						// 	pre, entry)
					}
				}
			}
			// e = r.mergeUncommittedEntries(pre, e)
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
	info    interface{}
}

func (r *replayEntry) String() string {
	return fmt.Sprintf("%v\n", r.info)
}
func (r *replayer) onReplayEntry(e entry.Entry, vf ReplayObserver) error {
	typ := e.GetType()
	switch typ {
	case entry.ETFlush:
		infobuf := e.GetInfoBuf()
		info := entry.Unmarshal(infobuf)
		r.updateVinfoAddrs(info.Group,info.GroupLSN,r.state.pos)
		r.updateaddrs(info.Group, r.version, info.GroupLSN)
		return nil
	case entry.ETCheckpoint:
		// fmt.Printf("ETCheckpoint\n")
		infobuf := e.GetInfoBuf()
		info := entry.Unmarshal(infobuf)
		r.updateVinfoAddrs(info.Group,info.GroupLSN,r.state.pos)
		r.updateaddrs(info.Group, r.version, info.GroupLSN)
		r.updateGroupLSN(info.Group, info.GroupLSN)
		info.Info = &VFileAddress{
			Group:   info.Group,
			LSN:     info.GroupLSN,
			Version: r.version,
			Offset:  r.state.pos,
		}
		replayEty := &replayEntry{
			entryType: typ,
			payload:   make([]byte, e.GetPayloadSize()),
			info:      info,
		}
		copy(replayEty.payload, e.GetPayload())
		r.checkpoints = append(r.checkpoints, replayEty)

		for _, ckp := range info.Checkpoints {
			interval, ok := r.checkpointrange[ckp.Group]
			if !ok {
				//TODO: all the ranges
				interval = common.NewClosedIntervalsByIntervals(ckp.Ranges)
			} else {
				interval.TryMerge(*ckp.Ranges)
			}
			r.checkpointrange[ckp.Group] = interval
		}
		vf.OnNewCheckpoint(info)
	case entry.ETUncommitted:
		// fmt.Printf("ETUncommitted\n")
		infobuf := e.GetInfoBuf()
		info := entry.Unmarshal(infobuf)
		r.updateVinfoAddrs(info.Group,info.GroupLSN,r.state.pos)
		r.updateaddrs(info.Group, r.version, info.GroupLSN)
		r.updateGroupLSN(info.Group, info.GroupLSN)
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
	case entry.ETTxn:
		// fmt.Printf("ETTxn\n")
		infobuf := e.GetInfoBuf()
		info := entry.Unmarshal(infobuf)
		r.updateVinfoAddrs(info.Group,info.GroupLSN,r.state.pos)
		r.updateaddrs(info.Group, r.version, info.GroupLSN)
		r.updateGroupLSN(info.Group, info.GroupLSN)
		info.Info = &VFileAddress{
			Group:   info.Group,
			LSN:     info.GroupLSN,
			Version: r.version,
			Offset:  r.state.pos,
		}
		replayEty := &replayEntry{
			entryType: e.GetType(),
			group:     info.Group,
			commitId:  info.GroupLSN,
			tid:       info.TxnId,
			payload:   make([]byte, e.GetPayloadSize()),
		}
		copy(replayEty.payload, e.GetPayload())
		r.entrys = append(r.entrys, replayEty)
		vf.OnNewTxn(info)
	default:
		// fmt.Printf("default\n")
		infobuf := e.GetInfoBuf()
		info := entry.Unmarshal(infobuf)
		r.updateVinfoAddrs(info.Group,info.GroupLSN,r.state.pos)
		r.updateaddrs(info.Group, r.version, info.GroupLSN)
		r.updateGroupLSN(info.Group, info.GroupLSN)
		info.Info = &VFileAddress{
			Group:   info.Group,
			LSN:     info.GroupLSN,
			Version: r.version,
			Offset:  r.state.pos,
		}
		replayEty := &replayEntry{
			entryType: e.GetType(),
			group:     info.Group,
			commitId:  info.GroupLSN,
			payload:   make([]byte, e.GetPayloadSize()),
		}
		copy(replayEty.payload, e.GetPayload())
		r.entrys = append(r.entrys, replayEty)
		vf.OnNewCommit(info)
	}
	return nil
}

func (r *replayer) replayHandler(v VFile, o ReplayObserver) error {
	vfile := v.(*vFile)
	if vfile.version != r.version {
		r.state.pos = 0
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
		vfile.Truncate(int64(r.state.pos))
		return err
	}

	n, err := entry.ReadFrom(vfile)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return err
		}
		vfile.Truncate(int64(r.state.pos))
		return err
	}
	if n != entry.TotalSizeExpectMeta() {
		if current.pos == r.state.pos+n {
			vfile.Truncate(int64(current.pos))
			return io.EOF
		} else {
			return errors.New(fmt.Sprintf(
				"payload mismatch: %d != %d", n, entry.GetPayloadSize()))
		}
	}
	if err = r.onReplayEntry(entry, o); err != nil {
		return err
	}
	r.state.pos += entry.TotalSize()
	return nil
}
