package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

var (
	ErrGroupNotFount = errors.New("group not found")
	ErrLsnNotFount   = errors.New("lsn not found")
)

type WalInfo struct {
	checkpointInfo      map[uint32]*checkpointInfo //gid-ckp
	ckpMu               sync.RWMutex
	walDriverLsnMap     map[uint32]map[uint64]uint64 //gid-walLsn-driverLsn
	lsnMu               sync.RWMutex
	walLsnTidMap        map[uint64]uint64 //tid-lsn
	driverCheckpointing uint64
	driverCheckpointed  uint64
	walCurrentLsn       map[uint32]uint64
	lsnmu               sync.RWMutex
	syncing             map[uint32]uint64

	checkpointed   map[uint32]uint64
	checkpointedMu sync.RWMutex
	synced         map[uint32]uint64
	syncedMu       sync.RWMutex
	ckpcnt         map[uint32]uint64
	ckpcntMu       sync.RWMutex
}

func (w *WalInfo) GetCurrSeqNum(gid uint32) (lsn uint64)
func (w *WalInfo) GetSynced(gid uint32) (lsn uint64)
func (w *WalInfo) GetPendding(gid uint32) (cnt uint64)
func (w *WalInfo) GetCheckpointed(gid uint32) (lsn uint64)

func (w *WalInfo) allocateLsn(gid uint32) uint64 {
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

func (w *WalInfo) logDriverLsn(driverEntry *driver.Entry) {
	info := driverEntry.Entry.GetInfo().(*entry.Info)
	if info.Group == GroupCKP {
		w.logCheckpointInfo(info)
	}

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

	if info.Group == GroupC {
		w.walLsnTidMap[info.TxnId] = info.GroupLSN
	}
}

func (w *WalInfo) onAppend() {
	w.syncedMu.Lock()
	for gid, lsn := range w.syncing {
		w.synced[gid] = lsn
	}
	w.syncedMu.Unlock()
}

func (w *WalInfo) getDriverLsn(gid uint32, lsn uint64) (driverLsn uint64, err error) {
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

func (w *WalInfo) logCheckpointInfo(info *entry.Info) any {
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

func (w *WalInfo) onCheckpoint() {
	w.checkpointedMu.Lock()
	for gid, ckp := range w.checkpointInfo {
		ckped := ckp.GetCheckpointed()
		w.checkpointed[gid] = ckped
	}
	w.checkpointedMu.Unlock()
	w.ckpcntMu.Lock()
	for gid, ckp := range w.checkpointInfo {
		w.ckpcnt[gid] = ckp.GetCkpCnt()
	}
	w.ckpcntMu.Unlock()
}

func (w *WalInfo) getDriverCheckpointed() (gid uint32, driverLsn uint64) {
	driverLsn = math.MaxInt64
	w.checkpointedMu.RLock()
	for g, lsn := range w.checkpointed {
		drLsn, err := w.getDriverLsn(g, lsn)
		if err != nil {
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

func (w *WalInfo) makeInternalCheckpointEntry() (e entry.Entry) {
	e = entry.GetBase()
	lsn := w.GetSynced(GroupCKP)
	e.SetType(entry.ETPostCommit)
	buf, err := w.marshalPostCommitEntry()
	if err != nil {
		panic(err)
	}
	err = e.Unmarshal(buf)
	if err != nil {
		panic(err)
	}
	info := &entry.Info{}
	info.TargetLsn = lsn
	info.Group = GroupInternal
	e.SetInfo(info)
	return
}

func (w *WalInfo) marshalPostCommitEntry() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = w.writePostCommitEntry(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}

func (w *WalInfo) writePostCommitEntry(writer io.Writer) (n int64, err error) {
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
