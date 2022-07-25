package wal

import (
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	driverEntry "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

func (w *WalImpl) FuzzyCheckpoint(gid uint32, indexes []*wal.Index) (ckpEntry entry.Entry) {
	ckpEntry = w.makeCheckpointEntry(gid, indexes)
	drentry, _, _ := w.doAppend(GroupCKP, ckpEntry)
	w.checkpointQueue.Enqueue(drentry)
	return
}

func (w *WalImpl) makeCheckpointEntry(gid uint32, indexes []*wal.Index) (ckpEntry entry.Entry) {
	for _, index := range indexes {
		if index.LSN > 100000000 {
			logutil.Infof("IndexErr: Checkpoint Index: %s", index.String())
		}
	}
	defer func() {
		for _, index := range indexes {
			if index.LSN > 100000000 {
				logutil.Infof("IndexErr: Checkpoint Index: %s", index.String())
			}
		}
	}()
	commands := make(map[uint64]entry.CommandInfo)
	for _, idx := range indexes {
		cmdInfo, ok := commands[idx.LSN]
		if !ok {
			cmdInfo = entry.CommandInfo{
				CommandIds: []uint32{idx.CSN},
				Size:       idx.Size,
			}
		} else {
			existed := false
			for _, csn := range cmdInfo.CommandIds {
				if csn == idx.CSN {
					existed = true
					break
				}
			}
			if existed {
				continue
			}
			cmdInfo.CommandIds = append(cmdInfo.CommandIds, idx.CSN)
			if cmdInfo.Size != idx.Size {
				panic("logic error")
			}
		}
		commands[idx.LSN] = cmdInfo
	}
	info := &entry.Info{
		Group: entry.GTCKp,
		Checkpoints: []*entry.CkpRanges{{
			Group:   gid,
			Command: commands,
		}},
	}
	ckpEntry = entry.GetBase()
	ckpEntry.SetType(entry.ETCheckpoint)
	ckpEntry.SetInfo(info)
	return
}

func (w *WalImpl) onLogCKPInfoQueue(items ...any) {
	for _, item := range items {
		e := item.(*driverEntry.Entry)
		e.WaitDone()
		w.logCheckpointInfo(e.Info)
	}
	w.onCheckpoint()
}

func (w *WalImpl) onCheckpoint() {
	w.WalInfo.onCheckpoint()
	w.truncatingQueue.Enqueue(struct{}{})
}

func (w *WalImpl) CkpCkp() {
	e := w.makeInternalCheckpointEntry()
	_, err := w.Append(GroupInternal, e)
	if err != nil {
		panic(err)
	}
	e.WaitDone()
}

//tid-lsn-ckped uclsn-tid,tid-clsn,cckped
func (w *WalImpl) CkpUC() {
	ckpedlsn := w.GetCheckpointed(GroupC)
	ucLsn := w.GetCheckpointed(GroupUC)
	maxLsn := w.GetSynced(GroupUC)
	ckpedUC := ucLsn
	for i := ucLsn + 1; i <= maxLsn; i++ {
		tid, ok := w.ucLsnTidMap[i]
		if !ok {
			panic("logic error")
		}
		lsn, ok := w.cTidLsnMap[tid]
		if !ok {
			break
		}
		if lsn > ckpedlsn {
			break
		}
		ckpedUC = i
	}
	w.SetCheckpointed(GroupUC, ckpedUC)
}

func (w *WalImpl) onTruncatingQueue(items ...any) {
	gid, driverLsn := w.getDriverCheckpointed()
	if driverLsn == 0 && gid == 0 {
		return
	}
	if gid == GroupUC {
		w.CkpUC()
	}
	if gid == GroupCKP {
		w.CkpCkp()
		_, driverLsn = w.getDriverCheckpointed()
	}
	atomic.StoreUint64(&w.driverCheckpointing, driverLsn)
	w.truncateQueue.Enqueue(struct{}{})
}

func (w *WalImpl) onTruncateQueue(items ...any) {
	lsn := atomic.LoadUint64(&w.driverCheckpointing)
	if lsn != w.driverCheckpointed {
		err := w.driver.Truncate(lsn)
		for err != nil {
			lsn = atomic.LoadUint64(&w.driverCheckpointing)
			err = w.driver.Truncate(lsn)
		}
		w.driverCheckpointed = lsn
	}
}
