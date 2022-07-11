package wal

import (
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

func (w *WalImpl) Checkpoint(indexes []*wal.Index) (ckpEntry entry.Entry) {
	ckpEntry = w.makeCheckpointEntry(indexes)
	w.Append(GroupCKP, ckpEntry)
	return
}

func (w *WalImpl) makeCheckpointEntry(indexes []*wal.Index) (ckpEntry entry.Entry) {
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
		Checkpoints: []entry.CkpRanges{{
			Group:   GroupC,
			Command: commands,
		}},
	}
	ckpEntry = entry.GetBase()
	ckpEntry.SetType(entry.ETCheckpoint)
	ckpEntry.SetInfo(info)
	return
}

func (w *WalImpl) onLogCKPInfoQueue(items []any) any {
	for _, item := range items {
		e := item.(entry.Entry)
		info := e.GetInfo().(*entry.Info)
		e.WaitDone()
		e.Free()
		w.logCheckpointInfo(info)
	}
	w.onCheckpoint()
	return nil
}

func (w *WalImpl) onCheckpoint() {
	w.WalInfo.onCheckpoint()
	w.truncateQueue <- struct{}{}
}

func (w *WalImpl) CkpCkp() {
	e := w.makeInternalCheckpointEntry()
	_, err := w.Append(GroupInternal, e)
	if err != nil {
		panic(err)
	}
	e.WaitDone()
}

func (w *WalImpl) Truncate(driverLsn uint64) {
	w.driver.Truncate(driverLsn)
}

func (w *WalImpl) onTruncatingQueue(items []any) any {
	gid, driverLsn := w.getDriverCheckpointed()
	if gid == GroupCKP {
		w.CkpCkp()
		_, driverLsn = w.getDriverCheckpointed()
	}
	atomic.StoreUint64(&w.driverCheckpointing, driverLsn)
	return struct{}{}
}

func (w *WalImpl) onTruncateQueue(items []any) any {
	lsn := atomic.LoadUint64(&w.driverCheckpointing)
	if lsn != w.driverCheckpointed {
		err := w.driver.Truncate(lsn)
		for err != nil {
			lsn = atomic.LoadUint64(&w.driverCheckpointing)
			err = w.driver.Truncate(lsn)
		}
		w.driverCheckpointed = lsn
	}
	return struct{}{}
}
