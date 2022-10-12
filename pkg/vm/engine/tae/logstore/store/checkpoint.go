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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	driverEntry "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

func (w *StoreImpl) FuzzyCheckpoint(gid uint32, indexes []*Index) (ckpEntry entry.Entry, err error) {
	ckpEntry = w.makeFuzzyCheckpointEntry(gid, indexes)
	drentry, _, _ := w.doAppend(GroupCKP, ckpEntry)
	if drentry == nil {
		panic(err)
	}
	_, err = w.checkpointQueue.Enqueue(drentry)
	if err != nil {
		panic(err)
	}
	return
}

func (w *StoreImpl) makeFuzzyCheckpointEntry(gid uint32, indexes []*Index) (ckpEntry entry.Entry) {
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

func (w *StoreImpl) RangeCheckpoint(gid uint32, start, end uint64) (ckpEntry entry.Entry, err error) {
	ckpEntry = w.makeRangeCheckpointEntry(gid, start, end)
	drentry, _, err := w.doAppend(GroupCKP, ckpEntry)
	if err == common.ErrClose {
		return nil, err
	}
	if err != nil {
		panic(err)
	}
	_, err = w.checkpointQueue.Enqueue(drentry)
	if err != nil {
		panic(err)
	}
	return
}

func (w *StoreImpl) makeRangeCheckpointEntry(gid uint32, start, end uint64) (ckpEntry entry.Entry) {
	info := &entry.Info{
		Group: entry.GTCKp,
		Checkpoints: []*entry.CkpRanges{{
			Group:  gid,
			Ranges: common.NewClosedIntervalsByInterval(&common.ClosedInterval{Start: start, End: end}),
		}},
	}
	ckpEntry = entry.GetBase()
	ckpEntry.SetType(entry.ETCheckpoint)
	ckpEntry.SetInfo(info)
	return
}

func (w *StoreImpl) onLogCKPInfoQueue(items ...any) {
	for _, item := range items {
		e := item.(*driverEntry.Entry)
		err := e.WaitDone()
		if err != nil {
			panic(err)
		}
		w.logCheckpointInfo(e.Info)
	}
	w.onCheckpoint()
}

func (w *StoreImpl) onCheckpoint() {
	w.StoreInfo.onCheckpoint()
	_, err := w.truncatingQueue.Enqueue(struct{}{})
	if err != nil {
		panic(err)
	}
}

func (w *StoreImpl) CkpCkp() {
	e := w.makeInternalCheckpointEntry()
	_, err := w.Append(GroupInternal, e)
	if err == common.ErrClose {
		return
	}
	if err != nil {
		panic(err)
	}
	err = e.WaitDone()
	if err != nil {
		panic(err)
	}
}

func (w *StoreImpl) onTruncatingQueue(items ...any) {
	gid, driverLsn := w.getDriverCheckpointed()
	if gid == 0 {
		return
	}
	if gid == GroupCKP || gid == GroupInternal {
		w.CkpCkp()
		gid, driverLsn = w.getDriverCheckpointed()
		if gid == 0 {
			panic("logic error")
		}
	}
	w.driverCheckpointing.Store(driverLsn)
	_, err := w.truncateQueue.Enqueue(struct{}{})
	if err != nil {
		panic(err)
	}
}

func (w *StoreImpl) onTruncateQueue(items ...any) {
	lsn := w.driverCheckpointing.Load()
	if lsn != w.driverCheckpointed {
		err := w.driver.Truncate(lsn)
		for err != nil {
			lsn = w.driverCheckpointing.Load()
			err = w.driver.Truncate(lsn)
		}
		w.driverCheckpointed = lsn
	}
}
