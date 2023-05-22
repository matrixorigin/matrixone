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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	driverEntry "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

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
	ckpEntry.SetType(entry.IOET_WALEntry_Checkpoint)
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
	w.ckpCkp()
}

func (w *StoreImpl) ckpCkp() {
	e := w.makeInternalCheckpointEntry()
	driverEntry, _, err := w.doAppend(GroupInternal, e)
	if err == common.ErrClose {
		return
	}
	if err != nil {
		panic(err)
	}
	w.truncatingQueue.Enqueue(driverEntry)
	err = e.WaitDone()
	if err != nil {
		panic(err)
	}
	e.Free()
}

func (w *StoreImpl) onTruncatingQueue(items ...any) {
	for _, item := range items {
		e := item.(*driverEntry.Entry)
		err := e.WaitDone()
		if err != nil {
			panic(err)
		}
		w.logCheckpointInfo(e.Info)
	}
	gid, driverLsn := w.getDriverCheckpointed()
	if gid == 0 {
		return
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
