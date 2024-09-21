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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	driverEntry "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"go.uber.org/zap"
)

func BuildFilesEntry(files []string) (entry.Entry, error) {
	vec := containers.NewVector(types.T_char.ToType())
	for _, file := range files {
		vec.Append([]byte(file), false)
	}
	defer vec.Close()
	buf, err := vec.GetDownstreamVector().MarshalBinary()
	if err != nil {
		return nil, err
	}
	filesEntry := entry.GetBase()
	if err = filesEntry.SetPayload(buf); err != nil {
		return nil, err
	}
	info := &entry.Info{
		Group: GroupFiles,
	}
	filesEntry.SetType(entry.IOET_WALEntry_Checkpoint)
	filesEntry.SetInfo(info)
	return filesEntry, nil
}

func (w *StoreImpl) RangeCheckpoint(gid uint32, start, end uint64, files ...string) (ckpEntry entry.Entry, err error) {
	logutil.Info("TRACE-WAL-TRUNCATE-RangeCheckpoint", zap.Uint32("group", gid), zap.Uint64("lsn", end))
	ckpEntry = w.makeRangeCheckpointEntry(gid, start, end)
	drentry, _, err := w.doAppend(GroupCKP, ckpEntry)
	if err == sm.ErrClose {
		return nil, err
	}
	if err != nil {
		panic(err)
	}
	if len(files) > 0 {
		var fileEntry entry.Entry
		fileEntry, err = BuildFilesEntry(files)
		if err != nil {
			return
		}
		_, _, err = w.doAppend(GroupFiles, fileEntry)
		if err != nil {
			return
		}
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
		logutil.Info("TRACE-WAL-TRUNCATE-CKP-Entry",
			zap.Uint32("group", e.Info.Checkpoints[0].Group),
			zap.Uint64("lsn", e.Info.Checkpoints[0].Ranges.GetMax()))
		w.logCheckpointInfo(e.Info)
	}
	w.onCheckpoint()
}

func (w *StoreImpl) onCheckpoint() {
	w.StoreInfo.onCheckpoint()
	w.ckpCkp()
}

func (w *StoreImpl) ckpCkp() {
	t0 := time.Now()
	e := w.makeInternalCheckpointEntry()
	driverEntry, _, err := w.doAppend(GroupInternal, e)
	if err == sm.ErrClose {
		return
	}
	if err != nil {
		panic(err)
	}
	logutil.Info("TRACE-WAL-TRUNCATE-Internal-Entry",
		zap.String("duration", time.Since(t0).String()))
	w.truncatingQueue.Enqueue(driverEntry)
	err = e.WaitDone()
	if err != nil {
		panic(err)
	}
	e.Free()
}

func (w *StoreImpl) onTruncatingQueue(items ...any) {
	t0 := time.Now()
	for _, item := range items {
		e := item.(*driverEntry.Entry)
		err := e.WaitDone()
		if err != nil {
			panic(err)
		}
		w.logCheckpointInfo(e.Info)
	}
	tTruncateEntry := time.Since(t0)
	t0 = time.Now()
	gid, driverLsn := w.getDriverCheckpointed()
	tGetDriverEntry := time.Since(t0)
	logutil.Info("TRACE-WAL-TRUNCATE",
		zap.String("wait truncating entry takes", tTruncateEntry.String()),
		zap.String("get driver lsn takes", tGetDriverEntry.String()),
		zap.Uint64("driver lsn", driverLsn))
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
	if lsn != w.driverCheckpointed.Load() {
		err := w.driver.Truncate(lsn)
		for err != nil {
			lsn = w.driverCheckpointing.Load()
			err = w.driver.Truncate(lsn)
		}
		t := time.Now()
		w.gcWalDriverLsnMap(lsn)
		logutil.Info("TRACE-WAL-TRUNCATE-GC-Store", zap.String("duration", time.Since(t).String()))
		w.driverCheckpointed.Store(lsn)
	}
}
