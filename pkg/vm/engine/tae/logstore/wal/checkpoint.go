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

package wal

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	driverEntry "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
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

// user should guarantee that the checkpoint is valid
// it is not safe to call this function concurrently
func (w *StoreImpl) RangeCheckpoint(
	start, end uint64, files ...string,
) (ckpEntry entry.Entry, err error) {
	var (
		gid     = GroupUserTxn
		drentry *driverEntry.Entry
	)
	defer func() {
		var (
			logger = logutil.Info
		)
		if err != nil {
			logger = logutil.Error
			ckpEntry = nil
		}
		logger(
			"TRACE-WAL-TRUNCATE-Send-Intent",
			zap.Uint64("lsn-intent", end),
			zap.Error(err),
		)
	}()

	// TODO: it is too bad!!!
	// we should not split the checkpoint entry into two entries!!!
	if end < w.watermark.lsnCheckpointed.Load() {
		err = ErrStaleCheckpointIntent
		return
	}
	ckpEntry = w.makeRangeCheckpointEntry(gid, start, end)
	if drentry, _, err = w.doAppend(GroupCKP, ckpEntry); err != nil {
		return
	}
	if len(files) > 0 {
		var fileEntry entry.Entry
		if fileEntry, err = BuildFilesEntry(files); err != nil {
			return
		}
		// TODO: too bad!!!
		if _, _, err = w.doAppend(GroupFiles, fileEntry); err != nil {
			return
		}
	}
	_, err = w.checkpointQueue.Enqueue(drentry)
	return
}

func (w *StoreImpl) makeRangeCheckpointEntry(
	gid uint32, start, end uint64,
) (ckpEntry entry.Entry) {
	// TODO: this entry Info is too bad!!!
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

func (w *StoreImpl) onCheckpointIntent(intents ...any) {
	for _, intent := range intents {
		e := intent.(*driverEntry.Entry)
		err := e.WaitDone()
		logger := logutil.Info
		if err != nil {
			logger = logutil.Error
		}
		logger(
			"TRACE-WAL-TRUNCATE-Intent-Committed",
			zap.Uint64("lsn", e.Info.Checkpoints[0].GetMax()),
			zap.Error(err),
		)
		if err == nil {
			w.updateLSNCheckpointed(e.Info)
		}
	}
	w.updateDSNCheckpointed()
}

func (w *StoreImpl) updateDSNCheckpointed() {
	start := time.Now()
	dsn, found := w.getCheckpointedDSNIntent()
	logutil.Info(
		"TRACE-WAL-TRUNCATE",
		zap.Duration("calculate-dsn-cost", time.Since(start)),
		zap.Uint64("dsn", dsn),
		zap.Bool("found", found),
	)
	if !found {
		return
	}
	if dsn > w.watermark.dsnCheckpointed.Load() {
		for i := 0; i < 10; i++ {
			if err := w.driver.Truncate(dsn); err != nil {
				logutil.Error(
					"TRACE-WAL-TRUNCATE-Error",
					zap.Uint64("dsn-intent", dsn),
					zap.Uint64("dsn-checkpointed", w.watermark.dsnCheckpointed.Load()),
					zap.Int("retry", i),
					zap.Error(err),
				)
				continue
			}
			start = time.Now()
			w.gcDSNMapping(dsn)
			prev := w.watermark.dsnCheckpointed.Swap(dsn)
			logutil.Info(
				"TRACE-WAL-TRUNCATE-GC-Store",
				zap.Duration("duration", time.Since(start)),
				zap.Uint64("dsn", dsn),
				zap.Uint64("dsn-prev", prev),
			)
			break
		}
	}
}
