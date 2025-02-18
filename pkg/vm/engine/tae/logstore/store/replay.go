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
	"context"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
)

func (w *StoreImpl) Replay(
	ctx context.Context, h ApplyHandle, modeGetter func() driver.ReplayMode, opt *driver.ReplayOption,
) error {
	err := w.driver.Replay(ctx, func(e *entry.Entry) driver.ReplayEntryState {
		state, err := w.replayEntry(e, h)
		if err != nil {
			panic(err)
		}
		return state
	}, modeGetter, opt)
	if err != nil {
		return err
	}
	lsn, err := w.driver.GetTruncated()
	if err != nil {
		panic(err)
	}
	w.StoreInfo.onCheckpoint()
	w.watermark.dsnCheckpointed.Store(lsn)
	for g, lsn := range w.syncing {
		w.watermark.nextLSN[g] = lsn
		w.synced[g] = lsn
	}
	for g, ckped := range w.checkpointed {
		if w.watermark.nextLSN[g] == 0 {
			w.watermark.nextLSN[g] = ckped
			w.synced[g] = ckped
		}
		if w.lsn2dsn.minLSN[g] <= w.watermark.dsnCheckpointed.Load() {
			minLSN := w.lsn2dsn.minLSN[g]
			for ; minLSN <= ckped+1; minLSN++ {
				dsn, err := w.getDriverLsn(g, minLSN)
				if err == nil && dsn > w.watermark.dsnCheckpointed.Load() {
					break
				}
			}
			w.lsn2dsn.minLSN[g] = minLSN
		}
	}
	return nil
}

func (w *StoreImpl) onReplayLsn(g uint32, lsn uint64) {
	_, ok := w.lsn2dsn.minLSN[g]
	if !ok {
		w.lsn2dsn.minLSN[g] = lsn
	}
}

func (w *StoreImpl) replayEntry(e *entry.Entry, h ApplyHandle) (driver.ReplayEntryState, error) {
	walEntry := e.Entry
	info := e.Info
	switch info.Group {
	case GroupInternal:
		return driver.RE_Internal, nil
	case GroupCKP:
		w.logCheckpointInfo(info)
		// TODO:  should return?
	}
	w.logDriverLsn(e)
	w.onReplayLsn(info.Group, info.GroupLSN)
	state := h(info.Group, info.GroupLSN, walEntry.GetPayload(), walEntry.GetType(), walEntry.GetInfo())
	return state, nil
}
