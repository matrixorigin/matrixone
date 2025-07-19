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
	dsn, err := w.driver.GetTruncated()
	if err != nil {
		panic(err)
	}
	w.watermark.dsnCheckpointed.Store(dsn)

	if lsnCheckpointed := w.watermark.lsnCheckpointed.Load(); lsnCheckpointed > 0 {
		if w.watermark.allocatedLSN[GroupUserTxn] == 0 {
			w.watermark.allocatedLSN[GroupUserTxn] = lsnCheckpointed
		}
	}

	return nil
}

func (w *StoreImpl) replayEntry(e *entry.Entry, h ApplyHandle) (driver.ReplayEntryState, error) {
	walEntry := e.Entry
	info := e.Info
	switch info.Group {
	case GroupInternal:
		return driver.RE_Internal, nil
	case GroupCKP:
		w.updateLSNCheckpointed(info)
		// TODO:  should return?
	}
	state := h(info.Group, info.GroupLSN, walEntry.GetPayload(), walEntry.GetType(), walEntry.GetInfo())
	w.replayAllocatedLSN(info.Group, info.GroupLSN)
	w.logDSN(e)
	return state, nil
}
