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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
)

func (w *StoreImpl) Replay(h ApplyHandle) error {
	err := w.driver.Replay(func(e *entry.Entry) {
		err := w.replayEntry(e, h)
		if err != nil {
			panic(err)
		}
	})
	if err != nil {
		panic(err)
	}
	lsn, err := w.driver.GetTruncated()
	if err != nil {
		panic(err)
	}
	w.StoreInfo.onCheckpoint()
	w.driverCheckpointed = lsn
	w.driverCheckpointing = lsn
	for g, lsn := range w.syncing {
		w.walCurrentLsn[g] = lsn
		w.synced[g] = lsn
	}
	return nil
}

func (w *StoreImpl) replayEntry(e *entry.Entry, h ApplyHandle) error {
	walEntry := e.Entry
	info := e.Info
	switch info.Group {
	case GroupInternal:
	case GroupCKP:
		w.logCheckpointInfo(info)
	case GroupC:
	}
	w.logDriverLsn(e)
	h(info.Group, info.GroupLSN, walEntry.GetPayload(), walEntry.GetType(), walEntry.GetInfo())
	return nil
}
