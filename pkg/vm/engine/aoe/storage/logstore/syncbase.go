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

package logstore

import (
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"sync/atomic"
)

type syncBase struct {
	synced, syncing             uint64
	checkpointed, checkpointing uint64
}

func (base *syncBase) OnEntryReceived(e Entry) error {
	if info := e.GetAuxilaryInfo(); info != nil {
		switch v := info.(type) {
		case uint64:
			base.syncing = v
		case *common.Range:
			base.checkpointing = v.Right
		default:
			panic("not supported")
		}
	}
	return nil
}

func (base *syncBase) GetCheckpointId() uint64 {
	return atomic.LoadUint64(&base.checkpointed)
}

func (base *syncBase) SetCheckpointId(id uint64) {
	atomic.StoreUint64(&base.checkpointed, id)
}

func (base *syncBase) GetSyncedId() uint64 {
	return atomic.LoadUint64(&base.synced)
}

func (base *syncBase) SetSyncedId(id uint64) {
	atomic.StoreUint64(&base.synced, id)
}

func (base *syncBase) OnCommit() {
	if base.checkpointing > base.checkpointed {
		base.SetCheckpointId(base.checkpointing)
	}
	if base.syncing > base.synced {
		base.SetSyncedId(base.syncing)
	}
}
