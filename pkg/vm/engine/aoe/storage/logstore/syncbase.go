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
