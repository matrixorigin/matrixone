package event

import (
	"matrixone/pkg/vm/engine/aoe/storage/logutil"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

func NewLoggingEventListener() EventListener {
	return EventListener{
		BackgroundErrorCB: func(err error) {
			logutil.Errorf("BackgroundError %s", err)
		},

		MemTableFullCB: func(table imem.IMemTable) {
			logutil.Debugf("MemTable %d is full", table.GetMeta().GetID())
		},

		FlushBlockBeginCB: func(table imem.IMemTable) {
			logutil.Debugf("MemTable %d begins to flush", table.GetMeta().GetID())
		},

		FlushBlockEndCB: func(table imem.IMemTable) {
			logutil.Debugf("MemTable %d end flush", table.GetMeta().GetID())
		},

		CheckpointStartCB: func(info *md.MetaInfo) {
			logutil.Debugf("Start checkpoint %d", info.CheckPoint)
		},

		CheckpointEndCB: func(info *md.MetaInfo) {
			logutil.Debugf("End checkpoint %d", info.CheckPoint)
		},
	}
}
