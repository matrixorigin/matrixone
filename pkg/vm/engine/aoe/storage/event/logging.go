package event

import (
	logutil2 "matrixone/pkg/logutil"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

func NewLoggingEventListener() EventListener {
	return EventListener{
		BackgroundErrorCB: func(err error) {
			logutil2.Errorf("BackgroundError %s", err)
		},

		MemTableFullCB: func(table imem.IMemTable) {
			logutil2.Debugf("MemTable %d is full", table.GetMeta().GetID())
		},

		FlushBlockBeginCB: func(table imem.IMemTable) {
			logutil2.Debugf("MemTable %d begins to flush", table.GetMeta().GetID())
		},

		FlushBlockEndCB: func(table imem.IMemTable) {
			logutil2.Debugf("MemTable %d end flush", table.GetMeta().GetID())
		},

		CheckpointStartCB: func(info *md.MetaInfo) {
			logutil2.Debugf("Start checkpoint %d", info.CheckPoint)
		},

		CheckpointEndCB: func(info *md.MetaInfo) {
			logutil2.Debugf("End checkpoint %d", info.CheckPoint)
		},
	}
}
