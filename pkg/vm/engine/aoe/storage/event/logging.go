package event

import (
	log "github.com/sirupsen/logrus"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

func NewLoggingEventListener() EventListener {
	return EventListener{
		BackgroundErrorCB: func(err error) {
			log.Errorf("BackgroundError %s", err)
		},

		MemTableFullCB: func(table imem.IMemTable) {
			log.Infof("MemTable %d is full", table.GetMeta().GetID())
		},

		FlushBlockBeginCB: func(table imem.IMemTable) {
			log.Infof("MemTable %d begins to flush", table.GetMeta().GetID())
		},

		FlushBlockEndCB: func(table imem.IMemTable) {
			log.Infof("MemTable %d end flush", table.GetMeta().GetID())
		},

		CheckpointStartCB: func(info *md.MetaInfo) {
			log.Infof("Start checkpoint %d", info.CheckPoint)
		},

		CheckpointEndCB: func(info *md.MetaInfo) {
			log.Infof("End checkpoint %d", info.CheckPoint)
		},
	}
}
