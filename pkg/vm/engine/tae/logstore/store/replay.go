package store

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
)

func (w *StoreImpl) Replay(h ApplyHandle)error {
	w.driver.Replay(func(e *entry.Entry) {
		w.replayEntry(e, h)
	})
	lsn,err:=w.driver.GetTruncated()
	if err!= nil{
		panic(err)
	}
	w.onCheckpoint()
	w.driverCheckpointed=lsn
	w.driverCheckpointing=lsn
	for g,lsn:=range w.syncing{
		w.walCurrentLsn[g]=lsn
		w.synced[g] = lsn
	}
	return nil
}

func (w *StoreImpl) replayEntry(e *entry.Entry, h ApplyHandle)error {
	walEntry := e.Entry
	info := e.Info
	switch info.Group {
	case GroupInternal:
	case GroupCKP:
		w.logCheckpointInfo(info)
	case GroupC:
	}
	w.logDriverLsn(e)
	h(info.Group, info.GroupLSN, walEntry.GetPayload(), walEntry.GetType(), nil)
	return nil
}
