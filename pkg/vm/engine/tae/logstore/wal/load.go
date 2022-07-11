package wal

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

func (w *WalImpl) Load(gid uint32, lsn uint64) (entry.Entry, error) {
	driverLsn, err := w.getDriverLsn(gid, lsn)
	if err != nil {
		return nil, err
	}
	driverEntry := w.driver.Read(driverLsn)
	return driverEntry.Entry, nil
}
