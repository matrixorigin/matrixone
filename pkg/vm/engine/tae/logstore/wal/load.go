package wal

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

func (w *WalImpl) Load(gid uint32, lsn uint64) (entry.Entry, error) {
	driverLsn, err := w.retryGetDriverLsn(gid, lsn)
	if err != nil {
		return nil, err
	}
	driverEntry, err := w.driver.Read(driverLsn)
	return driverEntry.Entry, err
}
