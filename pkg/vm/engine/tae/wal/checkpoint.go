package wal

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

func (driver *walDriver) Checkpoint(indexes []*Index) (e LogEntry, err error) {
	return driver.impl.FuzzyCheckpoint(GroupC,indexes)
}
func (driver *walDriver) onCheckpoint(items ...any) {
	for _, item := range items {
		e := item.(entry.Entry)
		err := e.WaitDone()
		if err != nil {
			panic(err)
		}
	}
	driver.CkpUC()
}

//tid-lsn-ckped uclsn-tid,tid-clsn,cckped
func (driver *walDriver) CkpUC() {
	ckpedlsn := driver.impl.GetCheckpointed(GroupC)
	ucLsn := driver.impl.GetCheckpointed(GroupUC)
	maxLsn := driver.impl.GetSynced(GroupUC)
	ckpedUC := ucLsn
	for i := ucLsn + 1; i <= maxLsn; i++ {
		tid, ok := driver.ucLsnTidMap[i]
		if !ok {
			panic("logic error")
		}
		lsn, ok := driver.cTidLsnMap[tid]
		if !ok {
			break
		}
		if lsn > ckpedlsn {
			break
		}
		ckpedUC = i
	}
	logutil.Infof("checkpoint UC %d", ckpedUC)
	// w.impl.RangeCheckpoint(GroupUC, ckpedUC)
}
