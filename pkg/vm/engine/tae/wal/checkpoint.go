package wal

import (
	"time"
)

func (driver *walDriver) Checkpoint(indexes []*Index) (e LogEntry, err error) {
	e, err = driver.impl.FuzzyCheckpoint(GroupC, indexes)
	return
}

func (driver *walDriver) checkpointTicker() {
	defer driver.wg.Done()
	ticker := time.NewTicker(driver.ckpDuration)
	for {
		select {
		case <-driver.cancelContext.Done():
			return
		case <-ticker.C:
			driver.CkpUC()
		}
	}
}

//tid-lsn-ckped uclsn-tid,tid-clsn,cckped
func (driver *walDriver) CkpUC() {
	ckpedlsn := driver.impl.GetCheckpointed(GroupC)
	ucLsn := driver.impl.GetCheckpointed(GroupUC)
	maxLsn := driver.impl.GetSynced(GroupUC)
	ckpedUC := ucLsn
	driver.cmu.RLock()
	driver.ucmu.RLock()
	for i := ucLsn + 1; i <= maxLsn; i++ {
		tid, ok := driver.ucLsnTidMap[i]
		if !ok {
			break
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
	driver.cmu.RUnlock()
	driver.ucmu.RUnlock()
	if ckpedUC == ucLsn {
		return
	}
	driver.impl.RangeCheckpoint(GroupUC, 0, ckpedUC)
}
