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

package wal

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

func (driver *walDriver) Checkpoint(indexes []*Index) (e LogEntry, err error) {
	e, err = driver.impl.FuzzyCheckpoint(GroupPrepare, indexes)
	return
}

func (driver *walDriver) RangeCheckpoint(start, end uint64) (e LogEntry, err error) {
	e, err = driver.impl.RangeCheckpoint(GroupPrepare, start, end)
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

// tid-lsn-ckped uclsn-tid,tid-clsn,cckped
func (driver *walDriver) CkpUC() {
	ckpedlsn := driver.impl.GetCheckpointed(GroupPrepare)
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
	e, err := driver.impl.RangeCheckpoint(GroupUC, 0, ckpedUC)
	if err == common.ErrClose {
		return
	}
	if err != nil {
		panic(err)
	}
	err = e.WaitDone()
	if err != nil {
		panic(err)
	}
}
