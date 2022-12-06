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

package batchstoredriver

import (
	"math"
	"sync"
	"sync/atomic"

	// "github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
)

var (
	ErrGroupNotExist       = moerr.NewInternalErrorNoCtx("group not existed")
	ErrLsnNotExist         = moerr.NewInternalErrorNoCtx("lsn not existed")
	ErrVFileVersionTimeOut = moerr.NewInternalErrorNoCtx("get vfile version timeout")
	ErrLsnCheckpointed     = moerr.NewInternalErrorNoCtx("lsn has been checkpointed")
)

type syncBase struct {
	*sync.RWMutex
	Lsn              uint64
	lsnmu            sync.RWMutex
	checkpointing    atomic.Uint64
	ckpmu            *sync.RWMutex
	truncatedVersion int
	syncing          uint64
	synced           uint64
	addrs            map[int]*common.ClosedIntervals //group-version-glsn range
	addrmu           sync.RWMutex
	commitCond       sync.Cond
}

func newSyncBase() *syncBase {
	return &syncBase{
		lsnmu:      sync.RWMutex{},
		addrs:      make(map[int]*common.ClosedIntervals),
		addrmu:     sync.RWMutex{},
		ckpmu:      &sync.RWMutex{},
		commitCond: *sync.NewCond(new(sync.Mutex)),
	}
}

func (base *syncBase) GetVersionByGLSN(lsn uint64) (int, error) {
	base.addrmu.RLock()
	defer base.addrmu.RUnlock()
	for ver, interval := range base.addrs {
		if interval.Contains(*common.NewClosedIntervalsByInt(lsn)) {
			return ver, nil
		}
	}
	// for ver, lsns := range base.addrs {
	// 	logutil.Infof("versionsMap %d %v", ver, lsns)
	// }
	return 0, ErrLsnNotExist
}

func (base *syncBase) OnEntryReceived(v *entry.Entry) error {
	base.syncing = v.Lsn
	base.addrmu.Lock()
	defer base.addrmu.Unlock()
	addr := v.Ctx.(*VFileAddress)
	interval, ok := base.addrs[addr.Version]
	if !ok {
		interval = common.NewClosedIntervals()
		base.addrs[addr.Version] = interval
	}
	interval.TryMerge(*common.NewClosedIntervalsByInt(v.Lsn))
	return nil
}

func (base *syncBase) GetTruncated() (uint64, error) {
	lsn := base.checkpointing.Load()
	return lsn, nil
}

func (base *syncBase) OnCommit() {
	base.commitCond.L.Lock()
	base.commitCond.Broadcast()
	base.commitCond.L.Unlock()

	if base.syncing > base.synced {
		base.synced = base.syncing
	}
}

func (base *syncBase) AllocateLsn() uint64 {
	base.lsnmu.Lock()
	defer base.lsnmu.Unlock()
	base.Lsn++
	return base.Lsn
}

func (base *syncBase) GetCurrSeqNum() uint64 {
	base.lsnmu.RLock()
	defer base.lsnmu.RUnlock()
	return base.Lsn
}

func (base *syncBase) onTruncatedFile(id int) {
	base.truncatedVersion = id
}

func (base *syncBase) onReplay(r *replayer) {
	base.Lsn = r.maxlsn
	base.synced = r.maxlsn
	base.syncing = r.maxlsn
	if r.minlsn != math.MaxUint64 {
		base.checkpointing.Store(r.minlsn)
	}
}
