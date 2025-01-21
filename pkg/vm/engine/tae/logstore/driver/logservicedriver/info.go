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

package logservicedriver

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var ErrDSNNotFound = moerr.NewInternalErrorNoCtx("dsn not found")
var ErrRetryTimeOut = moerr.NewInternalErrorNoCtx("retry time out")

type DSNStats struct {
	Truncated uint64
	Min       uint64
	Max       uint64
}

type tokenController struct {
	sync.Cond
	nextToken uint64
	bm        roaring64.Bitmap
	maxCount  uint64
}

func newTokenController(maxCount uint64) *tokenController {
	return &tokenController{
		maxCount: maxCount,
		Cond:     *sync.NewCond(new(sync.Mutex)),
	}
}

func (rc *tokenController) Putback(tokens ...uint64) {
	rc.L.Lock()
	defer rc.L.Unlock()
	for _, token := range tokens {
		rc.bm.Remove(token)
	}
	rc.Broadcast()
}

func (rc *tokenController) Apply() (token uint64) {
	rc.L.Lock()
	defer rc.L.Unlock()
	for {
		if rc.bm.IsEmpty() {
			token = rc.nextToken
			rc.nextToken++
			rc.bm.Add(token)
			return
		}
		minimum := rc.bm.Minimum()
		if rc.nextToken < rc.maxCount+minimum {
			token = rc.nextToken
			rc.nextToken++
			rc.bm.Add(token)
			return
		}
		// logutil.Infof("too much pendding writes: %d, %d, %d", rc.bm.Minimum(), rc.bm.Maximum(), rc.bm.GetCardinality())
		rc.Wait()
	}
}

type driverInfo struct {
	// PSN: physical sequence number. here is the lsn from logservice
	psn struct {
		mu sync.RWMutex
		// key: PSN, value: DSNs
		dsnMap  map[uint64]common.ClosedInterval
		records roaring64.Bitmap
	}

	watermark struct {
		mu            sync.RWMutex
		committingDSN uint64
		committedDSN  uint64

		// dsn: driver sequence number
		// it is monotonically continuously increasing
		// PSN:[DSN:LSN, DSN:LSN, DSN:LSN, ...]
		// One : Many
		nextDSN atomic.Uint64
	}

	truncateDSNIntent atomic.Uint64 //
	truncatedPSN      uint64        //
}

func newDriverInfo() *driverInfo {
	d := new(driverInfo)
	d.psn.dsnMap = make(map[uint64]common.ClosedInterval)
	return d
}

func (info *driverInfo) initState(stats *DSNStats) {
	info.watermark.nextDSN.Store(stats.Max)
	info.watermark.committingDSN = stats.Max
	info.watermark.committedDSN = stats.Max
	if stats.Min != math.MaxUint64 {
		info.truncateDSNIntent.Store(stats.Min - 1)
	}
	info.truncatedPSN = stats.Truncated
}

// psn: physical sequence number
// dsns: dsns in the log entry of the psn
func (info *driverInfo) recordPSNInfo(
	psn uint64, dsns common.ClosedInterval,
) {
	info.psn.dsnMap[psn] = dsns
	info.psn.records.Add(psn)
}

func (info *driverInfo) GetDSN() uint64 {
	return info.watermark.nextDSN.Load()
}

func (info *driverInfo) getNextValidPSN(psn uint64) uint64 {
	info.psn.mu.RLock()
	defer info.psn.mu.RUnlock()
	if info.psn.records.IsEmpty() {
		return 0
	}
	maxPSN := info.psn.records.Maximum()
	// [psn >= maxPSN]
	if psn >= maxPSN {
		return maxPSN
	}
	// [psn < maxPSN]
	// PXU TODO: psn++???
	psn++
	for !info.psn.records.Contains(psn) {
		psn++
	}
	return psn
}

func (info *driverInfo) isToTruncate(psn, dsn uint64) bool {
	maxDSN := info.getMaxDSN(psn)

	// psn cannot be found in the psn.dsnMap
	if maxDSN == 0 {
		return false
	}

	// the maxDSN of the psn is less equal to the dsn
	return maxDSN <= dsn
}

func (info *driverInfo) getMaxDSN(psn uint64) uint64 {
	info.psn.mu.RLock()
	defer info.psn.mu.RUnlock()
	dsnRange, ok := info.psn.dsnMap[psn]
	if !ok {
		return 0
	}
	return dsnRange.End
}

func (info *driverInfo) allocateDSN() uint64 {
	return info.watermark.nextDSN.Add(1)
}

func (info *driverInfo) recordCommitInfo(committer *groupCommitter) {
	dsnRange := committer.writer.Entry.DSNRange()

	info.psn.mu.Lock()
	info.psn.records.Add(committer.psn)
	info.psn.dsnMap[committer.psn] = dsnRange
	info.psn.mu.Unlock()

	if dsnRange.Start != info.watermark.committingDSN+1 {
		panic(fmt.Sprintf(
			"logic err, expect %d, actual %s",
			info.watermark.committingDSN+1,
			dsnRange.String(),
		))
	}
	info.watermark.committingDSN = dsnRange.End
}

func (info *driverInfo) gcPSN(psn uint64) {
	info.psn.mu.Lock()
	defer info.psn.mu.Unlock()
	candidates := make([]uint64, 0)
	// collect all the PSN that is less than the given PSN
	for sn := range info.psn.dsnMap {
		if sn < psn {
			candidates = append(candidates, sn)
		}
	}
	// remove 0 to the given PSN from the validPSN
	info.psn.records.RemoveRange(0, psn)

	// remove the PSN from the map
	for _, lsn := range candidates {
		delete(info.psn.dsnMap, lsn)
	}
}

func (info *driverInfo) getCommittedDSNWatermark() uint64 {
	info.watermark.mu.RLock()
	defer info.watermark.mu.RUnlock()
	return info.watermark.committedDSN
}

func (info *driverInfo) commitWatermark() {
	info.watermark.mu.Lock()
	info.watermark.committedDSN = info.watermark.committingDSN
	info.watermark.mu.Unlock()
}
