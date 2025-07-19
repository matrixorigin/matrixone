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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"go.uber.org/zap"
)

var ErrDSNNotFound = moerr.NewInternalErrorNoCtx("dsn not found")
var ErrRetryTimeOut = moerr.NewInternalErrorNoCtx("retry time out")

type DSNStats struct {
	Truncated uint64
	Min       uint64
	Max       uint64
}

type sequenceNumberState struct {
	// PSN: physical sequence number. here is the lsn from logservice
	sequence struct {
		mu sync.RWMutex
		// key: PSN, value: DSNs
		psn2DSNMap map[uint64]common.ClosedInterval
		psns       roaring64.Bitmap
	}

	watermark struct {
		mu           sync.RWMutex
		committedDSN uint64

		// dsn: driver sequence number
		// it is monotonically continuously increasing
		// PSN:[DSN:LSN, DSN:LSN, DSN:LSN, ...]
		// One : Many
		nextDSN atomic.Uint64
	}

	truncateDSNIntent atomic.Uint64 //
	truncatedPSN      uint64        //
}

func newSequenceNumberState() *sequenceNumberState {
	d := new(sequenceNumberState)
	d.sequence.psn2DSNMap = make(map[uint64]common.ClosedInterval)
	return d
}

func (sns *sequenceNumberState) initState(stats *DSNStats) {
	sns.watermark.nextDSN.Store(stats.Max)
	sns.watermark.committedDSN = stats.Max
	if stats.Min != math.MaxUint64 {
		sns.truncateDSNIntent.Store(stats.Min - 1)
	}
	sns.truncatedPSN = stats.Truncated
}

// psn: physical sequence number
// dsns: dsns in the log entry of the psn
func (sns *sequenceNumberState) recordSequenceNumbers(
	psn uint64, dsns common.ClosedInterval,
) {
	sns.sequence.mu.Lock()
	defer sns.sequence.mu.Unlock()
	sns.sequence.psn2DSNMap[psn] = dsns
	sns.sequence.psns.Add(psn)
}

func (sns *sequenceNumberState) GetDSN() uint64 {
	return sns.watermark.nextDSN.Load()
}

func (sns *sequenceNumberState) getNextValidPSN(psn uint64) uint64 {
	sns.sequence.mu.RLock()
	defer sns.sequence.mu.RUnlock()
	if sns.sequence.psns.IsEmpty() {
		return 0
	}
	maxPSN := sns.sequence.psns.Maximum()
	// [psn >= maxPSN]
	if psn >= maxPSN {
		return maxPSN
	}
	// [psn < maxPSN]
	// PXU TODO: psn++???
	psn++
	for !sns.sequence.psns.Contains(psn) {
		psn++
	}
	return psn
}

func (sns *sequenceNumberState) isToTruncate(psn, dsn uint64) bool {
	maxDSN := sns.getMaxDSN(psn)

	// psn cannot be found in the psn.psn2DSNMap
	if maxDSN == 0 {
		return false
	}

	// the maxDSN of the psn is less equal to the dsn
	return maxDSN <= dsn
}

func (sns *sequenceNumberState) getMaxDSN(psn uint64) uint64 {
	sns.sequence.mu.RLock()
	defer sns.sequence.mu.RUnlock()
	dsnRange, ok := sns.sequence.psn2DSNMap[psn]
	if !ok {
		return 0
	}
	return dsnRange.End
}

func (sns *sequenceNumberState) allocateDSN() uint64 {
	return sns.watermark.nextDSN.Add(1)
}

func (sns *sequenceNumberState) recordCommitInfo(committer *groupCommitter) {
	dsnRange := committer.writer.Entry.DSNRange()

	sns.sequence.mu.Lock()
	sns.sequence.psns.Add(committer.psn)
	sns.sequence.psn2DSNMap[committer.psn] = dsnRange
	sns.sequence.mu.Unlock()

	sns.watermark.mu.Lock()
	defer sns.watermark.mu.Unlock()
	if dsnRange.Start != sns.watermark.committedDSN+1 {
		panic(fmt.Sprintf(
			"logic err, expect %d, actual %s",
			sns.watermark.committedDSN+1,
			dsnRange.String(),
		))
	}
	sns.watermark.committedDSN = dsnRange.End
}

func (sns *sequenceNumberState) truncateByPSN(psn uint64) {
	var (
		beforeCnt uint64
		afterCnt  uint64
	)
	defer func() {
		logutil.Info(
			"Wal-Truncate-By-PSN",
			zap.Uint64("psn", psn),
			zap.Uint64("before", beforeCnt),
			zap.Uint64("after", afterCnt),
			zap.Uint64("deleted", beforeCnt-afterCnt),
		)
	}()
	sns.sequence.mu.Lock()
	defer sns.sequence.mu.Unlock()
	beforeCnt = uint64(len(sns.sequence.psn2DSNMap))
	candidates := make([]uint64, 0)
	// collect all the PSN that is less than the given PSN
	for sn := range sns.sequence.psn2DSNMap {
		if sn < psn {
			candidates = append(candidates, sn)
		}
	}
	// remove 0 to the given PSN from the validPSN
	sns.sequence.psns.RemoveRange(0, psn)

	// remove the PSN from the map
	for _, lsn := range candidates {
		delete(sns.sequence.psn2DSNMap, lsn)
	}
	afterCnt = uint64(len(sns.sequence.psn2DSNMap))
}

func (sns *sequenceNumberState) getCommittedDSNWatermark() uint64 {
	sns.watermark.mu.RLock()
	defer sns.watermark.mu.RUnlock()
	return sns.watermark.committedDSN
}
