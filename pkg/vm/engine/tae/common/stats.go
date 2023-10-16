// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const DefaultNotLoadMoreThan = 4096

var (
	NotLoadMoreThan atomic.Int32
)

func init() {
	NotLoadMoreThan.Store(DefaultNotLoadMoreThan)
}

type TableCompactStat struct {
	sync.RWMutex

	Inited bool

	// Configs

	// make this table apply flush table tail policy
	FlushTableTailEnabled bool
	// how often to flush table tail
	// this duration will be add some random value to avoid flush many tables at the same time
	FlushGapDuration time.Duration
	// if the size of table tail, in bytes, exceeds FlushMemCapacity, flush it immediately
	FlushMemCapacity int

	EstimateRowSize int

	// Status

	// dirty end range flushed by last flush txn. If we are waiting for a ckp [a, b], if all dirty tables' LastFlush are greater than b,
	// the checkpoint is ready to collect data and write all down.
	LastFlush types.TS
	// FlushDeadline is the deadline to flush table tail
	FlushDeadline time.Time
}

func (s *TableCompactStat) ResetDeadlineWithLock() {
	// add random +/- 10%
	factor := 1.0 + float64(rand.Intn(21)-10)/100.0
	s.FlushDeadline = time.Now().Add(time.Duration(factor * float64(s.FlushGapDuration)))
}

func (s *TableCompactStat) InitWithLock(durationHint time.Duration) {
	s.FlushGapDuration = durationHint * 5
	s.FlushMemCapacity = 20 * 1024 * 1024
	s.FlushTableTailEnabled = true
	s.Inited = true
}

func (s *TableCompactStat) UpdateEstimateRowSize(rowSize int) {
	s.Lock()
	defer s.Unlock()
	s.EstimateRowSize = rowSize
}

func (s *TableCompactStat) GetEstimateRowSize() int {
	s.RLock()
	defer s.RUnlock()
	return s.EstimateRowSize
}
