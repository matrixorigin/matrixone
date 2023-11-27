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
	"encoding/hex"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const (
	DefaultNotLoadMoreThan  = 4096
	DefaultMinRowsQualified = 40960
	DefaultMaxMergeObjN     = 2

	Const1GBytes = 1 << 30
	Const1MBytes = 1 << 20
)

var (
	RuntimeNotLoadMoreThan  atomic.Int32
	RuntimeMaxMergeObjN     atomic.Int32
	RuntimeMinRowsQualified atomic.Int32
)

func init() {
	RuntimeNotLoadMoreThan.Store(DefaultNotLoadMoreThan)
	RuntimeMaxMergeObjN.Store(DefaultMaxMergeObjN)
	RuntimeMinRowsQualified.Store(DefaultMinRowsQualified)
}

type MergeHistory struct {
	LastTime time.Time
	OSize    int
	NObj     int
	NBlk     int
}

func (h *MergeHistory) Add(osize, nobj, nblk int) {
	h.OSize = osize
	h.NObj = nobj
	h.NBlk = nblk
	h.LastTime = time.Now()
}

func (h *MergeHistory) IsLastBefore(d time.Duration) bool {
	return h.LastTime.Before(time.Now().Add(-d))
}

func (h *MergeHistory) String() string {
	return fmt.Sprintf(
		"(%v) no%v nb%v osize%v",
		h.LastTime.Format("2006-01-02_15:04:05"),
		h.NObj, h.NBlk,
		HumanReadableBytes(h.OSize),
	)
}

type TableCompactStat struct {
	sync.RWMutex

	Inited bool

	// Configs

	// how often to flush table tail
	// this duration will be add some random value to avoid flush many tables at the same time
	FlushGapDuration time.Duration
	// if the size of table tail, in bytes, exceeds FlushMemCapacity, flush it immediately
	FlushMemCapacity int

	// Status

	// dirty end range flushed by last flush txn. If we are waiting for a ckp [a, b], and all dirty tables' LastFlush are greater than b,
	// the checkpoint is ready to collect data and write all down.
	LastFlush types.TS
	// FlushDeadline is the deadline to flush table tail
	FlushDeadline time.Time

	MergeHist MergeHistory
}

func (s *TableCompactStat) ResetDeadlineWithLock() {
	// add random +/- 10%
	factor := 1.0 + float64(rand.Intn(21)-10)/100.0
	s.FlushDeadline = time.Now().Add(time.Duration(factor * float64(s.FlushGapDuration)))
}

func (s *TableCompactStat) InitWithLock(durationHint time.Duration) {
	s.FlushGapDuration = durationHint * 5
	s.FlushMemCapacity = 20 * 1024 * 1024
	s.Inited = true
}

func (s *TableCompactStat) AddMerge(osize, nobj, nblk int) {
	s.Lock()
	defer s.Unlock()
	s.MergeHist.Add(osize, nobj, nblk)
}

func (s *TableCompactStat) GetLastFlush() types.TS {
	s.RLock()
	defer s.RUnlock()
	return s.LastFlush
}

func (s *TableCompactStat) GetLastMerge() *MergeHistory {
	s.RLock()
	defer s.RUnlock()
	return &s.MergeHist
}

func HumanReadableBytes(bytes int) string {
	if bytes < 1024 {
		return fmt.Sprintf("%dB", bytes)
	}
	if bytes < Const1MBytes {
		return fmt.Sprintf("%.2fKB", float64(bytes)/1024)
	}
	if bytes < Const1GBytes {
		return fmt.Sprintf("%.2fMB", float64(bytes)/1024/1024)
	}
	return fmt.Sprintf("%.2fGB", float64(bytes)/1024/1024/1024)
}

func ShortSegId(x types.Uuid) string {
	var shortuuid [8]byte
	hex.Encode(shortuuid[:], x[:4])
	return string(shortuuid[:])
}

func ShortObjId(x types.Objectid) string {
	var shortuuid [8]byte
	hex.Encode(shortuuid[:], x[:4])
	return string(shortuuid[:])
}
