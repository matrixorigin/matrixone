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
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"golang.org/x/exp/constraints"
)

type TempDelCacheEntry struct {
	Bat     *batch.Batch
	Release func()
}

type DeletesCollectRecorder struct {
	LoadCost   time.Duration
	BisectCost time.Duration
	MemCost    time.Duration
	TempCache  map[string]TempDelCacheEntry
}

type DeletesCollectBoard struct {
	DeletesCollectRecorder
	LoadMax, BisectMax, MemMax time.Duration
	LoadCnt                    int
}

func (r *DeletesCollectBoard) Add(other *DeletesCollectRecorder) {
	r.LoadCost += other.LoadCost
	r.BisectCost += other.BisectCost
	r.MemCost += other.MemCost

	if other.LoadCost > r.LoadMax {
		r.LoadMax = other.LoadCost
	}
	if other.BisectCost > r.BisectMax {
		r.BisectMax = other.BisectCost
	}
	if other.MemCost > r.MemMax {
		r.MemMax = other.MemCost
	}
	if other.LoadCost > 0 {
		r.LoadCnt++
	}
}

func (r *DeletesCollectBoard) String() string {
	return fmt.Sprintf(
		"LoadCost:%v BisectCost:%v MemCost:%v LoadMax:%v BisectMax:%v MemMax:%v LoadCnt:%v",
		r.LoadCost, r.BisectCost, r.MemCost, r.LoadMax, r.BisectMax, r.MemMax, r.LoadCnt)
}

type RecorderKey struct{}

const (
	DefaultMinOsizeQualifiedMB   = 110   // MB
	DefaultMaxOsizeObjMB         = 128   // MB
	DefaultMinCNMergeSize        = 80000 // MB
	DefaultCNMergeMemControlHint = 8192  // MB
	DefaultMaxMergeObjN          = 16

	Const1GBytes = 1 << 30
	Const1MBytes = 1 << 20
)

var (
	RuntimeMaxMergeObjN        atomic.Int32
	RuntimeOsizeRowsQualified  atomic.Uint32
	RuntimeMaxObjOsize         atomic.Uint32
	RuntimeMinCNMergeSize      atomic.Uint64
	RuntimeCNMergeMemControl   atomic.Uint64
	RuntimeCNTakeOverAll       atomic.Bool
	IsStandaloneBoost          atomic.Bool
	ShouldStandaloneCNTakeOver atomic.Bool
	Epsilon                    float64

	RuntimeOverallFlushMemCap atomic.Uint64
)

func init() {
	RuntimeMaxMergeObjN.Store(DefaultMaxMergeObjN)
	RuntimeOsizeRowsQualified.Store(DefaultMinOsizeQualifiedMB * Const1MBytes)
	RuntimeMaxObjOsize.Store(DefaultMaxOsizeObjMB * Const1MBytes)
	Epsilon = math.Nextafter(1, 2) - 1
}

type Number interface {
	constraints.Integer | constraints.Float
}

///////
/// statistics component
///////

type TrendKind int
type WorkloadKind int

const (
	TrendUnknown TrendKind = iota
	TrendDecII
	TrendDecI
	TrendStable
	TrendIncI
	TrendIncII
)

const (
	WorkUnknown WorkloadKind = iota
	WorkQuiet
	WorkApInsert
	WorkApQuiet
	WorkTpUpdate
	WorkMixed
)

func (t TrendKind) String() string {
	switch t {
	case TrendDecII:
		return "DecII"
	case TrendDecI:
		return "DecI"
	case TrendStable:
		return "Stable"
	case TrendIncI:
		return "IncI"
	case TrendIncII:
		return "IncII"
	default:
		return "UnknownTrend"
	}
}

type HistorySampler[T Number] interface {
	Append(x T)
	V() T
	QueryTrend() (TrendKind, TrendKind, TrendKind)
	String() string
}

// general sampler
type SampleIII[T Number] struct {
	new, sample, v1, v2, v3 T

	lastSampleT time.Time

	// f1 = 1.0
	f2, f3 float64

	// config
	d1, d2, d3 time.Duration
}

var F2 = 0.7
var F3 = 0.4
var D1 = 30 * time.Second
var D2 = 2 * time.Minute
var D3 = 5 * time.Minute

// optimize for saving space
type FixedSampleIII[T Number] struct {
	new, sample, v1, v2, v3 T
	lastSampleT             time.Time
}

func (s *FixedSampleIII[T]) tick(x T) {
	s.new = x
	// init
	if s.lastSampleT.IsZero() {
		s.lastSampleT = time.Now()
		s.sample = x
		s.v1 = x
		s.v2 = x
		s.v3 = x
		return
	}
	now := time.Now()
	span := now.Sub(s.lastSampleT)
	if span < D1 {
		return
	}
	// rotate and sample this point

	// cnt history values need to be pushed back
	// cnt := int(span / s.samplePeriod)
	if span > D3 {
		s.v3 = s.sample
		s.v2 = s.sample
		s.v1 = s.sample
	} else if span > D2 {
		s.v3 = moveAvg(s.v3, s.v2, F2)
		s.v2 = s.sample
		s.v1 = s.sample
	} else {
		s.v3 = moveAvg(s.v3, s.v2, F3)
		s.v2 = moveAvg(s.v2, s.v1, F2)
		s.v1 = s.sample
	}

	s.lastSampleT = now
	s.sample = x
}

func (s *FixedSampleIII[T]) Append(x T) {
	s.tick(x)
}

func (s *FixedSampleIII[T]) V() T {
	return s.new
}

func (s *FixedSampleIII[T]) QueryTrend() (TrendKind, TrendKind, TrendKind) {
	judgeTrend := func(vprev, vnow T) TrendKind {
		if roundZero(vprev) {
			if vnow > 0 {
				return TrendIncI
			} else if vnow < 0 {
				return TrendDecI
			} else {
				return TrendStable
			}
		}
		delta := float64(vnow - vprev)
		deltaPercent := math.Abs(delta / float64(vprev))
		if math.Signbit(delta) {
			deltaPercent = -deltaPercent
		}

		if deltaPercent < -0.4 {
			return TrendDecII
		} else if deltaPercent < -0.01 {
			return TrendDecI
		} else if deltaPercent < 0.01 {
			return TrendStable
		} else if deltaPercent < 0.4 {
			return TrendIncI
		} else {
			return TrendIncII
		}
	}
	s.tick(s.new)
	return judgeTrend(s.v1, s.new), judgeTrend(s.v2, s.new), judgeTrend(s.v3, s.new)
}

func (s *FixedSampleIII[T]) String() string {
	x, m, l := s.QueryTrend()
	return fmt.Sprintf(
		"Sample(%v/%v/{%v,%v,%v}/%v,%v,%v)",
		s.new, s.lastSampleT.Format("2006-01-02_15:04:05"),
		s.v1, s.v2, s.v3,
		x, m, l,
	)
}

func NewSmapler35m[T Number]() FixedSampleIII[T] {
	return FixedSampleIII[T]{}
}

func selectFactor(cnt int) float64 {
	list := []int{2, 3, 4, 6, 7, 10, 13, 21, 44}
	for i, v := range list {
		if cnt <= v {
			return float64(9-i) / 10.0
		}
	}
	return 0.05
}

func NewSampleIII[T Number](sp, d2, d3 time.Duration) *SampleIII[T] {
	return &SampleIII[T]{
		d1: sp,
		d2: d2,
		d3: d3,
		f2: selectFactor(int(d2 / sp)),
		f3: selectFactor(int(d3 / sp)),
	}
}

func (s *SampleIII[T]) tick(x T) {
	s.new = x
	// init
	if s.lastSampleT.IsZero() {
		s.lastSampleT = time.Now()
		s.sample = x
		s.v1 = x
		s.v2 = x
		s.v3 = x
		return
	}
	now := time.Now()
	span := now.Sub(s.lastSampleT)
	if span < s.d1 {
		return
	}
	// rotate and sample this point

	// cnt history values need to be pushed back
	// cnt := int(span / s.samplePeriod)
	if span > s.d3 {
		s.v3 = s.sample
		s.v2 = s.sample
		s.v1 = s.sample
	} else if span > s.d2 {
		s.v3 = moveAvg(s.v3, s.v2, s.f2)
		s.v2 = s.sample
		s.v1 = s.sample
	} else {
		s.v3 = moveAvg(s.v3, s.v2, s.f3)
		s.v2 = moveAvg(s.v2, s.v1, s.f2)
		s.v1 = s.sample
	}

	s.lastSampleT = now
	s.sample = x
}

func (s *SampleIII[T]) Append(x T) {
	s.tick(x)
}

func (s *SampleIII[T]) V() T {
	return s.new
}

func (s *SampleIII[T]) QueryTrend() (TrendKind, TrendKind, TrendKind) {
	judgeTrend := func(vprev, vnow T) TrendKind {
		if roundZero(vprev) {
			if vnow > 0 {
				return TrendIncI
			} else if vnow < 0 {
				return TrendDecI
			} else {
				return TrendStable
			}
		}
		delta := float64(vnow - vprev)
		deltaPercent := math.Abs(delta / float64(vprev))
		if math.Signbit(delta) {
			deltaPercent = -deltaPercent
		}

		if deltaPercent < -0.4 {
			return TrendDecII
		} else if deltaPercent < -0.01 {
			return TrendDecI
		} else if deltaPercent < 0.01 {
			return TrendStable
		} else if deltaPercent < 0.4 {
			return TrendIncI
		} else {
			return TrendIncII
		}
	}
	s.tick(s.new)
	return judgeTrend(s.v1, s.new), judgeTrend(s.v2, s.new), judgeTrend(s.v3, s.new)
}

func (s *SampleIII[T]) String() string {
	x, m, l := s.QueryTrend()
	return fmt.Sprintf(
		"Sample(%v/%v/{%v@-%v,%v@-%v(%.2f),%v@-%v(%.2f)}/%v,%v,%v)",
		s.new, s.lastSampleT.Format("2006-01-02_15:04:05"),
		s.v1, s.d1,
		s.v2, s.d2, s.f2,
		s.v3, s.d3, s.f3,
		x, m, l,
	)
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

///
/// Table statistics
///

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

	// FlushDeadline is the deadline to flush table tail
	FlushDeadline time.Time

	WorkloadGuess  WorkloadKind
	WorkloadStreak int
	RowCnt         FixedSampleIII[int]
	RowDel         FixedSampleIII[int]
	MergeHist      MergeHistory
}

func NewTableCompactStat() *TableCompactStat {
	return &TableCompactStat{
		RowCnt: FixedSampleIII[int]{},
		RowDel: FixedSampleIII[int]{},
	}
}

func (s *TableCompactStat) ResetDeadlineWithLock() {
	// add random +/- 10%
	factor := 1.0 + float64(rand.Intn(21)-10)/100.0
	s.FlushDeadline = time.Now().Add(time.Duration(factor * float64(s.FlushGapDuration)))
}

func (s *TableCompactStat) InitWithLock(durationHint time.Duration) {
	s.FlushGapDuration = durationHint * 5
	s.FlushMemCapacity = 20 * 1024 * 1024
	s.ResetDeadlineWithLock()
	s.Inited = true
}

func (s *TableCompactStat) AddMerge(osize, nobj, nblk int) {
	s.Lock()
	defer s.Unlock()
	s.MergeHist.Add(osize, nobj, nblk)
}

func (s *TableCompactStat) GetLastMerge() *MergeHistory {
	s.RLock()
	defer s.RUnlock()
	return &s.MergeHist
}

func (s *TableCompactStat) AddRowStat(rows, dels int) {
	s.Lock()
	defer s.Unlock()
	s.RowCnt.Append(rows)
	s.RowDel.Append(dels)

	rs, rm, rl := s.RowCnt.QueryTrend()
	ds, dm, dl := s.RowDel.QueryTrend()

	guess := WorkUnknown
	if IsApLikeDel(ds, dm, dl, dels) && s.RowCnt.V() > 1000*1000 {
		if IsLongStable(rs, rm, rl) {
			guess = WorkApQuiet
		} else {
			guess = WorkApInsert
		}
	}

	// force a bigger merge
	if s.WorkloadGuess == WorkApInsert && s.WorkloadStreak > 20 {
		guess = WorkApQuiet
	}

	if s.WorkloadGuess == guess {
		s.WorkloadStreak++
	} else {
		s.WorkloadGuess = guess
		s.WorkloadStreak = 0
	}
}

func (s *TableCompactStat) GetWorkloadGuess() WorkloadKind {
	s.RLock()
	defer s.RUnlock()
	return s.WorkloadGuess
}

func IsApLikeDel(s, m, l TrendKind, val int) bool {
	return s == TrendStable && m == TrendStable && l == TrendStable && val == 0
}

func IsLongStable(s, m, l TrendKind) bool {
	return s == TrendStable && m == TrendStable && l <= TrendIncI && l >= TrendDecI
}

////
// Other utils
////

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

func moveAvg[T Number](prev, now T, f float64) T {
	return T((1-f)*float64(prev) + f*float64(now))
}

func roundZero[T Number](v T) bool {
	return math.Abs(float64(v)) < Epsilon
}
