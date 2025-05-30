// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package merge

import (
	"context"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"time"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

var (
	DefaultLayerZeroOpts = &LayerZeroOpts{
		LowerSelectSize: 0,
		UpperSelectSize: common.DefaultMinOsizeQualifiedBytes,
		LowerSelectRows: 0,
		UpperSelectRows: math.MaxUint32,
		// time:	1	2	3	4	5	6	7	8	9	10	11	12	13	14	15	16	17	18	19	20
		// val :	32	32	32	32	32	32	32	32	32	32	32	31	31	31	30	30	29	29	28	27
		//
		// time:	21	22	23	24	25	26	27	28	29	30	31	32	33	34	35	36	37	38	39	40
		// val :	26	25	23	21	18	16	14	12	10	9	8	8	7	6	6	5	5	4	4	4
		//
		// time:	41	42	43	44	45	46	47	48	49	50	51	52	53	54	55	56	57	58	59	60
		// val :	3	3	3	3	3	2	2	2	2	2	2	2	2	2	2	2	2	2	2	1
		Start:    32,
		End:      1,
		Duration: 1 * time.Hour,
		CPoints:  [4]float64{0.70, 0.0, 0.1, 1.0},
	}
)

const (
	NoteByTolerance = "trigger by tolerance %v"
	NoteBySize      = "trigger by size %v"

	TenYears = 3000 * 24 * time.Hour
)

// t: parameter [0,1]
// p0, p1, p2, p3: control point value of x or y axis
func cubicBezier(t float64, p0, p1, p2, p3 float64) float64 {
	// Using Horner's method for more efficient polynomial evaluation
	// B(t) = (1-t)^3 * P0 + 3*(1-t)^2*t * P1 + 3*(1-t)*t^2 * P2 + t^3 * P3
	// Expanded form: P0 - 3*P0*t + 3*P0*t^2 - P0*t^3 + 3*P1*t - 6*P1*t^2 + 3*P1*t^3 + 3*P2*t^2 - 3*P2*t^3 + P3*t^3
	// Rearranged: P0 + t*(-3*P0 + 3*P1) + t^2*(3*P0 - 6*P1 + 3*P2) + t^3*(-P0 + 3*P1 - 3*P2 + P3)

	c3 := -p0 + 3*p1 - 3*p2 + p3
	c2 := 3*p0 - 6*p1 + 3*p2
	c1 := -3*p0 + 3*p1
	c0 := p0

	// Apply Horner's method: ((c3*t + c2)*t + c1)*t + c0
	return ((c3*t+c2)*t+c1)*t + c0
}

func findTForX(x, p0x, p1x, p2x, p3x float64) float64 {
	epsilon := 0.0001
	minT, maxT := 0.0, 1.0
	for maxT-minT > epsilon {
		midT := (minT + maxT) / 2
		currentX := cubicBezier(midT, p0x, p1x, p2x, p3x)
		if currentX < x {
			minT = midT
		} else {
			maxT = midT
		}
	}
	return minT
}

type LayerZeroOpts struct {
	LowerSelectSize uint32
	UpperSelectSize uint32
	LowerSelectRows uint32
	UpperSelectRows uint32

	Start, End int
	Duration   time.Duration
	CPoints    [4]float64
}

func (o *LayerZeroOpts) String() string {
	return fmt.Sprintf(
		"LayerZeroOpts{S: %d, E: %d, D: %v, C: %v}",
		o.Start, o.End, o.Duration, o.CPoints,
	)
}

func (o *LayerZeroOpts) Clone() *LayerZeroOpts {
	return &LayerZeroOpts{
		LowerSelectSize: o.LowerSelectSize,
		UpperSelectSize: o.UpperSelectSize,
		LowerSelectRows: o.LowerSelectRows,
		UpperSelectRows: o.UpperSelectRows,
		Start:           o.Start,
		End:             o.End,
		Duration:        o.Duration,
		CPoints:         o.CPoints,
	}
}

func NewLayerZeroOpts() *LayerZeroOpts {
	return DefaultLayerZeroOpts.Clone()
}

func (o *LayerZeroOpts) CalcTolerance(lastMergeTimeAgo time.Duration) int {
	if lastMergeTimeAgo > TenYears {
		// avoid busy merge when the system is just started
		lastMergeTimeAgo = o.Duration * time.Duration(rand.Intn(9)+1) / 10
	}
	// If no merge has happened yet or duration is zero, return the start tolerance
	if lastMergeTimeAgo <= 0 || o.Duration <= 0 {
		return o.Start
	}
	// If more time has passed than the duration, use the end tolerance
	if lastMergeTimeAgo >= o.Duration {
		return o.End
	}
	// Calculate the linear progression between start and end based on elapsed time
	x := float64(lastMergeTimeAgo) / float64(o.Duration)

	t := findTForX(x, 0.0, o.CPoints[0], o.CPoints[2], 1.0)
	ratio := cubicBezier(t, 0.0, o.CPoints[1], o.CPoints[3], 1.0)

	tolerance := o.Start - int(float64(o.Start-o.End)*ratio)
	return tolerance
}

func (o *LayerZeroOpts) WithToleranceDegressionCurve(
	start, end int, duration time.Duration, cpoints [4]float64,
) *LayerZeroOpts {
	o.Start = start
	o.End = end
	o.Duration = duration
	o.CPoints = cpoints
	return o
}

func (o *LayerZeroOpts) WithLowerBoundSize(lowerBoundSize uint32) *LayerZeroOpts {
	o.LowerSelectSize = lowerBoundSize
	return o
}

func (o *LayerZeroOpts) WithUpperBoundSize(upperBoundSize uint32) *LayerZeroOpts {
	o.UpperSelectSize = upperBoundSize
	return o
}

func (o *LayerZeroOpts) WithUpperBoundRows(upperBoundRows uint32) *LayerZeroOpts {
	o.UpperSelectRows = upperBoundRows
	return o
}

func (o *LayerZeroOpts) WithLowerBoundRows(lowerBoundRows uint32) *LayerZeroOpts {
	o.LowerSelectRows = lowerBoundRows
	return o
}

type LayerZeroStats struct {
	Count     int
	AvgSize   int
	AvgRows   int
	HistoSize [8]int // <1MB, 1-2MB, 2-4MB, 4-8MB, 8-16MB, 16-32MB, 32-64MB, 64-128MB
	Tolerance int

	isValid func(stat *objectio.ObjectStats) bool
}

func (s *LayerZeroStats) String() string {
	return fmt.Sprintf(
		"Count: %d, AvgSize: %v, AvgRows: %d, OSizeDist: %v, Tolerance: %d",
		s.Count,
		common.HumanReadableBytes(s.AvgSize),
		s.AvgRows,
		s.HistoSize,
		s.Tolerance,
	)
}

// Upgrading notes: for object created before Date X,
// we don't pick it for zero layer merge.

// policy
// 1. if all zero layzer objects sum up to more than 128MB
// 2. zero layzer objects count tolerance is decreasing by time

func IsValidLayerZeroSmallObj(obj *objectio.ObjectStats, opts *LayerZeroOpts) bool {
	if obj.GetLevel() != 0 {
		return false
	}

	osize := obj.OriginSize()
	rows := obj.Rows()

	return osize >= opts.LowerSelectSize &&
		osize <= opts.UpperSelectSize &&
		rows >= opts.LowerSelectRows &&
		rows <= opts.UpperSelectRows
}

// sizeLevel determines the size level of an object based on its size in bytes
// Returns an index from 0 to 7 corresponding to the HistSize array:
// 0: <1MB, 1: 1MB-2MB, 2: 2MB-4MB, 3: 4MB-8MB,
// 4: 8MB-16MB, 5: 16MB-32MB, 6: 32MB-64MB, 7: >=64MB
func sizeLevel(sizeInBytes uint32, maxLevel int) int {
	sizeInMB := sizeInBytes >> 20
	lv := 32 - bits.LeadingZeros32(sizeInMB)
	if lv > maxLevel {
		return maxLevel
	}
	return lv
}

func CalculateLayerZeroStats(ctx context.Context,
	statsList []*objectio.ObjectStats,
	lastMergeTimeAgo time.Duration,
	opts *LayerZeroOpts,
) *LayerZeroStats {
	stats := &LayerZeroStats{
		isValid: func(stat *objectio.ObjectStats) bool {
			return IsValidLayerZeroSmallObj(stat, opts)
		},
		Tolerance: opts.CalcTolerance(lastMergeTimeAgo),
	}
	sumSize := 0
	sumRows := 0
	for _, stat := range statsList {
		if stats.isValid(stat) {
			stats.Count++
			sumSize += int(stat.OriginSize())
			stats.HistoSize[sizeLevel(stat.OriginSize(), len(stats.HistoSize)-1)]++
			sumRows += int(stat.Rows())
		}
	}
	if stats.Count > 0 {
		stats.AvgSize = sumSize / stats.Count
		stats.AvgRows = sumRows / stats.Count
	}
	return stats
}

func GatherLayerZeroMergeTasks(ctx context.Context,
	objList []*objectio.ObjectStats,
	lastMergeTimeAgo time.Duration,
	opts *LayerZeroOpts,
) []mergeTask {
	stats := CalculateLayerZeroStats(ctx, objList, lastMergeTimeAgo, opts)
	if stats.Count == 0 {
		return nil
	}

	if stats.Tolerance < stats.Count ||
		stats.AvgSize*stats.Count > common.DefaultMaxOsizeObjBytes {
		var note string
		if stats.Tolerance < stats.Count {
			note = fmt.Sprintf(NoteByTolerance, stats.Tolerance)
		} else {
			note = fmt.Sprintf(
				NoteBySize,
				common.HumanReadableBytes(stats.AvgSize*stats.Count),
			)
		}
		objs := make([]*objectio.ObjectStats, 0, stats.Count)
		for _, obj := range objList {
			if IsValidLayerZeroSmallObj(obj, opts) {
				objs = append(objs, obj)
			}
		}
		return []mergeTask{{objs: objs, note: note, kind: taskHostDN}}
	}

	return nil
}
