// Copyright 2026 Matrix Origin
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

package plan

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// encodedRegularIndexEstimate is private, attempt-local cost input. It neither
// replaces an executable expression nor changes the bound plan's statistics.
type encodedRegularIndexEstimate struct {
	blockInput              bool
	filter                  int32
	selectivity, outputRows float64
	fixedWork               float64
}

func (e encodedRegularIndexEstimate) valid(rows float64) bool {
	return finitePositive(rows) && finiteNonnegative(e.selectivity) && e.selectivity <= 1 &&
		finiteNonnegative(e.outputRows) && e.outputRows <= rows &&
		finiteNonnegative(e.fixedWork)
}

func finiteNonnegative(v float64) bool {
	return v >= 0 && !math.IsInf(v, 0) && !math.IsNaN(v)
}

// TemporalRangeStatistics contains only copied scalars from one existing
// column observation. It has no authority over execution rows or metadata.
type TemporalRangeStatistics = engine.TemporalStats

// estimateTemporalRange uses the ordinary uniform/NDV approximation. The
// inverse range is correct independently; this estimate can be wrong under
// skew or different block layouts and must never become an execution filter.
func estimateTemporalRange(s TemporalRangeStatistics, c types.TemporalCandidate, kind types.T) (float64, bool) {
	if s.Kind != kind || (kind != types.T_datetime && kind != types.T_timestamp) ||
		!finitePositive(s.Rows) || !finitePositive(s.NDV) || float64(s.Nulls) > s.Rows ||
		math.IsNaN(s.Min) || math.IsNaN(s.Max) || math.IsInf(s.Min, 0) || math.IsInf(s.Max, 0) || s.Min > s.Max ||
		c.Full || c.Empty || c.Lo > c.Hi {
		return 0, false
	}
	var spans [4][2]float64
	n := 0
	add := func(lo, hi int64) {
		a, b := max(s.Min, float64(lo)), min(s.Max, float64(hi))
		if a <= b {
			spans[n] = [2]float64{a, b}
			n++
		}
	}
	add(c.Lo, c.Hi)
	if kind == types.T_datetime {
		add(c.Sentinel, c.Sentinel)
		add(math.MinInt64, -2)
		add(int64(types.DatetimeFromClock(9999, 12, 31, 23, 59, 59, 999999))+1, math.MaxInt64)
	}
	if n == 0 {
		return 0, true
	}
	for i := 1; i < n; i++ {
		for j := i; j > 0 && spans[j][0] < spans[j-1][0]; j-- {
			spans[j], spans[j-1] = spans[j-1], spans[j]
		}
	}
	width, points := 0., 0
	lo, hi := spans[0][0], spans[0][1]
	finish := func() {
		width += hi - lo
		if hi == lo {
			points++
		}
	}
	for _, span := range spans[1:n] {
		if span[0] <= hi {
			hi = max(hi, span[1])
		} else {
			finish()
			lo, hi = span[0], span[1]
		}
	}
	finish()
	nonnull := (s.Rows - float64(s.Nulls)) / s.Rows
	if s.Min == s.Max {
		return nonnull, true
	}
	ndv := min(s.NDV, max(1, s.Rows-float64(s.Nulls)))
	fraction := max(1/ndv, width/(s.Max-s.Min)+float64(points)/ndv)
	return min(1, fraction) * nonnull, true
}

// This is the ordinary block-granular input estimate, not a physical bound.
// Row residual and output work continue to use their matching-row estimate.
func temporalCBOIndexInputRows(rows, candidateRows float64) float64 {
	return min(rows, max(candidateRows, float64(estimatedBlocks(candidateRows/float64(objectio.BlockMaxRows)))*float64(objectio.BlockMaxRows)))
}
