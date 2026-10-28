// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package window

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// localRankIndex owns the compressed ranks of [start,horizon) within a
// materialized partition [start,end). Rank zero denotes a NULL or NaN,
// neither of which compares less than any input in the scalar comparison.
type localRankIndex struct {
	start, end, next int
	horizon          int
	width            uint64 // zero means the cumulative prefix
	ranks            []int
	tree             []uint64
	charge           uint64
	fallback         bool
}

func (ctr *container) fallbackLocalRankIndex(start, end int, width uint64) {
	ctr.freeLocalRankIndex()
	ctr.localRank = &localRankIndex{start: start, end: end, width: width, fallback: true}
}

// Rank results are fresh fixed-width vectors: a failed append has not
// published a row. Optional index memory must yield before output is rejected.
func (ctr *container) appendLocalRankResult(proc *process.Process, result *vector.Vector, value uint64, isNull bool) error {
	err := vector.AppendFixed(result, value, isNull, proc.Mp())
	if err == nil || ctr.localRank == nil || ctr.localRank.charge == 0 {
		return err
	}
	if cancel := proc.Ctx.Err(); cancel != nil {
		return cancel
	}
	if !mpool.IsRetryableAllocationCapacity(err) {
		return err
	}
	ctr.fallbackLocalRankIndex(ctr.localRank.start, ctr.localRank.end, ctr.localRank.width)
	return vector.AppendFixed(result, value, isNull, proc.Mp())
}

func (ctr *container) freeLocalRankIndex() {
	if ctr.localRank != nil {
		charge := ctr.localRank.charge
		ctr.localRank = nil
		ctr.allocation.ReleaseMetadata(charge)
	}
}

func localRankNaN(v *vector.Vector, row int) bool {
	row = localRankRow(v, row)
	switch v.GetType().Oid {
	case types.T_float32:
		return math.IsNaN(float64(vector.MustFixedColNoTypeCheck[float32](v)[row]))
	case types.T_float64:
		return math.IsNaN(vector.MustFixedColNoTypeCheck[float64](v)[row])
	default:
		return false
	}
}

// rankIndex returns nil when a partition should use the original scan. A
// rejected capacity reservation is cached for the partition, so it is not
// retried for every row. Other errors retain their original meaning.
func (ctr *container) rankIndex(proc *process.Process, v *vector.Vector, start, end, row int, width uint64, outputEnd int) (*localRankIndex, error) {
	horizon := min(end, outputEnd)
	s := ctr.localRank
	if s != nil && (s.start != start || s.end != end || s.width != width || (!s.fallback && s.next != row)) {
		ctr.freeLocalRankIndex()
		s = nil
	}
	if s != nil {
		if s.fallback {
			return nil, nil
		}
		if row < s.horizon {
			if s.ranks != nil {
				return s, nil
			}
			s.next = row + 1
			return nil, nil
		}
		// Geometric growth amortizes rebuilding while limiting construction to
		// at most twice the demanded prefix. Release old storage before admission.
		horizon = max(horizon, s.horizon+min(end-s.horizon, s.horizon-start))
		ctr.freeLocalRankIndex()
		s = nil
	} else if row != start {
		ctr.fallbackLocalRankIndex(start, end, width)
		return nil, nil
	}
	// Short initial fragments may be followed by enough rows to amortize an
	// index. Only permanently small partitions/widths stay on the scan path.
	if end-start < 128 || (width > 0 && width < 128) {
		ctr.fallbackLocalRankIndex(start, end, width)
		return nil, nil
	}
	if horizon-start < 128 {
		ctr.localRank = &localRankIndex{start: start, end: end, next: row + 1, horizon: horizon, width: width}
		return nil, nil
	}
	// Inspect only the demanded prefix. An all-NULL prefix can be followed by
	// values, so reconsider it at its horizon instead of disabling the index.
	if localRankValueIsNull(v, start) {
		allNull := v.IsConstNull()
		if !v.IsConst() {
			allNull = true
			for pos := start; pos < horizon; {
				if err := checkCanceled(proc, pos-start); err != nil {
					return nil, err
				}
				next := pos + min(65536, horizon-pos)
				if v.GetNulls().CountRange(uint64(pos), uint64(next)) != next-pos {
					allNull = false
					break
				}
				pos = next
			}
		}
		if allNull {
			ctr.localRank = &localRankIndex{start: start, end: end, next: row + 1, horizon: horizon,
				width: width, fallback: v.IsConstNull() || horizon == end}
			return nil, nil
		}
	}
	n := horizon - start
	// Two int arrays and a uint64 tree coexist during construction. The extra
	// tree element and all multiplication are checked before admission.
	if uint64(n) > (math.MaxUint64-8)/24 {
		ctr.fallbackLocalRankIndex(start, end, width)
		return nil, nil
	}
	charge := uint64(n)*24 + 8
	if err := ctr.allocation.ReserveMetadata(proc, charge); err != nil {
		if cancel := proc.Ctx.Err(); cancel != nil {
			return nil, cancel
		}
		if mpool.IsRetryableAllocationCapacity(err) {
			ctr.fallbackLocalRankIndex(start, end, width)
			return nil, nil
		}
		return nil, err
	}
	s = &localRankIndex{start: start, end: end, next: row, horizon: horizon, width: width, charge: charge}
	// Publish ownership before construction, so every error releases the charge.
	ctr.localRank = s
	ok := false
	defer func() {
		if !ok {
			ctr.freeLocalRankIndex()
		}
	}()
	order := make([]int, 0, n)
	s.ranks = make([]int, n)
	s.tree = make([]uint64, n+1)
	for i := 0; i < n; i++ {
		if err := checkCanceled(proc, i); err != nil {
			return nil, err
		}
		if !localRankValueIsNull(v, start+i) && !localRankNaN(v, start+i) {
			order = append(order, start+i)
		}
	}
	if err := sortRankRows(proc, v, order); err != nil {
		return nil, err
	}
	rank := 0
	for i, row := range order {
		if err := checkCanceled(proc, i); err != nil {
			return nil, err
		}
		if i == 0 {
			rank++
		} else {
			less, err := localRankValueLess(v, order[i-1], row)
			if err != nil {
				return nil, err
			}
			if less {
				rank++
			}
		}
		s.ranks[row-start] = rank
	}
	if err := s.seed(proc, row); err != nil {
		return nil, err
	}
	// The sort scratch no longer belongs to the index; keep its charge until the
	// partition is released, avoiding under-accounting while the backing is live.
	ok = true
	return s, nil
}

func (s *localRankIndex) seed(proc *process.Process, row int) error {
	if row == s.start {
		return nil
	}
	// A rebuilt tree includes the already consumed active physical rows.
	// Include row-width: advance adds the current row and then evicts that row.
	seedStart := s.start
	if s.width > 0 && uint64(row-s.start) > s.width {
		seedStart = row - int(s.width)
	}
	for i := seedStart; i < row; i++ {
		if err := checkCanceled(proc, i-seedStart); err != nil {
			return err
		}
		if rank := s.ranks[i-s.start]; rank != 0 {
			s.tree[rank]++
		}
	}
	// Convert rank frequencies to a Fenwick tree in linear time.
	for i := 1; i < len(s.tree); i++ {
		if err := checkCanceled(proc, i); err != nil {
			return err
		}
		if parent := i + (i & -i); parent < len(s.tree) {
			s.tree[parent] += s.tree[i]
		}
	}
	return nil
}

// sortRankRows uses an in-place heap sort with cancellation checks on every
// bounded batch of heap operations, including equal-value comparisons.
func sortRankRows(proc *process.Process, v *vector.Vector, rows []int) error {
	steps := 0
	check := func() error { steps++; return checkCanceled(proc, steps) }
	sift := func(root, end int) error {
		for {
			if err := check(); err != nil {
				return err
			}
			child := root*2 + 1
			if child >= end {
				return nil
			}
			if child+1 < end {
				less, err := localRankValueLess(v, rows[child], rows[child+1])
				if err != nil {
					return err
				}
				if less {
					child++
				}
			}
			less, err := localRankValueLess(v, rows[root], rows[child])
			if err != nil {
				return err
			}
			if !less {
				return nil
			}
			rows[root], rows[child] = rows[child], rows[root]
			root = child
		}
	}
	for i := len(rows)/2 - 1; i >= 0; i-- {
		if err := sift(i, len(rows)); err != nil {
			return err
		}
	}
	for i := len(rows) - 1; i > 0; i-- {
		if err := check(); err != nil {
			return err
		}
		rows[0], rows[i] = rows[i], rows[0]
		if err := sift(0, i); err != nil {
			return err
		}
	}
	return nil
}

func (s *localRankIndex) add(rank int, remove bool) {
	for i := rank; i > 0 && i < len(s.tree); i += i & -i {
		if remove {
			s.tree[i]--
		} else {
			s.tree[i]++
		}
	}
}

func (s *localRankIndex) advance(row int) uint64 {
	rank := s.ranks[row-s.start]
	s.add(rank, false)
	if s.width > 0 && uint64(row-s.start) >= s.width {
		s.add(s.ranks[row-s.start-int(s.width)], true)
	}
	s.next = row + 1
	if rank == 0 {
		return 0
	}
	var less uint64
	for i := rank - 1; i > 0; i -= i & -i {
		less += s.tree[i]
	}
	return less
}
