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

package objectio

import (
	"context"
	"slices"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

// ExactScanOp contains only total, same-type comparisons; NULL never passes WHERE.
type ExactScanOp uint8

const (
	ExactScanEQ ExactScanOp = iota
	ExactScanNE
	ExactScanLT
	ExactScanLE
	ExactScanGT
	ExactScanGE
)

// ExactScanPredicate uses the bound execution type, not the stored vector's
// historical width/scale. Value stores the exact scalar's physical bits.
type ExactScanPredicate struct {
	Column int
	Type   types.Type
	Op     ExactScanOp
	Value  uint64
}

// ExactScanFilter is immutable after admission and owns its scalar descriptors.
type ExactScanFilter struct{ predicates []ExactScanPredicate }

func NewExactScanFilter(predicates []ExactScanPredicate) *ExactScanFilter {
	if len(predicates) == 0 {
		return nil
	}
	for _, p := range predicates {
		if p.Column < 0 || p.Op > ExactScanGE {
			return nil
		}
		switch p.Type.Oid {
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_date, types.T_decimal64:
		default:
			return nil
		}
	}
	return &ExactScanFilter{predicates: slices.Clone(predicates)}
}

// ExactScanStats is metadata only. InputBytes describes visible fixed-width
// values, not allocated cache backing. FilterTime excludes I/O and output copies.
type ExactScanStats struct {
	Applied    bool
	InputRows  int
	InputBytes int64
	OutputRows int
	FilterTime time.Duration
}

// ExactScanState is owned by one sequential reader/filter execution. Bound views
// are cleared before every return; only row scratch and scalar predicates persist.
type ExactScanState struct {
	filter *ExactScanFilter
	views  []vector.Vector
	rows   []int64
}

func NewExactScanState(filter *ExactScanFilter) *ExactScanState {
	if filter == nil {
		return nil
	}
	return &ExactScanState{filter: filter}
}
func (s *ExactScanState) Close() {
	if s == nil {
		return
	}
	s.clearViews()
	if s.rows != nil {
		vector.PutSels(s.rows)
		s.rows = nil
	}
	s.views = nil
}
func (s *ExactScanState) clearViews() {
	for i := range s.views {
		s.views[i].Free(nil)
		s.views[i] = vector.Vector{}
	}
}

// MaterializeExactScan runs while the caller owns all entry leases. Source
// vectors never escape this package. An unsupported representation is copied
// with the original selection and Applied=false for the generic evaluator.
func MaterializeExactScan(ctx context.Context, entries []fileservice.IOEntry,
	typs []types.Type, destinations []*vector.Vector, inputRows []int64,
	state *ExactScanState, mp *mpool.MPool) (stats ExactScanStats, err error) {
	if state == nil || state.filter == nil || mp == nil || len(entries) == 0 || len(entries) != len(typs) || len(entries) != len(destinations) {
		return stats, moerr.NewInvalidInputNoCtx("invalid exact scan materialization request")
	}
	if len(state.views) < len(entries) {
		state.views = make([]vector.Vector, len(entries))
	}
	state.views = state.views[:len(entries)]
	defer state.clearViews()
	eligible := true
	n := -1
	for i := range entries {
		if destinations[i] == nil || destinations[i].GetType().Oid != typs[i].Oid {
			return stats, moerr.NewInvalidInputNoCtx("invalid exact scan destination type")
		}
		if !typs[i].IsFixedLen() {
			eligible = false
		}
		if err = bindCachedVectorForScope(&state.views[i], entries[i].CachedData); err != nil {
			return stats, err
		}
		v := &state.views[i]
		if v.GetType().Oid != typs[i].Oid {
			return stats, moerr.NewInvalidInputNoCtx("exact scan source type differs from destination")
		}
		if n < 0 {
			n = v.Length()
		} else if n != v.Length() {
			return stats, moerr.NewInvalidInputNoCtx("exact scan column lengths differ")
		}
		if v.HasGrouping() {
			eligible = false
		}
	}
	for _, row := range inputRows {
		if row < 0 || row >= int64(n) {
			return stats, moerr.NewInvalidInputNoCtx("invalid exact scan input selection")
		}
	}
	for _, p := range state.filter.predicates {
		if p.Column >= len(typs) || typs[p.Column].Oid != p.Type.Oid || typs[p.Column].Scale != p.Type.Scale {
			eligible = false
			break
		}
	}
	if n > int(BlockMaxRows) || len(inputRows) > int(BlockMaxRows) {
		eligible = false
	}
	stats.InputRows = n
	if inputRows != nil {
		stats.InputRows = len(inputRows)
	}
	for _, typ := range typs {
		stats.InputBytes += int64(typ.TypeSize()) * int64(stats.InputRows)
	}
	rows := inputRows
	if eligible {
		if state.rows == nil {
			state.rows = vector.GetSels()
		}
		if need := max(n, len(inputRows)); cap(state.rows) < need {
			state.rows = make([]int64, 0, need)
		}
		start := time.Now()
		stats.Applied = true
		for i := 0; i < len(state.filter.predicates); {
			p := state.filter.predicates[i]
			if err = ctx.Err(); err != nil {
				stats.FilterTime = time.Since(start)
				return stats, err
			}
			if rows != nil && len(rows) == 0 {
				break
			}
			if n == 0 {
				break
			}
			allBefore := rows == nil
			v := &state.views[p.Column]
			if i+1 < len(state.filter.predicates) && p.Op == ExactScanGE &&
				state.filter.predicates[i+1].Op == ExactScanLE &&
				state.filter.predicates[i+1].Column == p.Column &&
				rows == nil && !v.IsConst() && v.GetNulls().EmptyByFlag() {
				// BETWEEN needs one value pass and one selection write, rather
				// than gathering the lower-bound survivors for the upper bound.
				rows = state.selectBetween(p, state.filter.predicates[i+1], n)
				i += 2
			} else {
				rows = state.selectRows(p, rows, n)
				i++
			}
			if allBefore && len(rows) == n {
				rows = nil
			}
		}
		stats.FilterTime = time.Since(start)
		stats.Applied = true
	}
	stats.OutputRows = n
	if rows != nil {
		stats.OutputRows = len(rows)
	}
	for i := range destinations {
		if err = ctx.Err(); err != nil {
			return stats, err
		}
		if rows == nil {
			err = destinations[i].UnionBatch(&state.views[i], 0, n, nil, mp)
		} else {
			err = destinations[i].Union(&state.views[i], rows, mp)
		}
		if err != nil {
			return stats, err
		}
	}
	return stats, nil
}

func (s *ExactScanState) selectBetween(lower, upper ExactScanPredicate, n int) []int64 {
	v := &s.views[lower.Column]
	switch lower.Type.Oid {
	case types.T_int8:
		return selectExactBetween[int8, int8](s, v, lower.Value, upper.Value, n)
	case types.T_int16:
		return selectExactBetween[int16, int16](s, v, lower.Value, upper.Value, n)
	case types.T_int32:
		return selectExactBetween[int32, int32](s, v, lower.Value, upper.Value, n)
	case types.T_date:
		return selectExactBetween[types.Date, int32](s, v, lower.Value, upper.Value, n)
	case types.T_int64:
		return selectExactBetween[int64, int64](s, v, lower.Value, upper.Value, n)
	case types.T_decimal64:
		return selectExactBetween[types.Decimal64, int64](s, v, lower.Value, upper.Value, n)
	case types.T_uint8:
		return selectExactBetween[uint8, uint8](s, v, lower.Value, upper.Value, n)
	case types.T_uint16:
		return selectExactBetween[uint16, uint16](s, v, lower.Value, upper.Value, n)
	case types.T_uint32:
		return selectExactBetween[uint32, uint32](s, v, lower.Value, upper.Value, n)
	case types.T_uint64:
		return selectExactBetween[uint64, uint64](s, v, lower.Value, upper.Value, n)
	}
	panic(moerr.NewInternalErrorNoCtx("unadmitted exact scan range type"))
}

func selectExactBetween[T, C exactOrdered](s *ExactScanState, v *vector.Vector, lower, upper uint64, n int) []int64 {
	values := vector.MustFixedColNoTypeCheck[T](v)[:n]
	lo, hi := C(lower), C(upper)
	out := s.rows[:0]
	for row, value := range values {
		if lo <= C(value) && C(value) <= hi {
			out = append(out, int64(row))
		}
	}
	s.rows = out
	return out
}

func (s *ExactScanState) selectRows(p ExactScanPredicate, rows []int64, n int) []int64 {
	v := &s.views[p.Column]
	switch p.Type.Oid {
	case types.T_int8:
		return selectExactOrdered[int8, int8](s, v, p, rows, n)
	case types.T_int16:
		return selectExactOrdered[int16, int16](s, v, p, rows, n)
	case types.T_int32:
		return selectExactOrdered[int32, int32](s, v, p, rows, n)
	case types.T_date:
		return selectExactOrdered[types.Date, int32](s, v, p, rows, n)
	case types.T_int64:
		return selectExactOrdered[int64, int64](s, v, p, rows, n)
	case types.T_decimal64:
		// Keep the physical slice typed Decimal64; compare its signed coefficients.
		return selectExactOrdered[types.Decimal64, int64](s, v, p, rows, n)
	case types.T_uint8:
		return selectExactOrdered[uint8, uint8](s, v, p, rows, n)
	case types.T_uint16:
		return selectExactOrdered[uint16, uint16](s, v, p, rows, n)
	case types.T_uint32:
		return selectExactOrdered[uint32, uint32](s, v, p, rows, n)
	case types.T_uint64:
		return selectExactOrdered[uint64, uint64](s, v, p, rows, n)
	}
	panic(moerr.NewInternalErrorNoCtx("unadmitted exact scan type"))
}

type exactOrdered interface {
	~int8 | ~int16 | ~int32 | ~int64 | ~uint8 | ~uint16 | ~uint32 | ~uint64
}

func selectExactOrdered[T, C exactOrdered](s *ExactScanState, v *vector.Vector, p ExactScanPredicate, rows []int64, n int) []int64 {
	out := s.rows[:0]
	if v.IsConstNull() {
		return out
	}
	values := vector.MustFixedColNoTypeCheck[T](v)
	bound := C(p.Value)
	if v.IsConst() {
		value := C(values[0])
		keep := false
		switch p.Op {
		case ExactScanEQ:
			keep = value == bound
		case ExactScanNE:
			keep = value != bound
		case ExactScanLT:
			keep = value < bound
		case ExactScanLE:
			keep = value <= bound
		case ExactScanGT:
			keep = value > bound
		case ExactScanGE:
			keep = value >= bound
		}
		if keep {
			return rows
		}
		return out
	}
	count := n
	if rows != nil {
		count = len(rows)
	}
	nulls := v.GetNulls()
	hasNulls := !nulls.EmptyByFlag()
	switch p.Op {
	case ExactScanEQ:
		for i := 0; i < count; i++ {
			row := i
			if rows != nil {
				row = int(rows[i])
			}
			if hasNulls && nulls.Contains(uint64(row)) {
				continue
			}
			if C(values[row]) == bound {
				out = append(out, int64(row))
			}
		}
	case ExactScanNE:
		for i := 0; i < count; i++ {
			row := i
			if rows != nil {
				row = int(rows[i])
			}
			if hasNulls && nulls.Contains(uint64(row)) {
				continue
			}
			if C(values[row]) != bound {
				out = append(out, int64(row))
			}
		}
	case ExactScanLT:
		for i := 0; i < count; i++ {
			row := i
			if rows != nil {
				row = int(rows[i])
			}
			if hasNulls && nulls.Contains(uint64(row)) {
				continue
			}
			if C(values[row]) < bound {
				out = append(out, int64(row))
			}
		}
	case ExactScanLE:
		for i := 0; i < count; i++ {
			row := i
			if rows != nil {
				row = int(rows[i])
			}
			if hasNulls && nulls.Contains(uint64(row)) {
				continue
			}
			if C(values[row]) <= bound {
				out = append(out, int64(row))
			}
		}
	case ExactScanGT:
		for i := 0; i < count; i++ {
			row := i
			if rows != nil {
				row = int(rows[i])
			}
			if hasNulls && nulls.Contains(uint64(row)) {
				continue
			}
			if C(values[row]) > bound {
				out = append(out, int64(row))
			}
		}
	case ExactScanGE:
		for i := 0; i < count; i++ {
			row := i
			if rows != nil {
				row = int(rows[i])
			}
			if hasNulls && nulls.Contains(uint64(row)) {
				continue
			}
			if C(values[row]) >= bound {
				out = append(out, int64(row))
			}
		}
	}
	s.rows = out
	return out
}
