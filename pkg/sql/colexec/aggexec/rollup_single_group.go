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

package aggexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"math"
)

// This optional capability belongs only to fixed-state executors whose result
// can be copied without transferring their live state. It is deliberately not
// part of AggFuncExec, nor implemented by the shared aggExec base.
type singleGroupReuse interface {
	singleGroupReuseState() (*aggExec, bool)
	appendSingleGroupResult(*vector.Vector) error
	bulkFillSingleGroupRange(int, int, []*vector.Vector) error
}

func reusableSingleGroup(agg AggFuncExec) (singleGroupReuse, *aggExec, bool) {
	reuse, ok := agg.(singleGroupReuse)
	if !ok {
		return nil, nil, false
	}
	state, ok := reuse.singleGroupReuseState()
	if !ok || state == nil || state.chunkSize != 1 || state.isDistinct || state.saveArg ||
		len(state.state) != 1 || len(state.standby) != 0 {
		return nil, nil, false
	}
	s := &state.state[0]
	if s.length != 1 || s.capacity != 1 || len(s.vecs) != len(state.stateTypes) ||
		!s.allocation.sameGeneration(state.allocation) {
		return nil, nil, false
	}
	for i, v := range s.vecs {
		if v == nil || v.IsConst() || v.Length() != 1 || v.Capacity() < AggBatchSize ||
			!v.GetType().Eq(state.stateTypes[i]) ||
			!vector.AllocationAccountSelectionsEqual(v.AllocationAccountSelection(), state.allocation.vectorSelection()) {
			return nil, nil, false
		}
	}
	return reuse, state, true
}

// SupportsSingleGroupReuse probes the entire initialized aggregate list. Callers
// must retain their ordinary execution path if any aggregate is unsupported.
func SupportsSingleGroupReuse(aggs []GroupAggFuncExec) bool {
	if len(aggs) == 0 {
		return false
	}
	for _, agg := range aggs {
		if _, _, ok := reusableSingleGroup(agg); !ok {
			return false
		}
	}
	return true
}

// AppendSingleGroupResultAndReset copies the completed value into dst, then
// resets the live state without allocation. A successful newly created vector
// belongs to the caller. On failure a new vector is freed here; an existing dst
// remains caller-owned. State is reset only after append succeeds. Callers must
// abort the whole output row on error, including any earlier successful columns.
func AppendSingleGroupResultAndReset(agg AggFuncExec, dst *vector.Vector) (*vector.Vector, error) {
	reuse, state, ok := reusableSingleGroup(agg)
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("aggregate does not support single-group reuse")
	}
	created := dst == nil
	if created {
		var err error
		dst, err = state.allocation.newVector(state.retType)
		if err != nil {
			return nil, err
		}
	} else {
		if dst.IsConst() || !dst.GetType().Eq(state.retType) ||
			!vector.AllocationAccountSelectionsEqual(dst.AllocationAccountSelection(), state.allocation.vectorSelection()) {
			return nil, moerr.NewInternalErrorNoCtx("invalid single-group result destination")
		}
		for _, v := range state.state[0].vecs {
			if dst == v {
				return nil, moerr.NewInternalErrorNoCtx("single-group result aliases live state")
			}
		}
	}
	if err := reuse.appendSingleGroupResult(dst); err != nil {
		if created {
			dst.Free(state.mp)
		}
		return nil, err
	}
	return dst, nil
}

// BulkFillSingleGroupRange borrows the original input for [offset, offset+rows).
// It preserves BulkFill's local accumulation and final merge order.
func BulkFillSingleGroupRange(agg AggFuncExec, offset, rows int, vectors []*vector.Vector) error {
	reuse, state, ok := reusableSingleGroup(agg)
	if !ok {
		return moerr.NewInternalErrorNoCtx("aggregate does not support single-group reuse")
	}
	if len(vectors) != 1 || vectors[0] == nil || offset < 0 || rows < 0 ||
		offset > math.MaxInt-rows || !vectors[0].CoversLogicalRows(offset, rows) {
		return moerr.NewInternalErrorNoCtx("invalid single-group input range")
	}
	if _, star := agg.(*countStarExec); !star &&
		(len(state.argTypes) != 1 || !vectors[0].GetType().Eq(state.argTypes[0])) {
		return moerr.NewInternalErrorNoCtx("invalid single-group input type")
	}
	return reuse.bulkFillSingleGroupRange(offset, rows, vectors)
}

func (exec *countStarExec) singleGroupReuseState() (*aggExec, bool) {
	if exec == nil {
		return nil, false
	}
	return &exec.aggExec, exec.extra == 0 && len(exec.argTypes) <= 1
}

func (exec *countColumnExec) singleGroupReuseState() (*aggExec, bool) {
	if exec == nil {
		return nil, false
	}
	return &exec.aggExec, exec.extra == 0 && len(exec.argTypes) == 1 && len(exec.distinctContributions) == 0
}

func appendSingleGroupCount(exec *aggExec, dst *vector.Vector) error {
	counts := vector.MustFixedColNoTypeCheck[int64](exec.state[0].vecs[0])
	if err := vector.AppendFixed(dst, counts[0], false, exec.mp); err != nil {
		return err
	}
	counts[0] = 0
	return nil
}

func (exec *countStarExec) appendSingleGroupResult(dst *vector.Vector) error {
	return appendSingleGroupCount(&exec.aggExec, dst)
}

func (exec *countColumnExec) appendSingleGroupResult(dst *vector.Vector) error {
	return appendSingleGroupCount(&exec.aggExec, dst)
}

func (exec *countStarExec) bulkFillSingleGroupRange(_ int, rows int, _ []*vector.Vector) error {
	vector.MustFixedColNoTypeCheck[int64](exec.state[0].vecs[0])[0] += int64(rows)
	return nil
}

func (exec *countColumnExec) bulkFillSingleGroupRange(offset, rows int, vectors []*vector.Vector) error {
	vec := vectors[0]
	count := int64(rows)
	if vec.HasNull() {
		for i := 0; i < rows; i++ {
			row := offset + i
			if vec.IsConst() {
				row = 0
			}
			if vec.IsNull(uint64(row)) {
				count--
			}
		}
	}
	vector.MustFixedColNoTypeCheck[int64](exec.state[0].vecs[0])[0] += count
	return nil
}

func (exec *sumAvgExec[T, A]) singleGroupReuseState() (*aggExec, bool) {
	if exec == nil {
		return nil, false
	}
	return &exec.aggExec, true
}

func (exec *sumAvgExec[T, A]) bulkFillSingleGroupRange(offset, rows int, vectors []*vector.Vector) error {
	if exec.isSum {
		return exec.bulkFillSumRange(0, offset, rows, vectors)
	}
	return exec.bulkFillAvgRange(0, offset, rows, vectors)
}

func (exec *sumAvgExec[T, A]) appendSingleGroupResult(dst *vector.Vector) error {
	sumVec := exec.state[0].vecs[0]
	sums := vector.MustFixedColNoTypeCheck[T](sumVec)
	var err error
	if exec.isSum {
		err = vector.AppendFixed(dst, sums[0], sumVec.IsNull(0), exec.mp)
	} else {
		count := vector.MustFixedColNoTypeCheck[int64](exec.state[0].vecs[1])[0]
		if count == 0 {
			err = vector.AppendNull(dst, exec.mp)
		} else if !exec.exactAvg {
			err = vector.AppendFixed(dst, float64(sums[0])/float64(count), false, exec.mp)
		} else if exec.retType.Oid == types.T_decimal256 {
			var avg types.Decimal256
			avg, err = decAvg(decimal256FromNativeSum(sums[0]), count, 0, exec.retType)
			if err == nil {
				err = appendNativeAvgResult(dst, avg, exec.mp)
			}
		} else {
			var avg types.Decimal128
			avg, err = nativeIntegerAvg128(sums[0], count, exec.retType)
			if err == nil {
				err = appendNativeAvgResult(dst, avg, exec.mp)
			}
		}
	}
	if err != nil {
		return err
	}
	sums[0] = 0
	if exec.isSum {
		// GroupGrow already reserved this NULL bit; this does not allocate.
		sumVec.SetNull(0)
	} else {
		vector.MustFixedColNoTypeCheck[int64](exec.state[0].vecs[1])[0] = 0
	}
	exec.windowNonNullCount = 0
	return nil
}
