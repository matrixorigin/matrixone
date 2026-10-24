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

package aggexec

import (
	"context"
	"encoding/binary"
	"math"
	"math/big"
	"math/bits"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// quantileExactWeightedExec is the exact weighted percentile aggregate
// quantileExactWeighted(value, weight, level). Retained state keeps every
// surviving (value, weight) pair as typed rows in the generic saved-argument
// arena, so the framework's DISTINCT, merge, interim-wire and spill protocols
// round-trip the payload without aggregate-specific serialization (design
// decision D11). The level reaches the executor as configuration. Finalize
// restores one group's typed values while converting every stored weight
// exactly once in the same arena scan, sums with an overflow check, computes
// k = ceil(level * W) once with math/big and
// returns the smallest present value whose cumulative weight reaches k (no
// interpolation; the result type is the value's own type). The sort and the
// cumulative scan carry their own bounded cancellation checkpoints (decision
// D12), so cancelling a large group or window partition's finalization
// surfaces without running the sort or scan to completion.
type quantileExactWeightedExec struct {
	aggExec
	level      *big.Rat
	arithmetic percentileArithmeticScratch
}

const quantileExactWeightedConfigVersion byte = 1

// QuantileExactWeightedSupportedValueType admits the exact ordered-percentile
// numeric value domain: BIT, signed/unsigned integer widths, FLOAT32/64 and
// DECIMAL64/128, excluding DECIMAL256 and width-38 decimals like
// percentile_cont.
func QuantileExactWeightedSupportedValueType(typ types.Type) bool {
	switch typ.Oid {
	case types.T_bit, types.T_int8, types.T_int16, types.T_int32,
		types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32,
		types.T_uint64, types.T_float32, types.T_float64:
		return true
	case types.T_decimal64:
		return true
	case types.T_decimal128:
		return typ.Width < 38
	}
	return false
}

// QuantileExactWeightedSupportedWeightType admits exactly decision D4's typed
// weight set: signed/unsigned integer widths, BIT, FLOAT32/64 and
// DECIMAL64/128. DECIMAL256 is deliberately not a weight type.
func QuantileExactWeightedSupportedWeightType(typ types.Type) bool {
	switch typ.Oid {
	case types.T_bit, types.T_int8, types.T_int16, types.T_int32,
		types.T_int64, types.T_uint8, types.T_uint16, types.T_uint32,
		types.T_uint64, types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128:
		return true
	}
	return false
}

// QuantileExactWeightedReturnType reports the aggregate's result type: the
// value argument's own type, because member selection never interpolates.
func QuantileExactWeightedReturnType(args []types.Type) types.Type {
	if len(args) == 0 {
		return types.T_float64.ToType()
	}
	return args[0]
}

// EncodeQuantileExactWeightedConfig packs the validated level text produced by
// the compile-time percentile codec into the executor's versioned
// configuration. The payload keeps the exact rational text so finalize
// recomputes k = ceil(level * W) in arbitrary precision.
func EncodeQuantileExactWeightedConfig(level []byte) []byte {
	config := make([]byte, 0, 2+len(level))
	config = append(config, quantileExactWeightedConfigVersion, 0)
	config = append(config, level...)
	return config
}

func decodeQuantileExactWeightedConfig(partialResult any) (*big.Rat, error) {
	b, ok := partialResult.([]byte)
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx(
			"quantileExactWeighted: expected []byte level configuration")
	}
	if len(b) >= 2 && b[0] == quantileExactWeightedConfigVersion {
		if b[1] != 0 {
			return nil, moerr.NewInvalidInputNoCtx(
				"quantileExactWeighted: invalid configuration")
		}
		b = b[2:]
	}
	text := string(b)
	if text == "" {
		return nil, moerr.NewInvalidInputNoCtx(
			"quantileExactWeighted: level is empty")
	}
	level, ok := new(big.Rat).SetString(text)
	if !ok || level.Sign() < 0 || level.Cmp(big.NewRat(1, 1)) > 0 {
		return nil, moerr.NewInvalidInputNoCtxf(
			"quantileExactWeighted: level must be in [0,1], got %q", text)
	}
	if _, err := strconv.ParseFloat(text, 64); err != nil {
		return nil, moerr.NewInvalidInputNoCtxf(
			"quantileExactWeighted: invalid level %q", text)
	}
	return level, nil
}

func makeQuantileExactWeightedExec(
	mp *mpool.MPool, aggID int64, isDistinct bool, params []types.Type,
) (AggFuncExec, error) {
	if len(params) != 2 {
		return nil, moerr.NewInternalErrorNoCtx(
			"quantileExactWeighted requires a value and a weight argument")
	}
	if !QuantileExactWeightedSupportedValueType(params[0]) {
		return nil, moerr.NewInvalidInputNoCtxf(
			"quantileExactWeighted does not support %s values", params[0].Oid)
	}
	if !QuantileExactWeightedSupportedWeightType(params[1]) {
		return nil, moerr.NewInvalidInputNoCtxf(
			"quantileExactWeighted does not support %s weights", params[1].Oid)
	}
	exec := &quantileExactWeightedExec{}
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId:      aggID,
		isDistinct: isDistinct,
		argTypes:   append([]types.Type(nil), params...),
		retType:    params[0],
		emptyNull:  true,
		saveArg:    true,
	}
	return exec, nil
}

func (exec *quantileExactWeightedExec) SetExtraInformation(
	partialResult any, _ int,
) error {
	level, err := decodeQuantileExactWeightedConfig(partialResult)
	if err != nil {
		return err
	}
	exec.level = level
	return nil
}

func (exec *quantileExactWeightedExec) Fill(
	groupIndex int, row int, vectors []*vector.Vector,
) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *quantileExactWeightedExec) BulkFill(
	groupIndex int, vectors []*vector.Vector,
) error {
	if len(vectors) != 2 || vectors[0] == nil || vectors[1] == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	groups, err := makeAccountedScratch[uint64](
		exec.allocation, exec.mp, vectors[0].Length())
	if err != nil {
		return err
	}
	defer mpool.FreeSlice(exec.mp, groups)
	for i := range groups {
		groups[i] = uint64(groupIndex + 1)
	}
	return exec.BatchFill(0, groups, vectors)
}

func (exec *quantileExactWeightedExec) BatchFill(
	offset int, groups []uint64, vectors []*vector.Vector,
) error {
	if len(vectors) != 2 || vectors[0] == nil || vectors[1] == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	// Weights are validated in their own argument type before any conversion,
	// so an invalid row aborts the query at fill time (decision D4). The scan
	// alone performs no allocations and cannot perturb the preflight contract.
	if err := exec.validateWeightRows(offset, groups, vectors); err != nil {
		return err
	}
	// The generic saved-argument admission enforces the paired-NULL rule for
	// multi-argument rows and deduplicates exact typed pairs under DISTINCT.
	return exec.batchFillArgs(offset, groups, vectors, exec.isDistinct)
}

func (exec *quantileExactWeightedExec) validateWeightRows(
	offset int, groups []uint64, vectors []*vector.Vector,
) error {
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		logicalRow := offset + i
		valueRow, err := preflightPhysicalRow(vectors[0], logicalRow)
		if err != nil {
			return err
		}
		weightRow, err := preflightPhysicalRow(vectors[1], logicalRow)
		if err != nil {
			return err
		}
		if vectors[0].IsNull(uint64(valueRow)) ||
			vectors[1].IsNull(uint64(weightRow)) {
			continue
		}
		raw := vectors[1].GetRawBytesAt(weightRow)
		if _, err = quantileExactWeightedWeightBytes(
			exec.argTypes[1], raw); err != nil {
			return err
		}
	}
	return nil
}

func (exec *quantileExactWeightedExec) Merge(
	next AggFuncExec, groupIdx1, groupIdx2 int,
) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *quantileExactWeightedExec) BatchMerge(
	next AggFuncExec, offset int, groups []uint64,
) error {
	other, ok := next.(*quantileExactWeightedExec)
	if !ok || !exec.compatible(other) {
		return mpool.ErrAllocationAccountMismatch
	}
	return exec.batchMergeArgs(&other.aggExec, offset, groups, exec.isDistinct)
}

func (exec *quantileExactWeightedExec) PreflightBatchMerge(
	next AggFuncExec, offset int, groups []uint64,
) error {
	other, ok := next.(*quantileExactWeightedExec)
	if !ok || !exec.compatible(other) {
		return mpool.ErrAllocationAccountMismatch
	}
	return exec.aggExec.PreflightBatchMerge(other, offset, groups)
}

func (exec *quantileExactWeightedExec) compatible(
	other *quantileExactWeightedExec,
) bool {
	return other != nil && exec.aggId == other.aggId &&
		len(exec.argTypes) == 2 && len(other.argTypes) == 2 &&
		exec.argTypes[0].Eq(other.argTypes[0]) &&
		exec.argTypes[1].Eq(other.argTypes[1]) &&
		exec.retType.Eq(other.retType) &&
		exec.level != nil && other.level != nil &&
		exec.level.Cmp(other.level) == 0
}

func (exec *quantileExactWeightedExec) Flush() ([]*vector.Vector, error) {
	return exec.FlushWithContext(context.Background())
}

// FlushWithContext finalizes every group. SCRATCH is allocation-accounted and
// released on the success path and every error path; RESULT VECTORS are freed
// only on an error return through the framework's error-only cleanup, and a
// successful return transfers them to the caller (decision D12: the window
// and group-merge consumers retain, read, and later release returned output).
func (exec *quantileExactWeightedExec) FlushWithContext(
	ctx context.Context,
) (_ []*vector.Vector, retErr error) {
	if exec.level == nil {
		return nil, moerr.NewInternalErrorNoCtx(
			"quantileExactWeighted: level configuration is not set")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	results := make([]*vector.Vector, len(exec.state))
	defer freeAggregateResultsOnError(exec.mp, results, &retErr)
	for chunk := range exec.state {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		state := &exec.state[chunk]
		result, err := exec.newFinalVector(exec.retType)
		if err != nil {
			return nil, err
		}
		results[chunk] = result
		if err = result.PreExtend(int(state.length), exec.mp); err != nil {
			return nil, err
		}
		for row := uint16(0); row < uint16(state.length); row++ {
			if err := context.Cause(ctx); err != nil {
				return nil, err
			}
			if state.argCnt[row] == 0 {
				if err := vector.AppendNull(result, exec.mp); err != nil {
					return nil, err
				}
				continue
			}
			if err := exec.appendGroupResult(ctx, result, state, row); err != nil {
				return nil, err
			}
		}
	}
	return results, nil
}

// appendGroupResult finalizes one non-empty group and appends its selected
// value (or NULL) to the chunk's result vector. It owns the group's restored
// value rows and the converted-weight collapse scratch (both freed on every
// return); the completed result vector belongs to FlushWithContext's
// caller-owned contract.
func (exec *quantileExactWeightedExec) appendGroupResult(
	ctx context.Context, result *vector.Vector, state *aggState, row uint16,
) error {
	values, rowWeights, total, err := exec.restorePairs(ctx, state, row)
	if err != nil {
		return err
	}
	defer values.Free(exec.mp)
	defer mpool.FreeSlice(exec.mp, rowWeights)
	resultRow, err := exec.weightedResultRow(ctx, values, rowWeights, total)
	if err != nil {
		return err
	}
	if resultRow < 0 {
		return vector.AppendNull(result, exec.mp)
	}
	return result.UnionOne(values, resultRow, exec.mp)
}

// restorePairs materializes one group's typed values from the saved-argument
// arena and converts every retained weight exactly once during the same
// single scan, accumulating the group's exact total W. The typed weight
// COLUMN is never materialized a second time (decision D12's resource model):
// per-group temporary data is the restored value rows plus the 8-byte-per-
// row sort selectors and converted collapse weights, never a second copy of
// the pair payload. Cancellation is checked at a bounded row cadence so a
// cancelled context aborts the scan of a large group without finishing it.
func (exec *quantileExactWeightedExec) restorePairs(
	ctx context.Context, state *aggState, row uint16,
) (*vector.Vector, []uint64, uint64, error) {
	count := int(state.argCnt[row])
	values, err := exec.newFinalVector(exec.argTypes[0])
	if err != nil {
		return nil, nil, 0, err
	}
	rowWeights, err := makeAccountedScratch[uint64](
		exec.allocation, exec.mp, count)
	if err != nil {
		values.Free(exec.mp)
		return nil, nil, 0, err
	}
	freeBoth := func() {
		values.Free(exec.mp)
		mpool.FreeSlice(exec.mp, rowWeights)
	}
	if err = values.PreExtend(count, exec.mp); err != nil {
		freeBoth()
		return nil, nil, 0, err
	}
	values.SetLength(count)
	var total uint64
	index := 0
	err = state.iterWithValue(row, func(key, value []byte) error {
		if index&1023 == 0 {
			if err := context.Cause(ctx); err != nil {
				return err
			}
		}
		payload := aggPayloadFromKeyValue(&exec.aggInfo, key, value)
		valueBytes, weightBytes, err := parseQuantileExactWeightedPair(
			payload,
			exec.argTypes[0].TypeSize(),
			exec.argTypes[1].TypeSize(),
		)
		if err != nil {
			return err
		}
		if index >= count {
			return moerr.NewInternalErrorNoCtx(
				"quantileExactWeighted retained argument count mismatch")
		}
		if err := values.SetRawBytesAt(index, valueBytes, exec.mp); err != nil {
			return err
		}
		weight, err := quantileExactWeightedWeightBytes(exec.argTypes[1], weightBytes)
		if err != nil {
			return err
		}
		summed := total + weight
		if summed < total {
			return moerr.NewInvalidInputNoCtx(
				"quantileExactWeighted: total weight exceeds the uint64 range")
		}
		total = summed
		rowWeights[index] = weight
		index++
		return nil
	})
	if err == nil && index != count {
		err = moerr.NewInternalErrorNoCtx(
			"quantileExactWeighted retained argument count mismatch")
	}
	if err != nil {
		freeBoth()
		return nil, nil, 0, err
	}
	return values, rowWeights, total, nil
}

// weightedResultRow returns the physical row of the selected value, or -1
// when the group retained no pair. The sort renames the group's rows into
// ascending value order through quantileSortSelectors, whose heap steps carry
// the cancellation checkpoints decision D12 requires inside sorting, and the
// cumulative scan checks cancellation at the same bounded row cadence. It owns
// the allocation-accounted sort selector scratch and frees it on every
// return; the restored values and converted weights stay owned by the caller.
func (exec *quantileExactWeightedExec) weightedResultRow(
	ctx context.Context, values *vector.Vector,
	rowWeights []uint64, total uint64,
) (int64, error) {
	count := len(rowWeights)
	if count == 0 {
		return -1, nil
	}
	selectors, err := makeAccountedScratch[int64](
		exec.allocation, exec.mp, count)
	if err != nil {
		return 0, err
	}
	defer mpool.FreeSlice(exec.mp, selectors)
	for i := range selectors {
		selectors[i] = int64(i)
	}
	if err := quantileSortSelectors(ctx, values, selectors); err != nil {
		return 0, err
	}
	threshold, err := exec.weightedThreshold(total)
	if err != nil {
		return 0, err
	}
	var cumulative uint64
	for i := 0; i < count; i++ {
		if i&1023 == 0 {
			if err := context.Cause(ctx); err != nil {
				return 0, err
			}
		}
		cumulative += rowWeights[selectors[i]]
		if cumulative >= threshold {
			return selectors[i], nil
		}
	}
	return selectors[0], nil
}

// quantileValueLessFunc returns the ascending SQL-order comparison between
// two rows of the group's restored value vector: the same total order
// SortByVectors applies to these value types (integers by width, floats
// through the deterministic NaN/-0 total order, decimals through their own
// Lt), so a finalized group's order equals a plain SortByVectors result.
func quantileValueLessFunc(
	values *vector.Vector,
) func(i, j int64) bool {
	switch values.GetType().Oid {
	case types.T_bit:
		col := vector.MustFixedColNoTypeCheck[uint64](values)
		return func(i, j int64) bool { return col[i] < col[j] }
	case types.T_int8:
		col := vector.MustFixedColNoTypeCheck[int8](values)
		return func(i, j int64) bool { return col[i] < col[j] }
	case types.T_int16:
		col := vector.MustFixedColNoTypeCheck[int16](values)
		return func(i, j int64) bool { return col[i] < col[j] }
	case types.T_int32:
		col := vector.MustFixedColNoTypeCheck[int32](values)
		return func(i, j int64) bool { return col[i] < col[j] }
	case types.T_int64:
		col := vector.MustFixedColNoTypeCheck[int64](values)
		return func(i, j int64) bool { return col[i] < col[j] }
	case types.T_uint8:
		col := vector.MustFixedColNoTypeCheck[uint8](values)
		return func(i, j int64) bool { return col[i] < col[j] }
	case types.T_uint16:
		col := vector.MustFixedColNoTypeCheck[uint16](values)
		return func(i, j int64) bool { return col[i] < col[j] }
	case types.T_uint32:
		col := vector.MustFixedColNoTypeCheck[uint32](values)
		return func(i, j int64) bool { return col[i] < col[j] }
	case types.T_uint64:
		col := vector.MustFixedColNoTypeCheck[uint64](values)
		return func(i, j int64) bool { return col[i] < col[j] }
	case types.T_float32:
		col := vector.MustFixedColNoTypeCheck[float32](values)
		return func(i, j int64) bool {
			return types.Float32OrderAscCompare(col[i], col[j]) < 0
		}
	case types.T_float64:
		col := vector.MustFixedColNoTypeCheck[float64](values)
		return func(i, j int64) bool {
			return types.Float64OrderAscCompare(col[i], col[j]) < 0
		}
	case types.T_decimal64:
		col := vector.MustFixedColNoTypeCheck[types.Decimal64](values)
		return func(i, j int64) bool { return col[i].Lt(col[j]) }
	case types.T_decimal128:
		col := vector.MustFixedColNoTypeCheck[types.Decimal128](values)
		return func(i, j int64) bool { return col[i].Lt(col[j]) }
	}
	return nil
}

// quantileSortCheckSteps bounds the work performed between cancellation
// checks inside the sort, mirroring the per-1024-row cadence of the restore
// and cumulative scans.
const quantileSortCheckSteps = 1024

// quantileSortSelectors orders selectors ascending by the typed values they
// index, following pkg/sort's own heapsort shape (max-heap build, then root
// extraction). Every quantileSortCheckSteps elementary swaps it re-checks
// the context, so finalizing a large group or window partition surfaces an
// already-cancelled context at a bounded work cadence without running the
// sort to completion (decision D12's in-sort cancellation points), while the
// ordering stays identical to SortByVectors over the same single vector.
func quantileSortSelectors(
	ctx context.Context, values *vector.Vector, selectors []int64,
) error {
	count := len(selectors)
	if count < 2 {
		return nil
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	less := quantileValueLessFunc(values)
	if less == nil {
		return moerr.NewInternalErrorNoCtxf(
			"quantileExactWeighted cannot sort %s values", values.GetType().Oid)
	}
	var swapSteps int
	siftDown := func(root, end int) error {
		for {
			child := 2*root + 1
			if child >= end {
				return nil
			}
			// Advance to the greater child, exactly as pkg/sort's own
			// heapsort does, so the root carries the greatest value.
			if child+1 < end && less(selectors[child], selectors[child+1]) {
				child++
			}
			if !less(selectors[root], selectors[child]) {
				return nil
			}
			selectors[root], selectors[child] = selectors[child], selectors[root]
			root = child
			swapSteps++
			if swapSteps%quantileSortCheckSteps == 0 {
				if err := context.Cause(ctx); err != nil {
					return err
				}
			}
		}
	}
	// Build the max-heap (greatest value at the root), then extract the root
	// into the tail so the selectors converge to ascending value order.
	for i := (count - 1) / 2; i >= 0; i-- {
		if err := siftDown(i, count); err != nil {
			return err
		}
	}
	for i := count - 1; i > 0; i-- {
		selectors[0], selectors[i] = selectors[i], selectors[0]
		if err := siftDown(0, i); err != nil {
			return err
		}
	}
	return nil
}

// weightedThreshold computes k = ceil(level * W) exactly once per group with
// arbitrary-precision arithmetic. level <= 1 guarantees k <= W, so k always
// fits uint64 even for levels like 1e-100 whose rational denominator is huge.
func (exec *quantileExactWeightedExec) weightedThreshold(total uint64) (uint64, error) {
	if total == 0 {
		return 0, nil
	}
	scratch := &exec.arithmetic
	scratch.constants()
	scratch.rank.SetUint64(total)
	scratch.rank.Mul(&scratch.rank, exec.level.Num())
	scratch.quotient.QuoRem(&scratch.rank, exec.level.Denom(), &scratch.remainder)
	if scratch.remainder.Sign() != 0 {
		scratch.quotient.Add(&scratch.quotient, &scratch.one)
	}
	if !scratch.quotient.IsUint64() {
		return 0, moerr.NewInternalErrorNoCtx(
			"quantileExactWeighted: level threshold exceeds the total weight")
	}
	return scratch.quotient.Uint64(), nil
}

func (exec *quantileExactWeightedExec) newFinalVector(
	typ types.Type,
) (*vector.Vector, error) {
	if exec.allocation != nil {
		return exec.allocation.newVector(typ)
	}
	return vector.NewOffHeapVecWithType(typ), nil
}

// parseQuantileExactWeightedPair splits one retained multi-argument member,
// which the arena stores as length-delimited raw segments
// [len][value][len][weight] for both saved and canonical DISTINCT forms.
func parseQuantileExactWeightedPair(
	payload []byte, valueSize, weightSize int,
) ([]byte, []byte, error) {
	// The malformed-argument error is constructed only inside a failing
	// branch: building it up front would report an internal error for every
	// valid retained pair (moerr construction reports immediately).
	badRetainedPair := func() error {
		return moerr.NewInternalErrorNoCtx(
			"quantileExactWeighted has invalid retained arguments")
	}
	if len(payload) < 8 {
		return nil, nil, badRetainedPair()
	}
	valueLen := int(binary.BigEndian.Uint32(payload[:4]))
	if valueLen < 0 || 4+valueLen+4 > len(payload) || valueLen != valueSize {
		return nil, nil, badRetainedPair()
	}
	weightAt := 4 + valueLen
	weightLen := int(binary.BigEndian.Uint32(payload[weightAt : weightAt+4]))
	if weightLen < 0 || weightAt+4+weightLen > len(payload) ||
		weightLen != weightSize {
		return nil, nil, badRetainedPair()
	}
	return payload[4 : 4+valueLen], payload[weightAt+4 : weightAt+4+weightLen], nil
}

func (exec *quantileExactWeightedExec) Size() int64 {
	var size int64
	for _, state := range exec.state {
		size += int64(cap(state.argCnt))*4 +
			int64(cap(state.argbuf)) + int64(cap(state.argScratch))
	}
	for _, state := range exec.standby {
		size += int64(cap(state.argCnt))*4 +
			int64(cap(state.argbuf)) + int64(cap(state.argScratch))
	}
	return size
}

func invalidQuantileExactWeightedWeight(reason string) error {
	return moerr.NewInvalidInputNoCtxf(
		"quantileExactWeighted: weight must be %s", reason)
}

// quantileExactWeightedWeightBytes converts one stored weight, in its own
// argument type and without any preceding conversion, exactly to uint64.
// Non-negative integers, integral values of FLOAT32/64 within [0, 2^63] and
// integral non-negative DECIMAL64/128 values inside [0, MaxUint64] are
// accepted; everything else is an invalid input (decision D4).
func quantileExactWeightedWeightBytes(
	typ types.Type, raw []byte,
) (uint64, error) {
	switch typ.Oid {
	case types.T_bit:
		return types.DecodeUint64(raw), nil
	case types.T_uint8:
		return uint64(types.DecodeUint8(raw)), nil
	case types.T_uint16:
		return uint64(types.DecodeUint16(raw)), nil
	case types.T_uint32:
		return uint64(types.DecodeUint32(raw)), nil
	case types.T_uint64:
		return types.DecodeUint64(raw), nil
	case types.T_int8:
		v := types.DecodeInt8(raw)
		if v < 0 {
			return 0, invalidQuantileExactWeightedWeight("a non-negative integer")
		}
		return uint64(v), nil
	case types.T_int16:
		v := types.DecodeInt16(raw)
		if v < 0 {
			return 0, invalidQuantileExactWeightedWeight("a non-negative integer")
		}
		return uint64(v), nil
	case types.T_int32:
		v := types.DecodeInt32(raw)
		if v < 0 {
			return 0, invalidQuantileExactWeightedWeight("a non-negative integer")
		}
		return uint64(v), nil
	case types.T_int64:
		v := types.DecodeInt64(raw)
		if v < 0 {
			return 0, invalidQuantileExactWeightedWeight("a non-negative integer")
		}
		return uint64(v), nil
	case types.T_float32:
		return quantileExactWeightedFloatWeight(
			float64(types.DecodeFloat32(raw)))
	case types.T_float64:
		return quantileExactWeightedFloatWeight(types.DecodeFloat64(raw))
	case types.T_decimal64:
		weight := types.DecodeDecimal64(raw)
		if weight.Sign() {
			return 0, invalidQuantileExactWeightedWeight("a non-negative integer")
		}
		return quantileExactWeightedDecimal64Weight(uint64(weight), typ.Scale)
	case types.T_decimal128:
		weight := types.DecodeDecimal128(raw)
		if weight.Sign() {
			return 0, invalidQuantileExactWeightedWeight("a non-negative integer")
		}
		return quantileExactWeightedDecimal128Weight(
			weight.B0_63, weight.B64_127, typ.Scale)
	}
	return 0, invalidQuantileExactWeightedWeight("a supported numeric type")
}

func quantileExactWeightedFloatWeight(value float64) (uint64, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, invalidQuantileExactWeightedWeight("finite")
	}
	if value < 0 {
		return 0, invalidQuantileExactWeightedWeight("non-negative")
	}
	if value != math.Trunc(value) {
		return 0, invalidQuantileExactWeightedWeight("an integral value")
	}
	if value == 0 {
		return 0, nil
	}
	// Integral float64 values up to 2^63 convert exactly; above that the
	// implicit stored integer exceeds the exact conversion range.
	if value > float64(math.MaxInt64) {
		return 0, invalidQuantileExactWeightedWeight(
			"within the uint64 range")
	}
	return uint64(value), nil
}

func quantileExactWeightedDecimal64Weight(
	mantissa uint64, scale int32,
) (uint64, error) {
	if scale <= 0 {
		return mantissa, nil
	}
	if scale >= int32(len(types.Pow10)) {
		return 0, moerr.NewInternalErrorNoCtx(
			"quantileExactWeighted: decimal weight scale is out of range")
	}
	divisor := types.Pow10[scale]
	q, r := bits.Div64(0, mantissa, divisor)
	if r != 0 {
		return 0, invalidQuantileExactWeightedWeight("an integral value")
	}
	return q, nil
}

func quantileExactWeightedDecimal128Weight(
	lo, hi uint64, scale int32,
) (uint64, error) {
	if scale <= 0 {
		if hi != 0 {
			return 0, invalidQuantileExactWeightedWeight(
				"within the uint64 range")
		}
		return lo, nil
	}
	if scale >= int32(len(types.Pow10)) {
		// DECIMAL128 scales reach 38, beyond the uint64 divisor table; the
		// quotient and remainder come from exact big-integer division.
		mantissa := new(big.Int).SetUint64(lo)
		high := new(big.Int).SetUint64(hi)
		high.Lsh(high, 64)
		mantissa.Or(mantissa, high)
		divisor := new(big.Int).Exp(
			big.NewInt(10), big.NewInt(int64(scale)), nil)
		quotient, remainder := new(big.Int).QuoRem(
			mantissa, divisor, new(big.Int))
		if remainder.Sign() != 0 {
			return 0, invalidQuantileExactWeightedWeight("an integral value")
		}
		if !quotient.IsUint64() {
			return 0, invalidQuantileExactWeightedWeight(
				"within the uint64 range")
		}
		return quotient.Uint64(), nil
	}
	divisor := types.Pow10[scale]
	// 128-by-64 long division: hi/d then the low word with the carried
	// remainder; the row is integral only when the final remainder is zero.
	quotientHigh, remainder := bits.Div64(0, hi, divisor)
	quotient, rem := bits.Div64(remainder, lo, divisor)
	if rem != 0 {
		return 0, invalidQuantileExactWeightedWeight("an integral value")
	}
	if quotientHigh != 0 {
		return 0, invalidQuantileExactWeightedWeight(
			"within the uint64 range")
	}
	return quotient, nil
}
