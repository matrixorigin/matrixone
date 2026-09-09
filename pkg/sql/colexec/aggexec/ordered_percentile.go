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
	"bufio"
	"cmp"
	"container/heap"
	"context"
	"io"
	"math/big"
	"os"
	"slices"
	"sort"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mosort "github.com/matrixorigin/matrixone/pkg/sort"
)

const orderedPercentileConfigVersion byte = 1
const (
	orderedPercentileMaxRunSize = int64(8 << 20)
	orderedPercentileMinRunSize = int64(64 << 10)
	orderedPercentileRunFanIn   = 64
)

// EncodeOrderedPercentileConfig stores the direction and validated percentile
// text in the aggregate extra configuration. The value argument itself stays
// in the executor argument list; only the direct percentile argument is
// removed by compile-time aggregate configuration construction.
func EncodeOrderedPercentileConfig(percentile []byte, descending bool) []byte {
	config := make([]byte, 2+len(percentile))
	config[0] = orderedPercentileConfigVersion
	if descending {
		config[1] = 1
	}
	copy(config[2:], percentile)
	return config
}

func ConfigureOrderedPercentileSpill(
	agg AggFuncExec,
	limit int64,
	ctx context.Context,
	createFile func() (*os.File, error),
	report func(int64, int64, int64),
) {
	if ctx == nil {
		ctx = context.Background()
	}
	if limit > orderedPercentileMaxRunSize {
		limit = orderedPercentileMaxRunSize
	}
	if limit < orderedPercentileMinRunSize {
		limit = orderedPercentileMinRunSize
	}
	switch exec := agg.(type) {
	case *orderedPercentileExec[int8, float64]:
		configureOrderedPercentileExecSpill(exec, limit, ctx, createFile, report)
	case *orderedPercentileExec[int16, float64]:
		configureOrderedPercentileExecSpill(exec, limit, ctx, createFile, report)
	case *orderedPercentileExec[int32, float64]:
		configureOrderedPercentileExecSpill(exec, limit, ctx, createFile, report)
	case *orderedPercentileExec[int64, float64]:
		configureOrderedPercentileExecSpill(exec, limit, ctx, createFile, report)
	case *orderedPercentileExec[uint8, float64]:
		configureOrderedPercentileExecSpill(exec, limit, ctx, createFile, report)
	case *orderedPercentileExec[uint16, float64]:
		configureOrderedPercentileExecSpill(exec, limit, ctx, createFile, report)
	case *orderedPercentileExec[uint32, float64]:
		configureOrderedPercentileExecSpill(exec, limit, ctx, createFile, report)
	case *orderedPercentileExec[uint64, float64]:
		configureOrderedPercentileExecSpill(exec, limit, ctx, createFile, report)
	case *orderedPercentileExec[float32, float64]:
		configureOrderedPercentileExecSpill(exec, limit, ctx, createFile, report)
	case *orderedPercentileExec[float64, float64]:
		configureOrderedPercentileExecSpill(exec, limit, ctx, createFile, report)
	case *orderedPercentileExec[types.Decimal64, types.Decimal128]:
		configureOrderedPercentileExecSpill(exec, limit, ctx, createFile, report)
	case *orderedPercentileExec[types.Decimal128, types.Decimal128]:
		configureOrderedPercentileExecSpill(exec, limit, ctx, createFile, report)
	case *orderedPercentileExec[int8, int8]:
		configureOrderedPercentileExecSpill(exec, limit, ctx, createFile, report)
	case *orderedPercentileExec[int16, int16]:
		configureOrderedPercentileExecSpill(exec, limit, ctx, createFile, report)
	case *orderedPercentileExec[int32, int32]:
		configureOrderedPercentileExecSpill(exec, limit, ctx, createFile, report)
	case *orderedPercentileExec[int64, int64]:
		configureOrderedPercentileExecSpill(exec, limit, ctx, createFile, report)
	case *orderedPercentileExec[uint8, uint8]:
		configureOrderedPercentileExecSpill(exec, limit, ctx, createFile, report)
	case *orderedPercentileExec[uint16, uint16]:
		configureOrderedPercentileExecSpill(exec, limit, ctx, createFile, report)
	case *orderedPercentileExec[uint32, uint32]:
		configureOrderedPercentileExecSpill(exec, limit, ctx, createFile, report)
	case *orderedPercentileExec[uint64, uint64]:
		configureOrderedPercentileExecSpill(exec, limit, ctx, createFile, report)
	case *orderedPercentileExec[float32, float32]:
		configureOrderedPercentileExecSpill(exec, limit, ctx, createFile, report)
	case *orderedPercentileExec[types.Decimal64, types.Decimal64]:
		configureOrderedPercentileExecSpill(exec, limit, ctx, createFile, report)
	}
}

func configureOrderedPercentileExecSpill[T numeric | types.Decimal64 | types.Decimal128, R types.FixedSizeTExceptStrType](
	exec *orderedPercentileExec[T, R],
	limit int64,
	ctx context.Context,
	createFile func() (*os.File, error),
	report func(int64, int64, int64),
) {
	exec.spillLimit = limit
	exec.spillContext = ctx
	exec.spillFile = createFile
	exec.spillReport = report
}

func PercentileContReturnType(args []types.Type) types.Type {
	if len(args) == 0 {
		return types.T_float64.ToType()
	}
	if args[0].IsDecimal() {
		scale := args[0].Scale
		if args[0].Width < 38 {
			scale++
		}
		if scale > 38 {
			scale = 38
		}
		return types.New(types.T_decimal128, 38, scale)
	}
	return types.T_float64.ToType()
}

func PercentileDiscReturnType(args []types.Type) types.Type {
	if len(args) == 0 {
		return types.T_float64.ToType()
	}
	return args[0]
}

// PercentileDiscSupportedType reports whether the engine has a SQL ordering
// implementation for a type. Unlike PERCENTILE_CONT, PERCENTILE_DISC returns
// one of its input values and therefore does not require numeric interpolation.
func PercentileDiscSupportedType(typ types.T) bool {
	return mosort.IsSupportedType(typ)
}

type orderedPercentileMode uint8

const (
	orderedPercentileContinuous orderedPercentileMode = iota
	orderedPercentileDiscrete
)

type orderedPercentileExec[T numeric | types.Decimal64 | types.Decimal128, R types.FixedSizeTExceptStrType] struct {
	medianColumnExecSelf[T, R]
	mode       orderedPercentileMode
	percentile *big.Rat
	descending bool
	arithmetic percentileArithmeticScratch

	spillLimit   int64
	spillContext context.Context
	spillFile    func() (*os.File, error)
	spillReport  func(int64, int64, int64)
	spillData    *os.File
	spillRuns    [][]orderedPercentileRun
}

type orderedPercentileRun struct {
	start int64
	end   int64
	pos   int64
	rows  uint64
	level uint8
}

// orderedPercentileDiscreteExec retains values in the generic aggregate
// argument arena. That representation supports both fixed and variable-width
// SQL-orderable types and participates in Group's allocation accounting and
// spill protocol. Numeric discrete percentiles keep using orderedPercentileExec
// below so their standalone external-run spill path remains unchanged.
type orderedPercentileDiscreteExec struct {
	aggExec
	percentile *big.Rat
	descending bool
	arithmetic percentileArithmeticScratch
}

func newOrderedPercentileDiscreteExec(
	mp *mpool.MPool, aggID int64, param types.Type,
) *orderedPercentileDiscreteExec {
	exec := &orderedPercentileDiscreteExec{}
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId:     aggID,
		argTypes:  []types.Type{param},
		retType:   param,
		emptyNull: true,
		saveArg:   true,
	}
	return exec
}

func (exec *orderedPercentileDiscreteExec) SetExtraInformation(
	partialResult any, _ int,
) error {
	percentile, descending, err := decodeOrderedPercentileConfig(partialResult)
	if err != nil {
		return err
	}
	exec.percentile = percentile
	exec.descending = descending
	return nil
}

func (exec *orderedPercentileDiscreteExec) Fill(
	groupIndex int, row int, vectors []*vector.Vector,
) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *orderedPercentileDiscreteExec) BulkFill(
	groupIndex int, vectors []*vector.Vector,
) error {
	if len(vectors) != 1 || vectors[0] == nil {
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

func (exec *orderedPercentileDiscreteExec) BatchFill(
	offset int, groups []uint64, vectors []*vector.Vector,
) error {
	return exec.batchFillArgs(offset, groups, vectors, false)
}

func (exec *orderedPercentileDiscreteExec) Merge(
	next AggFuncExec, groupIdx1, groupIdx2 int,
) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *orderedPercentileDiscreteExec) BatchMerge(
	next AggFuncExec, offset int, groups []uint64,
) error {
	other, ok := next.(*orderedPercentileDiscreteExec)
	if !ok || other == nil || !exec.compatible(other) {
		return mpool.ErrAllocationAccountMismatch
	}
	return exec.batchMergeArgs(&other.aggExec, offset, groups, false)
}

func (exec *orderedPercentileDiscreteExec) PreflightBatchMerge(
	next AggFuncExec, offset int, groups []uint64,
) error {
	other, ok := next.(*orderedPercentileDiscreteExec)
	if !ok || other == nil || !exec.compatible(other) {
		return mpool.ErrAllocationAccountMismatch
	}
	return exec.aggExec.PreflightBatchMerge(other, offset, groups)
}

func (exec *orderedPercentileDiscreteExec) compatible(
	other *orderedPercentileDiscreteExec,
) bool {
	return other != nil && exec.aggId == other.aggId &&
		len(exec.argTypes) == 1 && len(other.argTypes) == 1 &&
		exec.argTypes[0].Eq(other.argTypes[0]) && exec.retType.Eq(other.retType) &&
		exec.percentile != nil && other.percentile != nil &&
		exec.percentile.Cmp(other.percentile) == 0 &&
		exec.descending == other.descending
}

func (exec *orderedPercentileDiscreteExec) Flush() ([]*vector.Vector, error) {
	return exec.FlushWithContext(context.Background())
}

func (exec *orderedPercentileDiscreteExec) FlushWithContext(
	ctx context.Context,
) (_ []*vector.Vector, retErr error) {
	if exec.percentile == nil {
		return nil, moerr.NewInternalErrorNoCtx(
			"ordered percentile: percentile configuration is not set")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	results := make([]*vector.Vector, len(exec.state))
	defer freeAggregateResultsOnError(exec.mp, results, &retErr)
	for chunk := range exec.state {
		state := &exec.state[chunk]
		result, err := exec.newDiscreteVector(exec.retType)
		if err != nil {
			return nil, err
		}
		results[chunk] = result
		if err = result.PreExtend(int(state.length), exec.mp); err != nil {
			return nil, err
		}
		for row := uint16(0); row < uint16(state.length); row++ {
			if err = context.Cause(ctx); err != nil {
				return nil, err
			}
			if state.argCnt[row] == 0 {
				if err = vector.AppendNull(result, exec.mp); err != nil {
					return nil, err
				}
				continue
			}
			values, err := exec.restoreDiscreteValues(ctx, state, row)
			if err != nil {
				return nil, err
			}
			selectors, err := makeAccountedScratch[int64](
				exec.allocation, exec.mp, values.Length())
			if err != nil {
				values.Free(exec.mp)
				return nil, err
			}
			for i := range selectors {
				selectors[i] = int64(i)
			}
			mosort.SortByVectors(
				selectors,
				[]*vector.Vector{values},
				[]bool{exec.descending},
				[]bool{false},
			)
			rank := exec.arithmetic.discreteRank(
				uint64(len(selectors)), exec.percentile)
			err = result.UnionOne(values, selectors[rank], exec.mp)
			mpool.FreeSlice(exec.mp, selectors)
			values.Free(exec.mp)
			if err != nil {
				return nil, err
			}
		}
	}
	return results, nil
}

func (exec *orderedPercentileDiscreteExec) newDiscreteVector(
	typ types.Type,
) (*vector.Vector, error) {
	if exec.allocation != nil {
		return exec.allocation.newVector(typ)
	}
	return vector.NewOffHeapVecWithType(typ), nil
}

func (exec *orderedPercentileDiscreteExec) restoreDiscreteValues(
	ctx context.Context, state *aggState, row uint16,
) (*vector.Vector, error) {
	values, err := exec.newDiscreteVector(exec.argTypes[0])
	if err != nil {
		return nil, err
	}
	count := int(state.argCnt[row])
	if err = values.PreExtend(count, exec.mp); err != nil {
		values.Free(exec.mp)
		return nil, err
	}
	values.SetLength(count)
	index := 0
	err = state.iter(row, func(key []byte) error {
		if index&1023 == 0 {
			if err := context.Cause(ctx); err != nil {
				return err
			}
		}
		payload := aggPayloadFromKey(&exec.aggInfo, key)
		if !exec.argTypes[0].IsVarlen() &&
			len(payload) != exec.argTypes[0].TypeSize() {
			return moerr.NewInternalErrorNoCtx(
				"ordered percentile has invalid retained argument")
		}
		if index >= count {
			return moerr.NewInternalErrorNoCtx(
				"ordered percentile retained argument count mismatch")
		}
		if err := values.SetRawBytesAt(index, payload, exec.mp); err != nil {
			return err
		}
		index++
		return nil
	})
	if err == nil && index != count {
		err = moerr.NewInternalErrorNoCtx(
			"ordered percentile retained argument count mismatch")
	}
	if err != nil {
		values.Free(exec.mp)
		return nil, err
	}
	return values, nil
}

func (exec *orderedPercentileDiscreteExec) Size() int64 {
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

func newOrderedPercentileExec[T numeric | types.Decimal64 | types.Decimal128, R types.FixedSizeTExceptStrType](
	mp *mpool.MPool, info singleAggInfo, mode orderedPercentileMode, initial R,
) *orderedPercentileExec[T, R] {
	return &orderedPercentileExec[T, R]{
		medianColumnExecSelf: newMedianColumnExecSelf[T, R](mp, info, initial),
		mode:                 mode,
	}
}

func (exec *orderedPercentileExec[T, R]) SetAllocationAccount(
	allocation *AllocationAccount,
) error {
	if exec.hasSpillRuns() || exec.spillData != nil {
		return mpool.ErrAllocationAccountInvariant
	}
	// In accounted Group mode the generic saved-argument arena is the only
	// retained value representation. Group owns externalization of that state,
	// so the standalone ordered-run implementation remains confined to callers
	// that do not install an allocation account.
	exec.spillLimit = 0
	exec.spillContext = nil
	exec.spillFile = nil
	exec.spillReport = nil
	return exec.medianColumnExecSelf.SetAllocationAccount(allocation)
}

func (exec *orderedPercentileExec[T, R]) SetExtraInformation(partialResult any, groupIndex int) error {
	percentile, descending, err := decodeOrderedPercentileConfig(partialResult)
	if err != nil {
		return err
	}
	exec.percentile = percentile
	exec.descending = descending
	return nil
}

func decodeOrderedPercentileConfig(partialResult any) (*big.Rat, bool, error) {
	b, ok := partialResult.([]byte)
	if !ok {
		return nil, false, moerr.NewInternalErrorNoCtx(
			"ordered percentile: expected []byte config")
	}
	descending := false
	if len(b) >= 2 && b[0] == orderedPercentileConfigVersion {
		if b[1] > 1 {
			return nil, false, moerr.NewInvalidInputNoCtx(
				"ordered percentile: invalid sort direction")
		}
		descending = b[1] == 1
		b = b[2:]
	}
	// Keep direct executor tests and old serialized plans readable when the
	// config contains only the percentile text.
	text := string(b)
	if text == "" {
		return nil, false, moerr.NewInvalidInputNoCtx(
			"ordered percentile: percentile is empty")
	}
	p, ok := new(big.Rat).SetString(text)
	if !ok || p.Sign() < 0 || p.Cmp(big.NewRat(1, 1)) > 0 {
		return nil, false, moerr.NewInvalidInputNoCtxf(
			"ordered percentile: percentile must be in [0,1], got %q", text)
	}
	if _, err := strconv.ParseFloat(text, 64); err != nil {
		return nil, false, moerr.NewInvalidInputNoCtxf(
			"ordered percentile: invalid percentile %q", text)
	}
	return p, descending, nil
}

func (exec *orderedPercentileExec[T, R]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*orderedPercentileExec[T, R])
	if exec.percentile != nil && other.percentile != nil && exec.percentile.Cmp(other.percentile) != 0 {
		return moerr.NewInvalidInputNoCtx("ordered percentile: cannot merge different percentile configurations")
	}
	if exec.descending != other.descending {
		return moerr.NewInvalidInputNoCtx("ordered percentile: cannot merge different sort directions")
	}
	if exec.accounted != nil || other.accounted != nil {
		if exec.accounted == nil || other.accounted == nil ||
			exec.mode != other.mode || !exec.argType.Eq(other.argType) ||
			!exec.retType.Eq(other.retType) {
			return mpool.ErrAllocationAccountMismatch
		}
		return exec.medianColumnExecSelf.Merge(
			&other.medianColumnExecSelf, groupIdx1, groupIdx2)
	}
	if other.hasSpillRuns() {
		return moerr.NewInternalErrorNoCtx("spilled ordered percentile cannot be merged as a partial result")
	}
	if err := exec.medianColumnExecSelf.Merge(&other.medianColumnExecSelf, groupIdx1, groupIdx2); err != nil {
		return err
	}
	return exec.maybeSpillOrdered()
}

func (exec *orderedPercentileExec[T, R]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*orderedPercentileExec[T, R])
	if exec.percentile != nil && other.percentile != nil && exec.percentile.Cmp(other.percentile) != 0 {
		return moerr.NewInvalidInputNoCtx("ordered percentile: cannot merge different percentile configurations")
	}
	if exec.descending != other.descending {
		return moerr.NewInvalidInputNoCtx("ordered percentile: cannot merge different sort directions")
	}
	if exec.accounted != nil || other.accounted != nil {
		if exec.accounted == nil || other.accounted == nil ||
			exec.mode != other.mode || !exec.argType.Eq(other.argType) ||
			!exec.retType.Eq(other.retType) {
			return mpool.ErrAllocationAccountMismatch
		}
		return exec.medianColumnExecSelf.BatchMerge(
			&other.medianColumnExecSelf, offset, groups)
	}
	if other.hasSpillRuns() {
		return moerr.NewInternalErrorNoCtx("spilled ordered percentile cannot be merged as a partial result")
	}
	if err := exec.medianColumnExecSelf.BatchMerge(&other.medianColumnExecSelf, offset, groups); err != nil {
		return err
	}
	return exec.maybeSpillOrdered()
}

func (exec *orderedPercentileExec[T, R]) Flush() ([]*vector.Vector, error) {
	return exec.FlushWithContext(context.Background())
}

func (exec *orderedPercentileExec[T, R]) FlushWithContext(ctx context.Context) ([]*vector.Vector, error) {
	if exec.percentile == nil {
		return nil, moerr.NewInternalErrorNoCtx("ordered percentile: percentile configuration is not set")
	}
	if exec.accounted != nil {
		return exec.flushAccounted()
	}
	if exec.hasSpillRuns() {
		if err := exec.spillOrderedState(ctx); err != nil {
			return nil, err
		}
		if err := exec.compactAllSpilledRuns(ctx); err != nil {
			return nil, err
		}
	}
	groups := len(exec.groups)
	lim := exec.ret.getChunkSize()
	for i, x := 0, 0; i < groups; i += lim {
		n := groups - i
		if n > lim {
			n = lim
		}
		for j := 0; j < n; j++ {
			groupIndex := i + j
			group := exec.groups[groupIndex]
			if group.Length() == 0 && !exec.groupHasSpillRuns(groupIndex) {
				continue
			}
			markMedianGroupNotEmpty(&exec.ret, x, j)
			if err := exec.flushGroup(ctx, groupIndex, group, x, j); err != nil {
				return nil, err
			}
		}
		x++
	}
	return exec.ret.flushAll(), nil
}

func (exec *orderedPercentileExec[T, R]) flushAccounted() (
	_ []*vector.Vector, retErr error,
) {
	results := make([]*vector.Vector, len(exec.accounted.state))
	defer freeAggregateResultsOnError(exec.mp, results, &retErr)
	for chunk := range exec.accounted.state {
		state := &exec.accounted.state[chunk]
		result, err := exec.accounted.allocation.newVector(exec.retType)
		if err != nil {
			return nil, err
		}
		results[chunk] = result
		if err = result.PreExtendNulls(int(state.length), exec.mp); err != nil {
			return nil, err
		}
		if err = result.PreExtend(int(state.length), exec.mp); err != nil {
			return nil, err
		}
		result.SetLength(int(state.length))
		for row := uint16(0); row < uint16(state.length); row++ {
			if state.argCnt[row] == 0 {
				result.SetNull(uint64(row))
				continue
			}
			values, err := makeAccountedScratch[T](
				exec.accounted.allocation, exec.mp, int(state.argCnt[row]))
			if err != nil {
				return nil, err
			}
			index := 0
			err = state.iter(row, func(key []byte) error {
				payload := aggPayloadFromKey(&exec.accounted.aggInfo, key)
				if len(payload) != exec.argType.TypeSize() || index >= len(values) {
					return moerr.NewInternalErrorNoCtx(
						"ordered percentile has invalid retained argument")
				}
				values[index] = types.DecodeFixed[T](payload)
				index++
				return nil
			})
			if err == nil && index != len(values) {
				err = moerr.NewInternalErrorNoCtx(
					"ordered percentile retained argument count mismatch")
			}
			if err == nil {
				err = exec.setAccountedResult(values, result, int(row))
			}
			mpool.FreeSlice(exec.mp, values)
			if err != nil {
				return nil, err
			}
		}
	}
	return results, nil
}

func (exec *orderedPercentileExec[T, R]) setAccountedResult(
	values []T, result *vector.Vector, row int,
) error {
	slices.SortFunc(values, func(left, right T) int {
		comparison := compareOrderedPercentileValue(left, right)
		if exec.descending {
			comparison = -comparison
		}
		return comparison
	})
	lo, hi, fraction := orderedPercentileRanksWithScratch(
		&exec.arithmetic, uint64(len(values)), exec.percentile, exec.mode)
	if exec.mode == orderedPercentileDiscrete {
		value, ok := any(values[lo]).(R)
		if !ok {
			return moerr.NewInternalErrorNoCtx(
				"ordered percentile: result type mismatch")
		}
		vector.SetFixedAtWithTypeCheck(result, row, value)
		return nil
	}
	var value R
	switch low := any(values[lo]).(type) {
	case types.Decimal64:
		interpolated, err := exec.arithmetic.interpolateDecimal(
			FromD64ToD128(low),
			FromD64ToD128(any(values[hi]).(types.Decimal64)),
			fraction,
			exec.retType.Scale-exec.argType.Scale,
		)
		if err != nil {
			return err
		}
		value = any(interpolated).(R)
	case types.Decimal128:
		interpolated, err := exec.arithmetic.interpolateDecimal(
			low,
			any(values[hi]).(types.Decimal128),
			fraction,
			exec.retType.Scale-exec.argType.Scale,
		)
		if err != nil {
			return err
		}
		value = any(interpolated).(R)
	default:
		value = any(interpolateOrderedNumericValueWithScratch(
			&exec.arithmetic, values[lo], values[hi], fraction)).(R)
	}
	vector.SetFixedAtWithTypeCheck(result, row, value)
	return nil
}

func (exec *orderedPercentileExec[T, R]) flushGroup(ctx context.Context, groupIndex int, group *Vectors[T], x, y int) error {
	if exec.groupHasSpillRuns(groupIndex) {
		lo, hi, frac := orderedPercentileRanks(exec.spilledGroupRows(groupIndex), exec.percentile, exec.mode)
		loValue, hiValue, err := exec.selectSpilledValues(ctx, groupIndex, lo, hi)
		if err != nil {
			return err
		}
		if exec.mode == orderedPercentileDiscrete {
			return exec.setDiscreteResult(loValue, x, y)
		}
		return exec.setContinuousResult(loValue, hiValue, frac, x, y)
	}
	values := collectMedianValues(group)
	selectors, err := sortOrderedPercentileValues(exec.mp, exec.argType, values, exec.descending)
	if err != nil {
		return err
	}

	lo, hi, frac := orderedPercentileRanks(uint64(len(values)), exec.percentile, exec.mode)
	if exec.mode == orderedPercentileDiscrete {
		return exec.setDiscreteResult(values[int(selectors[lo])], x, y)
	}
	return exec.setContinuousResult(values[int(selectors[lo])], values[int(selectors[hi])], frac, x, y)
}

func (exec *orderedPercentileExec[T, R]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	if err := exec.medianColumnExecSelf.Fill(groupIndex, row, vectors); err != nil {
		return err
	}
	return exec.maybeSpillOrdered()
}

func (exec *orderedPercentileExec[T, R]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	if err := exec.medianColumnExecSelf.BulkFill(groupIndex, vectors); err != nil {
		return err
	}
	return exec.maybeSpillOrdered()
}

func (exec *orderedPercentileExec[T, R]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if err := exec.medianColumnExecSelf.BatchFill(offset, groups, vectors); err != nil {
		return err
	}
	return exec.maybeSpillOrdered()
}

// sortOrderedPercentileValues uses the same total comparator as spilled-run
// merge. FLOAT NaN must have one defined order across in-memory sorting, run
// creation, and k-way merge; otherwise spill can change percentile ranks.
func sortOrderedPercentileValues[T numeric | types.Decimal64 | types.Decimal128](
	_ *mpool.MPool, _ types.Type, values []T, descending bool,
) ([]int64, error) {
	selectors := make([]int64, len(values))
	for i := range selectors {
		selectors[i] = int64(i)
	}
	if len(values) < 2 {
		return selectors, nil
	}

	sort.Slice(selectors, func(i, j int) bool {
		left, right := selectors[i], selectors[j]
		cmp := compareOrderedPercentileValue(values[int(left)], values[int(right)])
		if descending {
			cmp = -cmp
		}
		if cmp == 0 {
			return left < right
		}
		return cmp < 0
	})
	return selectors, nil
}

func (exec *orderedPercentileExec[T, R]) setDiscreteResult(value T, x, y int) error {
	result, ok := any(value).(R)
	if !ok {
		return moerr.NewInternalErrorNoCtx("ordered percentile: result type mismatch")
	}
	exec.ret.values[x][y] = result
	return nil
}

func (exec *orderedPercentileExec[T, R]) setContinuousResult(lo, hi T, frac *big.Rat, x, y int) error {
	var value R
	switch lv := any(lo).(type) {
	case types.Decimal64:
		result, err := interpolateDecimal(FromD64ToD128(lv), FromD64ToD128(any(hi).(types.Decimal64)), frac, exec.retType.Scale-exec.argType.Scale)
		if err != nil {
			return err
		}
		value = any(result).(R)
	case types.Decimal128:
		result, err := interpolateDecimal(lv, any(hi).(types.Decimal128), frac, exec.retType.Scale-exec.argType.Scale)
		if err != nil {
			return err
		}
		value = any(result).(R)
	default:
		result := interpolateOrderedNumericValue(lo, hi, frac)
		value = any(result).(R)
	}
	exec.ret.values[x][y] = value
	return nil
}

func (exec *orderedPercentileExec[T, R]) SaveIntermediateResult(
	cnt int64,
	flags [][]uint8,
	writer io.Writer,
) error {
	if exec.hasSpillRuns() {
		return moerr.NewInternalErrorNoCtx("spilled ordered percentile cannot be serialized as a partial result")
	}
	return exec.medianColumnExecSelf.SaveIntermediateResult(cnt, flags, writer)
}

func (exec *orderedPercentileExec[T, R]) PreflightBatchMerge(
	next AggFuncExec, offset int, groups []uint64,
) error {
	other, ok := next.(*orderedPercentileExec[T, R])
	if !ok || other == nil || exec.mode != other.mode ||
		!exec.argType.Eq(other.argType) || !exec.retType.Eq(other.retType) ||
		exec.percentile == nil || other.percentile == nil ||
		exec.percentile.Cmp(other.percentile) != 0 ||
		exec.descending != other.descending {
		return mpool.ErrAllocationAccountMismatch
	}
	return exec.medianColumnExecSelf.preflightBatchMerge(
		&other.medianColumnExecSelf, offset, groups)
}

func (exec *orderedPercentileExec[T, R]) SaveIntermediateResultOfChunk(
	chunk int,
	writer io.Writer,
) error {
	if exec.hasSpillRuns() {
		return moerr.NewInternalErrorNoCtx("spilled ordered percentile cannot be serialized as a partial result")
	}
	return exec.medianColumnExecSelf.SaveIntermediateResultOfChunk(chunk, writer)
}

func (exec *orderedPercentileExec[T, R]) UnmarshalFromReader(reader io.Reader, mp *mpool.MPool) error {
	if exec.accounted != nil {
		return exec.medianColumnExecSelf.UnmarshalFromReader(reader, mp)
	}
	replacement := newMedianColumnExecSelf[T, R](
		mp,
		exec.singleAggInfo,
		exec.ret.InitialValue,
	)
	if err := replacement.unmarshalFromReader(reader, mp); err != nil {
		replacement.Free()
		return err
	}
	exec.closeSpillData()
	exec.medianColumnExecSelf.Free()
	exec.medianColumnExecSelf = replacement
	return nil
}

func (exec *orderedPercentileExec[T, R]) Size() int64 {
	return exec.medianColumnExecSelf.Size() + exec.fixedAndSpilledMemorySize()
}

func (exec *orderedPercentileExec[T, R]) AdditionalMemorySize() int64 {
	return exec.fixedAndSpilledMemorySize()
}

func (exec *orderedPercentileExec[T, R]) Free() {
	exec.closeSpillData()
	exec.spillLimit = 0
	exec.spillContext = nil
	exec.spillFile = nil
	exec.spillReport = nil
	exec.medianColumnExecSelf.Free()
}

func (exec *orderedPercentileExec[T, R]) closeSpillData() {
	if exec.spillData != nil {
		_ = exec.spillData.Close()
	}
	exec.spillData = nil
	exec.spillRuns = nil
}

func (exec *orderedPercentileExec[T, R]) fixedAndSpilledMemorySize() int64 {
	var size int64
	for _, runs := range exec.spillRuns {
		size += int64(cap(runs)) * int64(5*8)
	}
	return size
}

func (exec *orderedPercentileExec[T, R]) activeOrderedMemorySize() int64 {
	var size int64
	for _, group := range exec.groups {
		if group != nil {
			size += group.Size()
		}
	}
	return size
}

func (exec *orderedPercentileExec[T, R]) maybeSpillOrdered() error {
	if exec.spillLimit > 0 && exec.activeOrderedMemorySize() >= exec.spillLimit {
		return exec.spillOrderedState(exec.spillContext)
	}
	return nil
}

func (exec *orderedPercentileExec[T, R]) spillOrderedState(ctx context.Context) error {
	if exec.spillFile == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	groupCount := len(exec.groups)
	for len(exec.spillRuns) < groupCount {
		exec.spillRuns = append(exec.spillRuns, nil)
	}
	for groupIndex, group := range exec.groups {
		if group == nil || group.Length() == 0 {
			continue
		}
		values := collectMedianValues(group)
		if err := exec.writeOrderedRun(ctx, groupIndex, values); err != nil {
			return err
		}
	}
	for _, group := range exec.groups {
		if group != nil {
			group.Free(exec.mp)
		}
	}
	exec.groups = nil
	if groupCount > 0 {
		exec.groups = make([]*Vectors[T], groupCount)
		for i := range exec.groups {
			exec.groups[i] = NewVectors[T](exec.argType)
		}
	}
	return nil
}

func (exec *orderedPercentileExec[T, R]) writeOrderedRun(ctx context.Context, groupIndex int, values []T) error {
	selectors, err := sortOrderedPercentileValues(exec.mp, exec.argType, values, exec.descending)
	if err != nil {
		return err
	}
	if exec.spillData == nil {
		exec.spillData, err = exec.spillFile()
		if err != nil {
			return err
		}
	}
	start, err := exec.spillData.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	writer := bufio.NewWriterSize(exec.spillData, 64*1024)
	var rows int64
	for _, selector := range selectors {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if _, err = writer.Write(types.EncodeFixed(values[selector])); err != nil {
			return err
		}
		rows++
	}
	if err = writer.Flush(); err != nil {
		return err
	}
	end, err := exec.spillData.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	exec.spillRuns[groupIndex] = append(exec.spillRuns[groupIndex], orderedPercentileRun{
		start: start,
		end:   end,
		pos:   start,
		rows:  uint64(rows),
	})
	if exec.spillReport != nil {
		exec.spillReport(end-start, rows, exec.Size())
	}
	return exec.compactOrderedRuns(ctx, groupIndex)
}

func (exec *orderedPercentileExec[T, R]) compactAllSpilledRuns(ctx context.Context) error {
	for groupIndex := range exec.spillRuns {
		if err := exec.compactOrderedRuns(ctx, groupIndex); err != nil {
			return err
		}
	}
	return nil
}

func (exec *orderedPercentileExec[T, R]) compactOrderedRuns(ctx context.Context, groupIndex int) error {
	for {
		runs := exec.spillRuns[groupIndex]
		if len(runs) <= orderedPercentileRunFanIn {
			return nil
		}
		level, ok := firstCompactableRunLevel(runs)
		if !ok {
			return exec.compactFirstRuns(ctx, groupIndex)
		}
		if err := exec.compactRunsAtLevel(ctx, groupIndex, level); err != nil {
			return err
		}
	}
}

func firstCompactableRunLevel(runs []orderedPercentileRun) (uint8, bool) {
	for level := uint16(0); level <= 255; level++ {
		count := 0
		for _, run := range runs {
			if uint16(run.level) == level {
				count++
				if count >= orderedPercentileRunFanIn {
					return uint8(level), true
				}
			}
		}
	}
	return 0, false
}

func (exec *orderedPercentileExec[T, R]) compactRunsAtLevel(ctx context.Context, groupIndex int, level uint8) error {
	runs := exec.spillRuns[groupIndex]
	source := make([]orderedPercentileRun, 0, orderedPercentileRunFanIn)
	remaining := runs[:0]
	for _, run := range runs {
		if run.level == level && len(source) < orderedPercentileRunFanIn {
			source = append(source, run)
			continue
		}
		remaining = append(remaining, run)
	}
	nextLevel := level
	if nextLevel < 255 {
		nextLevel++
	}
	merged, err := exec.mergeOrderedRuns(ctx, source, nextLevel)
	if err != nil {
		return err
	}
	compacted := append([]orderedPercentileRun(nil), remaining...)
	compacted = append(compacted, merged)
	exec.spillRuns[groupIndex] = compacted
	return nil
}

func (exec *orderedPercentileExec[T, R]) compactFirstRuns(ctx context.Context, groupIndex int) error {
	runs := exec.spillRuns[groupIndex]
	source := append([]orderedPercentileRun(nil), runs[:orderedPercentileRunFanIn]...)
	remaining := append([]orderedPercentileRun(nil), runs[orderedPercentileRunFanIn:]...)
	merged, err := exec.mergeOrderedRuns(ctx, source, runs[0].level)
	if err != nil {
		return err
	}
	exec.spillRuns[groupIndex] = append(remaining, merged)
	return nil
}

func (exec *orderedPercentileExec[T, R]) mergeOrderedRuns(
	ctx context.Context,
	source []orderedPercentileRun,
	level uint8,
) (orderedPercentileRun, error) {
	var zero orderedPercentileRun
	if ctx == nil {
		ctx = context.Background()
	}
	if exec.spillData == nil {
		return zero, moerr.NewInternalErrorNoCtx("ordered percentile: missing spill data for run compaction")
	}
	runs := append([]orderedPercentileRun(nil), source...)
	heads := make([]T, len(runs))
	runHeap := &orderedPercentileRunHeap[T]{
		runs:       make([]int, 0, len(runs)),
		values:     heads,
		descending: exec.descending,
	}
	for i := range runs {
		runs[i].pos = runs[i].start
		value, ok, err := exec.readRunValue(&runs[i])
		if err != nil {
			return zero, err
		}
		if ok {
			heads[i] = value
			runHeap.runs = append(runHeap.runs, i)
		}
	}
	heap.Init(runHeap)

	start, err := exec.spillData.Seek(0, io.SeekEnd)
	if err != nil {
		return zero, err
	}
	writer := bufio.NewWriterSize(exec.spillData, 64*1024)
	var rows int64
	for runHeap.Len() > 0 {
		if err := context.Cause(ctx); err != nil {
			return zero, err
		}
		run := heap.Pop(runHeap).(int)
		value := heads[run]
		if _, err = writer.Write(types.EncodeFixed(value)); err != nil {
			return zero, err
		}
		rows++
		next, ok, err := exec.readRunValue(&runs[run])
		if err != nil {
			return zero, err
		}
		if ok {
			heads[run] = next
			heap.Push(runHeap, run)
		}
	}
	if err = writer.Flush(); err != nil {
		return zero, err
	}
	end, err := exec.spillData.Seek(0, io.SeekCurrent)
	if err != nil {
		return zero, err
	}
	return orderedPercentileRun{
		start: start,
		end:   end,
		pos:   start,
		rows:  uint64(rows),
		level: level,
	}, nil
}

func (exec *orderedPercentileExec[T, R]) hasSpillRuns() bool {
	for _, runs := range exec.spillRuns {
		if len(runs) > 0 {
			return true
		}
	}
	return false
}

func (exec *orderedPercentileExec[T, R]) groupHasSpillRuns(groupIndex int) bool {
	return groupIndex < len(exec.spillRuns) && len(exec.spillRuns[groupIndex]) > 0
}

func (exec *orderedPercentileExec[T, R]) spilledGroupRows(groupIndex int) uint64 {
	var rows uint64
	for _, run := range exec.spillRuns[groupIndex] {
		rows += run.rows
	}
	return rows
}

func (exec *orderedPercentileExec[T, R]) selectSpilledValues(ctx context.Context, groupIndex int, lo, hi uint64) (T, T, error) {
	runs := append([]orderedPercentileRun(nil), exec.spillRuns[groupIndex]...)
	heads := make([]T, len(runs))
	runHeap := &orderedPercentileRunHeap[T]{
		runs:       make([]int, 0, len(runs)),
		values:     heads,
		descending: exec.descending,
	}
	for i := range runs {
		runs[i].pos = runs[i].start
		value, ok, err := exec.readRunValue(&runs[i])
		if err != nil {
			var zero T
			return zero, zero, err
		}
		if ok {
			heads[i] = value
			runHeap.runs = append(runHeap.runs, i)
		}
	}
	heap.Init(runHeap)
	var loValue, hiValue T
	for rank := uint64(0); runHeap.Len() > 0; rank++ {
		if err := context.Cause(ctx); err != nil {
			var zero T
			return zero, zero, err
		}
		run := heap.Pop(runHeap).(int)
		value := heads[run]
		if rank == lo {
			loValue = value
		}
		if rank == hi {
			hiValue = value
			return loValue, hiValue, nil
		}
		next, ok, err := exec.readRunValue(&runs[run])
		if err != nil {
			var zero T
			return zero, zero, err
		}
		if ok {
			heads[run] = next
			heap.Push(runHeap, run)
		}
	}
	var zero T
	return zero, zero, moerr.NewInternalErrorNoCtx("ordered percentile: spilled run ended before requested rank")
}

func (exec *orderedPercentileExec[T, R]) readRunValue(run *orderedPercentileRun) (T, bool, error) {
	var zero T
	if run.pos >= run.end {
		return zero, false, nil
	}
	size := exec.argType.TypeSize()
	buf := make([]byte, size)
	if _, err := exec.spillData.ReadAt(buf, run.pos); err != nil {
		return zero, false, err
	}
	run.pos += int64(size)
	return types.DecodeFixed[T](buf), true, nil
}

type orderedPercentileRunHeap[T numeric | types.Decimal64 | types.Decimal128] struct {
	runs       []int
	values     []T
	descending bool
}

func (h orderedPercentileRunHeap[T]) Len() int { return len(h.runs) }
func (h orderedPercentileRunHeap[T]) Less(i, j int) bool {
	left, right := h.runs[i], h.runs[j]
	cmp := compareOrderedPercentileValue(h.values[left], h.values[right])
	if h.descending {
		cmp = -cmp
	}
	if cmp == 0 {
		return left < right
	}
	return cmp < 0
}
func (h orderedPercentileRunHeap[T]) Swap(i, j int) { h.runs[i], h.runs[j] = h.runs[j], h.runs[i] }
func (h *orderedPercentileRunHeap[T]) Push(value any) {
	h.runs = append(h.runs, value.(int))
}
func (h *orderedPercentileRunHeap[T]) Pop() any {
	last := len(h.runs) - 1
	value := h.runs[last]
	h.runs = h.runs[:last]
	return value
}

func compareOrderedPercentileValue[T numeric | types.Decimal64 | types.Decimal128](left, right T) int {
	switch l := any(left).(type) {
	case int8:
		return cmp.Compare(l, any(right).(int8))
	case int16:
		return cmp.Compare(l, any(right).(int16))
	case int32:
		return cmp.Compare(l, any(right).(int32))
	case int64:
		return cmp.Compare(l, any(right).(int64))
	case uint8:
		return cmp.Compare(l, any(right).(uint8))
	case uint16:
		return cmp.Compare(l, any(right).(uint16))
	case uint32:
		return cmp.Compare(l, any(right).(uint32))
	case uint64:
		return cmp.Compare(l, any(right).(uint64))
	case float32:
		return cmp.Compare(l, any(right).(float32))
	case float64:
		return cmp.Compare(l, any(right).(float64))
	case types.Decimal64:
		return l.Compare(any(right).(types.Decimal64))
	case types.Decimal128:
		return l.Compare(any(right).(types.Decimal128))
	default:
		panic("unsupported ordered percentile type")
	}
}

func interpolateOrderedNumericValue[T numeric | types.Decimal64 | types.Decimal128](lo, hi T, frac *big.Rat) float64 {
	var scratch percentileArithmeticScratch
	return interpolateOrderedNumericValueWithScratch(
		&scratch, lo, hi, percentileFraction{
			numerator: frac.Num(), denominator: frac.Denom(),
		})
}

func interpolateOrderedNumericValueWithScratch[
	T numeric | types.Decimal64 | types.Decimal128,
](
	scratch *percentileArithmeticScratch,
	lo, hi T,
	fraction percentileFraction,
) float64 {
	switch lv := any(lo).(type) {
	case int8:
		return interpolateNumericWithScratch(scratch, lv, any(hi).(int8), fraction)
	case int16:
		return interpolateNumericWithScratch(scratch, lv, any(hi).(int16), fraction)
	case int32:
		return interpolateNumericWithScratch(scratch, lv, any(hi).(int32), fraction)
	case int64:
		return interpolateNumericWithScratch(scratch, lv, any(hi).(int64), fraction)
	case uint8:
		return interpolateNumericWithScratch(scratch, lv, any(hi).(uint8), fraction)
	case uint16:
		return interpolateNumericWithScratch(scratch, lv, any(hi).(uint16), fraction)
	case uint32:
		return interpolateNumericWithScratch(scratch, lv, any(hi).(uint32), fraction)
	case uint64:
		return interpolateNumericWithScratch(scratch, lv, any(hi).(uint64), fraction)
	case float32:
		return interpolateNumericWithScratch(scratch, lv, any(hi).(float32), fraction)
	case float64:
		return interpolateNumericWithScratch(scratch, lv, any(hi).(float64), fraction)
	default:
		panic("unsupported ordered percentile numeric type")
	}
}

func orderedPercentileRanksWithScratch(
	scratch *percentileArithmeticScratch,
	count uint64,
	p *big.Rat,
	mode orderedPercentileMode,
) (lo, hi uint64, fraction percentileFraction) {
	if mode == orderedPercentileDiscrete {
		lo = scratch.discreteRank(count, p)
		scratch.remainder.SetUint64(0)
		return lo, lo, percentileFraction{
			numerator: &scratch.remainder, denominator: p.Denom(),
		}
	}
	return scratch.ranks(count, p)
}

func orderedPercentileRanks(count uint64, p *big.Rat, mode orderedPercentileMode) (lo, hi uint64, frac *big.Rat) {
	var scratch percentileArithmeticScratch
	lo, hi, fraction := orderedPercentileRanksWithScratch(
		&scratch, count, p, mode)
	return lo, hi, new(big.Rat).SetFrac(
		new(big.Int).Set(fraction.numerator),
		new(big.Int).Set(fraction.denominator))
}

func makeOrderedPercentileExec(mp *mpool.MPool, aggID int64, isDistinct bool, param types.Type, mode orderedPercentileMode) (AggFuncExec, error) {
	if isDistinct {
		return nil, moerr.NewNotSupportedNoCtx("ordered percentile in distinct mode")
	}
	if mode == orderedPercentileContinuous && param.IsDecimal() && param.Width >= 38 {
		return nil, moerr.NewNotSupportedNoCtx("percentile_cont on maximum-width decimal order expressions")
	}
	info := singleAggInfo{
		aggID:     aggID,
		argType:   param,
		emptyNull: true,
	}
	if mode == orderedPercentileContinuous {
		info.retType = PercentileContReturnType([]types.Type{param})
	} else {
		info.retType = PercentileDiscReturnType([]types.Type{param})
	}
	switch param.Oid {
	case types.T_bit:
		if mode == orderedPercentileContinuous {
			return newOrderedPercentileExec[uint64, float64](mp, info, mode, 0), nil
		}
		return newOrderedPercentileExec[uint64, uint64](mp, info, mode, 0), nil
	case types.T_int8:
		if mode == orderedPercentileContinuous {
			return newOrderedPercentileExec[int8, float64](mp, info, mode, 0), nil
		}
		return newOrderedPercentileExec[int8, int8](mp, info, mode, 0), nil
	case types.T_int16:
		if mode == orderedPercentileContinuous {
			return newOrderedPercentileExec[int16, float64](mp, info, mode, 0), nil
		}
		return newOrderedPercentileExec[int16, int16](mp, info, mode, 0), nil
	case types.T_int32:
		if mode == orderedPercentileContinuous {
			return newOrderedPercentileExec[int32, float64](mp, info, mode, 0), nil
		}
		return newOrderedPercentileExec[int32, int32](mp, info, mode, 0), nil
	case types.T_int64:
		if mode == orderedPercentileContinuous {
			return newOrderedPercentileExec[int64, float64](mp, info, mode, 0), nil
		}
		return newOrderedPercentileExec[int64, int64](mp, info, mode, 0), nil
	case types.T_uint8:
		if mode == orderedPercentileContinuous {
			return newOrderedPercentileExec[uint8, float64](mp, info, mode, 0), nil
		}
		return newOrderedPercentileExec[uint8, uint8](mp, info, mode, 0), nil
	case types.T_uint16:
		if mode == orderedPercentileContinuous {
			return newOrderedPercentileExec[uint16, float64](mp, info, mode, 0), nil
		}
		return newOrderedPercentileExec[uint16, uint16](mp, info, mode, 0), nil
	case types.T_uint32:
		if mode == orderedPercentileContinuous {
			return newOrderedPercentileExec[uint32, float64](mp, info, mode, 0), nil
		}
		return newOrderedPercentileExec[uint32, uint32](mp, info, mode, 0), nil
	case types.T_uint64:
		if mode == orderedPercentileContinuous {
			return newOrderedPercentileExec[uint64, float64](mp, info, mode, 0), nil
		}
		return newOrderedPercentileExec[uint64, uint64](mp, info, mode, 0), nil
	case types.T_float32:
		if mode == orderedPercentileContinuous {
			return newOrderedPercentileExec[float32, float64](mp, info, mode, 0), nil
		}
		return newOrderedPercentileExec[float32, float32](mp, info, mode, 0), nil
	case types.T_float64:
		if mode == orderedPercentileContinuous {
			return newOrderedPercentileExec[float64, float64](mp, info, mode, 0), nil
		}
		return newOrderedPercentileExec[float64, float64](mp, info, mode, 0), nil
	case types.T_decimal64:
		if mode == orderedPercentileContinuous {
			return newOrderedPercentileExec[types.Decimal64, types.Decimal128](mp, info, mode, types.Decimal128{}), nil
		}
		return newOrderedPercentileExec[types.Decimal64, types.Decimal64](mp, info, mode, 0), nil
	case types.T_decimal128:
		if mode == orderedPercentileContinuous {
			return newOrderedPercentileExec[types.Decimal128, types.Decimal128](mp, info, mode, types.Decimal128{}), nil
		}
		return newOrderedPercentileExec[types.Decimal128, types.Decimal128](mp, info, mode, types.Decimal128{}), nil
	default:
		if mode == orderedPercentileDiscrete && PercentileDiscSupportedType(param.Oid) {
			return newOrderedPercentileDiscreteExec(mp, aggID, param), nil
		}
		return nil, moerr.NewInternalErrorNoCtx("unsupported type for ordered percentile")
	}
}
