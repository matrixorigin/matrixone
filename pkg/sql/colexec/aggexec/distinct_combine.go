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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type internalCombineValue interface {
	int64 | uint64 | float64 | types.Decimal128 | types.Decimal256
}

type internalCombineAdd[T internalCombineValue] func(T, T) (T, error)

type internalAvgCombineExec[T internalCombineValue] struct {
	aggExec
	add internalCombineAdd[T]
}

func (*internalAvgCombineExec[T]) sourcePreservingMerge() {}

func (exec *internalAvgCombineExec[T]) Fill(
	groupIndex int,
	row int,
	vectors []*vector.Vector,
) error {
	return exec.addPartial(uint64(groupIndex), row, vectors[0], vectors[1])
}

func (exec *internalAvgCombineExec[T]) BulkFill(
	groupIndex int,
	vectors []*vector.Vector,
) error {
	for row := 0; row < vectors[0].Length(); row++ {
		if err := exec.addPartial(
			uint64(groupIndex), row, vectors[0], vectors[1],
		); err != nil {
			return err
		}
	}
	return nil
}

func (exec *internalAvgCombineExec[T]) BatchFill(
	offset int,
	groups []uint64,
	vectors []*vector.Vector,
) error {
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		if err := exec.addPartial(
			group-1, offset+i, vectors[0], vectors[1],
		); err != nil {
			return err
		}
	}
	return nil
}

func (exec *internalAvgCombineExec[T]) addPartial(
	group uint64,
	row int,
	sumInput *vector.Vector,
	countInput *vector.Vector,
) error {
	sumRow := row
	if sumInput.IsConst() {
		sumRow = 0
	}
	countRow := row
	if countInput.IsConst() {
		countRow = 0
	}
	if countInput.IsNull(uint64(countRow)) {
		return moerr.NewInvalidInputNoCtx("internal AVG partial count is NULL")
	}
	count := vector.GetFixedAtNoTypeCheck[int64](countInput, countRow)
	if count < 0 {
		return moerr.NewInvalidInputNoCtx("internal AVG partial count is negative")
	}
	if count == 0 {
		if !sumInput.IsNull(uint64(sumRow)) {
			return moerr.NewInvalidInputNoCtx(
				"internal AVG partial sum is non-NULL for an empty partial")
		}
		return nil
	}
	if sumInput.IsNull(uint64(sumRow)) {
		return moerr.NewInvalidInputNoCtx(
			"internal AVG partial sum is NULL for a non-empty partial")
	}

	x, y := exec.getXY(group)
	sumState := exec.state[x].vecs[0]
	countState := exec.state[x].vecs[1]
	sums := chunkArr[T](sumState)
	counts := chunkArr[int64](countState)
	value := vector.GetFixedAtNoTypeCheck[T](sumInput, sumRow)
	if sumState.IsNull(uint64(y)) {
		sumState.UnsetNull(uint64(y))
		countState.UnsetNull(uint64(y))
		sums[y] = value
		counts[y] = count
		return nil
	}
	combined, err := exec.add(sums[y], value)
	if err != nil {
		return err
	}
	sums[y] = combined
	counts[y] += count
	return nil
}

func (exec *internalAvgCombineExec[T]) Merge(
	next AggFuncExec,
	groupIdx1 int,
	groupIdx2 int,
) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *internalAvgCombineExec[T]) BatchMerge(
	next AggFuncExec,
	offset int,
	groups []uint64,
) error {
	other := next.(*internalAvgCombineExec[T])
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		dstGroup := group - 1
		srcGroup := uint64(offset + i)
		dstX, dstY := exec.getXY(dstGroup)
		srcX, srcY := other.getXY(srcGroup)
		dstSum := exec.state[dstX].vecs[0]
		srcSum := other.state[srcX].vecs[0]
		srcCount := other.state[srcX].vecs[1]
		srcCounts := chunkRows[int64](srcCount)
		if srcSum.IsNull(uint64(srcY)) {
			if srcCounts[srcY] != 0 {
				return moerr.NewInvalidInputNoCtx(
					"internal AVG source state has a count without a sum")
			}
			continue
		}
		dstCount := exec.state[dstX].vecs[1]
		dstSums := chunkArr[T](dstSum)
		srcSums := chunkRows[T](srcSum)
		dstCounts := chunkArr[int64](dstCount)
		if srcCounts[srcY] <= 0 {
			return moerr.NewInvalidInputNoCtx(
				"internal AVG source state has a non-positive count")
		}
		if dstSum.IsNull(uint64(dstY)) {
			dstSum.UnsetNull(uint64(dstY))
			dstCount.UnsetNull(uint64(dstY))
			dstSums[dstY] = srcSums[srcY]
			dstCounts[dstY] = srcCounts[srcY]
			continue
		}
		if dstCounts[dstY] <= 0 {
			return moerr.NewInvalidInputNoCtx(
				"internal AVG destination state has a non-positive count")
		}
		combined, err := exec.add(dstSums[dstY], srcSums[srcY])
		if err != nil {
			return err
		}
		dstSums[dstY] = combined
		dstCounts[dstY] += srcCounts[srcY]
	}
	return nil
}

func (*internalAvgCombineExec[T]) SetExtraInformation(any, int) error {
	return nil
}

func (exec *internalAvgCombineExec[T]) Flush() (
	result []*vector.Vector,
	retErr error,
) {
	result = make([]*vector.Vector, len(exec.state))
	defer func() {
		if retErr != nil {
			for _, vec := range result {
				if vec != nil {
					vec.Free(exec.mp)
				}
			}
		}
	}()
	for i := range exec.state {
		result[i], retErr = exec.allocation.newVector(exec.retType)
		if retErr != nil {
			return nil, retErr
		}
		if retErr = result[i].PreExtend(int(exec.state[i].length), exec.mp); retErr != nil {
			return nil, retErr
		}
		sumVector := exec.state[i].vecs[0]
		countVector := exec.state[i].vecs[1]
		sums := vector.MustFixedColNoTypeCheck[T](sumVector)
		counts := vector.MustFixedColNoTypeCheck[int64](countVector)
		for row, count := range counts {
			if count < 0 {
				return nil, moerr.NewInvalidInputNoCtx(
					"internal AVG state has a negative count")
			}
			sumNull := sumVector.IsNull(uint64(row))
			if count == 0 {
				if !sumNull {
					return nil, moerr.NewInvalidInputNoCtx(
						"internal AVG state has a sum without a count")
				}
				if retErr = vector.AppendNull(result[i], exec.mp); retErr != nil {
					return nil, retErr
				}
				continue
			}
			if sumNull {
				return nil, moerr.NewInvalidInputNoCtx(
					"internal AVG state has a count without a sum")
			}
			if retErr = exec.appendAverage(result[i], sums[row], count); retErr != nil {
				return nil, retErr
			}
		}
		sumVector.Free(exec.mp)
		countVector.Free(exec.mp)
		exec.state[i].vecs[0] = nil
		exec.state[i].vecs[1] = nil
		exec.state[i].length = 0
		exec.state[i].capacity = 0
	}
	return result, nil
}

func (exec *internalAvgCombineExec[T]) appendAverage(
	result *vector.Vector,
	sum T,
	count int64,
) error {
	switch value := any(sum).(type) {
	case int64:
		return vector.AppendFixed(result, float64(value)/float64(count), false, exec.mp)
	case uint64:
		return vector.AppendFixed(result, float64(value)/float64(count), false, exec.mp)
	case float64:
		return vector.AppendFixed(result, value/float64(count), false, exec.mp)
	case types.Decimal128:
		average := decAvg(value, count, exec.argTypes[0].Scale, exec.retType.Scale)
		return vector.AppendFixed(result, average, false, exec.mp)
	case types.Decimal256:
		average := decAvg(value, count, exec.argTypes[0].Scale, exec.retType.Scale)
		return vector.AppendFixed(result, average, false, exec.mp)
	default:
		return moerr.NewInternalErrorNoCtxf(
			"unsupported internal AVG partial type %T", sum)
	}
}

func internalCheckedInt64Add(left, right int64) (int64, error) {
	result := left + right
	return result, int64OfCheck(left, right, result)
}

func internalCheckedUint64Add(left, right uint64) (uint64, error) {
	result := left + right
	return result, uint64OfCheck(left, right, result)
}

func internalFloat64Add(left, right float64) (float64, error) {
	return left + right, nil
}

type internalNumericCombineValue interface {
	int64 | uint64 | float64
}

func newInternalNumericCombineExec[T internalNumericCombineValue](
	mp *mpool.MPool,
	aggID int64,
	param types.Type,
	emptyNull bool,
	overflowCheck func(T, T, T) error,
) AggFuncExec {
	exec := &sumAvgExec[T, T]{
		isSum:   true,
		ofCheck: overflowCheck,
	}
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId:      aggID,
		argTypes:   []types.Type{param},
		retType:    param,
		stateTypes: []types.Type{param},
		emptyNull:  emptyNull,
	}
	return exec
}

func newInternalDecimalCombineExec[S sumAvgDecimalState](
	mp *mpool.MPool,
	aggID int64,
	param types.Type,
) AggFuncExec {
	exec := &sumAvgDecExec[S, S]{
		isSum:        true,
		localAddSafe: false,
	}
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId:      aggID,
		argTypes:   []types.Type{param},
		retType:    param,
		stateTypes: []types.Type{param},
		emptyNull:  true,
	}
	return exec
}

func makeInternalSumCombineExec(
	mp *mpool.MPool,
	aggID int64,
	param types.Type,
) (AggFuncExec, error) {
	switch param.Oid {
	case types.T_int64:
		return newInternalNumericCombineExec[int64](
			mp, aggID, param, true, int64OfCheck), nil
	case types.T_uint64:
		return newInternalNumericCombineExec[uint64](
			mp, aggID, param, true, uint64OfCheck), nil
	case types.T_float64:
		return newInternalNumericCombineExec[float64](
			mp, aggID, param, true, float64OfCheck), nil
	case types.T_decimal128:
		return newInternalDecimalCombineExec[types.Decimal128](
			mp, aggID, param), nil
	case types.T_decimal256:
		return newInternalDecimalCombineExec[types.Decimal256](
			mp, aggID, param), nil
	default:
		return nil, moerr.NewInternalErrorNoCtxf(
			"unsupported internal SUM partial type %s", param.Oid)
	}
}

func makeInternalCountCombineExec(
	mp *mpool.MPool,
	aggID int64,
	param types.Type,
) (AggFuncExec, error) {
	if param.Oid != types.T_int64 {
		return nil, moerr.NewInternalErrorNoCtxf(
			"unsupported internal COUNT partial type %s", param.Oid)
	}
	return newInternalNumericCombineExec[int64](
		mp, aggID, param, false, func(int64, int64, int64) error { return nil }), nil
}

func newInternalAvgCombineExec[T internalCombineValue](
	mp *mpool.MPool,
	aggID int64,
	params []types.Type,
	add internalCombineAdd[T],
) AggFuncExec {
	exec := &internalAvgCombineExec[T]{add: add}
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId:      aggID,
		argTypes:   params,
		retType:    params[2],
		stateTypes: []types.Type{params[0], types.T_int64.ToType()},
		emptyNull:  true,
	}
	return exec
}

func makeInternalAvgCombineExec(
	mp *mpool.MPool,
	aggID int64,
	params []types.Type,
) (AggFuncExec, error) {
	if len(params) != 3 || params[1].Oid != types.T_int64 {
		return nil, moerr.NewInternalErrorNoCtx(
			"internal AVG combine requires sum, count, and result-type arguments")
	}
	validResult := false
	switch params[0].Oid {
	case types.T_int64, types.T_uint64, types.T_float64:
		validResult = params[2].Oid == types.T_float64
	case types.T_decimal128:
		validResult = params[2].Oid == types.T_decimal128
	case types.T_decimal256:
		validResult = params[2].Oid == types.T_decimal256
	}
	if !validResult {
		return nil, moerr.NewInternalErrorNoCtxf(
			"internal AVG partial type %s cannot produce %s",
			params[0].Oid, params[2].Oid)
	}
	switch params[0].Oid {
	case types.T_int64:
		return newInternalAvgCombineExec[int64](
			mp, aggID, params, internalCheckedInt64Add), nil
	case types.T_uint64:
		return newInternalAvgCombineExec[uint64](
			mp, aggID, params, internalCheckedUint64Add), nil
	case types.T_float64:
		return newInternalAvgCombineExec[float64](
			mp, aggID, params, internalFloat64Add), nil
	case types.T_decimal128:
		return newInternalAvgCombineExec[types.Decimal128](
			mp, aggID, params, decimalStateAdd[types.Decimal128]), nil
	case types.T_decimal256:
		return newInternalAvgCombineExec[types.Decimal256](
			mp, aggID, params, decimalStateAdd[types.Decimal256]), nil
	default:
		return nil, moerr.NewInternalErrorNoCtxf(
			"unsupported internal AVG partial type %s", params[0].Oid)
	}
}
