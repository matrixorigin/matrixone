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
	"math/big"
	"sort"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const orderedPercentileConfigVersion byte = 1

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

func PercentileContReturnType(args []types.Type) types.Type {
	if len(args) == 0 {
		return types.T_float64.ToType()
	}
	if args[0].IsDecimal() {
		return types.New(types.T_decimal128, 38, args[0].Scale+1)
	}
	return types.T_float64.ToType()
}

func PercentileDiscReturnType(args []types.Type) types.Type {
	if len(args) == 0 {
		return types.T_float64.ToType()
	}
	return args[0]
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
}

func newOrderedPercentileExec[T numeric | types.Decimal64 | types.Decimal128, R types.FixedSizeTExceptStrType](
	mp *mpool.MPool, info singleAggInfo, mode orderedPercentileMode, initial R,
) *orderedPercentileExec[T, R] {
	return &orderedPercentileExec[T, R]{
		medianColumnExecSelf: newMedianColumnExecSelf[T, R](mp, info, initial),
		mode:                 mode,
	}
}

func (exec *orderedPercentileExec[T, R]) SetExtraInformation(partialResult any, groupIndex int) error {
	b, ok := partialResult.([]byte)
	if !ok {
		return moerr.NewInternalErrorNoCtx("ordered percentile: expected []byte config")
	}
	if len(b) >= 2 && b[0] == orderedPercentileConfigVersion {
		if b[1] > 1 {
			return moerr.NewInvalidInputNoCtx("ordered percentile: invalid sort direction")
		}
		exec.descending = b[1] == 1
		b = b[2:]
	} else {
		// Keep direct executor tests and old serialized plans readable when the
		// config contains only the percentile text.
		exec.descending = false
	}
	text := string(b)
	if text == "" {
		return moerr.NewInvalidInputNoCtx("ordered percentile: percentile is empty")
	}
	p, ok := new(big.Rat).SetString(text)
	if !ok || p.Sign() < 0 || p.Cmp(big.NewRat(1, 1)) > 0 {
		return moerr.NewInvalidInputNoCtxf("ordered percentile: percentile must be in [0,1], got %q", text)
	}
	if _, err := strconv.ParseFloat(text, 64); err != nil {
		return moerr.NewInvalidInputNoCtxf("ordered percentile: invalid percentile %q", text)
	}
	exec.percentile = p
	return nil
}

func (exec *orderedPercentileExec[T, R]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*orderedPercentileExec[T, R])
	if exec.percentile != nil && other.percentile != nil && exec.percentile.Cmp(other.percentile) != 0 {
		return moerr.NewInvalidInputNoCtx("ordered percentile: cannot merge different percentile configurations")
	}
	if exec.descending != other.descending {
		return moerr.NewInvalidInputNoCtx("ordered percentile: cannot merge different sort directions")
	}
	return exec.medianColumnExecSelf.Merge(&other.medianColumnExecSelf, groupIdx1, groupIdx2)
}

func (exec *orderedPercentileExec[T, R]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*orderedPercentileExec[T, R])
	if exec.percentile != nil && other.percentile != nil && exec.percentile.Cmp(other.percentile) != 0 {
		return moerr.NewInvalidInputNoCtx("ordered percentile: cannot merge different percentile configurations")
	}
	if exec.descending != other.descending {
		return moerr.NewInvalidInputNoCtx("ordered percentile: cannot merge different sort directions")
	}
	return exec.medianColumnExecSelf.BatchMerge(&other.medianColumnExecSelf, offset, groups)
}

func (exec *orderedPercentileExec[T, R]) Flush() ([]*vector.Vector, error) {
	if exec.percentile == nil {
		return nil, moerr.NewInternalErrorNoCtx("ordered percentile: percentile configuration is not set")
	}
	groups := len(exec.groups)
	lim := exec.ret.getChunkSize()
	for i, x := 0, 0; i < groups; i += lim {
		n := groups - i
		if n > lim {
			n = lim
		}
		for j := 0; j < n; j++ {
			group := exec.groups[i+j]
			if group.Length() == 0 {
				continue
			}
			markMedianGroupNotEmpty(&exec.ret, x, j)
			if err := exec.flushGroup(group, x, j); err != nil {
				return nil, err
			}
		}
		x++
	}
	return exec.ret.flushAll(), nil
}

func (exec *orderedPercentileExec[T, R]) flushGroup(group *Vectors[T], x, y int) error {
	values := collectMedianValues(group)
	compare := func(a, b T) int {
		result := compareOrderedPercentileValue(a, b)
		if exec.descending {
			return -result
		}
		return result
	}
	sort.Slice(values, func(i, j int) bool { return compare(values[i], values[j]) < 0 })

	lo, hi, frac := orderedPercentileRanks(uint64(len(values)), exec.percentile, exec.mode)
	if exec.mode == orderedPercentileDiscrete {
		return exec.setDiscreteResult(values[int(lo)], x, y)
	}
	return exec.setContinuousResult(values[int(lo)], values[int(hi)], frac, x, y)
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

func interpolateOrderedNumericValue[T numeric | types.Decimal64 | types.Decimal128](lo, hi T, frac *big.Rat) float64 {
	switch lv := any(lo).(type) {
	case int8:
		return interpolateNumeric(lv, any(hi).(int8), frac)
	case int16:
		return interpolateNumeric(lv, any(hi).(int16), frac)
	case int32:
		return interpolateNumeric(lv, any(hi).(int32), frac)
	case int64:
		return interpolateNumeric(lv, any(hi).(int64), frac)
	case uint8:
		return interpolateNumeric(lv, any(hi).(uint8), frac)
	case uint16:
		return interpolateNumeric(lv, any(hi).(uint16), frac)
	case uint32:
		return interpolateNumeric(lv, any(hi).(uint32), frac)
	case uint64:
		return interpolateNumeric(lv, any(hi).(uint64), frac)
	case float32:
		return interpolateNumeric(lv, any(hi).(float32), frac)
	case float64:
		return interpolateNumeric(lv, any(hi).(float64), frac)
	default:
		panic("unsupported ordered percentile numeric type")
	}
}

func orderedPercentileRanks(count uint64, p *big.Rat, mode orderedPercentileMode) (lo, hi uint64, frac *big.Rat) {
	if mode == orderedPercentileDiscrete {
		rank := new(big.Rat).Mul(p, new(big.Rat).SetInt(new(big.Int).SetUint64(count)))
		ceil := new(big.Int).Quo(rank.Num(), rank.Denom())
		if new(big.Int).Mod(rank.Num(), rank.Denom()).Sign() != 0 {
			ceil.Add(ceil, big.NewInt(1))
		}
		if ceil.Sign() == 0 {
			return 0, 0, new(big.Rat)
		}
		ceil.Sub(ceil, big.NewInt(1))
		return ceil.Uint64(), ceil.Uint64(), new(big.Rat)
	}
	return percentileRanks(count, p)
}

func makeOrderedPercentileExec(mp *mpool.MPool, aggID int64, isDistinct bool, param types.Type, mode orderedPercentileMode) (AggFuncExec, error) {
	if isDistinct {
		return nil, moerr.NewNotSupportedNoCtx("ordered percentile in distinct mode")
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
		return nil, moerr.NewInternalErrorNoCtx("unsupported type for ordered percentile")
	}
}

func compareOrderedPercentileValue[T numeric | types.Decimal64 | types.Decimal128](a, b T) int {
	switch av := any(a).(type) {
	case int8:
		return orderedCompare(av, any(b).(int8))
	case int16:
		return orderedCompare(av, any(b).(int16))
	case int32:
		return orderedCompare(av, any(b).(int32))
	case int64:
		return orderedCompare(av, any(b).(int64))
	case uint8:
		return orderedCompare(av, any(b).(uint8))
	case uint16:
		return orderedCompare(av, any(b).(uint16))
	case uint32:
		return orderedCompare(av, any(b).(uint32))
	case uint64:
		return orderedCompare(av, any(b).(uint64))
	case float32:
		return orderedCompare(av, any(b).(float32))
	case float64:
		return orderedCompare(av, any(b).(float64))
	case types.Decimal64:
		return av.Compare(any(b).(types.Decimal64))
	case types.Decimal128:
		return av.Compare(any(b).(types.Decimal128))
	default:
		panic("unsupported ordered percentile type")
	}
}
