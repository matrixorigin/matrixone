// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggexec

import (
	"math"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// --- Public percentile computation wrappers ---

// PercentileNumeric computes the p-th percentile (0 <= p <= 1) of numeric values
// using O(n) quickselect with linear interpolation.
func PercentileNumeric[T numeric](vs *Vectors[T], p float64) (float64, error) {
	vals := collectMedianValues(vs)
	return percentileNumericVals(vals, p), nil
}

// PercentileDecimal64 computes the p-th percentile of Decimal64 values,
// returning a Decimal128 with argScale+1 (same convention as median).
func PercentileDecimal64(vs *Vectors[types.Decimal64], p float64, argScale int32) (types.Decimal128, error) {
	vals := collectMedianValues(vs)
	return percentileDecimal64Vals(vals, p, argScale)
}

// PercentileDecimal128 computes the p-th percentile of Decimal128 values,
// returning a Decimal128 with argScale+1.
func PercentileDecimal128(vs *Vectors[types.Decimal128], p float64, argScale int32) (types.Decimal128, error) {
	vals := collectMedianValues(vs)
	return percentileDecimal128Vals(vals, p, argScale)
}

// --- Core percentile computation algorithms ---

// percentileNumericVals computes the p-th percentile (0 <= p <= 1) of numeric vals
// using O(n) quickselect with linear interpolation.
func percentileNumericVals[T numeric](vals []T, p float64) float64 {
	N := len(vals)
	if N == 0 || p < 0 || p > 1 {
		return math.NaN()
	}
	if N == 1 {
		return float64(vals[0])
	}
	idx := p * float64(N-1)
	lo := int(idx)
	hi := lo + 1
	if hi >= N {
		hi = N - 1
	}

	vLo := selectKthNumeric(vals, lo)
	if lo == hi {
		return float64(vLo)
	}
	vHi := selectKthNumeric(vals, hi)
	frac := idx - float64(lo)
	return (1-frac)*float64(vLo) + frac*float64(vHi)
}

// percentileDecimal64Vals computes the p-th percentile of Decimal64 values.
func percentileDecimal64Vals(vals []types.Decimal64, p float64, argScale int32) (types.Decimal128, error) {
	N := len(vals)
	if N == 0 || p < 0 || p > 1 {
		return types.Decimal128{}, nil
	}
	if N == 1 {
		return FromD64ToD128(vals[0]).Scale(1)
	}
	idx := p * float64(N-1)
	lo := int(idx)
	hi := lo + 1
	if hi >= N {
		hi = N - 1
	}

	compare := func(a, b types.Decimal64) int { return a.Compare(b) }
	vLo := selectKthFunc(vals, lo, compare)
	if lo == hi {
		return FromD64ToD128(vLo).Scale(1)
	}
	vHi := selectKthFunc(vals, hi, compare)
	frac := idx - float64(lo)

	// Use float64-based interpolation for accuracy.
	fLo := types.Decimal64ToFloat64(vLo, argScale)
	fHi := types.Decimal64ToFloat64(vHi, argScale)
	result := fLo + frac*(fHi-fLo)
	return types.Decimal128FromFloat64(result, 38, argScale+1)
}

// percentileDecimal128Vals computes the p-th percentile of Decimal128 values.
func percentileDecimal128Vals(vals []types.Decimal128, p float64, argScale int32) (types.Decimal128, error) {
	N := len(vals)
	if N == 0 || p < 0 || p > 1 {
		return types.Decimal128{}, nil
	}
	if N == 1 {
		var err error
		ret := vals[0]
		if ret, err = ret.Scale(1); err != nil {
			return types.Decimal128{}, err
		}
		return ret, nil
	}
	idx := p * float64(N-1)
	lo := int(idx)
	hi := lo + 1
	if hi >= N {
		hi = N - 1
	}

	compare := func(a, b types.Decimal128) int { return a.Compare(b) }
	vLo := selectKthFunc(vals, lo, compare)
	if lo == hi {
		var err error
		if vLo, err = vLo.Scale(1); err != nil {
			return types.Decimal128{}, err
		}
		return vLo, nil
	}
	vHi := selectKthFunc(vals, hi, compare)
	frac := idx - float64(lo)

	// Use float64-based interpolation for accuracy.
	fLo := types.Decimal128ToFloat64(vLo, argScale)
	fHi := types.Decimal128ToFloat64(vHi, argScale)
	result := fLo + frac*(fHi-fLo)
	return types.Decimal128FromFloat64(result, 38, argScale+1)
}

// --- Executor types ---

type approxPercentileNumericExec[T numeric] struct {
	medianColumnExecSelf[T, float64]
	percentile float64
}

type approxPercentileDecimalExec[T types.Decimal64 | types.Decimal128] struct {
	medianColumnExecSelf[T, types.Decimal128]
	percentile float64
}

// parsePercentileConfig parses the percentile constant from config bytes.
func parsePercentileConfig(partialResult any) (float64, error) {
	b, ok := partialResult.([]byte)
	if !ok {
		return 0, moerr.NewInternalErrorNoCtx("approx_percentile: expected []byte config")
	}
	p, err := strconv.ParseFloat(string(b), 64)
	if err != nil {
		return 0, err
	}
	if p < 0 || p > 1 {
		return 0, moerr.NewInternalErrorNoCtxf("approx_percentile: percentile must be in [0,1], got %f", p)
	}
	return p, nil
}

// SetExtraInformation parses the percentile constant from config bytes.
func (exec *approxPercentileNumericExec[T]) SetExtraInformation(partialResult any, groupIndex int) error {
	var err error
	exec.percentile, err = parsePercentileConfig(partialResult)
	return err
}

func (exec *approxPercentileDecimalExec[T]) SetExtraInformation(partialResult any, groupIndex int) error {
	var err error
	exec.percentile, err = parsePercentileConfig(partialResult)
	return err
}

// Merge for Numeric executor.
func (exec *approxPercentileNumericExec[T]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*approxPercentileNumericExec[T])
	return exec.medianColumnExecSelf.Merge(&other.medianColumnExecSelf, groupIdx1, groupIdx2)
}

// BatchMerge for Numeric executor.
func (exec *approxPercentileNumericExec[T]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*approxPercentileNumericExec[T])
	return exec.medianColumnExecSelf.BatchMerge(&other.medianColumnExecSelf, offset, groups)
}

// Merge for Decimal executor.
func (exec *approxPercentileDecimalExec[T]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other := next.(*approxPercentileDecimalExec[T])
	return exec.medianColumnExecSelf.Merge(&other.medianColumnExecSelf, groupIdx1, groupIdx2)
}

// BatchMerge for Decimal executor.
func (exec *approxPercentileDecimalExec[T]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*approxPercentileDecimalExec[T])
	return exec.medianColumnExecSelf.BatchMerge(&other.medianColumnExecSelf, offset, groups)
}

// Flush for Numeric executor.
func (exec *approxPercentileNumericExec[T]) Flush() ([]*vector.Vector, error) {
	vs := exec.ret.values
	groups := len(exec.groups)
	lim := exec.ret.getChunkSize()
	for i, x := 0, 0; i < groups; i += lim {
		n := groups - i
		if n > lim {
			n = lim
		}
		s := i
		for j := 0; j < n; j++ {
			rows := exec.groups[s].Length()
			if rows == 0 {
				s++
				continue
			}
			markMedianGroupNotEmpty(&exec.ret, x, j)
			var err error
			if vs[x][j], err = PercentileNumeric(exec.groups[s], exec.percentile); err != nil {
				return nil, err
			}
			s++
		}
	}
	return exec.ret.flushAll(), nil
}

// Flush for Decimal executor.
func (exec *approxPercentileDecimalExec[T]) Flush() ([]*vector.Vector, error) {
	var err error
	vs := exec.ret.values
	argIsDecimal128 := exec.singleAggInfo.argType.Oid == types.T_decimal128

	groups := len(exec.groups)
	lim := exec.ret.getChunkSize()

	if argIsDecimal128 {
		for i, x := 0, 0; i < groups; i += lim {
			n := groups - i
			if n > lim {
				n = lim
			}
			s := i
			for j := 0; j < n; j++ {
				rows := exec.groups[s].Length()
				if rows == 0 {
					s++
					continue
				}
				markMedianGroupNotEmpty(&exec.ret, x, j)
				var v types.Decimal128
				v, err = PercentileDecimal128(any(exec.groups[s]).(*Vectors[types.Decimal128]), exec.percentile, exec.singleAggInfo.argType.Scale)
				if err != nil {
					return nil, err
				}
				vs[x][j] = v
				s++
			}
		}
	} else {
		for i, x := 0, 0; i < groups; i += lim {
			n := groups - i
			if n > lim {
				n = lim
			}
			s := i
			for j := 0; j < n; j++ {
				rows := exec.groups[s].Length()
				if rows == 0 {
					s++
					continue
				}
				markMedianGroupNotEmpty(&exec.ret, x, j)
				var v types.Decimal128
				v, err = PercentileDecimal64(any(exec.groups[s]).(*Vectors[types.Decimal64]), exec.percentile, exec.singleAggInfo.argType.Scale)
				if err != nil {
					return nil, err
				}
				vs[x][j] = v
				s++
			}
		}
	}

	return exec.ret.flushAll(), nil
}

func newApproxPercentileExec(mp *mpool.MPool, info singleAggInfo) (AggFuncExec, error) {
	if info.distinct {
		return nil, moerr.NewNotSupportedNoCtx("approx_percentile in distinct mode")
	}

	switch info.argType.Oid {
	case types.T_bit:
		return &approxPercentileNumericExec[uint64]{
			medianColumnExecSelf: newMedianColumnExecSelf[uint64, float64](mp, info, 0),
		}, nil
	case types.T_int8:
		return &approxPercentileNumericExec[int8]{
			medianColumnExecSelf: newMedianColumnExecSelf[int8, float64](mp, info, 0),
		}, nil
	case types.T_int16:
		return &approxPercentileNumericExec[int16]{
			medianColumnExecSelf: newMedianColumnExecSelf[int16, float64](mp, info, 0),
		}, nil
	case types.T_int32:
		return &approxPercentileNumericExec[int32]{
			medianColumnExecSelf: newMedianColumnExecSelf[int32, float64](mp, info, 0),
		}, nil
	case types.T_int64:
		return &approxPercentileNumericExec[int64]{
			medianColumnExecSelf: newMedianColumnExecSelf[int64, float64](mp, info, 0),
		}, nil
	case types.T_uint8:
		return &approxPercentileNumericExec[uint8]{
			medianColumnExecSelf: newMedianColumnExecSelf[uint8, float64](mp, info, 0),
		}, nil
	case types.T_uint16:
		return &approxPercentileNumericExec[uint16]{
			medianColumnExecSelf: newMedianColumnExecSelf[uint16, float64](mp, info, 0),
		}, nil
	case types.T_uint32:
		return &approxPercentileNumericExec[uint32]{
			medianColumnExecSelf: newMedianColumnExecSelf[uint32, float64](mp, info, 0),
		}, nil
	case types.T_uint64:
		return &approxPercentileNumericExec[uint64]{
			medianColumnExecSelf: newMedianColumnExecSelf[uint64, float64](mp, info, 0),
		}, nil
	case types.T_float32:
		return &approxPercentileNumericExec[float32]{
			medianColumnExecSelf: newMedianColumnExecSelf[float32, float64](mp, info, 0),
		}, nil
	case types.T_float64:
		return &approxPercentileNumericExec[float64]{
			medianColumnExecSelf: newMedianColumnExecSelf[float64, float64](mp, info, 0),
		}, nil
	case types.T_decimal64:
		return &approxPercentileDecimalExec[types.Decimal64]{
			medianColumnExecSelf: newMedianColumnExecSelf[types.Decimal64, types.Decimal128](mp, info, types.Decimal128{}),
		}, nil
	case types.T_decimal128:
		return &approxPercentileDecimalExec[types.Decimal128]{
			medianColumnExecSelf: newMedianColumnExecSelf[types.Decimal128, types.Decimal128](mp, info, types.Decimal128{}),
		}, nil
	}
	return nil, moerr.NewInternalErrorNoCtx("unsupported type for approx_percentile()")
}
