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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

var AvgTwCacheSupportedTypes = []types.T{
	types.T_bit,
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_float32, types.T_float64,
	types.T_decimal64, types.T_decimal128,
}

func AvgTwCacheReturnType(typs []types.Type) types.Type {
	switch typs[0].Oid {
	case types.T_decimal64, types.T_decimal128:
		s := int32(12)
		if s < typs[0].Scale {
			s = typs[0].Scale
		}
		if s > typs[0].Scale+6 {
			s = typs[0].Scale + 6
		}
		return types.New(types.T_varchar, types.MaxVarcharLen, s)
	default:
		return types.T_char.ToType()
	}
}

type avgTwCacheNumericExec[A numeric] struct {
	aggExec
}

func newAvgTwCacheNumericExec[A numeric](mp *mpool.MPool, aggID int64, param types.Type) AggFuncExec {
	var exec avgTwCacheNumericExec[A]
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId:      aggID,
		isDistinct: false,
		argTypes:   []types.Type{param},
		retType:    AvgTwCacheReturnType([]types.Type{param}),
		stateTypes: []types.Type{types.T_float64.ToType(), types.T_int64.ToType()},
		emptyNull:  true,
		saveArg:    false,
	}
	return &exec
}

func (exec *avgTwCacheNumericExec[A]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *avgTwCacheNumericExec[A]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *avgTwCacheNumericExec[A]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}
		row := offset + i
		if vectors[0].IsConst() {
			row = 0
		}
		if vectors[0].IsNull(uint64(row)) {
			continue
		}

		x, y := exec.getXY(grp - 1)
		sumVec := exec.state[x].vecs[0]
		cntVec := exec.state[x].vecs[1]
		if sumVec.IsNull(uint64(y)) {
			sumVec.UnsetNull(uint64(y))
			cntVec.UnsetNull(uint64(y))
		}

		sums := vector.MustFixedColNoTypeCheck[float64](sumVec)
		cnts := vector.MustFixedColNoTypeCheck[int64](cntVec)
		sums[y] += float64(vector.GetFixedAtNoTypeCheck[A](vectors[0], row))
		cnts[y]++
	}
	return nil
}

func (exec *avgTwCacheNumericExec[A]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *avgTwCacheNumericExec[A]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*avgTwCacheNumericExec[A])
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}

		x1, y1 := exec.getXY(grp - 1)
		x2, y2 := other.getXY(uint64(offset + i))
		sumVec1, sumVec2 := exec.state[x1].vecs[0], other.state[x2].vecs[0]
		if sumVec2.IsNull(uint64(y2)) {
			continue
		}

		cntVec1, cntVec2 := exec.state[x1].vecs[1], other.state[x2].vecs[1]
		sums1 := vector.MustFixedColNoTypeCheck[float64](sumVec1)
		sums2 := vector.MustFixedColNoTypeCheck[float64](sumVec2)
		cnts1 := vector.MustFixedColNoTypeCheck[int64](cntVec1)
		cnts2 := vector.MustFixedColNoTypeCheck[int64](cntVec2)

		if sumVec1.IsNull(uint64(y1)) {
			sumVec1.UnsetNull(uint64(y1))
			cntVec1.UnsetNull(uint64(y1))
		}
		sums1[y1] += sums2[y2]
		cnts1[y1] += cnts2[y2]
	}
	return nil
}

func (exec *avgTwCacheNumericExec[A]) SetExtraInformation(partialResult any, _ int) error {
	return nil
}

func (exec *avgTwCacheNumericExec[A]) Flush() ([]*vector.Vector, error) {
	vecs := make([]*vector.Vector, len(exec.state))
	for i, st := range exec.state {
		vecs[i] = vector.NewOffHeapVecWithType(exec.retType)
		if err := vecs[i].PreExtend(int(st.length), exec.mp); err != nil {
			return nil, err
		}

		sums := vector.MustFixedColNoTypeCheck[float64](st.vecs[0])
		cnts := vector.MustFixedColNoTypeCheck[int64](st.vecs[1])
		for j := 0; j < int(st.length); j++ {
			if st.vecs[0].IsNull(uint64(j)) || cnts[j] == 0 {
				if err := vector.AppendBytes(vecs[i], nil, true, exec.mp); err != nil {
					return nil, err
				}
				continue
			}
			bs := make([]byte, 16)
			copy(bs[0:], types.EncodeFloat64(&sums[j]))
			copy(bs[8:], types.EncodeInt64(&cnts[j]))
			if err := vector.AppendBytes(vecs[i], bs, false, exec.mp); err != nil {
				return nil, err
			}
		}
	}
	return vecs, nil
}

type avgTwCacheDecimalExec[A types.Decimal64 | types.Decimal128] struct {
	aggExec
	scale int32
}

func newAvgTwCacheDecimalExec[A types.Decimal64 | types.Decimal128](mp *mpool.MPool, aggID int64, param types.Type) AggFuncExec {
	var exec avgTwCacheDecimalExec[A]
	exec.mp = mp
	exec.scale = param.Scale
	exec.aggInfo = aggInfo{
		aggId:      aggID,
		isDistinct: false,
		argTypes:   []types.Type{param},
		retType:    AvgTwCacheReturnType([]types.Type{param}),
		stateTypes: []types.Type{types.T_decimal128.ToType(), types.T_int64.ToType()},
		emptyNull:  true,
		saveArg:    false,
	}
	return &exec
}

func (exec *avgTwCacheDecimalExec[A]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *avgTwCacheDecimalExec[A]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *avgTwCacheDecimalExec[A]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}
		row := offset + i
		if vectors[0].IsConst() {
			row = 0
		}
		if vectors[0].IsNull(uint64(row)) {
			continue
		}

		x, y := exec.getXY(grp - 1)
		sumVec := exec.state[x].vecs[0]
		cntVec := exec.state[x].vecs[1]
		if sumVec.IsNull(uint64(y)) {
			sumVec.UnsetNull(uint64(y))
			cntVec.UnsetNull(uint64(y))
		}

		sums := vector.MustFixedColNoTypeCheck[types.Decimal128](sumVec)
		cnts := vector.MustFixedColNoTypeCheck[int64](cntVec)
		val := vector.GetFixedAtNoTypeCheck[A](vectors[0], row)
		var err error
		switch v := any(val).(type) {
		case types.Decimal64:
			sums[y], err = sums[y].Add64(v)
		case types.Decimal128:
			sums[y], err = sums[y].Add128(v)
		default:
			panic(moerr.NewInternalErrorNoCtx("unsupported avg_tw_cache decimal type"))
		}
		if err != nil {
			return err
		}
		cnts[y]++
	}
	return nil
}

func (exec *avgTwCacheDecimalExec[A]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *avgTwCacheDecimalExec[A]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*avgTwCacheDecimalExec[A])
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}

		x1, y1 := exec.getXY(grp - 1)
		x2, y2 := other.getXY(uint64(offset + i))
		sumVec1, sumVec2 := exec.state[x1].vecs[0], other.state[x2].vecs[0]
		if sumVec2.IsNull(uint64(y2)) {
			continue
		}

		cntVec1, cntVec2 := exec.state[x1].vecs[1], other.state[x2].vecs[1]
		sums1 := vector.MustFixedColNoTypeCheck[types.Decimal128](sumVec1)
		sums2 := vector.MustFixedColNoTypeCheck[types.Decimal128](sumVec2)
		cnts1 := vector.MustFixedColNoTypeCheck[int64](cntVec1)
		cnts2 := vector.MustFixedColNoTypeCheck[int64](cntVec2)

		if sumVec1.IsNull(uint64(y1)) {
			sumVec1.UnsetNull(uint64(y1))
			cntVec1.UnsetNull(uint64(y1))
			sums1[y1] = sums2[y2]
		} else {
			sum, err := sums1[y1].Add128(sums2[y2])
			if err != nil {
				return err
			}
			sums1[y1] = sum
		}
		cnts1[y1] += cnts2[y2]
	}
	return nil
}

func (exec *avgTwCacheDecimalExec[A]) SetExtraInformation(partialResult any, _ int) error {
	return nil
}

func (exec *avgTwCacheDecimalExec[A]) Flush() ([]*vector.Vector, error) {
	vecs := make([]*vector.Vector, len(exec.state))
	for i, st := range exec.state {
		vecs[i] = vector.NewOffHeapVecWithType(exec.retType)
		if err := vecs[i].PreExtend(int(st.length), exec.mp); err != nil {
			return nil, err
		}

		sums := vector.MustFixedColNoTypeCheck[types.Decimal128](st.vecs[0])
		cnts := vector.MustFixedColNoTypeCheck[int64](st.vecs[1])
		for j := 0; j < int(st.length); j++ {
			if st.vecs[0].IsNull(uint64(j)) || cnts[j] == 0 {
				if err := vector.AppendBytes(vecs[i], nil, true, exec.mp); err != nil {
					return nil, err
				}
				continue
			}
			bs := make([]byte, 28)
			copy(bs[0:], types.EncodeUint64(&sums[j].B0_63))
			copy(bs[8:], types.EncodeUint64(&sums[j].B64_127))
			copy(bs[16:], types.EncodeInt64(&cnts[j]))
			copy(bs[24:], types.EncodeInt32(&exec.scale))
			if err := vector.AppendBytes(vecs[i], bs, false, exec.mp); err != nil {
				return nil, err
			}
		}
	}
	return vecs, nil
}

func makeAvgTwCacheExec(mp *mpool.MPool, aggID int64, param types.Type) (AggFuncExec, error) {
	switch param.Oid {
	case types.T_bit, types.T_uint64:
		return newAvgTwCacheNumericExec[uint64](mp, aggID, param), nil
	case types.T_int8:
		return newAvgTwCacheNumericExec[int8](mp, aggID, param), nil
	case types.T_int16:
		return newAvgTwCacheNumericExec[int16](mp, aggID, param), nil
	case types.T_int32:
		return newAvgTwCacheNumericExec[int32](mp, aggID, param), nil
	case types.T_int64:
		return newAvgTwCacheNumericExec[int64](mp, aggID, param), nil
	case types.T_uint8:
		return newAvgTwCacheNumericExec[uint8](mp, aggID, param), nil
	case types.T_uint16:
		return newAvgTwCacheNumericExec[uint16](mp, aggID, param), nil
	case types.T_uint32:
		return newAvgTwCacheNumericExec[uint32](mp, aggID, param), nil
	case types.T_float32:
		return newAvgTwCacheNumericExec[float32](mp, aggID, param), nil
	case types.T_float64:
		return newAvgTwCacheNumericExec[float64](mp, aggID, param), nil
	case types.T_decimal64:
		return newAvgTwCacheDecimalExec[types.Decimal64](mp, aggID, param), nil
	case types.T_decimal128:
		return newAvgTwCacheDecimalExec[types.Decimal128](mp, aggID, param), nil
	default:
		return nil, moerr.NewInternalErrorNoCtxf("unsupported type '%v' for avg_tw_cache", param.Oid)
	}
}
