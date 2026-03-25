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

var AvgTwResultSupportedTypes = []types.T{
	types.T_char, types.T_varchar,
}

func AvgTwResultReturnType(typs []types.Type) types.Type {
	switch typs[0].Oid {
	case types.T_varchar:
		return types.New(types.T_decimal128, 18, typs[0].Scale)
	default:
		return types.T_float64.ToType()
	}
}

type avgTwResultFloatExec struct {
	aggExec
}

func newAvgTwResultFloatExec(mp *mpool.MPool, aggID int64, param types.Type) AggFuncExec {
	var exec avgTwResultFloatExec
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId:      aggID,
		isDistinct: false,
		argTypes:   []types.Type{param},
		retType:    AvgTwResultReturnType([]types.Type{param}),
		stateTypes: []types.Type{types.T_float64.ToType(), types.T_int64.ToType()},
		emptyNull:  true,
		saveArg:    false,
	}
	return &exec
}

func (exec *avgTwResultFloatExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *avgTwResultFloatExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *avgTwResultFloatExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
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
		value := vectors[0].GetBytesAt(row)
		if len(value) < 16 {
			return moerr.NewInternalErrorNoCtx("avg_tw_result: invalid float cache payload")
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
		sums[y] += types.DecodeFloat64(value[0:])
		cnts[y] += types.DecodeInt64(value[8:])
	}
	return nil
}

func (exec *avgTwResultFloatExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *avgTwResultFloatExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*avgTwResultFloatExec)
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
		if sumVec1.IsNull(uint64(y1)) {
			sumVec1.UnsetNull(uint64(y1))
			cntVec1.UnsetNull(uint64(y1))
		}
		sums1 := vector.MustFixedColNoTypeCheck[float64](sumVec1)
		sums2 := vector.MustFixedColNoTypeCheck[float64](sumVec2)
		cnts1 := vector.MustFixedColNoTypeCheck[int64](cntVec1)
		cnts2 := vector.MustFixedColNoTypeCheck[int64](cntVec2)
		sums1[y1] += sums2[y2]
		cnts1[y1] += cnts2[y2]
	}
	return nil
}

func (exec *avgTwResultFloatExec) SetExtraInformation(partialResult any, _ int) error {
	return nil
}

func (exec *avgTwResultFloatExec) Flush() ([]*vector.Vector, error) {
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
				if err := vector.AppendFixed(vecs[i], float64(0), true, exec.mp); err != nil {
					return nil, err
				}
				continue
			}
			if err := vector.AppendFixed(vecs[i], sums[j]/float64(cnts[j]), false, exec.mp); err != nil {
				return nil, err
			}
		}
	}
	return vecs, nil
}

type avgTwResultDecimalExec struct {
	aggExec
}

func newAvgTwResultDecimalExec(mp *mpool.MPool, aggID int64, param types.Type) AggFuncExec {
	var exec avgTwResultDecimalExec
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId:      aggID,
		isDistinct: false,
		argTypes:   []types.Type{param},
		retType:    AvgTwResultReturnType([]types.Type{param}),
		stateTypes: []types.Type{types.T_decimal128.ToType(), types.T_int64.ToType(), types.T_int32.ToType()},
		emptyNull:  true,
		saveArg:    false,
	}
	return &exec
}

func (exec *avgTwResultDecimalExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *avgTwResultDecimalExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *avgTwResultDecimalExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
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
		value := vectors[0].GetBytesAt(row)
		if len(value) < 28 {
			return moerr.NewInternalErrorNoCtx("avg_tw_result: invalid decimal cache payload")
		}

		sum := types.Decimal128{
			B0_63:   types.DecodeUint64(value[0:]),
			B64_127: types.DecodeUint64(value[8:]),
		}
		cnt := types.DecodeInt64(value[16:])
		scale := types.DecodeInt32(value[24:])

		x, y := exec.getXY(grp - 1)
		sumVec := exec.state[x].vecs[0]
		cntVec := exec.state[x].vecs[1]
		scaleVec := exec.state[x].vecs[2]
		if sumVec.IsNull(uint64(y)) {
			sumVec.UnsetNull(uint64(y))
			cntVec.UnsetNull(uint64(y))
			scaleVec.UnsetNull(uint64(y))
		}
		sums := vector.MustFixedColNoTypeCheck[types.Decimal128](sumVec)
		cnts := vector.MustFixedColNoTypeCheck[int64](cntVec)
		scales := vector.MustFixedColNoTypeCheck[int32](scaleVec)
		var err error
		sums[y], err = sums[y].Add128(sum)
		if err != nil {
			return err
		}
		cnts[y] += cnt
		scales[y] = scale
	}
	return nil
}

func (exec *avgTwResultDecimalExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *avgTwResultDecimalExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*avgTwResultDecimalExec)
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
		scaleVec1, scaleVec2 := exec.state[x1].vecs[2], other.state[x2].vecs[2]
		if sumVec1.IsNull(uint64(y1)) {
			sumVec1.UnsetNull(uint64(y1))
			cntVec1.UnsetNull(uint64(y1))
			scaleVec1.UnsetNull(uint64(y1))
			vector.MustFixedColNoTypeCheck[types.Decimal128](sumVec1)[y1] = vector.MustFixedColNoTypeCheck[types.Decimal128](sumVec2)[y2]
			vector.MustFixedColNoTypeCheck[int32](scaleVec1)[y1] = vector.MustFixedColNoTypeCheck[int32](scaleVec2)[y2]
		} else {
			sum, err := vector.MustFixedColNoTypeCheck[types.Decimal128](sumVec1)[y1].Add128(vector.MustFixedColNoTypeCheck[types.Decimal128](sumVec2)[y2])
			if err != nil {
				return err
			}
			vector.MustFixedColNoTypeCheck[types.Decimal128](sumVec1)[y1] = sum
		}
		vector.MustFixedColNoTypeCheck[int64](cntVec1)[y1] += vector.MustFixedColNoTypeCheck[int64](cntVec2)[y2]
	}
	return nil
}

func (exec *avgTwResultDecimalExec) SetExtraInformation(partialResult any, _ int) error {
	return nil
}

func (exec *avgTwResultDecimalExec) Flush() ([]*vector.Vector, error) {
	vecs := make([]*vector.Vector, len(exec.state))
	for i, st := range exec.state {
		vecs[i] = vector.NewOffHeapVecWithType(exec.retType)
		if err := vecs[i].PreExtend(int(st.length), exec.mp); err != nil {
			return nil, err
		}

		sums := vector.MustFixedColNoTypeCheck[types.Decimal128](st.vecs[0])
		cnts := vector.MustFixedColNoTypeCheck[int64](st.vecs[1])
		scales := vector.MustFixedColNoTypeCheck[int32](st.vecs[2])
		for j := 0; j < int(st.length); j++ {
			if st.vecs[0].IsNull(uint64(j)) || cnts[j] == 0 {
				if err := vector.AppendFixed(vecs[i], types.Decimal128{}, true, exec.mp); err != nil {
					return nil, err
				}
				continue
			}
			v, _, err := sums[j].Div(types.Decimal128{B0_63: uint64(cnts[j])}, scales[j], 0)
			if err != nil {
				return nil, err
			}
			if err := vector.AppendFixed(vecs[i], v, false, exec.mp); err != nil {
				return nil, err
			}
		}
	}
	return vecs, nil
}

func makeAvgTwResultExec(mp *mpool.MPool, aggID int64, param types.Type) (AggFuncExec, error) {
	switch param.Oid {
	case types.T_char:
		return newAvgTwResultFloatExec(mp, aggID, param), nil
	case types.T_varchar:
		return newAvgTwResultDecimalExec(mp, aggID, param), nil
	default:
		return nil, moerr.NewInternalErrorNoCtxf("unsupported type '%v' for avg_tw_result", param.Oid)
	}
}
