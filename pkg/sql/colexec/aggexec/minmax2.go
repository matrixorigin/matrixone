// Copyright 2021 - 2022 Matrix Origin
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
	"bytes"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type minMaxExecFixed[T types.FixedSizeT] struct {
	aggExec
	comp     func(T, T) int
	hasExtra bool
	extra    T
}

type minMaxExecBytes struct {
	aggExec
	comp     func([]byte, []byte) int
	hasExtra bool
	extra    []byte
}

func (exec *minMaxExecFixed[T]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *minMaxExecFixed[T]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *minMaxExecFixed[T]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}

		idx := uint64(i) + uint64(offset)
		if vectors[0].IsNull(idx) {
			continue
		} else {
			x, y := exec.getXY(grp - 1)
			aggs := vector.MustFixedColNoTypeCheck[T](exec.state[x].vecs[0])
			value := vector.GetFixedAtNoTypeCheck[T](vectors[0], int(idx))
			if exec.state[x].vecs[0].IsNull(uint64(y)) {
				exec.state[x].vecs[0].UnsetNull(uint64(y))
				aggs[y] = value
			} else {
				if exec.comp(value, aggs[y]) < 0 {
					aggs[y] = value
				}
			}
		}
	}
	return nil
}

func (exec *minMaxExecFixed[T]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *minMaxExecFixed[T]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*minMaxExecFixed[T])
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}

		x1, y1 := exec.getXY(grp - 1)
		x2, y2 := other.getXY(uint64(offset + i))

		aggs1 := vector.MustFixedColNoTypeCheck[T](exec.state[x1].vecs[0])
		aggs2 := vector.MustFixedColNoTypeCheck[T](other.state[x2].vecs[0])

		if other.state[x2].vecs[0].IsNull(uint64(y2)) {
			continue
		}
		if exec.state[x1].vecs[0].IsNull(uint64(y1)) {
			exec.state[x1].vecs[0].UnsetNull(uint64(y1))
			aggs1[y1] = aggs2[y2]
		} else if exec.comp(aggs2[y2], aggs1[y1]) < 0 {
			aggs1[y1] = aggs2[y2]
		}
	}
	return nil
}

func (exec *minMaxExecFixed[T]) SetExtraInformation(partialResult any, _ int) error {
	var ok bool
	exec.extra, ok = partialResult.(T)
	if !ok {
		return moerr.NewInternalErrorNoCtxf("invalid extra information type %T", partialResult)
	}
	exec.hasExtra = true
	return nil
}

func (exec *minMaxExecFixed[T]) Flush() ([]*vector.Vector, error) {
	// transfer vector to result
	vecs := make([]*vector.Vector, len(exec.state))
	for i := range vecs {
		vecs[i] = exec.state[i].vecs[0]
		exec.state[i].vecs[0] = nil
		exec.state[i].length = 0
		exec.state[i].capacity = 0
	}

	if exec.hasExtra {
		for _, vec := range vecs {
			for i := range vec.Length() {
				if vec.IsNull(uint64(i)) {
					vec.UnsetNull(uint64(i))
					vector.SetFixedAtNoTypeCheck(vec, int(i), exec.extra)
				} else {
					oldValue := vector.GetFixedAtNoTypeCheck[T](vec, int(i))
					if exec.comp(exec.extra, oldValue) < 0 {
						vector.SetFixedAtNoTypeCheck(vec, int(i), exec.extra)
					}
				}
			}
		}
	}
	return vecs, nil
}

func (exec *minMaxExecBytes) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *minMaxExecBytes) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *minMaxExecBytes) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}

		idx := uint64(i) + uint64(offset)
		if vectors[0].IsNull(idx) {
			continue
		} else {
			x, y := exec.getXY(grp - 1)
			value := vectors[0].GetBytesAt(int(idx))
			if exec.state[x].vecs[0].IsNull(uint64(y)) {
				exec.state[x].vecs[0].UnsetNull(uint64(y))
				vector.SetBytesAt(exec.state[x].vecs[0], int(y), value, exec.mp)
			} else {
				oldValue := exec.state[x].vecs[0].GetBytesAt(int(y))
				if exec.comp(value, oldValue) < 0 {
					vector.SetBytesAt(exec.state[x].vecs[0], int(y), value, exec.mp)
				}
			}
		}
	}
	return nil
}

func (exec *minMaxExecBytes) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *minMaxExecBytes) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*minMaxExecBytes)
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}

		x1, y1 := exec.getXY(grp - 1)
		x2, y2 := other.getXY(uint64(offset + i))

		if other.state[x2].vecs[0].IsNull(uint64(y2)) {
			continue
		}
		if exec.state[x1].vecs[0].IsNull(uint64(y1)) {
			value := other.state[x2].vecs[0].GetBytesAt(int(y2))
			exec.state[x1].vecs[0].UnsetNull(uint64(y1))
			vector.SetBytesAt(exec.state[x1].vecs[0], int(y1), value, exec.mp)
		} else {
			value := other.state[x2].vecs[0].GetBytesAt(int(y2))
			oldValue := exec.state[x1].vecs[0].GetBytesAt(int(y1))
			if exec.comp(value, oldValue) < 0 {
				vector.SetBytesAt(exec.state[x1].vecs[0], int(y1), value, exec.mp)
			}
		}
	}
	return nil
}

func (exec *minMaxExecBytes) SetExtraInformation(partialResult any, _ int) error {
	var ok bool
	exec.extra, ok = partialResult.([]byte)
	if !ok {
		return moerr.NewInternalErrorNoCtxf("invalid extra information type %T", partialResult)
	}
	exec.hasExtra = true
	return nil
}

func (exec *minMaxExecBytes) Flush() ([]*vector.Vector, error) {
	// transfer vector to result
	vecs := make([]*vector.Vector, len(exec.state))
	for i := range vecs {
		vecs[i] = exec.state[i].vecs[0]
		exec.state[i].vecs[0] = nil
		exec.state[i].length = 0
		exec.state[i].capacity = 0
	}

	if exec.hasExtra {
		for _, vec := range vecs {
			for i := range vec.Length() {
				if vec.IsNull(uint64(i)) {
					vec.UnsetNull(uint64(i))
					vector.SetBytesAt(vec, int(i), exec.extra, exec.mp)
				} else {
					oldValue := vec.GetBytesAt(int(i))
					if exec.comp(exec.extra, oldValue) < 0 {
						vector.SetBytesAt(vec, int(i), exec.extra, exec.mp)
					}
				}
			}
		}
	}
	return vecs, nil
}

func makeMinMaxExec(mp *mpool.MPool, aggID int64, isMin bool, param types.Type) AggFuncExec {
	switch param.Oid {
	case types.T_bool:
		return newBoolMinMaxExec(mp, aggID, isMin, param)
	case types.T_bit:
		return newGenericMinMaxExec[uint64](mp, aggID, isMin, param)
	case types.T_int8:
		return newGenericMinMaxExec[int8](mp, aggID, isMin, param)
	case types.T_int16:
		return newGenericMinMaxExec[int16](mp, aggID, isMin, param)
	case types.T_int32:
		return newGenericMinMaxExec[int32](mp, aggID, isMin, param)
	case types.T_int64:
		return newGenericMinMaxExec[int64](mp, aggID, isMin, param)
	case types.T_uint8:
		return newGenericMinMaxExec[uint8](mp, aggID, isMin, param)
	case types.T_uint16:
		return newGenericMinMaxExec[uint16](mp, aggID, isMin, param)
	case types.T_uint32:
		return newGenericMinMaxExec[uint32](mp, aggID, isMin, param)
	case types.T_uint64:
		return newGenericMinMaxExec[uint64](mp, aggID, isMin, param)
	case types.T_float32:
		return newGenericMinMaxExec[float32](mp, aggID, isMin, param)
	case types.T_float64:
		return newGenericMinMaxExec[float64](mp, aggID, isMin, param)
	case types.T_date:
		return newGenericMinMaxExec[types.Date](mp, aggID, isMin, param)
	case types.T_datetime:
		return newGenericMinMaxExec[types.Datetime](mp, aggID, isMin, param)
	case types.T_time:
		return newGenericMinMaxExec[types.Time](mp, aggID, isMin, param)
	case types.T_timestamp:
		return newGenericMinMaxExec[types.Timestamp](mp, aggID, isMin, param)
	case types.T_year:
		return newGenericMinMaxExec[types.MoYear](mp, aggID, isMin, param)
	case types.T_decimal64:
		return newDecimal64MinMaxExec(mp, aggID, isMin, param)
	case types.T_decimal128:
		return newDecimal128MinMaxExec(mp, aggID, isMin, param)
	case types.T_uuid:
		return newUuidMinMaxExec(mp, aggID, isMin, param)
	case types.T_enum:
		return newGenericMinMaxExec[types.Enum](mp, aggID, isMin, param)
	case types.T_char, types.T_varchar, types.T_blob,
		types.T_binary, types.T_varbinary, types.T_json, types.T_text, types.T_datalink:
		return newStrMinMaxExec(mp, aggID, isMin, param)
	case types.T_array_float32, types.T_array_float64:
		return newArrayMinMaxExec(mp, aggID, isMin, param)
	}
	panic(moerr.NewInternalErrorNoCtxf("unsupported type '%v' for min/max", param.Oid))
}

func setupAggInfo(a *aggInfo, aggId int64, param types.Type) {
	*a = aggInfo{
		aggId:      aggId,
		isDistinct: false,
		argTypes:   []types.Type{param},
		retType:    param,
		stateTypes: []types.Type{param},
		emptyNull:  true,
		saveArg:    false,
	}
}

func newBoolMinMaxExec(mp *mpool.MPool, aggID int64, isMin bool, param types.Type) AggFuncExec {
	var exec minMaxExecFixed[bool]
	exec.mp = mp
	if isMin {
		exec.comp = types.BoolAscCompare
	} else {
		exec.comp = types.BoolDescCompare
	}
	setupAggInfo(&exec.aggInfo, aggID, param)
	return &exec
}

func newGenericMinMaxExec[T types.OrderedT](mp *mpool.MPool, aggID int64, isMin bool, param types.Type) AggFuncExec {
	var exec minMaxExecFixed[T]
	exec.mp = mp
	if isMin {
		exec.comp = types.GenericAscCompare[T]
	} else {
		exec.comp = types.GenericDescCompare[T]
	}
	setupAggInfo(&exec.aggInfo, aggID, param)
	return &exec
}

func newDecimal64MinMaxExec(mp *mpool.MPool, aggID int64, isMin bool, param types.Type) AggFuncExec {
	var exec minMaxExecFixed[types.Decimal64]
	exec.mp = mp
	if isMin {
		exec.comp = types.Decimal64AscCompare
	} else {
		exec.comp = types.Decimal64DescCompare
	}
	setupAggInfo(&exec.aggInfo, aggID, param)
	return &exec
}

func newDecimal128MinMaxExec(mp *mpool.MPool, aggID int64, isMin bool, param types.Type) AggFuncExec {
	var exec minMaxExecFixed[types.Decimal128]
	exec.mp = mp
	if isMin {
		exec.comp = types.Decimal128AscCompare
	} else {
		exec.comp = types.Decimal128DescCompare
	}
	setupAggInfo(&exec.aggInfo, aggID, param)
	return &exec
}

func newUuidMinMaxExec(mp *mpool.MPool, aggID int64, isMin bool, param types.Type) AggFuncExec {
	var exec minMaxExecFixed[types.Uuid]
	exec.mp = mp
	if isMin {
		exec.comp = types.UuidAscCompare
	} else {
		exec.comp = types.UuidDescCompare
	}
	setupAggInfo(&exec.aggInfo, aggID, param)
	return &exec
}

func newStrMinMaxExec(mp *mpool.MPool, aggID int64, isMin bool, param types.Type) AggFuncExec {
	var exec minMaxExecBytes
	exec.mp = mp
	if isMin {
		exec.comp = bytes.Compare
	} else {
		exec.comp = func(x, y []byte) int { return -bytes.Compare(x, y) }
	}
	setupAggInfo(&exec.aggInfo, aggID, param)
	return &exec
}

func newArrayMinMaxExec(mp *mpool.MPool, aggID int64, isMin bool, param types.Type) AggFuncExec {
	var exec minMaxExecBytes
	exec.mp = mp
	if isMin {
		switch param.Oid {
		case types.T_array_float32:
			exec.comp = func(x, y []byte) int { return types.CompareArrayFromBytes[float32](x, y, false) }
		case types.T_array_float64:
			exec.comp = func(x, y []byte) int { return types.CompareArrayFromBytes[float64](x, y, false) }
		default:
			panic("Unsupported array type")
		}
	} else {
		switch param.Oid {
		case types.T_array_float32:
			exec.comp = func(x, y []byte) int { return types.CompareArrayFromBytes[float32](x, y, true) }
		case types.T_array_float64:
			exec.comp = func(x, y []byte) int { return types.CompareArrayFromBytes[float64](x, y, true) }
		default:
			panic("Unsupported array type")
		}
	}
	setupAggInfo(&exec.aggInfo, aggID, param)
	return &exec
}
