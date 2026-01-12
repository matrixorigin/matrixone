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
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type bitOp int8

const (
	bitXor bitOp = iota
	bitAnd
	bitOr
)

type bitOpExecFixed[T types.Ints | types.UInts] struct {
	aggExec
	op bitOp
}

type bitOpExecBytes struct {
	aggExec
	op bitOp
}

func (op bitOp) compute(a, b uint64) uint64 {
	switch op {
	case bitXor:
		return a ^ b
	case bitAnd:
		return a & b
	case bitOr:
		return a | b
	default:
		panic(moerr.NewInternalErrorNoCtxf("unsupported bit operation %d", op))
	}
}

func (op bitOp) computeBytes(a, b []byte) ([]byte, error) {
	var err error
	switch op {
	case bitXor:
		err = types.BitXor(a, a, b)
	case bitAnd:
		err = types.BitAnd(a, a, b)
	case bitOr:
		err = types.BitOr(a, a, b)
	default:
		panic(moerr.NewInternalErrorNoCtxf("unsupported bit operation %d", op))
	}
	return a, err
}

func (exec *bitOpExecFixed[T]) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *bitOpExecFixed[T]) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *bitOpExecFixed[T]) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}

		idx := uint64(i) + uint64(offset)
		if vectors[0].IsNull(idx) {
			continue
		} else {
			x, y := exec.getXY(grp - 1)
			value := vector.GetFixedAtNoTypeCheck[T](vectors[0], int(idx))
			aggs := vector.MustFixedColNoTypeCheck[uint64](exec.state[x].vecs[0])
			if exec.state[x].vecs[0].IsNull(uint64(y)) {
				exec.state[x].vecs[0].UnsetNull(uint64(y))
				aggs[y] = uint64(value)
			} else {
				aggs[y] = exec.op.compute(aggs[y], uint64(value))
			}
		}
	}
	return nil
}

func (exec *bitOpExecFixed[T]) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *bitOpExecFixed[T]) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*bitOpExecFixed[T])
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}

		x1, y1 := exec.getXY(grp - 1)
		x2, y2 := other.getXY(uint64(offset + i))
		aggs1 := vector.MustFixedColNoTypeCheck[uint64](exec.state[x1].vecs[0])
		aggs2 := vector.MustFixedColNoTypeCheck[uint64](other.state[x2].vecs[0])

		if other.state[x2].vecs[0].IsNull(uint64(y2)) {
			continue
		}
		if exec.state[x1].vecs[0].IsNull(uint64(y1)) {
			exec.state[x1].vecs[0].UnsetNull(uint64(y1))
			aggs1[y1] = aggs2[y2]
		} else {
			aggs1[y1] = exec.op.compute(aggs1[y1], aggs2[y2])
		}
	}
	return nil
}

func (exec *bitOpExecFixed[T]) SetExtraInformation(partialResult any, _ int) error {
	return nil
}

func (exec *bitOpExecFixed[T]) Flush() ([]*vector.Vector, error) {
	// transfer vector to result
	vecs := make([]*vector.Vector, len(exec.state))
	for i := range vecs {
		vecs[i] = exec.state[i].vecs[0]
		exec.state[i].vecs[0] = nil
		exec.state[i].length = 0
		exec.state[i].capacity = 0
	}
	return vecs, nil
}

func (exec *bitOpExecBytes) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *bitOpExecBytes) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *bitOpExecBytes) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
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
				// computeBytes will update oldValue in place
				_, err := exec.op.computeBytes(oldValue, value)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (exec *bitOpExecBytes) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *bitOpExecBytes) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*bitOpExecBytes)
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
			exec.state[x1].vecs[0].UnsetNull(uint64(y1))
			otherValue := other.state[x2].vecs[0].GetBytesAt(int(y2))
			vector.SetBytesAt(exec.state[x1].vecs[0], int(y1), otherValue, exec.mp)
		} else {
			oldValue := exec.state[x1].vecs[0].GetBytesAt(int(y1))
			otherValue := other.state[x2].vecs[0].GetBytesAt(int(y2))
			// computeBytes will update oldValue in place
			_, err := exec.op.computeBytes(oldValue, otherValue)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (exec *bitOpExecBytes) SetExtraInformation(partialResult any, _ int) error {
	return nil
}

func (exec *bitOpExecBytes) Flush() ([]*vector.Vector, error) {
	// transfer vector to result
	vecs := make([]*vector.Vector, len(exec.state))
	for i := range vecs {
		vecs[i] = exec.state[i].vecs[0]
		exec.state[i].vecs[0] = nil
		exec.state[i].length = 0
		exec.state[i].capacity = 0
	}
	return vecs, nil
}

func makeBitOpExecFixed[T types.Ints | types.UInts](mp *mpool.MPool, id int64, param types.Type, op bitOp) AggFuncExec {
	var bitOp bitOpExecFixed[T]
	bitOp.mp = mp
	bitOp.op = op
	bitOp.aggInfo = aggInfo{
		aggId:      id,
		isDistinct: false,
		argTypes:   []types.Type{param},
		retType:    types.T_uint64.ToType(),
		stateTypes: []types.Type{types.T_uint64.ToType()},
		emptyNull:  true,
		saveArg:    false,
	}
	return &bitOp
}

func makeBitOpExecBytes(mp *mpool.MPool, id int64, param types.Type, op bitOp) AggFuncExec {
	var bitOp bitOpExecBytes
	bitOp.mp = mp
	bitOp.op = op
	bitOp.aggInfo = aggInfo{
		aggId:      id,
		isDistinct: false,
		argTypes:   []types.Type{param},
		retType:    param,
		stateTypes: []types.Type{param},
		emptyNull:  true,
		saveArg:    false,
	}
	return &bitOp
}

func makeBitOpExec(mp *mpool.MPool, id int64, isDistinct bool, param types.Type, op bitOp) AggFuncExec {
	if isDistinct {
		// mysql documents says bitop does not support distinct
		panic(moerr.NewInternalErrorNoCtx("distinct bit operations are not supported"))
	}

	switch param.Oid {
	case types.T_int8:
		return makeBitOpExecFixed[int8](mp, id, param, op)
	case types.T_int16:
		return makeBitOpExecFixed[int16](mp, id, param, op)
	case types.T_int32:
		return makeBitOpExecFixed[int32](mp, id, param, op)
	case types.T_int64:
		return makeBitOpExecFixed[int64](mp, id, param, op)
	case types.T_uint8:
		return makeBitOpExecFixed[uint8](mp, id, param, op)
	case types.T_uint16:
		return makeBitOpExecFixed[uint16](mp, id, param, op)
	case types.T_uint32:
		return makeBitOpExecFixed[uint32](mp, id, param, op)
	case types.T_uint64:
		return makeBitOpExecFixed[uint64](mp, id, param, op)
	case types.T_bit:
		return makeBitOpExecFixed[uint64](mp, id, param, op)
	case types.T_binary:
		return makeBitOpExecBytes(mp, id, param, op)
	case types.T_varbinary:
		return makeBitOpExecBytes(mp, id, param, op)
	default:
		panic(moerr.NewInternalErrorNoCtxf("unsupported parameter type %s for bit operations", param.String()))
	}
}

func makeBitXorExec(mp *mpool.MPool, id int64, isDistinct bool, param types.Type) AggFuncExec {
	return makeBitOpExec(mp, id, isDistinct, param, bitXor)
}

func makeBitAndExec(mp *mpool.MPool, id int64, isDistinct bool, param types.Type) AggFuncExec {
	return makeBitOpExec(mp, id, isDistinct, param, bitAnd)
}

func makeBitOrExec(mp *mpool.MPool, id int64, isDistinct bool, param types.Type) AggFuncExec {
	return makeBitOpExec(mp, id, isDistinct, param, bitOr)
}
