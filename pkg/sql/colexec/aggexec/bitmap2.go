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

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type bmp struct {
	roaring.Bitmap
}

func (b *bmp) MarshalBinary() ([]byte, error) {
	return b.ToBytes()
}
func (b *bmp) UnmarshalBinary(data []byte) error {
	return b.Bitmap.UnmarshalBinary(data)
}

func makeBmp(mp *mpool.MPool) (*bmp, error) {
	return &bmp{}, nil
}

func makeBmpMarshalerUnmarshaler(mp *mpool.MPool) (MarshalerUnmarshaler, error) {
	return makeBmp(mp)
}

type bmpExecCommon struct {
	aggExec
}

func (e *bmpExecCommon) batchMerge(other *bmpExecCommon, offset int, groups []uint64) error {
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		x1, y1 := e.getXY(group - 1)
		x2, y2 := other.getXY(uint64(offset + i))
		if other.state[x2].mobs[y2] == nil {
			continue
		}
		if e.state[x1].mobs[y1] == nil {
			e.state[x1].mobs[y1] = other.state[x2].mobs[y2]
			other.state[x2].mobs[y2] = nil
		} else {
			mob1 := e.state[x1].mobs[y1].(*bmp)
			mob2 := other.state[x2].mobs[y2].(*bmp)
			mob1.Or(&mob2.Bitmap)
		}
	}
	return nil
}

func (e *bmpExecCommon) flush(typ types.Type) ([]*vector.Vector, error) {
	vecs := make([]*vector.Vector, len(e.state))
	for i, st := range e.state {
		vecs[i] = vector.NewOffHeapVecWithType(typ)
		vecs[i].PreExtend(int(st.length), e.mp)
		for j := 0; j < int(st.length); j++ {
			if st.mobs[j] == nil {
				vector.AppendNull(vecs[i], e.mp)
			} else {
				mob := st.mobs[j].(*bmp)
				bs, err := mob.MarshalBinary()
				if err != nil {
					return nil, err
				}
				vector.AppendBytes(vecs[i], bs, false, e.mp)
			}
		}
	}
	return vecs, nil
}

type bmpConstructExec struct {
	bmpExecCommon
}

func (e *bmpConstructExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return e.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (e *bmpConstructExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return e.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (e *bmpConstructExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}

		idx := uint64(i) + uint64(offset)
		if vectors[0].IsNull(idx) {
			continue
		} else {
			x, y := e.getXY(group - 1)
			value := vector.GetFixedAtNoTypeCheck[uint64](vectors[0], int(idx))
			if e.state[x].mobs[y] == nil {
				e.state[x].mobs[y], _ = makeBmp(e.mp)
			}
			mob := e.state[x].mobs[y].(*bmp)
			mob.Add(uint32(value))
		}
	}
	return nil
}

func (e *bmpConstructExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return e.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (e *bmpConstructExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*bmpConstructExec)
	return e.batchMerge(&other.bmpExecCommon, offset, groups)
}

func (e *bmpConstructExec) SetExtraInformation(partialResult any, _ int) error {
	return nil
}

func (e *bmpConstructExec) Flush() ([]*vector.Vector, error) {
	return e.flush(types.T_varbinary.ToType())
}

type bmpOrExec struct {
	bmpExecCommon
}

func (e *bmpOrExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return e.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (e *bmpOrExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return e.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (e *bmpOrExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}

		idx := uint64(i) + uint64(offset)
		if vectors[0].IsNull(idx) {
			continue
		} else {
			x, y := e.getXY(group - 1)
			bs := vectors[0].GetBytesAt(int(idx))
			if e.state[x].mobs[y] == nil {
				e.state[x].mobs[y], _ = makeBmp(e.mp)
			}
			mob := e.state[x].mobs[y].(*bmp)

			var mob2 bmp
			mob2.UnmarshalBinary(bs)
			mob.Or(&mob2.Bitmap)
		}
	}
	return nil
}

func (e *bmpOrExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return e.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (e *bmpOrExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*bmpOrExec)
	return e.batchMerge(&other.bmpExecCommon, offset, groups)
}

func (e *bmpOrExec) SetExtraInformation(partialResult any, _ int) error {
	return nil
}

func (e *bmpOrExec) Flush() ([]*vector.Vector, error) {
	return e.flush(types.T_varbinary.ToType())
}

func makeBmpOrExec(mp *mpool.MPool, id int64, param types.Type) *bmpOrExec {
	var exec bmpOrExec
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId:                    id,
		isDistinct:               false,
		argTypes:                 []types.Type{param},
		retType:                  param,
		stateTypes:               nil,
		emptyNull:                true,
		saveArg:                  false,
		makeMarshalerUnmarshaler: makeBmpMarshalerUnmarshaler,
	}
	return &exec
}

func makeBmpConstructExec(mp *mpool.MPool, id int64, param types.Type) *bmpConstructExec {
	var exec bmpConstructExec
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId:                    id,
		isDistinct:               false,
		argTypes:                 []types.Type{param},
		retType:                  types.T_varbinary.ToType(),
		stateTypes:               nil,
		emptyNull:                true,
		saveArg:                  false,
		makeMarshalerUnmarshaler: makeBmpMarshalerUnmarshaler,
	}
	return &exec
}
