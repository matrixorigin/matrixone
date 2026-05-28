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
	"io"
	"slices"

	hll "github.com/axiomhq/hyperloglog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type hllSketch struct {
	*hll.Sketch
}

func makeHllSketch(_ *mpool.MPool) (MarshalerUnmarshaler, error) {
	return &hllSketch{Sketch: hll.NewNoSparse()}, nil
}

func (s *hllSketch) MarshalBinary() ([]byte, error) {
	return s.Sketch.MarshalBinary()
}

func (s *hllSketch) UnmarshalBinary(data []byte) error {
	if s.Sketch == nil {
		s.Sketch = hll.NewNoSparse()
	}
	return s.Sketch.UnmarshalBinary(data)
}

func (s *hllSketch) UnmarshalFromReader(r io.Reader) error {
	bs, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return s.UnmarshalBinary(bs)
}

func (s *hllSketch) mergeBytes(data []byte) error {
	other := &hllSketch{}
	if err := other.UnmarshalBinary(data); err != nil {
		return moerr.NewInvalidInputNoCtxf("invalid HLL sketch: %v", err)
	}
	if s.Sketch == nil {
		s.Sketch = hll.NewNoSparse()
	}
	return s.Merge(other.Sketch)
}

type approxCountExec struct {
	aggExec
}

func makeApproxCount(mp *mpool.MPool, id int64, arg types.Type) AggFuncExec {
	var exec approxCountExec
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId:                    id,
		isDistinct:               false,
		argTypes:                 []types.Type{arg},
		retType:                  types.T_uint64.ToType(),
		emptyNull:                false,
		saveArg:                  false,
		makeMarshalerUnmarshaler: makeHllSketch,
	}
	return &exec
}

func (exec *approxCountExec) GroupGrow(more int) error {
	start := exec.GetNumGroups()
	if err := exec.aggExec.GroupGrow(more); err != nil {
		return err
	}
	for i := start; i < start+more; i++ {
		x, y := exec.getXY(uint64(i))
		if exec.state[x].mobs[y] == nil {
			exec.state[x].mobs[y], _ = makeHllSketch(exec.mp)
		}
	}
	return nil
}

func (exec *approxCountExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *approxCountExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *approxCountExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}
		idx := offset + i
		if vectors[0].IsConst() {
			idx = 0
		}
		if vectors[0].IsNull(uint64(idx)) {
			continue
		}
		x, y := exec.getXY(grp - 1)
		exec.state[x].mobs[y].(*hllSketch).Insert(vectors[0].GetRawBytesAt(idx))
	}
	return nil
}

func (exec *approxCountExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *approxCountExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*approxCountExec)
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}
		x1, y1 := exec.getXY(grp - 1)
		x2, y2 := other.getXY(uint64(offset + i))
		if err := exec.state[x1].mobs[y1].(*hllSketch).Merge(other.state[x2].mobs[y2].(*hllSketch).Sketch); err != nil {
			return err
		}
	}
	return nil
}

func (exec *approxCountExec) SetExtraInformation(partialResult any, groupIndex int) error {
	return nil
}

func (exec *approxCountExec) Flush() (_ []*vector.Vector, retErr error) {
	vecs := make([]*vector.Vector, len(exec.state))
	defer func() {
		if retErr != nil {
			for _, v := range vecs {
				if v != nil {
					v.Free(exec.mp)
				}
			}
		}
	}()
	for i, st := range exec.state {
		vecs[i] = vector.NewOffHeapVecWithType(types.T_uint64.ToType())
		if err := vecs[i].PreExtend(int(st.length), exec.mp); err != nil {
			return nil, err
		}
		vecs[i].SetLength(int(st.length))
		vals := vector.MustFixedColNoTypeCheck[uint64](vecs[i])
		for j := 0; j < int(st.length); j++ {
			if st.mobs[j] != nil {
				vals[j] = st.mobs[j].(*hllSketch).Estimate()
			}
		}
	}
	return vecs, nil
}

func (exec *approxCountExec) Size() int64 {
	return hllStateSize(exec.state)
}

func (exec *approxCountExec) Free() {
	exec.aggExec.Free()
}

type hllAddExec struct {
	aggExec
}

func makeHllAdd(mp *mpool.MPool, id int64, arg types.Type) AggFuncExec {
	var exec hllAddExec
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId:                    id,
		isDistinct:               false,
		argTypes:                 []types.Type{arg},
		retType:                  types.T_varbinary.ToType(),
		emptyNull:                false,
		saveArg:                  false,
		makeMarshalerUnmarshaler: makeHllSketch,
	}
	return &exec
}

func (exec *hllAddExec) GroupGrow(more int) error {
	start := exec.GetNumGroups()
	if err := exec.aggExec.GroupGrow(more); err != nil {
		return err
	}
	for i := start; i < start+more; i++ {
		x, y := exec.getXY(uint64(i))
		if exec.state[x].mobs[y] == nil {
			exec.state[x].mobs[y], _ = makeHllSketch(exec.mp)
		}
	}
	return nil
}

func (exec *hllAddExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *hllAddExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *hllAddExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}
		idx := offset + i
		if vectors[0].IsConst() {
			idx = 0
		}
		if vectors[0].IsNull(uint64(idx)) {
			continue
		}
		x, y := exec.getXY(grp - 1)
		exec.state[x].mobs[y].(*hllSketch).Insert(vectors[0].GetRawBytesAt(idx))
	}
	return nil
}

func (exec *hllAddExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *hllAddExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*hllAddExec)
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}
		x1, y1 := exec.getXY(grp - 1)
		x2, y2 := other.getXY(uint64(offset + i))
		if err := exec.state[x1].mobs[y1].(*hllSketch).Merge(other.state[x2].mobs[y2].(*hllSketch).Sketch); err != nil {
			return err
		}
	}
	return nil
}

func (exec *hllAddExec) SetExtraInformation(partialResult any, groupIndex int) error {
	return nil
}

func (exec *hllAddExec) Flush() ([]*vector.Vector, error) {
	return flushHllSketches(&exec.aggExec)
}

func (exec *hllAddExec) Size() int64 {
	return hllStateSize(exec.state)
}

func (exec *hllAddExec) Free() {
	exec.aggExec.Free()
}

type hllMergeExec struct {
	aggExec
}

func makeHllMerge(mp *mpool.MPool, id int64, arg types.Type) AggFuncExec {
	var exec hllMergeExec
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId:                    id,
		isDistinct:               false,
		argTypes:                 []types.Type{arg},
		retType:                  types.T_varbinary.ToType(),
		emptyNull:                false,
		saveArg:                  false,
		makeMarshalerUnmarshaler: makeHllSketch,
	}
	return &exec
}

func (exec *hllMergeExec) GroupGrow(more int) error {
	start := exec.GetNumGroups()
	if err := exec.aggExec.GroupGrow(more); err != nil {
		return err
	}
	for i := start; i < start+more; i++ {
		x, y := exec.getXY(uint64(i))
		if exec.state[x].mobs[y] == nil {
			exec.state[x].mobs[y], _ = makeHllSketch(exec.mp)
		}
	}
	return nil
}

func (exec *hllMergeExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.BatchFill(row, []uint64{uint64(groupIndex + 1)}, vectors)
}

func (exec *hllMergeExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	return exec.BatchFill(0, slices.Repeat([]uint64{uint64(groupIndex + 1)}, vectors[0].Length()), vectors)
}

func (exec *hllMergeExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}
		idx := offset + i
		if vectors[0].IsConst() {
			idx = 0
		}
		if vectors[0].IsNull(uint64(idx)) {
			continue
		}
		x, y := exec.getXY(grp - 1)
		if err := exec.state[x].mobs[y].(*hllSketch).mergeBytes(vectors[0].GetBytesAt(idx)); err != nil {
			return err
		}
	}
	return nil
}

func (exec *hllMergeExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	return exec.BatchMerge(next, groupIdx2, []uint64{uint64(groupIdx1 + 1)})
}

func (exec *hllMergeExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	other := next.(*hllMergeExec)
	for i, grp := range groups {
		if grp == GroupNotMatched {
			continue
		}
		x1, y1 := exec.getXY(grp - 1)
		x2, y2 := other.getXY(uint64(offset + i))
		if err := exec.state[x1].mobs[y1].(*hllSketch).Merge(other.state[x2].mobs[y2].(*hllSketch).Sketch); err != nil {
			return err
		}
	}
	return nil
}

func (exec *hllMergeExec) SetExtraInformation(partialResult any, groupIndex int) error {
	return nil
}

func (exec *hllMergeExec) Flush() ([]*vector.Vector, error) {
	return flushHllSketches(&exec.aggExec)
}

func (exec *hllMergeExec) Size() int64 {
	return hllStateSize(exec.state)
}

func (exec *hllMergeExec) Free() {
	exec.aggExec.Free()
}

func flushHllSketches(exec *aggExec) (_ []*vector.Vector, retErr error) {
	vecs := make([]*vector.Vector, len(exec.state))
	defer func() {
		if retErr != nil {
			for _, v := range vecs {
				if v != nil {
					v.Free(exec.mp)
				}
			}
		}
	}()
	for i, st := range exec.state {
		vecs[i] = vector.NewOffHeapVecWithType(types.T_varbinary.ToType())
		if err := vecs[i].PreExtend(int(st.length), exec.mp); err != nil {
			return nil, err
		}
		for j := 0; j < int(st.length); j++ {
			bs, err := st.mobs[j].(*hllSketch).MarshalBinary()
			if err != nil {
				return nil, err
			}
			if err := vector.AppendBytes(vecs[i], bs, false, exec.mp); err != nil {
				return nil, err
			}
		}
	}
	return vecs, nil
}

func hllStateSize(states []aggState) int64 {
	var size int64
	for _, st := range states {
		size += int64(cap(st.mobs)) * 8
		for _, mob := range st.mobs {
			if mob != nil {
				if bs, err := mob.MarshalBinary(); err == nil {
					size += int64(len(bs))
				}
			}
		}
	}
	return size
}
