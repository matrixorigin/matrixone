// Copyright 2021 Matrix Origin
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

package batch

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

func New(ro bool, attrs []string) *Batch {
	return &Batch{
		Ro:       ro,
		Cnt:      1,
		Attrs:    attrs,
		Vecs:     make([]*vector.Vector, len(attrs)),
		rowCount: 0,
	}
}

func NewWithSize(n int) *Batch {
	return &Batch{
		Cnt:      1,
		Vecs:     make([]*vector.Vector, n),
		rowCount: 0,
	}
}

func SetLength(bat *Batch, n int) {
	for _, vec := range bat.Vecs {
		vec.SetLength(n)
	}
	bat.rowCount = n
}

func (bat *Batch) MarshalBinary() ([]byte, error) {
	aggInfos := make([][]byte, len(bat.Aggs))
	for i, exec := range bat.Aggs {
		data, err := aggexec.MarshalAggFuncExec(exec)
		if err != nil {
			return nil, err
		}
		aggInfos[i] = data
	}

	return types.Encode(&EncodeBatch{
		rowCount:   int64(bat.rowCount),
		Vecs:       bat.Vecs,
		Attrs:      bat.Attrs,
		AggInfos:   aggInfos,
		Recursive:  bat.Recursive,
		ShuffleIdx: bat.ShuffleIDX,
	})
}

func (bat *Batch) UnmarshalBinary(data []byte) (err error) {
	return bat.unmarshalBinaryWithAnyMp(data, nil)
}

func (bat *Batch) UnmarshalBinaryWithCopy(data []byte, mp *mpool.MPool) error {
	return bat.unmarshalBinaryWithAnyMp(data, mp)
}

func (bat *Batch) unmarshalBinaryWithAnyMp(data []byte, mp *mpool.MPool) (err error) {
	rbat := new(EncodeBatch)
	if err = rbat.UnmarshalBinaryWithCopy(data, mp); err != nil {
		return err
	}

	bat.Recursive = rbat.Recursive
	bat.Cnt = 1
	bat.rowCount = int(rbat.rowCount)
	bat.Vecs = rbat.Vecs
	bat.Attrs = append(bat.Attrs, rbat.Attrs...)

	if len(rbat.AggInfos) > 0 {
		bat.Aggs = make([]aggexec.AggFuncExec, len(rbat.AggInfos))
		var aggMemoryManager aggexec.AggMemoryManager = nil
		if mp != nil {
			aggMemoryManager = aggexec.NewSimpleAggMemoryManager(mp)
		}

		for i, info := range rbat.AggInfos {
			if bat.Aggs[i], err = aggexec.UnmarshalAggFuncExec(aggMemoryManager, info); err != nil {
				return err
			}
		}
	}
	bat.ShuffleIDX = rbat.ShuffleIdx
	return nil
}

func (bat *Batch) Shrink(sels []int64, negate bool) {
	if !negate {
		if len(sels) == bat.rowCount {
			return
		}
	}
	for _, vec := range bat.Vecs {
		vec.Shrink(sels, negate)
	}
	if negate {
		bat.rowCount -= len(sels)
		return
	}
	bat.rowCount = len(sels)
}

func (bat *Batch) Shuffle(sels []int64, m *mpool.MPool) error {
	if len(sels) > 0 {
		mp := make(map[*vector.Vector]uint8)
		for _, vec := range bat.Vecs {
			if _, ok := mp[vec]; ok {
				continue
			}
			mp[vec]++
			if err := vec.Shuffle(sels, m); err != nil {
				return err
			}
		}
		bat.rowCount = len(sels)
	}
	return nil
}

func (bat *Batch) Size() int {
	var size int

	for _, vec := range bat.Vecs {
		size += vec.Size()
	}
	return size
}

func (bat *Batch) RowCount() int {
	return bat.rowCount
}

func (bat *Batch) VectorCount() int {
	return len(bat.Vecs)
}

func (bat *Batch) Prefetch(poses []int32, vecs []*vector.Vector) {
	for i, pos := range poses {
		vecs[i] = bat.GetVector(pos)
	}
}

func (bat *Batch) SetAttributes(attrs []string) {
	bat.Attrs = attrs
}

func (bat *Batch) SetVector(pos int32, vec *vector.Vector) {
	bat.Vecs[pos] = vec
}

func (bat *Batch) GetVector(pos int32) *vector.Vector {
	return bat.Vecs[pos]
}

func (bat *Batch) GetSubBatch(cols []string) *Batch {
	mp := make(map[string]int)
	for i, attr := range bat.Attrs {
		mp[attr] = i
	}
	rbat := NewWithSize(len(cols))
	for i, col := range cols {
		rbat.Vecs[i] = bat.Vecs[mp[col]]
	}
	rbat.rowCount = bat.rowCount
	return rbat
}

func (bat *Batch) Clean(m *mpool.MPool) {
	if bat == EmptyBatch {
		return
	}
	if atomic.LoadInt64(&bat.Cnt) == 0 {
		// panic("batch is already cleaned")
		return
	}
	if atomic.AddInt64(&bat.Cnt, -1) > 0 {
		return
	}
	for _, vec := range bat.Vecs {
		if vec != nil {
			vec.Free(m)
		}
	}
	for _, agg := range bat.Aggs {
		if agg != nil {
			agg.Free()
		}
	}
	bat.Attrs = nil
	bat.rowCount = 0
	bat.Vecs = nil
}

func (bat *Batch) Last() bool {
	return bat.Recursive > 0
}

func (bat *Batch) SetEnd() {
	bat.Recursive = 2
}

func (bat *Batch) SetLast() {
	bat.Recursive = 1
}

func (bat *Batch) End() bool {
	return bat.Recursive == 2
}

func (bat *Batch) CleanOnlyData() {
	for _, vec := range bat.Vecs {
		if vec != nil {
			vec.CleanOnlyData()
		}
	}
	bat.rowCount = 0
}

func (bat *Batch) String() string {
	var buf bytes.Buffer

	for i, vec := range bat.Vecs {
		buf.WriteString(fmt.Sprintf("%d : %s\n", i, vec.String()))
	}
	return buf.String()
}

func (bat *Batch) Log(tag string) {
	if bat == nil || bat.rowCount < 1 {
		return
	}
	logutil.Infof("\n" + tag + "\n" + bat.String())
}

func (bat *Batch) Dup(mp *mpool.MPool) (*Batch, error) {
	var err error

	rbat := NewWithSize(len(bat.Vecs))
	rbat.SetAttributes(bat.Attrs)
	rbat.Recursive = bat.Recursive
	for j, vec := range bat.Vecs {
		typ := *bat.GetVector(int32(j)).GetType()
		rvec := vector.NewVec(typ)
		if err = vector.GetUnionAllFunction(typ, mp)(rvec, vec); err != nil {
			rbat.Clean(mp)
			return nil, err
		}
		rbat.SetVector(int32(j), rvec)
	}
	rbat.rowCount = bat.rowCount

	//if len(bat.Aggs) > 0 {
	//	rbat.Aggs = make([]aggexec.AggFuncExec, len(bat.Aggs))
	//	aggMemoryManager := aggexec.NewSimpleAggMemoryManager(mp)
	//
	//	for i, agg := range bat.Aggs {
	//		rbat.Aggs[i], err = aggexec.CopyAggFuncExec(aggMemoryManager, agg)
	//		if err != nil {
	//			rbat.Clean(mp)
	//			return nil, err
	//		}
	//	}
	//}
	// if bat.AuxData != nil {
	// 	if m, ok := bat.AuxData.(*hashmap.JoinMap); ok {
	// rbat.AuxData = &hashmap.JoinMap{
	// 	cnt: m
	// }
	// 	}
	// }
	return rbat, nil
}

func (bat *Batch) PreExtend(m *mpool.MPool, rows int) error {
	for i := range bat.Vecs {
		if err := bat.Vecs[i].PreExtend(rows, m); err != nil {
			return err
		}
	}
	return nil
}

func (bat *Batch) AppendWithCopy(ctx context.Context, mh *mpool.MPool, b *Batch) (*Batch, error) {
	if bat == nil {
		return b.Dup(mh)
	}
	if len(bat.Vecs) != len(b.Vecs) {
		return nil, moerr.NewInternalError(ctx, "unexpected error happens in batch append")
	}
	if len(bat.Vecs) == 0 {
		return bat, nil
	}

	for i := range bat.Vecs {
		if err := bat.Vecs[i].UnionBatch(b.Vecs[i], 0, b.Vecs[i].Length(), nil, mh); err != nil {
			return bat, err
		}
		bat.Vecs[i].SetSorted(false)
	}
	bat.rowCount += b.rowCount
	return bat, nil
}

func (bat *Batch) Append(ctx context.Context, mh *mpool.MPool, b *Batch) (*Batch, error) {
	if bat == nil {
		return b, nil
	}
	if len(bat.Vecs) != len(b.Vecs) {
		return nil, moerr.NewInternalError(ctx, "unexpected error happens in batch append")
	}
	if len(bat.Vecs) == 0 {
		return bat, nil
	}

	for i := range bat.Vecs {
		if err := bat.Vecs[i].UnionBatch(b.Vecs[i], 0, b.Vecs[i].Length(), nil, mh); err != nil {
			return bat, err
		}
		bat.Vecs[i].SetSorted(false)
	}
	bat.rowCount += b.rowCount
	return bat, nil
}

func (bat *Batch) AddRowCount(rowCount int) {
	bat.rowCount += rowCount
}

func (bat *Batch) SetRowCount(rowCount int) {
	bat.rowCount = rowCount
}

func (bat *Batch) AddCnt(cnt int) {
	atomic.AddInt64(&bat.Cnt, int64(cnt))
}

// func (bat *Batch) SubCnt(cnt int) {
// 	atomic.StoreInt64(&bat.Cnt, bat.Cnt-int64(cnt))
// }

func (bat *Batch) SetCnt(cnt int64) {
	atomic.StoreInt64(&bat.Cnt, cnt)
}

func (bat *Batch) GetCnt() int64 {
	return atomic.LoadInt64(&bat.Cnt)
}

func (bat *Batch) ReplaceVector(oldVec *vector.Vector, newVec *vector.Vector) {
	for i, vec := range bat.Vecs {
		if vec == oldVec {
			bat.SetVector(int32(i), newVec)
		}
	}
}

func (bat *Batch) IsEmpty() bool {
	return bat.rowCount == 0 && bat.AuxData == nil && len(bat.Aggs) == 0
}

func (bat *Batch) DupJmAuxData() (ret *hashmap.JoinMap) {
	if bat.AuxData == nil {
		return
	}
	jm := bat.AuxData.(*hashmap.JoinMap)
	if jm.IsDup() {
		ret = jm.Dup()
	} else {
		ret = jm
		bat.AuxData = nil
	}
	return
}
