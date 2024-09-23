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

func NewWithSchema(ro bool, attrs []string, attTypes []types.Type) *Batch {
	bat := New(ro, attrs)
	for i, t := range attTypes {
		bat.Vecs[i] = vector.NewVec(t)
	}
	return bat
}

func SetLength(bat *Batch, n int) {
	for _, vec := range bat.Vecs {
		vec.SetLength(n)
	}
	bat.rowCount = n
}

func (bat *Batch) MarshalBinary() ([]byte, error) {
	// --------------------------------------------------------------------
	// | len | Zs... | len | Vecs... | len | Attrs... | len | AggInfos... |
	// --------------------------------------------------------------------
	var buf bytes.Buffer

	// row count.
	rl := int64(bat.rowCount)
	buf.Write(types.EncodeInt64(&rl))

	// Vecs
	l := int32(len(bat.Vecs))
	buf.Write(types.EncodeInt32(&l))
	for i := 0; i < int(l); i++ {
		data, err := bat.Vecs[i].MarshalBinary()
		if err != nil {
			return nil, err
		}
		size := int32(len(data))
		buf.Write(types.EncodeInt32(&size))
		buf.Write(data)
	}

	// Attrs
	l = int32(len(bat.Attrs))
	buf.Write(types.EncodeInt32(&l))
	for i := 0; i < int(l); i++ {
		size := int32(len(bat.Attrs[i]))
		buf.Write(types.EncodeInt32(&size))
		n, _ := buf.WriteString(bat.Attrs[i])
		if int32(n) != size {
			panic("unexpected length for string")
		}
	}

	// AggInfos
	aggInfos := make([][]byte, len(bat.Aggs))
	for i, exec := range bat.Aggs {
		data, err := aggexec.MarshalAggFuncExec(exec)
		if err != nil {
			return nil, err
		}
		aggInfos[i] = data
	}

	l = int32(len(aggInfos))
	buf.Write(types.EncodeInt32(&l))
	for i := 0; i < int(l); i++ {
		size := int32(len(aggInfos[i]))
		buf.Write(types.EncodeInt32(&size))
		buf.Write(aggInfos[i])
	}

	buf.Write(types.EncodeInt32(&bat.Recursive))
	buf.Write(types.EncodeInt32(&bat.ShuffleIDX))

	return buf.Bytes(), nil
}

func (bat *Batch) UnmarshalBinary(data []byte) (err error) {
	return bat.UnmarshalBinaryWithAnyMp(data, nil)
}

func (bat *Batch) UnmarshalBinaryWithAnyMp(data []byte, mp *mpool.MPool) (err error) {
	bat.rowCount = int(types.DecodeInt64(data[:8]))
	data = data[8:]

	l := types.DecodeInt32(data[:4])
	// reuse bat mem
	firstTime := bat.Vecs == nil
	if firstTime {
		bat.Vecs = make([]*vector.Vector, l)
		for i := range bat.Vecs {
			bat.Vecs[i] = vector.NewVecFromReuse()
		}
	}
	vecs := bat.Vecs
	data = data[4:]

	for i := 0; i < int(l); i++ {
		size := types.DecodeInt32(data[:4])
		data = data[4:]

		if err := vecs[i].UnmarshalBinary(data[:size]); err != nil {
			return err
		}

		data = data[size:]
	}

	l = types.DecodeInt32(data[:4])
	if firstTime {
		bat.Attrs = make([]string, l)
	}
	data = data[4:]

	for i := 0; i < int(l); i++ {
		size := types.DecodeInt32(data[:4])
		data = data[4:]
		bat.Attrs[i] = string(data[:size])
		data = data[size:]
	}

	l = types.DecodeInt32(data[:4])
	aggs := make([][]byte, l)

	data = data[4:]
	for i := 0; i < int(l); i++ {
		size := types.DecodeInt32(data[:4])
		data = data[4:]
		aggs[i] = data[:size]
		data = data[size:]
	}

	bat.Recursive = types.DecodeInt32(data[:4])
	data = data[4:]
	bat.ShuffleIDX = types.DecodeInt32(data[:4])
	bat.Cnt = 1

	if len(aggs) > 0 {
		bat.Aggs = make([]aggexec.AggFuncExec, len(aggs))
		var aggMemoryManager aggexec.AggMemoryManager = nil
		if mp != nil {
			aggMemoryManager = aggexec.NewSimpleAggMemoryManager(mp)
		}
		for i, info := range aggs {
			if bat.Aggs[i], err = aggexec.UnmarshalAggFuncExec(aggMemoryManager, info); err != nil {
				return err
			}
		}
	}
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
	// situations that batch was still in use.
	// we use `!= 0` but not `>0` to avoid the situation that the batch was cleaned more than required.
	if bat == EmptyBatch || bat == CteEndBatch || atomic.AddInt64(&bat.Cnt, -1) != 0 {
		return
	}

	for i, vec := range bat.Vecs {
		if vec != nil {
			bat.ReplaceVector(vec, nil, i)
			vec.Free(m)
		}
	}
	for _, agg := range bat.Aggs {
		if agg != nil {
			agg.Free()
		}
	}
	bat.Aggs = nil
	bat.Vecs = nil
	bat.Attrs = nil
	bat.SetRowCount(0)
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
	logutil.Info("\n" + tag + "\n" + bat.String())
}

// Dup used to copy a Batch object, this method will create a new batch
// and copy all vectors (Vecs) of the current batch to the new batch.
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
		rvec.SetSorted(vec.GetSorted())
		rbat.SetVector(int32(j), rvec)
	}
	rbat.rowCount = bat.rowCount
	rbat.ShuffleIDX = bat.ShuffleIDX

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

	return rbat, nil
}

func (bat *Batch) Union(bat2 *Batch, offset, cnt int, m *mpool.MPool) error {
	for i, vec := range bat.Vecs {
		if err := vec.UnionBatch(bat2.Vecs[i], int64(offset), cnt, nil, m); err != nil {
			return err
		}
	}
	bat.rowCount += cnt
	return nil
}

func (bat *Batch) UnionOne(bat2 *Batch, pos int64, m *mpool.MPool) error {
	for i, vec := range bat.Vecs {
		if err := vec.UnionOne(bat2.Vecs[i], pos, m); err != nil {
			return err
		}
	}
	bat.rowCount++
	return nil
}

func (bat *Batch) PreExtend(m *mpool.MPool, rows int) error {
	for i := range bat.Vecs {
		if err := bat.Vecs[i].PreExtend(rows, m); err != nil {
			return err
		}
	}
	return nil
}

// AppendWithCopy is used to append data from batch `b` to another batch `bat`. The function
// ensures that the batch structure is consistent and copies all vector data to the target batch.
// WARING: this function will cause a memory allocation.
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

func (bat *Batch) ReplaceVector(oldVec *vector.Vector, newVec *vector.Vector, startIndex int) {
	for i := startIndex; i < len(bat.Vecs); i++ {
		if bat.Vecs[i] == oldVec {
			bat.SetVector(int32(i), newVec)
		}
	}
}

func (bat *Batch) IsEmpty() bool {
	return bat.rowCount == 0 && len(bat.Aggs) == 0
}

func (bat *Batch) IsDone() bool {
	if bat == nil {
		return true
	}
	return bat.IsEmpty() || bat.Last()
}

func (bat *Batch) Allocated() int {
	if bat == nil {
		return 0
	}
	ret := 0
	for i := range bat.Vecs {
		if bat.Vecs[i] != nil {
			ret += bat.Vecs[i].Allocated()
		}
	}
	return ret
}

func (bat *Batch) Window(start, end int) (*Batch, error) {
	b := NewWithSize(len(bat.Vecs))
	var err error
	for i, vec := range bat.Vecs {
		b.Vecs[i], err = vec.Window(start, end)
		if err != nil {
			return nil, err
		}
	}
	b.rowCount = end - start
	return b, nil
}
