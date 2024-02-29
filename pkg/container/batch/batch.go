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

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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

func (info *aggInfo) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(types.EncodeInt64(&info.Op))
	buf.Write(types.EncodeBool(&info.Dist))
	buf.Write(types.EncodeType(&info.inputTypes))
	data, err := types.Encode(info.Agg)
	if err != nil {
		return nil, err
	}
	buf.Write(data)
	return buf.Bytes(), nil
}

func (info *aggInfo) UnmarshalBinary(data []byte) error {
	info.Op = types.DecodeInt64(data[:8])
	data = data[8:]
	info.Dist = types.DecodeBool(data[:1])
	data = data[1:]
	info.inputTypes = types.DecodeType(data[:types.TSize])
	data = data[types.TSize:]
	aggregate, err := agg.NewAgg(info.Op, info.Dist, []types.Type{info.inputTypes})
	if err != nil {
		return err
	}
	info.Agg = aggregate
	return types.Decode(data, info.Agg)
}

func (bat *Batch) MarshalBinary() ([]byte, error) {
	aggInfos := make([]aggInfo, len(bat.Aggs))
	for i := range aggInfos {
		aggInfos[i].Op = bat.Aggs[i].GetOperatorId()
		aggInfos[i].inputTypes = bat.Aggs[i].InputTypes()[0]
		aggInfos[i].Dist = bat.Aggs[i].IsDistinct()
		aggInfos[i].Agg = bat.Aggs[i]
	}
	return types.Encode(&EncodeBatch{
		rowCount:  int64(bat.rowCount),
		Vecs:      bat.Vecs,
		Attrs:     bat.Attrs,
		AggInfos:  aggInfos,
		Recursive: bat.Recursive,
	})
}

func (bat *Batch) UnmarshalBinary(data []byte) error {
	rbat := new(EncodeBatch)

	if err := types.Decode(data, rbat); err != nil {
		return err
	}
	bat.Recursive = rbat.Recursive
	bat.Cnt = 1
	bat.rowCount = int(rbat.rowCount)
	bat.Vecs = rbat.Vecs
	bat.Attrs = append(bat.Attrs, rbat.Attrs...)
	// initialize bat.Aggs only if necessary
	if len(rbat.AggInfos) > 0 {
		bat.Aggs = make([]agg.Agg[any], len(rbat.AggInfos))
		for i, info := range rbat.AggInfos {
			bat.Aggs[i] = info.Agg
		}
	}
	return nil
}

func (bat *Batch) Shrink(sels []int64) {
	if len(sels) == bat.rowCount {
		return
	}
	for _, vec := range bat.Vecs {
		vec.Shrink(sels, false)
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
			agg.Free(m)
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
	rbat := NewWithSize(len(bat.Vecs))
	rbat.SetAttributes(bat.Attrs)
	rbat.Recursive = bat.Recursive
	for j, vec := range bat.Vecs {
		typ := *bat.GetVector(int32(j)).GetType()
		rvec := vector.NewVec(typ)
		if err := vector.GetUnionAllFunction(typ, mp)(rvec, vec); err != nil {
			rbat.Clean(mp)
			return nil, err
		}
		rbat.SetVector(int32(j), rvec)
	}
	rbat.rowCount = bat.rowCount

	// if len(bat.Aggs) > 0 {
	// 	rbat.Aggs = make([]agg.Agg[any], len(bat.Aggs))
	// 	for i, agg := range bat.Aggs {
	// 		rbat.Aggs[i] = agg.Dup(mp)
	// 	}
	// }
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

func (bat *Batch) AntiShrink(sels []int64) {
	for _, vec := range bat.Vecs {
		vec.Shrink(sels, true)
	}
	bat.rowCount -= len(sels)
}

func (bat *Batch) IsEmpty() bool {
	return bat.rowCount == 0 && bat.AuxData == nil
}

func (bat *Batch) DupJmAuxData() (ret *hashmap.JoinMap) {
	jm := bat.AuxData.(*hashmap.JoinMap)
	if jm.IsDup() {
		ret = jm.Dup()
	} else {
		ret = jm
		bat.AuxData = nil
	}
	return
}
