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

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vectorize/shuffle"
)

func New(ro bool, attrs []string) *Batch {
	return &Batch{
		Ro:    ro,
		Attrs: attrs,
		Vecs:  make([]*vector.Vector, len(attrs)),
	}
}

func Reorder(bat *Batch, attrs []string) {
	if bat.Ro {
		Cow(bat)
	}
	for i, name := range attrs {
		for j, attr := range bat.Attrs {
			if name == attr {
				bat.Vecs[i], bat.Vecs[j] = bat.Vecs[j], bat.Vecs[i]
				bat.Attrs[i], bat.Attrs[j] = bat.Attrs[j], bat.Attrs[i]
			}
		}
	}
}

func SetLength(bat *Batch, n int) {
	for _, vec := range bat.Vecs {
		vec.SetLength(n)
	}
	bat.Zs = bat.Zs[:n]
}

func Length(bat *Batch) int {
	return len(bat.Zs)
}

func Cow(bat *Batch) {
	attrs := make([]string, len(bat.Attrs))
	copy(attrs, bat.Attrs)
	bat.Ro = false
	bat.Attrs = attrs
}

func NewWithSize(n int) *Batch {
	return &Batch{
		Cnt:  1,
		Vecs: make([]*vector.Vector, n),
	}
}

func (info *aggInfo) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	i32 := int32(info.Op)
	buf.Write(types.EncodeInt32(&i32))
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
	info.Op = int(types.DecodeInt32(data[:4]))
	data = data[4:]
	info.Dist = types.DecodeBool(data[:1])
	data = data[1:]
	info.inputTypes = types.DecodeType(data[:types.TSize])
	data = data[types.TSize:]
	aggregate, err := agg.New(info.Op, info.Dist, info.inputTypes)
	if err != nil {
		return err
	}
	info.Agg = aggregate
	return types.Decode(data, info.Agg)
}

func (bat *Batch) MarshalBinary() ([]byte, error) {
	aggInfo := make([]aggInfo, len(bat.Aggs))
	for i := range aggInfo {
		aggInfo[i].Op = bat.Aggs[i].GetOperatorId()
		aggInfo[i].inputTypes = bat.Aggs[i].GetInputTypes()[0]
		aggInfo[i].Dist = bat.Aggs[i].IsDistinct()
		aggInfo[i].Agg = bat.Aggs[i]
	}
	return types.Encode(&EncodeBatch{
		Zs:       bat.Zs,
		Vecs:     bat.Vecs,
		Attrs:    bat.Attrs,
		AggInfos: aggInfo,
	})
}

func (bat *Batch) UnmarshalBinary(data []byte) error {
	rbat := new(EncodeBatch)

	if err := types.Decode(data, rbat); err != nil {
		return err
	}
	bat.Cnt = 1
	bat.Zs = rbat.Zs // if you drop rbat.Zs is ok, if you need return rbat,  you must deepcopy Zs.
	bat.Vecs = rbat.Vecs
	bat.Attrs = rbat.Attrs
	// initialize bat.Aggs only if necessary
	if len(rbat.AggInfos) > 0 {
		bat.Aggs = make([]agg.Agg[any], len(rbat.AggInfos))
		for i, info := range rbat.AggInfos {
			bat.Aggs[i] = info.Agg
		}
	}
	return nil
}

// I think Shrink should have a mpool!!!
func (bat *Batch) Shrink(sels []int64) {
	for _, vec := range bat.Vecs {
		vec.Shrink(sels, false)
	}
	vs := bat.Zs
	for i, sel := range sels {
		vs[i] = vs[sel]
	}
	bat.Zs = bat.Zs[:len(sels)]
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

		ws := make([]int64, len(sels))
		bat.Zs = shuffle.FixedLengthShuffle(bat.Zs, ws, sels)
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

func (bat *Batch) Length() int {
	return len(bat.Zs)
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
	rbat.Zs = append([]int64{}, bat.Zs...)
	return rbat
}

func (bat *Batch) Clean(m *mpool.MPool) {
	if atomic.AddInt64(&bat.Cnt, -1) != 0 {
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
	if len(bat.Zs) != 0 {
		m.PutSels(bat.Zs)
		bat.Zs = nil
	}
	bat.Vecs = nil
}

func (bat *Batch) CleanOnlyData() {
	for _, vec := range bat.Vecs {
		if vec != nil {
			vec.CleanOnlyData()
		}
	}
	if len(bat.Zs) != 0 {
		bat.Zs = bat.Zs[:0]
	}
}

func (bat *Batch) String() string {
	var buf bytes.Buffer

	for i, vec := range bat.Vecs {
		buf.WriteString(fmt.Sprintf("%v\n", i))
		if len(bat.Zs) > 0 {
			buf.WriteString(fmt.Sprintf("\t%s\n", vec))
		}
	}
	return buf.String()
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

	// XXX Here is a good place to trigger an panic for fault injection.
	// fault.AddFaultPoint("panic_in_batch_append", ":::", "PANIC", 0, "")
	fault.TriggerFault("panic_in_batch_append")

	for i := range bat.Vecs {
		if err := bat.Vecs[i].UnionBatch(b.Vecs[i], 0, b.Vecs[i].Length(), nil, mh); err != nil {
			return bat, err
		}
	}
	bat.Zs = append(bat.Zs, b.Zs...)
	return bat, nil
}

// XXX I will slowly remove all code that uses InitZsone.
func (bat *Batch) SetZs(len int, m *mpool.MPool) {
	bat.Zs = m.GetSels()
	for i := 0; i < len; i++ {
		bat.Zs = append(bat.Zs, 1)
	}
}

// InitZsOne init Batch.Zs and values are all 1
func (bat *Batch) InitZsOne(len int) {
	bat.Zs = make([]int64, len)
	for i := range bat.Zs {
		bat.Zs[i]++
	}
}

func (bat *Batch) AddCnt(cnt int) {
	atomic.AddInt64(&bat.Cnt, int64(cnt))
}

func (bat *Batch) SubCnt(cnt int) {
	atomic.StoreInt64(&bat.Cnt, bat.Cnt-int64(cnt))
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
	selsMp := make(map[int64]bool)
	for _, sel := range sels {
		selsMp[sel] = true
	}
	newSels := make([]int64, 0, bat.Length()-len(sels))
	for i := 0; i < bat.Length(); i++ {
		if ok := selsMp[int64(i)]; !ok {
			newSels = append(newSels, int64(i))
		}
	}
	mp := make(map[*vector.Vector]uint8)
	for _, vec := range bat.Vecs {
		if _, ok := mp[vec]; ok {
			continue
		}
		mp[vec]++
		vec.Shrink(newSels, false)
	}
	vs := bat.Zs
	for i, sel := range newSels {
		vs[i] = vs[sel]
	}
	bat.Zs = bat.Zs[:len(newSels)]
}
