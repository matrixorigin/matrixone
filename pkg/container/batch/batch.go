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
	"fmt"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/vectorize/shuffle"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
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
		vector.SetLength(vec, n)
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

func (bat *Batch) MarshalBinary() ([]byte, error) {
	return types.Encode(&EncodeBatch{Zs: bat.Zs, Vecs: bat.Vecs})
}

func (bat *Batch) UnmarshalBinary(data []byte) error {
	rbat := new(EncodeBatch)
	if err := types.Decode(data, rbat); err != nil {
		return err
	}
	bat.Cnt = 1
	bat.Zs = rbat.Zs
	bat.Vecs = rbat.Vecs
	return nil
}

func (bat *Batch) ExpandNulls() {
	if len(bat.Zs) > 0 {
		for i := range bat.Vecs {
			bat.Vecs[i].TryExpandNulls(len(bat.Zs))
		}
	}
}

func (bat *Batch) Shrink(sels []int64) {
	mp := make(map[*vector.Vector]uint8)
	for _, vec := range bat.Vecs {
		if _, ok := mp[vec]; ok {
			continue
		}
		mp[vec]++
		vector.Shrink(vec, sels)
	}
	vs := bat.Zs
	for i, sel := range sels {
		vs[i] = vs[sel]
	}
	bat.Zs = bat.Zs[:len(sels)]
}

func (bat *Batch) Shuffle(sels []int64, m *mheap.Mheap) error {
	if len(sels) > 0 {
		mp := make(map[*vector.Vector]uint8)
		for _, vec := range bat.Vecs {
			if _, ok := mp[vec]; ok {
				continue
			}
			mp[vec]++
			if err := vector.Shuffle(vec, sels, m); err != nil {
				return err
			}
		}
		ws := m.GetSels()
		if cap(ws) < len(bat.Zs) {
			ws = make([]int64, len(bat.Zs))
		}
		ws = ws[:len(bat.Zs)]
		bat.Zs = shuffle.Int64Shuffle(bat.Zs, ws, sels)
		m.PutSels(ws)
	}
	return nil
}

func (bat *Batch) Size() int {
	var size int

	for _, vec := range bat.Vecs {
		size += len(vec.Data)
	}
	return size
}

func (bat *Batch) Length() int {
	return len(bat.Zs)
}

func (bat *Batch) Prefetch(poses []int32, vecs []*vector.Vector) {
	for i, pos := range poses {
		vecs[i] = bat.GetVector(pos)
	}
}

func (bat *Batch) GetVector(pos int32) *vector.Vector {
	return bat.Vecs[pos]
}

func (bat *Batch) Clean(m *mheap.Mheap) {
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
	}
	bat.Vecs = nil
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

func (bat *Batch) Append(mh *mheap.Mheap, b *Batch) (*Batch, error) {
	if bat == nil {
		return b, nil
	}
	if len(bat.Vecs) != len(b.Vecs) {
		return nil, errors.New(errno.InternalError, "unexpected error happens in batch append")
	}
	if len(bat.Vecs) == 0 {
		return bat, nil
	}
	flags := make([]uint8, vector.Length(b.Vecs[0]))
	for i := range flags {
		flags[i]++
	}
	for i := range bat.Vecs {
		if err := vector.UnionBatch(bat.Vecs[i], b.Vecs[i], 0, vector.Length(b.Vecs[i]), flags[:vector.Length(b.Vecs[i])], mh); err != nil {
			return bat, err
		}
	}
	bat.Zs = append(bat.Zs, b.Zs...)
	return bat, nil
}

// InitZsOne init Batch.Zs and values are all 1
func (bat *Batch) InitZsOne(len int) {
	bat.Zs = make([]int64, len)
	for i := range bat.Zs {
		bat.Zs[i]++
	}
}
