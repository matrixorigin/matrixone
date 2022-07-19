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

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
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
	for _, r := range bat.Rs {
		r.SetLength(n)
	}
	bat.Zs = bat.Zs[:n]
}

func Shrink(bat *Batch, sels []int64) {
	for _, vec := range bat.Vecs {
		vector.Shrink(vec, sels)
	}
	for _, r := range bat.Rs {
		r.Shrink(sels)
	}
	vs := bat.Zs
	for i, sel := range sels {
		vs[i] = vs[sel]
	}
	bat.Zs = bat.Zs[:len(sels)]
}

func Shuffle(bat *Batch, m *mheap.Mheap) error {
	if bat.SelsData != nil {
		for _, vec := range bat.Vecs {
			if err := vector.Shuffle(vec, bat.Sels, m); err != nil {
				return err
			}
		}
		for _, r := range bat.Rs {
			if err := r.Shuffle(bat.Sels, m); err != nil {
				return err
			}
		}
		data, err := mheap.Alloc(m, int64(len(bat.Zs))*8)
		if err != nil {
			return err
		}
		ws := encoding.DecodeInt64Slice(data)
		bat.Zs = shuffle.Int64Shuffle(bat.Zs, ws, bat.Sels)
		mheap.Free(m, data)
		mheap.Free(m, bat.SelsData)
		bat.Sels = nil
		bat.SelsData = nil
	}
	return nil
}

func Length(bat *Batch) int {
	return len(bat.Zs)
}

func Prefetch(bat *Batch, attrs []string, vecs []*vector.Vector) {
	for i, attr := range attrs {
		vecs[i] = GetVector(bat, attr)
	}
}

func GetVector(bat *Batch, name string) *vector.Vector {
	for i, attr := range bat.Attrs {
		if attr != name {
			continue
		}
		return bat.Vecs[i]
	}
	return nil
}

func GetVectorIndex(bat *Batch, name string) int {
	for i, attr := range bat.Attrs {
		if attr != name {
			continue
		}
		return i
	}
	return -1
}

func Clean(bat *Batch, m *mheap.Mheap) {
	if bat.SelsData != nil {
		mheap.Free(m, bat.SelsData)
		bat.Sels = nil
		bat.SelsData = nil
	}
	for _, vec := range bat.Vecs {
		if vec != nil {
			vector.Clean(vec, m)
		}
	}
	bat.Vecs = nil
	for _, r := range bat.Rs {
		r.Free(m)
	}
	bat.Rs = nil
	bat.As = nil
	bat.Zs = nil
}

func Reduce(bat *Batch, attrs []string, m *mheap.Mheap) {
	if bat.Ro {
		Cow(bat)
	}
	for _, attr := range attrs {
		for i := 0; i < len(bat.Attrs); i++ {
			if bat.Attrs[i] != attr {
				continue
			}
			if bat.Vecs[i].Ref != 0 {
				vector.Free(bat.Vecs[i], m)
			}
			if bat.Vecs[i].Ref == 0 {
				bat.Vecs = append(bat.Vecs[:i], bat.Vecs[i+1:]...)
				bat.Attrs = append(bat.Attrs[:i], bat.Attrs[i+1:]...)
				i--
			}
			break
		}
	}
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
		data, err := mheap.Alloc(m, int64(len(bat.Zs))*8)
		if err != nil {
			return err
		}
		ws := encoding.DecodeInt64Slice(data)
		bat.Zs = shuffle.Int64Shuffle(bat.Zs, ws, sels)
		mheap.Free(m, data)
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
			vector.Clean(vec, m)
		}
	}
	for _, r := range bat.Rs {
		if r != nil {
			r.Free(m)
		}
	}
	bat.Vecs = nil
	bat.Zs = nil
}

func (bat *Batch) String() string {
	var buf bytes.Buffer

	if len(bat.Sels) > 0 {
		fmt.Printf("%v\n", bat.Sels)
	}
	for i, attr := range bat.Attrs {
		buf.WriteString(fmt.Sprintf("%s\n", attr))
		if len(bat.Zs) > 0 {
			buf.WriteString(fmt.Sprintf("\t%s\n", bat.Vecs[i]))
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
			return nil, err
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
