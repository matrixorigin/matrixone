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

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/vectorize/shuffle"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

func New(n int) *Batch {
	return &Batch{
		Vecs: make([]*vector.Vector, n),
	}
}

func Reorder(bat *Batch, poses []int32) {
	for i, pos := range poses {
		bat.Vecs[i], bat.Vecs[pos] = bat.Vecs[pos], bat.Vecs[i]
	}
}

func SetLength(bat *Batch, n int) {
	for _, vec := range bat.Vecs {
		vector.SetLength(vec, n)
	}
	bat.Zs = bat.Zs[:n]
}

func Shrink(bat *Batch, sels []int64) {
	for _, vec := range bat.Vecs {
		vector.Shrink(vec, sels)
	}
	vs := bat.Zs
	for i, sel := range sels {
		vs[i] = vs[sel]
	}
	bat.Zs = bat.Zs[:len(sels)]
}

func Shuffle(bat *Batch, sels []int64, m *mheap.Mheap) error {
	if len(sels) > 0 {
		for _, vec := range bat.Vecs {
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

func Length(bat *Batch) int {
	return len(bat.Zs)
}

func Prefetch(bat *Batch, poses []int32, vecs []*vector.Vector) {
	for i, pos := range poses {
		vecs[i] = GetVector(bat, pos)
	}
}

func GetVector(bat *Batch, pos int32) *vector.Vector {
	return bat.Vecs[pos]
}

func Clean(bat *Batch, m *mheap.Mheap) {
	for _, vec := range bat.Vecs {
		if vec != nil {
			vector.Clean(vec, m)
		}
	}
	for _, r := range bat.Rs {
		r.Free(m)
	}
	bat.Vecs = nil
	bat.Zs = nil
}

func (bat *Batch) String() string {
	var buf bytes.Buffer

	for i, vec := range bat.Vecs {
		buf.WriteString(fmt.Sprintf("%v:\n", i))
		if len(bat.Zs) > 0 {
			buf.WriteString(fmt.Sprintf("\t%s\n", vec))
		}
	}
	return buf.String()
}

func (bat *Batch) Append(mp *mheap.Mheap, b *Batch) (*Batch, error) {
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
		if err := vector.UnionBatch(bat.Vecs[i], b.Vecs[i], 0, vector.Length(b.Vecs[i]), flags[:vector.Length(b.Vecs[i])], mp); err != nil {
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
