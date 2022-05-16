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
	"fmt"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 10
)

func TestBatch(t *testing.T) {
	mp := mheap.New(guest.New(1<<30, host.New(1<<30)))
	bat0 := newBatch(t, []types.Type{{Oid: types.T_int8}}, mp)
	bat1 := newBatch(t, []types.Type{{Oid: types.T_int8}}, mp)
	Reorder(bat0, []int32{0})
	SetLength(bat0, 10)
	sels := []int64{1, 2, 3}
	Shrink(bat0, sels)
	err := Shuffle(bat1, sels, mp)
	require.NoError(t, err)
	{
		vecs := make([]*vector.Vector, 1)
		Prefetch(bat0, []int32{0}, vecs)
	}
	fmt.Printf("%v\n", bat0.String())
	_, err = bat0.Append(mp, bat1)
	require.NoError(t, err)
	bat0.InitZsOne(Length(bat0))
	Clean(bat0, mp)
	Clean(bat1, mp)
}

// create a new block based on the attribute information, flg indicates if the data is all duplicated
func newBatch(t *testing.T, ts []types.Type, mp *mheap.Mheap) *Batch {
	bat := New(len(ts))
	bat.Zs = make([]int64, Rows)
	for i := range bat.Zs {
		bat.Zs[i] = 1
	}
	for i := range bat.Vecs {
		vec := vector.New(ts[i])
		switch vec.Typ.Oid {
		case types.T_int8:
			data, err := mheap.Alloc(mp, Rows*1)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeInt8Slice(vec.Data)[:Rows]
			for i := range vs {
				vs[i] = int8(i)
			}
			vec.Col = vs
		case types.T_int64:
			data, err := mheap.Alloc(mp, Rows*8)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeInt64Slice(vec.Data)[:Rows]
			for i := range vs {
				vs[i] = int64(i)
			}
			vec.Col = vs
		case types.T_float64:
			data, err := mheap.Alloc(mp, Rows*8)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeFloat64Slice(vec.Data)[:Rows]
			for i := range vs {
				vs[i] = float64(i)
			}
			vec.Col = vs
		case types.T_date:
			data, err := mheap.Alloc(mp, Rows*4)
			require.NoError(t, err)
			vec.Data = data
			vs := encoding.DecodeDateSlice(vec.Data)[:Rows]
			for i := range vs {
				vs[i] = types.Date(i)
			}
			vec.Col = vs
		case types.T_char, types.T_varchar:
			size := 0
			vs := make([][]byte, Rows)
			for i := range vs {
				vs[i] = []byte(strconv.Itoa(i))
				size += len(vs[i])
			}
			data, err := mheap.Alloc(mp, int64(size))
			require.NoError(t, err)
			data = data[:0]
			col := new(types.Bytes)
			o := uint32(0)
			for _, v := range vs {
				data = append(data, v...)
				col.Offsets = append(col.Offsets, o)
				o += uint32(len(v))
				col.Lengths = append(col.Lengths, uint32(len(v)))
			}
			col.Data = data
			vec.Col = col
			vec.Data = data
		}
		bat.Vecs[i] = vec
	}
	return bat
}
