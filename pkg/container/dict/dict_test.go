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

package dict

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDict_InsertBatchFixedLen(t *testing.T) {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	m := mheap.New(gm)
	dict, err := New(types.Type{Oid: types.T_int64}, m)
	require.Nil(t, err)

	v0 := vector.New(types.Type{Oid: types.T_int64})
	v0.Data = types.EncodeInt64Slice([]int64{5, 2, 3, 7, 3, 2})
	v0.Col = types.DecodeInt64Slice(v0.Data)

	ips, err := dict.InsertBatch(v0)
	require.Nil(t, err)
	require.Equal(t, uint64(1), ips[0])
	require.Equal(t, uint64(2), ips[1])
	require.Equal(t, uint64(3), ips[2])
	require.Equal(t, uint64(4), ips[3])
	require.Equal(t, uint64(3), ips[4])
	require.Equal(t, uint64(2), ips[5])

	v1 := vector.New(types.Type{Oid: types.T_int64})
	v1.Data = types.EncodeInt64Slice([]int64{4, 2, 1, 5})
	v1.Col = types.DecodeInt64Slice(v1.Data)

	ips, err = dict.InsertBatch(v1)
	require.Nil(t, err)
	require.Equal(t, uint64(5), ips[0])
	require.Equal(t, uint64(2), ips[1])
	require.Equal(t, uint64(6), ips[2])
	require.Equal(t, uint64(1), ips[3])
}

func TestDict_FindBatchFixedLen(t *testing.T) {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	m := mheap.New(gm)
	dict, err := New(types.Type{Oid: types.T_int16}, m)
	require.Nil(t, err)

	v0 := vector.New(types.Type{Oid: types.T_int16})
	v0.Data = types.EncodeInt16Slice([]int16{5, 2, 3, 7, 1, 4})
	v0.Col = types.DecodeInt16Slice(v0.Data)

	ips, err := dict.InsertBatch(v0)
	require.Nil(t, err)
	require.Equal(t, uint64(1), ips[0])
	require.Equal(t, uint64(2), ips[1])
	require.Equal(t, uint64(3), ips[2])
	require.Equal(t, uint64(4), ips[3])
	require.Equal(t, uint64(5), ips[4])
	require.Equal(t, uint64(6), ips[5])

	v1 := vector.New(types.Type{Oid: types.T_int16})
	v1.Data = types.EncodeInt16Slice([]int16{7, 3, 8, 4, 6, 3})
	v1.Col = types.DecodeInt16Slice(v1.Data)

	pos, err := dict.FindBatch(v1)
	require.Nil(t, err)
	require.Equal(t, uint64(4), pos[0])
	require.Equal(t, uint64(3), pos[1])
	require.Equal(t, uint64(0), pos[2])
	require.Equal(t, uint64(6), pos[3])
	require.Equal(t, uint64(0), pos[4])
	require.Equal(t, uint64(3), pos[5])
}

func TestDict_FindDataFixedLen(t *testing.T) {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	m := mheap.New(gm)
	dict, err := New(types.Type{Oid: types.T_int32}, m)
	require.Nil(t, err)

	v0 := vector.New(types.Type{Oid: types.T_int32})
	v0.Data = types.EncodeInt32Slice([]int32{5, 3, 1, 7, 1, 3})
	v0.Col = types.DecodeInt32Slice(v0.Data)

	ips, err := dict.InsertBatch(v0)
	require.Nil(t, err)
	require.Equal(t, uint64(1), ips[0])
	require.Equal(t, uint64(2), ips[1])
	require.Equal(t, uint64(3), ips[2])
	require.Equal(t, uint64(4), ips[3])
	require.Equal(t, uint64(3), ips[4])
	require.Equal(t, uint64(2), ips[5])

	data := dict.FindData(1)
	require.Equal(t, int32(5), data.Col.([]int32)[0])

	data = dict.FindData(2)
	require.Equal(t, int32(3), data.Col.([]int32)[0])

	data = dict.FindData(3)
	require.Equal(t, int32(1), data.Col.([]int32)[0])

	data = dict.FindData(4)
	require.Equal(t, int32(7), data.Col.([]int32)[0])
}

func TestDict_InsertBatchVarLen(t *testing.T) {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	m := mheap.New(gm)
	dict, err := New(types.Type{Oid: types.T_varchar}, m)
	require.Nil(t, err)

	v0 := vector.New(types.Type{Oid: types.T_varchar})
	v0.Col = &types.Bytes{
		Data:    []byte("helloMynameisTom"),
		Offsets: []uint32{0, 5, 7, 11, 13},
		Lengths: []uint32{5, 2, 4, 2, 3},
	}
	v0.Data = v0.Col.(*types.Bytes).Data

	ips, err := dict.InsertBatch(v0)
	require.Nil(t, err)
	require.Equal(t, uint64(1), ips[0])
	require.Equal(t, uint64(2), ips[1])
	require.Equal(t, uint64(3), ips[2])
	require.Equal(t, uint64(4), ips[3])
	require.Equal(t, uint64(5), ips[4])

	v1 := vector.New(types.Type{Oid: types.T_varchar})
	v1.Col = &types.Bytes{
		Data:    []byte("TomisMyfriend"),
		Offsets: []uint32{0, 3, 5, 7},
		Lengths: []uint32{3, 2, 2, 6},
	}
	v1.Data = v1.Col.(*types.Bytes).Data

	ips, err = dict.InsertBatch(v1)
	require.Nil(t, err)
	require.Equal(t, uint64(5), ips[0])
	require.Equal(t, uint64(4), ips[1])
	require.Equal(t, uint64(2), ips[2])
	require.Equal(t, uint64(6), ips[3])

	unique := dict.GetUnique()
	require.Equal(t, []byte("helloMynameisTomfriend"), unique.Data)
}

func TestDict_FindBatchVarLen(t *testing.T) {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	m := mheap.New(gm)
	dict, err := New(types.Type{Oid: types.T_varchar}, m)
	require.Nil(t, err)

	v0 := vector.New(types.Type{Oid: types.T_varchar})
	v0.Col = &types.Bytes{
		Data:    []byte("helloMynameisTom"),
		Offsets: []uint32{0, 5, 7, 11, 13},
		Lengths: []uint32{5, 2, 4, 2, 3},
	}
	v0.Data = v0.Col.(*types.Bytes).Data

	ips, err := dict.InsertBatch(v0)
	require.Nil(t, err)
	require.Equal(t, uint64(1), ips[0])
	require.Equal(t, uint64(2), ips[1])
	require.Equal(t, uint64(3), ips[2])
	require.Equal(t, uint64(4), ips[3])
	require.Equal(t, uint64(5), ips[4])

	unique := dict.GetUnique()
	require.Equal(t, []byte("helloMynameisTom"), unique.Data)

	v1 := vector.New(types.Type{Oid: types.T_varchar})
	v1.Col = &types.Bytes{
		Data:    []byte("JackisMyfriendname"),
		Offsets: []uint32{0, 4, 6, 8, 14},
		Lengths: []uint32{4, 2, 2, 6, 4},
	}
	v1.Data = v1.Col.(*types.Bytes).Data

	pos, err := dict.FindBatch(v1)
	require.Nil(t, err)
	require.Equal(t, uint64(0), pos[0])
	require.Equal(t, uint64(4), pos[1])
	require.Equal(t, uint64(2), pos[2])
	require.Equal(t, uint64(0), pos[3])
	require.Equal(t, uint64(3), pos[4])
}

func TestDict_FindDataVarLen(t *testing.T) {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	m := mheap.New(gm)
	dict, err := New(types.Type{Oid: types.T_varchar}, m)
	require.Nil(t, err)

	v0 := vector.New(types.Type{Oid: types.T_varchar})
	v0.Col = &types.Bytes{
		Data:    []byte("helloMynameisTomisTom"),
		Offsets: []uint32{0, 5, 7, 11, 13, 16, 18},
		Lengths: []uint32{5, 2, 4, 2, 3, 2, 3},
	}
	v0.Data = v0.Col.(*types.Bytes).Data

	ips, err := dict.InsertBatch(v0)
	require.Nil(t, err)
	require.Equal(t, uint64(1), ips[0])
	require.Equal(t, uint64(2), ips[1])
	require.Equal(t, uint64(3), ips[2])
	require.Equal(t, uint64(4), ips[3])
	require.Equal(t, uint64(5), ips[4])
	require.Equal(t, uint64(4), ips[5])
	require.Equal(t, uint64(5), ips[6])

	data := dict.FindData(1)
	require.Equal(t, []byte("hello"), data.Data)

	data = dict.FindData(2)
	require.Equal(t, []byte("My"), data.Data)

	data = dict.FindData(3)
	require.Equal(t, []byte("name"), data.Data)

	data = dict.FindData(4)
	require.Equal(t, []byte("is"), data.Data)

	data = dict.FindData(5)
	require.Equal(t, []byte("Tom"), data.Data)
}
