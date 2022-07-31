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

package hashmap

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 10
)

func TestInsert(t *testing.T) {
	mp := NewStrMap(false)
	ts := []types.Type{
		types.New(types.T_int8, 0, 0, 0),
		types.New(types.T_int16, 0, 0, 0),
		types.New(types.T_int32, 0, 0, 0),
		types.New(types.T_int64, 0, 0, 0),
		types.New(types.T_decimal64, 0, 0, 0),
		types.New(types.T_char, 0, 0, 0),
	}
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	m := mheap.New(gm)
	vecs := newVectors(ts, false, Rows, m)
	for i := 0; i < Rows; i++ {
		ok := mp.Insert(vecs, i)
		require.Equal(t, true, ok)
	}
	for _, vec := range vecs {
		vec.Free(m)
	}
	require.Equal(t, int64(0), m.Size())
}

func TestInertValue(t *testing.T) {
	mp := NewStrMap(false)
	ok := mp.InsertValue(int8(0))
	require.Equal(t, true, ok)
	ok = mp.InsertValue(int16(0))
	require.Equal(t, false, ok)
	ok = mp.InsertValue(int32(0))
	require.Equal(t, false, ok)
	ok = mp.InsertValue(int64(0))
	require.Equal(t, false, ok)
	ok = mp.InsertValue(uint8(0))
	require.Equal(t, false, ok)
	ok = mp.InsertValue(uint16(0))
	require.Equal(t, false, ok)
	ok = mp.InsertValue(uint32(0))
	require.Equal(t, false, ok)
	ok = mp.InsertValue(uint64(0))
	require.Equal(t, false, ok)
	ok = mp.InsertValue([]byte{})
	require.Equal(t, false, ok)
	ok = mp.InsertValue(types.Date(0))
	require.Equal(t, false, ok)
	ok = mp.InsertValue(types.Datetime(0))
	require.Equal(t, false, ok)
	ok = mp.InsertValue(types.Timestamp(0))
	require.Equal(t, false, ok)
	ok = mp.InsertValue(types.Decimal64{})
	require.Equal(t, false, ok)
	ok = mp.InsertValue(types.Decimal128{})
	require.Equal(t, false, ok)
}

func TestIterator(t *testing.T) {
	{
		mp := NewStrMap(false)
		ts := []types.Type{
			types.New(types.T_int8, 0, 0, 0),
			types.New(types.T_int16, 0, 0, 0),
			types.New(types.T_int32, 0, 0, 0),
			types.New(types.T_int64, 0, 0, 0),
			types.New(types.T_decimal64, 0, 0, 0),
			types.New(types.T_char, 0, 0, 0),
		}
		hm := host.New(1 << 30)
		gm := guest.New(1<<30, hm)
		m := mheap.New(gm)
		vecs := newVectors(ts, false, Rows, m)
		itr := mp.NewIterator(0, 0)
		vs, _ := itr.Insert(0, Rows, vecs)
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, vs[:Rows])
		vs, _ = itr.Find(0, Rows, vecs, nil)
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, vs[:Rows])
		for _, vec := range vecs {
			vec.Free(m)
		}
		require.Equal(t, int64(0), m.Size())
	}
	{
		mp := NewStrMap(true)
		ts := []types.Type{
			types.New(types.T_int8, 0, 0, 0),
			types.New(types.T_int16, 0, 0, 0),
			types.New(types.T_int32, 0, 0, 0),
			types.New(types.T_int64, 0, 0, 0),
			types.New(types.T_decimal64, 0, 0, 0),
			types.New(types.T_char, 0, 0, 0),
		}
		hm := host.New(1 << 30)
		gm := guest.New(1<<30, hm)
		m := mheap.New(gm)
		vecs := newVectors(ts, false, Rows, m)
		itr := mp.NewIterator(0, 0)
		vs, _ := itr.Insert(0, Rows, vecs)
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, vs[:Rows])
		vs, _ = itr.Find(0, Rows, vecs, nil)
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, vs[:Rows])
		for _, vec := range vecs {
			vec.Free(m)
		}
		require.Equal(t, int64(0), m.Size())
	}
	{
		mp := NewStrMap(true)
		ts := []types.Type{
			types.New(types.T_int8, 0, 0, 0),
			types.New(types.T_int16, 0, 0, 0),
			types.New(types.T_int32, 0, 0, 0),
			types.New(types.T_int64, 0, 0, 0),
			types.New(types.T_decimal64, 0, 0, 0),
			types.New(types.T_char, 0, 0, 0),
		}
		hm := host.New(1 << 30)
		gm := guest.New(1<<30, hm)
		m := mheap.New(gm)
		vecs := newVectorsWithNull(ts, false, Rows, m)
		itr := mp.NewIterator(0, 0)
		vs, _ := itr.Insert(0, Rows, vecs)
		require.Equal(t, []uint64{1, 2, 1, 3, 1, 4, 1, 5, 1, 6}, vs[:Rows])
		vs, _ = itr.Find(0, Rows, vecs, nil)
		require.Equal(t, []uint64{1, 2, 1, 3, 1, 4, 1, 5, 1, 6}, vs[:Rows])
		for _, vec := range vecs {
			vec.Free(m)
		}
		require.Equal(t, int64(0), m.Size())
	}
}

func newVectors(ts []types.Type, random bool, n int, m *mheap.Mheap) []*vector.Vector {
	vecs := make([]*vector.Vector, len(ts))
	for i := range vecs {
		vecs[i] = newVector(n, ts[i], m, random, nil)
		nulls.New(vecs[i].Nsp, n)
	}
	return vecs
}

func newVectorsWithNull(ts []types.Type, random bool, n int, m *mheap.Mheap) []*vector.Vector {
	vecs := make([]*vector.Vector, len(ts))
	for i := range vecs {
		vecs[i] = newVector(n, ts[i], m, random, nil)
		nulls.New(vecs[i].Nsp, n)
		nsp := vecs[i].GetNulls()
		for j := 0; j < n; j++ {
			if j%2 == 0 {
				nsp.Set(uint64(j))
			}
		}
	}
	return vecs
}

func newVector(n int, typ types.Type, m *mheap.Mheap, random bool, Values interface{}) *vector.Vector {
	switch typ.Oid {
	case types.T_int8:
		if vs, ok := Values.([]int8); ok {
			return newInt8Vector(n, typ, m, random, vs)
		}
		return newInt8Vector(n, typ, m, random, nil)
	case types.T_int16:
		if vs, ok := Values.([]int16); ok {
			return newInt16Vector(n, typ, m, random, vs)
		}
		return newInt16Vector(n, typ, m, random, nil)
	case types.T_int32:
		if vs, ok := Values.([]int32); ok {
			return newInt32Vector(n, typ, m, random, vs)
		}
		return newInt32Vector(n, typ, m, random, nil)
	case types.T_int64:
		if vs, ok := Values.([]int64); ok {
			return newInt64Vector(n, typ, m, random, vs)
		}
		return newInt64Vector(n, typ, m, random, nil)
	case types.T_uint32:
		if vs, ok := Values.([]uint32); ok {
			return newUInt32Vector(n, typ, m, random, vs)
		}
		return newUInt32Vector(n, typ, m, random, nil)
	case types.T_decimal64:
		if vs, ok := Values.([]types.Decimal64); ok {
			return newDecimal64Vector(n, typ, m, random, vs)
		}
		return newDecimal64Vector(n, typ, m, random, nil)
	case types.T_decimal128:
		if vs, ok := Values.([]types.Decimal128); ok {
			return newDecimal128Vector(n, typ, m, random, vs)
		}
		return newDecimal128Vector(n, typ, m, random, nil)
	case types.T_char, types.T_varchar:
		if vs, ok := Values.([]string); ok {
			return newStringVector(n, typ, m, random, vs)
		}
		return newStringVector(n, typ, m, random, nil)
	default:
		panic(fmt.Errorf("unsupport vector's type '%v", typ))
	}
}

func newInt8Vector(n int, typ types.Type, m *mheap.Mheap, random bool, vs []int8) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(int8(v), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func newInt16Vector(n int, typ types.Type, m *mheap.Mheap, random bool, vs []int16) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(int16(v), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func newInt32Vector(n int, typ types.Type, m *mheap.Mheap, random bool, vs []int32) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(int32(v), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func newInt64Vector(n int, typ types.Type, m *mheap.Mheap, random bool, vs []int64) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(int64(v), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func newUInt32Vector(n int, typ types.Type, m *mheap.Mheap, random bool, vs []uint32) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(uint32(v), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func newDecimal64Vector(n int, typ types.Type, m *mheap.Mheap, random bool, vs []types.Decimal64) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(types.InitDecimal64(int64(v)), m); err != nil {

			vec.Free(m)
			return nil
		}
	}
	return vec
}

func newDecimal128Vector(n int, typ types.Type, m *mheap.Mheap, random bool, vs []types.Decimal128) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append(types.InitDecimal128(int64(v)), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func newStringVector(n int, typ types.Type, m *mheap.Mheap, random bool, vs []string) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append([]byte(vs[i]), m); err != nil {
				vec.Free(m)
				return nil
			}
		}
		return vec
	}
	for i := 0; i < n; i++ {
		v := i
		if random {
			v = rand.Int()
		}
		if err := vec.Append([]byte(strconv.Itoa(v)), m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}
