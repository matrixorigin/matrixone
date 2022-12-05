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
	"math/rand"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 10
)

func TestInsert(t *testing.T) {
	m := mpool.MustNewZero()
	mp, err := NewStrMap(false, 0, 0, m)
	require.NoError(t, err)
	ts := []types.Type{
		types.New(types.T_int8, 0, 0, 0),
		types.New(types.T_int16, 0, 0, 0),
		types.New(types.T_int32, 0, 0, 0),
		types.New(types.T_int64, 0, 0, 0),
		types.New(types.T_decimal64, 0, 0, 0),
		types.New(types.T_char, 0, 0, 0),
	}
	vecs := newVectors(ts, false, Rows, m)
	for i := 0; i < Rows; i++ {
		ok, err := mp.Insert(vecs, i)
		require.NoError(t, err)
		require.Equal(t, true, ok)
	}
	for _, vec := range vecs {
		vec.Free(m)
	}
	mp.Free()
	require.Equal(t, int64(0), m.Stats().NumCurrBytes.Load())
}

func TestInsertValue(t *testing.T) {
	m := mpool.MustNewZero()
	mp, err := NewStrMap(false, 0, 0, m)
	require.NoError(t, err)
	ok, err := mp.InsertValue(int8(0))
	require.NoError(t, err)
	require.Equal(t, true, ok)
	ok, err = mp.InsertValue(int16(0))
	require.NoError(t, err)
	require.Equal(t, false, ok)
	ok, err = mp.InsertValue(int32(0))
	require.NoError(t, err)
	require.Equal(t, false, ok)
	ok, err = mp.InsertValue(int64(0))
	require.NoError(t, err)
	require.Equal(t, false, ok)
	ok, err = mp.InsertValue(uint8(0))
	require.NoError(t, err)
	require.Equal(t, false, ok)
	ok, err = mp.InsertValue(uint16(0))
	require.NoError(t, err)
	require.Equal(t, false, ok)
	ok, err = mp.InsertValue(uint32(0))
	require.NoError(t, err)
	require.Equal(t, false, ok)
	ok, err = mp.InsertValue(uint64(0))
	require.NoError(t, err)
	require.Equal(t, false, ok)
	ok, err = mp.InsertValue([]byte{})
	require.NoError(t, err)
	require.Equal(t, false, ok)
	ok, err = mp.InsertValue(types.Date(0))
	require.NoError(t, err)
	require.Equal(t, false, ok)
	ok, err = mp.InsertValue(types.Datetime(0))
	require.NoError(t, err)
	require.Equal(t, false, ok)
	ok, err = mp.InsertValue(types.Timestamp(0))
	require.NoError(t, err)
	require.Equal(t, false, ok)
	ok, err = mp.InsertValue(types.Decimal64{})
	require.NoError(t, err)
	require.Equal(t, false, ok)
	ok, err = mp.InsertValue(types.Decimal128{})
	require.NoError(t, err)
	require.Equal(t, false, ok)
	mp.Free()
	require.Equal(t, int64(0), m.Stats().NumCurrBytes.Load())
}

func TestIterator(t *testing.T) {
	{
		m := mpool.MustNewZero()
		mp, err := NewStrMap(false, 0, 0, m)
		require.NoError(t, err)
		ts := []types.Type{
			types.New(types.T_int8, 0, 0, 0),
			types.New(types.T_int16, 0, 0, 0),
			types.New(types.T_int32, 0, 0, 0),
			types.New(types.T_int64, 0, 0, 0),
			types.New(types.T_decimal64, 0, 0, 0),
			types.New(types.T_char, 0, 0, 0),
		}
		vecs := newVectors(ts, false, Rows, m)
		itr := mp.NewIterator()
		vs, _, err := itr.Insert(0, Rows, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, vs[:Rows])
		vs, _ = itr.Find(0, Rows, vecs, nil)
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, vs[:Rows])
		for _, vec := range vecs {
			vec.Free(m)
		}
		mp.Free()
		require.Equal(t, int64(0), m.Stats().NumCurrBytes.Load())
	}
	{
		m := mpool.MustNewZero()
		mp, err := NewStrMap(true, 0, 0, m)
		require.NoError(t, err)
		ts := []types.Type{
			types.New(types.T_int8, 0, 0, 0),
			types.New(types.T_int16, 0, 0, 0),
			types.New(types.T_int32, 0, 0, 0),
			types.New(types.T_int64, 0, 0, 0),
			types.New(types.T_decimal64, 0, 0, 0),
			types.New(types.T_char, 0, 0, 0),
		}
		vecs := newVectors(ts, false, Rows, m)
		itr := mp.NewIterator()
		vs, _, err := itr.Insert(0, Rows, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, vs[:Rows])
		vs, _ = itr.Find(0, Rows, vecs, nil)
		require.Equal(t, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, vs[:Rows])
		for _, vec := range vecs {
			vec.Free(m)
		}
		mp.Free()
		require.Equal(t, int64(0), m.Stats().NumCurrBytes.Load())
	}
	{
		m := mpool.MustNewZero()
		mp, err := NewStrMap(true, 0, 0, m)
		require.NoError(t, err)
		ts := []types.Type{
			types.New(types.T_int8, 0, 0, 0),
			types.New(types.T_int16, 0, 0, 0),
			types.New(types.T_int32, 0, 0, 0),
			types.New(types.T_int64, 0, 0, 0),
			types.New(types.T_decimal64, 0, 0, 0),
			types.New(types.T_char, 0, 0, 0),
		}
		vecs := newVectorsWithNull(ts, false, Rows, m)
		itr := mp.NewIterator()
		vs, _, err := itr.Insert(0, Rows, vecs)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2, 1, 3, 1, 4, 1, 5, 1, 6}, vs[:Rows])
		vs, _ = itr.Find(0, Rows, vecs, nil)
		require.Equal(t, []uint64{1, 2, 1, 3, 1, 4, 1, 5, 1, 6}, vs[:Rows])
		for _, vec := range vecs {
			vec.Free(m)
		}
		mp.Free()
		require.Equal(t, int64(0), m.Stats().NumCurrBytes.Load())
	}
}

func newVectors(ts []types.Type, random bool, n int, m *mpool.MPool) []*vector.Vector {
	vecs := make([]*vector.Vector, len(ts))
	for i := range vecs {
		vecs[i] = newVector(n, ts[i], m, random, nil)
		nulls.New(vecs[i].Nsp, n)
	}
	return vecs
}

func newVectorsWithNull(ts []types.Type, random bool, n int, m *mpool.MPool) []*vector.Vector {
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

func newVector(n int, typ types.Type, m *mpool.MPool, random bool, Values interface{}) *vector.Vector {
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
		panic(moerr.NewInternalErrorNoCtx("unsupport vector's type '%v", typ))
	}
}

func newInt8Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []int8) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], false, m); err != nil {
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
		if err := vec.Append(int8(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func newInt16Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []int16) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], false, m); err != nil {
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
		if err := vec.Append(int16(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func newInt32Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []int32) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], false, m); err != nil {
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
		if err := vec.Append(int32(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func newInt64Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []int64) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], false, m); err != nil {
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
		if err := vec.Append(int64(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func newUInt32Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []uint32) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], false, m); err != nil {
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
		if err := vec.Append(uint32(v), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func newDecimal64Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []types.Decimal64) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], false, m); err != nil {
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
		d, _ := types.InitDecimal64(int64(v), 64, 0)
		if err := vec.Append(d, false, m); err != nil {

			vec.Free(m)
			return nil
		}
	}
	return vec
}

func newDecimal128Vector(n int, typ types.Type, m *mpool.MPool, random bool, vs []types.Decimal128) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append(vs[i], false, m); err != nil {
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
		d, _ := types.InitDecimal128(int64(v), 64, 0)
		if err := vec.Append(d, false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}

func newStringVector(n int, typ types.Type, m *mpool.MPool, random bool, vs []string) *vector.Vector {
	vec := vector.New(typ)
	if vs != nil {
		for i := range vs {
			if err := vec.Append([]byte(vs[i]), false, m); err != nil {
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
		if err := vec.Append([]byte(strconv.Itoa(v)), false, m); err != nil {
			vec.Free(m)
			return nil
		}
	}
	return vec
}
