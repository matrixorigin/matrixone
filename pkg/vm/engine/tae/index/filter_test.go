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

package index

import (
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/stretchr/testify/require"
)

func TestStaticFilterNumeric(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	typ := types.T_int32.ToType()
	data := containers.MockVector2(typ, 40000, 0)
	defer data.Close()
	sf, err := NewBloomFilter(data)
	require.NoError(t, err)
	var positive *nulls.Bitmap
	var res bool
	var exist bool

	res, err = sf.MayContainsKey(types.EncodeValue(int32(1209), typ.Oid))
	require.NoError(t, err)
	require.True(t, res)

	res, err = sf.MayContainsKey(types.EncodeValue(int32(5555), typ.Oid))
	require.NoError(t, err)
	require.True(t, res)

	res, err = sf.MayContainsKey(types.EncodeValue(int32(40000), typ.Oid))
	require.NoError(t, err)
	require.False(t, res)

	require.Panics(t, func() {
		res, err = sf.MayContainsKey(types.EncodeValue(int16(0), typ.Oid))
	})

	query := containers.MockVector2(typ, 2000, 1000)
	defer query.Close()
	exist, positive, err = sf.MayContainsAnyKeys(query)
	require.NoError(t, err)
	require.Equal(t, 2000, positive.GetCardinality())
	require.True(t, exist)

	query = containers.MockVector2(typ, 20000, 40000)
	defer query.Close()
	_, positive, err = sf.MayContainsAnyKeys(query)
	require.NoError(t, err)
	fpRate := float32(positive.GetCardinality()) / float32(20000)
	require.True(t, fpRate < float32(0.01))

	var buf []byte
	buf, err = sf.Marshal()
	require.NoError(t, err)

	vec := containers.MockVector2(typ, 0, 0)
	defer vec.Close()
	sf1, err := NewBloomFilter(vec)
	require.NoError(t, err)
	err = sf1.Unmarshal(buf)
	require.NoError(t, err)

	query = containers.MockVector2(typ, 40000, 0)
	defer query.Close()
	exist, positive, err = sf.MayContainsAnyKeys(query)
	require.NoError(t, err)
	require.Equal(t, 40000, positive.GetCardinality())
	require.True(t, exist)
}

func TestNewBinaryFuseFilter(t *testing.T) {
	testutils.EnsureNoLeak(t)
	typ := types.T_uint32.ToType()
	data := containers.MockVector2(typ, 2000, 0)
	defer data.Close()
	_, err := NewBloomFilter(data)
	require.NoError(t, err)
}

func BenchmarkCreateFilter(b *testing.B) {
	rows := 1000
	data := containers.MockVector2(types.T_int64.ToType(), rows, 0)
	defer data.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewBloomFilter(data)
	}
}

func BenchmarkHybridBloomFilter(b *testing.B) {
	rows := 8192
	data := containers.MockVector(types.T_Rowid.ToType(), rows, true, nil)
	defer data.Close()

	prefixFn := func(in []byte) []byte {
		return in
	}

	bf, err := NewBloomFilter(data)
	require.NoError(b, err)
	buf, err := bf.Marshal()
	require.NoError(b, err)
	b.Logf("buf size: %d", len(buf))

	hbf, err := NewHybridBloomFilter(data, 1, prefixFn, 1, prefixFn)
	require.NoError(b, err)
	hbf_buf, err := hbf.Marshal()
	require.NoError(b, err)
	b.Logf("hbf_buf size: %d", len(hbf_buf))

	b.Run("maral-bf", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var bf bloomFilter
			bf.Unmarshal(buf)
		}
	})

	b.Run("unmaral-hbf", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var hbf hybridFilter
			hbf.Unmarshal(hbf_buf)
		}
	})

	b.Run("may-contains-hbf", func(b *testing.B) {
		var rowid types.Rowid
		bs := rowid[:]
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			hbf.MayContainsKey(bs)
		}
	})
	b.Run("prefix-may-contains-hbf", func(b *testing.B) {
		var rowid types.Rowid
		bs := rowid[:]
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			hbf.PrefixMayContainsKey(bs, 1, 1)
		}
	})
}

func TestHybridBloomFilter(t *testing.T) {
	obj1 := types.NewObjectid()
	obj2 := types.NewObjectid()
	obj3 := types.NewObjectid()
	obj4 := types.NewObjectid()
	blk1_0 := types.NewBlockidWithObjectID(obj1, 0)
	blk1_1 := types.NewBlockidWithObjectID(obj1, 1)
	blk2_0 := types.NewBlockidWithObjectID(obj2, 0)
	blk3_0 := types.NewBlockidWithObjectID(obj3, 0)
	blk3_1 := types.NewBlockidWithObjectID(obj3, 1)
	blk3_2 := types.NewBlockidWithObjectID(obj3, 2)
	blk3_3 := types.NewBlockidWithObjectID(obj3, 3)
	rowids := containers.MakeVector(types.T_Rowid.ToType(), common.DefaultAllocator)
	defer rowids.Close()
	for i := 0; i < 10; i++ {
		rowids.Append(*types.NewRowid(blk1_0, uint32(i)), false)
		rowids.Append(*types.NewRowid(blk1_1, uint32(i)), false)
		rowids.Append(*types.NewRowid(blk2_0, uint32(i)), false)
		rowids.Append(*types.NewRowid(blk3_0, uint32(i)), false)
		rowids.Append(*types.NewRowid(blk3_1, uint32(i)), false)
		rowids.Append(*types.NewRowid(blk3_2, uint32(i)), false)
	}
	objectFn := func(in []byte) []byte {
		return in[:types.ObjectBytesSize]
	}
	objectFnId := uint8(1)
	blockFn := func(in []byte) []byte {
		return in[:types.BlockidSize]
	}
	blockFnId := uint8(2)
	hbf, err := NewHybridBloomFilter(
		rowids,
		objectFnId, objectFn,
		blockFnId, blockFn,
	)
	require.NoError(t, err)
	hbf_buf, err := hbf.Marshal()
	require.NoError(t, err)
	var hbf2 hybridFilter
	err = hbf2.Unmarshal(hbf_buf)
	require.NoError(t, err)

	_, err = hbf2.PrefixMayContainsKey(types.NewRowid(blk1_0, 0)[:], 2, 1)
	require.NotNil(t, err)

	ok, err := hbf2.PrefixMayContainsKey(obj1[:], objectFnId, 1)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = hbf2.PrefixMayContainsKey(obj2[:], objectFnId, 1)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = hbf2.PrefixMayContainsKey(obj3[:], objectFnId, 1)
	require.NoError(t, err)
	require.True(t, ok)
	_, err = hbf2.PrefixMayContainsKey(obj4[:], objectFnId, 1)
	require.NoError(t, err)
	// false positive
	// require.False(t, ok)

	ok, err = hbf2.PrefixMayContainsKey(blk1_0[:], blockFnId, 2)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = hbf2.PrefixMayContainsKey(blk1_1[:], blockFnId, 2)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = hbf2.PrefixMayContainsKey(blk2_0[:], blockFnId, 2)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = hbf2.PrefixMayContainsKey(blk3_0[:], blockFnId, 2)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = hbf2.PrefixMayContainsKey(blk3_1[:], blockFnId, 2)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = hbf2.PrefixMayContainsKey(blk3_2[:], blockFnId, 2)
	require.NoError(t, err)
	require.True(t, ok)
	_, err = hbf2.PrefixMayContainsKey(blk3_3[:], blockFnId, 2)
	require.NoError(t, err)
	// false postive
	// require.False(t, ok)

	idVec := rowids.GetDownstreamVector()
	ids := vector.MustFixedCol[types.Rowid](idVec)
	for i := 0; i < len(ids); i++ {
		id := ids[i]
		ok, err = hbf2.MayContainsKey(id[:])
		require.NoError(t, err)
		require.True(t, ok)
	}

	rowid := types.NewRowid(blk3_2, 100)
	_, err = hbf2.MayContainsKey(rowid[:])
	require.NoError(t, err)
	// false postive
	// require.False(t, ok)
}

func TestStaticFilterString(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	typ := types.T_varchar.ToType()
	data := containers.MockVector2(typ, 40000, 0)
	defer data.Close()
	sf, err := NewBloomFilter(data)
	require.NoError(t, err)
	var positive *nulls.Bitmap
	var res bool
	var exist bool

	res, err = sf.MayContainsKey([]byte(strconv.Itoa(1209)))
	require.NoError(t, err)
	require.True(t, res)

	res, err = sf.MayContainsKey([]byte(strconv.Itoa(40000)))
	require.NoError(t, err)
	require.False(t, res)

	query := containers.MockVector2(typ, 2000, 1000)
	defer query.Close()
	exist, positive, err = sf.MayContainsAnyKeys(query)
	require.NoError(t, err)
	require.Equal(t, 2000, positive.GetCardinality())
	require.True(t, exist)

	query = containers.MockVector2(typ, 20000, 40000)
	defer query.Close()
	_, positive, err = sf.MayContainsAnyKeys(query)
	require.NoError(t, err)
	fpRate := float32(positive.GetCardinality()) / float32(20000)
	require.True(t, fpRate < float32(0.01))

	var buf []byte
	buf, err = sf.Marshal()
	require.NoError(t, err)

	query = containers.MockVector2(typ, 0, 0)
	defer query.Close()
	sf1, err := NewBloomFilter(query)
	require.NoError(t, err)
	err = sf1.Unmarshal(buf)
	require.NoError(t, err)

	query = containers.MockVector2(typ, 40000, 0)
	defer query.Close()
	exist, positive, err = sf.MayContainsAnyKeys(query)
	require.NoError(t, err)
	require.Equal(t, 40000, positive.GetCardinality())
	require.True(t, exist)
}
