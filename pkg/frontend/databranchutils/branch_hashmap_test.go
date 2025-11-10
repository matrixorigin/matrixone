// Copyright 2025 Matrix Origin
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

package databranchutils

import (
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestBranchHashmapBasic(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	keyVec := buildInt64Vector(t, mp, []int64{1, 2, 3})
	valVec := buildStringVector(t, mp, []string{"one", "two", "three"})
	defer keyVec.Free(mp)
	defer valVec.Free(mp)

	bh, err := NewBranchHashmap()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, bh.Close())
	}()

	require.NoError(t, bh.PutByVectors([]*vector.Vector{keyVec, valVec}, []int{0}))
	impl := bh.(*branchHashmap)
	totalEntries := 0
	for _, bucket := range impl.inMemory {
		totalEntries += len(bucket.entries)
	}
	require.Equal(t, 3, totalEntries)

	probe := buildInt64Vector(t, mp, []int64{2, 4})
	defer probe.Free(mp)

	results, err := bh.GetByVectors([]*vector.Vector{probe})
	require.NoError(t, err)
	require.Len(t, results, 2)

	require.True(t, results[0].Exists)
	require.Len(t, results[0].Rows, 1)

	tuple, _, err := bh.DecodeRow(results[0].Rows[0])
	require.NoError(t, err)
	require.Len(t, tuple, 2)
	require.Equal(t, int64(2), tuple[0])
	require.Equal(t, []byte("two"), tuple[1])

	require.False(t, results[1].Exists)
	require.Empty(t, results[1].Rows)
}

func TestBranchHashmapPopByVectors(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	keyVec := buildInt64Vector(t, mp, []int64{1, 2, 3})
	valVec := buildStringVector(t, mp, []string{"one", "two", "three"})
	defer keyVec.Free(mp)
	defer valVec.Free(mp)

	bh, err := NewBranchHashmap()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, bh.Close())
	}()

	require.NoError(t, bh.PutByVectors([]*vector.Vector{keyVec, valVec}, []int{0}))

	probe := buildInt64Vector(t, mp, []int64{2, 4})
	defer probe.Free(mp)

	results, err := bh.PopByVectors([]*vector.Vector{probe}, true)
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.True(t, results[0].Exists)
	require.Len(t, results[0].Rows, 1)
	require.False(t, results[1].Exists)

	// Ensure the key has been removed.
	after, err := bh.GetByVectors([]*vector.Vector{probe})
	require.NoError(t, err)
	require.False(t, after[0].Exists)
	require.Empty(t, after[0].Rows)
}

func TestBranchHashmapPopByVectorsPartial(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	keyVec := buildInt64Vector(t, mp, []int64{1, 1})
	valVec := buildStringVector(t, mp, []string{"one", "uno"})
	defer keyVec.Free(mp)
	defer valVec.Free(mp)

	bh, err := NewBranchHashmap()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, bh.Close())
	}()

	require.NoError(t, bh.PutByVectors([]*vector.Vector{keyVec, valVec}, []int{0}))

	probe := buildInt64Vector(t, mp, []int64{1})
	defer probe.Free(mp)

	results, err := bh.PopByVectors([]*vector.Vector{probe}, false)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.True(t, results[0].Exists)
	require.Len(t, results[0].Rows, 1)

	removedRow, _, err := bh.DecodeRow(results[0].Rows[0])
	require.NoError(t, err)
	require.Len(t, removedRow, 2)
	require.Equal(t, int64(1), removedRow[0])
	removedValueBytes, ok := removedRow[1].([]byte)
	require.True(t, ok)
	removedValue := string(removedValueBytes)
	require.True(t, removedValue == "one" || removedValue == "uno")

	after, err := bh.GetByVectors([]*vector.Vector{probe})
	require.NoError(t, err)
	require.Len(t, after, 1)
	require.True(t, after[0].Exists)
	require.Len(t, after[0].Rows, 1)
	remainingRow, _, err := bh.DecodeRow(after[0].Rows[0])
	require.NoError(t, err)
	require.Len(t, remainingRow, 2)
	require.Equal(t, int64(1), remainingRow[0])
	remainingValueBytes, ok := remainingRow[1].([]byte)
	require.True(t, ok)
	require.NotEqual(t, removedValue, string(remainingValueBytes))

	second, err := bh.PopByVectors([]*vector.Vector{probe}, false)
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.True(t, second[0].Exists)
	require.Len(t, second[0].Rows, 1)
	secondRow, _, err := bh.DecodeRow(second[0].Rows[0])
	require.NoError(t, err)
	require.Len(t, secondRow, 2)
	require.Equal(t, int64(1), secondRow[0])
	secondValueBytes, ok := secondRow[1].([]byte)
	require.True(t, ok)
	require.NotEqual(t, removedValue, string(secondValueBytes))

	final, err := bh.GetByVectors([]*vector.Vector{probe})
	require.NoError(t, err)
	require.Len(t, final, 1)
	require.False(t, final[0].Exists)
	require.Empty(t, final[0].Rows)
}

func TestBranchHashmapCompositeKey(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	key1 := buildInt64Vector(t, mp, []int64{1, 1, 2})
	key2 := buildStringVector(t, mp, []string{"a", "a", "b"})
	payload := buildInt64Vector(t, mp, []int64{10, 20, 30})
	defer key1.Free(mp)
	defer key2.Free(mp)
	defer payload.Free(mp)

	bh, err := NewBranchHashmap()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, bh.Close())
	}()

	require.NoError(t, bh.PutByVectors([]*vector.Vector{key1, key2, payload}, []int{0, 1}))

	probeKey1 := buildInt64Vector(t, mp, []int64{1, 2})
	probeKey2 := buildStringVector(t, mp, []string{"a", "b"})
	defer probeKey1.Free(mp)
	defer probeKey2.Free(mp)

	results, err := bh.GetByVectors([]*vector.Vector{probeKey1, probeKey2})
	require.NoError(t, err)
	require.Len(t, results, 2)

	require.True(t, results[0].Exists)
	require.Len(t, results[0].Rows, 2)

	firstRow, _, err := bh.DecodeRow(results[0].Rows[0])
	require.NoError(t, err)
	secondRow, _, err := bh.DecodeRow(results[0].Rows[1])
	require.NoError(t, err)

	values := [][]any{
		{firstRow[0], firstRow[1], firstRow[2]},
		{secondRow[0], secondRow[1], secondRow[2]},
	}

	require.ElementsMatch(t, [][]any{
		{int64(1), []byte("a"), int64(10)},
		{int64(1), []byte("a"), int64(20)},
	}, values)

	require.True(t, results[1].Exists)
	require.Len(t, results[1].Rows, 1)
	row, _, err := bh.DecodeRow(results[1].Rows[0])
	require.NoError(t, err)
	require.Equal(t, int64(2), row[0])
	require.Equal(t, []byte("b"), row[1])
	require.Equal(t, int64(30), row[2])
}

func TestBranchHashmapSpillAndRetrieve(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	allocator := newLimitedAllocator(80)
	bhIface, err := NewBranchHashmap(
		WithBranchHashmapAllocator(allocator),
		WithBranchHashmapSpillRoot(t.TempDir()),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, bhIface.Close())
	}()

	key := buildInt64Vector(t, mp, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	value := buildStringVector(t, mp, []string{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"})
	defer key.Free(mp)
	defer value.Free(mp)

	require.NoError(t, bhIface.PutByVectors([]*vector.Vector{key, value}, []int{0}))

	impl := bhIface.(*branchHashmap)
	require.NotEmpty(t, impl.spills, "expected data to spill on limited allocator")

	probe := buildInt64Vector(t, mp, []int64{9, 10})
	defer probe.Free(mp)

	results, err := bhIface.GetByVectors([]*vector.Vector{probe})
	require.NoError(t, err)
	require.Len(t, results, 2)

	for i, expected := range []struct {
		key   int64
		value string
	}{
		{9, "nine"},
		{10, "ten"},
	} {
		require.Truef(t, results[i].Exists, "expected key %d to exist", expected.key)
		require.Len(t, results[i].Rows, 1)
		row, _, err := bhIface.DecodeRow(results[i].Rows[0])
		require.NoError(t, err)
		require.Equal(t, expected.key, row[0])
		require.Equal(t, []byte(expected.value), row[1])
	}
}

func TestBranchHashmapPopByVectorsSpilled(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	allocator := newLimitedAllocator(80)
	bhIface, err := NewBranchHashmap(
		WithBranchHashmapAllocator(allocator),
		WithBranchHashmapSpillRoot(t.TempDir()),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, bhIface.Close())
	}()

	key := buildInt64Vector(t, mp, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	value := buildStringVector(t, mp, []string{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"})
	defer key.Free(mp)
	defer value.Free(mp)

	require.NoError(t, bhIface.PutByVectors([]*vector.Vector{key, value}, []int{0}))

	probe := buildInt64Vector(t, mp, []int64{9, 10})
	defer probe.Free(mp)

	results, err := bhIface.PopByVectors([]*vector.Vector{probe}, true)
	require.NoError(t, err)
	require.Len(t, results, 2)
	for i, expected := range []struct {
		key   int64
		value string
	}{
		{9, "nine"},
		{10, "ten"},
	} {
		require.Truef(t, results[i].Exists, "expected key %d to exist", expected.key)
		require.Len(t, results[i].Rows, 1)
		row, _, err := bhIface.DecodeRow(results[i].Rows[0])
		require.NoError(t, err)
		require.Equal(t, expected.key, row[0])
		require.Equal(t, []byte(expected.value), row[1])
	}

	after, err := bhIface.GetByVectors([]*vector.Vector{probe})
	require.NoError(t, err)
	require.False(t, after[0].Exists)
	require.False(t, after[1].Exists)
}

func TestBranchHashmapPopByVectorsPartialSpilled(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	allocator := newLimitedAllocator(80)
	bhIface, err := NewBranchHashmap(
		WithBranchHashmapAllocator(allocator),
		WithBranchHashmapSpillRoot(t.TempDir()),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, bhIface.Close())
	}()

	keys := make([]int64, 10)
	for i := range keys {
		keys[i] = 1
	}
	values := []string{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"}

	keyVec := buildInt64Vector(t, mp, keys)
	valVec := buildStringVector(t, mp, values)
	defer keyVec.Free(mp)
	defer valVec.Free(mp)

	require.NoError(t, bhIface.PutByVectors([]*vector.Vector{keyVec, valVec}, []int{0}))

	impl := bhIface.(*branchHashmap)
	require.NotEmpty(t, impl.spills, "expected data to spill on limited allocator")

	probe := buildInt64Vector(t, mp, []int64{1})
	defer probe.Free(mp)

	results, err := bhIface.PopByVectors([]*vector.Vector{probe}, false)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.True(t, results[0].Exists)
	require.Len(t, results[0].Rows, 1)

	after, err := bhIface.GetByVectors([]*vector.Vector{probe})
	require.NoError(t, err)
	require.Len(t, after, 1)
	require.True(t, after[0].Exists)
	require.Len(t, after[0].Rows, 9)

	_, err = bhIface.PopByVectors([]*vector.Vector{probe}, true)
	require.NoError(t, err)

	final, err := bhIface.GetByVectors([]*vector.Vector{probe})
	require.NoError(t, err)
	require.Len(t, final, 1)
	require.False(t, final[0].Exists)
	require.Empty(t, final[0].Rows)
}

func TestBranchHashmapForEach(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	keys := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	values := []string{"one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"}

	keyVec := buildInt64Vector(t, mp, keys)
	valVec := buildStringVector(t, mp, values)
	defer keyVec.Free(mp)
	defer valVec.Free(mp)

	allocator := newLimitedAllocator(80)
	bhIface, err := NewBranchHashmap(
		WithBranchHashmapAllocator(allocator),
		WithBranchHashmapSpillRoot(t.TempDir()),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, bhIface.Close())
	}()

	require.NoError(t, bhIface.PutByVectors([]*vector.Vector{keyVec, valVec}, []int{0}))

	collected := make(map[int64][]string)
	err = bhIface.ForEach(func(key []byte, rows [][]byte) error {
		tuple, _, err := bhIface.DecodeRow(key)
		require.NoError(t, err)
		require.Len(t, tuple, 1)
		keyVal, ok := tuple[0].(int64)
		require.True(t, ok)

		for _, rowBytes := range rows {
			rowTuple, _, err := bhIface.DecodeRow(rowBytes)
			require.NoError(t, err)
			require.Len(t, rowTuple, 2)
			valueBytes, ok := rowTuple[1].([]byte)
			require.True(t, ok)
			collected[keyVal] = append(collected[keyVal], string(valueBytes))
		}
		return nil
	})
	require.NoError(t, err)

	require.Len(t, collected, len(keys))
	for idx, key := range keys {
		require.Equal(t, []string{values[idx]}, collected[key])
	}
}

func TestBranchHashmapPopByEncodedKeyInMemory(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	keyVec := buildInt64Vector(t, mp, []int64{1, 1, 2})
	valVec := buildStringVector(t, mp, []string{"one", "uno", "two"})
	defer keyVec.Free(mp)
	defer valVec.Free(mp)

	bh, err := NewBranchHashmap()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, bh.Close())
	}()

	require.NoError(t, bh.PutByVectors([]*vector.Vector{keyVec, valVec}, []int{0}))

	encodedKeys := collectInt64EncodedKeys(t, bh)
	encodedKey, ok := encodedKeys[1]
	require.True(t, ok, "expected encoded key for value 1 from ForEach")

	popOnce, err := bh.PopByEncodedKey(encodedKey, false)
	require.NoError(t, err)
	require.Len(t, popOnce, 1)
	require.True(t, popOnce.Exists)
	require.Len(t, popOnce.Rows, 1)

	firstRow, _, err := bh.DecodeRow(popOnce.Rows[0])
	require.NoError(t, err)
	require.Equal(t, int64(1), firstRow[0])
	firstValueBytes, ok := firstRow[1].([]byte)
	require.True(t, ok)
	firstValue := string(firstValueBytes)

	probe := buildInt64Vector(t, mp, []int64{1})
	defer probe.Free(mp)

	remaining, err := bh.GetByVectors([]*vector.Vector{probe})
	require.NoError(t, err)
	require.True(t, remaining[0].Exists)
	require.Len(t, remaining[0].Rows, 1)

	popRest, err := bh.PopByEncodedKey(encodedKey, true)
	require.NoError(t, err)
	require.Len(t, popRest, 1)
	require.True(t, popRest.Exists)
	require.Len(t, popRest.Rows, 1)
	secondRow, _, err := bh.DecodeRow(popRest.Rows[0])
	require.NoError(t, err)
	require.Equal(t, int64(1), secondRow[0])
	secondValueBytes, ok := secondRow[1].([]byte)
	require.True(t, ok)
	secondValue := string(secondValueBytes)
	require.NotEqual(t, firstValue, secondValue, "expected PopByEncodedKey to remove different rows on repeated calls")

	final, err := bh.GetByVectors([]*vector.Vector{probe})
	require.NoError(t, err)
	require.False(t, final[0].Exists)
	require.Empty(t, final[0].Rows)
}

func TestBranchHashmapPopByEncodedKeySpilled(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	allocator := newLimitedAllocator(80)
	bhIface, err := NewBranchHashmap(
		WithBranchHashmapAllocator(allocator),
		WithBranchHashmapSpillRoot(t.TempDir()),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, bhIface.Close())
	}()

	keys := make([]int64, 10)
	for i := range keys {
		keys[i] = 1
	}
	values := []string{"v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9"}

	keyVec := buildInt64Vector(t, mp, keys)
	valVec := buildStringVector(t, mp, values)
	defer keyVec.Free(mp)
	defer valVec.Free(mp)

	require.NoError(t, bhIface.PutByVectors([]*vector.Vector{keyVec, valVec}, []int{0}))

	impl := bhIface.(*branchHashmap)
	impl.mu.Lock()
	require.NoError(t, impl.spill(impl.memInUse))
	impl.mu.Unlock()
	require.Empty(t, impl.inMemory)
	require.NotEmpty(t, impl.spills)

	encodedKeys := collectInt64EncodedKeys(t, bhIface)
	encodedKey, ok := encodedKeys[1]
	require.True(t, ok)

	popPartial, err := bhIface.PopByEncodedKey(encodedKey, false)
	require.NoError(t, err)
	require.Len(t, popPartial, 1)
	require.True(t, popPartial.Exists)
	require.Len(t, popPartial.Rows, 1)

	probe := buildInt64Vector(t, mp, []int64{1})
	defer probe.Free(mp)

	afterPartial, err := bhIface.GetByVectors([]*vector.Vector{probe})
	require.NoError(t, err)
	require.True(t, afterPartial[0].Exists)
	require.Len(t, afterPartial[0].Rows, 9)

	popRemaining, err := bhIface.PopByEncodedKey(encodedKey, true)
	require.NoError(t, err)
	require.Len(t, popRemaining, 1)
	require.True(t, popRemaining.Exists)
	require.Len(t, popRemaining.Rows, 9)

	final, err := bhIface.GetByVectors([]*vector.Vector{probe})
	require.NoError(t, err)
	require.False(t, final[0].Exists)
	require.Empty(t, final[0].Rows)
}

func TestBranchHashmapValidatePutVectorTypeMismatch(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	initialKey := buildInt64Vector(t, mp, []int64{1})
	initialVal := buildStringVector(t, mp, []string{"one"})
	defer initialKey.Free(mp)
	defer initialVal.Free(mp)

	bh, err := NewBranchHashmap()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, bh.Close())
	}()

	require.NoError(t, bh.PutByVectors([]*vector.Vector{initialKey, initialVal}, []int{0}))

	badKey := buildInt32Vector(t, mp, []int32{2})
	newVal := buildStringVector(t, mp, []string{"two"})
	defer badKey.Free(mp)
	defer newVal.Free(mp)

	err = bh.PutByVectors([]*vector.Vector{badKey, newVal}, []int{0})
	require.Error(t, err)
	require.Contains(t, err.Error(), "vector type mismatch")
}

func TestBranchHashmapValidatePutKeyTypeMismatch(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	initialKey := buildInt64Vector(t, mp, []int64{1})
	initialVal := buildStringVector(t, mp, []string{"one"})
	defer initialKey.Free(mp)
	defer initialVal.Free(mp)

	bh, err := NewBranchHashmap()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, bh.Close())
	}()

	require.NoError(t, bh.PutByVectors([]*vector.Vector{initialKey, initialVal}, []int{0}))

	nextKey := buildInt64Vector(t, mp, []int64{2})
	nextVal := buildStringVector(t, mp, []string{"two"})
	defer nextKey.Free(mp)
	defer nextVal.Free(mp)

	err = bh.PutByVectors([]*vector.Vector{nextKey, nextVal}, []int{1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "key column type mismatch")
}

func TestBranchHashmapWithCustomConcurrency(t *testing.T) {
	const desired = 3

	bh, err := NewBranchHashmap(WithBranchHashmapConcurrency(desired))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, bh.Close())
	}()

	impl := bh.(*branchHashmap)
	require.Equal(t, desired, impl.concurrency)
}

func TestEncodeRowCoversManyTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	type encodeSpec struct {
		name   string
		typ    types.Type
		append func(*vector.Vector)
		assert func(any)
	}

	decimal128Val := types.Decimal128{B0_63: 123, B64_127: 456}
	uuidVal := types.Uuid{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	var tsValue types.TS
	for i := range tsValue {
		tsValue[i] = byte(i)
	}

	specs := []encodeSpec{
		{
			name: "bool",
			typ:  types.T_bool.ToType(),
			append: func(vec *vector.Vector) {
				require.NoError(t, vector.AppendFixed(vec, true, false, mp))
			},
			assert: func(v any) {
				require.Equal(t, true, v.(bool))
			},
		},
		{
			name: "int8",
			typ:  types.T_int8.ToType(),
			append: func(vec *vector.Vector) {
				require.NoError(t, vector.AppendFixed(vec, int8(-8), false, mp))
			},
			assert: func(v any) {
				require.Equal(t, int8(-8), v.(int8))
			},
		},
		{
			name: "int16",
			typ:  types.T_int16.ToType(),
			append: func(vec *vector.Vector) {
				require.NoError(t, vector.AppendFixed(vec, int16(-16), false, mp))
			},
			assert: func(v any) {
				require.Equal(t, int16(-16), v.(int16))
			},
		},
		{
			name: "int32",
			typ:  types.T_int32.ToType(),
			append: func(vec *vector.Vector) {
				require.NoError(t, vector.AppendFixed(vec, int32(-32), false, mp))
			},
			assert: func(v any) {
				require.Equal(t, int32(-32), v.(int32))
			},
		},
		{
			name: "int64",
			typ:  types.T_int64.ToType(),
			append: func(vec *vector.Vector) {
				require.NoError(t, vector.AppendFixed(vec, int64(-64), false, mp))
			},
			assert: func(v any) {
				require.Equal(t, int64(-64), v.(int64))
			},
		},
		{
			name: "uint8",
			typ:  types.T_uint8.ToType(),
			append: func(vec *vector.Vector) {
				require.NoError(t, vector.AppendFixed(vec, uint8(8), false, mp))
			},
			assert: func(v any) {
				require.Equal(t, uint8(8), v.(uint8))
			},
		},
		{
			name: "uint16",
			typ:  types.T_uint16.ToType(),
			append: func(vec *vector.Vector) {
				require.NoError(t, vector.AppendFixed(vec, uint16(16), false, mp))
			},
			assert: func(v any) {
				require.Equal(t, uint16(16), v.(uint16))
			},
		},
		{
			name: "uint32",
			typ:  types.T_uint32.ToType(),
			append: func(vec *vector.Vector) {
				require.NoError(t, vector.AppendFixed(vec, uint32(32), false, mp))
			},
			assert: func(v any) {
				require.Equal(t, uint32(32), v.(uint32))
			},
		},
		{
			name: "uint64",
			typ:  types.T_uint64.ToType(),
			append: func(vec *vector.Vector) {
				require.NoError(t, vector.AppendFixed(vec, uint64(64), false, mp))
			},
			assert: func(v any) {
				require.Equal(t, uint64(64), v.(uint64))
			},
		},
		{
			name: "float32",
			typ:  types.T_float32.ToType(),
			append: func(vec *vector.Vector) {
				require.NoError(t, vector.AppendFixed(vec, float32(3.14), false, mp))
			},
			assert: func(v any) {
				require.Equal(t, float32(3.14), v.(float32))
			},
		},
		{
			name: "float64",
			typ:  types.T_float64.ToType(),
			append: func(vec *vector.Vector) {
				require.NoError(t, vector.AppendFixed(vec, -6.28, false, mp))
			},
			assert: func(v any) {
				require.Equal(t, -6.28, v.(float64))
			},
		},
		{
			name: "decimal64",
			typ:  types.T_decimal64.ToType(),
			append: func(vec *vector.Vector) {
				require.NoError(t, vector.AppendFixed(vec, types.Decimal64(1234), false, mp))
			},
			assert: func(v any) {
				require.Equal(t, types.Decimal64(1234), v.(types.Decimal64))
			},
		},
		{
			name: "decimal128",
			typ:  types.T_decimal128.ToType(),
			append: func(vec *vector.Vector) {
				require.NoError(t, vector.AppendFixed(vec, decimal128Val, false, mp))
			},
			assert: func(v any) {
				require.Equal(t, decimal128Val, v.(types.Decimal128))
			},
		},
		{
			name: "date",
			typ:  types.T_date.ToType(),
			append: func(vec *vector.Vector) {
				require.NoError(t, vector.AppendFixed(vec, types.Date(20240101), false, mp))
			},
			assert: func(v any) {
				require.Equal(t, types.Date(20240101), v.(types.Date))
			},
		},
		{
			name: "time",
			typ:  types.T_time.ToType(),
			append: func(vec *vector.Vector) {
				require.NoError(t, vector.AppendFixed(vec, types.Time(123456789), false, mp))
			},
			assert: func(v any) {
				require.Equal(t, int64(123456789), v.(int64))
			},
		},
		{
			name: "datetime",
			typ:  types.T_datetime.ToType(),
			append: func(vec *vector.Vector) {
				require.NoError(t, vector.AppendFixed(vec, types.Datetime(20240101123456), false, mp))
			},
			assert: func(v any) {
				require.Equal(t, types.Datetime(20240101123456), v.(types.Datetime))
			},
		},
		{
			name: "timestamp",
			typ:  types.T_timestamp.ToType(),
			append: func(vec *vector.Vector) {
				require.NoError(t, vector.AppendFixed(vec, types.Timestamp(987654321), false, mp))
			},
			assert: func(v any) {
				require.Equal(t, types.Timestamp(987654321), v.(types.Timestamp))
			},
		},
		{
			name: "bit",
			typ:  types.T_bit.ToType(),
			append: func(vec *vector.Vector) {
				require.NoError(t, vector.AppendFixed(vec, uint64(0b1011), false, mp))
			},
			assert: func(v any) {
				require.Equal(t, uint64(0b1011), v.(uint64))
			},
		},
		{
			name: "enum",
			typ:  types.T_enum.ToType(),
			append: func(vec *vector.Vector) {
				require.NoError(t, vector.AppendFixed(vec, types.Enum(42), false, mp))
			},
			assert: func(v any) {
				require.Equal(t, uint16(42), v.(uint16))
			},
		},
		{
			name: "uuid",
			typ:  types.T_uuid.ToType(),
			append: func(vec *vector.Vector) {
				require.NoError(t, vector.AppendFixed(vec, uuidVal, false, mp))
			},
			assert: func(v any) {
				require.Equal(t, uuidVal, v.(types.Uuid))
			},
		},
		{
			name: "char",
			typ:  types.T_char.ToType(),
			append: func(vec *vector.Vector) {
				require.NoError(t, vector.AppendBytes(vec, []byte("char"), false, mp))
			},
			assert: func(v any) {
				require.Equal(t, "char", string(v.([]byte)))
			},
		},
		{
			name: "array_float32",
			typ:  types.T_array_float32.ToType(),
			append: func(vec *vector.Vector) {
				require.NoError(t, vector.AppendBytes(vec, []byte{0, 1, 2}, false, mp))
			},
			assert: func(v any) {
				require.Equal(t, []byte{0, 1, 2}, v.([]byte))
			},
		},
		{
			name: "ts_default_case",
			typ:  types.T_TS.ToType(),
			append: func(vec *vector.Vector) {
				require.NoError(t, vector.AppendFixed(vec, tsValue, false, mp))
			},
			assert: func(v any) {
				require.Equal(t, tsValue[:], v.([]byte))
			},
		},
	}

	vecs := make([]*vector.Vector, len(specs))
	indexes := make([]int, len(specs))
	for i, spec := range specs {
		vec := vector.NewVec(spec.typ)
		spec.append(vec)
		vecs[i] = vec
		indexes[i] = i
		defer vec.Free(mp)
	}

	packer := types.NewPacker()
	defer packer.Close()

	require.NoError(t, encodeRow(packer, vecs, indexes, 0))
	tuple, err := types.Unpack(packer.Bytes())
	require.NoError(t, err)
	require.Len(t, tuple, len(specs))

	for i, spec := range specs {
		spec.assert(tuple[i])
	}
}

func collectInt64EncodedKeys(t *testing.T, bh BranchHashmap) map[int64][]byte {
	t.Helper()

	result := make(map[int64][]byte)
	err := bh.ForEach(func(key []byte, rows [][]byte) error {
		_ = rows
		tuple, _, err := bh.DecodeRow(key)
		require.NoError(t, err)
		require.Len(t, tuple, 1)
		keyVal, ok := tuple[0].(int64)
		require.True(t, ok)
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		result[keyVal] = keyCopy
		return nil
	})
	require.NoError(t, err)
	return result
}

func buildInt64Vector(t *testing.T, mp *mpool.MPool, values []int64) *vector.Vector {
	t.Helper()
	vec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(vec, values, nil, mp))
	return vec
}

func buildStringVector(t *testing.T, mp *mpool.MPool, values []string) *vector.Vector {
	t.Helper()
	vec := vector.NewVec(types.T_varchar.ToType())
	for _, v := range values {
		require.NoError(t, vector.AppendBytes(vec, []byte(v), false, mp))
	}
	return vec
}

func buildInt32Vector(t *testing.T, mp *mpool.MPool, values []int32) *vector.Vector {
	t.Helper()
	vec := vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixedList(vec, values, nil, mp))
	return vec
}

type limitedAllocator struct {
	mu    sync.Mutex
	limit uint64
	used  uint64
}

func newLimitedAllocator(limit uint64) *limitedAllocator {
	return &limitedAllocator{limit: limit}
}

func (l *limitedAllocator) Allocate(size uint64, _ malloc.Hints) ([]byte, malloc.Deallocator, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.used+size > l.limit {
		return nil, nil, nil
	}
	buf := make([]byte, int(size))
	l.used += size
	return buf, &limitedDeallocator{allocator: l, size: size}, nil
}

type limitedDeallocator struct {
	allocator *limitedAllocator
	size      uint64
}

func (d *limitedDeallocator) Deallocate(_ malloc.Hints) {
	d.allocator.mu.Lock()
	d.allocator.used -= d.size
	d.allocator.mu.Unlock()
}

func (d *limitedDeallocator) As(malloc.Trait) bool {
	return false
}
