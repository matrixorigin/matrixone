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
