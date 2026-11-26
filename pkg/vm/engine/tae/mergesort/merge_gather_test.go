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

package mergesort

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/mocks"
	"github.com/stretchr/testify/require"
)

// mergeInput builds batches of key (sorted within each batch), id (the
// global row id), a varchar payload with short and long values, and a
// nullable int32. Rows whose id satisfies deleted are deleted.
func mergeInput(sizes []int, key func(id int) int64, deleted func(id int) bool) []*containers.Batch {
	bats := make([]*containers.Batch, len(sizes))
	id := 0
	for b, n := range sizes {
		bat := containers.NewBatch()
		keys := containers.NewVector(types.T_int64.ToType())
		ids := containers.NewVector(types.T_int64.ToType())
		strs := containers.NewVector(types.T_varchar.ToType())
		opt := containers.NewVector(types.T_int32.ToType())
		for r := 0; r < n; r++ {
			keys.Append(key(id), false)
			ids.Append(int64(id), false)
			strs.Append([]byte(mergePayload(id)), false)
			opt.Append(int32(id), id%7 == 0)
			if deleted(id) {
				bat.Delete(r)
			}
			id++
		}
		bat.AddVector("key", keys)
		bat.AddVector("id", ids)
		bat.AddVector("s", strs)
		bat.AddVector("opt", opt)
		bats[b] = bat
	}
	return bats
}

func mergePayload(id int) string {
	if id%3 == 0 {
		return fmt.Sprintf("%s-%d", strings.Repeat("x", 30), id)
	}
	return fmt.Sprintf("s%d", id)
}

func closeBatches(bats []*containers.Batch) {
	for _, bat := range bats {
		bat.Close()
	}
}

// checkMergeOutput checks that every kept row landed where mapping says,
// with all its columns, that dropped rows map to -1, and that the blocks
// follow the layout.
func checkMergeOutput(t *testing.T, ret []*batch.Batch, mapping []int64, total int, kept func(id int) bool, toLayout []uint32) {
	t.Helper()
	require.Len(t, mapping, total)
	var outIDs []int64
	var outStrs []string
	var outOpt []int32
	var outNull []bool
	for b, bat := range ret {
		if bat == nil {
			continue
		}
		require.LessOrEqual(t, uint32(bat.RowCount()), toLayout[b])
		outIDs = append(outIDs, vector.MustFixedColWithTypeCheck[int64](bat.Vecs[1])...)
		for i := 0; i < bat.RowCount(); i++ {
			outStrs = append(outStrs, bat.Vecs[2].GetStringAt(i))
			outNull = append(outNull, bat.Vecs[3].IsNull(uint64(i)))
		}
		outOpt = append(outOpt, vector.MustFixedColWithTypeCheck[int32](bat.Vecs[3])...)
	}
	keptRows := 0
	for id := 0; id < total; id++ {
		if !kept(id) {
			require.Equal(t, int64(-1), mapping[id], "row %d", id)
			continue
		}
		keptRows++
		at := mapping[id]
		require.GreaterOrEqual(t, at, int64(0), "row %d", id)
		require.Equal(t, int64(id), outIDs[at])
		require.Equal(t, mergePayload(id), outStrs[at])
		require.Equal(t, id%7 == 0, outNull[at])
		if id%7 != 0 {
			require.Equal(t, int32(id), outOpt[at])
		}
	}
	require.Equal(t, keptRows, len(outIDs))
}

func TestReshapeBatchesMapping(t *testing.T) {
	pool := &testPool{pool: mocks.GetTestVectorPool()}
	sizes := []int{3000, 9000, 5000}
	total := 17000
	deleted := func(id int) bool { return id%11 == 0 }
	survivors := 0
	for id := 0; id < total; id++ {
		if !deleted(id) {
			survivors++
		}
	}

	t.Run("fits", func(t *testing.T) {
		bats := mergeInput(sizes, func(id int) int64 { return int64(id) }, deleted)
		defer closeBatches(bats)
		// The first block spans more than one gather chunk.
		toLayout := []uint32{10000, uint32(survivors - 10000)}
		ret, release, mapping, err := ReshapeBatches(bats, toLayout, pool)
		require.NoError(t, err)
		defer release()
		for b, bat := range ret {
			require.Equal(t, int(toLayout[b]), bat.RowCount())
		}
		checkMergeOutput(t, ret, mapping, total, func(id int) bool { return !deleted(id) }, toLayout)
	})

	t.Run("rows past the layout are dropped", func(t *testing.T) {
		bats := mergeInput(sizes, func(id int) int64 { return int64(id) }, deleted)
		defer closeBatches(bats)
		toLayout := []uint32{4000, 4000}
		ret, release, mapping, err := ReshapeBatches(bats, toLayout, pool)
		require.NoError(t, err)
		defer release()
		// The first 8000 kept rows are kept.
		seen := 0
		keep := make([]bool, total)
		for id := 0; id < total && seen < 8000; id++ {
			if !deleted(id) {
				keep[id] = true
				seen++
			}
		}
		checkMergeOutput(t, ret, mapping, total, func(id int) bool { return keep[id] }, toLayout)
	})

	t.Run("a full block makes the next one", func(t *testing.T) {
		bats := mergeInput([]int{5}, func(id int) int64 { return int64(id) }, func(int) bool { return false })
		defer closeBatches(bats)
		ret, release, mapping, err := ReshapeBatches(bats, []uint32{5, 5, 5}, pool)
		require.NoError(t, err)
		defer release()
		require.Equal(t, 5, ret[0].RowCount())
		require.NotNil(t, ret[1])
		require.Equal(t, 0, ret[1].RowCount())
		require.Nil(t, ret[2])
		require.Equal(t, []int64{0, 1, 2, 3, 4}, mapping)
	})

	t.Run("everything deleted", func(t *testing.T) {
		bats := mergeInput([]int{4}, func(id int) int64 { return int64(id) }, func(int) bool { return true })
		defer closeBatches(bats)
		ret, release, mapping, err := ReshapeBatches(bats, []uint32{4}, pool)
		require.NoError(t, err)
		defer release()
		require.Equal(t, 0, ret[0].RowCount())
		require.Equal(t, []int64{-1, -1, -1, -1}, mapping)
	})
}

func TestAObjMergeMapping(t *testing.T) {
	pool := &testPool{pool: mocks.GetTestVectorPool()}
	sizes := []int{3000, 9000, 5000}
	total := 17000
	// Keys interleave across the batches and repeat.
	key := func(id int) int64 {
		switch {
		case id < 3000:
			return int64(id * 3)
		case id < 12000:
			return int64((id - 3000))
		default:
			return int64((id - 12000) * 2)
		}
	}
	deleted := func(id int) bool { return id%13 == 5 }
	survivors := 0
	for id := 0; id < total; id++ {
		if !deleted(id) {
			survivors++
		}
	}
	bats := mergeInput(sizes, key, deleted)
	defer closeBatches(bats)
	toLayout := []uint32{10000, uint32(survivors - 10000)}
	ret, release, mapping, err := MergeAObj(context.Background(), pool, bats, 0, toLayout)
	require.NoError(t, err)
	defer release()
	keys := make([]int64, 0, survivors)
	for b, bat := range ret {
		require.Equal(t, int(toLayout[b]), bat.RowCount())
		keys = append(keys, vector.MustFixedColWithTypeCheck[int64](bat.Vecs[0])...)
	}
	for i := 1; i < len(keys); i++ {
		require.LessOrEqual(t, keys[i-1], keys[i])
	}
	checkMergeOutput(t, ret, mapping, total, func(id int) bool { return !deleted(id) }, toLayout)
}

func TestAObjMergeErrors(t *testing.T) {
	pool := &testPool{pool: mocks.GetTestVectorPool()}
	none := func(int) bool { return false }

	bats := mergeInput([]int{10, 10}, func(id int) int64 { return int64(id % 10) }, none)
	defer closeBatches(bats)
	_, _, _, err := MergeAObj(context.Background(), pool, bats, 0, []uint32{8, 8})
	require.Error(t, err, "20 rows do not fit a layout of 16")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, _, err = MergeAObj(ctx, pool, bats, 0, []uint32{20})
	require.ErrorIs(t, err, context.Canceled)
}
