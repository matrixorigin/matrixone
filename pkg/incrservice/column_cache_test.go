// Copyright 2023 Matrix Origin
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

package incrservice

import (
	"context"
	"testing"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
)

func TestNewColumnCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runColumnCacheTests(
		t,
		100,
		1,
		func(
			ctx context.Context,
			c *columnCache) {
			c.Lock()
			defer c.Unlock()
			require.NoError(t, c.waitPrevAllocatingLocked(ctx))
			assert.Equal(t, 100, c.ranges.left())
		},
	)
}

func TestColumnCacheAllocate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runColumnCacheTests(
		t,
		100,
		1,
		func(
			ctx context.Context,
			c *columnCache) {
			c.Lock()
			require.NoError(t, c.waitPrevAllocatingLocked(ctx))
			require.NoError(t, c.allocateLocked(ctx, 200))
			c.Unlock()

			c.Lock()
			defer c.Unlock()
			assert.Equal(t, 300, c.ranges.left())
		},
	)
}

func TestColumnCacheInsert(t *testing.T) {
	defer leaktest.AfterTest(t)()
	runColumnCacheTests(
		t,
		100,
		1,
		func(
			ctx context.Context,
			c *columnCache) {
			c.Lock()
			require.NoError(t, c.waitPrevAllocatingLocked(ctx))
			require.NoError(t, c.allocateLocked(ctx, 200))
			c.Unlock()

			c.Lock()
			defer c.Unlock()
			assert.Equal(t, 300, c.ranges.left())
		},
	)
}

func TestInsertInt8(t *testing.T) {
	fillValues := []int8{1, 2, 3, 4, 5, 6, 7, 8}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[int8](
		t,
		8,
		newTestVector[int8](8, types.New(types.T_int8, 0, 0), nil, nil),
		newTestVector(8, types.New(types.T_int8, 0, 0), fillValues, fillRows),
	)
}

func TestInsertInt8WithManual(t *testing.T) {
	manualValues := []int8{6, 9}
	manualRows := []int{0, 1}

	fillValues := []int8{6, 9, 1, 2, 3, 4, 5, 7, 8}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[int8](
		t,
		8,
		newTestVector(8, types.New(types.T_int8, 0, 0), manualValues, manualRows),
		newTestVector(8, types.New(types.T_int8, 0, 0), fillValues, fillRows),
	)
}

func TestInsertInt16(t *testing.T) {
	fillValues := []int16{1, 2, 3, 4, 5, 6, 7, 8}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[int16](
		t,
		8,
		newTestVector[int16](8, types.New(types.T_int16, 0, 0), nil, nil),
		newTestVector(8, types.New(types.T_int16, 0, 0), fillValues, fillRows),
	)
}

func TestInsertInt16WithManual(t *testing.T) {
	manualValues := []int16{6, 9}
	manualRows := []int{0, 1}

	fillValues := []int16{6, 9, 1, 2, 3, 4, 5, 7, 8}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[int16](
		t,
		8,
		newTestVector(8, types.New(types.T_int16, 0, 0), manualValues, manualRows),
		newTestVector(8, types.New(types.T_int16, 0, 0), fillValues, fillRows),
	)
}

func TestInsertInt32(t *testing.T) {
	fillValues := []int32{1, 2, 3, 4, 5, 6, 7, 8}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[int32](
		t,
		8,
		newTestVector[int32](8, types.New(types.T_int32, 0, 0), nil, nil),
		newTestVector(8, types.New(types.T_int32, 0, 0), fillValues, fillRows),
	)
}

func TestInsertInt32WithManual(t *testing.T) {
	manualValues := []int32{6, 9}
	manualRows := []int{0, 1}

	fillValues := []int32{6, 9, 1, 2, 3, 4, 5, 7, 8}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[int32](
		t,
		8,
		newTestVector(8, types.New(types.T_int32, 0, 0), manualValues, manualRows),
		newTestVector(8, types.New(types.T_int32, 0, 0), fillValues, fillRows),
	)
}

func TestInsertInt64(t *testing.T) {
	fillValues := []int64{1, 2, 3, 4, 5, 6, 7, 8}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[int64](
		t,
		8,
		newTestVector[int64](8, types.New(types.T_int64, 0, 0), nil, nil),
		newTestVector(8, types.New(types.T_int64, 0, 0), fillValues, fillRows),
	)
}

func TestInsertInt64WithManual(t *testing.T) {
	manualValues := []int64{6, 9}
	manualRows := []int{0, 1}

	fillValues := []int64{6, 9, 1, 2, 3, 4, 5, 7, 8}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[int64](
		t,
		8,
		newTestVector(8, types.New(types.T_int64, 0, 0), manualValues, manualRows),
		newTestVector(8, types.New(types.T_int64, 0, 0), fillValues, fillRows),
	)
}

func TestInsertUint8(t *testing.T) {
	fillValues := []uint8{1, 2, 3, 4, 5, 6, 7, 8}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[uint8](
		t,
		8,
		newTestVector[uint8](8, types.New(types.T_uint8, 0, 0), nil, nil),
		newTestVector(8, types.New(types.T_uint8, 0, 0), fillValues, fillRows),
	)
}

func TestInsertUint8WithManual(t *testing.T) {
	manualValues := []uint8{6, 9}
	manualRows := []int{0, 1}

	fillValues := []uint8{6, 9, 1, 2, 3, 4, 5, 7, 8}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[uint8](
		t,
		8,
		newTestVector(8, types.New(types.T_uint8, 0, 0), manualValues, manualRows),
		newTestVector(8, types.New(types.T_uint8, 0, 0), fillValues, fillRows),
	)
}

func TestInsertUint16(t *testing.T) {
	fillValues := []uint16{1, 2, 3, 4, 5, 6, 7, 8}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[uint16](
		t,
		8,
		newTestVector[uint16](8, types.New(types.T_uint16, 0, 0), nil, nil),
		newTestVector(8, types.New(types.T_uint16, 0, 0), fillValues, fillRows),
	)
}

func TestInsertUint16WithManual(t *testing.T) {
	manualValues := []uint16{6, 9}
	manualRows := []int{0, 1}

	fillValues := []uint16{6, 9, 1, 2, 3, 4, 5, 7, 8}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[uint16](
		t,
		8,
		newTestVector(8, types.New(types.T_uint16, 0, 0), manualValues, manualRows),
		newTestVector(8, types.New(types.T_uint16, 0, 0), fillValues, fillRows),
	)
}

func TestInsertUint32(t *testing.T) {
	fillValues := []uint32{1, 2, 3, 4, 5, 6, 7, 8}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[uint32](
		t,
		8,
		newTestVector[uint32](8, types.New(types.T_uint32, 0, 0), nil, nil),
		newTestVector(8, types.New(types.T_uint32, 0, 0), fillValues, fillRows),
	)
}

func TestInsertUint32WithManual(t *testing.T) {
	manualValues := []uint32{6, 9}
	manualRows := []int{0, 1}

	fillValues := []uint32{6, 9, 1, 2, 3, 4, 5, 7, 8}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[uint32](
		t,
		8,
		newTestVector(8, types.New(types.T_uint32, 0, 0), manualValues, manualRows),
		newTestVector(8, types.New(types.T_uint32, 0, 0), fillValues, fillRows),
	)
}

func TestInsertUint64(t *testing.T) {
	fillValues := []uint64{1, 2, 3, 4, 5, 6, 7, 8}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[uint64](
		t,
		8,
		newTestVector[uint64](8, types.New(types.T_uint64, 0, 0), nil, nil),
		newTestVector(8, types.New(types.T_uint64, 0, 0), fillValues, fillRows),
	)
}

func TestInsertUint64WithManual(t *testing.T) {
	manualValues := []uint64{6, 9}
	manualRows := []int{0, 1}

	fillValues := []uint64{6, 9, 1, 2, 3, 4, 5, 7, 8}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[uint64](
		t,
		8,
		newTestVector(8, types.New(types.T_uint64, 0, 0), manualValues, manualRows),
		newTestVector(8, types.New(types.T_uint64, 0, 0), fillValues, fillRows),
	)
}

func testColumnCacheInsert[T constraints.Integer](
	t *testing.T,
	rows int,
	input *vector.Vector,
	expect *vector.Vector) {
	runColumnCacheTests(
		t,
		1,
		1,
		func(
			ctx context.Context,
			c *columnCache) {
			require.NoError(t,
				c.insertAutoValues(ctx, input, rows))
			assert.Equal(t,
				vector.MustFixedCol[T](expect),
				vector.MustFixedCol[T](input))
		},
	)
}

func newTestVector[T constraints.Integer](
	rows int,
	vecType types.Type,
	fillValues []T,
	fillRows []int) *vector.Vector {
	fillMap := make(map[int]T)
	for _, i := range fillRows {
		fillMap[i] = fillValues[i]
	}

	vec := vector.NewVec(vecType)
	for i := 0; i < rows; i++ {
		if v, ok := fillMap[i]; ok {
			vector.AppendFixed(vec, v, false, mpool.MustNew("test"))
		} else {
			vector.AppendFixed[T](vec, 0, true, mpool.MustNew("test"))
		}
	}
	return vec
}

func runColumnCacheTests(
	t *testing.T,
	capacity int,
	step int,
	fn func(context.Context, *columnCache),
) {
	defer leaktest.AfterTest(t)()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	runAllocatorTests(
		t,
		func(a valueAllocator) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			a.(*allocator).store.Create(ctx, "k1", 0, step)
			fn(ctx, newColumnCache(ctx, "k1", capacity, step, a))
		},
	)
}
