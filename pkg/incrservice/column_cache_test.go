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
	"math"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
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
			require.NoError(t, c.allocateLocked(ctx, 0, 200, 0, nil))
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
			require.NoError(t, c.allocateLocked(ctx, 0, 200, 0, nil))
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
		8,
		newTestVector[int8](8, types.New(types.T_int8, 0, 0), nil, nil),
		newTestVector(8, types.New(types.T_int8, 0, 0), fillValues, fillRows),
	)
}

func TestInsertInt8WithManual(t *testing.T) {
	manualValues := []int8{6, 9}
	manualRows := []int{0, 1}

	fillValues := []int8{6, 9, 10, 11, 12, 13, 14, 15}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[int8](
		t,
		8,
		15,
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
		8,
		newTestVector[int16](8, types.New(types.T_int16, 0, 0), nil, nil),
		newTestVector(8, types.New(types.T_int16, 0, 0), fillValues, fillRows),
	)
}

func TestInsertInt16WithManual(t *testing.T) {
	manualValues := []int16{6, 9}
	manualRows := []int{0, 1}

	fillValues := []int16{6, 9, 10, 11, 12, 13, 14, 15}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[int16](
		t,
		8,
		15,
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
		8,
		newTestVector[int32](8, types.New(types.T_int32, 0, 0), nil, nil),
		newTestVector(8, types.New(types.T_int32, 0, 0), fillValues, fillRows),
	)
}

func TestInsertInt32WithManual(t *testing.T) {
	manualValues := []int32{6, 9}
	manualRows := []int{0, 1}

	fillValues := []int32{6, 9, 10, 11, 12, 13, 14, 15}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[int32](
		t,
		8,
		15,
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
		8,
		newTestVector[int64](8, types.New(types.T_int64, 0, 0), nil, nil),
		newTestVector(8, types.New(types.T_int64, 0, 0), fillValues, fillRows),
	)
}

func TestInsertInt64WithManual(t *testing.T) {
	manualValues := []int64{6, 9}
	manualRows := []int{0, 1}

	fillValues := []int64{6, 9, 10, 11, 12, 13, 14, 15}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[int64](
		t,
		8,
		15,
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
		8,
		newTestVector[uint8](8, types.New(types.T_uint8, 0, 0), nil, nil),
		newTestVector(8, types.New(types.T_uint8, 0, 0), fillValues, fillRows),
	)
}

func TestInsertUint8WithManual(t *testing.T) {
	manualValues := []uint8{6, 9}
	manualRows := []int{0, 1}

	fillValues := []uint8{6, 9, 10, 11, 12, 13, 14, 15}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[uint8](
		t,
		8,
		15,
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
		8,
		newTestVector[uint16](8, types.New(types.T_uint16, 0, 0), nil, nil),
		newTestVector(8, types.New(types.T_uint16, 0, 0), fillValues, fillRows),
	)
}

func TestInsertUint16WithManual(t *testing.T) {
	manualValues := []uint16{6, 9}
	manualRows := []int{0, 1}

	fillValues := []uint16{6, 9, 10, 11, 12, 13, 14, 15}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[uint16](
		t,
		8,
		15,
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
		8,
		newTestVector[uint32](8, types.New(types.T_uint32, 0, 0), nil, nil),
		newTestVector(8, types.New(types.T_uint32, 0, 0), fillValues, fillRows),
	)
}

func TestInsertUint32WithManual(t *testing.T) {
	manualValues := []uint32{6, 9}
	manualRows := []int{0, 1}

	fillValues := []uint32{6, 9, 10, 11, 12, 13, 14, 15}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[uint32](
		t,
		8,
		15,
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
		8,
		newTestVector[uint64](8, types.New(types.T_uint64, 0, 0), nil, nil),
		newTestVector(8, types.New(types.T_uint64, 0, 0), fillValues, fillRows),
	)
}

func TestInsertUint64WithManual(t *testing.T) {
	manualValues := []uint64{6, 9}
	manualRows := []int{0, 1}

	fillValues := []uint64{6, 9, 10, 11, 12, 13, 14, 15}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[uint64](
		t,
		8,
		15,
		newTestVector(8, types.New(types.T_uint64, 0, 0), manualValues, manualRows),
		newTestVector(8, types.New(types.T_uint64, 0, 0), fillValues, fillRows),
	)
}

func TestInsertWithManualMixed(t *testing.T) {
	manualValues := []uint64{3, 6}
	manualRows := []int{1, 3}

	fillValues := []uint64{1, 3, 4, 6, 7, 8, 9, 10}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[uint64](
		t,
		8,
		10,
		newTestVector(8, types.New(types.T_uint64, 0, 0), manualValues, manualRows),
		newTestVector(8, types.New(types.T_uint64, 0, 0), fillValues, fillRows),
	)
}

func TestLastInsertValueWithNoAutoInserted(t *testing.T) {
	manualValues := []uint64{1, 2, 3, 4, 5, 6, 7, 8}
	manualRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[uint64](
		t,
		8,
		0,
		newTestVector(8, types.New(types.T_uint64, 0, 0), manualValues, manualRows),
		newTestVector(8, types.New(types.T_uint64, 0, 0), manualValues, manualRows),
	)
}

func TestOverflow(t *testing.T) {
	runColumnCacheTests(
		t,
		1,
		1,
		func(
			ctx context.Context,
			cc *columnCache) {
			require.NoError(t, cc.updateTo(ctx, 0, math.MaxUint64, nil))
			require.True(t, cc.overflow)

			require.NoError(t,
				cc.applyAutoValues(
					ctx,
					0,
					1,
					nil,
					func(i int) bool { return false },
					func(i int, u uint64) error {
						require.Equal(t, uint64(0), u)
						return nil
					},
					nil))
		},
	)
}

func TestOverflowWithInit(t *testing.T) {
	runColumnCacheTestsWithInitOffset(
		t,
		1,
		1,
		math.MaxUint64,
		func(
			ctx context.Context,
			cc *columnCache) {
			require.True(t, cc.overflow)

			require.NoError(t,
				cc.applyAutoValues(
					ctx,
					0,
					1,
					nil,
					func(i int) bool { return false },
					func(i int, u uint64) error {
						require.Equal(t, uint64(0), u)
						return nil
					},
					nil))
		},
	)
}

func TestMergeAllocate(t *testing.T) {
	total := 3600000
	goroutines := 60
	batch := 6000
	rowsPerGoroutine := total / goroutines
	var added atomic.Uint64
	capacity := 10000
	runColumnCacheTests(
		t,
		capacity,
		1,
		func(
			ctx context.Context,
			cc *columnCache) {
			var wg sync.WaitGroup
			for i := 0; i < goroutines; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					n := rowsPerGoroutine / batch
					for i := 0; i < n; i++ {
						cc.applyAutoValues(
							ctx,
							0,
							batch,
							nil,
							func(i int) bool { return false },
							func(i int, u uint64) error {
								added.Add(1)
								return nil
							},
							nil)
					}
				}()
			}

			wg.Wait()
			assert.Equal(t, uint64(total), added.Load())
			assert.True(t, cc.allocateCount.Load() < 360)
		},
	)
}

func TestIssue9840(t *testing.T) {
	fillValues := []uint64{1, 2, 3, 4, 5, 6, 7, 8}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	input := newTestVector[uint64](8, types.New(types.T_uint64, 0, 0), nil, nil)
	// index 0 is manual, others is null, but index 1 has a invalid value
	vector.SetFixedAt[uint64](input, 0, 1)
	vector.SetFixedAt[uint64](input, 1, 5)
	input.GetNulls().Del(0)
	testColumnCacheInsert[uint64](
		t,
		8,
		8,
		input,
		newTestVector(8, types.New(types.T_uint64, 0, 0), fillValues, fillRows),
	)
}

func testColumnCacheInsert[T constraints.Integer](
	t *testing.T,
	rows int,
	expectLastInsertValue uint64,
	input *vector.Vector,
	expect *vector.Vector) {
	runColumnCacheTests(
		t,
		10,
		1,
		func(
			ctx context.Context,
			c *columnCache) {
			lastInsertValue, err := c.insertAutoValues(ctx, 0, input, rows, nil)
			require.NoError(t, err)
			assert.Equal(t, expectLastInsertValue, lastInsertValue)
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
	for i, v := range fillRows {
		fillMap[v] = fillValues[i]
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
	runColumnCacheTestsWithInitOffset(
		t,
		capacity,
		step,
		0,
		fn)
}

func runColumnCacheTestsWithInitOffset(
	t *testing.T,
	capacity int,
	step int,
	offset uint64,
	fn func(context.Context, *columnCache),
) {
	defer leaktest.AfterTest(t)()
	runtime.SetupProcessLevelRuntime(runtime.DefaultRuntime())
	runAllocatorTests(
		t,
		func(a valueAllocator) {
			ctx, cancel := context.WithCancel(defines.AttachAccountId(context.Background(), catalog.System_Account))
			defer cancel()
			col := AutoColumn{
				ColName: "k1",
				Offset:  offset,
				Step:    uint64(step),
			}
			a.(*allocator).store.Create(
				ctx,
				0,
				[]AutoColumn{col},
				nil)
			cc, err := newColumnCache(ctx, 0, col, Config{CountPerAllocate: capacity}, a, nil)
			require.NoError(t, err)
			fn(ctx, cc)
		},
	)
}
