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
	"fmt"
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
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
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

func TestColumnCacheInsertHonorsStatementSeries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	input := newTestVector[uint64](5, types.New(types.T_uint64, 0, 0), nil, nil)
	runColumnCacheTests(
		t,
		10,
		1,
		func(ctx context.Context, c *columnCache) {
			statementCtx := WithAutoIncrementOptions(ctx, 3, 2)
			lastInsertValue, err := c.insertAutoValues(
				statementCtx, 0, input, input.Length(), nil)
			require.NoError(t, err)
			require.Equal(t, uint64(2), lastInsertValue)
			require.Equal(t, []uint64{2, 5, 8, 11, 14},
				vector.MustFixedColWithTypeCheck[uint64](input))
		},
	)
}

func TestColumnCacheSessionSeriesAmortizesAllocation(t *testing.T) {
	runColumnCacheTests(t, 30, 1, func(ctx context.Context, c *columnCache) {
		ctx = WithAutoIncrementOptions(ctx, 3, 2)
		mp := mpool.MustNew("series-test")
		for i := 0; i < 100; i++ {
			v := vector.NewVec(types.T_uint64.ToType())
			require.NoError(t, vector.AppendFixed(v, uint64(0), true, mp))
			id, err := c.insertAutoValues(ctx, 0, v, 1, nil)
			v.Free(mp)
			require.NoError(t, err)
			require.Equal(t, uint64(2+3*i), id)
		}
		// Ten owned spans suffice for 100 one-row statements. The counter also
		// includes initial allocation, and guards against per-statement I/O.
		require.LessOrEqual(t, c.allocateCount.Load(), uint64(10))
		require.Zero(t, mp.CurrNB())
	})
}

func TestColumnCacheManualValuesAdvanceOnlyPositiveSequence(t *testing.T) {
	for _, tc := range []struct {
		name      string
		manual    int64
		increment uint64
		offset    uint64
		first     int64
	}{
		{"negative", -1, 1, 1, 1},
		{"minimum_signed", math.MinInt64, 1, 1, 1},
		{"explicit_zero", 0, 1, 1, 1},
		{"positive_control", 5, 1, 1, 6},
		{"negative_session_series", -1, 3, 2, 2},
		{"positive_session_series", 5, 3, 2, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runColumnCacheTests(t, 200, 1, func(ctx context.Context, c *columnCache) {
				mp := mpool.MustNewZero()
				input := vector.NewVec(types.T_int64.ToType())
				defer input.Free(mp)
				for i, v := range []int64{tc.manual, 0, 100, 0} {
					require.NoError(t, vector.AppendFixed(input, v, i == 1 || i == 3, mp))
				}
				ctx = WithAutoIncrementOptions(ctx, tc.increment, tc.offset)
				first, err := c.insertAutoValues(ctx, 0, input, 4, nil)
				require.NoError(t, err)
				require.Equal(t, uint64(tc.first), first)
				require.Equal(t, []int64{tc.manual, tc.first, 100, 101},
					vector.MustFixedColNoTypeCheck[int64](input))
				input.Free(mp)
				require.Zero(t, mp.CurrNB())
			})
		})
	}
}

func BenchmarkColumnCacheStatementSeries(b *testing.B) {
	for _, increment := range []uint64{1, 3, 64} {
		b.Run(fmt.Sprintf("increment_%d", increment), func(b *testing.B) {
			runtime.RunTest("", func(rt runtime.Runtime) {
				ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
				ctx = WithAutoIncrementOptions(ctx, increment, 1)
				store := NewMemStore()
				col := AutoColumn{ColName: "id", Step: 1}
				require.NoError(b, store.Create(ctx, 0, []AutoColumn{col}, nil))
				a := newValueAllocator("", store)
				defer a.close()
				c, err := newColumnCache(ctx, "", 0, col, Config{CountPerAllocate: 10000}, true, a, nil)
				require.NoError(b, err)
				var previous uint64
				apply := func(_ int, value uint64) error {
					if value <= previous || (value-1)%increment != 0 {
						b.Fatalf("invalid generated value %d after %d", value, previous)
					}
					previous = value
					return nil
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					err := c.applyAutoValues(ctx, 0, 1, nil, func(int) bool { return false }, apply, nil,
						NormalizeAutoIncrementOptions(increment, 1), 1)
					if err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(c.allocateCount.Load())/float64(b.N), "allocations/op")
			})
		})
	}
}

func TestColumnCacheConcurrentSessionSeries(t *testing.T) {
	runColumnCacheTests(t, 1000, 1, func(ctx context.Context, c *columnCache) {
		start := make(chan struct{})
		values := make(chan uint64, 60)
		errors := make(chan error, 3)
		var ready, done sync.WaitGroup
		for _, increment := range []uint64{1, 3, 64} {
			ready.Add(1)
			done.Add(1)
			go func(increment uint64) {
				defer done.Done()
				mp := mpool.MustNew("concurrent-series")
				statementCtx := WithAutoIncrementOptions(ctx, increment, 1)
				ready.Done()
				<-start
				var previous uint64
				for i := 0; i < 20; i++ {
					v := vector.NewVec(types.T_uint64.ToType())
					if err := vector.AppendFixed(v, uint64(0), true, mp); err != nil {
						v.Free(mp)
						errors <- err
						return
					}
					id, err := c.insertAutoValues(statementCtx, 0, v, 1, nil)
					v.Free(mp)
					if err != nil {
						errors <- err
						return
					}
					if id <= previous || (id-1)%increment != 0 {
						errors <- fmt.Errorf("series %d: value %d after %d", increment, id, previous)
						return
					}
					values <- id
					previous = id
				}
				if mp.CurrNB() != 0 {
					errors <- fmt.Errorf("unreleased vector memory: %d", mp.CurrNB())
				}
			}(increment)
		}
		ready.Wait()
		close(start)
		done.Wait()
		close(errors)
		close(values)
		for err := range errors {
			require.NoError(t, err)
		}
		seen := make(map[uint64]bool)
		for id := range values {
			require.False(t, seen[id], "sessions must not reuse the same owned value")
			seen[id] = true
		}
		require.Len(t, seen, 60)
	})
}

func TestInsertInt8(t *testing.T) {
	fillValues := []int8{1, 2, 3, 4, 5, 6, 7, 8}
	fillRows := []int{0, 1, 2, 3, 4, 5, 6, 7}
	testColumnCacheInsert[int8](
		t,
		8,
		1,
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
		10,
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
		1,
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
		10,
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
		1,
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
		10,
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
		1,
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
		10,
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
		1,
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
		10,
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
		1,
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
		10,
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
		1,
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
		10,
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
		1,
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
		10,
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
		1,
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
					nil,
					AutoIncrementOptions{}, 1))
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
					nil,
					AutoIncrementOptions{}, 1))
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
							nil,
							AutoIncrementOptions{}, batch)
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
	vector.SetFixedAtWithTypeCheck[uint64](input, 0, 1)
	vector.SetFixedAtWithTypeCheck[uint64](input, 1, 5)
	input.GetNulls().Del(0)
	testColumnCacheInsert[uint64](
		t,
		8,
		2,
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
				vector.MustFixedColWithTypeCheck[T](expect),
				vector.MustFixedColWithTypeCheck[T](input))
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
	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			defer leaktest.AfterTest(t)()
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
					cc, err := newColumnCache(ctx, sid, 0, col, Config{CountPerAllocate: capacity}, true, a, nil)
					require.NoError(t, err)
					fn(ctx, cc)
				},
			)
		},
	)

}

// TestOldestAllocateAtFollowsConsumableRange verifies that conflict detection
// starts at the allocation timestamp of the range that can still issue values.
// A newer prefetched range must not hide manual inserts that can conflict with
// values remaining in an older range.
func TestOldestAllocateAtFollowsConsumableRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			col := AutoColumn{
				ColName: "test_col",
				TableID: 100,
				Offset:  0,
				Step:    1,
			}
			cc := &columnCache{
				logger:      getLogger(sid),
				col:         col,
				cfg:         Config{CountPerAllocate: 100},
				ranges:      &ranges{step: 1, values: make([]uint64, 0, 1)},
				committed:   true,
				allocatingC: make(chan error, 1), // Initialize allocatingC channel
			}

			ts1 := timestamp.Timestamp{PhysicalTime: 1000, LogicalTime: 1}
			cc.applyAllocateLocked(1, 4, ts1, nil)
			assert.Equal(t, ts1, cc.ranges.oldestAllocateAt())

			cc.allocatingC = make(chan error, 1)
			ts2 := timestamp.Timestamp{PhysicalTime: 2000, LogicalTime: 2}
			cc.applyAllocateLocked(4, 7, ts2, nil)

			assert.Equal(t, ts1, cc.ranges.oldestAllocateAt(),
				"prefetch must retain the timestamp of the range still issuing values")
			assert.Equal(t, uint64(1), cc.ranges.next())
			assert.Equal(t, uint64(2), cc.ranges.next())
			assert.Equal(t, uint64(3), cc.ranges.next())
			assert.Equal(t, ts2, cc.ranges.oldestAllocateAt(),
				"timestamp should advance after the older range is exhausted")
		},
	)
}

// TestLastAllocateAtEmptyInitial verifies that an empty allocation timestamp
// remains a safe zero timestamp and the first valid allocation is exposed.
func TestLastAllocateAtEmptyInitial(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sid := ""
	runtime.RunTest(
		sid,
		func(rt runtime.Runtime) {
			col := AutoColumn{
				ColName: "test_col",
				TableID: 100,
				Offset:  0,
				Step:    1,
			}
			cc := &columnCache{
				logger:      getLogger(sid),
				col:         col,
				cfg:         Config{CountPerAllocate: 100},
				ranges:      &ranges{step: 1, values: make([]uint64, 0, 1)},
				committed:   true,
				allocatingC: make(chan error, 1), // Initialize allocatingC channel
			}

			// Verify initial state
			assert.True(t, cc.ranges.oldestAllocateAt().IsEmpty(), "Initial allocation timestamp should be empty")

			// First allocation should expose its timestamp.
			ts1 := timestamp.Timestamp{PhysicalTime: 1000, LogicalTime: 1}
			cc.applyAllocateLocked(1, 100, ts1, nil)
			assert.Equal(t, ts1, cc.ranges.oldestAllocateAt())
			assert.False(t, cc.ranges.oldestAllocateAt().IsEmpty())
		},
	)
}

func TestTerminalValueRetainsAllocateTimestamp(t *testing.T) {
	ts := timestamp.Timestamp{PhysicalTime: 1000, LogicalTime: 1}
	cc := &columnCache{
		col:         AutoColumn{Step: 1},
		ranges:      &ranges{step: 1},
		allocatingC: make(chan error, 1),
	}

	cc.applyAllocateLocked(math.MaxUint64, 0, ts, nil)

	require.True(t, cc.terminal)
	require.Equal(t, uint64(math.MaxUint64), cc.terminalValue)
	require.Equal(t, ts, cc.oldestAllocateAtLocked())
}

func TestWrappedAllocationPreservesAllTerminalValues(t *testing.T) {
	cc := &columnCache{
		col:         AutoColumn{Step: 1},
		ranges:      &ranges{step: 1},
		allocatingC: make(chan error, 1),
	}

	cc.applyAllocateLocked(math.MaxUint64-1, 0, timestamp.Timestamp{}, nil)

	require.Equal(t, uint64(math.MaxUint64-1), cc.ranges.next())
	require.True(t, cc.terminal)
	require.Equal(t, uint64(math.MaxUint64), cc.terminalValue)
}
