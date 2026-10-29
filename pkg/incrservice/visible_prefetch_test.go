// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package incrservice

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
)

func TestVisibleAutoIDPrefetchCurrentDemand(t *testing.T) {
	for _, committed := range []bool{false, true} {
		for _, tc := range []struct {
			name                  string
			rows, automatic, left int
			request, tailRequest  int
		}{
			{name: "empty", left: 0, tailRequest: 10000},
			{name: "explicit_only", rows: 3, left: 0, tailRequest: 10000},
			{name: "one", rows: 1, automatic: 1, left: 0, request: 10000},
			{name: "two", rows: 2, automatic: 2, left: 0, request: 500000},
			{name: "one_in_large_explicit_batch", rows: 20000, automatic: 1, left: 0, request: 10000},
			{name: "two_with_inventory", rows: 2, automatic: 2, left: 6000},
			{name: "one_exhausts_inventory", rows: 1, automatic: 1, left: 1, tailRequest: 10000},
			{name: "two_exhaust_inventory", rows: 2, automatic: 2, left: 2, tailRequest: 500002},
		} {
			t.Run(fmt.Sprintf("committed=%t/%s", committed, tc.name), func(t *testing.T) {
				runtime.RunTest("", func(runtime.Runtime) {
					ctx := defines.AttachAccountId(t.Context(), catalog.System_Account)
					store := &autoIDCacheStore{IncrValueStore: NewMemStore()}
					column := AutoColumn{ColName: "id", Step: 1}
					require.NoError(t, store.Create(ctx, 42, []AutoColumn{column}, nil))
					cfg := Config{}
					cfg.adjust()
					a := newValueAllocator("", store)
					defer a.close()
					cc, err := newColumnCache(ctx, "", 42, column, cfg, committed, a, nil)
					require.NoError(t, err)
					cc.consumedAutoRows = 500000
					cc.ranges.updateTo(uint64(10001 - tc.left))
					mp := internalPrefetchTestMP(t)
					v := vector.NewVec(types.T_uint64.ToType())
					defer v.Free(mp)
					values := make([]uint64, tc.rows)
					// Manual IDs lie below every remaining candidate and cannot inflate demand.
					for i := range values {
						values[i] = 7
					}
					require.NoError(t, vector.AppendFixedList(v, values, nil, mp))
					v.GetNulls().AddRange(0, uint64(tc.automatic))
					first, err := cc.insertAutoValues(ctx, 42, v, tc.rows, nil)
					require.NoError(t, err)
					cc.Lock()
					err = cc.waitPrevAllocatingLocked(ctx)
					cc.Unlock()
					require.NoError(t, err)
					want := []int{10000} // Cold reservation stays fixed even with a large learned history later.
					if tc.request > 0 {
						want = append(want, tc.request)
					}
					if committed && tc.tailRequest > 0 {
						want = append(want, tc.tailRequest)
					}
					require.Equal(t, want, store.requests())
					require.Equal(t, 500000+tc.automatic, cc.consumedAutoRows)
					if tc.automatic == 0 {
						require.Zero(t, first)
					} else {
						require.Equal(t, uint64(10001-tc.left), first)
						got := vector.MustFixedColWithTypeCheck[uint64](v)
						for i := range tc.automatic {
							require.Equal(t, first+uint64(i), got[i])
						}
					}
				})
			})
		}
	}
}

type prefetchUpdateErrorStore struct {
	IncrValueStore
	err error
}

func (s *prefetchUpdateErrorStore) UpdateMinValue(context.Context, uint64, string, uint64, client.TxnOperator) error {
	return s.err
}

func TestVisibleAutoIDPrefetchFailureDemand(t *testing.T) {
	for _, phase := range []string{"manual_update", "partial_apply", "reserved_before_apply"} {
		t.Run(phase, func(t *testing.T) {
			runtime.RunTest("", func(runtime.Runtime) {
				ctx := defines.AttachAccountId(t.Context(), catalog.System_Account)
				counted := &autoIDCacheStore{IncrValueStore: NewMemStore()}
				column := AutoColumn{ColName: "id", Step: 1, Offset: 126}
				require.NoError(t, counted.Create(ctx, 42, []AutoColumn{column}, nil))
				rejected := errors.New("manual update rejected")
				var store IncrValueStore = counted
				if phase == "manual_update" {
					store = &prefetchUpdateErrorStore{IncrValueStore: counted, err: rejected}
				}
				a := newValueAllocator("", store)
				defer a.close()
				cfg := Config{}
				cfg.adjust()
				cc, err := newColumnCache(ctx, "", 42, column, cfg, true, a, nil)
				require.NoError(t, err)
				cc.consumedAutoRows = 500000
				cc.ranges = &ranges{step: 1}
				if phase == "partial_apply" {
					cc.ranges.add(127, 129)
				}
				mp := internalPrefetchTestMP(t)
				if phase == "manual_update" {
					v := vector.NewVec(types.T_uint64.ToType())
					defer v.Free(mp)
					require.NoError(t, vector.AppendFixedList(v, []uint64{1000000, 0, 0}, nil, mp))
					v.GetNulls().AddRange(1, 3)
					_, err = cc.insertAutoValues(ctx, 42, v, 3, nil)
					require.ErrorIs(t, err, rejected)
				} else {
					v := vector.NewVec(types.T_int8.ToType())
					defer v.Free(mp)
					require.NoError(t, vector.AppendFixedList(v, []int8{0, 0}, nil, mp))
					v.GetNulls().AddRange(0, 2)
					_, err = cc.insertAutoValues(ctx, 42, v, 2, nil)
					require.Error(t, err)
					if phase == "partial_apply" {
						require.Equal(t, int8(127), vector.MustFixedColWithTypeCheck[int8](v)[0])
					}
				}
				cc.Lock()
				err = cc.waitPrevAllocatingLocked(ctx)
				cc.Unlock()
				require.NoError(t, err)
				require.Equal(t, 500000, cc.consumedAutoRows, "failed assignments never add learned demand")
				want := []int{10000, 10000} // Failure defer may retain the original fixed policy.
				if phase == "reserved_before_apply" {
					want = []int{10000, 500000}
				}
				require.Equal(t, want, counted.requests(), "only real pre-apply multi-row demand may use history on a failing call")
			})
		})
	}
}

type pendingPrefetch struct {
	count int
	apply func(uint64, uint64, timestamp.Timestamp, error)
}
type heldPrefetchAllocator struct {
	valueAllocator
	requests chan pendingPrefetch
}

func (a *heldPrefetchAllocator) asyncAllocate(_ context.Context, _ uint64, _ string, count int, _ client.TxnOperator, apply func(uint64, uint64, timestamp.Timestamp, error)) error {
	a.requests <- pendingPrefetch{count: count, apply: apply}
	return nil
}

func TestVisibleAutoIDPrefetchCancelAndRetire(t *testing.T) {
	ctx, cancel := context.WithCancel(defines.AttachAccountId(t.Context(), catalog.System_Account))
	defer cancel()
	cfg := Config{}
	cfg.adjust()
	a := &heldPrefetchAllocator{requests: make(chan pendingPrefetch, 2)}
	cc := &columnCache{col: AutoColumn{ColName: "id", Step: 1}, cfg: cfg, ranges: &ranges{step: 1}, allocator: a, committed: true, consumedAutoRows: 500000}
	cc.preAllocate(ctx, 42, 2, nil, true)
	pending := <-a.requests // Request admitted; callback deliberately remains blocked.
	require.Equal(t, 500000, pending.count)
	defer func() {
		if pending.apply != nil {
			pending.apply(0, 0, timestamp.Timestamp{}, context.Canceled)
		}
	}()
	mp := internalPrefetchTestMP(t)
	v := vector.NewVec(types.T_uint64.ToType())
	defer v.Free(mp)
	require.NoError(t, vector.AppendFixedList(v, []uint64{0, 0}, nil, mp))
	v.GetNulls().AddRange(0, 2)
	cancel()
	_, err := cc.insertAutoValues(ctx, 42, v, 2, nil)
	require.ErrorIs(t, err, context.Canceled)
	cc.retire()
	done := make(chan struct{})
	callback := pending.apply
	pending.apply = nil
	go func() { defer close(done); callback(1, 500001, timestamp.Timestamp{PhysicalTime: 100}, nil) }()
	waitAllocatorBorrowPhase(t, done)
	require.Equal(t, 500000, cc.consumedAutoRows)
	require.False(t, cc.allocating)
	require.Equal(t, uint64(1), cc.ranges.current())
	cc.preAllocate(t.Context(), 42, 600000, nil, true)
	select {
	case <-a.requests:
		t.Fatal("cancelled waiter or retired owner scheduled another allocation")
	default:
	}
}

func TestVisibleAutoIDPrefetchSaturatesAndCountsOnlyAutomaticRows(t *testing.T) {
	runtime.RunTest("", func(runtime.Runtime) {
		cfg := Config{}
		cfg.adjust()
		cc := &columnCache{col: AutoColumn{ColName: "id", Step: 1}, cfg: cfg, ranges: &ranges{step: 1}, consumedAutoRows: 5}
		cc.ranges.add(1, 10001)
		mp := internalPrefetchTestMP(t)
		v := vector.NewVec(types.T_uint64.ToType())
		defer v.Free(mp)
		require.NoError(t, vector.AppendFixedList(v, []uint64{0, 5, 0}, nil, mp))
		v.GetNulls().Add(0, 2)
		_, err := cc.insertAutoValues(t.Context(), 42, v, 3, nil)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 5, 6}, vector.MustFixedColWithTypeCheck[uint64](v))
		require.Equal(t, 7, cc.consumedAutoRows, "only the two automatic rows contribute")
		cc.consumedAutoRows = maxAutoIDPrefetch - 1
		v.GetNulls().AddRange(0, 3)
		_, err = cc.insertAutoValues(t.Context(), 42, v, 3, nil)
		require.NoError(t, err)
		require.Equal(t, maxAutoIDPrefetch, cc.consumedAutoRows)
	})
}
