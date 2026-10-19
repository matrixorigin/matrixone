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
	"math"
	"slices"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
)

func TestUnusedReservationRanges(t *testing.T) {
	for _, tc := range []struct {
		name                                  string
		values                                []uint64
		step, increment, offset, count, limit uint64
		want                                  []uint64
		remaining                             uint64
		saturated                             bool
	}{
		{"zero", []uint64{2, 1000}, 1, 1, 1, 0, 127, []uint64{2, 1000}, 0, false},
		{"partial", []uint64{2, 1000}, 1, 1, 1, 4, 127, []uint64{6, 1000}, 0, false},
		{"tinyint", []uint64{2, 1000}, 1, 1, 1, 130, 127, []uint64{127, 1000}, 5, true},
		{"series_at_limit", []uint64{2, 1000}, 1, 3, 1, 50, 127, []uint64{127, 1000}, 9, true},
		{"series_past_limit", []uint64{3, 1000}, 1, 3, 2, 50, 127, []uint64{127, 1000}, 9, true},
		{"disjoint", []uint64{2, 5, 20, 100}, 1, 1, 1, 5, 127, []uint64{22, 100}, 0, false},
		{"depleted", []uint64{2, 5}, 1, 1, 1, 8, 127, nil, 5, false},
		{"boundary_not_owned", []uint64{125, 127}, 1, 1, 1, 8, 127, []uint64{127, 127}, 6, true},
		{"past_boundary", []uint64{150, 200}, 1, 1, 1, 8, 127, []uint64{150, 200}, 8, true},
		{"no_residue", []uint64{2, 4, 8, 12}, 2, 2, 1, 3, 127, nil, 3, false},
		{"legacy_boundary", []uint64{1, 29}, 4, 1, 1, 30, 23, []uint64{25, 29}, 24, true},
		{"legacy_step", []uint64{1, 19}, 2, 3, 1, 2, 127, []uint64{13, 19}, 0, false},
		{"overflowing_period", []uint64{2, 100}, 3, math.MaxInt64, 2, 1, 127, nil, 0, false},
		{"uint64", []uint64{math.MaxUint64 - 3, math.MaxUint64}, 1, 1, 1, math.MaxUint64, math.MaxUint64, []uint64{math.MaxUint64, math.MaxUint64}, math.MaxUint64 - 3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &ranges{step: tc.step, values: slices.Clone(tc.values)}
			remaining, saturated := r.consumeUnused(tc.count, tc.limit, NormalizeAutoIncrementOptions(tc.increment, tc.offset))
			require.True(t, slices.Equal(tc.want, r.values), "want %v, got %v", tc.want, r.values)
			require.Equal(t, tc.remaining, remaining)
			require.Equal(t, tc.saturated, saturated)
		})
	}
}

func TestUnusedReservationRangesAgainstEnumeration(t *testing.T) {
	// This oracle enumerates owned values, independent of the run arithmetic.
	for step := uint64(1); step <= 4; step++ {
		for inc := uint64(1); inc <= 7; inc++ {
			for offset := uint64(1); offset <= inc; offset++ {
				for count := uint64(0); count <= 15; count++ {
					r := &ranges{step: step, values: []uint64{1, 1 + 8*step, 1 + 12*step, 1 + 20*step}}
					const limit = 23
					var owned []uint64
					for i := 0; i < len(r.values); i += 2 {
						for v := r.values[i]; v < r.values[i+1]; v += step {
							if v < limit && v >= offset && (v-offset)%inc == 0 {
								owned = append(owned, v)
							}
						}
					}
					remaining, _ := r.consumeUnused(count, limit, NormalizeAutoIncrementOptions(inc, offset))
					wantConsumed := min(count, uint64(len(owned)))
					require.Equal(t, count-wantConsumed, remaining)
					var after []uint64
					for {
						v := r.nextFor(NormalizeAutoIncrementOptions(inc, offset))
						if v == 0 || v >= limit {
							break
						}
						after = append(after, v)
					}
					require.True(t, slices.Equal(owned[wantConsumed:], after), "step=%d series=%d/%d count=%d", step, inc, offset, count)
				}
			}
		}
	}
}

type unusedReservationAllocator struct {
	valueAllocator
	err   error
	calls int
}

func (a *unusedReservationAllocator) allocate(context.Context, uint64, string, int, client.TxnOperator) (uint64, uint64, timestamp.Timestamp, error) {
	a.calls++
	return 0, 0, timestamp.Timestamp{}, a.err
}

func TestStatementReservationErrors(t *testing.T) {
	for _, name := range []string{"cancelled", "retired", "invalid_step", "allocator", "no_progress", "huge_increment", "overflow", "terminal"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			a := &unusedReservationAllocator{}
			c := &columnCache{col: AutoColumn{Step: 1}, ranges: &ranges{step: 1}, allocator: a, logger: getLogger("")}
			switch name {
			case "cancelled":
				cancel()
			case "retired":
				c.retired = true
			case "invalid_step":
				c.col.Step = 0
			case "allocator":
				a.err = moerr.NewInternalErrorNoCtx("allocator failure")
			case "huge_increment":
				ctx = WithAutoIncrementOptions(ctx, math.MaxUint64, 1)
			case "overflow":
				c.overflow = true
			case "terminal":
				c.terminal = true
				c.terminalValue = math.MaxUint64
			}
			err := c.reserveAutoValues(ctx, 0, &AutoIncrementReservation{Count: 3}, math.MaxUint64, nil)
			require.Error(t, err)
			if name == "cancelled" {
				require.ErrorIs(t, err, context.Canceled)
			}
			if name == "allocator" {
				require.ErrorIs(t, err, a.err)
			}
			if name == "no_progress" {
				require.Equal(t, maxAutoIncrementAllocationsPerRow, a.calls)
			}
			if name == "terminal" {
				require.Equal(t, uint64(math.MaxUint64), c.terminalValue)
				require.True(t, c.terminal)
			}
		})
	}
}

type unusedReservationWaitContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *unusedReservationWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func TestStatementReservationWaitLifecycle(t *testing.T) {
	for _, mode := range []string{"cancel", "retire", "allocation_error"} {
		t.Run(mode, func(t *testing.T) {
			base, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &unusedReservationWaitContext{Context: base, entered: make(chan struct{})}
			c := &columnCache{
				col: AutoColumn{Step: 1}, ranges: &ranges{step: 1},
				allocating: true, allocatingC: make(chan error, 1),
			}
			done := make(chan error, 1)
			go func() { done <- c.reserveAutoValues(ctx, 0, &AutoIncrementReservation{Count: 10}, 127, nil) }()
			// Done is observed only after the previous-allocation wait releases
			// the cache lock. Trigger the lifecycle event at that exact phase.
			<-ctx.entered
			var allocationErr error
			switch mode {
			case "cancel":
				cancel()
			case "retire":
				c.retire()
				c.applyAllocate(1, 100, timestamp.Timestamp{}, nil)
			case "allocation_error":
				allocationErr = moerr.NewInternalErrorNoCtx("prior allocation failed")
				c.applyAllocate(0, 0, timestamp.Timestamp{}, allocationErr)
			}
			err := <-done
			switch mode {
			case "cancel":
				require.ErrorIs(t, err, context.Canceled)
				c.applyAllocate(1, 100, timestamp.Timestamp{}, nil)
			case "retire":
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged))
			case "allocation_error":
				require.ErrorIs(t, err, allocationErr)
			}
			c.Lock()
			defer c.Unlock()
			if mode != "allocation_error" {
				require.Equal(t, []uint64{1, 100}, c.ranges.values, "a failed wait must not consume values")
			}
		})
	}
}

func TestStatementReservationPartialFailure(t *testing.T) {
	a := &unusedReservationAllocator{err: moerr.NewInternalErrorNoCtx("later allocation failed")}
	c := &columnCache{
		col: AutoColumn{Step: 1}, ranges: &ranges{step: 1}, allocator: a, logger: getLogger(""),
	}
	c.ranges.addWithTimestamp(1, 3, timestamp.Timestamp{PhysicalTime: 1})
	c.ranges.addWithTimestamp(10, 12, timestamp.Timestamp{PhysicalTime: 2})
	first := &AutoIncrementReservation{Count: 2}
	require.NoError(t, c.reserveAutoValues(context.Background(), 0, first, 127, nil))
	require.Equal(t, timestamp.Timestamp{PhysicalTime: 1}, first.Runs[0].AllocatedAt)
	require.Equal(t, timestamp.Timestamp{PhysicalTime: 2}, c.oldestAllocateAtLocked())
	second := &AutoIncrementReservation{Count: 3}
	require.ErrorIs(t, c.reserveAutoValues(context.Background(), 0, second, 127, nil), a.err)
	require.Equal(t, []AutoIncrementRun{{First: 10, Count: 2, Step: 1, AllocatedAt: timestamp.Timestamp{PhysicalTime: 2}}}, second.Runs)
	require.True(t, c.ranges.empty(), "failure must not restore already consumed ranges")
	require.False(t, c.allocating)
}

func TestStatementReservationTypedTableBoundary(t *testing.T) {
	for _, tc := range []struct {
		typ   types.Type
		limit uint64
	}{
		{types.T_int8.ToType(), math.MaxInt8}, {types.T_uint8.ToType(), math.MaxUint8},
		{types.T_int16.ToType(), math.MaxInt16}, {types.T_uint16.ToType(), math.MaxUint16},
		{types.T_int32.ToType(), math.MaxInt32}, {types.T_uint32.ToType(), math.MaxUint32},
		{types.New(types.T_int32, 24, -1), 1<<23 - 1}, {types.New(types.T_uint32, 24, -1), 1<<24 - 1},
		{types.T_int64.ToType(), math.MaxInt64}, {types.T_uint64.ToType(), math.MaxUint64},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			c := &tableCache{cols: []AutoColumn{{ColName: "id", ColIndex: 2, Step: 1}}}
			col := &columnCache{col: c.cols[0], ranges: &ranges{step: 1, values: []uint64{tc.limit - 3, tc.limit}}}
			c.mu.cols = map[string]*columnCache{"id": col}
			ctx := WithAutoIncrementReservation(context.Background(), &AutoIncrementReservation{ColumnIndex: 2, Type: tc.typ, Count: 10})
			id, err := c.insertAutoValues(ctx, 42, nil, 0, 0)
			require.NoError(t, err)
			require.Zero(t, id, "reservation must not publish LAST_INSERT_ID")
			require.True(t, col.ranges.empty() || col.ranges.values[0] == tc.limit)
		})
	}
	for _, mode := range []string{"type", "negative_column", "unknown_column", "rows", "estimate", "vectors"} {
		t.Run(mode, func(t *testing.T) {
			request := AutoIncrementReservation{Type: types.T_int8.ToType(), Count: 1}
			rows, estimate := 0, int64(0)
			var vectors []*vector.Vector
			switch mode {
			case "type":
				request.Type = types.T_bool.ToType()
			case "negative_column":
				request.ColumnIndex = -1
			case "rows":
				rows = 1
			case "estimate":
				estimate = 1
			case "vectors":
				vectors = []*vector.Vector{nil}
			}
			ctx := WithAutoIncrementReservation(context.Background(), &request)
			_, err := (&tableCache{}).insertAutoValues(ctx, 42, vectors, rows, estimate)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		})
	}
}
