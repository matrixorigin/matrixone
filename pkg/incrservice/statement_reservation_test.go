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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestStatementReservation(t *testing.T) {
	for _, capacity := range []int{1, 4, 1000} {
		runColumnCacheTests(t, capacity, 1, func(ctx context.Context, col *columnCache) {
			request := AutoIncrementReservation{Type: types.T_int8.ToType(), Count: 131}
			require.NoError(t, col.reserveAutoValues(ctx, 0, &request, 127, nil))
			require.Len(t, request.Runs, 1, "contiguous cache leases must be coalesced")
			var got []uint64
			for _, run := range request.Runs {
				for i := uint64(0); i < run.Count; i++ {
					got = append(got, run.First+i*run.Step)
				}
			}
			require.Len(t, got, 126)
			for i, v := range got {
				require.Equal(t, uint64(i+1), v)
			}
			next := AutoIncrementReservation{Type: types.T_int8.ToType(), Count: 1}
			require.NoError(t, col.reserveAutoValues(ctx, 0, &next, 127, nil))
			require.Equal(t, uint64(127), next.Runs[0].First)
			require.Equal(t, uint64(1), next.Runs[0].Count)
			exhausted := AutoIncrementReservation{Type: types.T_int8.ToType(), Count: 1}
			require.Error(t, col.reserveAutoValues(ctx, 0, &exhausted, 127, nil))
		})
	}
}

func TestStatementReservationSessionSeries(t *testing.T) {
	for increment := uint64(1); increment <= 7; increment++ {
		for offset := uint64(1); offset <= increment; offset++ {
			col := &columnCache{col: AutoColumn{Step: 1}, ranges: &ranges{step: 1, values: []uint64{1, 200}}}
			ctx := WithAutoIncrementOptions(context.Background(), increment, offset)
			request := AutoIncrementReservation{Type: types.T_int8.ToType(), Count: 1000}
			require.NoError(t, col.reserveAutoValues(ctx, 0, &request, 127, nil))
			var values []uint64
			for _, run := range request.Runs {
				for i := uint64(0); i < run.Count; i++ {
					values = append(values, run.First+i*run.Step)
				}
			}
			var want []uint64
			for value := offset; value < 127; value += increment {
				want = append(want, value)
			}
			require.Equal(t, want, values)
			next := AutoIncrementReservation{Type: types.T_int8.ToType(), Count: 1}
			err := col.reserveAutoValues(ctx, 0, &next, 127, nil)
			if (127-offset)%increment == 0 {
				require.NoError(t, err, "increment=%d offset=%d", increment, offset)
				require.Equal(t, uint64(127), next.Runs[0].First)
			} else {
				require.Error(t, err)
			}
		}
	}
}

func TestStatementReservationConcurrentOwnership(t *testing.T) {
	col := &columnCache{col: AutoColumn{Step: 1}, ranges: &ranges{step: 1, values: []uint64{1, 200}}}
	start := make(chan struct{})
	done := make(chan error, 2)
	requests := []AutoIncrementReservation{{Count: 40}, {Count: 40}}
	for i := range requests {
		go func() { <-start; done <- col.reserveAutoValues(context.Background(), 0, &requests[i], 127, nil) }()
	}
	close(start)
	for range requests {
		require.NoError(t, <-done)
	}
	seen := make(map[uint64]bool)
	for _, request := range requests {
		for _, run := range request.Runs {
			for i := uint64(0); i < run.Count; i++ {
				value := run.First + i*run.Step
				require.False(t, seen[value], "duplicate ownership of %d", value)
				seen[value] = true
			}
		}
	}
	require.Len(t, seen, 80)
	require.Equal(t, uint64(81), col.ranges.next())
}
