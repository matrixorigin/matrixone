// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package process

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func newCTEMemoryTestProcess() *Process {
	return NewTopProcess(context.Background(), mpool.MustNewZero(), nil, nil, nil, nil, nil, nil, nil, nil, nil)
}

func TestCTEMemoryBudgetResolutionAndStatementSnapshot(t *testing.T) {
	tests := []struct {
		name     string
		resolver func(string, bool, bool) (interface{}, error)
		want     uint64
	}{
		{name: "missing", want: DefaultCTEMemoryQuotaBytes},
		{name: "session", resolver: func(string, bool, bool) (interface{}, error) { return int64(4096), nil }, want: 4096},
		{name: "disabled", resolver: func(string, bool, bool) (interface{}, error) { return int64(0), nil }, want: 0},
		{name: "resolver error", resolver: func(string, bool, bool) (interface{}, error) { return nil, errors.New("resolver failed") }, want: DefaultCTEMemoryQuotaBytes},
		{name: "wrong type", resolver: func(string, bool, bool) (interface{}, error) { return "4096", nil }, want: DefaultCTEMemoryQuotaBytes},
		{name: "negative", resolver: func(string, bool, bool) (interface{}, error) { return int64(-1), nil }, want: DefaultCTEMemoryQuotaBytes},
		{name: "above maximum", resolver: func(string, bool, bool) (interface{}, error) { return int64(MaximumCTEMemoryQuotaBytes + 1), nil }, want: DefaultCTEMemoryQuotaBytes},
		{name: "maximum", resolver: func(string, bool, bool) (interface{}, error) { return int64(MaximumCTEMemoryQuotaBytes), nil }, want: MaximumCTEMemoryQuotaBytes},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := newCTEMemoryTestProcess()
			proc.SetResolveVariableFunc(test.resolver)
			budget := proc.GetCTEMemoryBudget()
			limit, used, closed := budget.Snapshot()
			require.Equal(t, test.want, limit)
			require.Zero(t, used)
			require.False(t, closed)
			proc.Free()
		})
	}

	proc := newCTEMemoryTestProcess()
	configured := int64(2048)
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return configured, nil })
	first := proc.GetCTEMemoryBudget()
	configured = 1024
	require.Same(t, first, proc.GetCTEMemoryBudget())
	limit, _, _ := first.Snapshot()
	require.Equal(t, uint64(2048), limit)

	proc.SetStmtProfile(&StmtProfile{})
	_, _, closed := first.Snapshot()
	require.True(t, closed)
	second := proc.GetCTEMemoryBudget()
	require.NotSame(t, first, second)
	limit, _, _ = second.Snapshot()
	require.Equal(t, uint64(1024), limit)
	proc.Free()
}

func TestCTEMemoryBudgetSharedReservations(t *testing.T) {
	proc := newCTEMemoryTestProcess()
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return int64(100), nil })
	child := proc.NewNoContextChildProc(0)
	require.Same(t, proc.GetCTEMemoryBudget(), child.GetCTEMemoryBudget())

	first, err := proc.GetCTEMemoryBudget().Reserve(context.Background(), 60)
	require.NoError(t, err)
	second, err := child.GetCTEMemoryBudget().Reserve(context.Background(), 40)
	require.NoError(t, err)
	_, err = child.GetCTEMemoryBudget().Reserve(context.Background(), 1)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrCteMemoryQuotaExceeded))
	require.Equal(t, "recursive CTE memory quota exceeded on this CN: projected 101 bytes, query limit 100 bytes; increase @@cte_max_memory_bytes or rewrite the query to converge", err.Error())

	require.NoError(t, first.Resize(context.Background(), 20))
	third, err := proc.GetCTEMemoryBudget().Reserve(context.Background(), 40)
	require.NoError(t, err)
	third.Release()
	third.Release()
	second.Release()
	first.Release()
	_, used, _ := proc.GetCTEMemoryBudget().Snapshot()
	require.Zero(t, used)
	proc.Free()
}

func TestCTEMemoryBudgetConcurrentCloseAndOverflow(t *testing.T) {
	budget := NewCTEMemoryBudget(1000)
	reservations := make([]*CTEMemoryReservation, 10)
	errs := make([]error, len(reservations))
	var wg sync.WaitGroup
	for i := range reservations {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reservations[i], errs[i] = budget.Reserve(context.Background(), 100)
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	_, used, _ := budget.Snapshot()
	require.Equal(t, uint64(1000), used)
	for _, reservation := range reservations {
		reservation.Release()
	}
	_, used, _ = budget.Snapshot()
	require.Zero(t, used)

	unlimited := NewCTEMemoryBudget(0)
	reservation, err := unlimited.Reserve(context.Background(), math.MaxUint64)
	require.NoError(t, err)
	_, err = unlimited.Reserve(context.Background(), 1)
	require.ErrorIs(t, err, ErrCTEMemoryBudgetInvalid)
	unlimited.Close()
	require.ErrorIs(t, reservation.Resize(context.Background(), 0), ErrCTEMemoryBudgetClosed)
	reservation.Release()
	reservation.Release()
	_, used, closed := unlimited.Snapshot()
	require.Zero(t, used)
	require.True(t, closed)

	proc := newCTEMemoryTestProcess()
	statementBudget := proc.GetCTEMemoryBudget()
	statementReservation, err := statementBudget.Reserve(context.Background(), 1)
	require.NoError(t, err)
	proc.Free()
	_, used, closed = statementBudget.Snapshot()
	require.Zero(t, used)
	require.True(t, closed)
	require.ErrorIs(t, statementReservation.Resize(context.Background(), 0), ErrCTEMemoryBudgetClosed)
	statementReservation.Release()
}
