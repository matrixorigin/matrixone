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

package runtimefilter

import (
	"context"
	"math"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func localDomainTestProcess(t *testing.T, capacity uint64) (*process.Process, *process.ExecutionResourceGeneration) {
	t.Helper()
	proc := testutil.NewProcess(t)
	proc.SetMessageBoard(message.NewMessageBoard())
	t.Cleanup(proc.Free)
	budget := process.MustNewExecutionResourceBudget(capacity, capacity)
	g, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	t.Cleanup(g.Close)
	require.NoError(t, proc.SetExecutionResourceBudgetForTesting(g))
	return proc, g
}

func TestLocalDomainCompleteAndIndependentFanout(t *testing.T) {
	proc, g := localDomainTestProcess(t, 16<<20)
	d, peak := NewLocalDomain(types.T_int64, 1000, 2, 8, 1000000)
	require.NotNil(t, d)
	t.Cleanup(d.Close)
	d.seed = 1
	require.NoError(t, d.PrepareProbe(proc))
	keys := testutil.MakeInt64Vector([]int64{1, 10001}, nil, proc.Mp())
	t.Cleanup(func() { keys.Free(proc.Mp()) })
	first, err := d.Begin(proc, 0, 2)
	require.NoError(t, err)
	require.NoError(t, first.AddVector(proc.Ctx, keys, 2))
	first.Complete(false)
	select {
	case <-d.done:
		t.Fatal("partial source domain cannot be published")
	default:
	}
	second, err := d.Begin(proc, 1, 2)
	require.NoError(t, err)
	require.NoError(t, second.AddVector(proc.Ctx, keys, 2))
	second.Complete(false)
	bloom, empty, err := d.Wait(proc.Ctx)
	require.NoError(t, err)
	require.False(t, empty)
	mask := make([]uint8, 2)
	require.True(t, bloom.TestFixedVectorInto(keys, mask))
	require.Equal(t, []uint8{1, 1}, mask)
	require.Equal(t, peak, g.Snapshot().Used)
	first.Complete(true)
	d.Close()
	d.Close()
	require.Zero(t, g.Snapshot().Used)
}

func TestLocalDomainFallbackAndCancellation(t *testing.T) {
	for _, name := range []string{"empty", "budget", "partial", "cancel", "closed-generation"} {
		t.Run(name, func(t *testing.T) {
			capacity := uint64(16 << 20)
			if name == "budget" {
				capacity = 1
			}
			proc, g := localDomainTestProcess(t, capacity)
			d, _ := NewLocalDomain(types.T_int64, 1000, 2, 4, 1000000)
			t.Cleanup(d.Close)
			if name == "closed-generation" {
				g.Close()
				_, err := d.Begin(proc, 0, 2)
				require.ErrorIs(t, err, process.ErrExecutionResourceClosed)
				_, empty, waitErr := d.Wait(proc.Ctx)
				require.ErrorIs(t, waitErr, process.ErrExecutionResourceClosed)
				require.False(t, empty)
				return
			}
			first, err := d.Begin(proc, 0, 2)
			require.NoError(t, err)
			if name == "cancel" {
				ctx, cancel := context.WithCancel(proc.Ctx)
				cancel()
				_, _, err := d.Wait(ctx)
				require.ErrorIs(t, err, context.Canceled)
				return
			}
			first.Complete(name == "partial")
			if name == "empty" {
				second, err := d.Begin(proc, 1, 2)
				require.NoError(t, err)
				second.Complete(false)
			}
			bloom, empty, err := d.Wait(proc.Ctx)
			require.NoError(t, err)
			if name == "empty" {
				require.NotNil(t, bloom)
				require.True(t, empty)
			} else {
				require.Nil(t, bloom)
				require.False(t, empty, "fallback must never prove an empty domain")
			}
			d.Close()
			require.Zero(t, g.Snapshot().Used)
		})
	}
}

func TestLocalDomainSourceIdentity(t *testing.T) {
	proc, _ := localDomainTestProcess(t, 16<<20)
	d, _ := NewLocalDomain(types.T_uint64, 1000, 2, 1, 1000000)
	t.Cleanup(d.Close)
	first, err := d.Begin(proc, 0, 2)
	require.NoError(t, err)
	_, err = d.Begin(proc, 0, 2)
	require.Error(t, err, "duplicate identity cannot count twice toward completion")
	_, err = d.Begin(proc, 1, 1)
	require.Error(t, err, "source fanout must agree")
	first.Complete(true)
	_, empty, err := d.Wait(proc.Ctx)
	require.NoError(t, err)
	require.False(t, empty)
}

func TestLocalDomainConcurrentSources(t *testing.T) {
	proc, g := localDomainTestProcess(t, 16<<20)
	d, _ := NewLocalDomain(types.T_int64, 1000, 8, 16, 1000000)
	t.Cleanup(d.Close)
	keys := testutil.MakeInt64Vector([]int64{1, 10001}, nil, proc.Mp())
	t.Cleanup(func() { keys.Free(proc.Mp()) })
	var wg sync.WaitGroup
	errors := make(chan error, 8)
	for id := range 8 {
		wg.Go(func() {
			p, err := d.Begin(proc, id, 8)
			if err == nil {
				err = p.AddVector(proc.Ctx, keys, 2)
				p.Complete(err != nil)
			}
			errors <- err
		})
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	bloom, empty, err := d.Wait(proc.Ctx)
	require.NoError(t, err)
	require.False(t, empty)
	mask := make([]uint8, 2)
	require.True(t, bloom.TestFixedVectorInto(keys, mask))
	require.Equal(t, []uint8{1, 1}, mask)
	rows, _, _ := d.Stats()
	require.Equal(t, int64(16), rows)
	d.Close()
	require.Zero(t, g.Snapshot().Used)
}

func TestLocalDomainCompletedCardinalityAdmission(t *testing.T) {
	proc, _ := localDomainTestProcess(t, 16<<20)
	// The estimate fails the 10% ratio. Actual completed keys may still qualify.
	d, _ := NewLocalDomain(types.T_int64, 20, 1, 2, 101)
	require.NotNil(t, d)
	t.Cleanup(d.Close)
	keys := testutil.MakeInt64Vector([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil, proc.Mp())
	t.Cleanup(func() { keys.Free(proc.Mp()) })
	source, err := d.Begin(proc, 0, 1)
	require.NoError(t, err)
	require.NoError(t, source.AddVector(proc.Ctx, keys, 10))
	require.False(t, d.ProbeEligible(101), "partial membership cannot authorize rejection")
	source.Complete(false)
	bloom, empty, err := d.Wait(proc.Ctx)
	require.NoError(t, err)
	require.NotNil(t, bloom)
	require.False(t, empty)
	require.True(t, d.ProbeEligible(101))
	for _, ndv := range []float64{100, 1, 0, math.NaN(), math.Inf(1)} {
		require.False(t, d.ProbeEligible(ndv), "each probe keeps the strict known 10%% bound")
	}
	snapshot := d.Snapshot()
	require.Equal(t, int64(20), snapshot.EstimatedRows)
	require.Equal(t, int64(10), snapshot.ObservedRows)
	require.Equal(t, 1, snapshot.Finished)
}

func TestLocalDomainUnprofitableSourcePassesEarly(t *testing.T) {
	proc, _ := localDomainTestProcess(t, 16<<20)
	d, _ := NewLocalDomain(types.T_int64, 20, 2, 2, 100)
	t.Cleanup(d.Close)
	keys := testutil.MakeInt64Vector([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil, proc.Mp())
	t.Cleanup(func() { keys.Free(proc.Mp()) })
	source, err := d.Begin(proc, 0, 2)
	require.NoError(t, err)
	require.NoError(t, source.AddVector(proc.Ctx, keys, 10))
	// The second producer has not even started; PASS needs no absence proof.
	bloom, empty, err := d.Wait(proc.Ctx)
	require.NoError(t, err)
	require.Nil(t, bloom)
	require.False(t, empty)
	require.Equal(t, LocalDomainUnprofitable, d.Snapshot().PassReason)
	require.False(t, d.ProbeEligible(100))
	second, err := d.Begin(proc, 1, 2)
	require.NoError(t, err)
	require.NoError(t, second.AddVector(proc.Ctx, keys, 10), "normal source execution continues")
	source.Complete(false)
	second.Complete(false)
}
