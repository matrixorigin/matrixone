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
	"fmt"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func newLocalFilterTestProcess(t *testing.T, cap uint64) (*process.Process, *process.ExecutionResourceGeneration) {
	t.Helper()
	proc := testutil.NewProcess(t)
	proc.SetMessageBoard(message.NewMessageBoard())
	t.Cleanup(proc.Free)
	budget := process.MustNewExecutionResourceBudget(cap, cap)
	g, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	t.Cleanup(g.Close)
	require.NoError(t, proc.SetExecutionResourceBudgetForTesting(g))
	return proc, g
}

func TestLocalShuffleFilterCompleteDomain(t *testing.T) {
	proc, g := newLocalFilterTestProcess(t, 16<<20)
	f, _ := NewLocalShuffleFilter(71, types.T_int64, 1000, 2)
	t.Cleanup(f.Close)
	vec := testutil.MakeInt64Vector([]int64{1, 10001}, nil, proc.Mp())
	t.Cleanup(func() { vec.Free(proc.Mp()) })
	first, err := f.Begin(proc, 0, 2)
	require.NoError(t, err)
	first.AddVector(vec)
	require.NoError(t, first.Complete(false))
	require.False(t, f.Ready(), "a partial domain cannot filter a streaming probe")
	select {
	case <-f.done:
		t.Fatal("a partial build must not seal membership")
	default:
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = f.Wait(canceled, nil)
	require.ErrorIs(t, err, context.Canceled)
	second, err := f.Begin(proc, 1, 2)
	require.NoError(t, err)
	second.AddVector(vec)
	require.NoError(t, second.Complete(false))
	require.True(t, f.Ready())
	require.NoError(t, first.Complete(true), "completion retry cannot alter the sealed filter")
	bloom, err := f.Wait(context.Background(), nil)
	require.NoError(t, err)
	require.NotNil(t, bloom)
	output := make([]uint8, 2)
	require.True(t, bloom.TestFixedVectorInto(vec, output))
	require.Equal(t, []uint8{1, 1}, output)
	f.Close()
	f.Close()
	require.Zero(t, g.Snapshot().Used)
}

func TestLocalShuffleFilterFallbackAndPartialPrepare(t *testing.T) {
	for _, scenario := range []string{"empty", "spill", "capacity", "partial", "closed-generation"} {
		t.Run(scenario, func(t *testing.T) {
			cap := uint64(16 << 20)
			if scenario == "capacity" {
				cap = 1
			}
			proc, g := newLocalFilterTestProcess(t, cap)
			f, _ := NewLocalShuffleFilter(72, types.T_int64, 1000, 2)
			t.Cleanup(f.Close)
			if scenario == "closed-generation" {
				g.Close()
				_, err := f.Begin(proc, 0, 2)
				require.ErrorIs(t, err, process.ErrExecutionResourceClosed)
				f.Close()
				require.Zero(t, g.Snapshot().Used)
				return
			}
			first, err := f.Begin(proc, 0, 2)
			require.NoError(t, err)
			if scenario == "partial" {
				f.Close()
				_, err = f.Wait(context.Background(), nil)
				require.NoError(t, err)
				require.Zero(t, g.Snapshot().Used)
				return
			}
			second, err := f.Begin(proc, 1, 2)
			require.NoError(t, err)
			require.NoError(t, first.Complete(false))
			require.NoError(t, second.Complete(scenario == "spill"))
			bloom, err := f.Wait(context.Background(), nil)
			require.NoError(t, err)
			receiver := message.NewMessageReceiver([]int32{72}, message.AddrBroadCastOnCurrentCN(), proc.GetMessageBoard())
			msgs, _, err := receiver.ReceiveMessage(false, context.Background())
			require.NoError(t, err)
			require.Len(t, msgs, 1)
			if scenario == "empty" {
				require.NotNil(t, bloom)
				require.Equal(t, int32(message.RuntimeFilter_DROP), msgs[0].(message.RuntimeFilterMessage).Typ)
			} else {
				require.Nil(t, bloom)
				require.Equal(t, int32(message.RuntimeFilter_PASS), msgs[0].(message.RuntimeFilterMessage).Typ)
			}
			f.Close()
			require.Zero(t, g.Snapshot().Used)
		})
	}
}

func TestLocalShuffleFilterConcurrentCompletion(t *testing.T) {
	proc, g := newLocalFilterTestProcess(t, 16<<20)
	f, _ := NewLocalShuffleFilter(73, types.T_int64, 1000, 8)
	t.Cleanup(f.Close)
	vec := testutil.MakeInt64Vector([]int64{1, 200, 30000}, nil, proc.Mp())
	t.Cleanup(func() { vec.Free(proc.Mp()) })
	var wg sync.WaitGroup
	errors := make(chan error, 8)
	for id := range 8 {
		p, err := f.Begin(proc, id, 8)
		require.NoError(t, err)
		p.AddVector(vec)
		wg.Add(1)
		go func() { defer wg.Done(); errors <- p.Complete(false) }()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	bloom, err := f.Wait(context.Background(), nil)
	require.NoError(t, err)
	output := make([]uint8, 3)
	require.True(t, bloom.TestFixedVectorInto(vec, output))
	require.Equal(t, []uint8{1, 1, 1}, output)
	f.Close()
	require.Zero(t, g.Snapshot().Used)
}

func TestLocalShuffleFilterStoppedConsumer(t *testing.T) {
	f, _ := NewLocalShuffleFilter(74, types.T_int64, 1000, 2)
	t.Cleanup(f.Close)
	stopped := make(chan struct{})
	close(stopped)
	_, err := f.Wait(context.Background(), stopped)
	require.ErrorIs(t, err, context.Canceled)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f.Close()
	_, err = f.Wait(ctx, stopped)
	require.ErrorIs(t, err, context.Canceled, "cancellation wins over a closed coordinator")
}

func TestLocalShuffleFilterIndependentProbeFanout(t *testing.T) {
	for _, probeCount := range []int{1, 8} {
		t.Run(fmt.Sprintf("probe=%d", probeCount), func(t *testing.T) {
			proc, g := newLocalFilterTestProcess(t, 16<<20)
			f, _ := NewLocalShuffleFilter(75, types.T_int64, 1000, 8)
			t.Cleanup(f.Close)
			enabled, err := f.PrepareProbe(proc, probeCount)
			require.NoError(t, err)
			require.True(t, enabled)
			require.Zero(t, f.expected, "probe admission cannot seal build fanout")
			for id := range 8 {
				producer, err := f.Begin(proc, id, 8)
				require.NoError(t, err)
				require.NoError(t, producer.Complete(false))
				if id != 7 {
					select {
					case <-f.done:
						t.Fatal("incomplete build domain")
					default:
					}
				}
			}
			_, err = f.Wait(proc.Ctx, nil)
			require.NoError(t, err)
			f.Close()
			require.Zero(t, g.Snapshot().Used)
		})
	}
}
