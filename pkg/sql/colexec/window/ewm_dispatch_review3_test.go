// Copyright 2021 - 2026 MatrixOne
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package window

// The eighth pre-PR review round's dispatch regressions: the peer
// directory is sized WITHOUT allocating, the sizing scan honors the
// bounded in-loop cancellation interval like every other staging
// sweep, and the exact-capacity directory is admitted through the
// statement budget BEFORE its construction - a capacity rejection can
// leave no partition-sized backing behind, and an admitted directory's
// backing equals the span bytes the charge accounts for.

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// noEwmCancel is the always-live staging cancellation hook.
func noEwmCancel() error { return nil }

// wireStagingDirect builds one Window argument and its aggregate state
// wired for a direct stagePartition call: one partition of `rows`
// distinct-key rows with one peer-group start per row (the largest
// possible directory), values all 7.
func wireStagingDirect(t *testing.T, rows int) (*Window, *process.Process, *ewmAggState) {
	t.Helper()
	proc, mp := ewmReviewProc(t)
	keys := make([]int64, rows)
	values := make([]float64, rows)
	for i := range keys {
		keys[i] = int64(i)
		values[i] = 7
	}
	keyVec := ewmInt64Vec(t, mp, keys...)
	xVec := ewmFloatVec(t, mp, values...)
	os := make([]int64, rows)
	for i := range os {
		os[i] = int64(i)
	}
	arg := ewmReviewWire(t, proc, "ewmmean",
		aggexec.EwmConfig{Mode: aggexec.EwmModeRow, Alpha: 0.5, Adjust: false},
		[][]*vector.Vector{{keyVec}}, xVec, nil, rows, []int64{0}, os)
	st := &ewmAggState{}
	arg.ctr.ewmAggs = []*ewmAggState{st}
	require.NoError(t, st.loadConfig(arg.Aggs[0]))
	return arg, proc, st
}

// openTightBudget wires the statement's execution-resource generation
// and allocation account of the existing admission test's shape.
func openTightBudget(t *testing.T, arg *Window, proc *process.Process, cap uint64) (
	*mpool.AllocationAccount, *process.ExecutionResourceGeneration,
) {
	t.Helper()
	budget := process.MustNewExecutionResourceBudget(cap, cap)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	require.NoError(t, proc.SetExecutionResourceBudgetForTesting(generation))
	registry, rerr := mpool.NewAllocationAccountRegistry(1, 8)
	require.NoError(t, rerr)
	account, aerr := registry.Open(1 << 28)
	require.NoError(t, aerr)
	require.NoError(t, arg.SetAllocationAccount(account))
	t.Cleanup(func() {
		require.NoError(t, arg.ClearAllocationAccount(account))
		generation.Close()
		budget.Close()
	})
	return account, generation
}

// TestEwmDispatchPeerDirectoryAdmission pins the admission ordering of
// the peer-group directory: it must not exist before the statement
// budget admits it (a capacity rejection therefore leaves no
// partition-sized Go backing allocated), the sizing walk honors the
// bounded in-loop cancellation at the fixed 1024-step interval, and a
// fitting budget admits the directory whose backing capacity is
// exactly the charged span bytes.
func TestEwmDispatchPeerDirectoryAdmission(t *testing.T) {
	const rows = 8192

	t.Run("a capacity rejection precedes the directory allocation", func(t *testing.T) {
		arg, proc, st := wireStagingDirect(t, rows)
		_, generation := openTightBudget(t, arg, proc, 1)

		err := st.stagePartition(&arg.ctr, proc, 0, rows, 0, 0, noEwmCancel)
		require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity,
			"staging beyond the statement budget must surface the capacity error")
		require.Nil(t, st.spans,
			"the peer directory must not be allocated before the budget admits it")
		require.Nil(t, st.rows, "no staging allocation of a rejected partition")
		require.Zero(t, st.charge, "the rejected attempt holds no charge")
		require.Zero(t, generation.Used(),
			"the rejected attempt leaves the generation at zero")
	})

	t.Run("the sizing scan honors the bounded cancellation", func(t *testing.T) {
		arg, proc, st := wireStagingDirect(t, rows)
		openTightBudget(t, arg, proc, 1<<24)

		// both deterministic flips must surface the cause with the same
		// invariant: with 'done == 1' the walk stops inside its own
		// interval checks, and with 'done == 8' every interval check of
		// the partition's sizing walk stays live, so the flip lands in
		// the post-admission construction walk - on the pre-fix code
		// that same flip was observed only after the unpolled estimate
		// scan and the two partition-sized allocations. The flip check
		// must be the last cancellation invocation (bounded work): no
		// directory, no staged rows, no charge, and a clean later run
		// publishes the canonical profile.
		for _, done := range []int{1, 8} {
			calls := 0
			cancel := func() error {
				calls++
				if calls > done {
					return context.Canceled
				}
				return nil
			}
			err := st.stagePartition(&arg.ctr, proc, 0, rows, 0, 0, cancel)
			require.ErrorIs(t, err, context.Canceled,
				"a cancellation after %d live sizing checks must surface the cause", done)
			require.Nil(t, st.spans,
				"a canceled sizing (flip after %d live checks) leaves no directory allocated", done)
			require.Nil(t, st.rows,
				"a canceled sizing (flip after %d live checks) stages no rows", done)
			require.Zero(t, st.charge,
				"a canceled sizing (flip after %d live checks) holds no charge", done)
			require.Equal(t, done+1, calls,
				"the flip check must be the last cancellation invocation (flip after %d)", done)
		}

		// clean reuse after the failed stagings: a fresh generation over
		// the canonical fixture publishes the complete profile
		proc2, mp2 := ewmReviewProc(t)
		keys2 := ewmInt64Vec(t, mp2, 1, 2, 3, 4)
		x2 := ewmFloatVec(t, mp2, 1, 2, 3, 4)
		arg2 := ewmReviewWire(t, proc2, "ewmmean",
			aggexec.EwmConfig{Mode: aggexec.EwmModeRow, Alpha: 0.5, Adjust: false},
			[][]*vector.Vector{{keys2}}, x2, nil, 4, []int64{0}, []int64{0, 1, 2, 3})
		require.Equal(t, []float64{1, 1.5, 2.25, 3.125},
			ewmRunRange(t, arg2, proc2, 0, 4),
			"a generation after the canceled stagings publishes complete values")
	})

	t.Run("an admitted directory matches its charged backing", func(t *testing.T) {
		arg, proc, st := wireStagingDirect(t, rows)
		_, generation := openTightBudget(t, arg, proc, 1<<24)

		require.NoError(t, st.stagePartition(&arg.ctr, proc, 0, rows, 0, 0, noEwmCancel))
		require.NotNil(t, st.spans)
		require.Equal(t, rows, len(st.spans),
			"one peer-group start per row plus the partition start")
		require.Equal(t, rows, cap(st.spans),
			"the directory's backing capacity is its exact charged span count")
		require.NotNil(t, st.eval, "staging built the evaluator")
		require.GreaterOrEqual(t, st.charge, uint64(rows)*8,
			"the charge covers the directory's backing bytes at the admitted estimate")
		arg.ctr.freeEwmState()
		require.Zero(t, st.charge, "the teardown releases the charge")
		require.Zero(t, generation.Used(),
			"freeEwmState returns the generation to zero")
	})
}
