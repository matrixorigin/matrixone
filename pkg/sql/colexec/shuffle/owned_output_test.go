// Copyright 2026 Matrix Origin
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

package shuffle

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestOwnedCompleteKeyShuffle(t *testing.T) {
	for _, values := range [][]int64{{7, 7, 7}, {0, 1, 2, 3, 4, 5, 6, 7}} {
		proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
		budget, err := proc.GetExecutionResourceBudget()
		require.NoError(t, err)
		registry, err := mpool.NewAllocationAccountRegistry(1, 128)
		require.NoError(t, err)
		account, err := registry.OpenWithController(1<<20, budget)
		require.NoError(t, err)
		arg := NewArgument()
		arg.BucketNum, arg.ShuffleType = 8, int32(plan.ShuffleType_Hash)
		arg.ShuffleKeyColumns, arg.DrainAllBuckets, arg.OwnedOutput = []int32{0}, true, true
		arg.OwnedColumnCount = 1
		require.NoError(t, arg.SetAllocationAccount(account))
		input := batch.NewWithSize(1)
		input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(input.Vecs[0], values, nil, proc.Mp()))
		input.SetRowCount(len(values))
		input.ShuffleIDX = 99
		source := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
		arg.AppendChild(source)
		require.NoError(t, vm.Prepare(arg, proc))
		var got []int64
		for {
			result, err := vm.Exec(arg, proc)
			require.NoError(t, err)
			if result.Batch == nil || result.Status == vm.ExecStop {
				break
			}
			if result.Batch.RowCount() == 0 {
				continue
			}
			require.NotSame(t, input, result.Batch)
			require.True(t, vector.AllocationAccountSelectionsEqual(arg.outputAllocation, result.Batch.AllocationAccountSelection()))
			got = append(got, vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[0])...)
		}
		require.ElementsMatch(t, values, got)
		require.Equal(t, int32(99), input.ShuffleIDX)
		require.Nil(t, input.AllocationAccountSelection())
		arg.Reset(proc, false, nil)
		arg.Free(proc, false, nil)
		require.Zero(t, account.Snapshot().Used)
		require.NoError(t, arg.ClearAllocationAccount(account))
		require.Zero(t, budget.Used())
		account.Seal()
		_, err = registry.Finalize(account)
		require.NoError(t, err)
		arg.Release()
		input.Clean(proc.Mp())
		source.ResetBatchs()
		require.Zero(t, proc.Mp().CurrNB())
		proc.Free()
	}
}

func TestOwnedShuffleSharedPoolLifecycle(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<20, budget)
	require.NoError(t, err)
	pool := NewOwnedShufflePool(8, 8, 1)
	ops := make([]*Shuffle, 8)
	for i := range ops {
		ops[i] = NewArgument()
	}
	var wg sync.WaitGroup
	errs := make([]error, len(ops))
	for i, op := range ops {
		op.BucketNum, op.ShuffleType = 8, int32(plan.ShuffleType_Hash)
		op.ShuffleKeyColumns, op.DrainAllBuckets, op.OwnedOutput = []int32{0}, true, true
		op.OwnedColumnCount = 1
		op.SetShufflePool(pool)
		require.NoError(t, op.SetAllocationAccount(account))
		wg.Add(1)
		go func(i int, op *Shuffle) { defer wg.Done(); errs[i] = op.Prepare(proc) }(i, op)
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	firstUsage := budget.Used()
	require.Greater(t, firstUsage, uint64(64<<10+8*8*4<<10), "shared and per-worker metadata must be charged")
	for _, op := range ops {
		op.Reset(proc, false, nil)
		op.Free(proc, false, nil)
	}
	require.Positive(t, budget.Used(), "spool metadata remains admitted until transport drain")
	for _, op := range ops {
		require.NoError(t, op.ClearAllocationAccount(account))
		op.Release()
	}
	require.Zero(t, budget.Used())
	require.Zero(t, account.Snapshot().Used)
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

func TestOwnedShuffleRejectsTooManyBuckets(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<20, budget)
	require.NoError(t, err)
	arg := NewArgument()
	arg.BucketNum, arg.ShuffleType = MaxOwnedBuckets+1, int32(plan.ShuffleType_Hash)
	arg.ShuffleKeyColumns, arg.DrainAllBuckets, arg.OwnedOutput = []int32{0}, true, true
	arg.OwnedColumnCount = 1
	require.NoError(t, arg.SetAllocationAccount(account))
	require.Error(t, arg.Prepare(proc))
	pool := NewOwnedShufflePool(MaxOwnedBuckets+1, 1, 1)
	require.Error(t, pool.initOwned(proc, arg.outputAllocation))
	require.Zero(t, budget.Used())
	require.NoError(t, arg.ClearAllocationAccount(account))
	arg.Release()
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

func TestOwnedShuffleFailureReleasesReservations(t *testing.T) {
	for _, tc := range []struct {
		name             string
		queryLimit       int64
		accountCap       uint64
		failInit         bool
		exhaustAfterPool bool
	}{
		{"metadata admission", 64 << 10, 1 << 20, true, false},
		{"holder admission", 1 << 20, 1 << 20, true, true},
		{"hash scratch admission", 1 << 20, 64, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			proc.Base.Lim.Size = tc.queryLimit
			budget, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			registry, err := mpool.NewAllocationAccountRegistry(1, 128)
			require.NoError(t, err)
			account, err := registry.OpenWithController(tc.accountCap, budget)
			require.NoError(t, err)
			arg := NewArgument()
			arg.BucketNum, arg.ShuffleType = 8, int32(plan.ShuffleType_Hash)
			arg.ShuffleKeyColumns, arg.DrainAllBuckets, arg.OwnedOutput = []int32{0}, true, true
			arg.OwnedColumnCount = 1
			require.NoError(t, arg.SetAllocationAccount(account))
			var pressure *process.ExecutionTransientMemoryReservation
			if tc.exhaustAfterPool {
				pool := NewOwnedShufflePool(8, 1, 1)
				arg.SetShufflePool(pool)
				require.NoError(t, pool.initOwned(proc, arg.outputAllocation))
				available, err := budget.AvailableMemory()
				require.NoError(t, err)
				require.Greater(t, available, uint64(1024))
				pressure, err = budget.ReserveTransientMemory(available - 1024)
				require.NoError(t, err)
				defer pressure.Release()
			}
			err = arg.Prepare(proc)
			if !tc.failInit {
				require.NoError(t, err)
				input := batch.NewWithSize(1)
				input.Vecs[0] = vector.NewConstNull(types.T_int64.ToType(), 128, proc.Mp())
				input.SetRowCount(128)
				_, err = arg.shuffleCompleteKey(input, proc)
				input.Clean(proc.Mp())
			}
			require.Error(t, err)
			arg.Reset(proc, true, err)
			arg.Free(proc, true, err)
			require.Zero(t, account.Snapshot().Used)
			require.NoError(t, arg.ClearAllocationAccount(account))
			if pressure != nil {
				pressure.Release()
			}
			require.Zero(t, budget.Used())
			account.Seal()
			_, err = registry.Finalize(account)
			require.NoError(t, err)
			arg.Release()
		})
	}
}

func TestOwnedShuffleAbortBeforeInitialization(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	proc.Base.Lim.Size = 1
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<20, budget)
	require.NoError(t, err)
	arg := NewArgument()
	defer arg.Release()
	arg.BucketNum, arg.ShuffleType = 2, int32(plan.ShuffleType_Hash)
	arg.ShuffleKeyColumns, arg.DrainAllBuckets, arg.OwnedOutput = []int32{0}, true, true
	arg.OwnedColumnCount = 1
	require.NoError(t, arg.SetAllocationAccount(account))
	pool := NewOwnedShufflePool(2, 1, 1)
	cause := errors.New("earlier producer failure")
	pool.abortWithError(proc.Mp(), cause)
	arg.SetShufflePool(pool)
	require.ErrorIs(t, arg.Prepare(proc), cause)
	arg.Reset(proc, true, context.Canceled)
	arg.Free(proc, true, context.Canceled)
	require.Zero(t, budget.Used())
	require.NoError(t, arg.ClearAllocationAccount(account))
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

func TestOwnedShufflePreservesFailureDuringAdmission(t *testing.T) {
	for _, tc := range []struct {
		name string
		stop bool
	}{{"producer failure", false}, {"successful stop", true}} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
			entered, resume := make(chan struct{}), make(chan struct{})
			budget.SetAggregateCapProvider(func() (uint64, error) { close(entered); <-resume; return 1, nil })
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			require.NoError(t, proc.SetExecutionResourceBudgetForTesting(generation))
			registry, err := mpool.NewAllocationAccountRegistry(1, 128)
			require.NoError(t, err)
			account, err := registry.OpenWithController(1<<20, generation)
			require.NoError(t, err)
			arg := NewArgument()
			defer arg.Release()
			arg.BucketNum, arg.ShuffleType = 2, int32(plan.ShuffleType_Hash)
			arg.ShuffleKeyColumns, arg.DrainAllBuckets, arg.OwnedOutput = []int32{0}, true, true
			arg.OwnedColumnCount = 1
			pool := NewOwnedShufflePool(2, 1, 1)
			arg.SetShufflePool(pool)
			require.NoError(t, arg.SetAllocationAccount(account))
			child := proc.NewContextChildProc(0)
			done, finished := make(chan error, 1), make(chan struct{})
			var release sync.Once
			defer func() { release.Do(func() { close(resume) }); <-finished }()
			go func() { defer close(finished); done <- arg.Prepare(child) }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("budget provider was not entered")
			}
			var cause error = errors.New("earlier producer failure")
			if tc.stop {
				cause = process.ErrPipelineStopped
				child.Cancel(cause)
			} else {
				pool.abortWithError(proc.Mp(), cause)
			}
			release.Do(func() { close(resume) })
			var got error
			select {
			case got = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("initialization did not return")
			}
			normalized, stopped := process.NormalizePipelineRunError(got, child.Ctx, proc.GetTopContext())
			arg.Reset(child, normalized != nil, normalized)
			arg.Free(child, normalized != nil, normalized)
			require.NoError(t, arg.ClearAllocationAccount(account))
			require.Zero(t, generation.Used())
			account.Seal()
			_, err = registry.Finalize(account)
			require.NoError(t, err)
			require.ErrorIs(t, got, cause)
			if tc.stop {
				require.True(t, stopped)
				require.NoError(t, normalized)
			}
		})
	}
}

func TestOwnedShufflePreservesCauseDuringHolderAdmission(t *testing.T) {
	for _, tc := range []struct {
		name string
		stop bool
	}{
		{"producer failure", false},
		{"successful stop", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			require.NoError(t, proc.SetExecutionResourceBudgetForTesting(generation))
			registry, err := mpool.NewAllocationAccountRegistry(1, 128)
			require.NoError(t, err)
			account, err := registry.OpenWithController(1<<20, generation)
			require.NoError(t, err)
			pool := NewOwnedShufflePool(2, 3, 1)
			started, late, unstarted := NewArgument(), NewArgument(), NewArgument()
			defer started.Release()
			defer late.Release()
			defer unstarted.Release()
			for _, op := range []*Shuffle{started, late, unstarted} {
				op.BucketNum, op.ShuffleType = 2, int32(plan.ShuffleType_Hash)
				op.ShuffleKeyColumns, op.DrainAllBuckets, op.OwnedOutput = []int32{0}, true, true
				op.OwnedColumnCount = 1
				op.SetShufflePool(pool)
				require.NoError(t, op.SetAllocationAccount(account))
			}
			startedProc, lateProc, unstartedProc := proc.NewContextChildProc(0), proc.NewContextChildProc(0), proc.NewContextChildProc(0)
			require.NoError(t, started.Prepare(startedProc))
			entered, resume := make(chan struct{}), make(chan struct{})
			budget.SetAggregateCapProvider(func() (uint64, error) {
				close(entered)
				<-resume
				return 1, nil
			})
			done, finished := make(chan error, 1), make(chan struct{})
			var release sync.Once
			defer func() {
				release.Do(func() { close(resume) })
				<-finished
			}()
			go func() { defer close(finished); done <- late.Prepare(lateProc) }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("holder admission was not entered")
			}
			var expected error = errors.New("earlier producer failure")
			if tc.stop {
				expected = process.ErrPipelineStopped
				lateProc.Cancel(expected)
			} else {
				pool.abortWithError(proc.Mp(), expected)
			}
			pipeline.New(0, nil, unstarted).Cleanup(unstartedProc, false, false, nil)
			release.Do(func() { close(resume) })
			var got error
			select {
			case got = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("holder admission did not finish")
			}
			normalized, stopped := process.NormalizePipelineRunError(got, lateProc.Ctx, proc.GetTopContext())
			late.Reset(lateProc, normalized != nil, normalized)
			late.Free(lateProc, normalized != nil, normalized)
			started.Reset(startedProc, false, nil)
			started.Free(startedProc, false, nil)
			require.NoError(t, late.ClearAllocationAccount(account))
			require.NoError(t, started.ClearAllocationAccount(account))
			require.NoError(t, unstarted.ClearAllocationAccount(account))
			require.Zero(t, generation.Used())
			account.Seal()
			_, err = registry.Finalize(account)
			require.NoError(t, err)
			require.ErrorIs(t, got, expected)
			if tc.stop {
				require.True(t, stopped)
				require.NoError(t, normalized)
			}
		})
	}
}

type pausedOwnedMemoryAdmission struct {
	*ownedMemoryAdmission
	decided chan struct{}
	resume  chan struct{}
}

func (a *pausedOwnedMemoryAdmission) ResolveAdmissionFailure(err error, stopped bool) error {
	result := a.ownedMemoryAdmission.ResolveAdmissionFailure(err, stopped)
	close(a.decided)
	<-a.resume
	return result
}

func TestOwnedShuffleRejectionBeforeStop(t *testing.T) {
	for _, initialized := range []bool{false, true} {
		name := "shared initialization"
		if initialized {
			name = "holder admission"
		}
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
			generation, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			require.NoError(t, proc.SetExecutionResourceBudgetForTesting(generation))
			registry, err := mpool.NewAllocationAccountRegistry(1, 128)
			require.NoError(t, err)
			account, err := registry.OpenWithController(1<<20, generation)
			require.NoError(t, err)
			pool := NewOwnedShufflePool(2, 2, 1)
			started := NewArgument()
			defer started.Release()
			started.BucketNum, started.ShuffleType = 2, int32(plan.ShuffleType_Hash)
			started.ShuffleKeyColumns, started.DrainAllBuckets, started.OwnedOutput = []int32{0}, true, true
			started.OwnedColumnCount = 1
			started.SetShufflePool(pool)
			require.NoError(t, started.SetAllocationAccount(account))
			startedProc, lateProc := proc.NewContextChildProc(0), proc.NewContextChildProc(0)
			if initialized {
				require.NoError(t, started.Prepare(startedProc))
			}
			guard := &pausedOwnedMemoryAdmission{ownedMemoryAdmission: &ownedMemoryAdmission{pool: pool}, decided: make(chan struct{}), resume: make(chan struct{})}
			done, finished := make(chan error, 1), make(chan struct{})
			var release sync.Once
			defer func() {
				release.Do(func() { close(guard.resume) })
				<-finished
			}()
			go func() {
				defer close(finished)
				_, err := generation.ReserveTransientMemoryGuarded(2<<20, lateProc.Ctx.Done(), guard)
				done <- err
			}()
			select {
			case <-guard.decided:
			case <-time.After(5 * time.Second):
				t.Fatal("admission decision was not recorded")
			}
			lateProc.Cancel(process.ErrPipelineStopped)
			release.Do(func() { close(guard.resume) })
			var got error
			select {
			case got = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("admission did not return")
			}
			later := errors.New("later producer failure")
			resolved := started.ResolvePipelineError(later)
			normalized, _ := process.NormalizePipelineRunError(got, lateProc.Ctx, proc.GetTopContext())
			started.Reset(startedProc, true, got)
			started.Free(startedProc, true, got)
			require.NoError(t, started.ClearAllocationAccount(account))
			require.Zero(t, generation.Used())
			account.Seal()
			_, err = registry.Finalize(account)
			require.NoError(t, err)
			require.ErrorIs(t, got, process.ErrExecutionResourceAdmission)
			require.ErrorIs(t, normalized, process.ErrExecutionResourceAdmission)
			require.ErrorIs(t, resolved, process.ErrExecutionResourceAdmission)
		})
	}
}

// The test can expire this deadline at the admission barrier while retaining
// a diagnostic cancellation cause in the underlying context.
type manuallyExpiredDeadline struct{ context.Context }

func (c manuallyExpiredDeadline) Err() error {
	if c.Context.Err() != nil {
		return context.DeadlineExceeded
	}
	return nil
}

func TestOwnedShuffleDeadlineDuringAdmission(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	require.NoError(t, proc.SetExecutionResourceBudgetForTesting(generation))
	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<20, generation)
	require.NoError(t, err)
	arg := NewArgument()
	defer arg.Release()
	arg.BucketNum, arg.ShuffleType = 2, int32(plan.ShuffleType_Hash)
	arg.ShuffleKeyColumns, arg.DrainAllBuckets, arg.OwnedOutput = []int32{0}, true, true
	arg.OwnedColumnCount = 1
	arg.SetShufflePool(NewOwnedShufflePool(2, 1, 1))
	require.NoError(t, arg.SetAllocationAccount(account))
	diagnostic := errors.New("deadline diagnostic")
	causeCtx, expire := context.WithCancelCause(context.Background())
	defer expire(nil)
	ctx := manuallyExpiredDeadline{causeCtx}
	child := proc.NewContextChildProc(0)
	child.Ctx = ctx
	entered, resume := make(chan struct{}), make(chan struct{})
	budget.SetAggregateCapProvider(func() (uint64, error) { close(entered); <-resume; return 1, nil })
	done, finished := make(chan error, 1), make(chan struct{})
	var release sync.Once
	defer func() { release.Do(func() { close(resume) }); <-finished }()
	go func() { defer close(finished); done <- arg.Prepare(child) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("budget provider was not entered")
	}
	expire(diagnostic)
	require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	require.ErrorIs(t, context.Cause(ctx), diagnostic)
	release.Do(func() { close(resume) })
	var got error
	select {
	case got = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("admission did not return")
	}
	normalized, _ := process.NormalizePipelineRunError(got, ctx, ctx)
	arg.Reset(child, true, normalized)
	arg.Free(child, true, normalized)
	require.NoError(t, arg.ClearAllocationAccount(account))
	require.Zero(t, generation.Used())
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)
	require.ErrorIs(t, got, context.DeadlineExceeded)
	require.ErrorIs(t, normalized, context.DeadlineExceeded)
	require.NotErrorIs(t, got, diagnostic)
}

func TestOwnedShuffleUnstartedCloneStopsSiblings(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<20, budget)
	require.NoError(t, err)
	pool := NewOwnedShufflePool(4, 3, 1)
	started, late, unstarted := NewArgument(), NewArgument(), NewArgument()
	defer started.Release()
	defer late.Release()
	defer unstarted.Release()
	for _, op := range []*Shuffle{started, late, unstarted} {
		op.BucketNum, op.ShuffleType = 4, int32(plan.ShuffleType_Hash)
		op.ShuffleKeyColumns, op.DrainAllBuckets, op.OwnedOutput = []int32{0}, true, true
		op.OwnedColumnCount = 1
		op.SetShufflePool(pool)
		require.NoError(t, op.SetAllocationAccount(account))
	}
	startedProc, lateProc, unstartedProc := proc.NewContextChildProc(0), proc.NewContextChildProc(0), proc.NewContextChildProc(0)
	require.NoError(t, started.Prepare(startedProc))
	// An unsubmitted runtime clone receives the normal stopped-pipeline cleanup
	// without ever becoming a pool holder.
	lateProc.Cancel(process.ErrPipelineStopped)
	pipeline.New(0, nil, unstarted).Cleanup(unstartedProc, false, false, nil)
	require.True(t, pool.allStop())
	// A sibling can reach Prepare after the shared pool is stopped. Preserve
	// the consumer's stop cause so scope cleanup treats it as a successful stop.
	lateErr := late.Prepare(lateProc)
	normalized, stopped := process.NormalizePipelineRunError(lateErr, lateProc.Ctx, proc.GetTopContext())
	late.Reset(lateProc, true, lateErr)
	late.Free(lateProc, true, lateErr)
	started.Reset(startedProc, false, nil)
	started.Free(startedProc, false, nil)
	require.Zero(t, account.Snapshot().Used)
	require.NoError(t, started.ClearAllocationAccount(account))
	require.NoError(t, late.ClearAllocationAccount(account))
	require.NoError(t, unstarted.ClearAllocationAccount(account))
	require.Zero(t, budget.Used())
	account.Seal()
	_, err = registry.Finalize(account)
	require.NoError(t, err)
	require.ErrorIs(t, lateErr, process.ErrPipelineStopped)
	require.True(t, stopped)
	require.NoError(t, normalized)
}

func TestOwnedShuffleSelectionAndOutputFailure(t *testing.T) {
	for _, failure := range []string{"selection", "output"} {
		t.Run(failure, func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			budget, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			registry, err := mpool.NewAllocationAccountRegistry(1, 128)
			require.NoError(t, err)
			account, err := registry.OpenWithController(1<<20, budget)
			require.NoError(t, err)
			arg := NewArgument()
			defer arg.Release()
			arg.BucketNum, arg.ShuffleType = 4, int32(plan.ShuffleType_Hash)
			arg.ShuffleKeyColumns, arg.DrainAllBuckets, arg.OwnedOutput = []int32{0}, true, true
			arg.OwnedColumnCount = 1
			require.NoError(t, arg.SetAllocationAccount(account))
			require.NoError(t, arg.Prepare(proc))
			arg.ctr.keyHashes, err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_uint64.ToType(), arg.outputAllocation)
			require.NoError(t, err)
			require.NoError(t, arg.ctr.keyHashes.PreExtend(128, proc.Mp()))
			if failure == "output" {
				arg.ctr.selectionBacking, err = mpool.MakeSliceAccounted[int32](128, proc.Mp(), account, mpool.AllocationOwnerSet, 13)
				require.NoError(t, err)
			}
			input := batch.NewWithSize(1)
			input.Vecs[0] = vector.NewConstNull(types.T_int64.ToType(), 128, proc.Mp())
			input.SetRowCount(128)
			input.ShuffleIDX = 99
			account.Seal() // the next physical allocation must fail deterministically
			_, err = arg.shuffleCompleteKey(input, proc)
			require.ErrorIs(t, err, mpool.ErrAllocationAccountSealed)
			require.Equal(t, int32(99), input.ShuffleIDX)
			arg.Reset(proc, true, err)
			arg.Free(proc, true, err)
			input.Clean(proc.Mp())
			require.Zero(t, account.Snapshot().Used)
			require.NoError(t, arg.ClearAllocationAccount(account))
			require.Zero(t, budget.Used())
			_, err = registry.Finalize(account)
			require.NoError(t, err)
		})
	}
}
