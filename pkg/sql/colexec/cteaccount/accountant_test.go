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

package cteaccount

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func newAccountantTestProcess(t *testing.T, limit int64) *process.Process {
	t.Helper()
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return limit, nil })
	return proc
}

func makeAccountantBatch(proc *process.Process, value string) *batch.Batch {
	bat := batch.New([]string{"value"})
	bat.Vecs[0] = testutil.MakeVarcharVector([]string{value}, nil, proc.Mp())
	bat.SetRowCount(1)
	return bat
}

func TestAccountantAdmissionRollbackCommitAndRelease(t *testing.T) {
	proc := newAccountantTestProcess(t, 1<<20)
	src := makeAccountantBatch(proc, "recursive payload")
	defer src.Clean(proc.Mp())

	var accountant Accountant
	require.NoError(t, accountant.Bind(proc, nil))
	txn, err := accountant.BeginReplacement(proc.Ctx, nil, src)
	require.NoError(t, err)
	_, used, _ := proc.GetCTEMemoryBudget().Snapshot()
	require.Equal(t, uint64(src.Size()), used)
	txn.Rollback()
	_, used, _ = proc.GetCTEMemoryBudget().Snapshot()
	require.Zero(t, used)

	txn, err = accountant.BeginReplacement(proc.Ctx, nil, src)
	require.NoError(t, err)
	cached, err := src.Dup(proc.Mp())
	require.NoError(t, err)
	require.NoError(t, txn.Commit(cached))
	require.Equal(t, uint64(cached.Allocated()), accountant.Retained())
	_, used, _ = proc.GetCTEMemoryBudget().Snapshot()
	require.Equal(t, accountant.Retained(), used)

	accountant.Release()
	accountant.Release()
	_, used, _ = proc.GetCTEMemoryBudget().Snapshot()
	require.Zero(t, used)
	cached.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountantReplacementAdmissionKeepsOldSlotCharged(t *testing.T) {
	proc := newAccountantTestProcess(t, 1<<20)
	old := makeAccountantBatch(proc, "old retained payload")
	src := makeAccountantBatch(proc, "new replacement payload")
	limit := int64(old.Allocated() + src.Size() - 1)
	proc.SetResolveVariableFunc(func(string, bool, bool) (interface{}, error) { return limit, nil })
	defer old.Clean(proc.Mp())
	defer src.Clean(proc.Mp())

	var accountant Accountant
	require.NoError(t, accountant.Bind(proc, []*batch.Batch{old}))
	_, err := accountant.BeginReplacement(proc.Ctx, old, src)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrCteMemoryQuotaExceeded))
	_, used, closed := proc.GetCTEMemoryBudget().Snapshot()
	require.False(t, closed)
	require.Equal(t, uint64(old.Allocated()), used)
	accountant.Release()
}

func TestAccountantPreCopyAndReconcileRejection(t *testing.T) {
	proc := newAccountantTestProcess(t, 32)
	large := makeAccountantBatch(proc, "a logical payload larger than the configured limit")
	defer large.Clean(proc.Mp())
	var accountant Accountant
	require.NoError(t, accountant.Bind(proc, nil))
	_, err := accountant.BeginReplacement(proc.Ctx, nil, large)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrCteMemoryQuotaExceeded))
	_, used, _ := proc.GetCTEMemoryBudget().Snapshot()
	require.Zero(t, used)
	accountant.Release()
	proc.Free()

	proc = newAccountantTestProcess(t, 512)
	small := makeAccountantBatch(proc, "x")
	defer small.Clean(proc.Mp())
	var reconcile Accountant
	require.NoError(t, reconcile.Bind(proc, nil))
	txn, err := reconcile.BeginReplacement(proc.Ctx, nil, small)
	require.NoError(t, err)
	oversized, err := small.Dup(proc.Mp())
	require.NoError(t, err)
	require.NoError(t, oversized.Vecs[0].PreExtendWithArea(1, 4096, proc.Mp()))
	require.Greater(t, oversized.Allocated(), 512)
	err = txn.Commit(oversized)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrCteMemoryQuotaExceeded))
	oversized.Clean(proc.Mp())
	txn.Discard()
	require.Zero(t, reconcile.Retained())
	_, used, _ = proc.GetCTEMemoryBudget().Snapshot()
	require.Zero(t, used)
	reconcile.Release()
	proc.Free()
}

func TestAccountantMarkerReleasesOldSlotAndOwnersShareBudget(t *testing.T) {
	proc := newAccountantTestProcess(t, 1<<20)
	firstCache := makeAccountantBatch(proc, "first")
	secondCache := makeAccountantBatch(proc, "second")
	var first, second Accountant
	require.NoError(t, first.Bind(proc, []*batch.Batch{firstCache}))
	require.NoError(t, second.Bind(proc, []*batch.Batch{secondCache}))
	want := uint64(firstCache.Allocated() + secondCache.Allocated())
	_, used, _ := proc.GetCTEMemoryBudget().Snapshot()
	require.Equal(t, want, used)

	marker := makeAccountantBatch(proc, "marker")
	marker.SetLast()
	txn, err := first.BeginReplacement(proc.Ctx, firstCache, marker)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(marker))
	require.Zero(t, first.Retained())
	_, used, _ = proc.GetCTEMemoryBudget().Snapshot()
	require.Equal(t, uint64(secondCache.Allocated()), used)

	first.Release()
	second.Release()
	firstCache.Clean(proc.Mp())
	secondCache.Clean(proc.Mp())
	marker.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountantDiscardAfterMutation(t *testing.T) {
	proc := newAccountantTestProcess(t, 1<<20)
	old := makeAccountantBatch(proc, "old")
	additional := makeAccountantBatch(proc, "additional")
	var accountant Accountant
	require.NoError(t, accountant.Bind(proc, []*batch.Batch{old, additional}))
	txn, err := accountant.BeginReplacement(context.Background(), old, old)
	require.NoError(t, err)
	old.Clean(proc.Mp())
	txn.Discard()
	require.Equal(t, uint64(additional.Allocated()), accountant.Retained())
	accountant.Release()
	additional.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}
