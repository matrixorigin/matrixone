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

package disttae

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestWorkspaceAllocationIndependentClose(t *testing.T) {
	proc := testutil.NewProcess(t)
	budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
	parent, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	require.NoError(t, proc.SetExecutionResourceBudgetForTesting(parent))
	defer parent.Close()
	txn := &Transaction{proc: proc}
	clone := txn.CloneSnapshotWS().(*Transaction)
	v, err := txn.newWorkspaceVector(types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(v, int64(42), false, proc.Mp()))
	w, err := clone.newWorkspaceVector(types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(w, int64(7), false, proc.Mp()))
	v.Free(proc.Mp())
	txn.closeWorkspaceAllocation()
	txn.closeWorkspaceAllocation()
	require.False(t, parent.Closed())
	require.NoError(t, vector.AppendFixed(w, int64(8), false, proc.Mp()))
	_, err = txn.workspaceAllocation()
	require.ErrorIs(t, err, mpool.ErrAllocationAccountSealed)
	w.Free(proc.Mp())
	clone.closeWorkspaceAllocation()
	require.Zero(t, budget.AggregateUsed())
}

func TestWorkspaceAllocationConcurrentInitialization(t *testing.T) {
	proc := testutil.NewProcess(t)
	budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
	parent, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	require.NoError(t, proc.SetExecutionResourceBudgetForTesting(parent))
	defer parent.Close()
	txn := &Transaction{proc: proc}
	defer txn.closeWorkspaceAllocation()
	const workers = 16
	selections := make([]*vector.AllocationAccountSelection, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range workers {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; selections[i], errs[i] = txn.workspaceAllocation() }()
	}
	close(start)
	wg.Wait()
	for i := range workers {
		require.NoError(t, errs[i])
		require.Same(t, selections[0], selections[i])
	}
	require.Zero(t, budget.AggregateUsed())
}
