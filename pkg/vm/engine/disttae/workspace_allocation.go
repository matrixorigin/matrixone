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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

func (txn *Transaction) workspaceAllocation() (*vector.AllocationAccountSelection, error) {
	return txn.workspaceAllocationWithLimit(0)
}

func (txn *Transaction) workspaceAllocationWithLimit(limit uint64) (*vector.AllocationAccountSelection, error) {
	txn.workspaceAllocationMu.Lock()
	defer txn.workspaceAllocationMu.Unlock()
	if txn.workspaceAllocationClosed {
		return nil, mpool.ErrAllocationAccountSealed
	}
	if txn.workspaceSelection != nil {
		return txn.workspaceSelection, nil
	}
	parent, err := txn.proc.GetExecutionResourceBudget()
	if err != nil {
		return nil, err
	}
	generation, err := parent.OpenIndependentGeneration()
	if err != nil {
		return nil, err
	}
	registry, err := generation.AllocationAccountRegistry()
	if err != nil {
		generation.Close()
		return nil, err
	}
	if limit == 0 || limit > generation.Cap() {
		limit = generation.Cap()
	}
	account, err := registry.OpenWithController(limit, generation)
	if err != nil {
		generation.Close()
		return nil, err
	}
	selection, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerTxnWorkspace, 1, 2, 3, 4)
	if err != nil {
		_, _, _ = registry.CompleteTerminal(account)
		generation.Close()
		return nil, err
	}
	txn.workspaceGeneration, txn.workspaceAccount, txn.workspaceSelection = generation, account, selection
	return selection, nil
}

func (txn *Transaction) copyWorkspaceBatch(src *batch.Batch) (*batch.Batch, error) {
	selection, err := txn.workspaceAllocation()
	if err != nil {
		return nil, err
	}
	dst, err := util.CopyBatchWithAllocation(src, txn.proc, selection)
	if err != nil {
		return nil, err
	}
	dst.Recursive, dst.ShuffleIDX = src.Recursive, src.ShuffleIDX
	for i, vec := range src.Vecs {
		dst.Vecs[i].SetSorted(vec.GetSorted())
	}
	return dst, nil
}

func (txn *Transaction) newWorkspaceVector(typ types.Type) (*vector.Vector, error) {
	selection, err := txn.workspaceAllocation()
	if err != nil {
		return nil, err
	}
	v := vector.NewOffHeapVecWithType(typ)
	if err := v.SetAllocationAccount(selection); err != nil {
		return nil, err
	}
	return v, nil
}

// Called after all workspace batches have been cleaned, under txn's terminal
// quiescence contract. Free remains valid after the independent generation closes.
func (txn *Transaction) closeWorkspaceAllocation() {
	txn.workspaceAllocationMu.Lock()
	defer txn.workspaceAllocationMu.Unlock()
	if txn.workspaceAllocationClosed {
		return
	}
	txn.workspaceAllocationClosed = true
	if txn.workspaceGeneration == nil {
		return
	}
	registry, err := txn.workspaceGeneration.AllocationAccountRegistry()
	if err == nil {
		_, _, err = registry.CompleteTerminal(txn.workspaceAccount)
	}
	txn.workspaceGeneration.Close()
	if err != nil {
		logutil.Errorf("transaction workspace allocation terminal: %v", err)
	}
}

// The keyed injection narrows the real account; it does not manufacture an error.
func (txn *Transaction) copyWorkspaceBatchForTable(src *batch.Batch, database string) (*batch.Batch, error) {
	selection, err := txn.workspaceAllocationWithLimit(objectio.CNWorkspaceAllocationLimit(database))
	if err != nil {
		return nil, err
	}
	return util.CopyBatchWithAllocation(src, txn.proc, selection)
}
