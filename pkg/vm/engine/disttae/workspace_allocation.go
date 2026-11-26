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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
)

// workspaceAllocation returns the accounted view of the transaction's pool that
// charges workspace-owned memory to the workspace account. Workspace batches
// carry no account: every allocation that grows them goes through this view.
func (txn *Transaction) workspaceAllocation() (*mpool.MPool, error) {
	return txn.workspaceAllocationWithLimit(0)
}

// workspaceMP is the workspace's pool: every txn.writes batch, staged copy
// and workspace rowid vector is created with it and operated through it. It
// is the workspace view once it exists. Batches are copied or generated
// through workspaceAllocation before entering the workspace.
func (txn *Transaction) workspaceMP() *mpool.MPool {
	txn.workspaceAllocationMu.Lock()
	defer txn.workspaceAllocationMu.Unlock()
	if txn.workspaceView != nil {
		return txn.workspaceView
	}
	return txn.proc.Mp()
}

func (txn *Transaction) workspaceAllocationWithLimit(limit uint64) (*mpool.MPool, error) {
	txn.workspaceAllocationMu.Lock()
	defer txn.workspaceAllocationMu.Unlock()
	if txn.workspaceAllocationClosed {
		return nil, mpool.ErrAllocationAccountSealed
	}
	if txn.workspaceView != nil {
		return txn.workspaceView, nil
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
	view, err := txn.proc.Mp().WithAccount(account, mpool.AllocationOwnerTxnWorkspace, 1, mpool.AllocationCapacityClassDefault)
	if err != nil {
		_, _, _ = registry.CompleteTerminal(account)
		generation.Close()
		return nil, err
	}
	txn.workspaceGeneration, txn.workspaceAccount, txn.workspaceView = generation, account, view
	return view, nil
}

// copyWorkspaceBatch copies src into a workspace-owned batch charged to the
// workspace account. The copy is remembered so that an INSERT of it adopts
// its generated rowids instead of copying them.
func (txn *Transaction) copyWorkspaceBatch(src *batch.Batch) (*batch.Batch, error) {
	view, err := txn.workspaceAllocation()
	if err != nil {
		return nil, err
	}
	dst, err := util.CopyBatchWithAllocation(src, txn.proc, view)
	if err != nil {
		return nil, err
	}
	txn.rememberWorkspaceCopy(dst)
	dst.Recursive, dst.ShuffleIDX = src.Recursive, src.ShuffleIDX
	for i, vec := range src.Vecs {
		dst.Vecs[i].SetSorted(vec.GetSorted())
	}
	return dst, nil
}

// newWorkspaceVector returns an empty workspace-owned vector and the view its
// allocations must go through.
func (txn *Transaction) newWorkspaceVector(typ types.Type) (*vector.Vector, *mpool.MPool, error) {
	view, err := txn.workspaceAllocation()
	if err != nil {
		return nil, nil, err
	}
	return vector.NewVec(typ, view), view, nil
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
	txn.workspaceCopies = nil
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
	view, err := txn.workspaceAllocationWithLimit(objectio.CNWorkspaceAllocationLimit(database))
	if err != nil {
		return nil, err
	}
	dst, err := util.CopyBatchWithAllocation(src, txn.proc, view)
	if err != nil {
		return nil, err
	}
	txn.rememberWorkspaceCopy(dst)
	return dst, nil
}

// Batches carry no allocation account, so the workspace remembers which
// batches it generated or copied itself. A remembered batch is workspace-owned: INSERT
// adopts its workspace-charged rowids. A raw caller batch is not, and gets an
// independent rowid copy.
func (txn *Transaction) rememberWorkspaceCopy(bat *batch.Batch) {
	txn.workspaceAllocationMu.Lock()
	defer txn.workspaceAllocationMu.Unlock()
	if txn.workspaceCopies == nil {
		txn.workspaceCopies = make(map[*batch.Batch]struct{})
	}
	txn.workspaceCopies[bat] = struct{}{}
}

// isWorkspaceCopy reports whether bat is a workspace copy, keeping it
// remembered.
func (txn *Transaction) isWorkspaceCopy(bat *batch.Batch) bool {
	txn.workspaceAllocationMu.Lock()
	defer txn.workspaceAllocationMu.Unlock()
	_, ok := txn.workspaceCopies[bat]
	return ok
}

// takeWorkspaceCopy reports whether bat is a workspace copy and forgets it.
func (txn *Transaction) takeWorkspaceCopy(bat *batch.Batch) bool {
	txn.workspaceAllocationMu.Lock()
	defer txn.workspaceAllocationMu.Unlock()
	_, ok := txn.workspaceCopies[bat]
	delete(txn.workspaceCopies, bat)
	return ok
}

// adoptCatalogInsert consumes a catalog tuple built with workspaceAllocation.
// Allocation authority and ownership already belong to the transaction, so
// WriteBatch only adds its rowids and records it. The failure owner releases
// the tuple before it has entered txn.writes.
func (txn *Transaction) adoptCatalogInsert(note string, tableID uint64, tableName string, rows *batch.Batch, attrs []string) (err error) {
	txn.rememberWorkspaceCopy(rows)
	published := false
	defer func() {
		if !published {
			txn.takeWorkspaceCopy(rows)
			rows.Clean(txn.workspaceMP())
		}
	}()
	_, err = txn.WriteBatch(INSERT, note, catalog.System_Account, catalog.MO_CATALOG_ID, tableID,
		catalog.MO_CATALOG, tableName, rows, attrs, txn.tnStores[0])
	published = err == nil
	return err
}
