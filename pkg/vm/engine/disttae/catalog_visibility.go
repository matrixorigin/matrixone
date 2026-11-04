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

// CatalogVisibility identifies the transaction-local catalog read domain.
// Callers borrow it only within the statement owner, never as a concurrent map lock.
func (txn *Transaction) CatalogVisibility() (uint64, bool) {
	revision := txn.catalogVisibilityRevision.Load()
	stable := txn.catalogMutations.Load() == 0
	return revision, stable && txn.catalogVisibilityRevision.Load() == revision
}

func (txn *Transaction) beginCatalogMutation() {
	txn.catalogMutations.Add(1)
	txn.catalogVisibilityRevision.Add(1)
}

func (txn *Transaction) endCatalogMutation() {
	txn.catalogVisibilityRevision.Add(1)
	txn.catalogMutations.Add(-1)
}
