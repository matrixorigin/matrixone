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

package catalog

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func (entry *TableEntry) RegisterRebaseLogicalDML(txnID string) error {
	if txnID == "" {
		return moerr.NewInvalidInputNoCtx("empty logical DML transaction identity")
	}
	entry.rebaseLogicalDML.Lock()
	defer entry.rebaseLogicalDML.Unlock()
	if entry.rebaseLogicalDML.pending == nil {
		entry.rebaseLogicalDML.pending = make(map[string]struct{})
	}
	entry.rebaseLogicalDML.pending[txnID] = struct{}{}
	return nil
}

// ResolveRebaseLogicalDML is called only after the transaction's final decision.
// A repeated/foreign completion cannot create a write that was never registered.
func (entry *TableEntry) ResolveRebaseLogicalDML(txnID string, committed *types.TS) {
	entry.rebaseLogicalDML.Lock()
	defer entry.rebaseLogicalDML.Unlock()
	if _, ok := entry.rebaseLogicalDML.pending[txnID]; !ok {
		return
	}
	if committed != nil && committed.GT(&entry.rebaseLogicalDML.committed) {
		entry.rebaseLogicalDML.committed = *committed
	}
	delete(entry.rebaseLogicalDML.pending, txnID)
}

func (entry *TableEntry) RebaseLogicalDMLBound() types.TS {
	entry.rebaseLogicalDML.Lock()
	defer entry.rebaseLogicalDML.Unlock()
	return entry.rebaseLogicalDML.committed
}

// RestoreRebaseLogicalDMLBound merges a persisted checkpoint upper bound. It
// neither completes a prepared transaction nor moves an accepted bound backward.
func (entry *TableEntry) RestoreRebaseLogicalDMLBound(bound types.TS) {
	entry.rebaseLogicalDML.Lock()
	defer entry.rebaseLogicalDML.Unlock()
	if bound.GT(&entry.rebaseLogicalDML.committed) {
		entry.rebaseLogicalDML.committed = bound
	}
}

func (entry *TableEntry) HasRebaseLogicalDMLAfter(fixed types.TS) bool {
	entry.rebaseLogicalDML.Lock()
	defer entry.rebaseLogicalDML.Unlock()
	return len(entry.rebaseLogicalDML.pending) > 0 || entry.rebaseLogicalDML.committed.GT(&fixed)
}
