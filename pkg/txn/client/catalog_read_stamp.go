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

package client

import "github.com/matrixorigin/matrixone/pkg/pb/timestamp"

// CatalogReadStamp detects statement-local metadata reuse after a transaction
// or workspace transition. It does not permit concurrent workspace mutation.
type CatalogReadStamp struct {
	TransactionID string
	Snapshot      timestamp.Timestamp
	Revision      uint64
	Workspace     Workspace
}

func (tc *txnOperator) CatalogReadStamp() (CatalogReadStamp, error) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	if err := tc.checkStatus(true); err != nil {
		return CatalogReadStamp{}, err
	}
	return CatalogReadStamp{TransactionID: string(tc.mu.txn.ID), Snapshot: tc.mu.txn.SnapshotTS, Revision: tc.catalogReadRevision.Load(), Workspace: tc.reset.workspace}, nil
}

// TryCatalogReadStamp is the optional, bounded counterpart of CatalogReadStamp.
// It never waits behind transaction work or changes a SQL error/diagnostic.
// Callers must check their context and workspace visibility separately, outside
// storage fences. A stamp does not permit concurrent workspace mutation.
func (tc *txnOperator) TryCatalogReadStamp() (CatalogReadStamp, bool) {
	if tc == nil || !tc.mu.TryRLock() {
		return CatalogReadStamp{}, false
	}
	defer tc.mu.RUnlock()
	// Native IDs are normally 16 bytes. Decline unusual IDs before copying so
	// an optional observation cannot retain or allocate unbounded storage.
	if tc.mu.closed || tc.mu.terminalAction != txnTerminalActionNone || len(tc.mu.txn.ID) == 0 || len(tc.mu.txn.ID) > 64 {
		return CatalogReadStamp{}, false
	}
	return CatalogReadStamp{TransactionID: string(tc.mu.txn.ID), Snapshot: tc.mu.txn.SnapshotTS, Revision: tc.catalogReadRevision.Load(), Workspace: tc.reset.workspace}, true
}
