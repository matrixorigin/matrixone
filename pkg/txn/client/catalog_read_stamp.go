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

import (
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

// CatalogReadStamp detects statement-local metadata reuse after a transaction
// or workspace transition. It does not permit concurrent workspace mutation.
type CatalogReadStamp struct {
	TransactionID string
	Snapshot      timestamp.Timestamp
	Revision      uint64
	Workspace     Workspace
	// Op is the actual caller's operator, which can wrap the raw stamp reader.
	// Raw operator sampling leaves Op and Visibility unbound.
	Op         TxnOperator
	Visibility uint64
}

// CatalogSnapshotMatch preserves the two existing metadata consumers' distinct
// snapshot contracts. Structural matching includes the timestamp's metadata.
type CatalogSnapshotMatch uint8

const (
	CatalogSnapshotSemantic CatalogSnapshotMatch = iota
	CatalogSnapshotStructural
)

// BindRead associates a raw sample with its caller and workspace observation.
// The caller samples visibility outside the operator's lock. This value owns
// neither the transaction nor a relation and is valid only within its statement.
func (stamp CatalogReadStamp) BindRead(op TxnOperator, visibility uint64) CatalogReadStamp {
	stamp.Op, stamp.Visibility = op, visibility
	return stamp
}

// MatchesRead compares bound observations without sampling or acquiring locks.
// Sampling/error policy stays with the consumer; a stamp is not a concurrency
// fence or a substitute for statement, tenant, schema or authorization checks.
func (stamp CatalogReadStamp) MatchesRead(current CatalogReadStamp, snapshot CatalogSnapshotMatch) bool {
	if stamp.Op == nil || stamp.Op != current.Op || stamp.TransactionID != current.TransactionID ||
		stamp.Workspace != current.Workspace || stamp.Revision != current.Revision || stamp.Visibility != current.Visibility {
		return false
	}
	switch snapshot {
	case CatalogSnapshotSemantic:
		return stamp.Snapshot.Equal(current.Snapshot)
	case CatalogSnapshotStructural:
		return reflect.DeepEqual(stamp.Snapshot, current.Snapshot)
	default:
		return false
	}
}

// TryCatalogReadStamp binds an optional observation to the actual operator.
// It never waits behind transaction work. Pointer workspace admission preserves
// the optional native-reader contract; mandatory readers retain their own
// capture, visibility and error policy and can use BindRead/MatchesRead directly.
func TryCatalogReadStamp(op TxnOperator) (CatalogReadStamp, bool) {
	reader, ok := op.(interface {
		TryCatalogReadStamp() (CatalogReadStamp, bool)
	})
	if !ok || op == nil {
		return CatalogReadStamp{}, false
	}
	stamp, ok := reader.TryCatalogReadStamp()
	if !ok {
		return CatalogReadStamp{}, false
	}
	workspace, ok := stamp.Workspace.(interface{ CatalogVisibility() (uint64, bool) })
	if !ok || reflect.TypeOf(stamp.Workspace).Kind() != reflect.Pointer {
		return CatalogReadStamp{}, false
	}
	visibility, stable := workspace.CatalogVisibility()
	return stamp.BindRead(op, visibility), stable
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
