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

package databranchutils

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

// LineageOwnerLifecycleLockSQL returns the lifecycle write barrier. It writes
// the bootstrap-created SNAPSHOT feature registry row, which exists before any
// snapshot, PITR, or data-branch lineage row can be created. The write-write
// conflict makes an optimistic owner publication retry when a concurrent
// publication crosses the barrier. Commit-time validation also uses this write
// barrier for pessimistic transactions to detect a completed owner writer.
// updated_at is assigned explicitly so crossing the barrier does not change the
// feature registry's observable metadata.
func LineageOwnerLifecycleLockSQL() string {
	return fmt.Sprintf(
		"update %s.%s set scope_spec = scope_spec, updated_at = updated_at where feature_code = 'SNAPSHOT'",
		catalog.MO_CATALOG, catalog.MO_FEATURE_REGISTRY,
	)
}

// LineageOwnerLifecyclePessimisticLockSQL locks the bootstrap-created SNAPSHOT
// feature registry row without creating a new MVCC version. The lock remains
// held by the caller's transaction through the protected owner mutation.
func LineageOwnerLifecyclePessimisticLockSQL() string {
	return fmt.Sprintf(
		"select feature_code from %s.%s where feature_code = 'SNAPSHOT' for update",
		catalog.MO_CATALOG, catalog.MO_FEATURE_REGISTRY,
	)
}

// LineageOwnerLifecycleLockSQLForTxn selects the lifecycle gate that matches
// the caller's transaction mode. Pessimistic transactions already have a row
// lock path, so they must not use the no-op write that can expose an extra
// feature-registry MVCC version to a later quota read.
func LineageOwnerLifecycleLockSQLForTxn(txnOp client.TxnOperator) string {
	if txnOp != nil && txnOp.Txn().IsPessimistic() {
		return LineageOwnerLifecyclePessimisticLockSQL()
	}
	return LineageOwnerLifecycleLockSQL()
}

// LockLineageOwnerLifecycle crosses the stable write barrier through the
// caller's transaction. The caller must keep that transaction open through
// the owner mutation, restore, or lineage-GC decision that it protects.
func LockLineageOwnerLifecycle(exec func(string) error) error {
	return exec(LineageOwnerLifecycleLockSQL())
}
