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
)

// LineageOwnerPublicationLockSQL writes the bootstrap-created SNAPSHOT feature
// registry row, which exists before any snapshot, PITR, or data-branch lineage
// row can be created. Pessimistic transactions serialize on its row lock;
// optimistic transactions serialize through a write-write conflict and retry.
// A SELECT FOR UPDATE is insufficient here because optimistic transactions do
// not take that locking path. updated_at is assigned explicitly so crossing the
// barrier does not change the feature registry's observable metadata.
func LineageOwnerPublicationLockSQL() string {
	return fmt.Sprintf(
		"update %s.%s set scope_spec = scope_spec, updated_at = updated_at where feature_code = 'SNAPSHOT'",
		catalog.MO_CATALOG, catalog.MO_FEATURE_REGISTRY,
	)
}

// LockLineageOwnerPublication crosses the stable write barrier through the
// caller's transaction. The caller must keep that transaction open through the
// owner publication or ALTER decision that the barrier protects.
func LockLineageOwnerPublication(exec func(string) error) error {
	return exec(LineageOwnerPublicationLockSQL())
}
