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

// LineageOwnerPublicationLockSQL writes a catalog row that exists before any
// snapshot, PITR, or data-branch lineage row can be created. Pessimistic
// transactions serialize on its row lock; optimistic transactions serialize
// through a write-write conflict and retry. A SELECT FOR UPDATE is insufficient
// here because optimistic transactions do not take that locking path.
func LineageOwnerPublicationLockSQL() string {
	return fmt.Sprintf(
		"update %s.%s set dat_id = dat_id where dat_id = %d",
		catalog.MO_CATALOG, catalog.MO_DATABASE, catalog.MO_CATALOG_ID,
	)
}

// LockLineageOwnerPublication crosses the stable write barrier through the
// caller's transaction. The caller must keep that transaction open through the
// owner publication or ALTER decision that the barrier protects.
func LockLineageOwnerPublication(exec func(string) error) error {
	return exec(LineageOwnerPublicationLockSQL())
}
