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

package disttae

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
)

// A private ALTER COPY relation may defer the shared inventory write until
// publication. The obligation survives statement rollback independently from
// a later fence, so a caller cannot commit an unprotected private relation.
type privateInventoryObligation struct {
	accountID  uint32
	databaseID uint64
	tableID    uint64
	createStmt int
	fenceStmt  int
	hasFence   bool
}

func (c *tableOpsChain) addPrivateInventory(accountID uint32, databaseID, tableID uint64, statementID int) {
	c.Lock()
	defer c.Unlock()
	c.privateInventories = append(c.privateInventories, privateInventoryObligation{
		accountID: accountID, databaseID: databaseID, tableID: tableID, createStmt: statementID,
	})
}

// Only allocator metadata on the exact transaction-private relation may use
// its existing publication obligation instead of taking the inventory lock.
func (c *tableOpsChain) ownsPrivateAutoIncrement(accountID uint32, databaseID, tableID uint64, reqs []*api.AlterTableReq) bool {
	if len(reqs) == 0 {
		return false
	}
	for _, req := range reqs {
		if req == nil || req.Kind != api.AlterKind_UpdateAutoIncrement || req.GetUpdateAutoIncrement() == nil ||
			req.DbId != databaseID || req.TableId != tableID {
			return false
		}
	}
	return c.ownsPrivateInventory(accountID, databaseID, tableID)
}

func (c *tableOpsChain) ownsPrivateInventory(accountID uint32, databaseID, tableID uint64) bool {
	c.RLock()
	defer c.RUnlock()
	for _, entry := range c.privateInventories {
		if entry.accountID == accountID && entry.databaseID == databaseID && entry.tableID == tableID {
			return true
		}
	}
	return false
}

func (c *tableOpsChain) fencePrivateInventory(accountID uint32, databaseID uint64, statementID int) {
	c.Lock()
	defer c.Unlock()
	for i := range c.privateInventories {
		entry := &c.privateInventories[i]
		if !entry.hasFence && entry.accountID == accountID && entry.databaseID == databaseID {
			entry.hasFence = true
			entry.fenceStmt = statementID
		}
	}
}

func (c *tableOpsChain) rollbackPrivateInventoriesLocked(statementID int) {
	kept := c.privateInventories[:0]
	for _, entry := range c.privateInventories {
		if entry.createStmt == statementID {
			continue
		}
		if entry.hasFence && entry.fenceStmt == statementID {
			entry.hasFence = false
		}
		kept = append(kept, entry)
	}
	c.privateInventories = kept
}

func (txn *Transaction) checkPrivateInventoryFences(ctx context.Context) error {
	if txn.tableOps == nil {
		return nil
	}
	txn.tableOps.RLock()
	defer txn.tableOps.RUnlock()
	for _, entry := range txn.tableOps.privateInventories {
		if !entry.hasFence {
			return moerr.NewInternalErrorf(ctx, "private ALTER COPY has no inventory fence for database %d", entry.databaseID)
		}
	}
	return nil
}
