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
	"fmt"
	"math"
)

const MODatabaseInventory = "mo_database_inventory"

// Database IDs, unlike names, must not be reused for a replacement database.
const MoDatabaseInventoryDDL = `create table mo_catalog.mo_database_inventory (
    account_id int unsigned not null,
    database_id bigint unsigned not null,
    epoch bigint unsigned not null,
    primary key(account_id, database_id)
)`

// DatabaseInventoryIncrementSQL is shared by directory writers that own
// different executor types. The caller must use its existing transaction and
// reject any affected-row count other than one, including exhausted epochs.
func DatabaseInventoryIncrementSQL(accountID uint32, databaseID uint64) string {
	return fmt.Sprintf("update mo_catalog.%s set epoch=epoch+1 where account_id=%d and database_id=%d and epoch>0 and epoch<%d",
		MODatabaseInventory, accountID, databaseID, uint64(math.MaxUint64))
}
