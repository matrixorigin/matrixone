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

package db

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

// tableReadTarget returns a conservative bound, not a published logtail
// watermark. Limit the operation to the catalog policy whose dependency and
// identity the frontend validates after applying the returned target.
func (db *DB) tableReadTarget(table api.TableID) (timestamp.Timestamp, bool) {
	if !db.IsWriteMode() || db.Catalog == nil || db.TxnMgr == nil ||
		table.DbId != catalog.MO_CATALOG_ID || table.DbName != catalog.MO_CATALOG ||
		table.TbName != "mo_mysql_compatibility_mode" {
		return timestamp.Timestamp{}, false
	}
	bound, ok := db.TxnMgr.CatalogReadTarget()
	if !ok {
		return timestamp.Timestamp{}, false
	}
	catalogDB, err := db.Catalog.GetDatabaseByID(table.DbId)
	if err != nil {
		return timestamp.Timestamp{}, false
	}
	entry, err := catalogDB.GetTableEntryByID(table.TbId)
	if err != nil || !entry.IsActive() {
		return timestamp.Timestamp{}, false
	}
	schema := entry.GetLastestSchema(false)
	// mo_catalog is shared across tenants. The request's AccId identifies the
	// tenant whose rows are being read, while the physical catalog table is
	// owned by SYS; those IDs are intentionally different.
	if schema == nil || schema.Name != table.TbName {
		return timestamp.Timestamp{}, false
	}
	if content := entry.GetLatestKnownDMLPrepare(); content.GT(&bound) {
		bound = content
	}
	// Direct changes to the core catalog tables also affect name-to-ID
	// bindings, even if they did not go through a catalog DDL operation.
	for _, id := range [...]uint64{catalog.MO_DATABASE_ID, catalog.MO_TABLES_ID, catalog.MO_COLUMNS_ID} {
		core, err := catalogDB.GetTableEntryByID(id)
		if err != nil {
			return timestamp.Timestamp{}, false
		}
		if content := core.GetLatestKnownDMLPrepare(); content.GT(&bound) {
			bound = content
		}
	}
	if bound.IsEmpty() {
		return timestamp.Timestamp{}, false
	}
	return bound.ToTimestamp(), true
}
