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

package disttae

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
)

// adoptCatalogCreate consumes a CREATE pair. TN's decoder requires mo_columns
// to immediately follow its mo_tables entry, including with parallel producers.
func (txn *Transaction) adoptCatalogCreate(note string, table, columns *batch.Batch) (err error) {
	batches := []*batch.Batch{table, columns}
	published := 0
	defer func() {
		for _, rows := range batches[published:] {
			rows.Clean(txn.proc.Mp())
		}
	}()
	if err := txn.proc.Ctx.Err(); err != nil {
		return err
	}
	txn.readOnly.Store(false)
	txn.Lock()
	defer txn.Unlock()
	for i, tableID := range []uint64{catalog.MO_TABLES_ID, catalog.MO_COLUMNS_ID} {
		name := catalog.MO_TABLES
		if tableID == catalog.MO_COLUMNS_ID {
			name = catalog.MO_COLUMNS
		}
		_, err = txn.appendBatchLocked(Entry{
			typ: INSERT, accountId: catalog.System_Account,
			databaseId: catalog.MO_CATALOG_ID, tableId: tableID,
			databaseName: catalog.MO_CATALOG, tableName: name,
			bat: batches[i], tnStore: txn.tnStores[0], note: note,
			pkCheckPos: -1, pkCheckReady: true,
		})
		if err != nil {
			return err
		}
		published++
	}
	return nil
}
