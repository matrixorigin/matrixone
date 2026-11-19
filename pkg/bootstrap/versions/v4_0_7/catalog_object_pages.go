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

package v4_0_7

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type CatalogTableKey struct{ Database, Table string }

// ListForeignKeyTables returns complete child-table groups in stable catalog
// order. A child table, not an individual FK row, is the unit of reconciliation.
func (v *versionHandle) ListForeignKeyTables(tx executor.TxnExecutor, tenantID int32, after CatalogTableKey, pageBytes uint64) ([]CatalogTableKey, error) {
	sql := fmt.Sprintf("select distinct db_name, table_name from mo_catalog.mo_foreign_keys "+
		"where db_name > ? or (db_name = ? and table_name > ?) order by db_name, table_name limit %d", CatalogDataPageSize)
	res, err := tx.Exec(sql, versions.UpgradeStatementOption(uint32(tenantID)).WithParams([]string{after.Database, after.Database, after.Table}).WithMaxResultBytes(pageBytes))
	if err != nil {
		return nil, err
	}
	defer res.Close()
	tables := make([]CatalogTableKey, 0, CatalogDataPageSize)
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			tables = append(tables, CatalogTableKey{Database: cols[0].GetStringAt(i), Table: cols[1].GetStringAt(i)})
		}
		return true
	})
	return tables, nil
}

// UpgradeForeignKeyTable keeps every constraint row of one child together and
// caps each retained definition or SHOW CREATE result before it is cloned.
func (v *versionHandle) UpgradeForeignKeyTable(ctx context.Context, tenantID int32, tx executor.TxnExecutor, table CatalogTableKey, objectLimit uint64) error {
	filter := " AND fk.db_name = " + sqlquote.String(table.Database) + " AND fk.table_name = " + sqlquote.String(table.Table)
	return upgradeLegacyForeignKeyMetadataWhere(ctx, tenantID, limitedCatalogTxn{TxnExecutor: tx, limit: objectLimit}, filter)
}

type limitedCatalogTxn struct {
	executor.TxnExecutor
	limit uint64
}

func (t limitedCatalogTxn) Exec(sql string, opts executor.StatementOption) (executor.Result, error) {
	current := opts.MaxResultBytes()
	if t.limit != 0 && (current == 0 || t.limit < current) {
		opts = opts.WithMaxResultBytes(t.limit)
	}
	return t.TxnExecutor.Exec(sql, opts)
}
