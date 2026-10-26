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

package v4_0_18

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// The HNSW metadata table gained quantize_min and quantize_max, the trained range of an
// int8/uint8 model. Each metadata table is created per index at CREATE INDEX, so the tables
// to alter are only known at runtime; this widens the ones that already exist, as v4_0_7
// did for nrow and build_ts.
var hnswMetadataQuantizeColumns = []string{
	catalog.Hnsw_TblCol_Metadata_Quantize_Min,
	catalog.Hnsw_TblCol_Metadata_Quantize_Max,
}

// listHnswMetadataTables returns (database, table) for every HNSW metadata table this
// account owns that still exists; a catalog row that outlives its table is skipped.
func listHnswMetadataTables(txn executor.TxnExecutor, accountId uint32) ([][2]string, error) {
	sql := fmt.Sprintf(
		"select distinct t.reldatabase, i.index_table_name "+
			"from %s.mo_indexes i join %s.mo_tables t on t.rel_id = i.table_id "+
			"join %s.mo_tables it on it.reldatabase = t.reldatabase and it.relname = i.index_table_name "+
			"where i.algo_table_type = %s and i.index_table_name is not null and i.index_table_name != ''",
		catalog.MO_CATALOG, catalog.MO_CATALOG, catalog.MO_CATALOG, sqlquote.String(catalog.Hnsw_TblType_Metadata))
	res, err := txn.Exec(sql, versions.UpgradeStatementOption(accountId))
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var out [][2]string
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			out = append(out, [2]string{cols[0].GetStringAt(i), cols[1].GetStringAt(i)})
		}
		return true
	})
	return out, nil
}

// hasColumn reports whether db.tbl already has the named column.
func hasColumn(txn executor.TxnExecutor, accountId uint32, db, tbl, col string) (bool, error) {
	sql := fmt.Sprintf(
		"select count(*) from %s.mo_columns where att_database = %s and att_relname = %s and attname = %s",
		catalog.MO_CATALOG, sqlquote.String(db), sqlquote.String(tbl), sqlquote.String(col))
	res, err := txn.Exec(sql, versions.UpgradeStatementOption(accountId))
	if err != nil {
		return false, err
	}
	defer res.Close()

	found := false
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			if vector.GetFixedAtWithTypeCheck[int64](cols[0], i) > 0 {
				found = true
			}
		}
		return true
	})
	return found, nil
}

// upgradeHnswMetadataQuantize adds the columns to every HNSW metadata table of the account.
// Idempotent: each column is checked before it is added, so a retried upgrade is a no-op.
func upgradeHnswMetadataQuantize(_ context.Context, tenantID int32, txn executor.TxnExecutor) error {
	accountId := uint32(tenantID)
	tables, err := listHnswMetadataTables(txn, accountId)
	if err != nil {
		return err
	}
	for _, t := range tables {
		db, tbl := t[0], t[1]
		for _, col := range hnswMetadataQuantizeColumns {
			has, err := hasColumn(txn, accountId, db, tbl, col)
			if err != nil {
				return err
			}
			if has {
				continue
			}
			alter := fmt.Sprintf("alter table %s add column %s double default null",
				sqlquote.QualifiedIdent(db, tbl), sqlquote.Ident(col))
			res, err := txn.Exec(alter, versions.UpgradeStatementOption(accountId))
			if err != nil {
				return err
			}
			res.Close()
		}
	}
	return nil
}
