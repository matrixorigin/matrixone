// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package v4_0_6

import (
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// The vector and fulltext2 index metadata tables gained nrow and build_ts. Unlike a catalog
// table there is no single DDL to bump: each metadata table is created per index at CREATE
// INDEX by that algorithm's plugin schema, so the set of tables to alter is only known at
// runtime and differs per account.
//
// This is not cosmetic. A CN running the new code writes six-value metadata rows, so the first
// CDC sync or rebuild against an index created before the columns existed would fail on a
// column-count mismatch (or "unknown column build_ts" for fulltext2's named-column insert).
// Readers already tolerate the old four-column shape; writers cannot, which is what makes the
// migration required rather than optional.
//
// REINDEX rewrites metadata ROWS, not the table, so an index never picks the columns up on its
// own -- without this entry a pre-existing index stays broken for writes forever.
var indexMetadataProvenanceTypes = []string{
	catalog.Hnsw_TblType_Metadata,
	catalog.Cagra_TblType_Metadata,
	catalog.Ivfpq_TblType_Metadata,
	catalog.FullText2Index_TblType_Metadata,
}

// indexMetadataProvenanceColumns is the column each table type must end up with. fulltext2
// already had nrow, so only build_ts is added there.
func indexMetadataProvenanceColumns(tblType string) []string {
	if tblType == catalog.FullText2Index_TblType_Metadata {
		return []string{catalog.FullText2Index_TblCol_Metadata_Build_Ts}
	}
	return []string{catalog.Hnsw_TblCol_Metadata_Nrow, catalog.Hnsw_TblCol_Metadata_Build_Ts}
}

// listIndexMetadataTables returns (database, table, algo_table_type) for every index metadata
// table this account owns.
func listIndexMetadataTables(txn executor.TxnExecutor, accountId uint32) ([][3]string, error) {
	quoted := make([]string, 0, len(indexMetadataProvenanceTypes))
	for _, t := range indexMetadataProvenanceTypes {
		quoted = append(quoted, sqlquote.String(t))
	}
	sql := fmt.Sprintf(
		"select distinct t.reldatabase, i.index_table_name, i.algo_table_type "+
			"from %s.mo_indexes i join %s.mo_tables t on t.rel_id = i.table_id "+
			"where i.algo_table_type in (%s) and i.index_table_name is not null and i.index_table_name != ''",
		catalog.MO_CATALOG, catalog.MO_CATALOG, strings.Join(quoted, ","))

	res, err := txn.Exec(sql, versions.UpgradeStatementOption(accountId))
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var out [][3]string
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			out = append(out, [3]string{
				cols[0].GetStringAt(i), cols[1].GetStringAt(i), cols[2].GetStringAt(i),
			})
		}
		return true
	})
	return out, nil
}

// hasColumn reports whether db.tbl already has the named column.
func hasIndexMetadataColumn(txn executor.TxnExecutor, accountId uint32, db, tbl, col string) (bool, error) {
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

// upgradeIndexMetadataProvenance adds the columns to every index metadata table this account
// already owns. It follows upgradeLegacyForeignKeyMetadata: work that a fixed UpgSql cannot
// express is a plain function called from HandleTenantUpgrade, not a new hook on UpgradeEntry.
//
// Idempotent -- it checks each column before altering, so a retried upgrade is a no-op, and MO
// has no ADD COLUMN IF NOT EXISTS.
func upgradeIndexMetadataProvenance(_ context.Context, tenantID int32, txn executor.TxnExecutor) error {
	accountId := uint32(tenantID)
	tables, err := listIndexMetadataTables(txn, accountId)
	if err != nil {
		return err
	}
	for _, t := range tables {
		db, tbl, typ := t[0], t[1], t[2]
		for _, col := range indexMetadataProvenanceColumns(typ) {
			has, err := hasIndexMetadataColumn(txn, accountId, db, tbl, col)
			if err != nil {
				return err
			}
			if has {
				continue
			}
			alter := fmt.Sprintf("alter table %s add column %s bigint not null default 0",
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
