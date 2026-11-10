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

package frontend

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
)

// showMetadataTarget follows the planner's database qualification precedence.
// Historical metadata and view definitions retain their existing database gate.
func showMetadataTarget(stmt tree.Statement) (database, table string, baseTableOnly, supported bool) {
	switch st := stmt.(type) {
	case *tree.ShowTables:
		return st.DBName, "", false, st.AtTsExpr == nil
	case *tree.ShowColumns:
		if st.Table == nil {
			return st.DBName, "", false, false
		}
		database = st.Table.GetDBName()
		if database == "" {
			database = st.DBName
		}
		return database, st.Table.GetTableName(), false, st.Table.GetTableName() != ""
	case *tree.ShowCreateTable:
		if st.Name == nil {
			return "", "", true, false
		}
		return st.Name.GetDBName(), st.Name.GetTableName(), true, st.AtTsExpr == nil && st.Name.GetTableName() != ""
	}
	return "", "", false, false
}

func showMetadataVisibilitySQL(stmt tree.Statement, currentDatabase string, protocolVersion int64) (string, string) {
	database, table, baseTableOnly, supported := showMetadataTarget(stmt)
	if !supported {
		return "", ""
	}
	if database == "" {
		database = currentDatabase
	}
	if database == "" {
		return "", ""
	}
	dbLiteral := escapeSQLString(database)
	local := sysview.MetadataVisibilityCTE(protocolVersion) +
		"SELECT 1 FROM __mo_visible_tables WHERE reldatabase = " + dbLiteral
	if table != "" {
		local += " AND relname = " + escapeSQLString(table)
	}
	if baseTableOnly {
		local += " AND relkind != '" + catalog.SystemViewRel + "'"
	}
	local += " LIMIT 1"
	// Subscription tables have no local catalog row. Only broad grants on the
	// subscriber database apply; never compare subscriber roles to publisher IDs.
	subscription := sysview.SubscriptionMetadataVisibilitySQL(dbLiteral, protocolVersion) +
		fmt.Sprintf(" WHERE EXISTS (SELECT 1 FROM mo_catalog.mo_database d WHERE d.account_id = current_account_id() AND d.datname = %s AND d.dat_type = '%s') LIMIT 1", dbLiteral, catalog.SystemDBTypeSubscription)
	return local, subscription
}

func showSubscriptionBaseTableSQL(database, table string) string {
	return fmt.Sprintf("SHOW FULL TABLES FROM %s WHERE %s = %s AND Table_type != 'VIEW'",
		sqlquote.Ident(database), sqlquote.Ident("Tables_in_"+database), escapeSQLString(table))
}

func authenticateShowMetadata(ctx context.Context, ses *Session, stmt tree.Statement) (allowed bool, stats statistic.StatsArray, err error) {
	stats.Reset()
	protocol := currentProtocolVersion(ses.GetProc())
	// SHOW TABLES object-level admission and its per-table filter must be
	// enabled together. Older protocols retain the existing database gate;
	// their role SQL cannot represent the full inherited-role closure.
	if _, ok := stmt.(*tree.ShowTables); ok && protocol < defines.MORPCVersion41 {
		return false, stats, nil
	}
	localSQL, subscriptionSQL := showMetadataVisibilitySQL(stmt, ses.GetDatabaseName(), protocol)
	if localSQL == "" {
		return false, stats, nil
	}
	bh := ses.GetBackgroundExec(ctx, &BackgroundExecOption{forcePessimisticRC: true})
	defer func() { stats = bh.GetExecStatsArray(); bh.Close() }()
	if err = bh.Exec(ctx, "begin;"); err != nil {
		return false, stats, err
	}
	defer func() { err = finishTxn(ctx, bh, err) }()
	hasRows := func(sql string) (bool, error) {
		bh.ClearExecResultSet()
		if execErr := bh.Exec(ctx, sql); execErr != nil {
			return false, execErr
		}
		results, resultErr := getResultSet(ctx, bh)
		return execResultArrayHasData(results), resultErr
	}
	allowed, err = hasRows(localSQL)
	if err != nil || allowed {
		return allowed, stats, err
	}
	allowed, err = hasRows(subscriptionSQL)
	if err != nil || !allowed {
		return allowed, stats, err
	}
	database, table, baseTableOnly, _ := showMetadataTarget(stmt)
	if baseTableOnly {
		if database == "" {
			database = ses.GetDatabaseName()
		}
		// A subscriber-local broad grant cannot authorize publisher view
		// definitions. Reuse subscription resolution and the publication
		// allowlist in SHOW TABLES, in this already initialized background txn.
		allowed, err = hasRows(showSubscriptionBaseTableSQL(database, table))
	}
	return allowed, stats, err
}
