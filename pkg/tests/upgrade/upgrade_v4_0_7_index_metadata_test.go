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

package upgrade

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// An index created before nrow/build_ts existed keeps a metadata table without them, because the
// table is created per index at CREATE INDEX and REINDEX rewrites its rows rather than the table.
// A CN running the new code writes rows that include both columns, so without the tenant upgrade
// the first write against such an index fails on a column-count mismatch. This drives the real
// upgrade against a real cluster: build a legacy-shaped metadata table by dropping the columns,
// run the tenant upgrade, and require that the columns are back AND that a metadata write of the
// current shape succeeds.
func TestV407UpgradeAddsIndexMetadataProvenanceColumns(t *testing.T) {
	embed.RunSingleCNBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		const dbName = "idx_meta_provenance_upgrade"
		exec := func(q string, args ...any) {
			t.Helper()
			_, err := conn.ExecContext(ctx, fmt.Sprintf(q, args...))
			require.NoError(t, err, q)
		}

		exec("set experimental_hnsw_index = 1")
		exec("drop database if exists %s", dbName)
		exec("create database %s", dbName)
		exec("use %s", dbName)
		exec("create table t(id bigint primary key, v vecf32(3))")
		exec("insert into t values (1,'[1,2,3]'),(2,'[4,5,6]')")
		exec("create index idx using hnsw on t(v) op_type 'vector_l2_ops'")

		// The metadata table this account just created, straight from the catalog the upgrade
		// itself enumerates.
		var metaTable string
		require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
			"select index_table_name from %s.mo_indexes where algo_table_type = '%s' "+
				"and table_id in (select rel_id from %s.mo_tables where reldatabase = '%s' and relname = 't')",
			catalog.MO_CATALOG, catalog.Hnsw_TblType_Metadata, catalog.MO_CATALOG, dbName)).Scan(&metaTable))
		require.NotEmpty(t, metaTable)

		hasColumn := func(col string) bool {
			t.Helper()
			var n int
			require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
				"select count(*) from %s.mo_columns where att_database = '%s' and att_relname = '%s' and attname = '%s'",
				catalog.MO_CATALOG, dbName, metaTable, col)).Scan(&n))
			return n > 0
		}

		// A freshly created index already has them -- that is the forward path.
		require.True(t, hasColumn(catalog.Hnsw_TblCol_Metadata_Nrow))
		require.True(t, hasColumn(catalog.Hnsw_TblCol_Metadata_Build_Ts))

		// Make it look like an index created before the columns existed.
		exec("alter table `%s`.`%s` drop column %s", dbName, metaTable, catalog.Hnsw_TblCol_Metadata_Build_Ts)
		exec("alter table `%s`.`%s` drop column %s", dbName, metaTable, catalog.Hnsw_TblCol_Metadata_Nrow)
		require.False(t, hasColumn(catalog.Hnsw_TblCol_Metadata_Nrow), "legacy shape")
		require.False(t, hasColumn(catalog.Hnsw_TblCol_Metadata_Build_Ts), "legacy shape")

		// A current-shape metadata write must fail against the legacy table -- this is the
		// breakage the migration exists to prevent, asserted rather than assumed.
		_, err = conn.ExecContext(ctx, fmt.Sprintf(
			"insert into `%s`.`%s` values ('probe', 'chk', 1, 2, 3, 4)", dbName, metaTable))
		require.Error(t, err, "six values into a legacy four-column metadata table must fail")

		runV407TenantUpgrade(t, ctx, cn)

		require.True(t, hasColumn(catalog.Hnsw_TblCol_Metadata_Nrow), "upgrade restores nrow")
		require.True(t, hasColumn(catalog.Hnsw_TblCol_Metadata_Build_Ts), "upgrade restores build_ts")

		// And the write that failed before now succeeds.
		exec("insert into `%s`.`%s` values ('probe', 'chk', 1, 2, 3, 4)", dbName, metaTable)

		// Idempotent: a second run is a no-op, not an error.
		runV407TenantUpgrade(t, ctx, cn)
		require.True(t, hasColumn(catalog.Hnsw_TblCol_Metadata_Build_Ts))

		exec("drop database if exists %s", dbName)
	})
}

// runV407TenantUpgrade drives the real v4_0_7 tenant upgrade through its public entry point, so
// the migration is exercised exactly as bootstrap runs it rather than through a stub. The
// migration lives in 4.0.7 because a cluster already at 4.0.6 never re-runs the 4.0.6 handler.
func runV407TenantUpgrade(t *testing.T, ctx context.Context, cn embed.ServiceOperator) {
	t.Helper()
	sqlExecutor := testutils.GetSQLExecutor(cn)
	require.NotNil(t, sqlExecutor)
	require.NoError(t, sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		return v4_0_7.Handler.HandleTenantUpgrade(ctx, int32(catalog.System_Account), txn)
	}, executor.Options{}.
		WithDatabase(catalog.MO_CATALOG).
		WithAccountID(catalog.System_Account).
		WithWaitCommittedLogApplied()))
}

// The migration widens the metadata table with ALTER TABLE ADD COLUMN, which MO resolves to
// AlterTable_COPY: the table is rebuilt from constructCreateTableSQL, and that generator cannot
// express relkind. For an index metadata table relkind is the ONLY thing keeping it out of the
// relkind-keyed filters -- buildTableInfoListWhereClause hides it with `relkind not in (...)`
// and applies its __mo_index_% name fallback only inside mo_catalog -- so losing it makes the
// table visible to snapshot restore, PITR restore and CLONE as if it were a user table.
//
// The unit tests pin the mechanism (the option carries a kind, alter.go sets it,
// buildCreateTable adopts it). This pins the OUTCOME: run the real migration against a real
// cluster and require the kind to survive it.
func TestV407UpgradeKeepsIndexMetadataHidden(t *testing.T) {
	embed.RunSingleCNBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		const dbName = "idx_meta_hidden_upgrade"
		exec := func(q string, args ...any) {
			t.Helper()
			_, err := conn.ExecContext(ctx, fmt.Sprintf(q, args...))
			require.NoError(t, err, q)
		}

		exec("set experimental_hnsw_index = 1")
		exec("drop database if exists %s", dbName)
		exec("create database %s", dbName)
		exec("use %s", dbName)
		exec("create table t(id bigint primary key, v vecf32(3))")
		exec("insert into t values (1,'[1,2,3]'),(2,'[4,5,6]')")
		exec("create index idx using hnsw on t(v) op_type 'vector_l2_ops'")

		var metaTable string
		require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
			"select index_table_name from %s.mo_indexes where algo_table_type = '%s' "+
				"and table_id in (select rel_id from %s.mo_tables where reldatabase = '%s' and relname = 't')",
			catalog.MO_CATALOG, catalog.Hnsw_TblType_Metadata, catalog.MO_CATALOG, dbName)).Scan(&metaTable))
		require.NotEmpty(t, metaTable)

		relKind := func() string {
			t.Helper()
			var k string
			require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
				"select relkind from %s.mo_tables where reldatabase = '%s' and relname = '%s'",
				catalog.MO_CATALOG, dbName, metaTable)).Scan(&k))
			return k
		}
		// hiddenByRelKind reproduces the predicate the restore/CLONE table listing uses
		// (pkg/frontend/snapshot.go buildTableInfoListWhereClause): a hidden index table must
		// NOT appear among the tables that listing would carry.
		hiddenByRelKind := func() bool {
			t.Helper()
			var n int
			require.NoError(t, conn.QueryRowContext(ctx, fmt.Sprintf(
				"select count(*) from %s.mo_tables where reldatabase = '%s' and relname = '%s' "+
					"and relkind not in ('%s','%s','%s','%s','%s')",
				catalog.MO_CATALOG, dbName, metaTable,
				catalog.SystemIndexRel,
				catalog.Hnsw_TblType_Metadata, catalog.Cagra_TblType_Metadata,
				catalog.Ivfpq_TblType_Metadata, catalog.FullText2Index_TblType_Metadata)).Scan(&n))
			return n == 0
		}

		before := relKind()
		require.Equal(t, catalog.Hnsw_TblType_Metadata, before,
			"a fresh hnsw metadata table carries its algorithm's kind")
		require.True(t, hiddenByRelKind(), "and is excluded from the restore/CLONE listing")

		// Legacy shape, so the upgrade actually has work to do and really runs the ALTER.
		// DROP COLUMN is itself an AlterTable_COPY, so this also covers the rebuild in the
		// other direction; asserting here rather than only at the end localises a failure to
		// the operation that caused it.
		exec("alter table `%s`.`%s` drop column %s", dbName, metaTable, catalog.Hnsw_TblCol_Metadata_Build_Ts)
		exec("alter table `%s`.`%s` drop column %s", dbName, metaTable, catalog.Hnsw_TblCol_Metadata_Nrow)
		require.Equal(t, before, relKind(), "DROP COLUMN's rebuild must keep the kind too")

		runV407TenantUpgrade(t, ctx, cn)

		require.Equal(t, before, relKind(),
			"the migration's ALTER must not change the table's kind; losing it un-hides the "+
				"table from snapshot restore, PITR restore and CLONE")
		require.True(t, hiddenByRelKind(),
			"and it stays out of the listing those paths build")

		exec("drop database if exists %s", dbName)
	})
}
