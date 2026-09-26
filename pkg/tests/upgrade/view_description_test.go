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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func openViewDescriptionDB(t *testing.T, port int64, user string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", fmt.Sprintf("%s@tcp(127.0.0.1:%d)/", user, port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func TestViewDescriptionPublicSQL(t *testing.T) {
	embed.RunSingleCNBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openViewDescriptionDB(t, cn.GetServiceConfig().CN.Frontend.Port, "dump:111")
		exec := func(q string) { t.Helper(); _, err := db.ExecContext(ctx, q); require.NoError(t, err, q) }
		exec("create database view_description_test")
		defer exec("drop database view_description_test")
		exec("create table view_description_test.src (x varchar(5), qty int not null default 7)")
		exec("create view view_description_test.v as select x as label, qty from view_description_test.src")
		describe := func(query string) [][]sql.NullString {
			t.Helper()
			rows, err := db.QueryContext(ctx, query)
			require.NoError(t, err)
			defer rows.Close()
			columns, err := rows.Columns()
			require.NoError(t, err)
			var result [][]sql.NullString
			for rows.Next() {
				values := make([]sql.NullString, len(columns))
				targets := make([]any, len(columns))
				for i := range targets {
					targets[i] = &values[i]
				}
				require.NoError(t, rows.Scan(targets...))
				result = append(result, values)
			}
			require.NoError(t, rows.Err())
			return result
		}
		before := describe("desc view_description_test.v")
		require.Len(t, before, 2)
		require.Equal(t, "VARCHAR(5)", before[0][1].String)
		exec("alter table view_description_test.src modify column x varchar(60)")
		exec("alter table view_description_test.src modify column qty bigint not null default 9")
		after := describe("desc view_description_test.v")
		require.Len(t, after, 2)
		require.Equal(t, "VARCHAR(60)", after[0][1].String)
		require.Equal(t, "9", after[1][4].String)
		require.Equal(t, "NO", after[1][2].String)
		filtered := describe("show columns from view_description_test.v like 'label'")
		require.Len(t, filtered, 1)
		require.Equal(t, "VARCHAR(60)", filtered[0][1].String)
		filtered = describe("show columns from view_description_test.v where Field = 'qty'")
		require.Len(t, filtered, 1)
		require.Equal(t, "9", filtered[0][4].String)
		full := describe("show full columns from view_description_test.v")
		require.Len(t, full, 2)
		require.Equal(t, "VARCHAR(60)", full[0][1].String)
		require.Equal(t, "9", full[1][5].String)
		var stored string
		require.NoError(t, db.QueryRowContext(ctx, "select mo_show_visible_bin(atttyp,3) from mo_catalog.mo_columns where att_database='view_description_test' and att_relname='v' and attname='label'").Scan(&stored))
		require.Equal(t, "VARCHAR(5)", stored)
		// A prepared SHOW must not retain the VALUES rows produced by its first bind.
		showConn, err := db.Conn(ctx)
		require.NoError(t, err)
		func() {
			defer showConn.Close()
			showStmt, err := showConn.PrepareContext(ctx, "show columns from view_description_test.v")
			require.NoError(t, err)
			defer showStmt.Close()
			checkShow := func(want ...string) string {
				rows, err := showStmt.QueryContext(ctx)
				require.NoError(t, err)
				defer rows.Close()
				var got []string
				var firstType string
				for rows.Next() {
					var field, typ, nullable, key, defaultValue, extra, comment sql.NullString
					require.NoError(t, rows.Scan(&field, &typ, &nullable, &key, &defaultValue, &extra, &comment))
					got = append(got, field.String)
					if len(got) == 1 {
						firstType = typ.String
					}
				}
				require.NoError(t, rows.Err())
				require.Equal(t, want, got)
				return firstType
			}
			require.Equal(t, "VARCHAR(60)", checkShow("label", "qty"))
			writer, err := db.Conn(ctx)
			require.NoError(t, err)
			defer writer.Close()
			_, err = writer.ExecContext(ctx, "use view_description_test")
			require.NoError(t, err)
			_, err = writer.ExecContext(ctx, "alter view v as select qty as changed from src")
			require.NoError(t, err)
			checkShow("changed")
			_, err = writer.ExecContext(ctx, "alter view v as select x as label, qty from src")
			require.NoError(t, err)
			_, err = writer.ExecContext(ctx, "create view nested as select label from v")
			require.NoError(t, err)
			nestedStmt, err := showConn.PrepareContext(ctx, "show columns from view_description_test.nested")
			require.NoError(t, err)
			defer nestedStmt.Close()
			checkNested := func(want string) {
				rows, err := nestedStmt.QueryContext(ctx)
				require.NoError(t, err)
				defer rows.Close()
				require.True(t, rows.Next())
				var field, typ, nullable, key, defaultValue, extra, comment sql.NullString
				require.NoError(t, rows.Scan(&field, &typ, &nullable, &key, &defaultValue, &extra, &comment))
				require.Equal(t, "label", field.String)
				require.Equal(t, want, typ.String)
				require.False(t, rows.Next())
				require.NoError(t, rows.Err())
			}
			checkNested("VARCHAR(60)")
			_, err = writer.ExecContext(ctx, "alter table src modify column x varchar(70)")
			require.NoError(t, err)
			require.Equal(t, "VARCHAR(70)", checkShow("label", "qty"))
			checkNested("VARCHAR(70)")
			_, err = writer.ExecContext(ctx, "drop table src")
			require.NoError(t, err)
			func() {
				rows, queryErr := showStmt.QueryContext(ctx)
				if rows != nil {
					defer rows.Close()
					require.NoError(t, rows.Err())
				}
				require.Error(t, queryErr, "prepared SHOW must reject a View whose source was dropped")
			}()
			_, err = writer.ExecContext(ctx, "create table src (x varchar(60), qty bigint not null default 9)")
			require.NoError(t, err)
			require.Equal(t, "VARCHAR(60)", checkShow("label", "qty"))
		}()
		exec("create table view_description_test.unrelated_source (x int)")
		exec("create view view_description_test.unrelated_view as select x from view_description_test.unrelated_source")
		exec("drop table view_description_test.unrelated_source")
		var width int
		require.NoError(t, db.QueryRowContext(ctx, "select character_maximum_length from information_schema.columns where table_schema='view_description_test' and table_name='v' and column_name='label'").Scan(&width))
		require.Equal(t, 60, width)
		prepared, err := db.PrepareContext(ctx, "select character_maximum_length from information_schema.columns where table_schema='view_description_test' and table_name='v' and column_name='label'")
		require.NoError(t, err)
		defer func() { require.NoError(t, prepared.Close()) }()
		require.NoError(t, prepared.QueryRowContext(ctx).Scan(&width))
		require.Equal(t, 60, width)
		exec("create snapshot view_description_history for account")
		defer exec("drop snapshot view_description_history")
		exec("drop table view_description_test.src")
		require.NoError(t, db.QueryRowContext(ctx,
			"select character_maximum_length from information_schema.columns {snapshot = 'view_description_history'} "+
				"where table_schema='view_description_test' and table_name='v' and column_name='label'").Scan(&width))
		require.Equal(t, 60, width, "historical metadata must bind the historical source after it is dropped")
		var invalidFields [7]sql.NullString
		err = db.QueryRowContext(ctx, "desc view_description_test.v").Scan(
			&invalidFields[0], &invalidFields[1], &invalidFields[2], &invalidFields[3],
			&invalidFields[4], &invalidFields[5], &invalidFields[6])
		require.Error(t, err)
		warningConn, err := db.Conn(ctx)
		require.NoError(t, err)
		func() {
			defer warningConn.Close()
			func() {
				rows, err := warningConn.QueryContext(ctx,
					"select column_name from information_schema.columns "+
						"where table_schema='view_description_test' and table_name='v'")
				require.NoError(t, err)
				defer rows.Close()
				require.False(t, rows.Next(), "invalid View metadata must not expose stored columns")
				require.NoError(t, rows.Err())
			}()
			warnings, err := warningConn.QueryContext(ctx, "show warnings")
			require.NoError(t, err)
			defer warnings.Close()
			require.True(t, warnings.Next())
			var level, message string
			var code int
			require.NoError(t, warnings.Scan(&level, &code, &message))
			require.Equal(t, "Warning", level)
			require.Equal(t, 1356, code)
			require.Contains(t, message, "view_description_test.v")
			require.NoError(t, warnings.Err())
		}()
		exec("create table view_description_test.src (x varchar(90), qty bigint not null default 11)")
		repaired := describe("desc view_description_test.v")
		require.Len(t, repaired, 2)
		require.Equal(t, "VARCHAR(90)", repaired[0][1].String)
		require.Equal(t, "11", repaired[1][4].String)
		require.NoError(t, db.QueryRowContext(ctx, "select character_maximum_length from information_schema.columns where table_schema='view_description_test' and table_name='v' and column_name='label'").Scan(&width))
		require.Equal(t, 90, width)
		require.NoError(t, prepared.QueryRowContext(ctx).Scan(&width))
		require.Equal(t, 90, width, "prepared metadata reads must rebind the View")

		// A confirmed missing source database remains a skippable View error,
		// unlike a failed database lookup which must reach the caller unchanged.
		exec("create database view_description_source_db")
		exec("create table view_description_source_db.src (x int)")
		exec("create view view_description_test.missing_db_view as select x from view_description_source_db.src")
		sourceConn, err := db.Conn(ctx)
		require.NoError(t, err)
		func() {
			defer sourceConn.Close()
			_, err := sourceConn.ExecContext(ctx, "use view_description_source_db")
			require.NoError(t, err)
			_, err = sourceConn.ExecContext(ctx,
				"create view view_description_test.missing_default_db_view as select x from src")
			require.NoError(t, err)
		}()
		exec("drop database view_description_source_db")
		missingConn, err := db.Conn(ctx)
		require.NoError(t, err)
		func() {
			defer missingConn.Close()
			func() {
				rows, err := missingConn.QueryContext(ctx,
					"select column_name from information_schema.columns "+
						"where table_schema='view_description_test' and table_name in ('missing_db_view','missing_default_db_view')")
				require.NoError(t, err)
				defer rows.Close()
				require.False(t, rows.Next())
				require.NoError(t, rows.Err())
			}()
			warnings, err := missingConn.QueryContext(ctx, "show warnings")
			require.NoError(t, err)
			defer warnings.Close()
			require.True(t, warnings.Next())
			var level, message string
			var code int
			require.NoError(t, warnings.Scan(&level, &code, &message))
			require.Equal(t, "Warning", level)
			require.Equal(t, 1356, code)
			require.Contains(t, message, "missing_db_view")
			require.NoError(t, warnings.Err())
		}()
	})
}

func TestViewDescriptionPrivileges(t *testing.T) {
	embed.RunSingleCNBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		admin := openViewDescriptionDB(t, port, "dump:111")
		exec := func(q string) { t.Helper(); _, err := admin.ExecContext(ctx, q); require.NoError(t, err, q) }
		exec("create database view_description_priv")
		defer exec("drop database view_description_priv")
		exec("create table view_description_priv.src (x varchar(60))")
		exec("create view view_description_priv.v as select x from view_description_priv.src")
		exec("create user view_description_reader identified by 'reader_pass'")
		defer exec("drop user view_description_reader")
		exec("create role view_description_role")
		defer exec("drop role view_description_role")
		exec("grant view_description_role to view_description_reader")
		exec("grant connect on account * to view_description_role")
		reader := openViewDescriptionDB(t, port, "view_description_reader:reader_pass")
		_, err = reader.ExecContext(ctx, "set role view_description_role")
		require.NoError(t, err)
		_, err = reader.ExecContext(ctx, "use view_description_priv")
		require.NoError(t, err)
		var width int
		err = reader.QueryRowContext(ctx, "select character_maximum_length from information_schema.columns where table_schema='view_description_priv' and table_name='v'").Scan(&width)
		require.Error(t, err, "an invisible View must not be bound or exposed")
		exec("grant show tables on database view_description_priv to view_description_role")
		require.NoError(t, reader.QueryRowContext(ctx, "select character_maximum_length from information_schema.columns where table_schema='view_description_priv' and table_name='v'").Scan(&width))
		require.Equal(t, 60, width)
		var sourceValue string
		err = reader.QueryRowContext(ctx, "select x from view_description_priv.src").Scan(&sourceValue)
		require.Error(t, err, "metadata visibility must not grant source-table SELECT")
	})
}

func TestViewDescriptionTwoCN(t *testing.T) {
	cluster, err := embed.NewCluster(embed.WithCNCount(2), embed.WithTesting(), embed.WithConcurrentTestClusters())
	require.NoError(t, err)
	defer func() { require.NoError(t, cluster.Close()) }()
	require.NoError(t, cluster.Start())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cn0, err := cluster.GetCNService(0)
	require.NoError(t, err)
	cn1, err := cluster.GetCNService(1)
	require.NoError(t, err)
	first := openViewDescriptionDB(t, cn0.GetServiceConfig().CN.Frontend.Port, "dump:111")
	second := openViewDescriptionDB(t, cn1.GetServiceConfig().CN.Frontend.Port, "dump:111")
	_, err = first.ExecContext(ctx, "create database view_description_two_cn")
	require.NoError(t, err)
	defer func() {
		_, err := first.ExecContext(ctx, "drop database view_description_two_cn")
		require.NoError(t, err)
	}()
	_, err = first.ExecContext(ctx, "create table view_description_two_cn.src (x varchar(5))")
	require.NoError(t, err)
	_, err = first.ExecContext(ctx, "create view view_description_two_cn.v as select x from view_description_two_cn.src")
	require.NoError(t, err)
	_, err = first.ExecContext(ctx, "alter table view_description_two_cn.src modify column x varchar(60)")
	require.NoError(t, err)
	var width int
	require.NoError(t, second.QueryRowContext(ctx, "select character_maximum_length from information_schema.columns where table_schema='view_description_two_cn' and table_name='v'").Scan(&width))
	require.Equal(t, 60, width)
}

func TestViewDescriptionSubscription(t *testing.T) {
	embed.RunSingleCNBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		sys := openViewDescriptionDB(t, port, "dump:111")
		exec := func(q string) { t.Helper(); _, err := sys.ExecContext(ctx, q); require.NoError(t, err, q) }
		exec("create account view_description_sub admin_name='admin' identified by '111'")
		defer exec("drop account view_description_sub")
		exec("create database view_description_pub")
		defer exec("drop database view_description_pub")
		exec("create table view_description_pub.src (x varchar(5) character set utf8mb4 collate utf8mb4_general_ci)")
		exec("create table view_description_pub.charset_src (mb4 varchar(5) character set utf8mb4 collate utf8mb4_general_ci, mb4_bin varchar(5) character set utf8mb4 collate utf8mb4_bin, legacy varchar(5) character set utf8, raw varbinary(5), txt text)")
		exec("create view view_description_pub.charset_v as select mb4, mb4_bin, legacy, raw, txt from view_description_pub.charset_src")
		exec("create table view_description_pub.bad_src (x int)")
		exec("create view view_description_pub.v as select x from view_description_pub.src")
		exec("create view view_description_pub.bad_view as select x from view_description_pub.bad_src")
		exec("create publication view_description_publication database view_description_pub account view_description_sub")
		defer exec("drop publication view_description_publication")
		subscriber := openViewDescriptionDB(t, port, "view_description_sub#admin#accountadmin:111")
		_, err = subscriber.ExecContext(ctx, "create database subscribed from sys publication view_description_publication")
		require.NoError(t, err)
		defer func() { _, err := subscriber.ExecContext(ctx, "drop database subscribed"); require.NoError(t, err) }()
		// Simulate a tenant whose V58 COLUMNS definition has not yet been migrated,
		// while all SQL and table functions execute on this newer CN.
		var tenantID uint32
		require.NoError(t, sys.QueryRowContext(ctx,
			"select account_id from mo_catalog.mo_account where account_name='view_description_sub'").Scan(&tenantID))
		sqlExecutor := testutils.GetSQLExecutor(cn)
		replaceColumns := func(ctx context.Context, ddl string) error {
			return sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
				for _, statement := range []string{"drop view if exists information_schema.COLUMNS", ddl} {
					res, execErr := txn.Exec(statement, versions.UpgradeStatementOption(tenantID))
					if execErr != nil {
						return execErr
					}
					res.Close()
				}
				return nil
			}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).
				WithAccountID(tenantID).WithWaitCommittedLogApplied())
		}
		defer func() {
			restoreCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			require.NoError(t, replaceColumns(restoreCtx, sysview.InformationSchemaColumnsDDL))
		}()
		require.NoError(t, replaceColumns(ctx, sysview.InformationSchemaColumnsV58DDL()))
		var legacyColumn string
		require.NoError(t, subscriber.QueryRowContext(ctx,
			"select column_name from information_schema.columns where table_schema='subscribed' and table_name='v'").Scan(&legacyColumn))
		require.Equal(t, "x", legacyColumn)
		// Compare the historical projection with the new one on the same CN.
		// Publisher View, subscription table and subscription View must all
		// agree on the supported character-set selector variants.
		type charsetColumn struct {
			name, charset, collation string
		}
		readCharsets := func(db *sql.DB, schema, table string) []charsetColumn {
			t.Helper()
			rows, err := db.QueryContext(ctx,
				"select column_name, character_set_name, collation_name from information_schema.columns "+
					"where table_schema=? and table_name=? order by ordinal_position", schema, table)
			require.NoError(t, err)
			defer rows.Close()
			var columns []charsetColumn
			for rows.Next() {
				var name string
				var charset, collation sql.NullString
				require.NoError(t, rows.Scan(&name, &charset, &collation))
				columns = append(columns, charsetColumn{name, charset.String, collation.String})
			}
			require.NoError(t, rows.Err())
			return columns
		}
		selectorRows, err := sys.QueryContext(ctx,
			"select internal_column_character_set(atttyp) from mo_catalog.mo_columns "+
				"where att_database='view_description_pub' and att_relname='charset_src' and attname in ('mb4','mb4_bin','legacy','raw')")
		require.NoError(t, err)
		var selectors []int64
		func() {
			defer selectorRows.Close()
			for selectorRows.Next() {
				var selector int64
				require.NoError(t, selectorRows.Scan(&selector))
				selectors = append(selectors, selector)
			}
			require.NoError(t, selectorRows.Err())
		}()
		// DDL exposes explicit utf8mb4-bin (1), binary (2), and utf8 /
		// utf8mb4-general (3). Legacy selector 0 is covered by the DDL UT.
		require.ElementsMatch(t, []int64{1, 2, 3, 3}, selectors)
		publisherTable := readCharsets(sys, "view_description_pub", "charset_src")
		require.Len(t, publisherTable, 5)
		publisherView := readCharsets(sys, "view_description_pub", "charset_v")
		require.Equal(t, publisherTable, publisherView)
		legacySubscriptionView := readCharsets(subscriber, "subscribed", "charset_v")
		require.Equal(t, publisherView, legacySubscriptionView)
		require.Equal(t, "utf8mb4", publisherTable[0].charset)
		require.Equal(t, "utf8mb4_general_ci", publisherTable[0].collation)
		require.Equal(t, "binary", publisherTable[3].charset)

		require.NoError(t, replaceColumns(ctx, sysview.InformationSchemaColumnsDDL))
		require.Equal(t, publisherTable, readCharsets(subscriber, "subscribed", "charset_src"))
		require.Equal(t, publisherView, readCharsets(subscriber, "subscribed", "charset_v"))
		require.Equal(t, legacySubscriptionView, readCharsets(subscriber, "subscribed", "charset_v"))

		// The snapshot belongs to the subscriber, but the View's source database
		// belongs to the publisher. Database existence and relation binding must
		// use the same account and historical timestamp.
		_, err = subscriber.ExecContext(ctx, "create snapshot view_description_sub_history for account")
		require.NoError(t, err)
		defer func() {
			_, dropErr := subscriber.ExecContext(ctx, "drop snapshot view_description_sub_history")
			require.NoError(t, dropErr)
		}()
		exec("alter table view_description_pub.src modify column x varchar(60)")
		checkHistoricalView := func(snapshotName string, wantWidth int) {
			t.Helper()
			conn, err := subscriber.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()
			var column string
			var width int
			require.NoError(t, conn.QueryRowContext(ctx,
				"select column_name, character_maximum_length from information_schema.columns {snapshot = '"+snapshotName+"'} "+
					"where table_schema='subscribed' and table_name='v'").Scan(&column, &width))
			require.Equal(t, "x", column)
			require.Equal(t, wantWidth, width)
			warnings, err := conn.QueryContext(ctx, "show warnings")
			require.NoError(t, err)
			defer warnings.Close()
			for warnings.Next() {
				var level, message string
				var code int
				require.NoError(t, warnings.Scan(&level, &code, &message))
				require.NotEqual(t, 1356, code, "a valid historical subscription View must not be skipped")
			}
			require.NoError(t, warnings.Err())
		}
		checkHistoricalView("view_description_sub_history", 5)
		// An unrelated same-named empty database in the subscriber must never
		// substitute for the publisher's source catalog at a later snapshot.
		_, err = subscriber.ExecContext(ctx, "create database view_description_pub")
		require.NoError(t, err)
		defer func() {
			_, dropErr := subscriber.ExecContext(ctx, "drop database view_description_pub")
			require.NoError(t, dropErr)
		}()
		var field, typ, nullable, key, defaultValue, extra, comment sql.NullString
		require.NoError(t, subscriber.QueryRowContext(ctx, "desc subscribed.v").Scan(
			&field, &typ, &nullable, &key, &defaultValue, &extra, &comment))
		require.Equal(t, "x", field.String)
		require.Equal(t, "VARCHAR(60)", typ.String)
		showStmt, err := subscriber.PrepareContext(ctx, "show columns from subscribed.v")
		require.NoError(t, err)
		defer showStmt.Close()
		checkSubscribedShow := func(want string) {
			rows, err := showStmt.QueryContext(ctx)
			require.NoError(t, err)
			defer rows.Close()
			require.True(t, rows.Next())
			require.NoError(t, rows.Scan(&field, &typ, &nullable, &key, &defaultValue, &extra, &comment))
			require.Equal(t, "x", field.String)
			require.Equal(t, want, typ.String)
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		}
		checkSubscribedShow("VARCHAR(60)")
		checkSubscribedShow("VARCHAR(60)")
		// System View columns remain sourced from the catalog, not regenerated
		// from the internal SQL used to populate the View.
		rows, err := subscriber.QueryContext(ctx, "desc mo_catalog.mo_variables")
		require.NoError(t, err)
		func() {
			defer rows.Close()
			var found bool
			for rows.Next() {
				require.NoError(t, rows.Scan(&field, &typ, &nullable, &key, &defaultValue, &extra, &comment))
				found = found || field.String == "configuration_id"
			}
			require.NoError(t, rows.Err())
			require.True(t, found)
		}()
		exec("drop table view_description_pub.bad_src")
		query := "select character_maximum_length from information_schema.columns where table_schema='subscribed' and table_name='v' and column_name='x'"
		var width int
		require.NoError(t, subscriber.QueryRowContext(ctx, query).Scan(&width), "a filtered-out invalid View must not be bound")
		require.Equal(t, 60, width)
		prepared, err := subscriber.PrepareContext(ctx, query)
		require.NoError(t, err)
		defer prepared.Close()
		_, err = subscriber.ExecContext(ctx, "create snapshot view_description_sub_same_name for account")
		require.NoError(t, err)
		defer func() {
			_, dropErr := subscriber.ExecContext(ctx, "drop snapshot view_description_sub_same_name")
			require.NoError(t, dropErr)
		}()
		exec("alter table view_description_pub.src modify column x varchar(90)")
		checkHistoricalView("view_description_sub_same_name", 60)
		checkSubscribedShow("VARCHAR(90)")
		require.NoError(t, prepared.QueryRowContext(ctx).Scan(&width))
		require.Equal(t, 90, width)
		// Batch metadata scans omit invalid Views, but retain a diagnostic for
		// the client. Keep both queries on the same connection for SHOW WARNINGS.
		warningConn, err := subscriber.Conn(ctx)
		require.NoError(t, err)
		defer warningConn.Close()
		func() {
			rows, err := warningConn.QueryContext(ctx,
				"select distinct table_name from information_schema.columns where table_schema='subscribed'")
			require.NoError(t, err)
			defer rows.Close()
			var names []string
			for rows.Next() {
				var name string
				require.NoError(t, rows.Scan(&name))
				names = append(names, name)
			}
			require.NoError(t, rows.Err())
			require.Contains(t, names, "v")
			require.NotContains(t, names, "bad_view")
		}()
		func() {
			rows, err := warningConn.QueryContext(ctx, "show warnings")
			require.NoError(t, err)
			defer rows.Close()
			var found, warningCount int
			for rows.Next() {
				warningCount++
				var level, message string
				var code int
				require.NoError(t, rows.Scan(&level, &code, &message))
				if level == "Warning" && code == 1356 && strings.Contains(message, "bad_view") {
					found++
				}
			}
			require.NoError(t, rows.Err())
			require.Equal(t, 1, warningCount, "one warning per skipped View")
			require.Equal(t, 1, found, "the skipped invalid View must produce warning 1356")
		}()

		_, err = subscriber.ExecContext(ctx, "create role metadata_reader")
		require.NoError(t, err)
		_, err = subscriber.ExecContext(ctx, "create user metadata_user identified by 'reader_pass'")
		require.NoError(t, err)
		defer subscriber.ExecContext(ctx, "drop user metadata_user")
		defer subscriber.ExecContext(ctx, "drop role metadata_reader")
		_, err = subscriber.ExecContext(ctx, "grant metadata_reader to metadata_user")
		require.NoError(t, err)
		_, err = subscriber.ExecContext(ctx, "grant connect on account * to metadata_reader")
		require.NoError(t, err)
		reader := openViewDescriptionDB(t, port, "view_description_sub#metadata_user#metadata_reader:reader_pass")
		_, err = reader.ExecContext(ctx, "set role metadata_reader")
		require.NoError(t, err)
		defer reader.Close()
		err = reader.QueryRowContext(ctx, query).Scan(&width)
		require.Error(t, err, "an invisible subscription View must not be bound or exposed")
		_, err = subscriber.ExecContext(ctx, "grant show tables on database subscribed to metadata_reader")
		require.NoError(t, err)
		require.NoError(t, reader.QueryRowContext(ctx, query).Scan(&width))
		require.Equal(t, 90, width)
	})
}
