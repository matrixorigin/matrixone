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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestViewMetadataRevalidationTenantMarkerExecutesAgainstCatalog(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		const account = "view_metadata_marker_account"
		execSQLRequire(t, ctx, db, "set role moadmin")
		execSQLMaybe(t, ctx, db, "drop account if exists `"+account+"`")
		execSQLRequire(t, ctx, db,
			"create account `"+account+"` admin_name 'admin' identified by '111'")
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop account if exists `"+account+"`")
		}()

		value, ok := moruntime.ServiceRuntime(cn.ServiceID()).
			GetGlobalVariables(moruntime.InternalSQLExecutor)
		require.True(t, ok)
		sqlExecutor, ok := value.(executor.SQLExecutor)
		require.True(t, ok)
		require.NoError(t, compile.RequireViewMetadataRevalidation(ctx, sqlExecutor))

		var status string
		require.NoError(t, db.QueryRowContext(ctx,
			"select d.source_relation_kind from mo_catalog.mo_view_dependencies d "+
				"join mo_catalog.mo_account a on d.account_id=a.account_id "+
				"where a.account_name=? and d.target_relation_id=0 and d.dependency_ordinal=0",
			account).Scan(&status))
		require.Equal(t, "REVALIDATE_REQUIRED", status)
	})
}

func TestAccountPITRInvalidatesSubscriberViewMetadata(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		sysDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer sysDB.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()

		const (
			publisher   = "view_metadata_pitr_publisher"
			subscriber  = "view_metadata_pitr_subscriber"
			publishedDB = "view_metadata_pitr_source"
			subDB       = "view_metadata_pitr_subscription"
			localDB     = "view_metadata_pitr_local"
			publication = "view_metadata_pitr_publication"
			pitr        = "view_metadata_pitr"
		)
		cleanup := func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, sysDB, "drop account if exists `"+subscriber+"`")
			execSQLMaybe(t, cleanupCtx, sysDB, "drop account if exists `"+publisher+"`")
		}
		cleanup()
		defer cleanup()

		execSQLRequire(t, ctx, sysDB,
			"create account `"+publisher+"` admin_name 'admin' identified by '111'")
		execSQLRequire(t, ctx, sysDB,
			"create account `"+subscriber+"` admin_name 'admin' identified by '111'")
		publisherDB, err := sql.Open("mysql", fmt.Sprintf(
			"%s#admin#accountadmin:111@tcp(127.0.0.1:%d)/", publisher, port))
		require.NoError(t, err)
		defer publisherDB.Close()
		subscriberDB, err := sql.Open("mysql", fmt.Sprintf(
			"%s#admin#accountadmin:111@tcp(127.0.0.1:%d)/", subscriber, port))
		require.NoError(t, err)
		defer subscriberDB.Close()

		execSQLRequire(t, ctx, publisherDB, "create pitr "+pitr+" for account range 1 'h'")
		execSQLRequire(t, ctx, publisherDB, "create database `"+publishedDB+"`")
		execSQLRequire(t, ctx, publisherDB,
			"create table `"+publishedDB+"`.source_table (value int)")
		var slept int
		require.NoError(t, publisherDB.QueryRowContext(ctx, "select sleep(1)").Scan(&slept))
		var restoreAt string
		require.NoError(t, publisherDB.QueryRowContext(ctx,
			"select cast(current_timestamp as char)").Scan(&restoreAt))

		execSQLRequire(t, ctx, publisherDB,
			"create publication `"+publication+"` database `"+publishedDB+"` account `"+subscriber+"`")
		execSQLRequire(t, ctx, subscriberDB,
			"create database `"+subDB+"` from `"+publisher+"` publication `"+publication+"`")
		execSQLRequire(t, ctx, subscriberDB, "create database `"+localDB+"`")
		execSQLRequire(t, ctx, subscriberDB,
			"create view `"+localDB+"`.subscriber_view as select value from `"+subDB+"`.source_table")
		execSQLRequire(t, ctx, publisherDB,
			"alter table `"+publishedDB+"`.source_table modify value bigint")
		require.Eventually(t, func() bool {
			return strings.EqualFold("BIGINT",
				viewColumnType(t, ctx, subscriberDB, localDB, "subscriber_view"))
		}, 30*time.Second, 500*time.Millisecond)

		execSQLRequire(t, ctx, publisherDB, "restore from pitr "+pitr+" '"+restoreAt+"'")
		require.True(t, strings.EqualFold("INT",
			viewColumnType(t, ctx, publisherDB, publishedDB, "source_table")))
		require.Eventually(t, func() bool {
			return viewRefreshStatus(t, ctx, subscriberDB, localDB, "subscriber_view") == "PENDING"
		}, 30*time.Second, 500*time.Millisecond)
	})
}

func TestSubscriberCanDescribePublishedView(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		sysDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer sysDB.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		const (
			account     = "view_metadata_desc_subscriber"
			database    = "view_metadata_desc_source"
			subDatabase = "view_metadata_desc_subscription"
			localDB     = "view_metadata_desc_local"
			publication = "view_metadata_desc_publication"
		)
		cleanup := func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, sysDB, "drop publication if exists `"+publication+"`")
			execSQLMaybe(t, cleanupCtx, sysDB, "drop database if exists `"+database+"`")
			execSQLMaybe(t, cleanupCtx, sysDB, "drop account if exists `"+account+"`")
		}
		cleanup()
		defer cleanup()

		execSQLRequire(t, ctx, sysDB,
			"create account `"+account+"` admin_name 'admin' identified by '111'")
		execSQLRequire(t, ctx, sysDB, "create database `"+database+"`")
		execSQLRequire(t, ctx, sysDB, "create table `"+database+"`.source_table (value bigint)")
		execSQLRequire(t, ctx, sysDB, "insert into `"+database+"`.source_table values (1)")
		execSQLRequire(t, ctx, sysDB,
			"create view `"+database+"`.published_view as select value from `"+database+"`.source_table")
		execSQLRequire(t, ctx, sysDB,
			"create publication `"+publication+"` database `"+database+"` account `"+account+"`")

		subscriberDB, err := sql.Open("mysql", fmt.Sprintf(
			"%s#admin#accountadmin:111@tcp(127.0.0.1:%d)/", account, port))
		require.NoError(t, err)
		defer subscriberDB.Close()
		execSQLRequire(t, ctx, subscriberDB,
			"create database `"+subDatabase+"` from sys publication `"+publication+"`")
		execSQLRequire(t, ctx, subscriberDB, "create database `"+localDB+"`")
		waitForViewMetadataRevalidation(t, ctx, sysDB)
		execSQLRequire(t, ctx, subscriberDB,
			"create table `"+localDB+"`.like_text like `"+subDatabase+"`.source_table")
		execSQLRequire(t, ctx, subscriberDB,
			"prepare like_stmt from 'create table `"+localDB+"`.like_prepared like `"+
				subDatabase+"`.source_table'")
		execSQLRequire(t, ctx, subscriberDB, "execute like_stmt")
		execSQLRequire(t, ctx, subscriberDB, "deallocate prepare like_stmt")
		execSQLRequire(t, ctx, subscriberDB,
			"create table `"+localDB+"`.cloned clone `"+subDatabase+"`.source_table")

		var maximum, minimum int64
		require.NoError(t, subscriberDB.QueryRowContext(ctx,
			"show table_values from `"+subDatabase+"`.source_table").Scan(&maximum, &minimum))
		require.Equal(t, int64(1), maximum)
		require.Equal(t, int64(1), minimum)

		for _, statement := range []string{
			"select * from `" + subDatabase + "`.published_view",
			"desc `" + subDatabase + "`.published_view",
			"show columns from `" + subDatabase + "`.published_view",
		} {
			func() {
				rows, queryErr := subscriberDB.QueryContext(ctx, statement)
				require.NoError(t, queryErr, statement)
				defer func() { require.NoError(t, rows.Close()) }()
				require.True(t, rows.Next(), statement)
				columns, columnsErr := rows.Columns()
				require.NoError(t, columnsErr)
				values := make([]sql.RawBytes, len(columns))
				dest := make([]any, len(columns))
				for index := range values {
					dest[index] = &values[index]
				}
				require.NoError(t, rows.Scan(dest...))
				require.False(t, rows.Next(), statement)
				require.NoError(t, rows.Err())
				if strings.HasPrefix(statement, "select") {
					return
				}
				require.Equal(t, "value", string(values[0]))
				require.True(t, strings.Contains(strings.ToLower(string(values[1])), "bigint"), string(values[1]))
			}()
		}
	})
}

func waitForViewMetadataRevalidation(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	require.Eventually(t, func() bool {
		var count int
		err := db.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_view_dependencies where target_relation_id=0 "+
				"and dependency_ordinal=0 and source_relation_kind in ('REVALIDATE_REQUIRED','REVALIDATE_SCAN')").
			Scan(&count)
		return err == nil && count == 0
	}, 30*time.Second, 100*time.Millisecond)
}

func viewRefreshStatus(t *testing.T, ctx context.Context, db *sql.DB, database, view string) string {
	t.Helper()
	var status string
	err := db.QueryRowContext(ctx,
		"select r.status from mo_catalog.mo_view_refresh r join mo_catalog.mo_tables t "+
			"on r.account_id=t.account_id and r.target_relation_id=t.rel_id "+
			"where t.reldatabase=? and t.relname=?",
		database, view).Scan(&status)
	if err != nil {
		return ""
	}
	return status
}

func viewColumnType(t *testing.T, ctx context.Context, db *sql.DB, database, view string) string {
	t.Helper()
	var columnType string
	err := db.QueryRowContext(ctx,
		"select data_type from information_schema.columns "+
			"where table_schema=? and table_name=? and column_name='value'",
		database, view).Scan(&columnType)
	if err != nil {
		return ""
	}
	return columnType
}
