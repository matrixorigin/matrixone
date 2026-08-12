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
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue26882ComQueryCacheTracksViewIdentity(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(2)

		reader, err := db.Conn(ctx)
		require.NoError(t, err)
		defer reader.Close()
		ddl, err := db.Conn(ctx)
		require.NoError(t, err)
		defer ddl.Close()

		database := testutils.GetDatabaseName(t)
		mustExec(t, ctx, ddl, fmt.Sprintf("create database `%s`", database))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = ddl.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", database))
		}()
		mustExec(t, ctx, reader, fmt.Sprintf("use `%s`", database))
		mustExec(t, ctx, ddl, fmt.Sprintf("use `%s`", database))
		mustExec(t, ctx, ddl, "create table t (id int primary key, v varchar(10))")
		mustExec(t, ctx, ddl, "insert into t values (1, 'old')")
		mustExec(t, ctx, ddl, "create view vv as select id from t")

		const exactSQL = "select * from vv order by id"
		columns, values := queryIssue26882Row(t, ctx, reader, exactSQL)
		require.Equal(t, []string{"id"}, columns)
		require.Equal(t, []string{"1"}, values)

		mustExec(t, ctx, ddl, "drop view vv")
		mustExec(t, ctx, ddl,
			"create view vv as select id, v, concat(v, 'x') as label from t")

		for range 2 {
			columns, values = queryIssue26882Row(t, ctx, reader, exactSQL)
			require.Equal(t, []string{"id", "v", "label"}, columns)
			require.Equal(t, []string{"1", "old", "oldx"}, values)
		}

		mustExec(t, ctx, ddl, "drop view vv")
		rows, err := reader.QueryContext(ctx, exactSQL)
		if rows != nil {
			defer rows.Close()
			require.NoError(t, rows.Err())
		}
		require.Error(t, err)
		var mysqlErr *mysql.MySQLError
		require.ErrorAs(t, err, &mysqlErr)
		require.Equal(t, uint16(1146), mysqlErr.Number)

		const (
			publisherAccount  = "issue_26882_publisher"
			subscriberAccount = "issue_26882_subscriber"
			publishedDB       = "issue_26882_published"
			subscriptionDB    = "issue_26882_subscription"
			publicationName   = "issue_26882_publication"
		)
		cleanupAccounts := func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, _ = ddl.ExecContext(cleanupCtx, "drop account if exists `"+subscriberAccount+"`")
			_, _ = ddl.ExecContext(cleanupCtx, "drop account if exists `"+publisherAccount+"`")
		}
		cleanupAccounts()
		defer cleanupAccounts()

		mustExec(t, ctx, ddl,
			"create account `"+publisherAccount+"` admin_name 'admin' identified by '111'")
		mustExec(t, ctx, ddl,
			"create account `"+subscriberAccount+"` admin_name 'admin' identified by '111'")

		publisherDB, err := sql.Open("mysql", fmt.Sprintf(
			"%s#admin#accountadmin:111@tcp(127.0.0.1:%d)/", publisherAccount, port,
		))
		require.NoError(t, err)
		defer publisherDB.Close()
		execSQLRequire(t, ctx, publisherDB, "create database `"+publishedDB+"`")
		execSQLRequire(t, ctx, publisherDB,
			"create view `"+publishedDB+"`.scanless_view as select 1 as value")
		execSQLRequire(t, ctx, publisherDB,
			"create publication `"+publicationName+"` database `"+publishedDB+"` account `"+subscriberAccount+"`")

		subscriberDB, err := sql.Open("mysql", fmt.Sprintf(
			"%s#admin#accountadmin:111@tcp(127.0.0.1:%d)/", subscriberAccount, port,
		))
		require.NoError(t, err)
		defer subscriberDB.Close()
		execSQLRequire(t, ctx, subscriberDB,
			"create database `"+subscriptionDB+"` from `"+publisherAccount+"` publication `"+publicationName+"`")
		subscriptionReader, err := subscriberDB.Conn(ctx)
		require.NoError(t, err)
		defer subscriptionReader.Close()
		mustExec(t, ctx, subscriptionReader, "use `"+subscriptionDB+"`")

		const subscriptionSQL = "select * from scanless_view"
		for range 2 {
			columns, values = queryIssue26882Row(t, ctx, subscriptionReader, subscriptionSQL)
			require.Equal(t, []string{"value"}, columns)
			require.Equal(t, []string{"1"}, values)
		}
	})
}

func queryIssue26882Row(
	t *testing.T,
	ctx context.Context,
	conn *sql.Conn,
	query string,
) ([]string, []string) {
	t.Helper()
	rows, err := conn.QueryContext(ctx, query)
	require.NoError(t, err)
	defer rows.Close()

	columns, err := rows.Columns()
	require.NoError(t, err)
	require.True(t, rows.Next())
	values := make([]sql.RawBytes, len(columns))
	dest := make([]any, len(columns))
	for i := range values {
		dest[i] = &values[i]
	}
	require.NoError(t, rows.Scan(dest...))
	result := make([]string, len(values))
	for i := range values {
		result[i] = string(values[i])
	}
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	return columns, result
}
