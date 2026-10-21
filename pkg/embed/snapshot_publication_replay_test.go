// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/stretchr/testify/require"
)

func TestSnapshotDatabasePublicationReplayBoundaries(t *testing.T) {
	// Reuse the same fixture as the surrounding SQL regressions. A second,
	// two-CN shared fixture cannot be admitted while that cluster is active.
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		admin := openIcebergTestDB(t, c)
		defer admin.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		_, err := admin.ExecContext(ctx, "create account pub_replay_sub admin_name 'admin' identified by '111'")
		require.NoError(t, err)
		defer func() { _, err := admin.Exec("drop account pub_replay_sub"); require.NoError(t, err) }()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		subscriber, err := sql.Open("mysql", fmt.Sprintf("pub_replay_sub#admin:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer subscriber.Close()
		for _, tc := range []struct {
			name, comment, database, publication, table string
			externalFK                                  bool
			explicitTables                              bool
		}{
			{name: "quoted_comment", comment: "owner's copy"},
			{name: "backslash_comment", comment: `owner's C:\new\table\`},
			{name: "external_fk", comment: "live publication", externalFK: true},
			{name: "quoted_publication", publication: "review-pub"},
			{name: "quoted_database", database: "review-db"},
			{name: "quoted_table", table: "review-items", explicitTables: true},
			{name: "quoted_identifiers", database: "review-db", publication: "review-pub", table: "review-items", explicitTables: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				dbName, pubName, tableName := tc.database, tc.publication, tc.table
				if dbName == "" {
					dbName = "pub_replay_" + tc.name
				}
				if pubName == "" {
					pubName = "pub_replay_pub"
				}
				if tableName == "" {
					tableName = "parent_t"
				}
				db, publication, table := sqlquote.Ident(dbName), sqlquote.Ident(pubName), sqlquote.Ident(tableName)
				external, snapshot := "pub_replay_external", "pub_replay_snapshot"
				exec := func(query string) {
					t.Helper()
					_, err := admin.ExecContext(ctx, query)
					require.NoError(t, err, query)
				}
				exec("create database " + db)
				defer func() {
					_, err := subscriber.Exec("drop database if exists subscribed")
					require.NoError(t, err)
					for _, query := range []string{"drop snapshot if exists " + snapshot, "drop publication if exists " + publication,
						"drop database if exists " + external, "drop database if exists " + db} {
						_, err := admin.ExecContext(context.Background(), query)
						require.NoError(t, err, query)
					}
				}()
				exec("create table " + db + "." + table + "(id int primary key)")
				exec("insert into " + db + "." + table + " values(1)")
				publishOptions := " account all"
				if tc.explicitTables {
					exec("create table " + db + ".`other-items`(id int primary key)")
					exec("insert into " + db + ".`other-items` values(1)")
					publishOptions = " table " + table + ",`other-items` account pub_replay_sub"
				}
				exec("create publication " + publication + " database " + db + publishOptions + " comment " + sqlquote.String(tc.comment))
				_, err := subscriber.ExecContext(ctx, "create database subscribed from sys publication "+publication)
				require.NoError(t, err)
				if tc.externalFK {
					exec("create database " + external)
					exec("create table " + external + ".child_t(id int primary key,parent_id int,foreign key(parent_id) references " + db + ".parent_t(id))")
					exec("insert into " + external + ".child_t values(1,1)")
				}
				exec("create snapshot " + snapshot + " for database " + db)
				exec("insert into " + db + "." + table + " values(2)")
				if tc.explicitTables {
					exec("insert into " + db + ".`other-items` values(2)")
				}
				wantComment := tc.comment
				if tc.externalFK {
					wantComment += " changed after snapshot"
					exec("alter publication " + publication + " comment " + sqlquote.String(wantComment))
				}
				for range 2 {
					exec(fmt.Sprintf("restore database %s{snapshot='%s'}", db, snapshot))
					var rows int
					require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from "+db+"."+table).Scan(&rows))
					if tc.externalFK {
						require.Equal(t, 2, rows, "a skipped database must retain its live data")
						require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from "+external+".child_t").Scan(&rows))
						require.Equal(t, 1, rows)
					} else {
						require.Equal(t, 1, rows)
					}
					var subscribedRows int
					require.NoError(t, subscriber.QueryRowContext(ctx, "select count(*) from subscribed."+table).Scan(&subscribedRows))
					if tc.externalFK {
						require.Equal(t, 2, subscribedRows)
					} else {
						require.Equal(t, 1, subscribedRows)
					}
					if tc.explicitTables {
						require.NoError(t, subscriber.QueryRowContext(ctx, "select count(*) from subscribed.`other-items`").Scan(&subscribedRows))
						require.Equal(t, 1, subscribedRows)
					}
					var restoredDB, restoredTables, restoredAccounts, comment string
					result, err := admin.QueryContext(ctx, "select database_name,table_list,account_list,comment from mo_catalog.mo_pubs where pub_name="+sqlquote.String(pubName))
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, result.Close()) })
					require.True(t, result.Next())
					require.NoError(t, result.Scan(&restoredDB, &restoredTables, &restoredAccounts, &comment))
					require.Equal(t, dbName, restoredDB)
					if tc.explicitTables {
						require.ElementsMatch(t, []string{tableName, "other-items"}, strings.Split(restoredTables, ","))
						require.Equal(t, "pub_replay_sub", restoredAccounts)
					} else {
						require.Equal(t, "*", restoredTables)
						require.Equal(t, "all", restoredAccounts)
					}
					require.Equal(t, wantComment, comment)
					require.False(t, result.Next(), "exactly one publication must survive repeated restore")
					require.NoError(t, result.Err())
					require.NoError(t, result.Close())
				}
			})
		}
	})
}
