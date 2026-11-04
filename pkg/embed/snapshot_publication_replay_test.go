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
		// View definitions below use the selected database for unqualified
		// dependencies; keep USE and CREATE/ALTER on the same session.
		admin.SetMaxOpenConns(1)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		_, err := admin.ExecContext(ctx, "create account pub_replay_sub admin_name 'admin' identified by '111'")
		require.NoError(t, err)
		cancel()
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
			emptyAccounts                               bool
			viewScope, incompleteViewScope              bool
			caseSensitive, caseCollision                bool
		}{
			{name: "quoted_comment", comment: "owner's copy"},
			{name: "backslash_comment", comment: `owner's C:\new\table\`},
			{name: "external_fk", comment: "live publication", externalFK: true},
			{name: "quoted_database_and_publication", database: "review-db", publication: "review-pub"},
			{name: "quoted_identifiers", database: "review-db", publication: "review-pub", table: "review-items", explicitTables: true},
			{name: "empty_accounts", comment: "owner's empty authorization", emptyAccounts: true},
			{name: "empty_quoted_tables", database: "review-empty-db", publication: "review-empty-pub", table: "review-items", explicitTables: true, emptyAccounts: true},
			{name: "complete_view_scope", explicitTables: true, viewScope: true},
			{name: "incomplete_view_scope", explicitTables: true, viewScope: true, incompleteViewScope: true},
			{name: "case_sensitive_noncollision", table: "PublishedT", explicitTables: true, caseSensitive: true},
			{name: "case_sensitive_collision", table: "PublishedT", explicitTables: true, caseSensitive: true, caseCollision: true},
		} {
			if !t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
				defer cancel()
				if tc.caseSensitive {
					for _, pool := range []*sql.DB{admin, subscriber} {
						var savedMode int64
						require.NoError(t, pool.QueryRowContext(ctx, "select @@global.lower_case_table_names").Scan(&savedMode))
						defer func() {
							cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
							defer cancel()
							_, err := pool.ExecContext(cleanupCtx, fmt.Sprintf("set global lower_case_table_names = %d", savedMode))
							require.NoError(t, err)
						}()
						_, err := pool.ExecContext(ctx, "set global lower_case_table_names = 0")
						require.NoError(t, err)
						// Variables are tenant-scoped and existing sessions retain
						// their initial snapshot. Reconnect both tenants' sessions.
						pool.SetMaxIdleConns(0)
						pool.SetMaxIdleConns(2)
						var sessionMode int64
						require.NoError(t, pool.QueryRowContext(ctx, "select @@lower_case_table_names").Scan(&sessionMode))
						require.Zero(t, sessionMode)
					}
				}
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
					if tc.viewScope {
						_, err := admin.Exec("use mo_catalog")
						require.NoError(t, err)
					}
					for _, name := range []string{"subscribed", "denied_sub", "granted_sub"} {
						_, err := subscriber.Exec("drop database if exists " + name)
						require.NoError(t, err)
					}
					for _, query := range []string{"drop snapshot if exists " + snapshot, "drop publication if exists " + publication,
						"drop database if exists " + external, "drop database if exists " + db} {
						_, err := admin.ExecContext(context.Background(), query)
						require.NoError(t, err, query)
					}
				}()
				exec("create table " + db + "." + table + "(id int primary key)")
				exec("insert into " + db + "." + table + " values(1)")
				if tc.caseCollision {
					exec("create table " + db + ".publishedt(id int primary key)")
					exec("insert into " + db + ".publishedt values(901)")
				}
				publishOptions := " account all"
				if tc.emptyAccounts {
					publishOptions = " account pub_replay_sub"
				}
				if tc.explicitTables {
					exec("create table " + db + ".`other-items`(id int primary key)")
					exec("insert into " + db + ".`other-items` values(1)")
					publishOptions = " table " + table + ",`other-items` account pub_replay_sub"
				}
				if tc.viewScope {
					exec("use " + db)
					exec("create view scope_v as select id from " + table)
					publishOptions = " table " + table + ",`other-items`,scope_v account pub_replay_sub"
				}
				exec("create publication " + publication + " database " + db + publishOptions + " comment " + sqlquote.String(tc.comment))
				_, err := subscriber.ExecContext(ctx, "create database subscribed from sys publication "+publication)
				require.NoError(t, err)
				if tc.caseCollision {
					var privateValue int
					require.NoError(t, admin.QueryRowContext(ctx, "select id from "+db+".publishedt").Scan(&privateValue))
					require.Equal(t, 901, privateValue, "the private table exists at the publisher")
					require.ErrorContains(t, subscriber.QueryRowContext(ctx, "select id from subscribed.publishedt").Scan(&privateValue),
						`table "publishedt" does not exist`, "the case-distinct private table must be hidden before restore")
				}
				if tc.emptyAccounts {
					exec("alter publication " + publication + " account drop pub_replay_sub")
					var accounts string
					require.NoError(t, admin.QueryRowContext(ctx, "select account_list from mo_catalog.mo_pubs where pub_name="+sqlquote.String(pubName)).Scan(&accounts))
					require.Empty(t, accounts, "supported ACCOUNT DROP must produce a zero-authorization snapshot")
				}
				if tc.externalFK {
					exec("create database " + external)
					exec("create table " + external + ".child_t(id int primary key,parent_id int,foreign key(parent_id) references " + db + ".parent_t(id))")
					exec("insert into " + external + ".child_t values(1,1)")
				}
				if tc.incompleteViewScope {
					// CREATE was valid. The saved publication now contains a
					// redefined view that needs an unselected table: replay must
					// retain main's scope validation and reject atomically.
					exec("create table private_t(id int primary key)")
					exec("alter view scope_v as select id from private_t")
				}
				exec("create snapshot " + snapshot + " for database " + db)
				exec("insert into " + db + "." + table + " values(2)")
				if tc.explicitTables {
					exec("insert into " + db + ".`other-items` values(2)")
				}
				if tc.emptyAccounts {
					// The live publication is authorized again; restore must
					// recover the snapshot's empty set, not preserve this grant.
					exec("alter publication " + publication + " account add pub_replay_sub")
					var liveRows int
					require.NoError(t, subscriber.QueryRowContext(ctx, "select count(*) from subscribed."+table).Scan(&liveRows))
					require.Equal(t, 2, liveRows)
				}
				wantComment := tc.comment
				if tc.externalFK {
					wantComment += " changed after snapshot"
					exec("alter publication " + publication + " comment " + sqlquote.String(wantComment))
				}
				for range 2 {
					restoreSQL := fmt.Sprintf("restore database %s{snapshot='%s'}", db, snapshot)
					if tc.incompleteViewScope {
						_, err := admin.ExecContext(ctx, restoreSQL)
						require.ErrorContains(t, err, "private_t")
						var rows int
						require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from "+db+"."+table).Scan(&rows))
						require.Equal(t, 2, rows, "scope rejection must roll back restored data")
						var tables string
						require.NoError(t, admin.QueryRowContext(ctx, "select table_list from mo_catalog.mo_pubs where pub_name="+sqlquote.String(pubName)).Scan(&tables))
						require.ElementsMatch(t, []string{tableName, "other-items", "scope_v"}, strings.Split(tables, ","))
						continue
					}
					exec(restoreSQL)
					var rows int
					require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from "+db+"."+table).Scan(&rows))
					if tc.externalFK {
						require.Equal(t, 2, rows, "a skipped database must retain its live data")
						require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from "+external+".child_t").Scan(&rows))
						require.Equal(t, 1, rows)
					} else {
						require.Equal(t, 1, rows)
					}
					if tc.caseCollision {
						var privateValue int
						require.NoError(t, admin.QueryRowContext(ctx, "select id from "+db+".publishedt").Scan(&privateValue))
						require.Equal(t, 901, privateValue, "private table data remains distinct")
						require.ErrorContains(t, subscriber.QueryRowContext(ctx, "select id from subscribed.publishedt").Scan(&privateValue),
							`table "publishedt" does not exist`, "restore must not grant the lowercase private table")
					}
					var subscribedRows int
					if tc.emptyAccounts {
						require.ErrorContains(t, subscriber.QueryRowContext(ctx, "select count(*) from subscribed."+table).Scan(&subscribedRows), "not allowed", "existing subscriber must lose authorization")
						_, err := subscriber.ExecContext(ctx, "create database denied_sub from sys publication "+publication)
						require.ErrorContains(t, err, "not allowed", "an empty authorization set must not allow a new subscription")
					} else {
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
						if tc.viewScope {
							require.NoError(t, subscriber.QueryRowContext(ctx, "select id from subscribed.scope_v").Scan(&subscribedRows))
							require.Equal(t, 1, subscribedRows)
						}
					}
					func() {
						var restoredDB, restoredTables, restoredAccounts, comment string
						result, err := admin.QueryContext(ctx, "select database_name,table_list,account_list,comment from mo_catalog.mo_pubs where pub_name="+sqlquote.String(pubName))
						require.NoError(t, err)
						defer func() { require.NoError(t, result.Close()) }()
						require.True(t, result.Next())
						require.NoError(t, result.Scan(&restoredDB, &restoredTables, &restoredAccounts, &comment))
						require.Equal(t, dbName, restoredDB)
						if tc.explicitTables {
							wantTables := []string{tableName, "other-items"}
							if tc.viewScope {
								wantTables = append(wantTables, "scope_v")
							}
							require.ElementsMatch(t, wantTables, strings.Split(restoredTables, ","))
						} else {
							require.Equal(t, "*", restoredTables)
						}
						if tc.emptyAccounts {
							require.Empty(t, restoredAccounts)
						} else if tc.explicitTables {
							require.Equal(t, "pub_replay_sub", restoredAccounts)
						} else {
							require.Equal(t, "all", restoredAccounts)
						}
						require.Equal(t, wantComment, comment)
						require.False(t, result.Next(), "exactly one publication must survive repeated restore")
						require.NoError(t, result.Err())
						require.NoError(t, result.Close())
					}()
				}
				if tc.emptyAccounts {
					exec("alter publication " + publication + " account add pub_replay_sub")
					var rows int
					require.NoError(t, subscriber.QueryRowContext(ctx, "select count(*) from subscribed."+table).Scan(&rows))
					require.Equal(t, 1, rows)
					_, err := subscriber.ExecContext(ctx, "drop database subscribed")
					require.NoError(t, err)
					_, err = subscriber.ExecContext(ctx, "create database granted_sub from sys publication "+publication)
					require.NoError(t, err)
					require.NoError(t, subscriber.QueryRowContext(ctx, "select count(*) from granted_sub."+table).Scan(&rows))
					require.Equal(t, 1, rows)
				}
			}) {
				// A failed scenario may leave shared catalog state behind.
				return
			}
		}
	})
}

func TestSnapshotEmptyPublicationTablesRestore(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		admin := openIcebergTestDB(t, c)
		defer admin.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		exec := func(query string) {
			t.Helper()
			_, err := admin.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		exec("create account empty_tables_sub admin_name 'admin' identified by '111'")
		cancel()
		defer func() { _, err := admin.Exec("drop account empty_tables_sub"); require.NoError(t, err) }()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		sub, err := sql.Open("mysql", fmt.Sprintf("empty_tables_sub#admin:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer sub.Close()
		for _, accounts := range []string{"all", "named", "empty"} {
			if !t.Run(accounts, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
				defer cancel()
				exec := func(query string) {
					t.Helper()
					_, err := admin.ExecContext(ctx, query)
					require.NoError(t, err, query)
				}
				db := sqlquote.Ident("empty-tables-" + accounts)
				const pub, snap = "`empty-tables-pub`", "empty_tables_snapshot"
				exec("create database " + db)
				defer func() {
					_, err := sub.Exec("drop database if exists empty_tables_subscription")
					require.NoError(t, err)
					for _, query := range []string{"drop snapshot if exists " + snap, "drop publication if exists " + pub, "drop database " + db} {
						_, err := admin.Exec(query)
						require.NoError(t, err, query)
					}
				}()
				exec("create table " + db + ".`private-t`(id int primary key)")
				exec("insert into " + db + ".`private-t` values(1)")
				exec("create table " + db + ".`published-t`(id int primary key)")
				grant := "empty_tables_sub"
				if accounts == "all" {
					grant = "all"
				}
				exec("create publication " + pub + " database " + db + " table `published-t` account " + grant + " comment 'owner''s zero tables'")
				_, err := sub.ExecContext(ctx, "create database empty_tables_subscription from sys publication "+pub)
				require.NoError(t, err)
				exec("drop table " + db + ".`published-t`")
				if accounts == "empty" {
					exec("alter publication " + pub + " account drop empty_tables_sub")
				}
				assertSelection := func() {
					t.Helper()
					var all bool
					var tables, grants, comment string
					require.NoError(t, admin.QueryRowContext(ctx, "select all_table,table_list,account_list,comment from mo_catalog.mo_pubs where pub_name='empty-tables-pub'").Scan(&all, &tables, &grants, &comment))
					require.False(t, all)
					require.Empty(t, tables)
					wantGrant := grant
					if accounts == "empty" {
						wantGrant = ""
					}
					require.Equal(t, wantGrant, grants)
					require.Equal(t, "owner's zero tables", comment)
				}
				assertSelection()
				exec("create snapshot " + snap + " for database " + db)
				exec("insert into " + db + ".`private-t` values(2)")
				for range 2 {
					exec("restore database " + db + "{snapshot='" + snap + "'}")
					var rows int
					require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from "+db+".`private-t`").Scan(&rows))
					require.Equal(t, 1, rows)
					assertSelection()
					var pubTables string
					require.NoError(t, admin.QueryRowContext(ctx, "select pub_tables from mo_catalog.mo_subs where pub_name='empty-tables-pub' and sub_account_name='empty_tables_sub'").Scan(&pubTables))
					require.Empty(t, pubTables, "existing subscription must also retain zero tables")
					_, err := sub.ExecContext(ctx, "select * from empty_tables_subscription.`private-t`")
					if accounts == "empty" {
						require.ErrorContains(t, err, "not allowed")
					} else {
						require.ErrorContains(t, err, "private-t", "restoring a zero-table selection must not publish a private table")
					}
					// Even reusing a formerly published name must not turn the
					// explicit empty selection into ALL or revive a dropped name.
					exec("create table " + db + ".`published-t`(id int primary key)")
					_, err = sub.ExecContext(ctx, "select * from empty_tables_subscription.`published-t`")
					if accounts == "empty" {
						require.ErrorContains(t, err, "not allowed")
					} else {
						require.ErrorContains(t, err, "published-t")
					}
				}
			}) {
				// A failed scenario may leave shared catalog state behind.
				return
			}
		}
	})
}

func TestSnapshotPublicationSkipsExternalTables(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		admin := openIcebergTestDB(t, c)
		defer admin.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		mustExec := func(query string) {
			t.Helper()
			_, err := admin.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		mustExec("create account external_pub_owner admin_name 'admin' identified by '111'")
		mustExec("create account external_pub_sub admin_name 'admin' identified by '111'")
		cancel()
		defer func() {
			for _, account := range []string{"external_pub_sub", "external_pub_owner"} {
				_, err := admin.Exec("drop account " + account)
				require.NoError(t, err)
			}
		}()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		connect := func(account string) *sql.DB {
			t.Helper()
			db, err := sql.Open("mysql", fmt.Sprintf("%s#admin:111@tcp(127.0.0.1:%d)/", account, cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			return db
		}
		owner, sub := connect("external_pub_owner"), connect("external_pub_sub")
		owner.SetMaxOpenConns(1)
		defer owner.Close()
		defer sub.Close()
		for _, tc := range []struct {
			name, selection, want string
			unservableView        bool
		}{
			{name: "no_publication"},
			{name: "all_tables", selection: "*", want: "*"},
			{name: "ordinary", selection: "base_t", want: "base_t"},
			{name: "external_only", selection: "`ext-t`"},
			{name: "mixed", selection: "base_t,`ext-t`", want: "base_t"},
			{name: "unservable_no_publication", unservableView: true},
			{name: "unservable_all_tables", selection: "*", want: "*", unservableView: true},
			{name: "unservable_explicit", selection: "base_t,v_ft", want: "base_t", unservableView: true},
		} {
			if !t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
				defer cancel()
				exec := func(query string) {
					t.Helper()
					_, err := owner.ExecContext(ctx, query)
					require.NoError(t, err, query)
				}
				const db, pub, snap = "external_pub_db", "external_pub", "external_pub_snapshot"
				exec("create database " + db)
				defer func() {
					_, err := sub.Exec("drop database if exists external_pub_subscription")
					require.NoError(t, err)
					for _, query := range []string{"drop snapshot if exists " + snap, "drop publication if exists " + pub, "drop database if exists " + db} {
						_, err := owner.Exec(query)
						require.NoError(t, err, query)
					}
				}()
				exec("create table " + db + ".base_t(id int primary key, body text)")
				exec("insert into " + db + ".base_t(id) values(1)")
				exec("create external table " + db + ".`ext-t`(id int) infile{'filepath'='/unused-publication-test.csv','format'='csv'}")
				if tc.unservableView {
					exec("use " + db)
					exec("create fulltext index ft_body on " + db + ".base_t(body)")
					exec("create view " + db + ".v_ft as select id from base_t where match(body) against('hello')")
				}
				if tc.selection != "" {
					selection := ""
					if tc.selection != "*" {
						selection = " table " + tc.selection
					}
					exec("create publication " + pub + " database " + db + selection + " account external_pub_sub comment 'external selection'")
					_, err := sub.ExecContext(ctx, "create database external_pub_subscription from external_pub_owner publication "+pub)
					require.NoError(t, err)
				}
				level := "database " + db
				if tc.unservableView {
					exec("drop index ft_body on " + db + ".base_t")
				}
				exec("create snapshot " + snap + " for " + level)
				exec("insert into " + db + ".base_t(id) values(2)")
				for range 2 {
					exec("restore " + level + "{snapshot='" + snap + "'}")
					var rows int
					require.NoError(t, owner.QueryRowContext(ctx, "select count(*) from "+db+".base_t").Scan(&rows))
					require.Equal(t, 1, rows, "skipping an external table must not roll back the ordinary table restore")
					require.NoError(t, owner.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='"+db+"' and relname='ext-t'").Scan(&rows))
					require.Zero(t, rows)
					if tc.unservableView {
						require.NoError(t, owner.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='"+db+"' and relname='v_ft'").Scan(&rows))
						require.Zero(t, rows, "restore deliberately omits the unservable FULLTEXT view")
					}
					if tc.selection == "" {
						continue
					}
					var all bool
					var tables, grants, comment string
					require.NoError(t, admin.QueryRowContext(ctx, "select all_table,table_list,account_list,comment from mo_catalog.mo_pubs where account_name='external_pub_owner' and pub_name='"+pub+"'").Scan(&all, &tables, &grants, &comment))
					require.Equal(t, tc.want == "*", all)
					require.Equal(t, tc.want, tables)
					require.Equal(t, "external_pub_sub", grants)
					require.Equal(t, "external selection", comment)
					var pubTables string
					require.NoError(t, admin.QueryRowContext(ctx, "select pub_tables from mo_catalog.mo_subs where pub_account_name='external_pub_owner' and pub_name='"+pub+"' and sub_account_name='external_pub_sub'").Scan(&pubTables))
					require.Equal(t, tc.want, pubTables)
					if tc.want != "" {
						require.NoError(t, sub.QueryRowContext(ctx, "select count(*) from external_pub_subscription.base_t").Scan(&rows))
						require.Equal(t, 1, rows)
					} else {
						_, err := sub.ExecContext(ctx, "select * from external_pub_subscription.base_t")
						require.ErrorContains(t, err, "base_t", "external-only selection must not widen to ALL")
					}
					exec("create table " + db + ".`ext-t`(id int primary key)")
					if tc.want != "*" {
						_, err := sub.ExecContext(ctx, "select * from external_pub_subscription.`ext-t`")
						require.ErrorContains(t, err, "ext-t", "reusing an excluded name must not revive publication access")
					}
					if tc.unservableView {
						exec("use " + db)
						exec("create view " + db + ".v_ft as select id from base_t")
						if tc.want != "*" {
							_, err := sub.ExecContext(ctx, "select * from external_pub_subscription.v_ft")
							require.ErrorContains(t, err, "v_ft", "reusing an omitted view name must not revive publication access")
						}
					}
				}
			}) {
				// A failed scenario may leave shared catalog state behind.
				return
			}
		}
	})
}
