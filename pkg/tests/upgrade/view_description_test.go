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

	"github.com/matrixorigin/matrixone/pkg/embed"
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
		exec("drop table view_description_test.src")
		var invalidFields [7]sql.NullString
		err = db.QueryRowContext(ctx, "desc view_description_test.v").Scan(
			&invalidFields[0], &invalidFields[1], &invalidFields[2], &invalidFields[3],
			&invalidFields[4], &invalidFields[5], &invalidFields[6])
		require.Error(t, err)
		exec("create table view_description_test.src (x varchar(90), qty bigint not null default 11)")
		repaired := describe("desc view_description_test.v")
		require.Len(t, repaired, 2)
		require.Equal(t, "VARCHAR(90)", repaired[0][1].String)
		require.Equal(t, "11", repaired[1][4].String)
		require.NoError(t, db.QueryRowContext(ctx, "select character_maximum_length from information_schema.columns where table_schema='view_description_test' and table_name='v' and column_name='label'").Scan(&width))
		require.Equal(t, 90, width)
		require.NoError(t, prepared.QueryRowContext(ctx).Scan(&width))
		require.Equal(t, 90, width, "prepared metadata reads must rebind the View")
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
		exec("create table view_description_pub.src (x varchar(5))")
		exec("create table view_description_pub.bad_src (x int)")
		exec("create view view_description_pub.v as select x from view_description_pub.src")
		exec("create view view_description_pub.bad_view as select x from view_description_pub.bad_src")
		exec("create publication view_description_publication database view_description_pub account view_description_sub")
		defer exec("drop publication view_description_publication")
		subscriber := openViewDescriptionDB(t, port, "view_description_sub#admin#accountadmin:111")
		_, err = subscriber.ExecContext(ctx, "create database subscribed from sys publication view_description_publication")
		require.NoError(t, err)
		defer func() { _, err := subscriber.ExecContext(ctx, "drop database subscribed"); require.NoError(t, err) }()
		exec("alter table view_description_pub.src modify column x varchar(60)")
		exec("drop table view_description_pub.bad_src")
		query := "select character_maximum_length from information_schema.columns where table_schema='subscribed' and table_name='v' and column_name='x'"
		var width int
		require.NoError(t, subscriber.QueryRowContext(ctx, query).Scan(&width), "a filtered-out invalid View must not be bound")
		require.Equal(t, 60, width)
		prepared, err := subscriber.PrepareContext(ctx, query)
		require.NoError(t, err)
		defer prepared.Close()
		exec("alter table view_description_pub.src modify column x varchar(90)")
		require.NoError(t, prepared.QueryRowContext(ctx).Scan(&width))
		require.Equal(t, 90, width)
		rows, err := subscriber.QueryContext(ctx, "select table_name from information_schema.columns where table_schema='subscribed'")
		require.Error(t, err, "an authorized full subscription scan must report its invalid View")
		if rows != nil {
			require.NoError(t, rows.Close())
		}

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
