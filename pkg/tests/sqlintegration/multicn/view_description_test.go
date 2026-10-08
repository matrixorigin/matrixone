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

package multicn

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
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

func TestViewDescriptionTwoCN(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(2))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cn0, err := cluster.GetCNService(0)
	require.NoError(t, err)
	cn1, err := cluster.GetCNService(1)
	require.NoError(t, err)
	first := openViewDescriptionDB(t, cn0.GetServiceConfig().CN.Frontend.Port, "dump:111")
	second := openViewDescriptionDB(t, cn1.GetServiceConfig().CN.Frontend.Port, "dump:111")
	defer func() {
		if first != nil {
			require.NoError(t, first.Close())
		}
		if second != nil {
			require.NoError(t, second.Close())
		}
	}()
	exec := func(db *sql.DB, statement string) {
		t.Helper()
		_, err := db.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}
	exec(first, "create database view_description_two_cn")
	defer func() {
		// If restart failed, the isolated cluster owns the remaining catalog;
		// do not issue cleanup SQL through a deliberately closed connection.
		if first == nil {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_, err := first.ExecContext(cleanupCtx, "drop snapshot if exists view_description_restore")
		require.NoError(t, err)
		_, err = first.ExecContext(cleanupCtx, "drop database view_description_two_cn")
		require.NoError(t, err)
	}()
	exec(first, "create table view_description_two_cn.src (x varchar(5))")
	exec(first, "insert into view_description_two_cn.src values ('saved')")
	exec(first, "create view view_description_two_cn.v as select x from view_description_two_cn.src")
	exec(first, "create view view_description_two_cn.nested as select x from view_description_two_cn.v")
	exec(first, "alter table view_description_two_cn.src modify column x varchar(60)")

	checkShow := func(query func() (*sql.Rows, error), want string) {
		t.Helper()
		rows, err := query()
		require.NoError(t, err)
		defer rows.Close()
		require.True(t, rows.Next())
		var field, typ, nullable, key, defaultValue, extra, comment sql.NullString
		require.NoError(t, rows.Scan(&field, &typ, &nullable, &key, &defaultValue, &extra, &comment))
		require.Equal(t, "x", field.String)
		require.Equal(t, want, typ.String)
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
	}
	check := func(width int, value string) {
		t.Helper()
		for _, db := range []*sql.DB{first, second} {
			for _, name := range []string{"v", "nested"} {
				var gotWidth int
				require.NoError(t, db.QueryRowContext(ctx,
					"select character_maximum_length from information_schema.columns "+
						"where table_schema='view_description_two_cn' and table_name=? and column_name='x'", name).Scan(&gotWidth))
				require.Equal(t, width, gotWidth)
				checkShow(func() (*sql.Rows, error) {
					return db.QueryContext(ctx, "desc view_description_two_cn."+name)
				}, fmt.Sprintf("VARCHAR(%d)", width))
				var gotValue string
				require.NoError(t, db.QueryRowContext(ctx, "select x from view_description_two_cn."+name).Scan(&gotValue))
				require.Equal(t, value, gotValue)
			}
		}
		// CTAS is a separate consumer, not an expected value computed with the descriptor.
		exec(second, "create table view_description_two_cn.copied as select x from view_description_two_cn.nested")
		func() {
			defer exec(second, "drop table view_description_two_cn.copied")
			checkShow(func() (*sql.Rows, error) {
				return second.QueryContext(ctx, "desc view_description_two_cn.copied")
			}, fmt.Sprintf("VARCHAR(%d)", width))
		}()
	}
	check(60, "saved")
	exec(first, "create snapshot view_description_restore for account")
	var viewID, sourceID uint64
	identity := func(name string) uint64 {
		t.Helper()
		var id uint64
		require.NoError(t, first.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase='view_description_two_cn' and relname=?", name).Scan(&id))
		return id
	}
	viewID = identity("v")

	// Prime a plan on the other CN. Restoring only the source leaves both View
	// definitions untouched, so validating the root View alone is insufficient.
	func() {
		prepared, err := second.PrepareContext(ctx, "show columns from view_description_two_cn.nested")
		require.NoError(t, err)
		defer prepared.Close()
		checkPrepared := func(want string) {
			t.Helper()
			checkShow(func() (*sql.Rows, error) { return prepared.QueryContext(ctx) }, want)
		}
		checkPrepared("VARCHAR(60)")
		exec(first, "alter table view_description_two_cn.src modify column x varchar(90)")
		exec(first, "update view_description_two_cn.src set x='changed'")
		check(90, "changed")
		checkPrepared("VARCHAR(90)")
		sourceID = identity("src")
		exec(first, "restore table view_description_two_cn.src {snapshot='view_description_restore'}")
		require.NotEqual(t, sourceID, identity("src"), "restore must exercise physical replacement")
		require.Equal(t, viewID, identity("v"), "source restore must not recreate the dependent View")
		checkPrepared("VARCHAR(60)")
		check(60, "saved")
	}()

	// Restart the same cluster immediately, retaining its catalog and UUIDs.
	// This is same-binary persistence evidence, not mixed-binary compatibility.
	oldFirst, oldSecond := cn0.RawService(), cn1.RawService()
	firstID, secondID := cn0.ServiceID(), cn1.ServiceID()
	require.NoError(t, first.Close())
	first = nil
	require.NoError(t, second.Close())
	second = nil
	require.NoError(t, cluster.Close())
	require.NoError(t, cluster.Start())
	require.Equal(t, firstID, cn0.ServiceID())
	require.Equal(t, secondID, cn1.ServiceID())
	require.NotSame(t, oldFirst, cn0.RawService())
	require.NotSame(t, oldSecond, cn1.RawService())
	first = openViewDescriptionDB(t, cn0.GetServiceConfig().CN.Frontend.Port, "dump:111")
	second = openViewDescriptionDB(t, cn1.GetServiceConfig().CN.Frontend.Port, "dump:111")
	check(60, "saved")
}
