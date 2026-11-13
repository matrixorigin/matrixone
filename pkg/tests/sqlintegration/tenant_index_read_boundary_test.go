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

package sqlintegration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestTenantAdminIndexReadBoundary(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		open := func(login string) *sql.DB {
			db, e := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/", login, port))
			require.NoError(t, e)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			return db
		}
		sys := open("dump")
		_, err = sys.ExecContext(ctx, "create account test_index_reader admin_name 'admin' identified by '111'")
		require.NoError(t, err)
		defer cleanupSQLIntegration(t, cn, "drop account if exists test_index_reader", "drop account if exists test_index_other")
		admin := open("test_index_reader#admin")
		for _, q := range []string{
			"create database index_boundary",
			"create table index_boundary.t (id int primary key, v int, key iv(v))",
			"insert into index_boundary.t values (1,10),(2,20)",
		} {
			_, err = admin.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		for _, q := range []string{
			"create role index_reader_role",
			"create user index_reader identified by '111' default role index_reader_role",
			"grant connect on account * to index_reader_role",
			"grant select on table index_boundary.t to index_reader_role",
			"grant index_reader_role to admin",
		} {
			_, err = admin.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		var role, index string
		require.NoError(t, admin.QueryRowContext(ctx, "select current_role()").Scan(&role))
		require.Equal(t, "accountadmin", role)
		require.NoError(t, admin.QueryRowContext(ctx, "select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='index_boundary' and relname='t') and index_table_name is not null and index_table_name<>'' limit 1").Scan(&index))
		target := "index_boundary.`" + index + "`"
		var before, after int
		require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from "+target).Scan(&before))
		require.Equal(t, 2, before)
		prepared, err := admin.PrepareContext(ctx, "select count(*) from "+target)
		require.NoError(t, err)
		defer prepared.Close()
		require.NoError(t, prepared.QueryRowContext(ctx).Scan(&after))
		require.Equal(t, before, after)
		// Both prepare protocols must bind the permitted read, then observe a
		// role change on the same connection before reusing their handles.
		active, err := admin.Conn(ctx)
		require.NoError(t, err)
		defer active.Close()
		_, err = active.ExecContext(ctx, "set enable_remap_hint = 0")
		require.NoError(t, err)
		retained, err := active.PrepareContext(ctx, "select count(*) from "+target)
		require.NoError(t, err)
		defer retained.Close()
		_, err = active.ExecContext(ctx, "prepare index_read_sql from 'select count(*) from "+target+"'")
		require.NoError(t, err)
		defer active.ExecContext(ctx, "deallocate prepare index_read_sql")
		require.NoError(t, retained.QueryRowContext(ctx).Scan(&after))
		require.Equal(t, before, after)
		require.NoError(t, active.QueryRowContext(ctx, "execute index_read_sql").Scan(&after))
		require.Equal(t, before, after)

		_, err = active.ExecContext(ctx, "set role index_reader_role")
		require.NoError(t, err)
		require.NoError(t, active.QueryRowContext(ctx, "select count(*) from index_boundary.t").Scan(&after))
		require.Equal(t, 2, after)
		for _, read := range []func() error{
			func() error { return active.QueryRowContext(ctx, "select count(*) from "+target).Scan(&after) },
			func() error { return retained.QueryRowContext(ctx).Scan(&after) },
			func() error { return active.QueryRowContext(ctx, "execute index_read_sql").Scan(&after) },
		} {
			err = read()
			var denied *mysql.MySQLError
			require.True(t, errors.As(err, &denied))
			require.EqualValues(t, 20101, denied.Number)
		}
		_, err = active.ExecContext(ctx, "set role accountadmin")
		require.NoError(t, err)
		require.NoError(t, retained.QueryRowContext(ctx).Scan(&after))
		require.Equal(t, before, after)
		require.NoError(t, active.QueryRowContext(ctx, "execute index_read_sql").Scan(&after))
		require.Equal(t, before, after)
		for _, q := range []string{
			"delete from " + target,
			"update " + target + " set __mo_index_pri_col=1",
			"insert into " + target + " select * from " + target + " limit 0",
		} {
			_, err = admin.ExecContext(ctx, q)
			require.Error(t, err, q)
			var sqlErr *mysql.MySQLError
			require.True(t, errors.As(err, &sqlErr), q)
			require.EqualValues(t, 20101, sqlErr.Number, q)
		}
		preparedWrite, err := admin.PrepareContext(ctx, "delete from "+target)
		if err == nil {
			defer preparedWrite.Close()
			_, err = preparedWrite.ExecContext(ctx)
		}
		var preparedErr *mysql.MySQLError
		require.True(t, errors.As(err, &preparedErr))
		require.EqualValues(t, 20101, preparedErr.Number)
		require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from "+target).Scan(&after))
		require.Equal(t, before, after)
		reader := open("test_index_reader#index_reader#index_reader_role")
		require.NoError(t, reader.QueryRowContext(ctx, "select count(*) from index_boundary.t").Scan(&after))
		require.Equal(t, 2, after)
		err = reader.QueryRowContext(ctx, "select count(*) from "+target).Scan(&after)
		var sqlErr *mysql.MySQLError
		require.True(t, errors.As(err, &sqlErr))
		require.EqualValues(t, 20101, sqlErr.Number)

		_, err = sys.ExecContext(ctx, "create account test_index_other admin_name 'admin' identified by '111'")
		require.NoError(t, err)
		other := open("test_index_other#admin")
		_, err = other.ExecContext(ctx, "create database index_boundary")
		require.NoError(t, err)
		err = other.QueryRowContext(ctx, "select count(*) from "+target).Scan(&after)
		require.Error(t, err, "another tenant must not read the first tenant's physical index")

		for _, q := range []string{
			"create table index_boundary.vectors(id int primary key, embedding vecf32(2))",
			"insert into index_boundary.vectors values (1,'[1,2]'),(2,'[2,3]')",
			"create index idx using ivfflat on index_boundary.vectors(embedding) lists=1 op_type 'vector_l2_ops'",
		} {
			_, err = admin.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		rows, err := admin.QueryContext(ctx, "select index_table_name from mo_catalog.mo_indexes where name='idx' and algo='ivfflat' and algo_table_type in ('metadata','entries') and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='index_boundary' and relname='vectors')")
		require.NoError(t, err)
		defer rows.Close()
		var ivfTables []string
		for rows.Next() {
			var name string
			require.NoError(t, rows.Scan(&name))
			ivfTables = append(ivfTables, name)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.Len(t, ivfTables, 2)
		// The nightly audit combines the base and physical-index observations
		// in a single statement to keep them on the same snapshot.
		query := "select count(*) from index_boundary.vectors union all select count(*) from index_boundary.`" + ivfTables[0] + "` union all select count(*) from index_boundary.`" + ivfTables[1] + "`"
		rows, err = admin.QueryContext(ctx, query)
		require.NoError(t, err)
		defer rows.Close()
		var observations int
		for rows.Next() {
			var count int
			require.NoError(t, rows.Scan(&count))
			observations++
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.Equal(t, 3, observations)
		for _, name := range ivfTables {
			physical := "index_boundary.`" + name + "`"
			require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from "+physical).Scan(&after))
			_, err = admin.ExecContext(ctx, "delete from "+physical)
			require.Error(t, err)
			require.True(t, errors.As(err, &sqlErr))
			require.EqualValues(t, 20101, sqlErr.Number)
		}

		for _, q := range []string{
			"set experimental_fulltext_index=1",
			"create table index_boundary.docs(id bigint primary key,body text,fulltext index ft(body))",
			"insert into index_boundary.docs values(1,'aurora'),(2,'nebula')",
		} {
			_, err = admin.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		var fulltext string
		require.NoError(t, admin.QueryRowContext(ctx, "select index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='index_boundary' and relname='docs') and index_table_name is not null and index_table_name<>'' limit 1").Scan(&fulltext))
		physical := "index_boundary.`" + fulltext + "`"
		require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from "+physical).Scan(&before))
		_, err = admin.ExecContext(ctx, "delete from "+physical)
		require.Error(t, err)
		require.True(t, errors.As(err, &sqlErr))
		require.EqualValues(t, 20101, sqlErr.Number)
		require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from "+physical).Scan(&after))
		require.Equal(t, before, after)
	})
}
