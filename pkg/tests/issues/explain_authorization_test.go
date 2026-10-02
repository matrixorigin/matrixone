// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied.

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestExplainAuthorizationUsesExecutedStatement(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
		defer cancel()
		connect := func(user string, cn int) *sql.Conn {
			service, err := c.GetCNService(cn)
			require.NoError(t, err)
			db, err := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/", user, service.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			return conn
		}
		exec := func(conn *sql.Conn, query string) {
			_, err := conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		deny := func(conn *sql.Conn, query string) {
			_, err := conn.ExecContext(ctx, query)
			require.ErrorContains(t, err, "do not have privilege", query)
		}
		sys := connect("dump", 0)
		exec(sys, "create account explain_auth_case admin_name 'admin' identified by '111'")
		defer func() {
			cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
			defer done()
			_, err := sys.ExecContext(cleanup, "drop account if exists explain_auth_case")
			require.NoError(t, err)
		}()
		admin := connect("explain_auth_case#admin", 0)
		for _, query := range []string{
			"create database app", "create table app.t(id int)", "insert into app.t values(1)",
			"create role reader", "create user u identified by '111'", "grant reader to u", "grant select on table app.t to reader",
			"create role writer", "create user w identified by '111'", "grant writer to w", "grant select, delete on table app.t to writer",
		} {
			exec(admin, query)
		}
		reader, writer := connect("explain_auth_case#u#reader", 1), connect("explain_auth_case#w#writer", 2)
		// A cached source SELECT cannot authorize a forbidden catalog target,
		// even when the INSERT would produce no rows.
		exec(reader, "select id from app.t")
		deny(reader, "insert into mo_catalog.mo_user_grant(role_id,user_id,granted_time,with_grant_option) select id,id,now(),false from app.t where id < 0")
		rowsRemain := func(want int) {
			var n int
			require.NoError(t, admin.QueryRowContext(ctx, "select count(*) from app.t").Scan(&n))
			require.Equal(t, want, n)
		}
		prefixes := []string{"explain ", "explain analyze ", "explain phyplan ", "explain phyplan analyze "}
		for i, prefix := range prefixes {
			readSQL, writeSQL := prefix+"select id from app.t", prefix+"delete from app.t"
			exec(reader, readSQL)
			deny(reader, writeSQL)
			rowsRemain(1)
			deny(reader, fmt.Sprintf("prepare denied_write from '%s'", writeSQL))
			exec(reader, fmt.Sprintf("prepare r%d from '%s'", i, readSQL))
			exec(writer, fmt.Sprintf("prepare w%d from '%s'", i, writeSQL))
		}
		// Binary PREPARE on this release accepts ordinary DML; wrapper plans
		// use SQL PREPARE FROM STRING because its binary grammar excludes EXPLAIN.
		read, err := reader.PrepareContext(ctx, "select id from app.t")
		require.NoError(t, err)
		defer read.Close()
		write, err := writer.PrepareContext(ctx, "delete from app.t")
		require.NoError(t, err)
		defer write.Close()
		exec(reader, "prepare raw_read from 'select id from app.t'")
		exec(writer, "prepare raw_write from 'delete from app.t'")
		exec(admin, "revoke select on table app.t from reader")
		exec(admin, "revoke delete on table app.t from writer")
		deny(reader, "select id from app.t")
		_, err = read.ExecContext(ctx)
		require.ErrorContains(t, err, "do not have privilege")
		_, err = write.ExecContext(ctx)
		require.ErrorContains(t, err, "do not have privilege")
		rowsRemain(1)
		for i, prefix := range prefixes {
			deny(reader, prefix+"select id from app.t")
			deny(writer, prefix+"delete from app.t")

			deny(reader, fmt.Sprintf("execute r%d", i))
			deny(writer, fmt.Sprintf("execute w%d", i))
			rowsRemain(1)
		}
		for _, prefix := range []string{"explain force execute ", "explain analyze force execute "} {
			deny(reader, prefix+"raw_read")
			deny(writer, prefix+"raw_write")
			rowsRemain(1)
		}
		exec(admin, "grant select on table app.t to reader")
		exec(admin, "grant delete on table app.t to writer")
		for i, prefix := range prefixes {
			exec(reader, prefix+"select id from app.t")
			exec(reader, fmt.Sprintf("execute r%d", i))
		}
		// Plain EXPLAIN must not execute a write; ANALYZE must execute an authorized one.
		exec(writer, "explain delete from app.t")
		rowsRemain(1)
		exec(writer, "execute w1")
		rowsRemain(0)
		exec(admin, "insert into app.t values(1)")
		exec(writer, "execute w3")
		rowsRemain(0)
		exec(admin, "insert into app.t values(1)")
		_, err = read.ExecContext(ctx)
		require.NoError(t, err)
		_, err = write.ExecContext(ctx)
		require.NoError(t, err)
		rowsRemain(0)
		// Authorization must not depend on whether unrelated grants were warmed
		// under different enabled roles in earlier statements.
		for _, query := range []string{"create table app.other(id int)", "insert into app.other values(1)", "create role other_reader", "grant select on table app.other to other_reader", "grant other_reader to u"} {
			exec(admin, query)
		}
		exec(reader, "set secondary role all")
		exec(reader, "set enable_privilege_cache = off")
		combined := "select a.id from app.t a join app.other b on a.id = b.id"
		_, coldErr := reader.ExecContext(ctx, combined)
		require.ErrorContains(t, coldErr, "do not have privilege")
		exec(reader, "set enable_privilege_cache = on")
		deny(reader, combined) // No old facts: partial fills must not mix either.
		for _, warmOrder := range [][]string{{"app.t", "app.other"}, {"app.other", "app.t"}} {
			for _, table := range warmOrder {
				exec(reader, "select id from "+table)
			}
			deny(reader, combined)
			deny(reader, "select a.id from app.other b join app.t a on a.id = b.id")
		}
		// Inherited siblings remain distinct even though they share an active root.
		for _, query := range []string{"create role combined_reader", "grant reader,other_reader to combined_reader", "grant combined_reader to u"} {
			exec(admin, query)
		}
		exec(reader, "set secondary role none")
		exec(reader, "set role combined_reader")
		exec(reader, "select id from app.t")
		exec(reader, "select id from app.other")
		deny(reader, combined)
		// One inherited role with both facts satisfies the unchanged SQL rule.
		exec(admin, "grant select on table app.other to reader")
		exec(reader, combined)
		exec(reader, combined)
		exec(reader, "set enable_privilege_cache = off")
		exec(reader, combined)
		exec(reader, "set enable_privilege_cache = on")
		exec(admin, "revoke select on table app.other from reader")
		deny(reader, combined)

	})
}
