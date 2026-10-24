// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestTemporaryTablePhysicalNameIsSessionScoped(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const schema = "tmp_iso_30584"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)

		owner, err := db.Conn(ctx)
		require.NoError(t, err)
		defer owner.Close()
		attacker, err := db.Conn(ctx)
		require.NoError(t, err)
		defer attacker.Close()

		exec := func(conn *sql.Conn, statement string, args ...any) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement, args...)
			require.NoError(t, err, statement)
		}
		exec(owner, "create database "+schema)
		exec(owner, "use "+schema)
		exec(attacker, "use "+schema)
		exec(owner, "create temporary table victim_t(id int primary key, v varchar(32))")
		exec(owner, "insert into victim_t values (1, 'owner')")

		var physical string
		require.NoError(t, owner.QueryRowContext(ctx,
			"select relname from mo_catalog.mo_tables where reldatabase=? and relkind='temporary_table'",
			schema).Scan(&physical))
		qualified := "`" + schema + "`.`" + physical + "`"

		var visible int
		require.NoError(t, attacker.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_tables where reldatabase=? and relname=?", schema, physical).Scan(&visible))
		require.Zero(t, visible, "foreign temporary roots must not be enumerable")

		assertMySQLError := func(statement string, want ...uint16) {
			t.Helper()
			_, err := attacker.ExecContext(ctx, statement)
			var sqlErr *mysql.MySQLError
			require.ErrorAs(t, err, &sqlErr, statement)
			for _, code := range want {
				if sqlErr.Number == code {
					return
				}
			}
			require.Failf(t, "unexpected MySQL error", "%s returned %d", statement, sqlErr.Number)
		}
		assertMySQLError("select * from "+qualified, 1146)
		assertMySQLError("insert into "+qualified+" values (2, 'attacker')", 1146)
		assertMySQLError("drop table "+qualified, 1051, 1146)

		var count int
		require.NoError(t, owner.QueryRowContext(ctx, "select count(*) from victim_t").Scan(&count))
		require.Equal(t, 1, count)
		exec(owner, "drop temporary table victim_t")
		exec(owner, "create temporary table victim_t(id int primary key, v varchar(32))")
		exec(owner, "insert into victim_t values (2, 'recreated')")
		require.NoError(t, owner.QueryRowContext(ctx, "select count(*) from victim_t").Scan(&count))
		require.Equal(t, 1, count)

		// The durable marker, rather than the textual prefix, classifies temporary
		// roots. A permanent table with the same generated-name shape remains
		// visible to the other session.
		permanent := "__mo_tmp_00112233445566778899aabbccddeeff_" + schema + "_x"
		exec(owner, "create table `"+permanent+"` (id int)")
		defer exec(owner, "drop table `"+permanent+"`")
		require.NoError(t, attacker.QueryRowContext(ctx,
			"select count(*) from mo_catalog.mo_tables where reldatabase=? and relname=?", schema, permanent).Scan(&visible))
		require.Equal(t, 1, visible)
	})
}
