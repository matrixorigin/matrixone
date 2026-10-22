//go:build mo_m0

// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestM0AppendPublicOrdinalsSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db := openM0SQL(t, cn)
		defer db.Close()
		const schema = "m0_public_ordinal"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		m0Exec(t, ctx, db, "create database "+schema)
		for _, table := range []string{"customer", "copy_oracle"} {
			m0Exec(t, ctx, db, "create table "+schema+"."+table+m0RollbackCustomerSchema)
			m0Exec(t, ctx, db, "insert into "+schema+"."+table+" values(1,1,1,50000,100,10)")
			m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+schema+"."+table+"')")
		}
		physicalBefore := publicOrdinalPhysicalColumns(t, ctx, db, schema)
		objectsBefore := m0ObjectHeaderProofs(t, ctx, db, cn, schema+".customer")
		beforeID, _ := m0TableGeneration(t, ctx, db, schema, "customer")
		m0Exec(t, ctx, db, "alter table "+schema+".customer add column added_text varchar(8)")
		afterID, version := m0TableGeneration(t, ctx, db, schema, "customer")
		require.Equal(t, beforeID, afterID)
		require.Equal(t, uint32(1), version)
		m0Exec(t, ctx, db, "alter table "+schema+".copy_oracle add column added_text varchar(8), algorithm=copy")
		var actual, expected int
		const query = "select ordinal_position from information_schema.columns where table_schema=? and table_name=? and column_name='added_text'"
		require.NoError(t, db.QueryRowContext(ctx, query, schema, "customer").Scan(&actual))
		require.NoError(t, db.QueryRowContext(ctx, query, schema, "copy_oracle").Scan(&expected))
		require.Equal(t, 7, expected)
		t.Logf("PUBLIC_ORDINAL metadata_only=%d copy=%d", actual, expected)
		require.Equal(t, expected, actual)
		m0Exec(t, ctx, db, "alter table "+schema+".customer add column added_num decimal(10,2)")
		m0Exec(t, ctx, db, "alter table "+schema+".copy_oracle add column added_num decimal(10,2), algorithm=copy")
		physicalAfter := publicOrdinalPhysicalColumns(t, ctx, db, schema)
		for name, identity := range physicalBefore {
			require.Equal(t, identity, physicalAfter[name], "physical identity changed: %s", name)
		}
		require.Equal(t, [2]int32{8, 7}, physicalAfter["added_text"])
		require.Equal(t, [2]int32{9, 8}, physicalAfter["added_num"])
		require.Equal(t, objectsBefore, m0ObjectHeaderProofs(t, ctx, db, cn, schema+".customer"))
		stmt, err := db.PrepareContext(ctx, "select ordinal_position from information_schema.columns where table_schema=? and table_name=? and column_name=?")
		require.NoError(t, err)
		defer stmt.Close()
		ordinal := func(connection *sql.DB, database, table, column string, want int) {
			t.Helper()
			var got int
			require.NoError(t, connection.QueryRowContext(ctx, "select ordinal_position from information_schema.columns where table_schema=? and table_name=? and column_name=?", database, table, column).Scan(&got))
			require.Equal(t, want, got)
		}
		for _, table := range []string{"customer", "copy_oracle"} {
			for i, name := range []string{"added_text", "added_num"} {
				var got int
				require.NoError(t, stmt.QueryRowContext(ctx, schema, table, name).Scan(&got))
				require.Equal(t, 7+i, got)
			}
		}
		var count int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from information_schema.columns where table_schema=? and table_name='customer' and ordinal_position in (7,8)", schema).Scan(&count))
		require.Equal(t, 2, count)
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+schema+".customer where added_text is null and added_num is null").Scan(&count))
		require.Equal(t, 1, count)

		m0Exec(t, ctx, db, "create snapshot m0_public_ordinal_snapshot for database "+schema)
		defer cleanupSQLIntegration(t, cn, "drop snapshot if exists m0_public_ordinal_snapshot")
		const child = "m0_public_ordinal_child"
		m0Exec(t, ctx, db, "create database "+child+" clone "+schema+" {snapshot='m0_public_ordinal_snapshot'}")
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+child)
		m0Exec(t, ctx, db, "drop snapshot m0_public_ordinal_snapshot")
		m0Exec(t, ctx, db, "alter table "+child+".customer add column child_text varchar(8)")
		ordinal(db, schema, "customer", "added_text", 7)
		ordinal(db, child, "customer", "added_text", 7)
		ordinal(db, child, "customer", "child_text", 9)
		m0Exec(t, ctx, db, "create table "+schema+".simple(a int primary key,b int)")
		m0Exec(t, ctx, db, "alter table "+schema+".simple add column added_text varchar(8)")
		ordinal(db, schema, "simple", "added_text", 3)

		m0Exec(t, ctx, db, "create account m0_ordinal_tenant admin_name='admin' identified by '111'")
		defer cleanupSQLIntegration(t, cn, "drop account if exists m0_ordinal_tenant")
		other, err := sql.Open("mysql", fmt.Sprintf("m0_ordinal_tenant#admin:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer other.Close()
		m0Exec(t, ctx, other, "create database "+schema)
		m0Exec(t, ctx, other, "create table "+schema+".customer(a int primary key,b int)")
		m0Exec(t, ctx, other, "alter table "+schema+".customer add column added_text varchar(8)")
		ordinal(other, schema, "customer", "added_text", 3)
		ordinal(db, schema, "customer", "added_text", 7)
		assertStoredSystemOrdinals(t, ctx, db, cn, schema, 7)
	})
}

func publicOrdinalPhysicalColumns(t *testing.T, ctx context.Context, db *sql.DB, schema string) map[string][2]int32 {
	t.Helper()
	rows, err := db.QueryContext(ctx, "select attname,attnum,attr_seqnum from mo_catalog.mo_columns where att_database=? and att_relname='customer' and attname<>? order by attnum", schema, catalog.Row_ID)
	require.NoError(t, err)
	defer rows.Close()
	result := make(map[string][2]int32)
	for rows.Next() {
		var name string
		var number, seqnum int32
		require.NoError(t, rows.Scan(&name, &number, &seqnum))
		result[name] = [2]int32{number, seqnum}
	}
	require.NoError(t, rows.Err())
	return result
}
