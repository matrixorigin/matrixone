//go:build mo_m0

// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package sqlintegration

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// A physical hidden primary-key column must not consume a public column
// ordinal. Compare both the inherited NULL path and the false extension with
// the same engine's ordinary COPY result, without renumbering physical seqnums.
func TestM0AppendPublicOrdinals(t *testing.T) {
	for _, definition := range []string{"boolean", "boolean default false"} {
		t.Run(definition, func(t *testing.T) {
			runSQLIntegration(t, func(cluster embed.Cluster) {
				cn, err := cluster.GetCNService(0)
				require.NoError(t, err)
				if definition != "boolean" {
					closeAdmission := admitM0BirthSQL(t, cn)
					defer closeAdmission()
				}
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
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
				beforeID, _ := m0TableGeneration(t, ctx, db, schema, "customer")
				m0Exec(t, ctx, db, "alter table "+schema+".customer add column cleaned "+definition)
				afterID, version := m0TableGeneration(t, ctx, db, schema, "customer")
				require.Equal(t, beforeID, afterID)
				require.Equal(t, uint32(1), version)
				m0Exec(t, ctx, db, "alter table "+schema+".copy_oracle add column cleaned "+definition+", algorithm=copy")
				var actual, expected int
				const query = "select ordinal_position from information_schema.columns where table_schema=? and table_name=? and column_name='cleaned'"
				require.NoError(t, db.QueryRowContext(ctx, query, schema, "customer").Scan(&actual))
				require.NoError(t, db.QueryRowContext(ctx, query, schema, "copy_oracle").Scan(&expected))
				require.Equal(t, 7, expected)
				t.Logf("PUBLIC_ORDINAL definition=%q metadata_only=%d copy=%d", definition, actual, expected)
				require.Equal(t, expected, actual)
			})
		})
	}
}
