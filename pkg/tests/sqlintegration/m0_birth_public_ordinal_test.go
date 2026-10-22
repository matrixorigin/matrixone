//go:build mo_m0

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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// The shared regression owns the inherited NULL path. This oracle additionally
// requires admitted literal-false ADDs and compares the entire public schema
// against COPY while preserving the old physical identities and objects.
func TestM0BirthFalsePublicOrdinalsSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		closeAdmission := admitM0BirthSQL(t, cn)
		defer closeAdmission()
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		db := openM0SQL(t, cn)
		defer db.Close()
		const schema = "m0_birth_public_ordinal"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		m0Exec(t, ctx, db, "create database "+schema)
		for _, table := range []string{"customer", "copy_oracle"} {
			m0Exec(t, ctx, db, "create table "+schema+"."+table+m0RollbackCustomerSchema)
			m0Exec(t, ctx, db, "insert into "+schema+"."+table+" values(1,1,1,50000,100,10)")
			m0Exec(t, ctx, db, "select mo_ctl('dn','flush','"+schema+"."+table+"')")
		}
		beforeID, _ := m0TableGeneration(t, ctx, db, schema, "customer")
		physicalBefore := publicOrdinalPhysicalColumns(t, ctx, db, schema)
		objectsBefore := m0ObjectFingerprints(t, ctx, db, schema+".customer")
		for index, column := range []string{"cleaned", "cleaned2"} {
			m0Exec(t, ctx, db, "alter table "+schema+".customer add column "+column+" boolean default false")
			afterID, version := m0TableGeneration(t, ctx, db, schema, "customer")
			require.Equal(t, beforeID, afterID)
			require.Equal(t, uint32(index+1), version)
			m0Exec(t, ctx, db, "alter table "+schema+".copy_oracle add column "+column+" boolean default false, algorithm=copy")
			require.Equal(t, birthPublicColumns(t, ctx, db, schema, "copy_oracle"), birthPublicColumns(t, ctx, db, schema, "customer"))
			var ordinal int
			require.NoError(t, db.QueryRowContext(ctx, "select ordinal_position from information_schema.columns where table_schema=? and table_name='customer' and column_name=?", schema, column).Scan(&ordinal))
			require.Equal(t, 7+index, ordinal)
			physicalAfter := publicOrdinalPhysicalColumns(t, ctx, db, schema)
			for name, identity := range physicalBefore {
				require.Equal(t, identity, physicalAfter[name], "old physical identity: %s", name)
			}
			require.Equal(t, [2]int32{int32(8 + index), int32(7 + index)}, physicalAfter[column])
			require.Equal(t, objectsBefore, m0ObjectFingerprints(t, ctx, db, schema+".customer"))
			require.Equal(t, 1, m0Count(t, ctx, db, "select count(*) from "+schema+".customer where "+column+"=false"))
			t.Logf("BIRTH_PUBLIC_SCHEMA column=%s ordinal=%d physical=%v COPY_equal=true", column, ordinal, physicalAfter[column])
		}
	})
}

type birthPublicColumn struct {
	Name, Type, Nullable string
	Ordinal              int
	Default              sql.NullString
}

func birthPublicColumns(t *testing.T, ctx context.Context, db *sql.DB, schema, table string) []birthPublicColumn {
	t.Helper()
	rows, err := db.QueryContext(ctx, "select column_name,column_type,is_nullable,ordinal_position,column_default from information_schema.columns where table_schema=? and table_name=? order by ordinal_position", schema, table)
	require.NoError(t, err)
	defer rows.Close()
	var columns []birthPublicColumn
	for rows.Next() {
		var column birthPublicColumn
		require.NoError(t, rows.Scan(&column.Name, &column.Type, &column.Nullable, &column.Ordinal, &column.Default))
		columns = append(columns, column)
	}
	require.NoError(t, rows.Err())
	return columns
}
