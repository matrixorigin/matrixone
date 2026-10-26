// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestPublicOrdinalsStoredSystemViewSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const schema = "public_ordinal_stored"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		for _, statement := range []string{
			"create database " + schema,
			"create table " + schema + ".customer(a int,b int,primary key(a,b))",
			"insert into " + schema + ".customer values(1,2)",
			"alter table " + schema + ".customer add column added_text varchar(8), algorithm=copy",
			"create view " + schema + ".columns as select 99 as ordinal_position",
			"create view " + schema + ".customer_view as select b,a from " + schema + ".customer",
		} {
			execSQLRequire(t, ctx, db, statement)
		}
		assertStoredSystemOrdinals(t, ctx, db, cn, schema, 3)
		var ordinal int
		require.NoError(t, db.QueryRowContext(ctx, "select ordinal_position from "+schema+".columns").Scan(&ordinal))
		require.Equal(t, 99, ordinal)
		require.NoError(t, db.QueryRowContext(ctx, "select ordinal_position from information_schema.columns where table_schema=? and table_name='customer_view' and column_name='a'", schema).Scan(&ordinal))
		require.Equal(t, 2, ordinal)
	})
}

func TestPublicOrdinalsKeepViewMetadataFiltersSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const schema = "public_ordinal_filter"
		db.SetMaxOpenConns(1)
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		for _, statement := range []string{
			"create database " + schema,
			"create table " + schema + ".t(a int,b int)",
			"create view " + schema + ".explicit_union_v(public_a,public_b) as (select a,b from " + schema + ".t union select a,b from " + schema + ".t)",
			"create view " + schema + ".explicit_union_replay_v as select public_a,public_b from " + schema + ".explicit_union_v",
			"use " + schema,
			"create view adjacent_block_comment_v/* migration-generated view */as select a from t;",
		} {
			execSQLRequire(t, ctx, db, statement)
		}
		// Preserve this existing parser rejection when the problematic View
		// itself is requested. It must not poison metadata for unrelated Views.
		var rejected int
		err = db.QueryRowContext(ctx, "select ordinal_position from information_schema.columns where table_schema=? and table_name='adjacent_block_comment_v'", schema).Scan(&rejected)
		require.ErrorContains(t, err, "syntax error")
		rows, err := db.QueryContext(ctx, "select table_name,column_name,ordinal_position from information_schema.columns where table_schema=? and table_name in ('explicit_union_v','explicit_union_replay_v') order by table_name,ordinal_position", schema)
		require.NoError(t, err)
		defer rows.Close()
		var got []string
		for rows.Next() {
			var table, column string
			var ordinal int
			require.NoError(t, rows.Scan(&table, &column, &ordinal))
			got = append(got, fmt.Sprintf("%s:%s:%d", table, column, ordinal))
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []string{"explicit_union_replay_v:public_a:1", "explicit_union_replay_v:public_b:2", "explicit_union_v:public_a:1", "explicit_union_v:public_b:2"}, got)
	})
}

func assertStoredSystemOrdinals(t *testing.T, ctx context.Context, db *sql.DB, cn embed.ServiceOperator, schema string, want int) {
	t.Helper()
	// Simulate an already persisted upgrade-era definition, not a freshly
	// bootstrapped current view. Always restore before releasing the fixture.
	install := func(ctx context.Context, definition string) error {
		return testutils.GetSQLExecutor(cn).ExecTxn(ctx, func(tx executor.TxnExecutor) error {
			for _, statement := range []string{"drop view if exists information_schema.COLUMNS", definition} {
				res, err := tx.Exec(statement, versions.UpgradeStatementOption(0))
				if err != nil {
					return err
				}
				res.Close()
			}
			return nil
		}, executor.Options{}.WithAccountID(0).WithWaitCommittedLogApplied())
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := install(cleanupCtx, sysview.InformationSchemaColumnsDDL); err != nil {
			t.Errorf("restore system COLUMNS definition: %v", err)
		}
	}()
	for _, definition := range []string{sysview.InformationSchemaColumnsV100DDL(), sysview.InformationSchemaColumnsV46DDL, sysview.InformationSchemaColumnsDDL} {
		require.NoError(t, install(ctx, definition))
		var persisted string
		require.NoError(t, db.QueryRowContext(ctx, "select viewdef from mo_catalog.mo_tables where account_id=0 and reldatabase='information_schema' and relname='columns'").Scan(&persisted))
		var view struct{ Stmt string }
		require.NoError(t, json.Unmarshal([]byte(persisted), &view))
		require.Equal(t, definition, view.Stmt)
		var ordinal int
		require.NoError(t, db.QueryRowContext(ctx, "select ordinal_position from information_schema.columns where table_schema=? and table_name='customer' and column_name='added_text'", schema).Scan(&ordinal))
		require.Equal(t, want, ordinal)
	}
}
