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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Reuse the subscription fixture's ordinary service and tenant. One row and two
// widths distinguish the snapshot namespace from the caller and current data.
func testSnapshotMetadataNamespace(t *testing.T, sys, tenant *sql.DB) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	exec := func(db *sql.DB, query string) {
		t.Helper()
		_, err := db.ExecContext(ctx, query)
		require.NoError(t, err, query)
	}
	exec(tenant, "create database snapshot_metadata_namespace")
	defer cleanupViewDescriptionSQL(t, tenant, "drop database snapshot_metadata_namespace")
	exec(tenant, "create table snapshot_metadata_namespace.src(x varchar(8))")
	exec(tenant, "insert into snapshot_metadata_namespace.src values ('old')")
	exec(tenant, "create index snapshot_metadata_ns_idx on snapshot_metadata_namespace.src (x)")
	exec(tenant, "create view snapshot_metadata_namespace.v as select x from snapshot_metadata_namespace.src")
	exec(sys, "create snapshot snapshot_metadata_namespace_history for account view_description_sub")
	defer cleanupViewDescriptionSQL(t, sys, "drop snapshot snapshot_metadata_namespace_history")
	exec(sys, "create snapshot snapshot_metadata_namespace_private for account view_description_sub")
	defer cleanupViewDescriptionSQL(t, sys, "drop snapshot snapshot_metadata_namespace_private")
	exec(tenant, "alter table snapshot_metadata_namespace.src modify x varchar(37)")
	exec(tenant, "update snapshot_metadata_namespace.src set x='new'")
	exec(tenant, "create snapshot snapshot_metadata_namespace_history for account")
	defer cleanupViewDescriptionSQL(t, tenant, "drop snapshot snapshot_metadata_namespace_history")

	var owner uint32
	require.NoError(t, tenant.QueryRowContext(ctx, "select current_account_id()").Scan(&owner))
	// The following read-only checks should not inherit setup's spent budget.
	cancel()
	for _, tc := range []struct {
		name             string
		db               *sql.DB
		value            string
		width, principal int
	}{
		{"system target", sys, "old", 8, 0},
		{"tenant own same-name snapshot", tenant, "new", 37, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
			defer cancel()
			var catalogTables int
			require.NoError(t, tc.db.QueryRowContext(ctx,
				"select count(*) from mo_catalog.mo_tables {snapshot='snapshot_metadata_namespace_history'} "+
					"where account_id=? and reldatabase='snapshot_metadata_namespace'", owner).Scan(&catalogTables))
			require.Greater(t, catalogTables, 2, "the snapshot must actually contain a physical index child")
			var visible string
			require.NoError(t, tc.db.QueryRowContext(ctx,
				"select group_concat(distinct table_name order by table_name) from information_schema.columns "+
					"{snapshot='snapshot_metadata_namespace_history'} where table_schema='snapshot_metadata_namespace'").Scan(&visible))
			require.Equal(t, "src,v", visible, "changing namespace must not expose physical index columns")
			for _, object := range []string{"src", "v"} {
				var value string
				require.NoError(t, tc.db.QueryRowContext(ctx, "select x from snapshot_metadata_namespace."+object+" {snapshot='snapshot_metadata_namespace_history'}").Scan(&value))
				require.Equal(t, tc.value, value)
				query := "select count(*), coalesce(max(character_maximum_length),0), current_account_id() from information_schema.columns {snapshot='snapshot_metadata_namespace_history'} where table_schema='snapshot_metadata_namespace' and table_name='" + object + "' and column_name='x'"
				check := func(row *sql.Row) {
					t.Helper()
					var count, width, principal int
					require.NoError(t, row.Scan(&count, &width, &principal))
					require.Equal(t, 1, count)
					require.Equal(t, tc.width, width)
					if tc.principal == 0 {
						require.Zero(t, principal)
					} else {
						require.NotZero(t, principal)
					}
				}
				check(tc.db.QueryRowContext(ctx, query))
				func() {
					prepared, err := tc.db.PrepareContext(ctx, query)
					require.NoError(t, err)
					defer prepared.Close()
					check(prepared.QueryRowContext(ctx))
					check(prepared.QueryRowContext(ctx))
				}()
			}
		})
	}
	var count int
	negativeCtx, negativeCancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer negativeCancel()
	err := tenant.QueryRowContext(negativeCtx, "select count(*) from information_schema.columns {snapshot='snapshot_metadata_namespace_private'}").Scan(&count)
	require.ErrorContains(t, err, "snapshot", "a snapshot name owned by sys is not a tenant capability")
}
