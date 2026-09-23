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

func TestViewDescriptionPublicSQL(t *testing.T) {
	embed.RunSingleCNBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer func() { require.NoError(t, db.Close()) }()
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
		exec("drop table view_description_test.src")
		invalid, err := db.QueryContext(ctx, "desc view_description_test.v")
		if invalid != nil {
			require.NoError(t, invalid.Close())
		}
		require.Error(t, err)
		exec("create table view_description_test.src (x varchar(90), qty bigint not null default 11)")
		repaired := describe("desc view_description_test.v")
		require.Len(t, repaired, 2)
		require.Equal(t, "VARCHAR(90)", repaired[0][1].String)
		require.Equal(t, "11", repaired[1][4].String)
		require.NoError(t, db.QueryRowContext(ctx, "select character_maximum_length from information_schema.columns where table_schema='view_description_test' and table_name='v' and column_name='label'").Scan(&width))
		require.Equal(t, 90, width)
	})
}
