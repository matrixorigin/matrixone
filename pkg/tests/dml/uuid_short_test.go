// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dml

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestUUIDShortSQL(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		seen := map[uint64]bool{}
		check := func(id uint64) {
			require.NotZero(t, id)
			require.False(t, seen[id], "duplicate UUID_SHORT")
			seen[id] = true
		}
		for _, index := range []int{0, 1} {
			db := openRetestSQLDBForCN(t, c, index)
			func() {
				defer db.Close()
				var maximum uint64
				require.NoError(t, db.QueryRowContext(ctx, "select cast('18446744073709551615' as unsigned)").Scan(&maximum))
				require.Equal(t, uint64(18446744073709551615), maximum)
				unsigned, err := db.PrepareContext(ctx, "select cast(? as unsigned)")
				require.NoError(t, err)
				defer unsigned.Close()
				require.NoError(t, unsigned.QueryRowContext(ctx, "18446744073709551615").Scan(&maximum))
				require.Equal(t, uint64(18446744073709551615), maximum)
				stmt, err := db.PrepareContext(ctx, "select uuid_short(),uuid_short()")
				require.NoError(t, err)
				defer stmt.Close()
				for range 2 {
					var a, b uint64
					require.NoError(t, stmt.QueryRowContext(ctx).Scan(&a, &b))
					check(a)
					check(b)
				}
				rows, err := db.QueryContext(ctx, "select uuid_short() from (select 1 as n union all select 2) t")
				require.NoError(t, err)
				defer rows.Close()
				count := 0
				for rows.Next() {
					var id uint64
					require.NoError(t, rows.Scan(&id))
					check(id)
					count++
				}
				require.NoError(t, rows.Err())
				require.NoError(t, rows.Close())
				require.Equal(t, 2, count)
				tx, err := db.BeginTx(ctx, nil)
				require.NoError(t, err)
				var id uint64
				require.NoError(t, tx.QueryRowContext(ctx, "select uuid_short()").Scan(&id))
				check(id)
				require.NoError(t, tx.Rollback())
				require.NoError(t, db.QueryRowContext(ctx, "select uuid_short()").Scan(&id))
				check(id)
				require.Error(t, db.QueryRowContext(ctx, "select uuid_short(1)").Scan(&id))
				require.NoError(t, db.QueryRowContext(ctx, "select uuid_short()").Scan(&id))
				check(id)
			}()
		}
		db := openRetestSQLDB(t, c)
		defer db.Close()
		rows := objectio.BlockMaxRows + 1
		var total, distinct int
		require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf("select count(*),count(distinct uuid_short()) from generate_series(1,%d) g", rows)).Scan(&total, &distinct))
		require.Equal(t, rows, total)
		require.Equal(t, rows, distinct)
		execSQLDB(t, ctx, db, "create database uuid_short_metadata")
		defer cleanupTestDatabases(t, db, "uuid_short_metadata")
		execSQLDB(t, ctx, db, "create table uuid_short_metadata.t as select uuid_short() as id")
		var typ string
		require.NoError(t, db.QueryRowContext(ctx, "select column_type from information_schema.columns where table_schema='uuid_short_metadata' and table_name='t' and column_name='id'").Scan(&typ))
		require.Equal(t, "BIGINT UNSIGNED", typ)
	})
}
