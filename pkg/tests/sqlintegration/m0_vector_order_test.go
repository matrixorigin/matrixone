//go:build mo_m0

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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// Low-level vector candidate pruning deliberately ignores NULL distances. It
// must not replace ordinary SQL NULL ordering. Use a CASE expression as an
// independent scalar-sort oracle, with a unique second key for stable LIMIT.
func TestM0NullableVectorOrderSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		const schema = "m0_vector_order"
		db, table := m0PrepareRollbackTable(t, ctx, cn, schema)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		m0Exec(t, ctx, db, "alter table "+table+" add column embedding vecf32(2)")
		require.Equal(t, uint32(1), m0ReadEvolution(t, ctx, db, schema).LayoutGeneration)
		// Query the real metadata consumer before mutation introduces tombstones.
		var physicalRows, nullRows int
		require.NoError(t, db.QueryRowContext(ctx, "select sum(rows_cnt),sum(null_cnt) from metadata_scan('"+table+"','embedding') g").Scan(&physicalRows, &nullRows))
		require.Equal(t, 3, physicalRows)
		require.Equal(t, physicalRows, nullRows)
		m0Exec(t, ctx, db, "update "+table+" set embedding='[2,0]' where c_id=1")
		read := func(statement string) []int {
			t.Helper()
			rows, err := db.QueryContext(ctx, statement)
			require.NoError(t, err)
			defer rows.Close()
			var out []int
			for rows.Next() {
				var id int
				require.NoError(t, rows.Scan(&id))
				out = append(out, id)
			}
			require.NoError(t, rows.Err())
			return out
		}
		scalar := read("select c_id from " + table + " order by case when embedding is null then -1 else l2_distance(embedding,'[0,0]') end,c_id limit 2")
		require.Equal(t, []int{2, 3}, scalar)
		require.Equal(t, scalar, read("select c_id from "+table+" order by l2_distance(embedding,'[0,0]'),c_id limit 2"))
		require.Equal(t, []int{1, 2}, read("select c_id from "+table+" order by l2_distance(embedding,'[0,0]') desc,c_id limit 2"))
		require.Equal(t, []int{1}, read("select c_id from "+table+" where embedding is not null order by l2_distance(embedding,'[0,0]') limit 2"))
		require.Equal(t, 2, m0Count(t, ctx, db, "select count(*) from "+table+" where embedding is null"))
	})
}
