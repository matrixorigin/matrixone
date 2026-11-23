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

package isolated

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestCatalogMetadataFunctions(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(1))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/metadata_functions", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	admin, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	require.NoError(t, waitSystemBootstrap(ctx, admin))
	execSQLRequire(t, ctx, admin, "create database metadata_functions")

	const width = 256
	columns := make([]string, width)
	for i := range columns {
		columns[i] = fmt.Sprintf("col%d int default %d", i, i)
	}
	execSQLRequire(t, ctx, db, "create table wide ("+strings.Join(columns, ",")+")")
	const extremaQuery = "select mo_table_col_min(att_database, att_relname, upper(attname)), " +
		"mo_table_col_max(att_database, att_relname, attname) from mo_catalog.mo_columns " +
		"where att_database='metadata_functions' and att_relname='wide' " +
		"and attname like 'col%' order by attnum"
	checkExtrema := func(empty bool) {
		t.Helper()
		rows, err := db.QueryContext(ctx, extremaQuery)
		require.NoError(t, err)
		defer rows.Close()
		var i int
		for rows.Next() {
			var lo, hi sql.NullInt64
			require.NoError(t, rows.Scan(&lo, &hi))
			if empty {
				require.False(t, lo.Valid)
				require.False(t, hi.Valid)
			} else {
				require.Equal(t, sql.NullInt64{Int64: int64(i), Valid: true}, lo)
				require.Equal(t, sql.NullInt64{Int64: int64(i + width), Valid: true}, hi)
			}
			i++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, width, i)
	}
	checkExtrema(true)
	values := make([]string, width)
	for i := range values {
		values[i] = fmt.Sprint(i + width)
	}
	execSQLRequire(t, ctx, db, "insert into wide (col0) values (0)")
	execSQLRequire(t, ctx, db, "insert into wide values ("+strings.Join(values, ",")+")")
	execSQLRequire(t, ctx, db, "select mo_ctl('dn','flush','metadata_functions.wide')")
	checkExtrema(false)
	var absent sql.NullString
	require.NoError(t, db.QueryRowContext(ctx, "select mo_table_col_max('metadata_functions','wide','absent')").Scan(&absent))
	require.False(t, absent.Valid)

	execSQLRequire(t, ctx, db, "create sequence seq start with 42")
	var value string
	require.ErrorContains(t, db.QueryRowContext(ctx, "select nextval('wide')").Scan(&value), "Table input is not a sequence")
	const count = 8192
	rows, err := db.QueryContext(ctx, "select nextval('seq') from generate_series(1,8192) g")
	require.NoError(t, err)
	defer rows.Close()
	var i int
	for rows.Next() {
		var got int
		require.NoError(t, rows.Scan(&got))
		require.Equal(t, 42+i, got)
		i++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, count, i)
}
