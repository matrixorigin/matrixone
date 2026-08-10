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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue26226ViewDistinctUsesVisibleSetValue(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		dbConn, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer dbConn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		execSQLRequire(t, ctx, dbConn, "set role moadmin")
		require.Eventually(t, func() bool {
			return clusterservice.AllKnownCNsSupportViewMetadataRefresh(cn.ServiceID())
		}, time.Minute, 100*time.Millisecond)

		const db = "issue_26226"
		for _, stmt := range []string{
			"drop database if exists " + db,
			"create database " + db,
			"create table " + db + ".t (id int primary key, flags set('', 'a'))",
			"insert into " + db + ".t values (1, ''), (2, 1)",
			"create view " + db + ".v_raw as select flags from " + db + ".t where id = 2",
			"create view " + db + ".v as select distinct flags from " + db + ".t",
			"create view " + db + ".v_order as select id, flags from " + db + ".t order by flags",
			"create view " + db + ".v_order_derived as select id, flags from (select id, flags from " + db + ".t) d order by flags",
			"create view " + db + ".v_order_cte as with d as (select id, flags from " + db + ".t) select id, flags from d order by flags",
			"create view " + db + ".v_group as select flags from " + db + ".t group by flags",
			"create view " + db + ".v_derived as select flags from (select id, flags from " + db + ".t) d where id = 2",
			"create view " + db + ".v_cte as with d as (select id, flags from " + db + ".t) select flags from d where id = 2",
			"create view " + db + ".v_union as select flags from " + db + ".t union all select flags from " + db + ".t",
			"create view " + db + ".v_union_distinct as select flags from " + db + ".t union select flags from " + db + ".t",
			"create view " + db + ".v_recursive as with recursive d(flags) as (select flags from " + db + ".t union all select flags from d where false) select flags from d",
			"create table " + db + ".copied as select flags from " + db + ".v_raw",
			"create table " + db + ".copied_derived as select flags from " + db + ".v_derived",
			"create table " + db + ".copied_cte as select flags from " + db + ".v_cte",
			"create table " + db + ".copied_union as select flags from " + db + ".v_union",
			"create table " + db + ".copied_union_distinct as select flags from " + db + ".v_union_distinct",
			"create table " + db + ".copied_recursive as select flags from " + db + ".v_recursive",
			"create table " + db + ".inserted (flags set('', 'a'))",
			"insert into " + db + ".inserted select flags from " + db + ".v_raw",
			"create table " + db + ".expr_src (flags set('a', 'b'))",
			"insert into " + db + ".expr_src values ('a')",
			"create table " + db + ".expr_dst (flags set('a', 'b'))",
			"insert into " + db + ".expr_dst select concat(flags, ',b') from " + db + ".expr_src",
			"create table " + db + ".semantic_t (priority enum('low','medium','high'), flags set('', 'a', 'b'))",
			"insert into " + db + ".semantic_t values ('low', ''), ('medium', 1), ('high', 'a')",
			"create view " + db + ".v_semantic_group as select priority, flags from " + db + ".semantic_t group by priority, flags",
			"create view " + db + ".v_semantic_distinct as select distinct priority, flags from " + db + ".semantic_t",
			"create view " + db + ".v_semantic_derived as select priority, flags from (select distinct priority, flags from " + db + ".semantic_t) d",
			"create table " + db + ".defaults_src (e enum('low','medium','high') default 'medium', s set('', 'a', 'b') default 'a', n int default 7)",
			"create table " + db + ".defaults_direct as select e, s, n from " + db + ".defaults_src",
			"create table " + db + ".defaults_derived as select e, s, n from (select e, s, n from " + db + ".defaults_src) d",
			"create table " + db + ".defaults_cte as with d as (select e, s, n from " + db + ".defaults_src) select e, s, n from d",
			"create table " + db + ".defaults_union as select e, s, n from " + db + ".defaults_src union all select e, s, n from " + db + ".defaults_src",
		} {
			execSQLRequire(t, ctx, dbConn, stmt)
		}
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, dbConn, "drop database if exists "+db)
		}()

		var baseCount, viewCount int
		require.NoError(t, dbConn.QueryRowContext(ctx,
			"select count(*) from (select distinct flags from "+db+".t) d").Scan(&baseCount))
		require.NoError(t, dbConn.QueryRowContext(ctx,
			"select count(*) from "+db+".v").Scan(&viewCount))
		require.Equal(t, 1, baseCount)
		require.Equal(t, baseCount, viewCount)
		for _, test := range []struct {
			tableName string
			dataType  string
		}{
			{tableName: "v_raw", dataType: "set"},
			{tableName: "v", dataType: "set"},
			{tableName: "v_order", dataType: "set"},
			{tableName: "v_order_derived", dataType: "set"},
			{tableName: "v_order_cte", dataType: "set"},
			{tableName: "v_group", dataType: "set"},
			{tableName: "v_derived", dataType: "set"},
			{tableName: "v_cte", dataType: "set"},
			{tableName: "v_union", dataType: "varchar"},
			{tableName: "v_union_distinct", dataType: "varchar"},
			{tableName: "v_recursive", dataType: "varchar"},
			{tableName: "copied_derived", dataType: "set"},
			{tableName: "copied_cte", dataType: "set"},
			{tableName: "copied_union", dataType: "varchar"},
			{tableName: "copied_union_distinct", dataType: "varchar"},
			{tableName: "copied_recursive", dataType: "varchar"},
		} {
			var dataType string
			require.NoError(t, dbConn.QueryRowContext(ctx,
				"select data_type from information_schema.columns "+
					"where table_schema = ? and table_name = ? and column_name = 'flags'",
				db, test.tableName).Scan(&dataType))
			require.Equal(t, test.dataType, strings.ToLower(dataType), test.tableName)
		}

		for _, query := range []string{
			"select cast(flags as unsigned) from " + db + ".t where id = 2",
			"select cast(flags as unsigned) from " + db + ".v_raw",
			"select cast(flags as unsigned) from " + db + ".v_order where id = 2",
			"select cast(flags as unsigned) from " + db + ".v_order_derived where id = 2",
			"select cast(flags as unsigned) from " + db + ".v_order_cte where id = 2",
			"select cast(flags as unsigned) from " + db + ".v_derived",
			"select cast(flags as unsigned) from " + db + ".v_cte",
			"select cast(flags as unsigned) from " + db + ".copied",
			"select cast(flags as unsigned) from " + db + ".copied_derived",
			"select cast(flags as unsigned) from " + db + ".copied_cte",
			"select cast(flags as unsigned) from " + db + ".inserted",
		} {
			var bitmap uint64
			require.NoError(t, dbConn.QueryRowContext(ctx, query).Scan(&bitmap))
			require.Equal(t, uint64(1), bitmap, query)
		}
		for _, query := range []string{
			"select concat(flags, 'x') from " + db + ".v_derived",
			"select concat(flags, 'x') from " + db + ".v_cte",
		} {
			var visibleValue string
			require.NoError(t, dbConn.QueryRowContext(ctx, query).Scan(&visibleValue))
			require.Equal(t, "x", visibleValue, query)
		}

		var nestedBitmap uint64
		require.NoError(t, dbConn.QueryRowContext(ctx,
			"select cast(flags as unsigned) from "+db+".expr_dst").Scan(&nestedBitmap))
		require.Equal(t, uint64(3), nestedBitmap)

		for _, view := range []string{
			"v_semantic_group",
			"v_semantic_distinct",
			"v_semantic_derived",
		} {
			rows, err := dbConn.QueryContext(ctx,
				"select priority, cast(flags as unsigned) from "+db+"."+view+" order by priority")
			require.NoError(t, err, view)
			defer rows.Close()
			var actual [][2]any
			for rows.Next() {
				var priority string
				var flags uint64
				require.NoError(t, rows.Scan(&priority, &flags))
				actual = append(actual, [2]any{priority, flags})
			}
			require.NoError(t, rows.Err())
			require.Equal(t, [][2]any{{"low", uint64(0)}, {"medium", uint64(0)}, {"high", uint64(2)}}, actual, view)
		}

		for _, test := range []struct {
			tableName    string
			wantDefaults []string
		}{
			{tableName: "defaults_direct", wantDefaults: []string{"'medium'", "'a'", "7"}},
			{tableName: "defaults_derived", wantDefaults: []string{"", "", ""}},
			{tableName: "defaults_cte", wantDefaults: []string{"", "", ""}},
			{tableName: "defaults_union", wantDefaults: []string{"", "", ""}},
		} {
			rows, err := dbConn.QueryContext(ctx,
				"select column_default from information_schema.columns "+
					"where table_schema = ? and table_name = ? and column_name in ('e', 's', 'n') "+
					"order by ordinal_position",
				db, test.tableName)
			require.NoError(t, err, test.tableName)
			defer rows.Close()
			var actualDefaults []string
			for rows.Next() {
				var defaultValue sql.NullString
				require.NoError(t, rows.Scan(&defaultValue))
				actualDefaults = append(actualDefaults, defaultValue.String)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, test.wantDefaults, actualDefaults, test.tableName)

			var name, createSQL string
			require.NoError(t, dbConn.QueryRowContext(ctx,
				"show create table "+db+"."+test.tableName).Scan(&name, &createSQL))
			if test.tableName == "defaults_direct" {
				require.Contains(t, createSQL, "DEFAULT 'medium'")
				require.Contains(t, createSQL, "DEFAULT 'a'")
				require.Contains(t, createSQL, "DEFAULT 7")
			} else {
				require.NotContains(t, createSQL, "DEFAULT 'medium'")
				require.NotContains(t, createSQL, "DEFAULT 'a'")
				require.NotContains(t, createSQL, "DEFAULT 7")
			}
		}
	})
}
