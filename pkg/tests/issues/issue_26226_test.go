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
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
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

		const db = "issue_26226"
		for _, stmt := range []string{
			"drop database if exists " + db,
			"create database " + db,
			"create table " + db + ".t (id int primary key, flags set('', 'a'))",
			"insert into " + db + ".t values (1, ''), (2, 1)",
			"create view " + db + ".v_raw as select flags from " + db + ".t where id = 2",
			"create view " + db + ".v as select distinct flags from " + db + ".t",
			"create table " + db + ".copied as select flags from " + db + ".v_raw",
			"create table " + db + ".inserted (flags set('', 'a'))",
			"insert into " + db + ".inserted select flags from " + db + ".v_raw",
			"create table " + db + ".expr_src (flags set('a', 'b'))",
			"insert into " + db + ".expr_src values ('a')",
			"create table " + db + ".expr_dst (flags set('a', 'b'))",
			"insert into " + db + ".expr_dst select concat(flags, ',b') from " + db + ".expr_src",
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

		for _, query := range []string{
			"select cast(flags as unsigned) from " + db + ".t where id = 2",
			"select cast(flags as unsigned) from " + db + ".v_raw",
			"select cast(flags as unsigned) from " + db + ".copied",
			"select cast(flags as unsigned) from " + db + ".inserted",
		} {
			var bitmap uint64
			require.NoError(t, dbConn.QueryRowContext(ctx, query).Scan(&bitmap))
			require.Equal(t, uint64(1), bitmap, query)
		}

		var nestedBitmap uint64
		require.NoError(t, dbConn.QueryRowContext(ctx,
			"select cast(flags as unsigned) from "+db+".expr_dst").Scan(&nestedBitmap))
		require.Equal(t, uint64(3), nestedBitmap)
	})
}
