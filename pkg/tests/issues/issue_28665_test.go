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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue28665WarningRowsFollowInputOrder(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)

		name := testutils.GetDatabaseName(t)
		execSQLRequire(t, ctx, db, "create database `"+name+"`")
		defer execSQLMaybe(t, context.Background(), db, "drop database if exists `"+name+"`")
		execSQLRequire(t, ctx, db, "use `"+name+"`")
		execSQLRequire(t, ctx, db,
			"create table src (g int, id int, v varchar(20), primary key (g, id))")
		execSQLRequire(t, ctx, db, "insert into src values "+
			"(3,1,'p'), (3,2,'q'), (3,3,'r'), "+
			"(1,1,'aa'), (1,2,'bbb'), (1,3,'cccc'), "+
			"(2,1,'x'), (2,2,'yy'), (2,3,'zzz')")
		execSQLRequire(t, ctx, db, "set session group_concat_max_len = 4")
		rows, err := db.QueryContext(ctx, "select g, group_concat(v order by id separator '|') "+
			"from src group by g order by g")
		require.NoError(t, err)
		var results []string
		for rows.Next() {
			var group int
			var value sql.NullString
			require.NoError(t, rows.Scan(&group, &value))
			results = append(results, fmt.Sprintf("%d=%s", group, value.String))
		}
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())

		warnings, err := db.QueryContext(ctx, "show warnings")
		require.NoError(t, err)
		var messages []string
		for warnings.Next() {
			var level, code, message string
			require.NoError(t, warnings.Scan(&level, &code, &message))
			messages = append(messages, message)
		}
		require.NoError(t, warnings.Close())
		require.NoError(t, warnings.Err())
		require.Equal(t, []string{"1=aa|b", "2=x|yy", "3=p|q|"}, results)
		require.Equal(t, []string{
			"Row 2 was cut by GROUP_CONCAT()",
			"Row 5 was cut by GROUP_CONCAT()",
			"Row 8 was cut by GROUP_CONCAT()",
		}, messages)

		rows, err = db.QueryContext(ctx, "select g, group_concat(v order by id separator '|') from src group by g order by g desc")
		require.NoError(t, err)
		results = results[:0]
		for rows.Next() {
			var group int
			var value sql.NullString
			require.NoError(t, rows.Scan(&group, &value))
			results = append(results, fmt.Sprintf("%d=%s", group, value.String))
		}
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
		warnings, err = db.QueryContext(ctx, "show warnings")
		require.NoError(t, err)
		messages = messages[:0]
		for warnings.Next() {
			var level, code, message string
			require.NoError(t, warnings.Scan(&level, &code, &message))
			messages = append(messages, message)
		}
		require.NoError(t, warnings.Close())
		require.NoError(t, warnings.Err())
		require.Equal(t, []string{"3=p|q|", "2=x|yy", "1=aa|b"}, results)
		require.Equal(t, []string{
			"Row 2 was cut by GROUP_CONCAT()",
			"Row 5 was cut by GROUP_CONCAT()",
			"Row 8 was cut by GROUP_CONCAT()",
		}, messages)

		rows, err = db.QueryContext(ctx, "select g, group_concat(v order by id separator '|') "+
			"from src group by g order by g limit 1")
		require.NoError(t, err)
		results = results[:0]
		for rows.Next() {
			var group int
			var value sql.NullString
			require.NoError(t, rows.Scan(&group, &value))
			results = append(results, fmt.Sprintf("%d=%s", group, value.String))
		}
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
		warnings, err = db.QueryContext(ctx, "show warnings")
		require.NoError(t, err)
		messages = messages[:0]
		for warnings.Next() {
			var level, code, message string
			require.NoError(t, warnings.Scan(&level, &code, &message))
			messages = append(messages, message)
		}
		require.NoError(t, warnings.Close())
		require.NoError(t, warnings.Err())
		require.Equal(t, []string{"1=aa|b"}, results)
		require.Equal(t, []string{
			"Row 2 was cut by GROUP_CONCAT()",
			"Row 5 was cut by GROUP_CONCAT()",
			"Row 8 was cut by GROUP_CONCAT()",
		}, messages)

		execSQLRequire(t, ctx, db, "set session group_concat_max_len = 100")
		rows, err = db.QueryContext(ctx, "select g, group_concat(v order by id separator '|') from src group by g order by g")
		require.NoError(t, err)
		for rows.Next() {
			var group int
			var value sql.NullString
			require.NoError(t, rows.Scan(&group, &value))
			require.Equal(t, map[int]string{
				1: "aa|bbb|cccc",
				2: "x|yy|zzz",
				3: "p|q|r",
			}[group], value.String)
		}
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
		warnings, err = db.QueryContext(ctx, "show warnings")
		require.NoError(t, err)
		require.False(t, warnings.Next())
		require.NoError(t, warnings.Close())
		require.NoError(t, warnings.Err())

		execSQLRequire(t, ctx, db,
			"create table distinct_src (g int, id int, v varchar(20), primary key (g, id))")
		execSQLRequire(t, ctx, db, "insert into distinct_src values "+
			"(1,1,'aa'), (1,2,'aa'), (1,3,'b'), (1,4,NULL), (1,5,'c'), "+
			"(2,1,'x'), (2,2,NULL), (2,3,'yy'), (2,4,'z')")
		execSQLRequire(t, ctx, db, "set session group_concat_max_len = 4")
		rows, err = db.QueryContext(ctx,
			"select g, group_concat(distinct v separator '|') from distinct_src group by g order by g")
		require.NoError(t, err)
		results = results[:0]
		for rows.Next() {
			var group int
			var value sql.NullString
			require.NoError(t, rows.Scan(&group, &value))
			results = append(results, fmt.Sprintf("%d=%s", group, value.String))
		}
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
		warnings, err = db.QueryContext(ctx, "show warnings")
		require.NoError(t, err)
		messages = messages[:0]
		for warnings.Next() {
			var level, code, message string
			require.NoError(t, warnings.Scan(&level, &code, &message))
			messages = append(messages, message)
		}
		require.NoError(t, warnings.Close())
		require.NoError(t, warnings.Err())
		require.Equal(t, []string{"1=aa|b", "2=x|yy"}, results)
		require.Equal(t, []string{
			"Row 3 was cut by GROUP_CONCAT()",
			"Row 8 was cut by GROUP_CONCAT()",
		}, messages)
	})
}
