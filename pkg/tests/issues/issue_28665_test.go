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
		queryResults := func(query string) (results []string, err error) {
			rows, queryErr := db.QueryContext(ctx, query)
			if queryErr != nil {
				return nil, queryErr
			}
			defer func() {
				if closeErr := rows.Close(); err == nil {
					err = closeErr
				}
			}()
			for rows.Next() {
				var group int
				var value sql.NullString
				if err = rows.Scan(&group, &value); err != nil {
					return nil, err
				}
				results = append(results, fmt.Sprintf("%d=%s", group, value.String))
			}
			if err = rows.Err(); err != nil {
				return nil, err
			}
			return results, nil
		}
		queryWarnings := func() (messages []string, err error) {
			rows, queryErr := db.QueryContext(ctx, "show warnings")
			if queryErr != nil {
				return nil, queryErr
			}
			defer func() {
				if closeErr := rows.Close(); err == nil {
					err = closeErr
				}
			}()
			for rows.Next() {
				var level, code, message string
				if err = rows.Scan(&level, &code, &message); err != nil {
					return nil, err
				}
				messages = append(messages, message)
			}
			if err = rows.Err(); err != nil {
				return nil, err
			}
			return messages, nil
		}

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
		results, err := queryResults("select g, group_concat(v order by id separator '|') " +
			"from src group by g order by g")
		require.NoError(t, err)
		messages, err := queryWarnings()
		require.NoError(t, err)
		require.Equal(t, []string{"1=aa|b", "2=x|yy", "3=p|q|"}, results)
		require.Equal(t, []string{
			"Row 2 was cut by GROUP_CONCAT()",
			"Row 5 was cut by GROUP_CONCAT()",
			"Row 8 was cut by GROUP_CONCAT()",
		}, messages)

		results, err = queryResults("select g, group_concat(v order by id separator '|') from src group by g order by g desc")
		require.NoError(t, err)
		messages, err = queryWarnings()
		require.NoError(t, err)
		require.Equal(t, []string{"3=p|q|", "2=x|yy", "1=aa|b"}, results)
		require.Equal(t, []string{
			"Row 2 was cut by GROUP_CONCAT()",
			"Row 5 was cut by GROUP_CONCAT()",
			"Row 8 was cut by GROUP_CONCAT()",
		}, messages)

		results, err = queryResults("select g, group_concat(v order by id separator '|') " +
			"from src group by g order by g limit 1")
		require.NoError(t, err)
		messages, err = queryWarnings()
		require.NoError(t, err)
		require.Equal(t, []string{"1=aa|b"}, results)
		require.Equal(t, []string{
			"Row 2 was cut by GROUP_CONCAT()",
			"Row 5 was cut by GROUP_CONCAT()",
			"Row 8 was cut by GROUP_CONCAT()",
		}, messages)

		execSQLRequire(t, ctx, db, "set session group_concat_max_len = 100")
		results, err = queryResults("select g, group_concat(v order by id separator '|') from src group by g order by g")
		require.NoError(t, err)
		for _, result := range results {
			var group int
			var value string
			_, err = fmt.Sscanf(result, "%d=%s", &group, &value)
			require.NoError(t, err)
			require.Equal(t, map[int]string{
				1: "aa|bbb|cccc",
				2: "x|yy|zzz",
				3: "p|q|r",
			}[group], value)
		}
		messages, err = queryWarnings()
		require.NoError(t, err)
		require.Empty(t, messages)

		execSQLRequire(t, ctx, db,
			"create table distinct_src (g int, id int, v varchar(20), primary key (g, id))")
		execSQLRequire(t, ctx, db, "insert into distinct_src values "+
			"(1,1,'aa'), (1,2,'aa'), (1,3,'b'), (1,4,NULL), (1,5,'c'), "+
			"(2,1,'x'), (2,2,NULL), (2,3,'yy'), (2,4,'z')")
		execSQLRequire(t, ctx, db, "set session group_concat_max_len = 4")
		results, err = queryResults(
			"select g, group_concat(distinct v separator '|') from distinct_src group by g order by g")
		require.NoError(t, err)
		messages, err = queryWarnings()
		require.NoError(t, err)
		require.Equal(t, []string{"1=aa|b", "2=x|yy"}, results)
		require.Equal(t, []string{
			"Row 3 was cut by GROUP_CONCAT()",
			"Row 8 was cut by GROUP_CONCAT()",
		}, messages)
	})
}
