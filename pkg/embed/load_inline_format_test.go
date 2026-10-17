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

package embed

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

func TestLoadInlineFormatOverMySQLProtocol(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		db := openIcebergTestDB(t, c)
		defer func() { require.NoError(t, db.Close()) }()
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		const database = "load_inline_format_test"

		// Repeat on the same instance to check teardown and reuse after errors.
		for pass := 0; pass < 2; pass++ {
			t.Run(fmt.Sprintf("pass%d", pass), func(t *testing.T) {
				_, err := db.ExecContext(ctx, "create database "+database)
				require.NoError(t, err)
				defer func() {
					cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cleanupCancel()
					_, err := db.ExecContext(cleanupCtx, "drop database "+database)
					require.NoError(t, err)
				}()
				exec := func(statement string) {
					t.Helper()
					_, err := db.ExecContext(ctx, statement)
					require.NoError(t, err, statement)
				}
				exec("create table " + database + ".t(id bigint primary key, vc varchar(16))")
				for i, format := range []string{"csv", "CSV", "Csv", "cSv"} {
					exec(fmt.Sprintf("load data inline format='%s', data='%d,CsV' into table %s.t fields terminated by ','", format, i+1, database))
				}
				exec(`load data inline format='JSONLINE', data='{"id":5,"vc":"ObJeCt"}', jsontype='object' into table ` + database + ".t")
				exec(`load data inline format='JsonLine', data='[6,"ArRaY"]', jsontype='array' into table ` + database + ".t")

				loadError := func(format string) *mysql.MySQLError {
					t.Helper()
					_, err := db.ExecContext(ctx, fmt.Sprintf("load data inline format='%s', data='99,rejected' into table %s.t", format, database))
					require.Error(t, err, "format=%q", format)
					var mysqlErr *mysql.MySQLError
					require.ErrorAs(t, err, &mysqlErr)
					return mysqlErr
				}
				for _, pair := range [][2]string{{"jsonline", "JSONLINE"}, {"arrow", "ARROW"}} {
					lower, upper := loadError(pair[0]), loadError(pair[1])
					require.Equal(t, uint16(20105), lower.Number)
					require.Equal(t, lower.Number, upper.Number)
					require.Equal(t, lower.Message, upper.Message)
					if pair[0] == "arrow" {
						require.Contains(t, lower.Message, "supported only by file-backed LOAD DATA")
					} else {
						require.Contains(t, lower.Message, "the jsonline format '' is not supported")
					}
				}
				for _, format := range []string{"foo", "JSON", "", " ", "CSV ", " CSV"} {
					require.Equal(t, uint16(20102), loadError(format).Number, "format=%q", format)
				}

				// Exact rows also prove failed loads did not partially insert data.
				rows, err := db.QueryContext(ctx, "select id, vc from "+database+".t order by id")
				require.NoError(t, err)
				defer func() { require.NoError(t, rows.Close()) }()
				for i, want := range []string{"CsV", "CsV", "CsV", "CsV", "ObJeCt", "ArRaY"} {
					require.True(t, rows.Next())
					var id int
					var value string
					require.NoError(t, rows.Scan(&id, &value))
					require.Equal(t, i+1, id)
					require.Equal(t, want, value)
				}
				require.False(t, rows.Next())
				require.NoError(t, rows.Err())
			})
			var remaining int
			require.NoError(t, db.QueryRowContext(ctx,
				"select count(*) from information_schema.schemata where schema_name = ?", database).Scan(&remaining))
			require.Zero(t, remaining, "database teardown after pass %d", pass)
		}
	})
}
