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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestJSONValuePublishedInputSources(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(query string) { t.Helper(); _, err := conn.ExecContext(ctx, query); require.NoError(t, err) }
		exec("create database json_value_reader_sources")
		defer cleanupSQLIntegration(t, cn, "drop database if exists json_value_reader_sources")
		exec("use json_value_reader_sources")
		exec(`create table docs (id bigint primary key, doc json)`)
		exec(`insert into docs values (1, '{"keep":1}'), (2, '{}'), (3, '{"keep":"bad"}')`)
		wantText := map[int64]sql.NullString{1: {String: "1", Valid: true}, 2: {}, 3: {String: "bad", Valid: true}, 4: {String: "4", Valid: true}}
		wantNumber := map[int64]int64{1: 1, 2: 7, 3: 9, 4: 4}
		type queryer interface {
			QueryContext(context.Context, string, ...any) (*sql.Rows, error)
		}
		check := func(input queryer, count int, filtered bool) {
			t.Helper()
			query := `select id, json_value(doc, '$.keep'), json_value(doc, '$.keep' returning signed default 7 on empty default 9 on error), json_value(json_object('keep', id), '$.keep' returning signed) from docs`
			if filtered {
				query += ` where id % 2 = 0`
			}
			rows, err := input.QueryContext(ctx, query)
			require.NoError(t, err)
			defer rows.Close()
			seen := make(map[int64]bool)
			for rows.Next() {
				var id, number, constructed int64
				var text sql.NullString
				require.NoError(t, rows.Scan(&id, &text, &number, &constructed))
				require.False(t, seen[id])
				seen[id] = true
				require.Equal(t, wantText[id], text)
				require.Equal(t, wantNumber[id], number)
				require.Equal(t, id, constructed)
				if filtered {
					require.Zero(t, id%2)
				}
			}
			require.NoError(t, rows.Err())
			require.Len(t, seen, count)
		}
		check(conn, 3, false)
		rows, err := conn.QueryContext(ctx, `select id,json_value(json_object('keep',5),if(id%2=0,'$.missing','$.keep')),json_value(json_array(5),if(id%2=0,'$[1]','$[0]') returning signed default 7 on empty),json_value(cast(concat('{"keep":',id,'}') as json),'$.keep' returning signed) from docs order by id`)
		require.NoError(t, err)
		func() {
			defer rows.Close()
			for _, wantID := range []int64{1, 2, 3} {
				require.True(t, rows.Next())
				var id, number, castNumber int64
				var text sql.NullString
				require.NoError(t, rows.Scan(&id, &text, &number, &castNumber))
				require.Equal(t, wantID, id)
				require.Equal(t, id, castNumber)
				if id%2 == 0 {
					require.False(t, text.Valid)
					require.Equal(t, int64(7), number)
				} else {
					require.Equal(t, sql.NullString{String: "5", Valid: true}, text)
					require.Equal(t, int64(5), number)
				}
			}
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		}()
		constantPrepared, err := conn.PrepareContext(ctx, `select json_value(json_object('keep', ?),'$.keep' returning signed)`)
		require.NoError(t, err)
		func() {
			defer constantPrepared.Close()
			for _, want := range []int64{5, 9} {
				var actual int64
				require.NoError(t, constantPrepared.QueryRowContext(ctx, want).Scan(&actual))
				require.Equal(t, want, actual)
			}
		}()
		castPrepared, err := conn.PrepareContext(ctx, `select json_value(cast(? as json),'$.keep' returning signed)`)
		require.NoError(t, err)
		func() {
			defer castPrepared.Close()
			for _, want := range []int64{5, 9} {
				var actual int64
				require.NoError(t, castPrepared.QueryRowContext(ctx, fmt.Sprintf(`{"keep":%d}`, want)).Scan(&actual))
				require.Equal(t, want, actual)
			}
		}()
		_, err = conn.ExecContext(ctx, `select json_value(cast('{' as json),'$.keep' default 'fallback' on error)`)
		require.Error(t, err, "producer parse errors cannot become a JSON_VALUE default")
		tx, err := conn.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		_, err = tx.ExecContext(ctx, `insert into docs values (4, '{"keep":4}')`)
		require.NoError(t, err)
		check(tx, 4, false)
		require.NoError(t, tx.Commit())
		var flush string
		require.NoError(t, conn.QueryRowContext(ctx, "select mo_ctl('dn','flush','json_value_reader_sources.docs')").Scan(&flush))
		check(conn, 4, false)
		check(conn, 2, true)
		prepared, err := conn.PrepareContext(ctx, `select json_value(doc, '$.keep' returning signed) from docs where id = ?`)
		require.NoError(t, err)
		defer prepared.Close()
		var actual int64
		require.NoError(t, prepared.QueryRowContext(ctx, 1).Scan(&actual))
		require.Equal(t, int64(1), actual)
		exec(`update docs set doc = '{"keep":5}' where id = 1`)
		require.NoError(t, prepared.QueryRowContext(ctx, 1).Scan(&actual))
		require.Equal(t, int64(5), actual)
	})
}
