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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// The two branch values have a correct NDV, but an even split estimate makes
// the four-row delete read the complete hidden indexes.
func TestIndexedEqualityPrunesDeleteMaintenance(t *testing.T) {
	RunBaseClusterTests(t, func(cluster Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?readTimeout=30s", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		mustExec := func(query string) {
			t.Helper()
			_, err := db.ExecContext(ctx, query)
			require.NoError(t, err)
		}

		const table = "qa_indexed_eq_delete.records"
		mustExec("create database qa_indexed_eq_delete")
		defer func() {
			_, err := db.Exec("drop database qa_indexed_eq_delete")
			if err != nil {
				t.Error(err)
			}
		}()
		mustExec("create table " + table + ` (
			row_key varchar(64) primary key, workspace varchar(64) not null,
			group_key varchar(64) not null, owner_key varchar(64) not null,
			token_key varchar(64) not null,
			index idx_group(group_key), index idx_token(owner_key, token_key))`)
		// Reach the costing threshold and keep the rare keys in the last
		// primary-key-ordered block so the block bounds can distinguish them.
		mustExec("insert into " + table + `
			select lpad(cast(result as char), 8, '0'), 'workspace',
				if(result > 49996, 'rare', 'common'),
				md5(concat('case-', floor(result / 8))), md5(concat('token-', result))
			from generate_series(1, 50000) g`)
		mustExec("select mo_ctl('dn','flush','" + table + "')")

		filter := " where group_key='rare' and workspace='workspace'"
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		rows, err := tx.QueryContext(ctx, "explain analyze delete from "+table+filter)
		require.NoError(t, err)
		defer rows.Close()
		var explain strings.Builder
		for rows.Next() {
			var line string
			require.NoError(t, rows.Scan(&line))
			explain.WriteString(line)
			explain.WriteByte('\n')
		}
		require.NoError(t, rows.Err())
		text := explain.String()
		hidden := regexp.MustCompile(`Table Scan on [^\n]*\.(__mo_index_secondary_[^\n ]*)[^\n]*\n[^\n]*inputRows=([0-9]+)`).FindAllStringSubmatch(text, -1)
		require.Len(t, hidden, 2, text)
		require.NotEqual(t, hidden[0][1], hidden[1][1], text)
		for _, scan := range hidden {
			require.Equal(t, "4", scan[2], text)
		}
		var count int
		require.NoError(t, tx.QueryRowContext(ctx, "select count(*) from "+table+filter).Scan(&count))
		require.Zero(t, count)
		require.NoError(t, tx.Rollback())
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from "+table+filter).Scan(&count))
		require.Equal(t, 4, count)
	})
}
