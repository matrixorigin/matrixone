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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestMoIndexStatsKeysSQL(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists index_stats_keys", "drop user if exists key_stats_reader")
		exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
		exec("create database index_stats_keys")
		exec("use index_stats_keys")
		exec("create table t(id bigint primary key,k int,b int,u int not null,s varchar(40),unique uk(u),index(k),index idx_comp(k,b),index idx_pref(s(2)),index idx_explicit(k,id))")
		exec("insert into t values(1,1,null,11,'ab-one'),(2,1,null,12,'ab-two'),(3,2,7,13,'xy-one'),(4,null,7,14,null),(5,null,7,15,null)")
		read := func(table, index, kind string, entries, cardinality float64) map[string]any {
			t.Helper()
			q := fmt.Sprintf("select index_type,row_type,object_id,metrics from mo_index_stats('index_stats_keys','%s','%s') as s", table, index)
			rows, err := conn.QueryContext(ctx, q)
			require.NoError(t, err, q)
			defer rows.Close()
			require.True(t, rows.Next())
			var typ, rowType, encoded string
			var object sql.NullString
			require.NoError(t, rows.Scan(&typ, &rowType, &object, &encoded))
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
			require.Equal(t, kind, typ)
			require.Equal(t, "index", rowType)
			require.False(t, object.Valid)
			var metrics map[string]any
			require.NoError(t, json.Unmarshal([]byte(encoded), &metrics))
			require.Equal(t, entries, metrics["entry_count"])
			require.Equal(t, cardinality, metrics["cardinality"])
			require.Equal(t, "btree", metrics["method"])
			require.NotContains(t, metrics, "async")
			require.NotContains(t, metrics, "diagnosis")
			return metrics
		}
		for _, tc := range []struct {
			name, kind string
			card       float64
		}{
			{"PRIMARY", "primary", 5}, {"uk", "unique", 5}, {"k", "secondary", 3}, {"idx_comp", "secondary", 3}, {"idx_pref", "secondary", 3}, {"idx_explicit", "secondary", 5},
		} {
			metrics := read("t", tc.name, tc.kind, 5, tc.card)
			require.NotContains(t, metrics["columns"], "__mo_alias_id")
		}
		metrics := read("t", "PRIMARY", "primary", 5, 5)
		require.Equal(t, []any{"id"}, metrics["columns"])
		metrics = read("t", "idx_pref", "secondary", 5, 3)
		require.Equal(t, map[string]any{"s": float64(2)}, metrics["prefix_lengths"])
		exec("create index idx_func on t((lower(s)))")
		read("t", "idx_func", "secondary", 5, 4)
		exec("update t set s='AB-ONE' where id=2")
		read("t", "idx_func", "secondary", 5, 3)
		exec("begin")
		exec("insert into t values(6,2,7,16,'xy-new')")
		read("t", "k", "secondary", 6, 3)
		read("t", "PRIMARY", "primary", 6, 6)
		exec("rollback")
		exec("delete from t where id=3")
		read("t", "k", "secondary", 4, 2)
		exec("create table cp(a int,b int,k int,primary key(a,b),index(k))")
		exec("insert into cp values(1,1,7),(1,2,7),(2,1,null)")
		read("cp", "PRIMARY", "primary", 3, 3)
		read("cp", "k", "secondary", 3, 2)
		exec("create table np(k int,index(k))")
		exec("insert into np values(1),(1),(null)")
		read("np", "k", "secondary", 3, 2)
		var noPrimary string
		require.ErrorContains(t, conn.QueryRowContext(ctx, "select metrics from mo_index_stats('index_stats_keys','np','PRIMARY') as s").Scan(&noPrimary), "does not exist")
		exec("create table uk_nulls(id int primary key,v int,unique(v))")
		exec("insert into uk_nulls values(1,null),(2,null),(3,1),(4,2)")
		// Legacy unique indexes omit NULL keys; versioned unique formats may
		// retain them with a PK discriminator. Always measure actual entries.
		var physical string
		require.NoError(t, conn.QueryRowContext(ctx, "select i.index_table_name from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on i.table_id=t.rel_id where t.reldatabase='index_stats_keys' and t.relname='uk_nulls' and i.name='v'").Scan(&physical))
		var count float64
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from `"+physical+"`").Scan(&count))
		cardinality := float64(2)
		if count > 2 {
			cardinality = 3
		}
		read("uk_nulls", "v", "unique", count, cardinality)
		// Cardinality follows SQL comparison semantics, including collations,
		// even when the index uses the legacy byte-oriented physical format.
		exec("create table ci(id int primary key,s varchar(40) collate utf8mb4_unicode_ci,n int,index sk(s),index pref(s(2)),index comp(s,n))")
		exec("insert into ci values(1,'Alpha',1),(2,'alpha',1),(3,'ALPHA ',2),(4,null,1),(5,null,1),(6,null,2)")
		read("ci", "sk", "secondary", 6, 2)
		read("ci", "pref", "secondary", 6, 2)
		read("ci", "comp", "secondary", 6, 4)
		exec("insert into ci values(7,'Beta',1)")
		read("ci", "sk", "secondary", 7, 3)
		read("ci", "pref", "secondary", 7, 3)
		read("ci", "comp", "secondary", 7, 5)
		exec("create table binary_keys(id int primary key,s varbinary(40),index sk(s))")
		exec("insert into binary_keys values(1,'Alpha'),(2,'alpha'),(3,'ALPHA '),(4,null)")
		read("binary_keys", "sk", "secondary", 4, 4)
		exec("create user key_stats_reader identified by '111'")
		reader, err := sql.Open("mysql", fmt.Sprintf("key_stats_reader:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer reader.Close()
		var encoded string
		q := "select metrics from mo_index_stats('index_stats_keys','t','k') as s"
		require.Error(t, reader.QueryRowContext(ctx, q).Scan(&encoded))
		exec("grant select on table index_stats_keys.t to public")
		require.NoError(t, reader.QueryRowContext(ctx, q).Scan(&encoded))
		exec("delete from t")
		read("t", "k", "secondary", 0, 0)
		read("t", "PRIMARY", "primary", 0, 0)
	})
}
