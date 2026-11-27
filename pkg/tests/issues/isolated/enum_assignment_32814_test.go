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
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBinaryEnumAssignment32814(t *testing.T) {
	path := t.TempDir()
	// This test uses the public MySQL TCP protocol and does not need a
	// second listener. A preexisting non-socket path suppresses that listener.
	noUnix := filepath.Join(path, "skip-unix-listener")
	require.NoError(t, os.WriteFile(noUnix, nil, 0600))
	cluster, err := embed.NewCluster(embed.WithTesting(), embed.WithDataPath(path), embed.WithCNCount(1), embed.WithPreStart(func(s embed.ServiceOperator) {
		if s.ServiceType() == metadata.ServiceType_CN {
			s.Adjust(func(cfg *embed.ServiceConfig) { cfg.CN.Frontend.UnixSocketAddress = noUnix })
		}
	}))
	if cluster != nil {
		t.Cleanup(func() { assert.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	require.NoError(t, cluster.Start())
	c, err := cluster.GetCNService(0)
	require.NoError(t, err)
	dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?readTimeout=25s&writeTimeout=25s", c.GetServiceConfig().CN.Frontend.Port)
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	var conn *sql.Conn
	var db *sql.DB
	connect := func(t *testing.T) {
		t.Helper()
		var err error
		db, err = sql.Open("mysql", dsn)
		require.NoError(t, err)
		require.EventuallyWithT(t, func(ch *assert.CollectT) { conn, err = db.Conn(ctx); assert.NoError(ch, err) }, 30*time.Second, 250*time.Millisecond)
		// Login can succeed before the CN's cluster snapshot reports the TN shard.
		clusterState, err := clusterservice.GetMOClusterWithContext(ctx, c.ServiceID())
		require.NoError(t, err)
		require.EventuallyWithT(t, func(ch *assert.CollectT) {
			clusterState.ForceRefresh(false)
			stores := clusterState.GetAllTNServices()
			if assert.Len(ch, stores, 1) {
				assert.NotEmpty(ch, stores[0].Shards)
			}
		}, 30*time.Second, 250*time.Millisecond)
	}
	closeConnect := func() {
		if conn != nil {
			assert.NoError(t, conn.Close())
			conn = nil
		}
		if db != nil {
			assert.NoError(t, db.Close())
			db = nil
		}
	}
	t.Cleanup(closeConnect)
	connect(t)
	exec := func(t *testing.T, q string) {
		t.Helper()
		_, err := conn.ExecContext(ctx, q)
		require.NoError(t, err, q)
	}
	type witness struct {
		id       int
		label    string
		hex      string
		position int
	}
	read := func(t *testing.T, table string) []witness {
		t.Helper()
		rs, err := conn.QueryContext(ctx, "select id,cast(e as char),hex(cast(e as char)),e+0 from "+table+" order by id")
		require.NoError(t, err, table)
		defer rs.Close()
		var out []witness
		for rs.Next() {
			var w witness
			require.NoError(t, rs.Scan(&w.id, &w.label, &w.hex, &w.position))
			out = append(out, w)
		}
		require.NoError(t, rs.Err())
		return out
	}
	exec(t, "create database enum_validation_32824")
	exec(t, "use enum_validation_32824")
	opt := " character set utf8mb4 collate utf8mb4_bin"
	src := "enum('A','a')" + opt
	dst := "enum('a','A')" + opt
	mk := func(t *testing.T, table, spec string) {
		t.Helper()
		exec(t, "create table "+table+"(id int primary key,e "+spec+")")
	}
	seeds := []witness{{1, "A", "41", 1}, {2, "a", "61", 2}}
	want := []witness{{1, "A", "41", 2}, {2, "a", "61", 1}}
	mk(t, "seed", src)
	exec(t, "insert into seed values(1,1),(2,2)")
	require.Equal(t, seeds, read(t, "seed"), "numeric input seeds both stored ordinals independently of text lookup")
	t.Run("grouped numeric conversion", func(t *testing.T) {
		rows, err := conn.QueryContext(ctx, "select e+0 from seed group by e order by 1")
		require.NoError(t, err)
		defer rows.Close()
		var positions []int
		for rows.Next() {
			var position int
			require.NoError(t, rows.Scan(&position))
			positions = append(positions, position)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []int{1, 2}, positions)
	})
	t.Run("column metadata", func(t *testing.T) {
		exec(t, "create table meta (b enum('A','a')"+opt+
			", g enum('A','Other') collate utf8mb4_general_ci, e enum('A','Other'), s varchar(5)"+opt+")")
		want := map[string]string{"b": "utf8mb4_bin", "g": "utf8mb4_general_ci", "e": "", "s": "utf8mb4_bin"}
		rows, err := conn.QueryContext(ctx, "show full columns from meta")
		require.NoError(t, err)
		defer rows.Close()
		got := make(map[string]string)
		for rows.Next() {
			var name string
			var collation sql.NullString
			var discard sql.RawBytes
			require.NoError(t, rows.Scan(&name, &discard, &collation, &discard, &discard, &discard, &discard, &discard, &discard))
			got[name] = collation.String
		}
		require.NoError(t, rows.Err())
		require.Equal(t, want, got)

		rows, err = conn.QueryContext(ctx, "select column_name, character_set_name, collation_name from information_schema.columns where table_schema = database() and table_name = 'meta' and column_name in ('b','g','e','s')")
		require.NoError(t, err)
		defer rows.Close()
		got = make(map[string]string)
		for rows.Next() {
			var name string
			var charset, collation sql.NullString
			require.NoError(t, rows.Scan(&name, &charset, &collation))
			if want[name] == "" {
				require.False(t, charset.Valid)
				require.False(t, collation.Valid)
			} else {
				require.Equal(t, "utf8mb4", charset.String)
			}
			got[name] = collation.String
		}
		require.NoError(t, rows.Err())
		require.Equal(t, want, got)
	})
	t.Run("dump and load v1", func(t *testing.T) {
		exec(t, "create stage enum_stage url = 'file://"+t.TempDir()+"/'")
		mk(t, "legacy_dump", "enum('2','1')")
		mk(t, "legacy_load", "enum('2','1')")
		mk(t, "explicit_load", "enum('2','1')"+opt)
		exec(t, "dump table legacy_dump to 'stage://enum_stage/legacy' metadata only")
		exec(t, "load table legacy_load from 'stage://enum_stage/legacy'")
		_, err := conn.ExecContext(ctx, "load table explicit_load from 'stage://enum_stage/legacy'")
		require.ErrorContains(t, err, "explicit ENUM collation")
		_, err = conn.ExecContext(ctx, "dump table explicit_load to 'stage://enum_stage/typed' metadata only")
		require.ErrorContains(t, err, "explicit ENUM collation")
	})
	t.Run("literal", func(t *testing.T) {
		mk(t, "literal_dst", dst)
		exec(t, "insert into literal_dst values(1,'A'),(2,'a')")
		assert.Equal(t, want, read(t, "literal_dst"))
	})
	t.Run("prepared", func(t *testing.T) {
		mk(t, "prepared_dst", dst)
		stmt, err := conn.PrepareContext(ctx, "insert into prepared_dst values(?,?)")
		require.NoError(t, err)
		defer stmt.Close()
		for _, w := range want {
			_, err = stmt.ExecContext(ctx, w.id, w.label)
			require.NoError(t, err)
		}
		assert.Equal(t, want, read(t, "prepared_dst"))
	})
	t.Run("insert select", func(t *testing.T) {
		mk(t, "select_dst", dst)
		exec(t, "insert into select_dst select id,e from seed")
		assert.Equal(t, want, read(t, "select_dst"))
	})
	t.Run("insert select distinct", func(t *testing.T) {
		mk(t, "distinct_dst", dst)
		exec(t, "insert into distinct_dst select distinct id,e from seed")
		assert.Equal(t, want, read(t, "distinct_dst"))
	})
	t.Run("update", func(t *testing.T) {
		mk(t, "update_dst", dst)
		exec(t, "insert into update_dst values(1,1),(2,2)")
		exec(t, "update update_dst set e='A' where id=1")
		exec(t, "update update_dst set e='a' where id=2")
		assert.Equal(t, want, read(t, "update_dst"))
	})
	t.Run("bulk csv", func(t *testing.T) {
		mk(t, "bulk_dst", dst)
		exec(t, "load data inline format='csv', data='1,A\n2,a\n' into table bulk_dst fields terminated by ','")
		assert.Equal(t, want, read(t, "bulk_dst"))
	})
	t.Run("comma and empty member", func(t *testing.T) {
		mk(t, "seed_delimited", "enum('A','a','','comma,value')"+opt)
		mk(t, "dst_delimited", "enum('','comma,value','a','A')"+opt)
		exec(t, "insert into seed_delimited values(1,1),(2,2),(3,3),(4,4)")
		exec(t, "insert into dst_delimited select id,e from seed_delimited")
		assert.Equal(t, []witness{{1, "A", "41", 4}, {2, "a", "61", 3}, {3, "", "", 1}, {4, "comma,value", "636F6D6D612C76616C7565", 2}}, read(t, "dst_delimited"))
	})
	t.Run("case insensitive lookup unchanged", func(t *testing.T) {
		mk(t, "ci_dst", "enum('a','Other') character set utf8mb4 collate utf8mb4_general_ci")
		exec(t, "insert into ci_dst values(1,'A'),(2,'oThEr')")
		assert.Equal(t, []witness{{1, "a", "61", 1}, {2, "Other", "4F74686572", 2}}, read(t, "ci_dst"))
	})
	t.Run("same CREATE default and legacy control", func(t *testing.T) {
		exec(t, "create table inherited_bin(id int primary key,e enum('a','A')) collate=utf8mb4_bin")
		exec(t, "insert into inherited_bin values(1,'A'),(2,'a')")
		assert.Equal(t, want, read(t, "inherited_bin"))
		mk(t, "legacy_enum", "enum('A','a')")
		exec(t, "insert into legacy_enum values(1,'A'),(2,'a')")
		assert.Equal(t, []witness{{1, "A", "41", 1}, {2, "A", "41", 1}}, read(t, "legacy_enum"))
	})
	t.Run("alter copy", func(t *testing.T) {
		mk(t, "alter_dst", src)
		exec(t, "insert into alter_dst select id,e from seed")
		exec(t, "alter table alter_dst algorithm=copy,add column ext int default 1")
		assert.Equal(t, seeds, read(t, "alter_dst"), "unrelated ALTER COPY must preserve exact ordinals")
		exec(t, "alter table alter_dst algorithm=copy,modify column e "+dst)
		assert.Equal(t, want, read(t, "alter_dst"), "changed domain maps by binary label, not source ordinal")
	})
	t.Run("restart and catalog reopen", func(t *testing.T) {
		closeConnect()
		require.NoError(t, cluster.Close())
		require.NoError(t, cluster.Start())
		connect(t)
		exec(t, "use enum_validation_32824")
		assert.Equal(t, seeds, read(t, "seed"), "source label/type catalog reopens")
		mk(t, "reopen_dst", dst)
		exec(t, "insert into reopen_dst select id,e from seed")
		assert.Equal(t, want, read(t, "reopen_dst"), "reopened source labels remap into a new binary destination")
		// The preexisting destination's persisted metadata must also guide write lookup.
		exec(t, "insert into select_dst values(3,'A'),(4,'a')")
		assert.Equal(t, []witness{{1, "A", "41", 2}, {2, "a", "61", 1}, {3, "A", "41", 2}, {4, "a", "61", 1}}, read(t, "select_dst"))
	})
}
