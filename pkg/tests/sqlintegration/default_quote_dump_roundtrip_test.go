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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// Reuse the canonical serial SQL fixture: one historical row, one index and
// empty destinations distinguish archive compatibility from accepting any hash.
func TestQuotedDefaultDumpRoundTrip(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		const schema = "quoted_default_dump_contract"
		defer cleanupSQLIntegration(t, cn, "drop database if exists "+schema)
		exec := func(t *testing.T, q string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		scalar := func(t *testing.T, q, want string) {
			t.Helper()
			var got string
			require.NoError(t, conn.QueryRowContext(ctx, q).Scan(&got), q)
			require.Equal(t, want, got, q)
		}
		showCreate := func(t *testing.T, name string) string {
			t.Helper()
			var table, ddl string
			require.NoError(t, conn.QueryRowContext(ctx, "show create table "+name).Scan(&table, &ddl))
			return ddl
		}
		invalidInput := func(t *testing.T, err error) {
			t.Helper()
			var sqlErr *mysql.MySQLError
			require.ErrorAs(t, err, &sqlErr)
			require.Equal(t, uint16(20301), sqlErr.Number)
		}
		exec(t, "create database "+schema)
		exec(t, "use "+schema)
		// Restore before releasing the session even after a failing assertion.
		defer func() {
			cleanupCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer stop()
			_, err := conn.ExecContext(cleanupCtx, "set sql_mode=DEFAULT")
			require.NoError(t, err)
		}()
		definition := "(id int primary key,a varchar(10) default 'it''s',b varchar(10) default 'x" + string(rune(96)) + "y',n int default 7,key ia(a))"
		create := func(t *testing.T, name, def string) { exec(t, "create table "+name+def) }
		archive := t.TempDir()
		require.NoError(t, os.CopyFS(archive, os.DirFS(filepath.Join("testdata", "default_quote_pre31067"))))
		readManifest := func(dir string) map[string]json.RawMessage {
			t.Helper()
			data, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
			require.NoError(t, err)
			var manifest map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(data, &manifest))
			return manifest
		}
		historical := readManifest(archive)
		create(t, "restored", definition)
		exec(t, "load table restored from '"+archive+"'")
		scalar(t, "select concat(id,':',a,':',b,':',n) from restored", "1:it's:x"+string(rune(96))+"y:7")
		currentDir := t.TempDir()
		exec(t, "dump table restored to '"+currentDir+"'")
		current := readManifest(currentDir)
		require.JSONEq(t, string(historical["schema_hash"]), string(current["schema_hash"]))
		type relationHash struct {
			Role              string
			IndexName         string `json:"index_name"`
			SchemaHash        string `json:"schema_hash"`
			LogicalSchemaHash string `json:"logical_schema_hash"`
		}
		var oldRelations, currentRelations []relationHash
		require.NoError(t, json.Unmarshal(historical["relations"], &oldRelations))
		require.NoError(t, json.Unmarshal(current["relations"], &currentRelations))
		require.Equal(t, oldRelations, currentRelations)
		exec(t, "insert into restored(id) values(2)")
		scalar(t, "select count(*) from restored force index(ia) where a='it''s' and n=7", "2")
		create(t, "current_copy", definition)
		exec(t, "load table current_copy from '"+currentDir+"'")
		exec(t, "insert into current_copy(id) values(3)")
		scalar(t, "select concat(a,':',b,':',n) from current_copy where id=3", "it's:x"+string(rune(96))+"y:7")

		for _, tc := range []struct{ name, def string }{
			{"wrong_default", strings.Replace(definition, "it''s", "changed", 1)},
			{"wrong_type", strings.Replace(definition, "b varchar(10)", "b varchar(11)", 1)},
			{"wrong_index", strings.Replace(definition, "key ia(a)", "key ia(b)", 1)},
		} {
			t.Run(tc.name, func(t *testing.T) {
				create(t, tc.name, tc.def)
				before := showCreate(t, tc.name)
				_, err := conn.ExecContext(ctx, "load table "+tc.name+" from '"+archive+"'")
				require.ErrorContains(t, err, "target table schema does not match table dump")
				invalidInput(t, err)
				require.Equal(t, before, showCreate(t, tc.name), "rejected LOAD must not alter declarations")
				scalar(t, "select count(*) from "+tc.name, "0")
			})
		}
		// A matching physical hash must not override a present, incompatible logical
		// index hash. Copy the historical archive; never mutate the frozen fixture.
		corrupt := t.TempDir()
		require.NoError(t, os.CopyFS(corrupt, os.DirFS(archive)))
		var relations []map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(historical["relations"], &relations))
		require.Len(t, relations, 2)
		relations[1]["logical_schema_hash"], err = json.Marshal(strings.Repeat("0", 64))
		require.NoError(t, err)
		historical["relations"], err = json.Marshal(relations)
		require.NoError(t, err)
		data, err := json.Marshal(historical)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(corrupt, "manifest.json"), data, 0600))
		create(t, "wrong_logical", definition)
		logicalBefore := showCreate(t, "wrong_logical")
		_, err = conn.ExecContext(ctx, "load table wrong_logical from '"+corrupt+"'")
		require.ErrorContains(t, err, "target table index topology does not match table dump")
		invalidInput(t, err)
		require.Equal(t, logicalBefore, showCreate(t, "wrong_logical"))
		scalar(t, "select count(*) from wrong_logical", "0")

		// Capture actual SHOW output, rename only its table header, then replay it
		// twice in each reader mode. Explicit bytes catch progressively doubled
		// quotes and backslashes even if both parser and formatter agree internally.
		values := []string{"it's", "x" + string(rune(96)) + "y", "q\"r", "", "  ", "a\\b", "a\\'b"}
		declarations := make([]string, 0, len(values))
		names := make([]string, 0, len(values))
		projections := make([]string, 0, len(values))
		for i, value := range values {
			name := fmt.Sprintf("v%d", i)
			literal := "'" + strings.ReplaceAll(strings.ReplaceAll(value, "\\", "\\\\"), "'", "''") + "'"
			declarations = append(declarations, name+" varchar(32) default "+literal)
			names = append(names, name)
			projections = append(projections, "hex("+name+")")
		}
		exec(t, "set sql_mode=DEFAULT")
		create(t, "seed", "(id int,"+strings.Join(declarations, ",")+",n int default 7,z int default null,dt date default '2024-01-01',e varchar(32) default (concat('it''s','!')),ts timestamp default current_timestamp)")
		for modeIndex, mode := range []string{"", "NO_BACKSLASH_ESCAPES", "ANSI_QUOTES", "ANSI_QUOTES,NO_BACKSLASH_ESCAPES"} {
			t.Run("show_"+mode, func(t *testing.T) {
				exec(t, "set sql_mode='"+mode+"'")
				source := "seed"
				for generation := 0; generation < 2; generation++ {
					ddl := showCreate(t, source)
					target := fmt.Sprintf("replay_%d_%d", modeIndex, generation)
					header := "CREATE TABLE " + string(rune(96)) + source + string(rune(96))
					require.True(t, strings.HasPrefix(ddl, header), ddl)
					ddl = strings.Replace(ddl, header, "CREATE TABLE "+string(rune(96))+target+string(rune(96)), 1)
					exec(t, ddl)
					exec(t, "insert into "+target+"(id) values(1)")
					var got = make([]string, len(values))
					args := make([]interface{}, len(values))
					for i := range got {
						args[i] = &got[i]
					}
					require.NoError(t, conn.QueryRowContext(ctx, "select "+strings.Join(projections, ",")+" from "+target).Scan(args...))
					for i, value := range values {
						require.Equal(t, strings.ToUpper(hex.EncodeToString([]byte(value))), got[i], names[i])
					}
					scalar(t, "select concat(n,':',z is null,':',dt,':',e,':',ts is not null) from "+target, "7:1:2024-01-01:it's!:1")
					source = target
				}
			})
		}
	})
}
