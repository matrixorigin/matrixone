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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// PR 32906 new reviews 5983418028/5983638763 (CTAS admitted table options and
// bare END SERIAL) and 5983495748 (parenthesized WITH time-window effective
// clause, nested WITH lexical scope), exercised end to end through the real
// embedded single-CN MySQL protocol. Every scenario owns its source tables so
// each subtest is independently selectable; the whole fixture lives in one
// owned database dropped by the parent defer.
func TestNewReviewClosuresEmbeddedSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()

		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)

		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?multiStatements=true", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		const testDB = "review598closures"
		var preexisting int
		require.NoError(t, conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?", testDB).Scan(&preexisting))
		require.Zero(t, preexisting, "owned database name must not preexist: %s", testDB)
		_, err = conn.ExecContext(ctx, "CREATE DATABASE "+testDB+"; USE "+testDB)
		require.NoError(t, err, "multi-statement create-database request")
		defer cleanupSQLIntegration(t, cn, "DROP DATABASE IF EXISTS "+testDB)

		exec := func(t *testing.T, statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		readOneRow := func(t *testing.T, query string) []string {
			t.Helper()
			rows, err := conn.QueryContext(ctx, query)
			require.NoError(t, err, query)
			defer rows.Close()
			cols, err := rows.Columns()
			require.NoError(t, err, query)
			var out []string
			for rows.Next() {
				vals := make([]any, len(cols))
				ptrs := make([]any, len(cols))
				for i := range vals {
					ptrs[i] = &vals[i]
				}
				require.NoError(t, rows.Scan(ptrs...), query)
				cells := make([]string, len(cols))
				for i, v := range vals {
					switch tv := v.(type) {
					case nil:
						cells[i] = "<NULL>"
					case []byte:
						cells[i] = string(tv)
					default:
						cells[i] = fmt.Sprintf("%v", v)
					}
				}
				out = append(out, cells...)
			}
			require.NoError(t, rows.Err(), query)
			require.NotEmpty(t, out, query)
			return out
		}

		t.Run("ctas_admitted_options_catalog_roundtrip", func(t *testing.T) {
			// createTableSQLForCatalog (pkg/sql/plan/build_ddl.go:119) preserves
			// the original statement for a single-statement request but stores
			// the canonical AST serialization when one COM_QUERY carries several
			// statements. Send both CTAS requests in one multi-statement protocol
			// message so the shared option formatters are the real producer of
			// the stored rel_createsql.
			const multi = "CREATE TABLE opt_ct COMPRESSION 'zlib' ENCRYPTION 'Y' DELAY_KEY_WRITE 1 STORAGE DISK " +
				"CONNECTION 'c one' DATA DIRECTORY '/data/x' INDEX DIRECTORY '/idx/x' PASSWORD 'p''ass\\\\' TABLESPACE ts1 AS SELECT 1 AS a; " +
				"CREATE TABLE opt_ctrl COMMENT 'plain' PROPERTIES ('k'='va l''ue') AS SELECT 2 AS b"
			exec(t, multi)
			require.Equal(t, []string{"1"}, readOneRow(t, "SELECT COUNT(*) FROM opt_ct"))
			require.Equal(t, []string{"1"}, readOneRow(t, "SELECT COUNT(*) FROM opt_ctrl"))

			var createSQL, ctrlSQL string
			require.NoError(t, conn.QueryRowContext(ctx,
				"SELECT rel_createsql FROM mo_catalog.mo_tables WHERE account_id = 0 AND reldatabase = ? AND relname = ?", testDB, "opt_ct").Scan(&createSQL), "opt_ct")
			require.NoError(t, conn.QueryRowContext(ctx,
				"SELECT rel_createsql FROM mo_catalog.mo_tables WHERE account_id = 0 AND reldatabase = ? AND relname = ?", testDB, "opt_ctrl").Scan(&ctrlSQL), "opt_ctrl")
			// The canonical serialization itself must be visible: the
			// multi-statement path reserializes through the shared formatters,
			// so the stored catalog rows directly carry the new option shapes.
			for _, must := range []string{
				"compression = 'zlib'", "encryption = 'Y'", "delay_key_write = 1",
				"STORAGE DISK", "connection = 'c one'", "data directory = '/data/x'",
				"index directory = '/idx/x'", "password = 'p''ass\\\\'", "tablespace = ts1",
			} {
				require.Contains(t, createSQL, must, createSQL)
			}
			require.Contains(t, ctrlSQL, "comment = 'plain'", ctrlSQL)

			// Reparse both stored rows; every option must be present exactly once
			// with its decoded value — a missing option fails the count instead
			// of passing silently, and the two tablespace-typed instances
			// (STORAGE and TABLESPACE) are counted separately.
			stmt, parseErr := parsers.ParseOne(t.Context(), dialect.MYSQL, createSQL, 1)
			require.NoError(t, parseErr, "stored rel_createsql must reparse: %s", createSQL)
			create, ok := stmt.(*tree.CreateTable)
			require.True(t, ok, createSQL)
			counts := map[string]int{}
			for _, o := range create.Options {
				switch v := o.(type) {
				case *tree.TableOptionCompression:
					counts["compression"]++
					require.Equal(t, "zlib", v.Compression, createSQL)
				case *tree.TableOptionEncryption:
					counts["encryption"]++
					require.Equal(t, "Y", v.Encryption, createSQL)
				case *tree.TableOptionDelayKeyWrite:
					counts["delay_key_write"]++
					require.Equal(t, uint64(1), v.Value, createSQL)
				case *tree.TableOptionTablespace:
					if v.Name != "" {
						counts["tablespace_ident"]++
						require.Equal(t, "ts1", v.Name, createSQL)
						require.Equal(t, "", v.StorageOpt, createSQL)
					} else {
						counts["storage_opt"]++
						require.Equal(t, " STORAGE DISK", v.StorageOpt, createSQL)
					}
				case *tree.TableOptionConnection:
					counts["connection"]++
					require.Equal(t, "c one", v.Connection, createSQL)
				case *tree.TableOptionDataDirectory:
					counts["data_directory"]++
					require.Equal(t, "/data/x", v.Dir, createSQL)
				case *tree.TableOptionIndexDirectory:
					counts["index_directory"]++
					require.Equal(t, "/idx/x", v.Dir, createSQL)
				case *tree.TableOptionPassword:
					counts["password"]++
					require.Equal(t, "p'ass\\", v.Password, createSQL)
				}
			}
			require.Exactly(t, map[string]int{
				"compression": 1, "encryption": 1, "delay_key_write": 1,
				"connection": 1, "data_directory": 1, "index_directory": 1,
				"password": 1, "storage_opt": 1, "tablespace_ident": 1,
			}, counts, createSQL)

			ctrlStmt, ctrlErr := parsers.ParseOne(t.Context(), dialect.MYSQL, ctrlSQL, 1)
			require.NoError(t, ctrlErr, "control rel_createsql must reparse: %s", ctrlSQL)
			ctrlCreate, ok := ctrlStmt.(*tree.CreateTable)
			require.True(t, ok, ctrlSQL)
			ctrlCounts := map[string]int{}
			for _, o := range ctrlCreate.Options {
				switch v := o.(type) {
				case *tree.TableOptionComment:
					ctrlCounts["comment"]++
					require.Equal(t, "plain", v.Comment, ctrlSQL)
				case *tree.TableOptionProperties:
					ctrlCounts["properties"]++
					require.Len(t, v.Preperties, 1, ctrlSQL)
					require.Equal(t, "k", v.Preperties[0].Key, ctrlSQL)
					require.Equal(t, "va l'ue", v.Preperties[0].Value, ctrlSQL)
				}
			}
			require.Exactly(t, map[string]int{"comment": 1, "properties": 1}, ctrlCounts, ctrlSQL)
		})

		t.Run("with_paren_time_window_matches_unwrapped", func(t *testing.T) {
			exec(t, "CREATE TABLE src_tw (ts TIMESTAMP)")
			exec(t, "INSERT INTO src_tw VALUES ('2026-01-01 12:00:00'), ('2026-01-01 12:00:30'), ('2026-01-01 12:02:15')")
			wrapped := "WITH c AS (SELECT ts FROM src_tw) (SELECT _wstart, COUNT(*) AS cnt FROM src_tw INTERVAL(ts, 1, minute))"
			unwrapped := "WITH c AS (SELECT ts FROM src_tw) SELECT _wstart, COUNT(*) AS cnt FROM src_tw INTERVAL(ts, 1, minute)"
			_ = wrapped
			_ = unwrapped
			// The reviews construct the window over the CTE, so both queries
			// must read through c, not the raw table.
			wrapped = "WITH c AS (SELECT ts FROM src_tw) (SELECT _wstart, COUNT(*) AS cnt FROM c INTERVAL(ts, 1, minute))"
			unwrapped = "WITH c AS (SELECT ts FROM src_tw) SELECT _wstart, COUNT(*) AS cnt FROM c INTERVAL(ts, 1, minute)"
			query := func(t *testing.T, q string) []string {
				rows, err := conn.QueryContext(ctx, q)
				require.NoError(t, err, q)
				defer rows.Close()
				var got []string
				for rows.Next() {
					var wstart, cnt string
					require.NoError(t, rows.Scan(&wstart, &cnt), q)
					got = append(got, wstart+"="+cnt)
				}
				require.NoError(t, rows.Err(), q)
				sort.Strings(got)
				return got
			}
			expected := []string{"2026-01-01 12:00:00=2", "2026-01-01 12:02:00=1"}
			require.Equal(t, expected, query(t, unwrapped), "unwrapped control")
			require.Equal(t, expected, query(t, wrapped), "parenthesized body must carry the inner time window")
		})

		t.Run("nested_with_lexical_scope_beats_same_name_physical", func(t *testing.T) {
			nested := "WITH outer_cte AS (SELECT 7 AS a) (WITH inner_cte AS (SELECT a FROM outer_cte) SELECT a FROM inner_cte)"
			flat := "WITH outer_cte AS (SELECT 7 AS a) SELECT a FROM outer_cte"

			require.Equal(t, []string{"7"}, readOneRow(t, flat), "flat control")
			require.Equal(t, []string{"7"}, readOneRow(t, nested), "outer CTE must stay visible inside the parenthesized inner WITH")

			// A physical table with the same name must not capture the query.
			exec(t, "CREATE TABLE outer_cte (a INT)")
			exec(t, "INSERT INTO outer_cte VALUES (99)")
			require.Equal(t, []string{"7"}, readOneRow(t, nested), "the CTE shadows the physical table")
			require.Equal(t, []string{"99"}, readOneRow(t, "SELECT a FROM outer_cte"), "physical table control")
			// Inner same-name legal shadowing: the inner CTE wins inside the
			// parenthesized body without polluting the outer scope (root13 F3).
			shadow := "WITH outer_cte AS (SELECT 7 AS a) (WITH outer_cte AS (SELECT 9 AS a) SELECT a FROM outer_cte)"
			require.Equal(t, []string{"9"}, readOneRow(t, shadow), "inner same-name CTE legally shadows the outer one")
			require.Equal(t, []string{"7"}, readOneRow(t, flat), "outer scope keeps its own definition after inner shadowing")
		})
	})
}
