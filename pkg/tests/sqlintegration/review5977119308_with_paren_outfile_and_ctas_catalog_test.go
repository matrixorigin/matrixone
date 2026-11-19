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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// Review 5977119308 on PR 32906, reproduced end to end on the embedded
// single-CN fixture with the real frontend export and catalog consumers:
//
//   - A WITH query whose bare parenthesized body owns a nested SELECT ... INTO
//     OUTFILE must export the rows to the requested test-owned file instead of
//     silently returning them to the client. Before the grammar fix the root
//     Select.Ep stayed nil, so the frontend (which initializes the export only
//     from that root field) never created the file.
//
//   - A real multi-statement protocol request that creates a table with CTAS
//     and ENGINE_ATTRIBUTE 'example' plus a COMMENT whose decoded value ends
//     in a backslash must store a mo_catalog.mo_tables.rel_createsql that
//     reparses and keeps the decoded option values. Before the shared
//     formatter fix the stored statement was malformed SQL.
//
// All objects live in one owned database; every scenario drops it in defer,
// the exported file lives in t.TempDir and is removed by the test itself.
func TestReview5977119308ExportAndCTASCatalogRoundtripSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()

		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)

		// The reviewer's scenario reaches the server as one multi-statement
		// protocol request, so the writer connection enables multiStatements.
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?multiStatements=true", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		const testDB = "review5977119308"
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

		// The shared source table belongs to the parent fixture so each sibling
		// subtest is independently selectable (root phase-5 finding: the CTAS
		// subtest consumed export_src from the OUTFILE sibling's setup).
		exec(t, "CREATE TABLE export_src (id INT, tag VARCHAR(16))")
		exec(t, "INSERT INTO export_src VALUES (1, 'one'), (2, 'two')")

		t.Run("with_paren_body_into_outfile_writes_real_file", func(t *testing.T) {
			outDir := t.TempDir()
			outFile := filepath.Join(outDir, "with_paren_outfile.csv")
			exec(t, "WITH cte AS (SELECT id, tag FROM export_src) (SELECT * FROM cte ORDER BY id INTO OUTFILE '"+outFile+"' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n')")
			defer func() {
				_ = os.Remove(outFile)
			}()

			data, readErr := os.ReadFile(outFile)
			require.NoError(t, readErr, "the bare parenthesized WITH body must have exported the file: %s", outFile)
			content := string(data)
			lines := strings.Split(strings.TrimRight(content, "\n"), "\n")
			// The statement's explicit FIELDS/LINES terminators hold; the export
			// also carries the column header and default-quoted string values,
			// which is the real consumer contract of this driver default.
			require.Len(t, lines, 3, "header plus exactly the two source rows must be exported: %q", content)
			require.Equal(t, "id,tag", lines[0], content)
			require.Equal(t, "1,\"one\"", lines[1], content)
			require.Equal(t, "2,\"two\"", lines[2], content)

			// Re-running the same export must be rejected on the existing owned
			// file: that is the real consumer contract proving the rows went to
			// the file instead of silently returning to the client.
			_, queryErr := conn.ExecContext(ctx, "WITH cte AS (SELECT id, tag FROM export_src) (SELECT * FROM cte ORDER BY id INTO OUTFILE '"+outFile+"' FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n')")
			require.Error(t, queryErr, "re-export to the same path must be rejected, not silently return rows to the client")
			var mysqlErr *mysql.MySQLError
			require.ErrorAs(t, queryErr, &mysqlErr)
			require.EqualValues(t, 20406, mysqlErr.Number, "existing-file rejection code: %v", queryErr)
			unchanged, readErr := os.ReadFile(outFile)
			require.NoError(t, readErr)
			require.Equal(t, content, string(unchanged), "the rejected re-export must leave the owned file untouched")
		})

		t.Run("ctas_engine_attribute_comment_catalog_roundtrip", func(t *testing.T) {
			// One multi-statement protocol request: CTAS create+populate from
			// the real source rows, then read them back through a second table.
			_, err := conn.ExecContext(ctx, "CREATE TABLE review_ct ENGINE_ATTRIBUTE 'example' COMMENT 'ends with backslash\\\\' AS SELECT id, tag FROM export_src; CREATE TABLE review_ctrl PROPERTIES ('k'='va l''ue') COMMENT 'plain' AS SELECT id FROM export_src")
			require.NoError(t, err, "multi-statement CTAS request")

			var populated, controlled int
			require.NoError(t, conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM review_ct").Scan(&populated))
			require.Equal(t, 2, populated, "CTAS must populate the real source rows")
			require.NoError(t, conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM review_ctrl").Scan(&controlled))
			require.Equal(t, 2, controlled)

			readRelCreateSQL := func(t *testing.T, table string) string {
				t.Helper()
				var createSQL string
				require.NoError(t, conn.QueryRowContext(ctx, "SELECT rel_createsql FROM mo_catalog.mo_tables WHERE account_id = 0 AND reldatabase = ? AND relname = ?", testDB, table).Scan(&createSQL), table)
				return createSQL
			}

			ctSQL := readRelCreateSQL(t, "review_ct")
			require.Contains(t, ctSQL, "ENGINE_ATTRIBUTE = 'example'", ctSQL)
			require.Contains(t, ctSQL, "comment = 'ends with backslash\\\\'", ctSQL)

			stmt, parseErr := parsers.ParseOne(t.Context(), dialect.MYSQL, ctSQL, 1)
			require.NoError(t, parseErr, "stored rel_createsql must reparse: %s", ctSQL)
			ctCreate, ok := stmt.(*tree.CreateTable)
			require.True(t, ok, ctSQL)
			var engine, comment string
			var hasEngine, hasComment bool
			for _, opt := range ctCreate.Options {
				switch option := opt.(type) {
				case *tree.TableOptionEngineAttr:
					engine, hasEngine = option.Engine, true
				case *tree.TableOptionComment:
					comment, hasComment = option.Comment, true
				}
			}
			require.True(t, hasEngine, ctSQL)
			require.Equal(t, "example", engine, ctSQL)
			require.True(t, hasComment, ctSQL)
			require.Equal(t, "ends with backslash\\", comment, "decoded comment value must survive the catalog roundtrip: %s", ctSQL)

			ctrlSQL := readRelCreateSQL(t, "review_ctrl")
			stmt, parseErr = parsers.ParseOne(t.Context(), dialect.MYSQL, ctrlSQL, 1)
			require.NoError(t, parseErr, "plain-comment and escaped-PROPERTIES control must keep reparsing: %s", ctrlSQL)
			ctrlCreate, ok := stmt.(*tree.CreateTable)
			require.True(t, ok, ctrlSQL)
			for _, opt := range ctrlCreate.Options {
				if props, ok := opt.(*tree.TableOptionProperties); ok {
					require.Len(t, props.Preperties, 1, ctrlSQL)
					require.Equal(t, "va l'ue", props.Preperties[0].Value, ctrlSQL)
					require.Equal(t, "k", props.Preperties[0].Key, ctrlSQL)
				}
				if commentOpt, ok := opt.(*tree.TableOptionComment); ok {
					require.Equal(t, "plain", commentOpt.Comment, ctrlSQL)
				}
			}
		})
	})
}
