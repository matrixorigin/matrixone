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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

// TestIssue30936EnumSetColumnCharacterMetadata covers the information_schema
// metadata contract for declared ENUM/SET columns: MySQL reports their
// maximum rendered label width as character metadata and leaves numeric
// metadata NULL, while MatrixOne reported NULL lengths for both and treated
// the bitmap-backed SET column as a 20-digit unsigned integer.
func TestIssue30936EnumSetColumnCharacterMetadata(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		dbName := testutils.GetDatabaseName(t)
		schemaName := strings.ToLower(dbName)

		writer, err := c.GetCNService(0)
		require.NoError(t, err)
		open := func(cn embed.ServiceOperator) *sql.DB {
			t.Helper()
			db, err := sql.Open("mysql", fmt.Sprintf(
				"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false",
				cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			db.SetMaxOpenConns(1)
			db.SetMaxIdleConns(1)
			return db
		}
		reader0 := open(writer)
		defer reader0.Close()

		stmt := open(writer)
		defer stmt.Close()
		execSQLRequire(t, ctx, stmt, "create database `"+dbName+"`")
		defer execSQLMaybe(t, ctx, stmt, "drop database if exists `"+dbName+"`")

		// id / u / c act as controls: integer/unsigned/varchar metadata must
		// not be disturbed by the ENUM/SET repair.
		execSQLRequire(t, ctx, stmt, "create table `"+dbName+"`.t30936 ("+
			"id int primary key, u bigint unsigned, c varchar(32),"+
			"e enum('a','bb'), s set('a','bb'),"+
			"e_rev enum('z','a'), s_rev set('z','a'))")
		execSQLRequire(t, ctx, stmt, "create table `"+dbName+"`.t30936_mb ("+
			"e enum('中','长长'), s set('中','长长'))")
		// The declared single empty member of ENUM('') is a zero-length
		// domain, not absent metadata; the shared BIGINT UNSIGNED identity
		// must not inherit label-driven metadata (#30936).
		execSQLRequire(t, ctx, stmt, "create table `"+dbName+"`.t30936_empty ("+
			"e_empty enum(''), s_empty set(''), u bigint unsigned)")
		execSQLRequire(t, ctx, stmt,
			"insert into `"+dbName+"`.t30936 values (1, 7, 'x', 'bb', 'a,bb', 'a', 'z,a')")
		execSQLRequire(t, ctx, stmt,
			"insert into `"+dbName+"`.t30936_mb values ('长长', '中,长长')")

		// Stored label values are outside the metadata defect, but their
		// round trip pins the declared labels the metadata derives from.
		var storedEnum, storedSet, storedEnumMB, storedSetMB string
		require.NoError(t, reader0.QueryRowContext(ctx,
			"select e, s from `"+dbName+"`.t30936 where id = 1").Scan(&storedEnum, &storedSet))
		require.Equal(t, "bb", storedEnum)
		require.Equal(t, "a,bb", storedSet)
		require.NoError(t, reader0.QueryRowContext(ctx,
			"select e, s from `"+dbName+"`.t30936_mb").Scan(&storedEnumMB, &storedSetMB))
		require.Equal(t, "长长", storedEnumMB)
		require.Equal(t, "中,长长", storedSetMB)

		// (column, table) -> data_type, [character_maximum_length,
		// character_octet_length, numeric_precision, numeric_scale].
		// MySQL reports CHARACTER_OCTET_LENGTH as the character length
		// times the 4-byte UTF-8 bound; a full 64-bit SET('a','bb','c',...)
		// value never exceeds that domain.
		expect := map[[2]string]struct {
			dataType string
			values   [4]int64
			valid    [4]bool
		}{
			{"id", "t30936"}:            {dataType: "int", valid: [4]bool{false, false, true, true}, values: [4]int64{0, 0, 10, 0}},
			{"u", "t30936"}:             {dataType: "bigint", valid: [4]bool{false, false, true, true}, values: [4]int64{0, 0, 20, 0}},
			{"c", "t30936"}:             {dataType: "varchar", valid: [4]bool{true, true, false, false}, values: [4]int64{32, 128, 0, 0}},
			{"e", "t30936"}:             {dataType: "enum", valid: [4]bool{true, true, false, false}, values: [4]int64{2, 8, 0, 0}},
			{"s", "t30936"}:             {dataType: "set", valid: [4]bool{true, true, false, false}, values: [4]int64{4, 16, 0, 0}},
			{"e_rev", "t30936"}:         {dataType: "enum", valid: [4]bool{true, true, false, false}, values: [4]int64{1, 4, 0, 0}},
			{"s_rev", "t30936"}:         {dataType: "set", valid: [4]bool{true, true, false, false}, values: [4]int64{3, 12, 0, 0}},
			{"e", "t30936_mb"}:          {dataType: "enum", valid: [4]bool{true, true, false, false}, values: [4]int64{2, 8, 0, 0}},
			{"s", "t30936_mb"}:          {dataType: "set", valid: [4]bool{true, true, false, false}, values: [4]int64{4, 16, 0, 0}},
			{"e_empty", "t30936_empty"}: {dataType: "enum", valid: [4]bool{true, true, false, false}, values: [4]int64{0, 0, 0, 0}},
			{"s_empty", "t30936_empty"}: {dataType: "set", valid: [4]bool{true, true, false, false}, values: [4]int64{0, 0, 0, 0}},
			{"u", "t30936_empty"}:       {dataType: "bigint", valid: [4]bool{false, false, true, true}, values: [4]int64{0, 0, 20, 0}},
		}

		query := `select column_name, table_name, data_type,
       character_maximum_length, character_octet_length,
       numeric_precision, numeric_scale
from information_schema.columns
where table_schema = ? and table_name in ('t30936', 't30936_mb', 't30936_empty')
order by table_name, ordinal_position`

		// The reported reproduction read every CN and got identical wrong
		// rows on each of them; keep the cross-CN check as part of the proof.
		for _, cnIdx := range []int{0, 1} {
			cn, err := c.GetCNService(cnIdx)
			require.NoError(t, err)
			db := open(cn)
			defer db.Close()

			rows, err := db.QueryContext(ctx, query, schemaName)
			require.NoError(t, err)
			defer func() { require.NoError(t, rows.Close()) }()
			got := make(map[[2]string]struct {
				dataType string
				values   [4]sql.NullInt64
			})
			for rows.Next() {
				var column, table, dataType string
				var fields [4]sql.NullInt64
				require.NoError(t, rows.Scan(&column, &table, &dataType,
					&fields[0], &fields[1], &fields[2], &fields[3]))
				got[[2]string{column, table}] = struct {
					dataType string
					values   [4]sql.NullInt64
				}{dataType: dataType, values: fields}
			}
			require.NoError(t, rows.Err())

			for key, want := range expect {
				row, ok := got[key]
				require.True(t, ok, "CN %d: missing information_schema row for %s.%s",
					cnIdx, key[1], key[0])
				require.Equal(t, want.dataType, row.dataType,
					"CN %d: %s.%s data_type", cnIdx, key[1], key[0])
				for i := range want.values {
					require.Equal(t, want.valid[i], row.values[i].Valid,
						"CN %d: %s.%s metadata field %d NULL state", cnIdx, key[1], key[0], i)
					if want.valid[i] {
						require.Equal(t, want.values[i], row.values[i].Int64,
							"CN %d: %s.%s metadata field %d", cnIdx, key[1], key[0], i)
					}
				}
				delete(got, key)
			}
			// Every table column appears exactly once; the physical tables
			// carry no hidden metadata columns that survived listing.
			require.Empty(t, got, "CN %d: unexpected information_schema rows", cnIdx)
		}
	})
}
