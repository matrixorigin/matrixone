// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/stretchr/testify/require"
)

func TestOutfileCSVRestoresStringAndBITValues(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		admin, err := sql.Open("mysql", fmt.Sprintf("sys#root#moadmin:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer admin.Close()
		exec := func(query string) {
			t.Helper()
			_, err := admin.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		const db = "outfile_csv_30702"
		exec("create database " + db)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, err := admin.ExecContext(cleanupCtx, "drop database "+db)
			require.NoError(t, err)
		}()
		exec("create table " + db + ".strings_src(id int primary key,s varchar(128))")
		values := []string{"plain", "", "a,b", "a\nb", "a\rb", "a\r\nb", "tail\r", `\N`, "NULL", `back\slash`, `"first`, "a::b||c", "你好"}
		stringTuples := make([]string, 0, len(values))
		for i, value := range values {
			stringTuples = append(stringTuples, fmt.Sprintf("(%d,cast(x'%s' as char))", i, hex.EncodeToString([]byte(value))))
		}
		exec("insert into " + db + ".strings_src values " + strings.Join(stringTuples, ",") + ",(99,null)")
		exec("create table " + db + ".bits_src(id int primary key,b bit(8))")
		var tuples []string
		for i := 0; i < 256; i++ {
			tuples = append(tuples, fmt.Sprintf("(%d,%d)", i, i))
		}
		exec("insert into " + db + ".bits_src values " + strings.Join(tuples, ",") + ",(999,null)")
		exec("create table " + db + ".wide_bits_src(id int primary key,b bit(32))")
		// Include multi-byte values a0bc, abbc, anbc, arbc, atbc and aZbc.
		exec("insert into " + db + ".wide_bits_src values (1,1314212940),(2,23630),(3,null),(4,1630560867),(5,1633837667),(6,1634624099),(7,1634886243),(8,1635017315),(9,1633313379)")
		exec("create table " + db + ".decimal_src(id int primary key,n decimal(9,2))")
		exec("insert into " + db + ".decimal_src values (1,12.34),(2,-5.67),(3,null)")
		exec("create table " + db + ".bits24_src(id int primary key,b bit(24))")
		// Leading/middle CR, including CR followed by ':' to collide with
		// the complete multi-byte 'r:' separator after escape translation.
		exec("insert into " + db + ".bits24_src values (1,6360418),(2,6382178),(3,null),(4,876898),(5,866914),(6,6360378),(7,6385250)")
		read := func(t *testing.T, ctx context.Context, table, column string) []string {
			t.Helper()
			rows, err := admin.QueryContext(ctx, "select id,hex("+column+") from "+table+" order by id")
			require.NoError(t, err)
			defer rows.Close()
			var result []string
			for rows.Next() {
				var id int
				var value sql.NullString
				require.NoError(t, rows.Scan(&id, &value))
				result = append(result, fmt.Sprintf("%d:%t:%s", id, value.Valid, value.String))
			}
			require.NoError(t, rows.Err())
			return result
		}
		cases := []struct {
			name, source, column, field, line, quote, extra string
		}{
			{"string-unenclosed", "strings_src", "s", ",", "\n", "", ""},
			{"string-enclosed", "strings_src", "s", ",", "\n", `"`, ""},
			{"string-custom", "strings_src", "s", "::", "||", "", ""},
			{"string-crlf", "strings_src", "s", ",", "\r\n", "", ""},
			{"bit-default", "bits_src", "b", ",", "\n", `"`, ""},
			{"bit-unenclosed", "bits_src", "b", ",", "\n", "", ""},
			{"bit-force-quote", "bits_src", "b", ",", "\n", `"`, " FORCE_QUOTE (b)"},
			{"bit32-default", "wide_bits_src", "b", ",", "\n", `"`, ""},
			{"bit32-unenclosed", "wide_bits_src", "b", ",", "\n", "", ""},
			{"decimal-dot-terminator", "decimal_src", "n", ".", "\n", "", ""},
			{"bit-translated-terminator", "bits_src", "b", "n", "\n", `"`, ""},
			{"bit-N-field-terminator", "bits_src", "b", "N:", "\n", `"`, ""},
			{"bit-N-line-terminator", "bits_src", "b", ",", "N:", `"`, ""},
			{"bit-N-field-force-quote", "bits_src", "b", "N:", "\n", `"`, " FORCE_QUOTE (b)"},
			{"bit-N-line-force-quote", "bits_src", "b", ",", "N:", `"`, " FORCE_QUOTE (b)"},
			{"bit-enclosed-0", "bits_src", "b", ",", "\n", "0", ""},
			{"bit-enclosed-b", "bits_src", "b", ",", "\n", "b", ""},
			{"bit-enclosed-n", "bits_src", "b", ",", "\n", "n", ""},
			{"bit-enclosed-r", "bits_src", "b", ",", "\n", "r", ""},
			{"bit-enclosed-t", "bits_src", "b", ",", "\n", "t", ""},
			{"bit-enclosed-Z", "bits_src", "b", ",", "\n", "Z", ""},
			{"bit-enclosed-N", "bits_src", "b", ",", "\n", "N", ""},
			{"bit32-enclosed-0", "wide_bits_src", "b", ",", "\n", "0", ""},
			{"bit32-enclosed-b", "wide_bits_src", "b", ",", "\n", "b", ""},
			{"bit32-enclosed-n", "wide_bits_src", "b", ",", "\n", "n", ""},
			{"bit32-enclosed-r", "wide_bits_src", "b", ",", "\n", "r", ""},
			{"bit32-enclosed-t", "wide_bits_src", "b", ",", "\n", "t", ""},
			{"bit32-enclosed-Z", "wide_bits_src", "b", ",", "\n", "Z", ""},
			{"enclosure-inside-field-terminator", "bits_src", "b", "::0", "\n", "0", ""},
			{"enclosure-inside-line-terminator", "bits_src", "b", ",", "||0", "0", ""},
			{"escape-field", "bits24_src", "b", `\`, "\n", `"`, ""},
			{"escape-line", "bits24_src", "b", ",", `\`, `"`, ""},
			{"escape-r-field", "bits24_src", "b", `\r`, "\n", `"`, ""},
			{"escape-r-line", "bits24_src", "b", ",", `\r`, `"`, ""},
			{"escape-field-force", "bits24_src", "b", `\`, "\n", `"`, " FORCE_QUOTE (b)"},
			{"escape-line-force", "bits24_src", "b", ",", `\`, `"`, " FORCE_QUOTE (b)"},
			{"escape-enclosure", "bits24_src", "b", ",", "\n", `\`, ""},
			{"escape-enclosure-force", "bits24_src", "b", ",", "\n", `\`, " FORCE_QUOTE (b)"},
			{"escape-enclosure-all-bytes", "bits_src", "b", ",", "\n", `\`, ""},
			{"escape-enclosure-strings", "strings_src", "s", ",", "\n", `\`, ""},
			{"escape-enclosure-r-field", "bits24_src", "b", "r", "\n", `\`, ""},
			{"escape-enclosure-r-field-force", "bits24_src", "b", "r", "\n", `\`, " FORCE_QUOTE (b)"},
			{"escape-enclosure-r-colon-field", "bits24_src", "b", "r:", "\n", `\`, ""},
			{"escape-enclosure-r-colon-field-force", "bits24_src", "b", "r:", "\n", `\`, " FORCE_QUOTE (b)"},
			{"cr-enclosure-custom-line", "bits24_src", "b", `\`, "||", "\r", ""},
			{"cr-enclosure-custom-line-force", "bits24_src", "b", `\`, "||", "\r", " FORCE_QUOTE (b)"},
			{"cr-enclosure-custom-line-n-colon", "bits24_src", "b", "n:", "||", "\r", ""},
			{"cr-enclosure-custom-line-n-colon-force", "bits24_src", "b", "n:", "||", "\r", " FORCE_QUOTE (b)"},
		}
		expected := make(map[string][]string)
		for _, tc := range cases {
			if _, exists := expected[tc.source]; exists {
				continue
			}
			exec("create table " + db + ".restored_" + tc.source + " like " + db + "." + tc.source)
			expected[tc.source] = read(t, ctx, db+"."+tc.source, tc.column)
		}
		// Setup owns a separate budget. Every case remains runnable after it ends.
		cancel()
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
				defer cancel()
				exec := func(query string) {
					t.Helper()
					_, err := admin.ExecContext(ctx, query)
					require.NoError(t, err, query)
				}
				path := filepath.Join(t.TempDir(), "rows.csv")
				dst := db + ".restored_" + tc.source
				exec("truncate table " + dst)
				fields := " FIELDS TERMINATED BY " + sqlquote.String(tc.field)
				quote := sqlquote.String(tc.quote)
				lines := " LINES TERMINATED BY " + sqlquote.String(tc.line)
				exec("select id," + tc.column + " from " + db + "." + tc.source + " order by id INTO OUTFILE " + sqlquote.String(path) + fields + " ENCLOSED BY " + quote + lines + " HEADER 'false'" + tc.extra)
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				require.NotEmpty(t, data)
				if tc.name == "string-unenclosed" {
					require.Contains(t, string(data), "a\\,b")
					require.Contains(t, string(data), "a\\\nb")
				}
				if strings.HasPrefix(tc.name, "bit-N-") {
					// The non-NULL BIT byte 78 must be framed as "N", not
					// emitted as the raw \N sentinel used by the genuine NULL.
					require.Contains(t, string(data), `"N"`)
				}
				exec("LOAD DATA INFILE " + sqlquote.String(path) + " INTO TABLE " + dst + fields + " OPTIONALLY ENCLOSED BY " + quote + lines)
				require.Equal(t, expected[tc.source], read(t, ctx, dst, tc.column))
			})
		}
		for _, tc := range []struct {
			name, field, line, quote string
			value                    int
			source                   string
		}{
			{"field-zero", "0:", "\n", "0", 48, "bits_src"},
			{"line-zero", ",", "0:", "0", 48, "bits_src"},
			{"line-n", ",", "n:", "n", 110, "bits_src"},
			{"line-N", ",", "N:", "N", 78, "bits_src"},
			{"normalized-crlf", `\`, "\r\n", "\n", 65, "bits_src"},
			{"cr-with-lf-escape-field", `\`, "\n", "\r", 2, "bits24_src"},
			{"cr-with-lf-n-colon-field", "n:", "\n", "\r", 7, "bits24_src"},
		} {
			for _, force := range []bool{false, true} {
				t.Run(fmt.Sprintf("enclosure-prefix/%s/force=%t", tc.name, force), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
					defer cancel()
					path := filepath.Join(t.TempDir(), "rejected.csv")
					options := " FIELDS TERMINATED BY " + sqlquote.String(tc.field) + " ENCLOSED BY " + sqlquote.String(tc.quote) +
						" LINES TERMINATED BY " + sqlquote.String(tc.line)
					extra := ""
					if force {
						extra = " FORCE_QUOTE (b)"
					}
					_, err := admin.ExecContext(ctx, fmt.Sprintf("select 101 as id,b from %s.%s where id=%d INTO OUTFILE %s%s HEADER 'false'%s",
						db, tc.source, tc.value, sqlquote.String(path), options, extra))
					if err == nil {
						// Diagnose an incorrectly accepted format using the actual
						// emitted bytes and LOAD, not an extracted encoder probe.
						data, readErr := os.ReadFile(path)
						require.NoError(t, readErr)
						_, createErr := admin.ExecContext(ctx, "create table "+db+".rejected_restore like "+db+"."+tc.source)
						require.NoError(t, createErr)
						defer func() {
							cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
							defer cleanupCancel()
							_, dropErr := admin.ExecContext(cleanupCtx, "drop table "+db+".rejected_restore")
							require.NoError(t, dropErr)
						}()
						_, loadErr := admin.ExecContext(ctx, "LOAD DATA INFILE "+sqlquote.String(path)+" INTO TABLE "+db+".rejected_restore"+options)
						t.Logf("accepted OUTFILE bytes=%x; LOAD error=%v", data, loadErr)
					}
					require.ErrorContains(t, err, "ENCLOSED BY must not be the first byte")
					require.NoFileExists(t, path)
				})
			}
		}
		for _, options := range []string{
			" FIELDS TERMINATED BY 'n' ENCLOSED BY ''",
			" FIELDS TERMINATED BY 'N:' ENCLOSED BY ''",
			" FIELDS ENCLOSED BY '' LINES TERMINATED BY 'N:'",
			" FIELDS TERMINATED BY " + sqlquote.String(`\`) + " ENCLOSED BY ''",
			" FIELDS TERMINATED BY " + sqlquote.String(`\r`) + " ENCLOSED BY ''",
			" FIELDS ENCLOSED BY '' LINES TERMINATED BY " + sqlquote.String(`\`),
		} {
			t.Run("unrepresentable-bare-format"+options, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
				defer cancel()
				path := filepath.Join(t.TempDir(), "rejected.csv")
				_, err := admin.ExecContext(ctx, "select id,s from "+db+".strings_src INTO OUTFILE "+sqlquote.String(path)+
					options+" HEADER 'false'")
				require.ErrorContains(t, err, "require a nonempty ENCLOSED BY")
				_, err = os.Stat(path)
				require.ErrorIs(t, err, os.ErrNotExist)
			})
		}
	})
}
