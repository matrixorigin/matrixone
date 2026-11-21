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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// TestIssue31816HexBitLiteralDecimalValue pins the public #31816 contract on
// one real mysql wire connection served by one base-cluster admission: an
// IsBin HEX/BIT literal keeps its unsigned big-endian integer value in every
// DECIMAL width, an out-of-range payload surfaces the decoder's own
// invalid-input diagnostic instead of wrapping two's-complement negative, a
// rejected statement never masks the wide-carrier successes, and INSERT,
// column-default and INTERVAL consumers store the same integer value.
//
// Error contract (read from this tree, not from other batches' memory): the
// three ParseDecimal{64,128,256}FromByte range helpers raise
// moerr.NewInvalidInputNoCtxf, i.e. ErrInvalidInput (20301); moerr maps that
// code to ER_UNKNOWN_ERROR, so the frontend error rewrite falls back to the
// MO error code itself: the driver sees Number 20301, SQLState HY000 and the
// exact message "invalid input: binary literal beyond the range, can't be
// converted to DecimalXX(w,s).".
//
// Ownership: one fixed *sql.Conn carries every statement (the rejected
// INSERT included), each cursor/rows/stmt/db/conn is closed by an explicit
// defer, the owned database is registered for a bounded background-context
// cleanup that also runs on FailNow and never swallows its error, and the
// session's original sql_mode is restored independently after strict mode
// was set and read back.
func TestIssue31816HexBitLiteralDecimalValue(t *testing.T) {
	defer func() { require.NoError(t, embed.CloseBaseClusterTests()) }()
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		t.Run("issue_31816_hex_bit_decimal", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()

			cn, err := c.GetCNService(0)
			require.NoError(t, err)
			db, err := sql.Open("mysql",
				fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			defer db.Close()
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()

			// Strict mode is set explicitly and read back; the original mode
			// is restored from an independent bounded context that registers
			// as soon as the original value is read, so even a failed SET or
			// read-back still restores it.
			var origMode string
			require.NoError(t, conn.QueryRowContext(ctx, "select @@sql_mode").Scan(&origMode))
			defer func() {
				restoreCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
				defer stop()
				if _, err := conn.ExecContext(restoreCtx, "set sql_mode='"+strings.ReplaceAll(origMode, "'", "''")+"'"); err != nil {
					t.Errorf("restore sql_mode failed: %v", err)
				}
			}()
			_, err = conn.ExecContext(ctx, "set sql_mode='STRICT_TRANS_TABLES'")
			require.NoError(t, err)
			var modeNow string
			require.NoError(t, conn.QueryRowContext(ctx, "select @@sql_mode").Scan(&modeNow))
			require.Equal(t, "STRICT_TRANS_TABLES", modeNow)

			// Owned database with a bounded background-context cleanup that
			// survives a FailNow and refuses to swallow its own error.
			dbName := strings.ToLower(testutils.GetDatabaseName(t))
			_, err = conn.ExecContext(ctx, "create database `"+dbName+"`")
			require.NoError(t, err)
			defer func() {
				dropCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
				defer stop()
				if _, err := conn.ExecContext(dropCtx, "drop database `"+dbName+"`"); err != nil {
					t.Errorf("owned-db cleanup failed: %v", err)
				}
			}()
			_, err = conn.ExecContext(ctx, "use `"+dbName+"`")
			require.NoError(t, err)

			// Hand-written expected text only; decimal results are scanned as
			// exact strings, never as floats.
			row := conn.QueryRowContext(ctx, `select
				cast(x'41' as decimal(3,1)), cast(x'41' as decimal(5,2)),
				cast(b'1000001' as decimal(5,0)), cast(x'0100' as decimal(6,3)),
				cast(x'96' as decimal(5,2)), cast(x'00000096' as decimal(5,2)),
				cast(x'00' as decimal(5,2)), cast(x'000000000000000096' as decimal(5,2)),
				cast(null as decimal(5,2))`)
			var d41s1, d41s2, bit65, d256, d150, d150lz, zero, d150pad string
			var nullDecimal sql.NullString
			require.NoError(t, row.Scan(&d41s1, &d41s2, &bit65, &d256, &d150, &d150lz,
				&zero, &d150pad, &nullDecimal))
			require.Equal(t, "65.0", d41s1)
			require.Equal(t, "65.00", d41s2)
			require.Equal(t, "65", bit65)
			require.Equal(t, "256.000", d256)
			require.Equal(t, "150.00", d150)
			require.Equal(t, "150.00", d150lz)
			require.Equal(t, "0.00", zero)
			require.Equal(t, "150.00", d150pad, "9-byte payload: leading zeros only pad")
			require.False(t, nullDecimal.Valid, "NULL cast result stays NULL")

			requireBinaryRangeErr := func(query, decimalType string) {
				_, err := conn.ExecContext(ctx, query)
				var myErr *mysql.MySQLError
				require.ErrorAs(t, err, &myErr)
				require.Equal(t, uint16(20301), myErr.Number)
				require.Equal(t, [5]byte{'H', 'Y', '0', '0', '0'}, myErr.SQLState)
				require.Equal(t, "invalid input: binary literal beyond the range, can't be converted to "+decimalType+".",
					myErr.Message)
			}
			queryString := func(query string) string {
				var got string
				require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&got))
				return got
			}

			// The 2^64-51 payload is rejected by each statement's own typed
			// carrier: an explicit decimal(18,0) binds the physical DECIMAL128
			// carrier (width > 16), while decimal(10,2) binds DECIMAL64; each
			// wide-carrier success is a separate statement that no failure
			// can mask.
			requireBinaryRangeErr("select cast(x'FFFFFFFFFFFFFFCD' as decimal(18,0))", "Decimal128(18,0)")
			requireBinaryRangeErr("select cast(x'FFFFFFFFFFFFFFCD' as decimal(10,2))", "Decimal64(10,2)")
			require.Equal(t, "18446744073709551565.00",
				queryString("select cast(x'FFFFFFFFFFFFFFCD' as decimal(38,2))"))
			require.Equal(t, "18446744073709551565.00",
				queryString("select cast(x'FFFFFFFFFFFFFFCD' as decimal(65,2))"))
			// The 16-byte 0xff value passes the physical carrier and only the
			// declared decimal(38,2) bound rejects it.
			requireBinaryRangeErr("select cast(x'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF' as decimal(38,2))", "Decimal128(38,2)")
			require.Equal(t, "340282366920938463463374607431768211455.00",
				queryString("select cast(x'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF' as decimal(65,2))"))

			_, err = conn.ExecContext(ctx,
				"create table dec_bin(id int primary key, a decimal(5,2), b decimal(38,2))")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "insert into dec_bin values (1, x'96', x'96')")
			require.NoError(t, err)
			row = conn.QueryRowContext(ctx, "select a, b from dec_bin where id = 1")
			var a1, b1 string
			require.NoError(t, row.Scan(&a1, &b1))
			require.Equal(t, "150.00", a1)
			require.Equal(t, "150.00", b1)

			// The rejected overflow INSERT is a typed error, and the very
			// same conn keeps serving: SELECT 1, exact unchanged rows and the
			// next valid INSERT all prove values, not just row counts.
			_, err = conn.ExecContext(ctx, "insert into dec_bin values (2, x'FFFFFFFFFFFFFFCD', x'96')")
			var myErr *mysql.MySQLError
			require.ErrorAs(t, err, &myErr)
			require.Equal(t, uint16(20301), myErr.Number)
			var alive int
			require.NoError(t, conn.QueryRowContext(ctx, "select 1").Scan(&alive))
			require.Equal(t, 1, alive)
			var cnt int
			require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from dec_bin").Scan(&cnt))
			require.Equal(t, 1, cnt)
			var ua, ub string
			require.NoError(t, conn.QueryRowContext(ctx,
				"select a, b from dec_bin where id = 1").Scan(&ua, &ub))
			require.Equal(t, "150.00", ua)
			require.Equal(t, "150.00", ub)
			_, err = conn.ExecContext(ctx, "insert into dec_bin values (2, x'96', x'96')")
			require.NoError(t, err)

			type decRow struct {
				id int
				a  string
				b  string
			}
			var got []decRow
			rows, err := conn.QueryContext(ctx, "select id, a, b from dec_bin order by id")
			require.NoError(t, err)
			defer rows.Close()
			for rows.Next() {
				var r decRow
				require.NoError(t, rows.Scan(&r.id, &r.a, &r.b))
				got = append(got, r)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, []decRow{
				{id: 1, a: "150.00", b: "150.00"},
				{id: 2, a: "150.00", b: "150.00"},
			}, got)

			// DDL column default consumes the same literal decode.
			_, err = conn.ExecContext(ctx,
				"create table dec_def(id int primary key, a decimal(5,2) default x'96')")
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "insert into dec_def values (1, default)")
			require.NoError(t, err)
			var defA string
			require.NoError(t, conn.QueryRowContext(ctx,
				"select a from dec_def where id = 1").Scan(&defA))
			require.Equal(t, "150.00", defA)

			// INTERVAL consumes the decoder's full DECIMAL value: each HEX/BIT
			// literal goes through the public IsBin->ParseDecimal cast before
			// the DAY unit binds, while the ordinary integer keeps its own
			// control and every case still lands on the same spelled date.
			// The raw (uncast) "interval x'..'" spellings are a separate,
			// pre-existing frontend text-interval route (NULL in the round-18
			// raw evidence); that independent issue stays open and is NOT
			// claimed fixed by this batch.
			var day1, day2, day3 string
			require.NoError(t, conn.QueryRowContext(ctx, `select
				date_add('2020-01-01', interval cast(x'05' as decimal(10,2)) day),
				date_add('2020-01-01', interval cast(b'101' as decimal(10,2)) day),
				date_add('2020-01-01', interval 5 day)`).Scan(&day1, &day2, &day3))
			require.Equal(t, "2020-01-06", day1)
			require.Equal(t, "2020-01-06", day2)
			require.Equal(t, "2020-01-06", day3)

			// One prepared control: the literal stays inside the prepared SQL
			// while the parameter is ordinary row data; the statement is
			// repeated, sees a NULL parameter and succeeds after the typed
			// errors above on this same conn.
			stmt, err := conn.PrepareContext(ctx, "select cast(x'41' as decimal(3,1)), cast(? as char)")
			require.NoError(t, err)
			defer stmt.Close()
			var p1 string
			var p2 sql.NullString
			require.NoError(t, stmt.QueryRowContext(ctx, "7").Scan(&p1, &p2))
			require.Equal(t, "65.0", p1)
			require.True(t, p2.Valid)
			require.Equal(t, "7", p2.String)
			require.NoError(t, stmt.QueryRowContext(ctx, nil).Scan(&p1, &p2))
			require.Equal(t, "65.0", p1, "literal decode survives the NULL parameter")
			require.False(t, p2.Valid)
		})
	})
}
