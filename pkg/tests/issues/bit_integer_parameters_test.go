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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"net"
	"sync"
	"testing"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

// Reuse the existing prepared-source cluster. No additional service or table
// is needed to observe the real SQL EXECUTE and COM_STMT source boundaries.
func testBitIntegerPreparedParameters(t *testing.T, ctx context.Context, db *sql.DB, port int64) {
	t.Run("binary source transitions", func(t *testing.T) {
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		stmt, err := conn.PrepareContext(ctx, `select hex(?),hex(char(?)),make_set(?,"a","b","c"),export_set(?,"Y","N","",4)`)
		require.NoError(t, err)
		defer stmt.Close()
		for _, tc := range []struct {
			input   any
			want    [4]string
			null    bool
			wantErr bool
		}{
			{nil, [4]string{"", "", "", ""}, true, false},
			{float64(1.5), [4]string{"2", "02", "b", "NYNN"}, false, false},
			{"1.5", [4]string{"312E35", "01", "a", "YNNN"}, false, false},
			{true, [4]string{"1", "01", "a", "YNNN"}, false, false},
			{uint64(math.MaxUint64), [4]string{"FFFFFFFFFFFFFFFF", "FFFFFFFF", "a,b,c", "YYYY"}, false, false},
			{"18446744073709551616", [4]string{}, false, true},
			{float64(-1.5), [4]string{"FFFFFFFFFFFFFFFE", "FFFFFFFE", "b,c", "NYYY"}, false, false},
			{nil, [4]string{}, true, false},
			{float64(1.5), [4]string{"2", "02", "b", "NYNN"}, false, false},
		} {
			t.Run(fmt.Sprint(tc.input), func(t *testing.T) {
				var got [4]sql.NullString
				err := stmt.QueryRowContext(ctx, tc.input, tc.input, tc.input, tc.input).Scan(&got[0], &got[1], &got[2], &got[3])
				if tc.wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				for i := range got {
					require.Equal(t, !tc.null || i == 1, got[i].Valid)
					require.Equal(t, tc.want[i], got[i].String)
				}
			})
		}
	})
	t.Run("SQL execute decimal and selector", func(t *testing.T) {
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		_, err = conn.ExecContext(ctx, `prepare bit_parameter_source from 'select export_set(if(true,?,cast("18446744073709551615" as unsigned)),"Y","N","",4)'`)
		require.NoError(t, err)
		defer func() {
			_, err := conn.ExecContext(ctx, "deallocate prepare bit_parameter_source")
			require.NoError(t, err)
		}()
		for _, tc := range []struct {
			source, want string
			wantErr      bool
		}{
			{"cast(1.40 as decimal(5,2))", "YNNN", false},
			{"cast(1.50 as decimal(5,2))", "NYNN", false},
			{"2.5e0", "NYNN", false},
			{"2.5", "YYNN", false},
			{"'-2tail'", "NYYY", false},
			{"cast('9223372036854775808' as decimal(20,0))", "", true},
			{"true", "YNNN", false},
		} {
			t.Run(tc.source, func(t *testing.T) {
				_, err := conn.ExecContext(ctx, "set @bit_parameter_value="+tc.source)
				require.NoError(t, err)
				var got string
				err = conn.QueryRowContext(ctx, "execute bit_parameter_source using @bit_parameter_value").Scan(&got)
				if tc.wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			})
		}
	})
	t.Run("SQL execute HEX numeric selector", func(t *testing.T) {
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		_, err = conn.ExecContext(ctx, `prepare hex_numeric_selector from 'select hex(if(true,?,2.5e0))'`)
		require.NoError(t, err)
		defer func() {
			_, err := conn.ExecContext(ctx, "deallocate prepare hex_numeric_selector")
			require.NoError(t, err)
		}()
		for _, tc := range []struct {
			source, want string
		}{
			{"1.5e0", "2"},
			{"-1.5e0", "FFFFFFFFFFFFFFFE"},
			{"cast(2.5 as decimal(2,1))", "3"},
			{"cast(9007199254740993 as decimal(20,0))", "20000000000001"},
			{"cast(18446744073709551615 as unsigned)", "FFFFFFFFFFFFFFFF"},
		} {
			_, err = conn.ExecContext(ctx, "set @hex_selector_value="+tc.source)
			require.NoError(t, err)
			var got string
			require.NoError(t, conn.QueryRowContext(ctx, "execute hex_numeric_selector using @hex_selector_value").Scan(&got))
			require.Equal(t, tc.want, got)
		}
		_, err = conn.ExecContext(ctx, "deallocate prepare hex_numeric_selector")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, `prepare hex_numeric_selector from 'select hex(if(true,?,"peer"))'`)
		require.NoError(t, err)
		for _, tc := range []struct {
			source, want string
		}{
			{"1.5e0", "312E35"},
			{"-1.5e0", "2D312E35"},
			{"cast(2.5 as decimal(2,1))", "322E35"},
		} {
			_, err = conn.ExecContext(ctx, "set @hex_selector_value="+tc.source)
			require.NoError(t, err)
			var got string
			require.NoError(t, conn.QueryRowContext(ctx, "execute hex_numeric_selector using @hex_selector_value").Scan(&got))
			require.Equal(t, tc.want, got)
		}
		_, err = conn.ExecContext(ctx, "deallocate prepare hex_numeric_selector")
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, `prepare hex_numeric_selector from 'select hex(if(true,if(true,?,2.5e0),"peer"))'`)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "set @hex_selector_value=1.5e0")
		require.NoError(t, err)
		var got string
		require.NoError(t, conn.QueryRowContext(ctx, "execute hex_numeric_selector using @hex_selector_value").Scan(&got))
		require.Equal(t, "312E35", got)
	})
	t.Run("IFNULL common value versus CASE source", func(t *testing.T) {
		for _, tc := range []struct {
			query, want string
		}{
			{`select hex(ifnull(cast(2.5 as decimal(20,1)),1.5e0))`, "2"},
			{`select hex(case when true then cast(2.5 as decimal(20,1)) else 1.5e0 end)`, "3"},
		} {
			var got string
			require.NoError(t, db.QueryRowContext(ctx, tc.query).Scan(&got))
			require.Equal(t, tc.want, got, tc.query)
		}
	})
	t.Run("IFNULL common value across prepared consumers", func(t *testing.T) {
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		for _, tc := range []struct {
			query, want string
			input       any
		}{
			{`select hex(ifnull(?,1.5e0))`, "2", float64(2.5)},
			{`select hex(if(true,ifnull(cast(2.5 as decimal(20,1)),1.5e0),0))`, "2", nil},
			{`select hex(if(true,ifnull(?,1.5e0),0))`, "2", float64(2.5)},
			{`select hex(ifnull(cast(? as decimal(20,1)),1.5e0))`, "2", "2.5"},
			{`select hex(ifnull(?,1.5e0))`, "2", nil},
			{`select hex(char(ifnull(?,1.5e0)))`, "02", float64(2.5)},
			{`select make_set(ifnull(?,1.5e0),'a','b','c')`, "b", float64(2.5)},
			{`select export_set(ifnull(?,1.5e0),'Y','N','',4)`, "NYNN", float64(2.5)},
			{`select hex(char(ifnull(cast(2.5 as decimal(20,1)),1.5e0)))`, "02", nil},
			{`select make_set(ifnull(cast(2.5 as decimal(20,1)),1.5e0),'a','b','c')`, "b", nil},
			{`select export_set(ifnull(cast(2.5 as decimal(20,1)),1.5e0),'Y','N','',4)`, "NYNN", nil},
		} {
			t.Run(tc.query+fmt.Sprint(tc.input), func(t *testing.T) {
				stmt, err := conn.PrepareContext(ctx, tc.query)
				require.NoError(t, err)
				defer stmt.Close()
				var got string
				if tc.input == nil && tc.query != `select hex(ifnull(?,1.5e0))` {
					err = stmt.QueryRowContext(ctx).Scan(&got)
				} else {
					err = stmt.QueryRowContext(ctx, tc.input).Scan(&got)
				}
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			})
		}
	})
	t.Run("SQL EXECUTE IFNULL decimal common value", func(t *testing.T) {
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		_, err = conn.ExecContext(ctx, `prepare hex_ifnull_common from 'select hex(ifnull(?,1.5e0))'`)
		require.NoError(t, err)
		defer func() {
			_, err := conn.ExecContext(ctx, "deallocate prepare hex_ifnull_common")
			require.NoError(t, err)
		}()
		_, err = conn.ExecContext(ctx, `set @ifnull_value=cast(2.5 as decimal(20,1))`)
		require.NoError(t, err)
		var got string
		require.NoError(t, conn.QueryRowContext(ctx, "execute hex_ifnull_common using @ifnull_value").Scan(&got))
		require.Equal(t, "2", got)
	})
	t.Run("COM_STMT numeric and text coalesce", func(t *testing.T) {
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		for _, tc := range []struct {
			query string
			cases []struct {
				input any
				want  string
			}
		}{
			{`select hex(coalesce(?, 2.5e0))`, []struct {
				input any
				want  string
			}{{float64(1.5), "2"}, {nil, "2"}, {int64(-2), "FFFFFFFFFFFFFFFE"}}},
			{`select hex(coalesce(?, 'peer'))`, []struct {
				input any
				want  string
			}{{float64(1.5), "312E35"}, {nil, "70656572"}}},
			{`select hex(coalesce(?, binary 'fallback'))`, []struct {
				input any
				want  string
			}{{"A", "41"}, {nil, "66616C6C6261636B"}}},
		} {
			t.Run(tc.query, func(t *testing.T) {
				stmt, err := conn.PrepareContext(ctx, tc.query)
				require.NoError(t, err)
				defer stmt.Close()
				for _, test := range tc.cases {
					var got string
					require.NoError(t, stmt.QueryRowContext(ctx, test.input).Scan(&got))
					require.Equal(t, test.want, got)
				}
			})
		}
	})
	t.Run("SQL execute binary coalesce", func(t *testing.T) {
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		_, err = conn.ExecContext(ctx, `prepare hex_binary_coalesce from 'select hex(coalesce(?, binary \'fallback\'))'`)
		require.NoError(t, err)
		defer func() {
			_, err := conn.ExecContext(ctx, "deallocate prepare hex_binary_coalesce")
			require.NoError(t, err)
		}()
		_, err = conn.ExecContext(ctx, `set @hex_binary_value=binary 'A\0B'`)
		require.NoError(t, err)
		var got string
		require.NoError(t, conn.QueryRowContext(ctx, "execute hex_binary_coalesce using @hex_binary_value").Scan(&got))
		require.Equal(t, "410042", got)
	})
	t.Run("binary decimal descriptor", func(t *testing.T) {
		var mu sync.Mutex
		var wire *issue28989TemporalTypeConn
		const network = "bitintegerdecimal"
		mysqlDriver.RegisterDialContext(network, func(ctx context.Context, addr string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
			if err != nil {
				return nil, err
			}
			wrapped := &issue28989TemporalTypeConn{Conn: conn}
			mu.Lock()
			wire = wrapped
			mu.Unlock()
			return wrapped, nil
		})
		defer mysqlDriver.DeregisterDialContext(network)
		rawDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@%s(127.0.0.1:%d)/?interpolateParams=false", network, port))
		require.NoError(t, err)
		defer rawDB.Close()
		rawDB.SetMaxOpenConns(1)
		rawDB.SetMaxIdleConns(1)
		stmt, err := rawDB.PrepareContext(ctx, `select export_set(?,"Y","N","",4)`)
		require.NoError(t, err)
		defer stmt.Close()
		mu.Lock()
		captured := wire
		mu.Unlock()
		require.NotNil(t, captured)
		for _, tc := range []struct {
			decimal, want string
			wantErr       bool
		}{
			{"1.40", "YNNN", false}, {"1.400", "YNNN", false}, {"1.5", "NYNN", false},
			{"-1.5", "NYYY", false}, {"9223372036854775808", "", true}, {"2.5", "YYNN", false},
		} {
			t.Run(tc.decimal, func(t *testing.T) {
				payload := append([]byte{byte(len(tc.decimal))}, tc.decimal...)
				captured.rewriteNext(defines.MYSQL_TYPE_NEWDECIMAL, payload)
				var got string
				err := stmt.QueryRowContext(ctx, "placeholder").Scan(&got)
				require.True(t, captured.wasRewritten())
				if tc.wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			})
		}
		ifnullStmt, err := rawDB.PrepareContext(ctx, `select hex(ifnull(?,1.5e0))`)
		require.NoError(t, err)
		defer ifnullStmt.Close()
		captured.rewriteNext(defines.MYSQL_TYPE_NEWDECIMAL, []byte{3, '2', '.', '5'})
		var got string
		require.NoError(t, ifnullStmt.QueryRowContext(ctx, "placeholder").Scan(&got))
		require.True(t, captured.wasRewritten())
		require.Equal(t, "2", got)
	})
}
