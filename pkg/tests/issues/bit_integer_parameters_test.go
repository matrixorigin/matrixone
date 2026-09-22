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
	})
}
