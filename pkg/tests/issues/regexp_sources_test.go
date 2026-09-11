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
	"net"
	"sync"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
)

// The Go driver sends []byte as STRING, not BLOB. Rewrite only the type
// descriptors of these two length-encoded parameters, and record what was
// actually sent. SQL PREPARE cannot substitute for this protocol evidence.
type regexpTypeConn struct {
	net.Conn
	mu    sync.Mutex
	mask  int
	types [2]byte
	seen  bool
}

func (c *regexpTypeConn) Write(data []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for pos := 0; pos+4 <= len(data); {
		n := int(data[pos]) | int(data[pos+1])<<8 | int(data[pos+2])<<16
		end := pos + 4 + n
		if end > len(data) {
			break
		}
		payload := data[pos+4 : end]
		// COM_STMT_EXECUTE header (10), NULL bitmap (1), new types flag (1).
		if len(payload) >= 16 && payload[0] == 0x17 && payload[11] == 1 {
			data = append([]byte(nil), data...)
			payload = data[pos+4 : end]
			for i := 0; i < 2; i++ {
				if c.mask&(1<<i) != 0 {
					payload[12+2*i] = 252
				}
				c.types[i] = payload[12+2*i]
			}
			c.seen = true
		}
		pos = end
	}
	return c.Conn.Write(data)
}

func TestRegexpPreparedProtocolSources(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		var wire *regexpTypeConn
		const network = "regexp-source-types"
		mysql.RegisterDialContext(network, func(ctx context.Context, addr string) (net.Conn, error) {
			connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
			if err != nil {
				return nil, err
			}
			wire = &regexpTypeConn{Conn: connection}
			return wire, nil
		})
		defer mysql.DeregisterDialContext(network)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@%s(127.0.0.1:%d)/?interpolateParams=false",
			network, cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		connection, err := db.Conn(ctx)
		require.NoError(t, err)
		defer connection.Close()
		withStatement := func(t *testing.T, query string, run func(*sql.Stmt)) {
			t.Helper()
			stmt, err := connection.PrepareContext(ctx, query)
			require.NoError(t, err)
			defer func() { require.NoError(t, stmt.Close()) }()
			run(stmt)
		}

		for _, query := range []string{
			"select regexp_instr(?, _binary'a')",
			"select regexp_instr(cast(NULL as binary), ?)",
			"select regexp_instr(regexp_substr(?, ?), _binary'.')",
		} {
			stmt, err := connection.PrepareContext(ctx, query)
			if stmt != nil {
				func() {
					defer func() { require.NoError(t, stmt.Close()) }()
				}()
			}
			require.Error(t, err, "COM_STMT_PREPARE must reject before any runtime BLOB value exists")
			var sqlError *mysql.MySQLError
			require.ErrorAs(t, err, &sqlError)
			require.Equal(t, uint16(3995), sqlError.Number)
		}

		type observation struct {
			value sql.NullString
			typ   string
		}
		observe := func(t *testing.T, stmt *sql.Stmt, mask int, subject, pattern string) observation {
			t.Helper()
			wire.mu.Lock()
			wire.mask, wire.seen = mask, false
			wire.mu.Unlock()
			rows, err := stmt.QueryContext(ctx, subject, pattern)
			require.NoError(t, err)
			defer func() { require.NoError(t, rows.Close()) }()
			columns, err := rows.ColumnTypes()
			require.NoError(t, err)
			require.Len(t, columns, 1)
			got := observation{typ: columns[0].DatabaseTypeName()}
			require.True(t, rows.Next())
			require.NoError(t, rows.Scan(&got.value))
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
			wire.mu.Lock()
			seen, sent := wire.seen, wire.types
			wire.mu.Unlock()
			require.True(t, seen, "no COM_STMT_EXECUTE type descriptors observed")
			for i := 0; i < 2; i++ {
				if mask&(1<<i) != 0 {
					require.Equal(t, byte(252), sent[i])
				} else {
					require.Equal(t, byte(254), sent[i])
				}
			}
			return got
		}
		// MySQL 8.4.8 oracle. Each mask independently changes subject and pattern.
		for _, tc := range []struct {
			name, query, subject, pattern string
			want                          [4]string
			textResult                    bool
		}{
			{"positions", "select regexp_instr(?, ?)", "éa", "a", [4]string{"2", "3", "2", "3"}, false},
			{"predicate", "select regexp_like(?, ?)", "éa", "é", [4]string{"1", "0", "0", "1"}, false},
			{"substr_encoding", "select regexp_substr(?, ?)", "éa", ".", [4]string{"é", "Ã", "é", "Ã"}, true},
			{"replace_encoding", "select regexp_replace(?, ?, 'X')", "éa", "a", [4]string{"éX", "Ã©X", "éX", "Ã©X"}, true},
			{"replacement_unicode", "select regexp_replace(?, ?, '中')", "éa", ".", [4]string{"中中", "中中中", "中中", "中中中"}, true},
			{"invalid_subject", "select regexp_instr(?, ?)", "\xffa", "a", [4]string{"0", "2", "0", "2"}, false},
			{"invalid_subject_regexp", "select ? regexp ?", "\xffa", "a", [4]string{"0", "1", "0", "1"}, false},
			{"invalid_subject_rlike", "select ? rlike ?", "\xffa", "a", [4]string{"0", "1", "0", "1"}, false},
			{"invalid_subject_like", "select regexp_like(?, ?)", "\xffa", "a", [4]string{"0", "1", "0", "1"}, false},
			{"invalid_pattern", "select regexp_instr(?, ?)", "abc", "a\xffb", [4]string{"1", "1", "0", "0"}, false},
			{"invalid_pattern_regexp", "select ? regexp ?", "abc", "a\xffb", [4]string{"1", "1", "0", "0"}, false},
			{"invalid_pattern_rlike", "select ? rlike ?", "abc", "a\xffb", [4]string{"1", "1", "0", "0"}, false},
			{"invalid_pattern_like", "select regexp_like(?, ?)", "abc", "a\xffb", [4]string{"1", "1", "0", "0"}, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				withStatement(t, tc.query, func(stmt *sql.Stmt) {
					for _, mask := range []int{1, 0, 1, 2, 0, 2, 3, 0, 3} {
						got := observe(t, stmt, mask, tc.subject, tc.pattern)
						require.Equal(t, sql.NullString{String: tc.want[mask], Valid: true}, got.value, "mask=%d", mask)
						if tc.textResult {
							wantType := "VARCHAR"
							if tc.name != "substr_encoding" {
								// MySQL uses LONGTEXT for REPLACE's expansion bound;
								// MO represents its unbounded text domain as TEXT.
								wantType = "TEXT"
							}
							require.Equal(t, wantType, got.typ, "marker metadata must not depend on the current BLOB packet")
						}
						withStatement(t, tc.query, func(fresh *sql.Stmt) {
							freshGot := observe(t, fresh, mask, tc.subject, tc.pattern)
							require.Equal(t, freshGot, got, "reuse must equal a fresh statement")
						})
					}
				})
			})
		}
		t.Run("instr_rebased_text_suffix", func(t *testing.T) {
			for _, tc := range []struct {
				subject, pattern string
				position         int64
			}{
				{"\xffa", "a", 2},   // invalid byte is before pos and is discarded
				{"a\xff", ".", 0},   // suffix begins with a truncatable byte
				{"aa\xff", "a", 2},  // invalid byte is after the match
				{"a\xe0b", "a", 0},  // pos=2 suffix truncates to empty
				{"a\xf0bb", "a", 0}, // F0-FF require four remaining bytes
				{"a\xf5b", "a", 0},
			} {
				withStatement(t, "select regexp_instr(?, ?, 2)", func(stmt *sql.Stmt) {
					got := observe(t, stmt, 0, tc.subject, tc.pattern)
					require.Equal(t, sql.NullString{String: fmt.Sprint(tc.position), Valid: true}, got.value, tc)
				})
			}
		})
		t.Run("malformed_text_errors", func(t *testing.T) {
			for _, subject := range []string{
				"a\xc3b", "\x80", "\xc0", "\xc1", "\xc2 ", "\xe0\x80\x80", "\xed\xa0\x80", "\xf4\x90\x80\x80",
				"a\xffbbb", "a\xf5bbb",
			} {
				for _, query := range []string{
					"select ? regexp ?", "select regexp_like(?, ?)", "select regexp_instr(?, ?)",
					"select regexp_substr(?, ?)", "select regexp_replace(?, ?, 'X')",
				} {
					withStatement(t, query, func(stmt *sql.Stmt) {
						wire.mu.Lock()
						wire.mask = 0
						wire.mu.Unlock()
						var value any
						err := stmt.QueryRowContext(ctx, subject, "a").Scan(&value)
						require.Error(t, err, "%s %x", query, subject)
						var sqlError *mysql.MySQLError
						require.ErrorAs(t, err, &sqlError)
						require.Equal(t, uint16(3854), sqlError.Number)
					})
				}
			}
		})
		t.Run("malformed_pattern_and_replacement_precede_null", func(t *testing.T) {
			for _, query := range []string{
				"select regexp_instr(?, ?)",
				"select regexp_substr(?, ?)",
				"select regexp_replace('abc', ?, ?)",
			} {
				withStatement(t, query, func(stmt *sql.Stmt) {
					var value any
					err := stmt.QueryRowContext(ctx, nil, "a\xc3b").Scan(&value)
					require.Error(t, err)
					var sqlError *mysql.MySQLError
					require.ErrorAs(t, err, &sqlError)
					require.Equal(t, uint16(3854), sqlError.Number)
				})
			}
		})
		t.Run("pattern_and_position_precede_operand_conversion", func(t *testing.T) {
			for _, tc := range []struct {
				query       string
				first, last any
			}{
				{"select regexp_replace(?, ?, 'X')", "a\xc3b", "["},
				{"select regexp_replace('abc', ?, ?)", "[", "X\xc3Y"},
				{"select regexp_replace('abc', ?, ?)", "", "X\xc3Y"},
				{"select regexp_substr(?, 'a', ?)", "a\xc3b", int64(0)},
				{"select regexp_replace(?, 'a', 'X', ?)", "a\xc3b", int64(-1)},
				{"select regexp_replace('abc', 'a', ?, ?)", "X\xc3Y", int64(0)},
			} {
				withStatement(t, tc.query, func(stmt *sql.Stmt) {
					var value any
					err := stmt.QueryRowContext(ctx, tc.first, tc.last).Scan(&value)
					require.Error(t, err, tc.query)
					var sqlError *mysql.MySQLError
					require.ErrorAs(t, err, &sqlError)
					require.NotEqual(t, uint16(3854), sqlError.Number, tc.query)
				})
			}
		})
		t.Run("replace_optional_null_precedence", func(t *testing.T) {
			for _, tc := range []struct {
				query string
				args  []any
				err   bool
			}{
				{"select regexp_replace(?, ?, ?, ?)", []any{"abc", "a\xc3b", "X", nil}, true},
				{"select regexp_replace(?, ?, ?, ?, ?)", []any{"abc", "a", "X", int64(0), nil}, false},
				{"select regexp_replace(?, ?, ?, ?)", []any{"abc", "a", "X\xc3Y", nil}, false},
				{"select regexp_replace(?, ?, ?, ?, ?)", []any{"abc", "a", "X\xc3Y", int64(1), nil}, false},
			} {
				withStatement(t, tc.query, func(stmt *sql.Stmt) {
					wire.mu.Lock()
					wire.mask = 0
					wire.mu.Unlock()
					var value sql.NullString
					err := stmt.QueryRowContext(ctx, tc.args...).Scan(&value)
					if tc.err {
						require.Error(t, err)
						var sqlError *mysql.MySQLError
						require.ErrorAs(t, err, &sqlError)
						require.Equal(t, uint16(3854), sqlError.Number)
					} else {
						require.NoError(t, err)
						require.False(t, value.Valid)
					}
				})
			}
		})
		t.Run("replace_null_precedes_malformed_subject", func(t *testing.T) {
			for _, query := range []string{
				"select regexp_replace(?, ?, 'X')",
				"select regexp_replace(?, 'a', ?)",
				"select regexp_replace(?, 'a', 'X', ?)",
				"select regexp_replace(?, 'a', 'X', 1, ?)",
			} {
				withStatement(t, query, func(stmt *sql.Stmt) {
					var value sql.NullString
					require.NoError(t, stmt.QueryRowContext(ctx, "a\xc3b", nil).Scan(&value), query)
					require.False(t, value.Valid)
				})
			}
			withStatement(t, "select regexp_replace(?, 'a', ?)", func(stmt *sql.Stmt) {
				var value sql.NullString
				err := stmt.QueryRowContext(ctx, nil, "X\xc3Y").Scan(&value)
				require.Error(t, err)
				var sqlError *mysql.MySQLError
				require.ErrorAs(t, err, &sqlError)
				require.Equal(t, uint16(3854), sqlError.Number)
			})
		})
		t.Run("error_and_null_reuse", func(t *testing.T) {
			stmt, err := connection.PrepareContext(ctx, "select regexp_substr(?, ?)")
			require.NoError(t, err)
			defer func() { require.NoError(t, stmt.Close()) }()
			wire.mu.Lock()
			wire.mask = 1
			wire.mu.Unlock()
			var value sql.NullString
			err = stmt.QueryRowContext(ctx, "éa", "[").Scan(&value)
			require.Error(t, err)
			var sqlError *mysql.MySQLError
			require.ErrorAs(t, err, &sqlError)
			// The NULL bitmap and BLOB descriptor are independent wire fields.
			require.NoError(t, stmt.QueryRowContext(ctx, nil, ".").Scan(&value))
			require.False(t, value.Valid)
			for _, mask := range []int{0, 1} {
				got := observe(t, stmt, mask, "éa", ".")
				want := "é"
				if mask == 1 {
					want = "Ã"
				}
				require.Equal(t, sql.NullString{String: want, Valid: true}, got.value)
				require.Equal(t, "VARCHAR", got.typ)
			}
		})
	})
}
