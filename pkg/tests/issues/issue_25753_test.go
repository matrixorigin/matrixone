// Copyright 2021 - 2026 Matrix Origin
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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
)

type issue25753TraceConn struct {
	net.Conn
	mu                     sync.Mutex
	reads                  []byte
	rewriteNextDecimalType bool
	rewroteDecimalType     bool
}

func (c *issue25753TraceConn) Read(data []byte) (int, error) {
	n, err := c.Conn.Read(data)
	if n > 0 {
		c.mu.Lock()
		c.reads = append(c.reads, data[:n]...)
		c.mu.Unlock()
	}
	return n, err
}

func (c *issue25753TraceConn) Write(data []byte) (int, error) {
	c.mu.Lock()
	if c.rewriteNextDecimalType {
		modified := append([]byte(nil), data...)
		if issue25753RewriteFirstParamAsDecimal(modified) {
			data = modified
			c.rewriteNextDecimalType = false
			c.rewroteDecimalType = true
		}
	}
	c.mu.Unlock()
	return c.Conn.Write(data)
}

func (c *issue25753TraceConn) rewriteNextParamAsDecimal() {
	c.mu.Lock()
	c.rewriteNextDecimalType = true
	c.rewroteDecimalType = false
	c.mu.Unlock()
}

func (c *issue25753TraceConn) didRewriteParamAsDecimal() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rewroteDecimalType
}

// issue25753RewriteFirstParamAsDecimal keeps the driver's length-encoded value
// and changes only its COM_STMT_EXECUTE type descriptor. This exercises the
// same wire shape used by clients that bind a direct DECIMAL parameter, while
// retaining the driver's real prepare/execute/result decoding path.
func issue25753RewriteFirstParamAsDecimal(data []byte) bool {
	const (
		packetHeaderSize = 4
		stmtExecute      = 0x17
		paramCount       = 1
	)

	for pos := 0; pos+packetHeaderSize <= len(data); {
		payloadLen := int(data[pos]) | int(data[pos+1])<<8 | int(data[pos+2])<<16
		end := pos + packetHeaderSize + payloadLen
		if end > len(data) {
			return false
		}
		payload := data[pos+packetHeaderSize : end]
		const nullBitmapLen = (paramCount + 7) / 8
		const executeHeaderLen = 1 + 4 + 1 + 4
		newTypesFlagPos := executeHeaderLen + nullBitmapLen
		typePos := newTypesFlagPos + 1
		if len(payload) > typePos+1 &&
			payload[0] == stmtExecute &&
			payload[newTypesFlagPos] == 1 &&
			(payload[typePos] == byte(defines.MYSQL_TYPE_VAR_STRING) ||
				payload[typePos] == byte(defines.MYSQL_TYPE_STRING)) {
			payload[typePos] = byte(defines.MYSQL_TYPE_NEWDECIMAL)
			return true
		}
		pos = end
	}
	return false
}

func (c *issue25753TraceConn) prepareStatementID(t *testing.T) uint32 {
	t.Helper()
	c.mu.Lock()
	data := append([]byte(nil), c.reads...)
	c.mu.Unlock()

	for pos := 0; pos+4 <= len(data); {
		payloadLen := int(data[pos]) | int(data[pos+1])<<8 | int(data[pos+2])<<16
		end := pos + 4 + payloadLen
		require.LessOrEqual(t, end, len(data))
		payload := data[pos+4 : end]
		if len(payload) == 12 && payload[0] == 0 &&
			binary.LittleEndian.Uint16(payload[5:]) == 1 &&
			binary.LittleEndian.Uint16(payload[7:]) == 2 {
			return binary.LittleEndian.Uint32(payload[1:])
		}
		pos = end
	}
	require.FailNow(t, "COM_STMT_PREPARE_OK packet not found")
	return 0
}

func (c *issue25753TraceConn) executeClosedStatement(t *testing.T, stmtID uint32) *mysqlDriver.MySQLError {
	t.Helper()
	payload := make([]byte, 10)
	payload[0] = 0x17 // COM_STMT_EXECUTE
	binary.LittleEndian.PutUint32(payload[1:], stmtID)
	// flags=0 and iteration-count=1; the server rejects the stale ID before
	// attempting to read parameter data.
	binary.LittleEndian.PutUint32(payload[6:], 1)
	packet := append([]byte{byte(len(payload)), 0, 0, 0}, payload...)
	require.NoError(t, c.SetDeadline(time.Now().Add(10*time.Second)))
	defer func() { require.NoError(t, c.SetDeadline(time.Time{})) }()
	_, err := c.Write(packet)
	require.NoError(t, err)

	header := make([]byte, 4)
	_, err = io.ReadFull(c, header)
	require.NoError(t, err)
	payloadLen := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	response := make([]byte, payloadLen)
	_, err = io.ReadFull(c, response)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(response), 9)
	require.Equal(t, byte(0xff), response[0])
	require.Equal(t, byte('#'), response[3])
	return &mysqlDriver.MySQLError{
		Number:   binary.LittleEndian.Uint16(response[1:]),
		SQLState: [5]byte(response[4:9]),
		Message:  string(response[9:]),
	}
}

// TestIssue25753PreparedNumericProtocolLifecycle exercises the real
// COM_STMT_PREPARE / COM_STMT_EXECUTE / COM_STMT_CLOSE path. In particular,
// interpolateParams=false prevents the client from replacing placeholders in
// a text query, and all executions below reuse the same server-side statement.
func TestIssue25753PreparedNumericProtocolLifecycle(t *testing.T) {
	embed.RunBaseClusterTests(t,
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			cn, err := c.GetCNService(0)
			require.NoError(t, err)

			port := cn.GetServiceConfig().CN.Frontend.Port
			var traceConn *issue25753TraceConn
			mysqlDriver.RegisterDialContext("issue25753", func(ctx context.Context, addr string) (net.Conn, error) {
				conn, dialErr := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
				if dialErr != nil {
					return nil, dialErr
				}
				traceConn = &issue25753TraceConn{Conn: conn}
				return traceConn, nil
			})
			dsn := fmt.Sprintf("dump:111@issue25753(127.0.0.1:%d)/?interpolateParams=false", port)
			db, err := sql.Open("mysql", dsn)
			require.NoError(t, err)
			defer db.Close()

			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()

			stmt, err := conn.PrepareContext(ctx, "select cast((? + ?) + 1 as decimal(30, 0)) as result")
			require.NoError(t, err)
			require.NotNil(t, traceConn)
			stmtID := traceConn.prepareStatementID(t)
			stmtClosed := false
			defer func() {
				if !stmtClosed {
					_ = stmt.Close()
				}
			}()

			directStmt, err := conn.PrepareContext(ctx, "select ? as result")
			require.NoError(t, err)
			defer directStmt.Close()

			// The Go driver sends string arguments as MYSQL_TYPE_VAR_STRING on
			// COM_STMT_EXECUTE.  These values are nevertheless valid numeric
			// arguments for overloaded functions; the execute path must infer
			// their numeric domain without changing the cached prepared plan.
			textAbsStmt, err := conn.PrepareContext(ctx, "select abs(?)")
			require.NoError(t, err)
			defer textAbsStmt.Close()
			var absResult string
			require.NoError(t, textAbsStmt.QueryRowContext(ctx, "-1.5").Scan(&absResult))
			require.Equal(t, "1.5", absResult)

			textSleepStmt, err := conn.PrepareContext(ctx, "select sleep(?)")
			require.NoError(t, err)
			defer textSleepStmt.Close()
			var sleepResult int64
			sleepStart := time.Now()
			require.NoError(t, textSleepStmt.QueryRowContext(ctx, "0.05").Scan(&sleepResult))
			require.GreaterOrEqual(t, time.Since(sleepStart), 40*time.Millisecond,
				"VAR_STRING sleep argument was not rebound to a fractional numeric value")
			require.Equal(t, int64(0), sleepResult)

			assertDirectNumeric := func(value any, databaseType string, scanTarget any) {
				t.Helper()
				rows, queryErr := directStmt.QueryContext(ctx, value)
				require.NoError(t, queryErr)
				defer rows.Close()

				columnTypes, typeErr := rows.ColumnTypes()
				require.NoError(t, typeErr)
				require.Len(t, columnTypes, 1)
				require.Equal(t, databaseType, columnTypes[0].DatabaseTypeName())
				require.True(t, rows.Next())
				require.NoError(t, rows.Scan(scanTarget))
				require.False(t, rows.Next())
				require.NoError(t, rows.Err())
			}

			// A direct numeric placeholder must publish and materialize the same
			// type. Previously the metadata was BIGINT while the value vector was
			// VARCHAR, which made binary-row decoding fail in GetInt64.
			var directInteger int64
			assertDirectNumeric(int64(-42), "BIGINT", &directInteger)
			require.Equal(t, int64(-42), directInteger)

			// The Go driver normally sends decimal-looking strings as VAR_STRING;
			// rewrite only the wire type to exercise a real DECIMAL COM_STMT bind.
			traceConn.rewriteNextParamAsDecimal()
			var directDecimal string
			assertDirectNumeric("-1.5", "DECIMAL", &directDecimal)
			require.True(t, traceConn.didRewriteParamAsDecimal(),
				"test did not send a direct MYSQL_TYPE_NEWDECIMAL parameter")
			require.Equal(t, "-1.5", directDecimal)

			assertValue := func(left, right any, expected string) {
				t.Helper()
				rows, queryErr := stmt.QueryContext(ctx, left, right)
				require.NoError(t, queryErr)
				defer rows.Close()

				columnTypes, typeErr := rows.ColumnTypes()
				require.NoError(t, typeErr)
				require.Len(t, columnTypes, 1)
				require.Equal(t, "DECIMAL", columnTypes[0].DatabaseTypeName())
				precision, scale, ok := columnTypes[0].DecimalSize()
				require.True(t, ok)
				require.Equal(t, int64(30), precision)
				require.Equal(t, int64(0), scale)

				require.True(t, rows.Next())
				var actual string
				require.NoError(t, rows.Scan(&actual))
				require.Equal(t, expected, actual)
				require.False(t, rows.Next())
				require.NoError(t, rows.Err())
			}

			// Every call uses a new-params-bound flag and a different client
			// transport type. The prepared decimal computation domain must stay
			// stable, including for integers above the exact float64 range.
			assertValue(int64(9007199254740993), int64(0), "9007199254740994")
			assertValue(uint64(9007199254740993), uint64(0), "9007199254740994")
			assertValue("9007199254740993", "0", "9007199254740994")
			assertValue(int64(-9007199254740993), int64(0), "-9007199254740992")
			assertValue("9007199254740993", int64(17), "9007199254741011")
			assertValue(uint64(9007199254740993), "29", "9007199254741023")

			// CTAS is a binary prepared DDL path: the table definition is cloned
			// for execution, then its CreateAsSelectSql is compiled as a follow-up
			// INSERT. The runtime parameter must reach that INSERT, not merely
			// create an empty table.
			const ctasDB = "issue25753_prepared_ctas_db"
			const ctasTable = ctasDB + ".issue25753_prepared_ctas"
			_, err = conn.ExecContext(ctx, "drop database if exists "+ctasDB)
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "create database "+ctasDB)
			require.NoError(t, err)
			defer func() {
				_, _ = conn.ExecContext(context.Background(), "drop database if exists "+ctasDB)
			}()
			_, err = conn.ExecContext(ctx, "drop table if exists "+ctasTable)
			require.NoError(t, err)
			ctasStmt, err := conn.PrepareContext(ctx,
				"create table "+ctasTable+" as select ? as value")
			require.NoError(t, err)
			_, err = ctasStmt.ExecContext(ctx, int64(42))
			require.NoError(t, err)
			require.NoError(t, ctasStmt.Close())
			var ctasValue int64
			require.NoError(t, conn.QueryRowContext(ctx,
				"select value from "+ctasTable).Scan(&ctasValue))
			require.Equal(t, int64(42), ctasValue)
			_, err = conn.ExecContext(ctx, "drop table if exists "+ctasTable)
			require.NoError(t, err)

			rows, err := stmt.QueryContext(ctx, nil, int64(0))
			require.NoError(t, err)
			defer rows.Close()
			require.True(t, rows.Next())
			var nullResult sql.NullString
			require.NoError(t, rows.Scan(&nullResult))
			require.False(t, nullResult.Valid)
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())

			// A conversion failure must have a stable server error code and must
			// not poison the cached statement or its parameter state.
			for range 2 {
				var ignored string
				err = stmt.QueryRowContext(ctx, "not-a-number", int64(0)).Scan(&ignored)
				require.Error(t, err)
				var mysqlErr *mysqlDriver.MySQLError
				require.True(t, errors.As(err, &mysqlErr), "expected MySQL protocol error, got %T: %v", err, err)
				require.Equal(t, moerr.ErrInvalidInput, mysqlErr.Number)
				require.Equal(t, [5]byte{'H', 'Y', '0', '0', '0'}, mysqlErr.SQLState)
			}

			assertValue(int64(9007199254740993), int64(0), "9007199254740994")

			// The driver's argument-count check is populated from the parameter
			// count in COM_STMT_PREPARE_OK.
			err = stmt.QueryRowContext(ctx, int64(1)).Scan(new(string))
			require.ErrorContains(t, err, "expected 2 arguments")

			require.NoError(t, stmt.Close())
			stmtClosed = true
			err = stmt.QueryRowContext(ctx, int64(1), int64(0)).Scan(new(string))
			require.ErrorContains(t, err, "statement is closed")

			closeErr := traceConn.executeClosedStatement(t, stmtID)
			require.Equal(t, moerr.ErrInvalidState, closeErr.Number)
			require.Equal(t, [5]byte{'H', 'Y', '0', '0', '0'}, closeErr.SQLState)
			require.True(t, strings.Contains(closeErr.Message, "does not exist"), closeErr.Message)
		},
	)
}
