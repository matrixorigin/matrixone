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

package embed

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/stretchr/testify/require"
)

// jsonDepthExecuteConn only rewrites the small, single-parameter EXECUTE
// packets in this test. The driver sends STRING for both string and []byte,
// so BLOB cases explicitly change that wire type without changing its encoding.
// Reuse cases remove the type vector, exercising the server's cached types.
type jsonDepthExecuteConn struct {
	net.Conn
	mu            sync.Mutex
	armed         bool
	reuse         bool
	wantType      byte
	wantNull      bool
	statementID   uint32
	haveStatement bool
	cachedType    byte
	observed      bool
}

func (c *jsonDepthExecuteConn) Write(data []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(data) < 5 || data[4] != 0x17 {
		return c.Conn.Write(data)
	}
	if !c.armed {
		return 0, fmt.Errorf("unexpected JSON_DEPTH EXECUTE")
	}
	// Four-byte packet header, command, statement ID, flags, iteration count,
	// one-byte NULL bitmap, and new-parameters-bound flag.
	if len(data) < 16 || int(data[0])|int(data[1])<<8|int(data[2])<<16 != len(data)-4 {
		return 0, fmt.Errorf("expected one complete JSON_DEPTH EXECUTE packet")
	}
	packet := append([]byte(nil), data...)
	id := binary.LittleEndian.Uint32(packet[5:9])
	if c.haveStatement && id != c.statementID {
		return 0, fmt.Errorf("prepared statement changed")
	}
	isNull := packet[14]&1 != 0
	if isNull != c.wantNull {
		return 0, fmt.Errorf("unexpected NULL bitmap")
	}
	if packet[15] != 0 {
		if len(packet) < 18 {
			return 0, fmt.Errorf("missing parameter type")
		}
		actual := packet[16]
		if !isNull {
			if c.wantType == byte(defines.MYSQL_TYPE_BLOB) && actual == byte(defines.MYSQL_TYPE_STRING) {
				packet[16] = c.wantType
			} else if actual != c.wantType {
				return 0, fmt.Errorf("unexpected parameter type %d", actual)
			}
			if packet[17] != 0 {
				return 0, fmt.Errorf("unexpected unsigned parameter")
			}
		}
		if c.reuse {
			if !c.haveStatement || c.cachedType != c.wantType {
				return 0, fmt.Errorf("no matching cached type")
			}
			packet[15] = 0
			packet = append(packet[:16], packet[18:]...)
			size := len(packet) - 4
			packet[0], packet[1], packet[2] = byte(size), byte(size>>8), byte(size>>16)
		}
	} else if !c.reuse || !c.haveStatement || c.cachedType != c.wantType {
		return 0, fmt.Errorf("unexpected cached-type EXECUTE")
	}
	if (packet[15] == 0) != c.reuse {
		return 0, fmt.Errorf("incorrect transmitted reuse flag")
	}
	n, err := c.Conn.Write(packet)
	if err != nil {
		return 0, err
	}
	if n != len(packet) {
		return 0, io.ErrShortWrite
	}
	c.statementID, c.haveStatement = id, true
	c.cachedType = c.wantType
	c.armed, c.observed = false, true
	return len(data), nil
}

func TestPreparedJsonDepthOverMySQLProtocol(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(cluster Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		connections := make(chan *jsonDepthExecuteConn, 1)
		network := "json-depth-" + t.Name()
		mysql.RegisterDialContext(network, func(ctx context.Context, addr string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
			if err != nil {
				return nil, err
			}
			wrapped := &jsonDepthExecuteConn{Conn: conn}
			select {
			case connections <- wrapped:
				return wrapped, nil
			default:
				_ = conn.Close()
				return nil, fmt.Errorf("unexpected reconnect")
			}
		})
		defer mysql.DeregisterDialContext(network)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@%s(127.0.0.1:%d)/?interpolateParams=false", network, cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		var wire *jsonDepthExecuteConn
		select {
		case wire = <-connections:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		stmt, err := conn.PrepareContext(ctx, "SELECT JSON_DEPTH(?) AS result")
		require.NoError(t, err)
		defer stmt.Close()
		cases := []struct {
			name      string
			value     any
			typ       byte
			reuse     bool
			depth     int64
			null      bool
			errorText string
		}{
			{"text", `{"a":[1]}`, byte(defines.MYSQL_TYPE_STRING), false, 3, false, ""},
			{"text-reuse", `[]`, byte(defines.MYSQL_TYPE_STRING), true, 1, false, ""},
			{"integer", int64(42), byte(defines.MYSQL_TYPE_LONGLONG), false, 0, false, "Invalid data type for JSON data"},
			{"integer-reuse", int64(43), byte(defines.MYSQL_TYPE_LONGLONG), true, 0, false, "Invalid data type for JSON data"},
			{"blob", []byte(`{"a":[1]}`), byte(defines.MYSQL_TYPE_BLOB), false, 0, false, "CHARACTER SET 'binary'"},
			{"blob-reuse", []byte(`[]`), byte(defines.MYSQL_TYPE_BLOB), true, 0, false, "CHARACTER SET 'binary'"},
			{"null-with-cached-blob", nil, byte(defines.MYSQL_TYPE_BLOB), true, 0, true, ""},
			{"blob-after-null", []byte(`{}`), byte(defines.MYSQL_TYPE_BLOB), true, 0, false, "CHARACTER SET 'binary'"},
			{"malformed-text", `not-json`, byte(defines.MYSQL_TYPE_STRING), false, 0, false, "invalid JSON document"},
			{"text-recovery", `{"a":{"b":[1]}}`, byte(defines.MYSQL_TYPE_STRING), true, 4, false, ""},
		}
		for _, tc := range cases {
			// Stop the sequence on failure: subsequent steps depend on cached types.
			if !t.Run(tc.name, func(t *testing.T) {
				wire.mu.Lock()
				wire.armed, wire.observed = true, false
				wire.reuse, wire.wantType, wire.wantNull = tc.reuse, tc.typ, tc.null
				wire.mu.Unlock()
				rows, err := stmt.QueryContext(ctx, tc.value)
				if rows != nil {
					defer rows.Close()
				}
				wire.mu.Lock()
				observed := wire.observed
				wire.mu.Unlock()
				require.True(t, observed, "expected EXECUTE packet was not sent")
				if tc.errorText != "" {
					var mysqlErr *mysql.MySQLError
					require.ErrorAs(t, err, &mysqlErr)
					require.Contains(t, mysqlErr.Message, tc.errorText)
					return
				}
				require.NoError(t, err)
				columns, err := rows.ColumnTypes()
				require.NoError(t, err)
				require.Len(t, columns, 1)
				require.Equal(t, "result", columns[0].Name())
				require.Equal(t, "BIGINT", columns[0].DatabaseTypeName())
				require.True(t, rows.Next())
				var result sql.NullInt64
				require.NoError(t, rows.Scan(&result))
				require.Equal(t, !tc.null, result.Valid)
				if !tc.null {
					require.Equal(t, tc.depth, result.Int64)
				}
				require.False(t, rows.Next())
				require.NoError(t, rows.Err())
				require.False(t, rows.NextResultSet())
				require.NoError(t, rows.Err())
			}) {
				return
			}
		}
	})
}
