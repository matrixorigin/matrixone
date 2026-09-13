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
	"net"
	"sync"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
)

const mysqlComStmtExecute byte = 0x17

type issue28791BinaryProtocolConn struct {
	net.Conn
	mu                 sync.Mutex
	executeParamCounts []int
	nextExecution      int
	patchedParamCounts []int
}

func (conn *issue28791BinaryProtocolConn) Write(packet []byte) (int, error) {
	if len(packet) >= 5 && packet[4] == mysqlComStmtExecute {
		conn.mu.Lock()
		defer conn.mu.Unlock()
		if conn.nextExecution >= len(conn.executeParamCounts) {
			return 0, fmt.Errorf("unexpected COM_STMT_EXECUTE packet %d", conn.nextExecution+1)
		}
		paramCount := conn.executeParamCounts[conn.nextExecution]
		typeOffset := 14 + (paramCount+7)/8 + 1 // header, null bitmap, new-params flag
		if typeOffset+paramCount*2 > len(packet) {
			return 0, fmt.Errorf("truncated COM_STMT_EXECUTE type table for %d params", paramCount)
		}
		patched := 0
		for i := 0; i < paramCount; i++ {
			if packet[typeOffset+2*i] == byte(defines.MYSQL_TYPE_STRING) {
				// go-sql-driver encodes []byte as MYSQL_TYPE_STRING; Connector/J's
				// setBytes uses the BLOB family. Preserve the payload and exercise
				// the same COM_STMT_EXECUTE domain that MatrixOne receives from it.
				packet[typeOffset+2*i] = byte(defines.MYSQL_TYPE_BLOB)
				patched++
			}
		}
		if patched == 0 {
			return 0, fmt.Errorf("COM_STMT_EXECUTE packet %d contained no byte parameters", conn.nextExecution+1)
		}
		conn.patchedParamCounts = append(conn.patchedParamCounts, patched)
		conn.nextExecution++
	}
	return conn.Conn.Write(packet)
}

func (conn *issue28791BinaryProtocolConn) patchedCounts() []int {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	return append([]int(nil), conn.patchedParamCounts...)
}

func TestIssue28791PreparedBinaryBitwiseOperators(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		const testDialName = "mo_issue_28791_binary_protocol"
		var binaryConn *issue28791BinaryProtocolConn
		mysqlDriver.RegisterDialContext(testDialName, func(ctx context.Context, address string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
			if err != nil {
				return nil, err
			}
			binaryConn = &issue28791BinaryProtocolConn{
				Conn:               conn,
				executeParamCounts: []int{12, 2},
			}
			return binaryConn, nil
		})
		t.Cleanup(func() { mysqlDriver.DeregisterDialContext(testDialName) })
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@%s(127.0.0.1:%d)/?interpolateParams=false", testDialName, port))
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		defer db.Close()

		stmt, err := db.PrepareContext(ctx,
			"select HEX(? & ?), HEX(? | ?), HEX(? ^ ?), HEX(~?), HEX(~?), HEX(? >> ?), HEX(? << ?)")
		require.NoError(t, err)
		defer stmt.Close()

		rows, err := stmt.QueryContext(ctx,
			[]byte{0x12}, []byte{0x34},
			[]byte{0x12}, []byte{0x34},
			[]byte{0x12}, []byte{0x34},
			[]byte{0x80}, []byte{0x12, 0x34},
			[]byte{0x80}, int64(1),
			[]byte{0x12, 0x34}, int64(8))
		require.NoError(t, err)
		defer rows.Close()
		require.True(t, rows.Next())
		var got [7]string
		require.NoError(t, rows.Scan(
			&got[0], &got[1], &got[2], &got[3], &got[4], &got[5], &got[6]))
		require.Equal(t, [7]string{"10", "36", "26", "7F", "EDCB", "40", "3400"}, got)
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())

		// Once both operands retain their BLOB domain, unequal byte lengths must
		// be rejected by the bytewise overload instead of silently using integers.
		mismatch, err := db.PrepareContext(ctx, "select HEX(? & ?)")
		require.NoError(t, err)
		defer mismatch.Close()
		mismatchRows, err := mismatch.QueryContext(ctx, []byte{0x01}, []byte{0x00, 0x01})
		if err == nil {
			defer mismatchRows.Close()
			require.False(t, mismatchRows.Next(), "unequal-length bytewise operands must fail")
			err = mismatchRows.Err()
		}
		require.Error(t, err)
		require.NotNil(t, binaryConn)
		require.Equal(t, []int{10, 2}, binaryConn.patchedCounts(),
			"the executed parameter type tables must carry MYSQL_TYPE_BLOB")
	})
}
