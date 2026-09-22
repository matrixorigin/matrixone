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

package frontend

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

// This exercises the wire packet loop, not just ParseExecuteData: an execute
// error must be written to the client and leave the same connection usable.
func TestMySQLWireMalformedExecuteKeepsPreparedTypesAndConnection(t *testing.T) {
	previousServerVars, ok := serverVarsMap.Load("")
	require.True(t, ok)
	serverVarsMap.Store("", &ServerLevelVariables{})
	parameters := &config.FrontendParameters{}
	parameters.SetDefaultValues()
	parameters.SkipCheckUser = true
	parameters.KillRountinesInterval = 0
	setPu("", config.NewParameterUnit(parameters, nil, nil, nil))
	setSessionAlloc("", NewLeakCheckAllocator())
	rm, err := NewRoutineManager(context.Background(), "")
	require.NoError(t, err)
	setRtMgr("", rm)

	clientConn, serverConn := net.Pipe()
	serverDone := make(chan struct{})
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
		select {
		case <-serverDone:
		case <-time.After(10 * time.Second):
			t.Fatal("MySQL wire server did not stop after the pipe closed")
		}
		rm.mu.RLock()
		remainingClients := len(rm.clients)
		remainingIDs := len(rm.routinesByConnID)
		rm.mu.RUnlock()
		rm.cancelCtx()
		serverVarsMap.Store("", previousServerVars)
		require.Zero(t, remainingClients, "connection cleanup must remove the client")
		require.Zero(t, remainingIDs, "connection cleanup must remove the connection ID")
	})
	go func() {
		defer close(serverDone)
		startInnerServer(serverConn)
	}()
	require.NoError(t, clientConn.SetDeadline(time.Now().Add(30*time.Second)))
	_, handshake := readWirePacket(t, clientConn)
	versionEnd := 1
	for versionEnd < len(handshake) && handshake[versionEnd] != 0 {
		versionEnd++
	}
	require.Greater(t, len(handshake), versionEnd+4)
	connectionID := binary.LittleEndian.Uint32(handshake[versionEnd+1:])
	writeWirePacket(t, clientConn, 1, wireHandshakeResponse("dump"))
	_, response := readWirePacket(t, clientConn)
	require.NotEmpty(t, response)
	require.Equal(t, byte(defines.OKHeader), response[0])
	routine := rm.getRoutineByConnID(connectionID)
	require.NotNil(t, routine)
	ses := routine.getSession()
	// Execute the prepared SET through the normal frontend path without a
	// catalog or a running TN service.
	storage := &disttae.Engine{}
	getPu("").StorageEngine = storage
	ses.GetProc().Base.SessionInfo.StorageEngine = storage
	op := newTestTxnOp()
	op.meta = txn.TxnMeta{ID: []byte{1, 2, 3, 4}, Status: txn.TxnStatus_Active}
	ses.txnHandler.Close()
	ses.txnHandler = InitTxnHandler(ses.GetService(), storage, context.Background(), op)

	installStmt := func(id uint32, sql string) *PrepareStmt {
		t.Helper()
		ctx := context.Background()
		name := getPrepareStmtName(id)
		parsed, err := mysql.Parse(ctx, sql, 1)
		require.NoError(t, err)
		prepared, err := buildPlan(ctx, nil, plan.NewEmptyCompilerContext(),
			tree.NewPrepareString(tree.Identifier(name), sql))
		require.NoError(t, err)
		stmt := &PrepareStmt{
			Name:                name,
			Sql:                 sql,
			PreparePlan:         prepared,
			PrepareStmt:         parsed[0],
			getFromSendLongData: make(map[int]struct{}),
		}
		require.NoError(t, ses.SetPrepareStmt(ctx, name, stmt))
		return stmt
	}
	zero := installStmt(1, "select 1")
	stmt := installStmt(2, "set @binary_value = ?")

	execute := func(id uint32, body []byte) []byte {
		t.Helper()
		payload := make([]byte, 5+len(body))
		payload[0] = byte(COM_STMT_EXECUTE)
		binary.LittleEndian.PutUint32(payload[1:], id)
		copy(payload[5:], body)
		writeWirePacket(t, clientConn, 0, payload)
		_, response := readWirePacket(t, clientConn)
		require.NotEmpty(t, response)
		return response
	}
	bind := func(flag byte, typ defines.MysqlType, value ...byte) []byte {
		body := []byte{0, 1, 0, 0, 0, 0, flag}
		if flag != 0 {
			body = append(body, byte(typ), 0)
		}
		return append(body, value...)
	}
	assertError := func(response []byte) {
		t.Helper()
		require.Equal(t, byte(defines.ErrHeader), response[0], "response: %x", response)
	}
	assertValue := func(want int64) {
		t.Helper()
		// The OK packet is emitted inside doComQuery, before ExecRequest's
		// parameter-vector cleanup. PING waits for that request to finish.
		writeWirePacket(t, clientConn, 0, []byte{byte(COM_PING)})
		_, pong := readWirePacket(t, clientConn)
		require.NotEmpty(t, pong)
		require.Equal(t, byte(defines.OKHeader), pong[0])
		variable, err := ses.GetUserDefinedVar("binary_value")
		require.NoError(t, err)
		require.EqualValues(t, want, variable.Value)
		require.Nil(t, stmt.params, "execute must release its parameter vector")
	}

	// Zero-parameter execute still needs all five fixed bytes after the ID.
	for n := 0; n < 5; n++ {
		assertError(execute(1, make([]byte, n)))
		require.Empty(t, zero.ParamTypes)
	}
	// A complete first bind publishes its type; subsequent malformed binds
	// must return errors without replacing it.
	require.Equal(t, byte(defines.OKHeader), execute(2, bind(2, defines.MYSQL_TYPE_TINY, 9))[0])
	assertValue(9)
	require.Equal(t, []byte{byte(defines.MYSQL_TYPE_TINY), 0}, stmt.ParamTypes)
	longData := make([]byte, 1+4+2+3)
	longData[0] = byte(COM_STMT_SEND_LONG_DATA)
	binary.LittleEndian.PutUint32(longData[1:], 2)
	copy(longData[7:], "bad")
	writeWirePacket(t, clientConn, 0, longData)
	assertError(execute(2, []byte{0, 1, 0, 0, 0, 0, 2}))
	require.Equal(t, []byte{byte(defines.MYSQL_TYPE_TINY), 0}, stmt.ParamTypes)
	require.Nil(t, stmt.params)
	require.Empty(t, stmt.getFromSendLongData)
	assertValue(9)
	assertError(execute(2, bind(255, defines.MYSQL_TYPE_SHORT, 0x34)))
	require.Equal(t, []byte{byte(defines.MYSQL_TYPE_TINY), 0}, stmt.ParamTypes)
	require.Nil(t, stmt.params)
	assertValue(9)
	require.Equal(t, byte(defines.OKHeader), execute(2, bind(0, 0, 7))[0])
	assertValue(7)
	require.Equal(t, []byte{byte(defines.MYSQL_TYPE_TINY), 0}, stmt.ParamTypes)
	require.Equal(t, byte(defines.OKHeader), execute(2, bind(255, defines.MYSQL_TYPE_SHORT, 0x34, 0x12))[0])
	assertValue(0x1234)
	require.Equal(t, []byte{byte(defines.MYSQL_TYPE_SHORT), 0}, stmt.ParamTypes)
	require.Equal(t, byte(defines.OKHeader), execute(2, bind(0, 0, 0x78, 0x56))[0])
	assertValue(0x5678)

	closePayload := make([]byte, 5)
	closePayload[0] = byte(COM_STMT_CLOSE)
	binary.LittleEndian.PutUint32(closePayload[1:], 2)
	writeWirePacket(t, clientConn, 0, closePayload)
	writeWirePacket(t, clientConn, 0, []byte{byte(COM_PING)})
	_, response = readWirePacket(t, clientConn)
	require.Equal(t, byte(defines.OKHeader), response[0])
	_, err = ses.GetPrepareStmt(context.Background(), stmt.Name)
	require.Error(t, err)
}

func TestParseExecuteDataRequiresCompleteFixedHeader(t *testing.T) {
	for _, offset := range []int{0, 4} {
		for bodyLen := 0; bodyLen < 5; bodyLen++ {
			proto, proc, stmt := newBinaryPrepareProtocolTestCase(t, "select 1")
			data := make([]byte, offset+bodyLen)
			stmt.cursorRequested = true
			require.Error(t, proto.ParseExecuteData(context.Background(), proc, stmt, data, offset),
				"offset %d, fixed body length %d", offset, bodyLen)
			require.True(t, stmt.cursorRequested, "a short packet must not change cursor state")
			stmt.Close()
		}

		proto, proc, stmt := newBinaryPrepareProtocolTestCase(t, "select 1")
		data := make([]byte, offset+5)
		require.NoError(t, proto.ParseExecuteData(context.Background(), proc, stmt, data, offset))
		stmt.Close()
	}

	proto, proc, stmt := newBinaryPrepareProtocolTestCase(t, "select 1")
	defer stmt.Close()
	require.Error(t, proto.ParseExecuteData(context.Background(), proc, stmt, nil, -1))
	require.Error(t, proto.ParseExecuteData(context.Background(), proc, stmt, nil, 1))
}

func TestParseExecuteDataNonzeroNewTypesAndMalformedRecovery(t *testing.T) {
	ctx := context.Background()
	proto, proc, stmt := newBinaryPrepareProtocolTestCase(t, "select ?")
	defer stmt.Close()
	packet := func(flag byte, typ defines.MysqlType, values ...byte) []byte {
		data := []byte{0, 1, 0, 0, 0, 0, flag}
		if flag != 0 {
			data = append(data, byte(typ), 0)
		}
		return append(data, values...)
	}

	require.NoError(t, proto.ParseExecuteData(ctx, proc, stmt,
		packet(1, defines.MYSQL_TYPE_TINY, 10), 0))
	require.Equal(t, "10", stmt.params.GetStringAt(0))
	require.Equal(t, []byte{byte(defines.MYSQL_TYPE_TINY), 0}, stmt.ParamTypes)

	// MySQL interprets every nonzero bind flag as "new types", including 2
	// and 255. A following zero flag must reuse the most recent type.
	require.NoError(t, proto.ParseExecuteData(ctx, proc, stmt,
		packet(2, defines.MYSQL_TYPE_SHORT, 0x34, 0x12), 0))
	require.Equal(t, "4660", stmt.params.GetStringAt(0))
	require.Equal(t, []byte{byte(defines.MYSQL_TYPE_SHORT), 0}, stmt.ParamTypes)
	require.NoError(t, proto.ParseExecuteData(ctx, proc, stmt,
		packet(0, 0, 42, 0), 0))
	require.Equal(t, "42", stmt.params.GetStringAt(0))
	require.NoError(t, proto.ParseExecuteData(ctx, proc, stmt,
		packet(255, defines.MYSQL_TYPE_TINY, 7), 0))
	require.Equal(t, "7", stmt.params.GetStringAt(0))
	require.Equal(t, []byte{byte(defines.MYSQL_TYPE_TINY), 0}, stmt.ParamTypes)

	// Neither a missing type array nor a complete type array followed by a
	// truncated value may replace the last successfully bound type.
	require.Error(t, proto.ParseExecuteData(ctx, proc, stmt,
		[]byte{0, 1, 0, 0, 0, 0, 2}, 0))
	require.Equal(t, []byte{byte(defines.MYSQL_TYPE_TINY), 0}, stmt.ParamTypes)
	require.Error(t, proto.ParseExecuteData(ctx, proc, stmt,
		packet(2, defines.MYSQL_TYPE_SHORT, 0x34), 0))
	require.Equal(t, []byte{byte(defines.MYSQL_TYPE_TINY), 0}, stmt.ParamTypes)
	require.NoError(t, proto.ParseExecuteData(ctx, proc, stmt,
		packet(0, 0, 9), 0))
	require.Equal(t, "9", stmt.params.GetStringAt(0))
}

func TestParseExecuteDataNonzeroNewTypesWithNullParam(t *testing.T) {
	proto, proc, stmt := newBinaryPrepareProtocolTestCase(t, "select ?")
	defer stmt.Close()
	data := []byte{0, 1, 0, 0, 0, 1, 2, byte(defines.MYSQL_TYPE_TINY), 0}
	require.NoError(t, proto.ParseExecuteData(context.Background(), proc, stmt, data, 0))
	require.Equal(t, []byte{byte(defines.MYSQL_TYPE_TINY), 0}, stmt.ParamTypes)
	require.True(t, stmt.params.GetNulls().Contains(0))
}
