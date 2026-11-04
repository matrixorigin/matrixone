//go:build integration

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

package frontend_test

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

type viewWireColumn struct {
	name   string
	length uint32
	typ    byte
}

type viewWireClient struct {
	t    *testing.T
	conn net.Conn
	seq  byte
}

func (w *viewWireClient) command(command byte, data []byte) {
	w.t.Helper()
	packet := make([]byte, 5+len(data))
	length := len(data) + 1
	packet[0], packet[1], packet[2], packet[4] = byte(length), byte(length>>8), byte(length>>16), command
	copy(packet[5:], data)
	for len(packet) != 0 {
		n, err := w.conn.Write(packet)
		require.NoError(w.t, err)
		require.Positive(w.t, n)
		packet = packet[n:]
	}
	w.seq = 1
}

func (w *viewWireClient) packet() []byte {
	w.t.Helper()
	var header [4]byte
	_, err := io.ReadFull(w.conn, header[:])
	require.NoError(w.t, err)
	require.Equal(w.t, w.seq, header[3])
	w.seq++
	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	require.Positive(w.t, length)
	require.Less(w.t, length, 1<<20, "fixture packets must remain bounded")
	data := make([]byte, length)
	_, err = io.ReadFull(w.conn, data)
	require.NoError(w.t, err)
	require.NotEqual(w.t, byte(0xff), data[0], "server error: %q", data)
	return data
}

func viewWireString(t *testing.T, data []byte) (string, []byte) {
	t.Helper()
	require.NotEmpty(t, data)
	// All strings in this fixture fit the one-byte length encoding.
	length := int(data[0])
	require.Less(t, length, 0xfb)
	require.GreaterOrEqual(t, len(data), 1+length)
	return string(data[1 : 1+length]), data[1+length:]
}

func (w *viewWireClient) columns(count int) ([]viewWireColumn, uint16) {
	w.t.Helper()
	columns := make([]viewWireColumn, count)
	for i := range columns {
		data := w.packet()
		var fields [6]string
		for j := range fields {
			fields[j], data = viewWireString(w.t, data)
		}
		require.Equal(w.t, "def", fields[0])
		require.Len(w.t, data, 13)
		require.Equal(w.t, byte(12), data[0])
		columns[i] = viewWireColumn{fields[4], binary.LittleEndian.Uint32(data[3:7]), data[7]}
	}
	end := w.packet()
	require.Len(w.t, end, 5)
	require.Equal(w.t, byte(0xfe), end[0])
	return columns, binary.LittleEndian.Uint16(end[3:5])
}

func (w *viewWireClient) rows(binaryRows bool) ([][]string, uint16) {
	w.t.Helper()
	var rows [][]string
	for {
		data := w.packet()
		if data[0] == 0xfe {
			require.Len(w.t, data, 5)
			return rows, binary.LittleEndian.Uint16(data[3:5])
		}
		var id, label string
		if binaryRows {
			require.GreaterOrEqual(w.t, len(data), 7)
			require.Equal(w.t, []byte{0, 0}, data[:2], "binary row header and NULL bitmap")
			id = fmt.Sprint(int32(binary.LittleEndian.Uint32(data[2:6])))
			label, data = viewWireString(w.t, data[6:])
		} else {
			id, data = viewWireString(w.t, data)
			label, data = viewWireString(w.t, data)
		}
		require.Empty(w.t, data)
		rows = append(rows, []string{id, label})
		require.LessOrEqual(w.t, len(rows), 2)
	}
}

func testNativeViewSchemaProtocol(t *testing.T, ctx context.Context, cn embed.ServiceOperator, svc cnservice.Service, database string) {
	exec := func(query string) {
		t.Helper()
		result, err := svc.GetSQLExecutor().Exec(ctx, query, executor.Options{}.WithDatabase(database))
		require.NoError(t, err, query)
		result.Close()
	}
	exec("create table wire_source (id int not null, label varchar(5) not null)")
	defer exec("drop table wire_source")
	exec("insert into wire_source values (1,'one'),(2,'two')")
	exec("create view wire_middle as select id,label from wire_source")
	defer exec("drop view wire_middle")
	exec("create view wire_root as select id,label from wire_middle")
	defer exec("drop view wire_root")
	identity := func() [2]string {
		t.Helper()
		result, err := svc.GetSQLExecutor().Exec(ctx, "select cast(rel_id as varchar),cast(rel_version as varchar) from mo_catalog.mo_tables where reldatabase='"+database+"' and relname='wire_root'", executor.Options{})
		require.NoError(t, err)
		defer result.Close()
		var value [2]string
		count := 0
		result.ReadRows(func(n int, columns []*vector.Vector) bool {
			count += n
			if n == 1 {
				value = [2]string{columns[0].GetStringAt(0), columns[1].GetStringAt(0)}
			}
			return true
		})
		require.Equal(t, 1, count)
		return value
	}
	rootIdentity := identity()
	var preparedBindings atomic.Int64
	var preparedCheckpoint atomic.Pointer[func(context.Context) error]
	roots, unregister := frontend.EnableNativeViewSchemaProtocolForTest(cn.ServiceID(), database, func(ctx context.Context) error {
		preparedBindings.Add(1)
		if checkpoint := preparedCheckpoint.Swap(nil); checkpoint != nil {
			return (*checkpoint)(ctx)
		}
		return nil
	})
	defer unregister()

	port := cn.GetServiceConfig().CN.Frontend.Port
	network := fmt.Sprintf("view-s5-%d", port)
	connections := make(chan net.Conn, 1)
	mysqldriver.RegisterDialContext(network, func(ctx context.Context, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
		if err != nil {
			return nil, err
		}
		select {
		case connections <- conn:
			return conn, nil
		default:
			_ = conn.Close()
			return nil, fmt.Errorf("unexpected protocol-fixture reconnect")
		}
	})
	defer mysqldriver.DeregisterDialContext(network)
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@%s(127.0.0.1:%d)/%s", network, port, database))
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	transport := <-connections
	// The driver performs real authentication. Raw exclusively owns this pinned
	// connection while commands below exercise the actual server wire owners.
	require.NoError(t, conn.Raw(func(any) error {
		require.NoError(t, transport.SetDeadline(time.Now().Add(30*time.Second)))
		defer transport.SetDeadline(time.Time{})
		w := &viewWireClient{t: t, conn: transport}
		const query = "select id,label from wire_root where id > ? order by id"
		w.command(0x03, []byte("select id,label from wire_root order by id"))
		require.Equal(t, []byte{2}, w.packet())
		textColumns, _ := w.columns(2)
		rows, _ := w.rows(false)
		require.Equal(t, [][]string{{"1", "one"}, {"2", "two"}}, rows)
		require.Positive(t, roots.Load(), "COM_QUERY must use the opt-in provider")

		beforePrepare := roots.Load()
		w.command(0x16, []byte(query))
		prepared := w.packet()
		require.GreaterOrEqual(t, len(prepared), 12)
		require.Zero(t, prepared[0])
		statementID := binary.LittleEndian.Uint32(prepared[1:5])
		require.EqualValues(t, 2, binary.LittleEndian.Uint16(prepared[5:7]))
		require.EqualValues(t, 1, binary.LittleEndian.Uint16(prepared[7:9]))
		w.columns(1)
		prepareColumns, _ := w.columns(2)
		require.Equal(t, textColumns, prepareColumns)
		require.Greater(t, roots.Load(), beforePrepare, "COM_STMT_PREPARE must derive the View")
		defer func() {
			id := make([]byte, 4)
			binary.LittleEndian.PutUint32(id, statementID)
			w.command(0x19, id)
		}()

		execute := func(id uint32, cursor bool) ([]viewWireColumn, uint16) {
			payload := make([]byte, 17)
			binary.LittleEndian.PutUint32(payload, id)
			if cursor {
				payload[4] = 1
			}
			binary.LittleEndian.PutUint32(payload[5:9], 1)
			payload[10], payload[11] = 1, 3 // new parameters; signed MYSQL_TYPE_LONG, value 0
			w.command(0x17, payload)
			require.Equal(t, []byte{2}, w.packet())
			return w.columns(2)
		}
		beforeExecute := roots.Load()
		cursorColumns, status := execute(statementID, true)
		require.Greater(t, roots.Load(), beforeExecute, "parameterized EXECUTE must use S3, not just its preceding PREPARE")
		require.Equal(t, prepareColumns, cursorColumns)
		require.NotZero(t, status&0x40, "SERVER_STATUS_CURSOR_EXISTS")
		beforeFetch := roots.Load()
		exec("alter table wire_source modify column label varchar(40) not null, algorithm=copy")
		exec("update wire_source set label='new_source'")
		require.Equal(t, rootIdentity, identity(), "the root View identity/version must stay unchanged; invalidation is transitive")
		for i, want := range []string{"one", "two"} {
			fetch := make([]byte, 8)
			binary.LittleEndian.PutUint32(fetch, statementID)
			binary.LittleEndian.PutUint32(fetch[4:], 1)
			w.command(0x1c, fetch)
			rows, status := w.rows(true)
			require.Equal(t, [][]string{{fmt.Sprint(i + 1), want}}, rows, "FETCH keeps the materialized generation")
			if i == 1 {
				require.NotZero(t, status&0x80, "SERVER_STATUS_LAST_ROW_SENT")
			}
		}
		require.Equal(t, beforeFetch, roots.Load(), "FETCH must not derive against the latest schema")
		currentColumns, _ := execute(statementID, false)
		rows, _ = w.rows(true)
		require.Equal(t, [][]string{{"1", "new_source"}, {"2", "new_source"}}, rows)
		require.Greater(t, roots.Load(), beforeFetch, "next EXECUTE must rebind the transitive source change")
		require.Equal(t, cursorColumns[1].typ, currentColumns[1].typ)
		require.Equal(t, "label", currentColumns[1].name)
		require.Positive(t, cursorColumns[1].length)
		require.Equal(t, cursorColumns[1].length*8, currentColumns[1].length, "varchar(5) -> varchar(40), unchanged wire charset")

		// Commit COPY after binding but before the locking SELECT executes.
		// The stale physical table must force the real compile retry owner to
		// bind a new S3 request, not execute a previously successful description.
		w.command(0x16, []byte(query+" for update"))
		retryPrepared := w.packet()
		require.GreaterOrEqual(t, len(retryPrepared), 12)
		require.Zero(t, retryPrepared[0])
		retryID := binary.LittleEndian.Uint32(retryPrepared[1:5])
		defer func() {
			id := make([]byte, 4)
			binary.LittleEndian.PutUint32(id, retryID)
			w.command(0x19, id)
		}()
		w.columns(1)
		retryDeclared, _ := w.columns(2)
		require.Equal(t, currentColumns, retryDeclared)
		checkpoint := func(ctx context.Context) error {
			guard, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			result, err := svc.GetSQLExecutor().Exec(guard, "alter table wire_source modify column label varchar(80) not null, algorithm=copy", executor.Options{}.WithDatabase(database))
			if err == nil {
				result.Close()
			}
			return err
		}
		beforeRetry := preparedBindings.Load()
		preparedCheckpoint.Store(&checkpoint)
		retryColumns, _ := execute(retryID, false)
		rows, _ = w.rows(true)
		require.Nil(t, preparedCheckpoint.Load(), "the deterministic post-bind checkpoint must run")
		require.GreaterOrEqual(t, preparedBindings.Load()-beforeRetry, int64(2), "execution-time definition change must enter the real prepared retry builder")
		require.Equal(t, [][]string{{"1", "new_source"}, {"2", "new_source"}}, rows)
		require.Equal(t, currentColumns[1].length*2, retryColumns[1].length)
		require.Equal(t, rootIdentity, identity())
		return nil
	}))
}
