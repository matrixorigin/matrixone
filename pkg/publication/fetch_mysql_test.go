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

package publication

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// A single-row MySQL protocol peer exercises the pinned driver's actual
// cancellation watcher and database/sql pool; no external service or port.
func objectReplyMySQL(t *testing.T) (*UpstreamExecutor, *atomic.Int64, <-chan struct{}) {
	t.Helper()
	var dials atomic.Int64
	var mu sync.Mutex
	var conns []net.Conn
	var wg sync.WaitGroup
	blocked := make(chan struct{}, 1)
	network := "publication-" + t.Name()
	mysql.RegisterDialContext(network, func(context.Context, string) (net.Conn, error) {
		client, peer := net.Pipe()
		mu.Lock()
		conns = append(conns, peer)
		mu.Unlock()
		dials.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer peer.Close()
			if err := serveObjectReply(peer, blocked); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, net.ErrClosed) {
				t.Errorf("MySQL protocol peer: %v", err)
			}
		}()
		return client, nil
	})
	t.Cleanup(func() { mysql.DeregisterDialContext(network) })
	cfg := mysql.NewConfig()
	cfg.Net, cfg.Addr, cfg.User = network, "fixture", "dump"
	connector, err := mysql.NewConnector(cfg)
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	t.Cleanup(func() {
		_ = db.Close()
		mu.Lock()
		for _, conn := range conns {
			_ = conn.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	executor := &UpstreamExecutor{conn: db}
	executor.initRetryPolicy(DefaultClassifier{})
	return executor, &dials, blocked
}

func serveObjectReply(conn net.Conn, blocked chan<- struct{}) error {
	if err := conn.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		return err
	}
	write := func(sequence byte, data []byte) error {
		header := []byte{byte(len(data)), byte(len(data) >> 8), byte(len(data) >> 16), sequence}
		_, err := conn.Write(append(header, data...))
		return err
	}
	read := func() ([]byte, error) {
		var header [4]byte
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			return nil, err
		}
		data := make([]byte, int(header[0])|int(header[1])<<8|int(header[2])<<16)
		_, err := io.ReadFull(conn, data)
		return data, err
	}
	const caps = uint32(1 | 1<<9 | 1<<15 | 1<<19) // protocol41, secure/plugin auth.
	hello := append([]byte{10}, []byte("8.0.0\x00")...)
	hello = binary.LittleEndian.AppendUint32(hello, 1)
	hello = append(hello, []byte("12345678\x00")...)
	hello = binary.LittleEndian.AppendUint16(hello, uint16(caps&0xffff))
	hello = append(hello, 33, 2, 0)
	hello = binary.LittleEndian.AppendUint16(hello, uint16(caps>>16))
	hello = append(hello, 21)
	hello = append(hello, make([]byte, 10)...)
	hello = append(hello, []byte("abcdefghijkl\x00mysql_native_password\x00")...)
	if err := write(0, hello); err != nil {
		return err
	}
	if _, err := read(); err != nil {
		return err
	}
	if err := write(2, []byte{0, 0, 0, 2, 0, 0, 0}); err != nil {
		return err
	}
	for {
		query, err := read()
		if err != nil {
			return err
		}
		if len(query) == 0 || query[0] == 1 { // COM_QUIT.
			return nil
		}
		if query[0] != 3 { // COM_QUERY.
			return errors.New("unexpected MySQL command")
		}
		if err := write(1, []byte{5}); err != nil {
			return err
		}
		for i, typ := range []byte{252, 8, 8, 8, 1} { // BLOB, three BIGINTs, BOOL.
			column := []byte{3, 'd', 'e', 'f', 0, 0, 0, 1, byte('a' + i), 0, 12, 33, 0, 255, 255, 0, 0, typ, 0, 0, 0, 0, 0}
			if err := write(byte(i+2), column); err != nil {
				return err
			}
		}
		eof := []byte{254, 0, 0, 2, 0}
		if err := write(7, eof); err != nil {
			return err
		}
		if string(query[1:]) == "blocked" {
			blocked <- struct{}{}
			_, err := read() // Cancellation must close the real driver's connection.
			return err
		}
		var row []byte
		for _, value := range []string{"reply", "5", "1", "1", "1"} {
			row = append(row, byte(len(value)))
			row = append(row, value...)
		}
		if err := write(8, row); err != nil {
			return err
		}
		if err := write(9, eof); err != nil {
			return err
		}
	}
}

func TestObjectReplyMySQLConnectionReuse(t *testing.T) {
	executor, dials, blocked := objectReplyMySQL(t)
	for i := 0; i < 30; i++ {
		reply, err := readObjectReply(context.Background(), executor, "ok", "obj", 1, time.Second*5, false)
		require.NoError(t, err)
		require.Equal(t, []byte("reply"), reply.MetadataData)
	}
	require.Equal(t, int64(1), dials.Load(), "successful queries reuse one physical connection")
	require.Zero(t, executor.conn.Stats().InUse)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	finished := make(chan struct{})
	var queryErr error
	go func() {
		defer close(finished)
		_, queryErr = readObjectReply(ctx, executor, "blocked", "obj", 1, time.Second*5, false)
	}()
	t.Cleanup(func() { cancel(); <-finished })
	select {
	case <-blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("query did not reach its blocked read")
	}
	cancel()
	select {
	case <-finished:
		require.ErrorIs(t, queryErr, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("canceled query did not retire")
	}
	require.Zero(t, executor.conn.Stats().InUse)
	reply, err := readObjectReply(context.Background(), executor, "ok", "obj", 1, time.Second*5, false)
	require.NoError(t, err)
	require.Equal(t, []byte("reply"), reply.MetadataData)
	require.Equal(t, int64(2), dials.Load(), "one replacement after the canceled read")
}
