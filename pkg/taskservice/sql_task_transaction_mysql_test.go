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

package taskservice

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// Exercise the real MySQL driver with a local protocol peer that accepts a
// COMMIT but withholds its acknowledgement. No database or network service is
// used by this test.
func TestSQLTaskMetadataTransactionMySQLCommitCancellation(t *testing.T) {
	commitSeen := make(chan struct{})
	serverDone := make(chan error, 1)
	cfg := mysql.NewConfig()
	cfg.User = "test"
	cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			serverDone <- serveSQLTaskCommitFault(server, commitSeen)
		}()
		return client, nil
	}
	connector, err := mysql.NewConnector(cfg)
	require.NoError(t, err)
	db := sql.OpenDB(connector)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		attempted bool
		err       error
	}
	done := make(chan result, 1)
	go func() {
		attempted, err := runSQLTaskMetadataTransaction(ctx, db, func(context.Context, *sql.Conn) error { return nil })
		done <- result{attempted, err}
	}()
	select {
	case <-commitSeen:
	case err := <-serverDone:
		t.Fatalf("protocol peer stopped before COMMIT: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("driver did not send COMMIT")
	}
	cancel()
	select {
	case result := <-done:
		require.True(t, result.attempted)
		require.ErrorIs(t, result.err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation did not interrupt COMMIT")
	}
	require.Zero(t, db.Stats().OpenConnections)
	select {
	case err := <-serverDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("driver did not close the uncertain connection")
	}
}

func serveSQLTaskCommitFault(conn net.Conn, commitSeen chan<- struct{}) error {
	// Protocol 4.1, secure authentication and plugin authentication.
	capabilities := uint32(0x000aa201)
	handshake := append([]byte{10}, []byte("5.7.0-test\x00")...)
	handshake = append(handshake, 1, 0, 0, 0)
	handshake = append(handshake, []byte("12345678")...)
	handshake = append(handshake, 0, byte(capabilities), byte(capabilities>>8))
	handshake = append(handshake, 45, 2, 0, byte(capabilities>>16), byte(capabilities>>24), 21)
	handshake = append(handshake, make([]byte, 10)...)
	handshake = append(handshake, []byte("abcdefghijkl\x00mysql_native_password\x00")...)
	if err := writeSQLTaskTestPacket(conn, 0, handshake); err != nil {
		return err
	}
	if _, err := readSQLTaskTestPacket(conn); err != nil {
		return err
	}
	if err := writeSQLTaskTestPacket(conn, 2, []byte{0, 0, 0, 2, 0, 0, 0}); err != nil {
		return err
	}
	for _, query := range []string{"BEGIN", "COMMIT"} {
		packet, err := readSQLTaskTestPacket(conn)
		if err != nil {
			return err
		}
		if len(packet) == 0 || packet[0] != 3 || string(packet[1:]) != query {
			return fmt.Errorf("unexpected command %q, expected %s", packet, query)
		}
		if query == "COMMIT" {
			close(commitSeen)
			var b [1]byte
			_, err := conn.Read(b[:])
			if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("waiting for client close: %w", err)
		}
		if err := writeSQLTaskTestPacket(conn, 1, []byte{0, 0, 0, 2, 0, 0, 0}); err != nil {
			return err
		}
	}
	return nil
}

func writeSQLTaskTestPacket(conn net.Conn, sequence byte, payload []byte) error {
	length := uint32(len(payload))
	header := []byte{byte(length), byte(length >> 8), byte(length >> 16), sequence}
	if _, err := conn.Write(header); err != nil {
		return err
	}
	_, err := conn.Write(payload)
	return err
}

func readSQLTaskTestPacket(conn net.Conn) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return nil, err
	}
	length := binary.LittleEndian.Uint32([]byte{header[0], header[1], header[2], 0})
	packet := make([]byte, length)
	_, err := io.ReadFull(conn, packet)
	return packet, err
}
