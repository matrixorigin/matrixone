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

package isolated

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// TestIssue29187LongDataWire exercises the real CN listener. A second pass
// uses the same running cluster, so a successful first pass cannot hide
// retained statement or connection state behind a cluster restart.
func TestIssue29187LongDataWire(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(1))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	address := fmt.Sprintf("127.0.0.1:%d", cn.GetServiceConfig().CN.Frontend.Port)
	for pass := 1; pass <= 2; pass++ {
		t.Run(fmt.Sprintf("same-cluster-pass-%d", pass), func(t *testing.T) {
			conn, err := net.DialTimeout("tcp", address, 10*time.Second)
			require.NoError(t, err)
			defer conn.Close()
			require.NoError(t, conn.SetDeadline(time.Now().Add(90*time.Second)))
			wire := issue29187Wire{t: t, conn: conn}
			wire.login("dump", "111")
			wire.queryOK("set max_allowed_packet = 1024")
			stmtA := wire.prepare("set @issue29187_a = ?")
			stmtB := wire.prepare("set @issue29187_b = ?")

			// Exactly 1024 bytes are accepted, and SEND itself emits nothing:
			// the PING and subsequent result set also detect a stray response.
			wire.sendLongData(stmtA, bytes.Repeat([]byte{'a'}, 512))
			wire.sendLongData(stmtA, bytes.Repeat([]byte{'b'}, 512))
			wire.ping()
			wire.executeOK(stmtA)
			wire.queryScalar("select length(@issue29187_a)", "1024")
			t.Log("exact 1024-byte stream executed; SEND emitted no response")

			// Crossing the cumulative limit must fail only at EXECUTE and only
			// for this statement. Another prepared statement remains usable.
			wire.sendLongData(stmtA, bytes.Repeat([]byte{'x'}, 1024))
			wire.sendLongData(stmtA, []byte{'y'})
			wire.executeError(stmtA, "max_allowed_packet")
			wire.ping() // catches an unsolicited SEND error packet
			wire.sendLongData(stmtB, []byte("other"))
			wire.executeOK(stmtB)
			wire.queryScalar("select @issue29187_b", "other")
			t.Log("1025-byte stream failed at EXECUTE; other statement remained usable")

			wire.reset(stmtA)
			wire.sendLongData(stmtA, []byte("reused"))
			wire.executeOK(stmtA)
			wire.queryScalar("select @issue29187_a", "reused")
			t.Log("COM_STMT_RESET cleared the deferred error on the same connection")
			wire.closeStmt(stmtA)
			wire.closeStmt(stmtB)
			wire.ping() // CLOSE is also response-free
			wire.executeError(stmtA, "")
			wire.command(0x01, nil) // COM_QUIT; no response
			t.Log("COM_STMT_CLOSE removed both statements; connection quit cleanly")
		})
	}
}

type issue29187Wire struct {
	t    *testing.T
	conn net.Conn
}

func (w issue29187Wire) writePacket(seq byte, payload []byte) {
	w.t.Helper()
	header := []byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16), seq}
	for _, part := range [][]byte{header, payload} {
		for len(part) > 0 {
			n, err := w.conn.Write(part)
			require.NoError(w.t, err)
			part = part[n:]
		}
	}
}

func (w issue29187Wire) readPacket() []byte {
	w.t.Helper()
	header := make([]byte, 4)
	_, err := io.ReadFull(w.conn, header)
	require.NoError(w.t, err)
	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	payload := make([]byte, length)
	_, err = io.ReadFull(w.conn, payload)
	require.NoError(w.t, err)
	return payload
}

func (w issue29187Wire) command(code byte, data []byte) {
	w.t.Helper()
	w.writePacket(0, append([]byte{code}, data...))
}

func (w issue29187Wire) expectOK() {
	w.t.Helper()
	payload := w.readPacket()
	require.NotEmpty(w.t, payload)
	require.Equalf(w.t, byte(0), payload[0], "expected OK, got %x", payload)
}

func (w issue29187Wire) login(username, password string) {
	w.t.Helper()
	handshake := w.readPacket()
	require.Equal(w.t, byte(10), handshake[0])
	pos := bytes.IndexByte(handshake[1:], 0) + 2
	require.Greater(w.t, pos, 1)
	pos += 4 // connection ID
	salt := append([]byte(nil), handshake[pos:pos+8]...)
	pos += 8 + 1 + 2 + 1 + 2 + 2 + 1 + 10
	salt = append(salt, handshake[pos:pos+12]...)
	stage1 := sha1.Sum([]byte(password))
	stage2 := sha1.Sum(stage1[:])
	outer := sha1.New()
	_, _ = outer.Write(salt)
	_, _ = outer.Write(stage2[:])
	mask := outer.Sum(nil)
	for i := range mask {
		mask[i] ^= stage1[i]
	}
	const capabilities = uint32(0x00000200 | 0x00008000 | 0x00080000) // protocol41, secure auth, plugin auth
	response := make([]byte, 4+4+1+23)
	binary.LittleEndian.PutUint32(response, capabilities)
	binary.LittleEndian.PutUint32(response[4:], 1<<24)
	response[8] = 45 // utf8mb4_general_ci
	response = append(response, username...)
	response = append(response, 0, byte(len(mask)))
	response = append(response, mask...)
	response = append(response, []byte("mysql_native_password\x00")...)
	w.writePacket(1, response)
	auth := w.readPacket()
	if len(auth) > 0 && auth[0] == 0xfe {
		// A server may request a native-password auth switch.
		parts := bytes.SplitN(auth[1:], []byte{0}, 2)
		require.Len(w.t, parts, 2)
		require.Equal(w.t, "mysql_native_password", string(parts[0]))
		switchSalt := bytes.TrimRight(parts[1], "\x00")
		switchHash := sha1.New()
		_, _ = switchHash.Write(switchSalt)
		_, _ = switchHash.Write(stage2[:])
		switchMask := switchHash.Sum(nil)
		for i := range switchMask {
			switchMask[i] ^= stage1[i]
		}
		w.writePacket(3, switchMask)
		auth = w.readPacket()
	}
	require.NotEmpty(w.t, auth)
	require.Equalf(w.t, byte(0), auth[0], "login failed: %s", string(auth))
}

func (w issue29187Wire) queryOK(sql string) {
	w.t.Helper()
	w.command(0x03, []byte(sql))
	w.expectOK()
}

func (w issue29187Wire) ping() {
	w.t.Helper()
	w.command(0x0e, nil)
	w.expectOK()
}

func (w issue29187Wire) prepare(sql string) uint32 {
	w.t.Helper()
	w.command(0x16, []byte(sql))
	response := w.readPacket()
	require.GreaterOrEqual(w.t, len(response), 12)
	require.Equalf(w.t, byte(0), response[0], "prepare failed: %s", string(response))
	id := binary.LittleEndian.Uint32(response[1:5])
	columns := binary.LittleEndian.Uint16(response[5:7])
	params := binary.LittleEndian.Uint16(response[7:9])
	require.EqualValues(w.t, 1, params)
	for i := uint16(0); i < params; i++ {
		_ = w.readPacket()
	}
	if params > 0 {
		_ = w.readPacket() // parameter metadata terminator
	}
	for i := uint16(0); i < columns; i++ {
		_ = w.readPacket()
	}
	if columns > 0 {
		_ = w.readPacket()
	}
	return id
}

func (w issue29187Wire) sendLongData(id uint32, chunk []byte) {
	w.t.Helper()
	data := make([]byte, 6)
	binary.LittleEndian.PutUint32(data, id)
	w.command(0x18, append(data, chunk...))
}

func (w issue29187Wire) execute(id uint32) []byte {
	w.t.Helper()
	data := make([]byte, 4, 13)
	binary.LittleEndian.PutUint32(data, id)
	data = append(data, 0, 1, 0, 0, 0, 1, 1, 0xfd, 0) // one NULL-flagged string parameter
	w.command(0x17, data)
	return w.readPacket()
}

func (w issue29187Wire) executeOK(id uint32) {
	w.t.Helper()
	response := w.execute(id)
	require.NotEmpty(w.t, response)
	require.Equalf(w.t, byte(0), response[0], "execute failed: %s", string(response))
}

func (w issue29187Wire) executeError(id uint32, contains string) {
	w.t.Helper()
	response := w.execute(id)
	require.NotEmpty(w.t, response)
	require.Equalf(w.t, byte(0xff), response[0], "expected execute error, got %x", response)
	require.Contains(w.t, string(response), contains)
}

func (w issue29187Wire) reset(id uint32) {
	w.t.Helper()
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, id)
	w.command(0x1a, data)
	w.expectOK()
}

func (w issue29187Wire) closeStmt(id uint32) {
	w.t.Helper()
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, id)
	w.command(0x19, data)
}

func (w issue29187Wire) queryScalar(sql, want string) {
	w.t.Helper()
	w.command(0x03, []byte(sql))
	count := w.readPacket()
	require.Equalf(w.t, []byte{1}, count, "expected one result column, got %x", count)
	_ = w.readPacket() // column definition
	_ = w.readPacket() // column metadata terminator
	row := w.readPacket()
	require.NotEmpty(w.t, row)
	require.NotEqualf(w.t, byte(0xff), row[0], "query failed: %s", string(row))
	length := int(row[0])
	require.LessOrEqual(w.t, length+1, len(row))
	require.Equal(w.t, want, string(row[1:1+length]))
	_ = w.readPacket() // result-set terminator
}
