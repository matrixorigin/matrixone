// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/stretchr/testify/require"
)

var testSlat = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0}

type mockServerConn struct {
	conn net.Conn
}

var _ ServerConn = (*mockServerConn)(nil)

func newMockServerConn(conn net.Conn) *mockServerConn {
	m := &mockServerConn{
		conn: conn,
	}
	return m
}

func (s *mockServerConn) ConnID() uint32    { return 0 }
func (s *mockServerConn) RawConn() net.Conn { return s.conn }
func (s *mockServerConn) HandleHandshake(_ *frontend.Packet) (*frontend.Packet, error) {
	return nil, nil
}
func (s *mockServerConn) ExecStmt(stmt string, resp chan<- []byte) error {
	sendResp(makeOKPacket(), resp)
	return nil
}
func (s *mockServerConn) Close() error {
	if s.conn != nil {
		_ = s.conn.Close()
	}
	return nil
}

var baseConnID atomic.Uint32

type testCNServer struct {
	sync.Mutex
	ctx      context.Context
	addr     string
	listener net.Listener
	started  bool
	quit     chan interface{}
}

type testHandler struct {
	mysqlProto *frontend.MysqlProtocolImpl
	connID     uint32
	conn       goetty.IOSession
}

func startTestCNServer(t *testing.T, ctx context.Context, addr string) func() error {
	b := &testCNServer{
		ctx:  ctx,
		addr: addr,
		quit: make(chan interface{}),
	}
	go func() {
		err := b.Start()
		require.NoError(t, err)
	}()
	require.True(t, b.waitCNServerReady())
	return func() error {
		return b.Stop()
	}
}

func (s *testCNServer) waitCNServerReady() bool {
	ctx, cancel := context.WithTimeout(s.ctx, time.Second*3)
	defer cancel()
	tick := time.NewTicker(time.Millisecond * 100)
	for {
		select {
		case <-ctx.Done():
			return false
		case <-tick.C:
			s.Lock()
			started := s.started
			s.Unlock()
			conn, err := net.Dial("tcp", s.addr)
			if err == nil && started {
				_ = conn.Close()
				return true
			}
			_ = conn.Close()
		}
	}
}

func (s *testCNServer) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.Lock()
	s.started = true
	s.Unlock()

	for {
		select {
		case <-s.ctx.Done():
			return nil
		default:
			conn, err := s.listener.Accept()
			if conn == nil {
				continue
			}
			if err != nil {
				select {
				case <-s.quit:
					return nil
				default:
					return err
				}
			} else {
				fp := config.FrontendParameters{}
				fp.SetDefaultValues()
				cid := baseConnID.Add(1)
				c := goetty.NewIOSession(goetty.WithSessionCodec(frontend.NewSqlCodec()),
					goetty.WithSessionConn(uint64(cid), conn))
				h := &testHandler{
					connID: cid,
					conn:   c,
					mysqlProto: frontend.NewMysqlClientProtocol(
						cid, c, 0, &fp),
				}
				go func(h *testHandler) {
					testHandle(h)
				}(h)
			}
		}
	}
}

func testHandle(h *testHandler) {
	// read salt from proxy.
	data := make([]byte, 20)
	_, _ = h.conn.RawConn().Read(data)
	// server writes init handshake.
	_ = h.mysqlProto.WritePacket(h.mysqlProto.MakeHandshakePayload())
	// server reads auth information from client.
	_, _ = h.conn.Read(goetty.ReadOptions{})
	// server writes ok packet.
	_ = h.mysqlProto.WritePacket(h.mysqlProto.MakeOKPayload(0, 0, 0, 0, ""))
	var err error
	for err == nil {
		_, err = h.conn.Read(goetty.ReadOptions{})
		h.mysqlProto.SetSequenceID(1)
		// set last insert id as connection id to do test more easily.
		_ = h.mysqlProto.WritePacket(h.mysqlProto.MakeOKPayload(0, uint64(h.connID), 0, 0, ""))
	}
}

func (s *testCNServer) Stop() error {
	close(s.quit)
	_ = s.listener.Close()
	return nil
}

func TestServerConn_Create(t *testing.T) {
	defer leaktest.AfterTest(t)

	addr := "127.0.0.1:38009"
	cn1 := &CNServer{
		reqLabel: newLabelInfo("t1", map[string]string{
			"k1": "v1",
			"k2": "v2",
		}),
		uuid: "cn11",
		addr: addr,
		salt: testSlat,
	}
	// server not started.
	sc, err := newServerConn(cn1, nil, nil)
	require.Error(t, err)
	require.Nil(t, sc)

	// start server.
	tp := newTestProxyHandler(t)
	defer tp.closeFn()
	stopFn := startTestCNServer(t, tp.ctx, addr)
	defer func() {
		require.NoError(t, stopFn())
	}()

	sc, err = newServerConn(cn1, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, sc)
}

func TestServerConn_Connect(t *testing.T) {
	defer leaktest.AfterTest(t)
	addr := "127.0.0.1:38090"
	cn1 := &CNServer{
		reqLabel: newLabelInfo("t1", map[string]string{
			"k1": "v1",
			"k2": "v2",
		}),
		uuid: "cn11",
		addr: addr,
		salt: testSlat,
	}
	tp := newTestProxyHandler(t)
	defer tp.closeFn()
	stopFn := startTestCNServer(t, tp.ctx, addr)
	defer func() {
		require.NoError(t, stopFn())
	}()

	sc, err := newServerConn(cn1, nil, tp.re)
	require.NoError(t, err)
	require.NotNil(t, sc)
	_, err = sc.HandleHandshake(&frontend.Packet{Payload: []byte{1}})
	require.NoError(t, err)
	require.NotEqual(t, 0, int(sc.ConnID()))
	err = sc.Close()
	require.NoError(t, err)
}

func TestFakeCNServer(t *testing.T) {
	defer leaktest.AfterTest(t)

	tp := newTestProxyHandler(t)
	defer tp.closeFn()

	addr := "127.0.0.1:38009"
	stopFn := startTestCNServer(t, tp.ctx, addr)
	defer func() {
		require.NoError(t, stopFn())
	}()

	li := labelInfo{}
	cn1 := &CNServer{
		reqLabel: newLabelInfo("t1", map[string]string{
			"k1": "v1",
			"k2": "v2",
		}),
		uuid: "cn11",
		addr: "127.0.0.1:38009",
	}

	cleanup := testStartClient(t, tp, li, cn1)
	defer cleanup()
}

func TestServerConn_ExecStmt(t *testing.T) {
	defer leaktest.AfterTest(t)

	addr := "127.0.0.1:38090"
	cn1 := &CNServer{
		reqLabel: newLabelInfo("t1", map[string]string{
			"k1": "v1",
			"k2": "v2",
		}),
		uuid: "cn11",
		addr: addr,
		salt: testSlat,
	}
	tp := newTestProxyHandler(t)
	defer tp.closeFn()
	stopFn := startTestCNServer(t, tp.ctx, addr)
	defer func() {
		require.NoError(t, stopFn())
	}()

	sc, err := newServerConn(cn1, nil, tp.re)
	require.NoError(t, err)
	require.NotNil(t, sc)
	_, err = sc.HandleHandshake(&frontend.Packet{Payload: []byte{1}})
	require.NoError(t, err)
	require.NotEqual(t, 0, int(sc.ConnID()))
	resp := make(chan []byte, 10)
	err = sc.ExecStmt("kill query", resp)
	require.NoError(t, err)
	res := <-resp
	ok := isOKPacket(res)
	require.True(t, ok)
}

func TestServerConnParseConnID(t *testing.T) {
	t.Run("too short error", func(t *testing.T) {
		s := &serverConn{}
		p := &frontend.Packet{
			Payload: []byte{10},
		}
		err := s.parseConnID(p)
		require.Error(t, err)
	})

	t.Run("no string", func(t *testing.T) {
		s := &serverConn{}
		p := &frontend.Packet{
			Length:  8,
			Payload: []byte{10},
		}
		p.Payload = append(p.Payload, []byte("v1")...)
		err := s.parseConnID(p)
		require.Error(t, err)
	})

	t.Run("no conn id", func(t *testing.T) {
		s := &serverConn{}
		p := &frontend.Packet{
			Length:  5,
			Payload: []byte{10},
		}
		p.Payload = append(p.Payload, []byte("v1")...)
		p.Payload = append(p.Payload, []byte{0}...)
		p.Payload = append(p.Payload, []byte{2, 0, 0, 0}...)
		err := s.parseConnID(p)
		require.Error(t, err)
	})

	t.Run("no error", func(t *testing.T) {
		s := &serverConn{}
		p := &frontend.Packet{
			Length:  8,
			Payload: []byte{10},
		}
		p.Payload = append(p.Payload, []byte("v1")...)
		p.Payload = append(p.Payload, []byte{0}...)
		p.Payload = append(p.Payload, []byte{2, 0, 0, 0}...)
		err := s.parseConnID(p)
		require.NoError(t, err)
	})
}
