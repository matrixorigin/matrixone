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
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/lni/goutils/leaktest"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/proxy"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

var testSlat = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0}
var testPacket = &frontend.Packet{
	Length:     1,
	SequenceID: 0,
	Payload:    []byte{1},
}

func testMakeCNServer(
	uuid string, addr string, connID uint32, hash LabelHash, reqLabel labelInfo,
) *CNServer {
	if strings.Contains(addr, "sock") {
		addr = "unix://" + addr
	}
	return &CNServer{
		connID:   connID,
		addr:     addr,
		uuid:     uuid,
		salt:     testSlat,
		hash:     hash,
		reqLabel: reqLabel,
	}
}

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
func (s *mockServerConn) HandleHandshake(_ *frontend.Packet, _ time.Duration) (*frontend.Packet, error) {
	return nil, nil
}
func (s *mockServerConn) ExecStmt(stmt internalStmt, resp chan<- []byte) (bool, error) {
	sendResp(makeOKPacket(8), resp)
	return true, nil
}
func (s *mockServerConn) Close() error {
	if s.conn != nil {
		_ = s.conn.Close()
	}
	return nil
}

var baseConnID atomic.Uint32

type tlsConfig struct {
	enabled  bool
	caFile   string
	certFile string
	keyFile  string
}

type testCNServer struct {
	sync.Mutex
	ctx      context.Context
	scheme   string
	addr     string
	listener net.Listener
	started  bool
	quit     chan interface{}

	globalVars map[string]string
	tlsCfg     tlsConfig
	tlsConfig  *tls.Config

	beforeHandle func()
}

type testHandler struct {
	mysqlProto  *frontend.MysqlProtocolImpl
	connID      uint32
	conn        goetty.IOSession
	sessionVars map[string]string
	labels      map[string]string
	server      *testCNServer
	status      uint16
}

type option func(s *testCNServer)

func withBeforeHandle(f func()) option {
	return func(s *testCNServer) {
		s.beforeHandle = f
	}
}

func startTestCNServer(t *testing.T, ctx context.Context, addr string, cfg *tlsConfig, opts ...option) func() error {
	b := &testCNServer{
		ctx:        ctx,
		scheme:     "tcp",
		addr:       addr,
		quit:       make(chan interface{}),
		globalVars: make(map[string]string),
	}
	for _, opt := range opts {
		opt(b)
	}
	if cfg != nil {
		b.tlsCfg = *cfg
	}
	if strings.Contains(addr, "sock") {
		b.scheme = "unix"
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
			conn, err := net.Dial(s.scheme, s.addr)
			if err == nil && started {
				_ = conn.Close()
				return true
			}
			if conn != nil {
				_ = conn.Close()
			}
		}
	}
}

func (s *testCNServer) Start() error {
	var err error
	if s.tlsCfg.enabled {
		s.tlsConfig, err = frontend.ConstructTLSConfig(
			context.TODO(),
			s.tlsCfg.caFile,
			s.tlsCfg.certFile,
			s.tlsCfg.keyFile,
		)
		if err != nil {
			return err
		}
	}
	s.listener, err = net.Listen(s.scheme, s.addr)
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
				fp := config.FrontendParameters{
					EnableTls: s.tlsCfg.enabled,
				}
				fp.SetDefaultValues()
				cid := baseConnID.Add(1)
				c := goetty.NewIOSession(goetty.WithSessionCodec(frontend.NewSqlCodec()),
					goetty.WithSessionConn(uint64(cid), conn))
				h := &testHandler{
					connID: cid,
					conn:   c,
					mysqlProto: frontend.NewMysqlClientProtocol(
						cid, c, 0, &fp),
					sessionVars: make(map[string]string),
					labels:      make(map[string]string),
					server:      s,
				}
				if s.beforeHandle != nil {
					s.beforeHandle()
				}
				go func(h *testHandler) {
					testHandle(h)
				}(h)
			}
		}
	}
}

func testHandle(h *testHandler) {
	// read extra info from proxy.
	extraInfo := proxy.ExtraInfo{}
	reader := bufio.NewReader(h.conn.RawConn())
	_ = extraInfo.Decode(reader)
	// server writes init handshake.
	_ = h.mysqlProto.WritePacket(h.mysqlProto.MakeHandshakePayload())
	// server reads auth information from client.
	_, _ = h.conn.Read(goetty.ReadOptions{})
	// server writes ok packet.
	_ = h.mysqlProto.WritePacket(h.mysqlProto.MakeOKPayload(0, uint64(h.connID), 0, 0, ""))
	for {
		msg, err := h.conn.Read(goetty.ReadOptions{})
		if err != nil {
			break
		}
		packet, ok := msg.(*frontend.Packet)
		if !ok {
			return
		}
		if packet.Length > 1 && packet.Payload[0] == 3 {
			if strings.HasPrefix(string(packet.Payload[1:]), "set session") {
				h.handleSetVar(packet)
			} else if string(packet.Payload[1:]) == "show session variables" {
				h.handleShowVar()
			} else if string(packet.Payload[1:]) == "show global variables" {
				h.handleShowGlobalVar()
			} else if string(packet.Payload[1:]) == "begin" {
				h.handleStartTxn()
			} else if string(packet.Payload[1:]) == "commit" || string(packet.Payload[1:]) == "rollback" {
				h.handleStopTxn()
			} else if strings.HasPrefix(string(packet.Payload[1:]), "kill connection") {
				h.handleKillConn()
			} else {
				h.handleCommon()
			}
		} else {
			h.handleCommon()
		}
	}
}

func (h *testHandler) handleCommon() {
	h.mysqlProto.SetSequenceID(1)
	// set last insert id as connection id to do test more easily.
	_ = h.mysqlProto.WritePacket(h.mysqlProto.MakeOKPayload(0, uint64(h.connID), h.status, 0, ""))
}

func (h *testHandler) handleSetVar(packet *frontend.Packet) {
	words := strings.Split(string(packet.Payload[1:]), " ")
	v := strings.Split(words[2], "=")
	h.sessionVars[v[0]] = strings.Trim(v[1], "'")
	h.mysqlProto.SetSequenceID(1)
	_ = h.mysqlProto.WritePacket(h.mysqlProto.MakeOKPayload(0, uint64(h.connID), h.status, 0, ""))
}

func (h *testHandler) handleKillConn() {
	h.server.globalVars["killed"] = "yes"
	h.mysqlProto.SetSequenceID(1)
	_ = h.mysqlProto.WritePacket(h.mysqlProto.MakeOKPayload(0, uint64(h.connID), h.status, 0, ""))
}

func (h *testHandler) handleShowVar() {
	h.mysqlProto.SetSequenceID(1)
	err := h.mysqlProto.SendColumnCountPacket(2)
	if err != nil {
		_ = h.mysqlProto.WritePacket(h.mysqlProto.MakeErrPayload(0, "", err.Error()))
		return
	}
	cols := []*plan.ColDef{
		{Typ: plan.Type{Id: int32(types.T_char)}, Name: "Variable_name"},
		{Typ: plan.Type{Id: int32(types.T_char)}, Name: "Value"},
	}
	columns := make([]interface{}, len(cols))
	res := &frontend.MysqlResultSet{}
	for i, col := range cols {
		c := new(frontend.MysqlColumn)
		c.SetName(col.Name)
		c.SetOrgName(col.Name)
		c.SetTable(col.Typ.Table)
		c.SetOrgTable(col.Typ.Table)
		c.SetAutoIncr(col.Typ.AutoIncr)
		c.SetSchema("")
		c.SetDecimal(col.Typ.Scale)
		columns[i] = c
		res.AddColumn(c)
	}
	for _, c := range columns {
		if err := h.mysqlProto.SendColumnDefinitionPacket(context.TODO(), c.(frontend.Column), 3); err != nil {
			_ = h.mysqlProto.WritePacket(h.mysqlProto.MakeErrPayload(0, "", err.Error()))
			return
		}
	}
	_ = h.mysqlProto.WritePacket(h.mysqlProto.MakeEOFPayload(0, h.status))
	for k, v := range h.sessionVars {
		row := make([]interface{}, 2)
		row[0] = k
		row[1] = v
		res.AddRow(row)
	}
	ses := &frontend.Session{}
	h.mysqlProto.SetSession(ses)
	if err := h.mysqlProto.SendResultSetTextBatchRow(res, res.GetRowCount()); err != nil {
		_ = h.mysqlProto.WritePacket(h.mysqlProto.MakeErrPayload(0, "", err.Error()))
		return
	}
	_ = h.mysqlProto.WritePacket(h.mysqlProto.MakeEOFPayload(0, h.status))
}

func (h *testHandler) handleShowGlobalVar() {
	h.mysqlProto.SetSequenceID(1)
	err := h.mysqlProto.SendColumnCountPacket(2)
	if err != nil {
		_ = h.mysqlProto.WritePacket(h.mysqlProto.MakeErrPayload(0, "", err.Error()))
		return
	}
	cols := []*plan.ColDef{
		{Typ: plan.Type{Id: int32(types.T_char)}, Name: "Variable_name"},
		{Typ: plan.Type{Id: int32(types.T_char)}, Name: "Value"},
	}
	columns := make([]interface{}, len(cols))
	res := &frontend.MysqlResultSet{}
	for i, col := range cols {
		c := new(frontend.MysqlColumn)
		c.SetName(col.Name)
		c.SetOrgName(col.Name)
		c.SetTable(col.Typ.Table)
		c.SetOrgTable(col.Typ.Table)
		c.SetAutoIncr(col.Typ.AutoIncr)
		c.SetSchema("")
		c.SetDecimal(col.Typ.Scale)
		columns[i] = c
		res.AddColumn(c)
	}
	for _, c := range columns {
		if err := h.mysqlProto.SendColumnDefinitionPacket(context.TODO(), c.(frontend.Column), 3); err != nil {
			_ = h.mysqlProto.WritePacket(h.mysqlProto.MakeErrPayload(0, "", err.Error()))
			return
		}
	}
	_ = h.mysqlProto.WritePacket(h.mysqlProto.MakeEOFPayload(0, h.status))
	for k, v := range h.server.globalVars {
		row := make([]interface{}, 2)
		row[0] = k
		row[1] = v
		res.AddRow(row)
	}
	ses := &frontend.Session{}
	h.mysqlProto.SetSession(ses)
	if err := h.mysqlProto.SendResultSetTextBatchRow(res, res.GetRowCount()); err != nil {
		_ = h.mysqlProto.WritePacket(h.mysqlProto.MakeErrPayload(0, "", err.Error()))
		return
	}
	_ = h.mysqlProto.WritePacket(h.mysqlProto.MakeEOFPayload(0, h.status))
}

func (h *testHandler) handleStartTxn() {
	h.status |= frontend.SERVER_STATUS_IN_TRANS
	h.handleCommon()
}

func (h *testHandler) handleStopTxn() {
	h.status &= ^frontend.SERVER_STATUS_IN_TRANS
	h.handleCommon()
}

func (s *testCNServer) Stop() error {
	close(s.quit)
	_ = s.listener.Close()
	return nil
}

func TestServerConn_Create(t *testing.T) {
	defer leaktest.AfterTest(t)

	temp := os.TempDir()
	addr := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(addr))
	cn1 := testMakeCNServer("cn11", addr, 0, "", labelInfo{})
	cn1.reqLabel = newLabelInfo("t1", map[string]string{
		"k1": "v1",
		"k2": "v2",
	})
	// server not started.
	sc, err := newServerConn(cn1, nil, nil, 0)
	require.Error(t, err)
	require.Nil(t, sc)

	// start server.
	tp := newTestProxyHandler(t)
	defer tp.closeFn()
	stopFn := startTestCNServer(t, tp.ctx, addr, nil)
	defer func() {
		require.NoError(t, stopFn())
	}()

	sc, err = newServerConn(cn1, nil, nil, 0)
	require.NoError(t, err)
	require.NotNil(t, sc)
}

func TestServerConn_Connect(t *testing.T) {
	defer leaktest.AfterTest(t)
	temp := os.TempDir()
	addr := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(addr))
	cn1 := testMakeCNServer("cn11", addr, 0, "", labelInfo{})
	cn1.reqLabel = newLabelInfo("t1", map[string]string{
		"k1": "v1",
		"k2": "v2",
	})
	tp := newTestProxyHandler(t)
	defer tp.closeFn()
	stopFn := startTestCNServer(t, tp.ctx, addr, nil)
	defer func() {
		require.NoError(t, stopFn())
	}()

	sc, err := newServerConn(cn1, nil, tp.re, 0)
	require.NoError(t, err)
	require.NotNil(t, sc)
	_, err = sc.HandleHandshake(&frontend.Packet{Payload: []byte{1}}, time.Second*3)
	require.NoError(t, err)
	require.NotEqual(t, 0, int(sc.ConnID()))
	err = sc.Close()
	require.NoError(t, err)
}

func TestFakeCNServer(t *testing.T) {
	defer leaktest.AfterTest(t)

	tp := newTestProxyHandler(t)
	defer tp.closeFn()

	temp := os.TempDir()
	addr := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(addr))
	stopFn := startTestCNServer(t, tp.ctx, addr, nil)
	defer func() {
		require.NoError(t, stopFn())
	}()

	li := labelInfo{}
	cn1 := testMakeCNServer("cn11", addr, 0, "", labelInfo{})
	cn1.reqLabel = newLabelInfo("t1", map[string]string{
		"k1": "v1",
		"k2": "v2",
	})

	cleanup := testStartClient(t, tp, clientInfo{labelInfo: li}, cn1)
	defer cleanup()
}

func TestServerConn_ExecStmt(t *testing.T) {
	defer leaktest.AfterTest(t)

	temp := os.TempDir()
	addr := fmt.Sprintf("%s/%d.sock", temp, time.Now().Nanosecond())
	require.NoError(t, os.RemoveAll(addr))
	cn1 := testMakeCNServer("cn11", addr, 0, "", labelInfo{})
	cn1.reqLabel = newLabelInfo("t1", map[string]string{
		"k1": "v1",
		"k2": "v2",
	})
	tp := newTestProxyHandler(t)
	defer tp.closeFn()
	stopFn := startTestCNServer(t, tp.ctx, addr, nil)
	defer func() {
		require.NoError(t, stopFn())
	}()

	sc, err := newServerConn(cn1, nil, tp.re, 0)
	require.NoError(t, err)
	require.NotNil(t, sc)
	_, err = sc.HandleHandshake(&frontend.Packet{Payload: []byte{1}}, time.Second*3)
	require.NoError(t, err)
	require.NotEqual(t, 0, int(sc.ConnID()))
	resp := make(chan []byte, 10)
	_, err = sc.ExecStmt(internalStmt{cmdType: cmdQuery, s: "kill query"}, resp)
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
