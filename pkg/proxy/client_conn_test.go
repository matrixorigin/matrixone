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
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/fagongzi/goetty/v2/buf"
	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plugin"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type mockNetConn struct {
	localIP    string
	localPort  int
	remoteIP   string
	remotePort int
	c          net.Conn
}

type readDeadlineTrackingConn struct {
	net.Conn
	mu       sync.Mutex
	deadline time.Time
}

func (c *readDeadlineTrackingConn) SetReadDeadline(deadline time.Time) error {
	c.mu.Lock()
	c.deadline = deadline
	c.mu.Unlock()
	return c.Conn.SetReadDeadline(deadline)
}

func (c *readDeadlineTrackingConn) readDeadline() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deadline
}

func newMockNetConn(
	localIP string, localPort int, remoteIP string, remotePort int, c net.Conn,
) *mockNetConn {
	return &mockNetConn{
		localIP:    localIP,
		localPort:  localPort,
		remoteIP:   remoteIP,
		remotePort: remotePort,
		c:          c,
	}
}

func (c *mockNetConn) SetRemote(addr string) {
	c.remoteIP = addr
}

func (c *mockNetConn) Read(b []byte) (n int, err error) {
	return c.c.Read(b)
}

func (c *mockNetConn) Write(b []byte) (n int, err error) {
	return c.c.Write(b)
}

func (c *mockNetConn) Close() error {
	return nil
}

func (c *mockNetConn) LocalAddr() net.Addr {
	return &net.TCPAddr{
		IP:   []byte(c.localIP),
		Port: c.localPort,
	}
}

func (c *mockNetConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{
		IP:   []byte(c.remoteIP),
		Port: c.remotePort,
	}
}

func (c *mockNetConn) SetDeadline(t time.Time) error {
	return nil
}

func (c *mockNetConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *mockNetConn) SetWriteDeadline(t time.Time) error {
	return nil
}

type mockClientConn struct {
	conn       net.Conn
	tenant     Tenant
	clientInfo clientInfo // need to set it explicitly
	router     Router
	tun        *tunnel
	redoStmts  []internalStmt
	killFn     func(ServerConn) error
}

var _ ClientConn = (*mockClientConn)(nil)

func newMockClientConn(
	conn net.Conn, tenant Tenant, ci clientInfo, router Router, tun *tunnel,
) ClientConn {
	c := &mockClientConn{
		conn:       conn,
		tenant:     tenant,
		clientInfo: ci,
		router:     router,
		tun:        tun,
	}
	return c
}

func (c *mockClientConn) ConnID() uint32                     { return 0 }
func (c *mockClientConn) GetSalt() []byte                    { return nil }
func (c *mockClientConn) GetHandshakePack() *frontend.Packet { return nil }
func (c *mockClientConn) RawConn() net.Conn                  { return c.conn }
func (c *mockClientConn) GetTenant() Tenant                  { return c.tenant }
func (c *mockClientConn) SendErrToClient(err error)          {}
func (c *mockClientConn) BuildConnWithServer(_ string) (ServerConn, error) {
	var err error
	li := &c.clientInfo.labelInfo
	c.clientInfo.labelInfo = newLabelInfo(c.clientInfo.Tenant, li.Labels)
	c.clientInfo.hash, err = c.clientInfo.getHash()
	if err != nil {
		return nil, err
	}
	cn, err := c.router.Route(context.TODO(), "", c.clientInfo, nil)
	if err != nil {
		return nil, err
	}
	cn.salt = testSlat
	sc, _, err := c.router.Connect(cn, testPacket, c.tun)
	if err != nil {
		return nil, err
	}
	// Set the use defined variables, including session variables and user variables.
	for _, stmt := range c.redoStmts {
		if _, err := sc.ExecStmt(stmt, nil); err != nil {
			return nil, err
		}
	}
	return sc, nil
}

func (c *mockClientConn) HandleEvent(ctx context.Context, e IEvent, resp chan<- []byte) error {
	defer e.notify()
	switch ev := e.(type) {
	case *killEvent:
		cn, err := c.router.SelectByConnID(ev.connID)
		if err != nil {
			sendResp([]byte(err.Error()), resp)
			return err
		}
		sendResp([]byte(cn.addr), resp)
		return nil
	case *setVarEvent:
		c.redoStmts = append(c.redoStmts, internalStmt{cmdType: cmdQuery, s: ev.stmt})
		sendResp([]byte("ok"), resp)
		return nil
	case *quitEvent:
		sendResp([]byte("ok"), resp)
		return nil
	default:
		sendResp([]byte("type not supported"), resp)
		return moerr.NewInternalErrorNoCtx("type not supported")
	}
}
func (c *mockClientConn) KillCurrentBackendConn(sc ServerConn) error {
	if c.killFn != nil {
		return c.killFn(sc)
	}
	return nil
}
func (c *mockClientConn) Close() error { return nil }

type mockConnCache struct {
	pushFn func(cacheKey, ServerConn) bool
}

func (m *mockConnCache) Push(key cacheKey, sc ServerConn) bool {
	if m.pushFn != nil {
		return m.pushFn(key, sc)
	}
	return true
}

func (m *mockConnCache) Pop(cacheKey, uint32, []byte, []byte) ServerConn { return nil }
func (m *mockConnCache) Count() int                                      { return 0 }
func (m *mockConnCache) Close() error                                    { return nil }

type killTestRouter struct {
	connectFn func(*CNServer, *frontend.Packet, *tunnel) (ServerConn, []byte, error)
}

func (r *killTestRouter) Route(ctx context.Context, sid string, client clientInfo, filter func(string) bool) (*CNServer, error) {
	return nil, nil
}

func (r *killTestRouter) SelectByConnID(connID uint32) (*CNServer, error) {
	return nil, nil
}

func (r *killTestRouter) AllServers(sid string) ([]*CNServer, error) {
	return nil, nil
}

func (r *killTestRouter) Connect(c *CNServer, handshakeResp *frontend.Packet, t *tunnel) (ServerConn, []byte, error) {
	return r.connectFn(c, handshakeResp, t)
}

type killCurrentServerConn struct {
	cn      *CNServer
	closeFn func() error
}

func (s *killCurrentServerConn) ConnID() uint32 { return s.cn.connID }
func (s *killCurrentServerConn) RawConn() net.Conn {
	return nil
}
func (s *killCurrentServerConn) HandleHandshake(_ *frontend.Packet, _ time.Duration) (*frontend.Packet, error) {
	return nil, nil
}
func (s *killCurrentServerConn) ExecStmt(stmt internalStmt, resp chan<- []byte) (bool, error) {
	return true, nil
}
func (s *killCurrentServerConn) GetCNServer() *CNServer   { return s.cn }
func (s *killCurrentServerConn) SetConnResponse(_ []byte) {}
func (s *killCurrentServerConn) GetConnResponse() []byte  { return nil }
func (s *killCurrentServerConn) CreateTime() time.Time    { return time.Now() }
func (s *killCurrentServerConn) Quit() error              { return nil }
func (s *killCurrentServerConn) Close() error {
	if s.closeFn != nil {
		return s.closeFn()
	}
	return nil
}

type killExecServerConn struct {
	stmt internalStmt
}

func (s *killExecServerConn) ConnID() uint32 { return 0 }
func (s *killExecServerConn) RawConn() net.Conn {
	return nil
}
func (s *killExecServerConn) HandleHandshake(_ *frontend.Packet, _ time.Duration) (*frontend.Packet, error) {
	return nil, nil
}
func (s *killExecServerConn) ExecStmt(stmt internalStmt, resp chan<- []byte) (bool, error) {
	s.stmt = stmt
	return true, nil
}
func (s *killExecServerConn) GetCNServer() *CNServer   { return nil }
func (s *killExecServerConn) SetConnResponse(_ []byte) {}
func (s *killExecServerConn) GetConnResponse() []byte  { return nil }
func (s *killExecServerConn) CreateTime() time.Time    { return time.Now() }
func (s *killExecServerConn) Quit() error              { return nil }
func (s *killExecServerConn) Close() error             { return nil }

func testStartClient(t *testing.T, tp *testProxyHandler, ci clientInfo, cn *CNServer) func() {
	if cn.salt == nil || len(cn.salt) != 20 {
		cn.salt = testSlat
	}
	clientProxy, client := net.Pipe()
	go func(ctx context.Context) {
		b := make([]byte, 10)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			_, _ = client.Read(b)
		}
	}(tp.ctx)
	tu := newTunnel(tp.ctx, tp.logger, tp.counterSet)
	sc, _, err := tp.ru.Connect(cn, testPacket, tu)
	require.NoError(t, err)
	cc := newMockClientConn(clientProxy, "t1", ci, tp.ru, tu)
	err = tu.run(cc, sc)
	require.NoError(t, err)
	select {
	case err := <-tu.errC:
		t.Fatalf("tunnel error: %v", err)
	default:
	}
	return func() {
		_ = tu.Close()
		_ = sc.Close()
	}
}

func TestClientConn_KillCurrentBackendConn(t *testing.T) {
	currentCN := &CNServer{
		connID: 10,
		uuid:   "cn1",
		addr:   "127.0.0.1:6001",
		salt:   testSlat,
	}
	execSC := &killExecServerConn{}
	activeTunnel := &tunnel{}
	router := &killTestRouter{
		connectFn: func(c *CNServer, handshakeResp *frontend.Packet, tun *tunnel) (ServerConn, []byte, error) {
			require.Equal(t, currentCN.uuid, c.uuid)
			require.Equal(t, currentCN.addr, c.addr)
			require.Equal(t, currentCN.salt, c.salt)
			require.NotZero(t, c.connID)
			require.Nil(t, tun, "temporary admin connections must not borrow the active session tunnel")
			return execSC, makeOKPacket(8), nil
		},
	}

	c := &clientConn{
		connID: 42,
		router: router,
		tun:    activeTunnel,
	}
	err := c.KillCurrentBackendConn(&killCurrentServerConn{cn: currentCN})
	require.NoError(t, err)
	require.Equal(t, cmdQuery, execSC.stmt.cmdType)
	require.Equal(t, "kill connection 42", execSC.stmt.s)
}

func TestClientConn_HandleQuitEventMarksExpectedCacheQuit(t *testing.T) {
	tun := &tunnel{}
	tun.mu.scp = &pipe{}
	tun.mu.scp.mu.cond = sync.NewCond(&tun.mu.scp.mu)

	c := &clientConn{
		tun: tun,
		sc:  &killCurrentServerConn{cn: &CNServer{connID: 11, uuid: "cn1"}},
		connCache: &mockConnCache{
			pushFn: func(key cacheKey, sc ServerConn) bool {
				return true
			},
		},
	}

	e := makeQuitEvent().(*quitEvent)
	errC := make(chan error, 1)
	go func() {
		errC <- c.HandleEvent(context.Background(), e, nil)
	}()
	e.wait()
	require.NoError(t, <-errC)
	require.True(t, tun.hasExpectedCacheQuit())
}

func copyCNServer(dst, src *CNServer) {
	dst.connID = src.connID
	dst.salt = make([]byte, len(src.salt))
	copy(dst.salt, src.salt[:])
	dst.reqLabel = src.reqLabel
	dst.cnLabel = make(map[string]metadata.LabelList)
	for k, v := range src.cnLabel {
		dst.cnLabel[k] = v
	}
	dst.hash = src.hash
	dst.uuid = src.uuid
	dst.addr = src.addr
	dst.internalConn = src.internalConn
	dst.clientAddr = src.clientAddr
}

func testStartNClients(t *testing.T, tp *testProxyHandler, ci clientInfo, cn *CNServer, n int) func() {
	var cleanFns []func()
	for i := 0; i < n; i++ {
		newCN := &CNServer{}
		copyCNServer(newCN, cn)
		c := testStartClient(t, tp, ci, newCN)
		cleanFns = append(cleanFns, c)
	}
	return func() {
		for _, f := range cleanFns {
			f()
		}
	}
}

func TestAccountParser(t *testing.T) {
	cases := []struct {
		str      string
		tenant   string
		username string
		hasErr   bool
	}{
		{
			str:      "t1:u1",
			tenant:   "t1",
			username: "u1",
			hasErr:   false,
		},
		{
			str:      "t1#u1",
			tenant:   "t1",
			username: "u1",
			hasErr:   false,
		},
		{
			str:      ":u1",
			tenant:   "",
			username: "",
			hasErr:   true,
		},
		{
			str:      "a:",
			tenant:   "",
			username: "",
			hasErr:   true,
		},
		{
			str:      "u1",
			tenant:   frontend.GetDefaultTenant(),
			username: "u1",
			hasErr:   false,
		},
		{
			str:      "t1:u1?a=1",
			tenant:   "t1",
			username: "u1",
			hasErr:   false,
		},
	}
	for _, item := range cases {
		a := clientInfo{}
		err := a.parse(item.str)
		if item.hasErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, string(a.labelInfo.Tenant), item.tenant)
		require.Equal(t, a.username, item.username)
	}
}

func newTestPu() *config.ParameterUnit {
	fp := config.FrontendParameters{}
	fp.SetDefaultValues()
	pu := config.NewParameterUnit(&fp, nil, nil, nil)
	return pu
}

func createNewClientConn(t *testing.T) (ClientConn, func()) {
	s := goetty.NewIOSession(goetty.WithSessionConn(1,
		newMockNetConn("127.0.0.1", 30001,
			"127.0.0.1", 30010, nil)),
		goetty.WithSessionCodec(WithProxyProtocolCodec(frontend.NewSqlCodec())))
	ctx, cancel := context.WithCancel(context.Background())
	frontend.SetSessionAlloc("", frontend.NewSessionAllocator(newTestPu()))
	clientBaseConnID = 90
	rt := runtime.DefaultRuntime()
	logger := rt.Logger()
	cs := newCounterSet()
	cc, err := newClientConn(
		ctx, &Config{}, logger, cs, s,
		nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, cc)
	return cc, func() {
		cancel()
		_ = cc.Close()
	}
}

func TestNewClientConn(t *testing.T) {
	cc, cleanup := createNewClientConn(t)
	defer cleanup()
	require.Equal(t, 91, int(cc.ConnID()))
	require.Equal(t, 20, len(cc.GetSalt()))
	require.NotNil(t, cc.RawConn())
}

func TestNewClientConnReleasesIOSessionWhenTLSConfigFails(t *testing.T) {
	s := goetty.NewIOSession(goetty.WithSessionConn(1,
		newMockNetConn("127.0.0.1", 30001,
			"127.0.0.1", 30010, nil)),
		goetty.WithSessionCodec(WithProxyProtocolCodec(frontend.NewSqlCodec())))
	allocator := frontend.NewLeakCheckAllocator()
	cfg := &Config{
		TLSEnabled:  true,
		TLSCAFile:   "testdata/does-not-exist-ca.pem",
		TLSCertFile: "testdata/does-not-exist-cert.pem",
		TLSKeyFile:  "testdata/does-not-exist-key.pem",
	}
	cc, err := newClientConn(
		context.Background(), cfg, runtime.DefaultRuntime().Logger(),
		newCounterSet(), s, nil, nil, nil, nil, nil, nil, nil,
		withClientConnAllocator(allocator),
	)
	require.Error(t, err)
	require.Nil(t, cc)
	require.True(t, allocator.CheckBalance())
}

func makeClientHandshakeResp() []byte {
	payload := make([]byte, 200)
	pos := 0
	copy(payload[pos:], []byte{141, 162, 10, 0}) // Capabilities Flags
	pos += 4
	copy(payload[pos:], []byte{0, 0, 0, 0}) // maximum packet size
	pos += 4
	payload[pos] = 45 // client charset
	pos += 1
	pos += 23 // filler
	username := "tenant1:user1"
	copy(payload[pos:], username) // login username
	pos += len(username)
	payload[pos] = 0 // the end of username
	pos += 1
	payload[pos] = 20 // length of auth response
	pos += 1
	pos += 20 // auth response
	dbname := "db1"
	copy(payload[pos:], dbname) // db name
	pos += len(dbname)
	payload[pos] = 0 // end of db name
	pos += 1
	plugin := "mysql_native_password"
	copy(payload[pos:], plugin)
	pos += 1 + len(plugin)
	data := make([]byte, pos+4)
	data[0] = uint8(pos)
	data[1] = uint8(pos >> 8)
	data[2] = uint8(pos >> 16)
	data[3] = 1
	copy(data[4:], payload)
	return data
}

func TestClientConn_ConnectToBackend(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	rt := runtime.DefaultRuntime()
	logger := rt.Logger()

	t.Run("cannot connect", func(t *testing.T) {
		nilC := (*clientConn)(nil)
		require.Equal(t, "", string(nilC.GetTenant()))
		require.Nil(t, nilC.RawConn())

		cc := &clientConn{
			log: logger,
		}
		cc.testHelper.connectToBackend = func() (ServerConn, error) {
			return nil, moerr.NewInternalErrorNoCtx("123 456")
		}

		sc, err := cc.BuildConnWithServer("aaa")
		require.ErrorContains(t, err, "123 456")
		require.Nil(t, sc)
	})

	t.Run("ok connect", func(t *testing.T) {
		local, remote := net.Pipe()
		require.NotNil(t, local)
		require.NotNil(t, remote)

		cc, cleanup := createNewClientConn(t)
		defer cleanup()
		c, ok := cc.(*clientConn)
		require.True(t, ok)
		require.NotNil(t, c)
		c.conn.UseConn(local)
		c.mysqlProto.UseConn(local)
		require.Equal(t, "", string(cc.GetTenant()))

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			b := make([]byte, 100)
			// client reads init handshake.
			n, err := remote.Read(b)
			require.NoError(t, err)
			require.NotEqual(t, 0, n)

			// client sends handshake resp.
			resp := makeClientHandshakeResp()
			n, err = remote.Write(resp)
			require.NoError(t, err)
			require.Equal(t, len(resp), n)
		}()

		_, err := cc.BuildConnWithServer("")
		require.Error(t, err) // just test client, no router set
		require.Equal(t, "tenant1", string(cc.GetTenant()))
		require.NotNil(t, cc.GetHandshakePack())
		wg.Wait()
	})
}

func TestClientConnHandshakeAdmission(t *testing.T) {
	limiter := newConnectionLimiter(3, 1)
	handleHandshake := func(lease *connectionLease) error {
		cc, cleanup := createNewClientConn(t)
		defer cleanup()
		client := cc.(*clientConn)
		client.admission = lease

		local, remote := net.Pipe()
		defer remote.Close()
		client.conn.UseConn(local)
		client.mysqlProto.UseConn(local)
		writeDone := make(chan struct{})
		go func() {
			defer close(writeDone)
			_, _ = remote.Write(makeClientHandshakeResp())
		}()
		err := client.handleHandshakeResp()
		<-writeDone
		return err
	}

	first, ok := limiter.acquire()
	require.True(t, ok)
	require.NoError(t, handleHandshake(first))

	second, ok := limiter.acquire()
	require.True(t, ok)
	require.ErrorIs(t, handleHandshake(second), errProxyConnectionLimit)
	second.release()

	first.release()
	third, ok := limiter.acquire()
	require.True(t, ok)
	require.NoError(t, handleHandshake(third))
	third.release()
	require.Equal(t, 0, limiter.total)
	require.Empty(t, limiter.byTenant)
}

type failAfterAllocator struct {
	frontend.Allocator
	remaining int
}

type failCapacityAllocator struct {
	frontend.Allocator
	failCapacity int
}

func (a *failCapacityAllocator) Alloc(capacity int) ([]byte, error) {
	if capacity == a.failCapacity {
		return nil, moerr.NewInternalErrorNoCtx("injected handoff allocator exhaustion")
	}
	return a.Allocator.Alloc(capacity)
}

func (a *failAfterAllocator) Alloc(capacity int) ([]byte, error) {
	if a.remaining == 0 {
		return nil, moerr.NewInternalErrorNoCtx("injected allocator exhaustion")
	}
	a.remaining--
	return a.Allocator.Alloc(capacity)
}

func TestClientConnHandshakeMemoryLifecycle(t *testing.T) {
	newClient := func(t *testing.T, allocator frontend.Allocator) (*clientConn, goetty.IOSession, net.Conn) {
		t.Helper()
		local, remote := net.Pipe()
		session := goetty.NewIOSession(
			goetty.WithSessionConn(1, local),
			goetty.WithSessionCodec(WithProxyProtocolCodec(frontend.NewSqlCodec())),
		)
		cc, err := newClientConn(
			context.Background(),
			&Config{ClientHandshakePacketLimit: defaultClientHandshakePacketLimit},
			runtime.DefaultRuntime().Logger(),
			newCounterSet(),
			session,
			nil, nil, nil, nil, nil, nil, nil,
			withClientConnAllocator(allocator),
		)
		require.NoError(t, err)
		return cc.(*clientConn), session, remote
	}

	t.Run("successful login transfers and releases ownership", func(t *testing.T) {
		allocator := frontend.NewLeakCheckAllocator()
		client, session, remote := newClient(t, allocator)
		defer remote.Close()

		response := makeClientHandshakeResp()
		payloadLength := int(defaultClientHandshakePacketLimit) - 1
		largeResponse := make([]byte, frontend.PacketHeaderLength+payloadLength)
		largeResponse[0] = byte(payloadLength)
		largeResponse[1] = byte(payloadLength >> 8)
		largeResponse[2] = byte(payloadLength >> 16)
		largeResponse[3] = response[3]
		copy(largeResponse[frontend.PacketHeaderLength:], response[frontend.PacketHeaderLength:])
		response = largeResponse
		writeDone := make(chan struct{})
		go func() {
			defer close(writeDone)
			_, _ = remote.Write(response)
		}()

		require.NoError(t, client.handleHandshakeResp())
		<-writeDone
		require.Equal(t, response[frontend.PacketHeaderLength:], client.handshakePack.Payload)
		buffered, ok := session.(goetty.BufferedIOSession)
		require.True(t, ok)
		require.Nil(t, buffered.InBuf().RawBuf(), "handshake-only input buffer must not cross into the tunnel phase")
		require.False(t, allocator.CheckBalance(), "live connection owns fixed and handshake buffers")

		require.NoError(t, client.Close())
		require.True(t, allocator.CheckBalance())
		require.NoError(t, client.Close(), "close must release the retained login exactly once")
		require.True(t, allocator.CheckBalance())
		require.NoError(t, session.Close())
	})

	t.Run("allocator exhaustion keeps cleanup balanced", func(t *testing.T) {
		leakCheck := frontend.NewLeakCheckAllocator()
		allocator := &failAfterAllocator{Allocator: leakCheck, remaining: 1}
		client, session, remote := newClient(t, allocator)
		defer remote.Close()

		writeDone := make(chan struct{})
		go func() {
			defer close(writeDone)
			_, _ = remote.Write(makeClientHandshakeResp())
		}()

		require.ErrorContains(t, client.handleHandshakeResp(), "injected allocator exhaustion")
		<-writeDone
		require.Nil(t, client.handshakePack)
		require.NoError(t, client.Close())
		require.True(t, leakCheck.CheckBalance())
		require.NoError(t, session.Close())
	})

	t.Run("close waits for backend handshake reader", func(t *testing.T) {
		allocator := frontend.NewLeakCheckAllocator()
		allocation, err := allocator.Alloc(4)
		require.NoError(t, err)
		copy(allocation, "auth")
		client := &clientConn{
			handshakePack: &frontend.Packet{
				Length:  4,
				Payload: allocation[:4],
			},
			handshakePayloadAllocation: allocation,
			sessionAllocator:           allocator,
		}

		readerEntered := make(chan struct{})
		releaseReader := make(chan struct{})
		readerPayload := make(chan []byte, 1)
		readerDone := make(chan struct{})
		go func() {
			defer close(readerDone)
			_, _, _ = client.connectWithHandshakePack(
				func(pack *frontend.Packet) (ServerConn, []byte, error) {
					close(readerEntered)
					<-releaseReader
					readerPayload <- append([]byte(nil), pack.Payload...)
					return nil, nil, nil
				},
			)
		}()
		<-readerEntered

		closeErr := make(chan error, 1)
		closeDone := make(chan struct{})
		go func() {
			defer close(closeDone)
			closeErr <- client.Close()
		}()
		require.Eventually(t, func() bool {
			if client.handshakePackMu.TryRLock() {
				client.handshakePackMu.RUnlock()
				return false
			}
			return true
		}, time.Second, time.Millisecond, "Close must wait as the exclusive payload owner")
		select {
		case <-closeDone:
			t.Fatal("Close released a handshake payload with an active reader")
		default:
		}

		close(releaseReader)
		<-readerDone
		<-closeDone
		require.NoError(t, <-closeErr)
		require.Equal(t, []byte("auth"), <-readerPayload)
		require.Nil(t, client.GetHandshakePack())
		require.True(t, allocator.CheckBalance())
	})

	t.Run("coalesced plaintext login hands off trailing packet", func(t *testing.T) {
		allocator := frontend.NewLeakCheckAllocator()
		client, session, remote := newClient(t, allocator)
		defer remote.Close()

		command := []byte{1, 0, 0, 0, 0x0e}
		stream := append(append([]byte{}, makeClientHandshakeResp()...), command...)
		writeDone := make(chan struct{})
		go func() {
			defer close(writeDone)
			_, _ = remote.Write(stream)
		}()

		require.NoError(t, client.handleHandshakeResp())
		<-writeDone
		require.IsType(t, &handshakeBufferedConn{}, client.RawConn())
		require.NoError(t, client.RawConn().SetReadDeadline(time.Now().Add(time.Second)))
		got := make([]byte, len(command))
		_, err := io.ReadFull(client.RawConn(), got)
		require.NoError(t, err)
		require.Equal(t, command, got)
		buffered := session.(goetty.BufferedIOSession)
		require.Nil(t, buffered.InBuf().RawBuf(), "phase-owned handshake buffer must be released")

		require.NoError(t, client.Close())
		require.NoError(t, session.Close())
		require.True(t, allocator.CheckBalance())
	})

	t.Run("trailing packet allocator exhaustion keeps cleanup balanced", func(t *testing.T) {
		command := []byte{1, 0, 0, 0, 0x0e}
		leakCheck := frontend.NewLeakCheckAllocator()
		allocator := &failCapacityAllocator{
			Allocator:    leakCheck,
			failCapacity: len(command),
		}
		client, session, remote := newClient(t, allocator)
		defer remote.Close()

		stream := append(append([]byte{}, makeClientHandshakeResp()...), command...)
		writeDone := make(chan struct{})
		go func() {
			defer close(writeDone)
			_, _ = remote.Write(stream)
		}()

		require.ErrorContains(t, client.handleHandshakeResp(), "injected handoff allocator exhaustion")
		<-writeDone
		require.NoError(t, client.Close())
		require.NoError(t, session.Close())
		require.True(t, leakCheck.CheckBalance())
	})

	t.Run("coalesced TLS login hands off trailing packet", func(t *testing.T) {
		allocator := frontend.NewLeakCheckAllocator()
		client, session, remote := newClient(t, allocator)

		cert, err := certGen(t.TempDir())
		require.NoError(t, err)
		client.tlsConfig, err = frontend.ConstructTLSConfig(
			context.Background(), cert.caFile, cert.certFile, cert.keyFile)
		require.NoError(t, err)
		client.tlsConnectTimeout = time.Second

		sslRequestPayload := make([]byte, 32)
		binary.LittleEndian.PutUint32(
			sslRequestPayload,
			frontend.DefaultCapability|frontend.CLIENT_SSL,
		)
		sslRequest := make([]byte, frontend.PacketHeaderLength+len(sslRequestPayload))
		sslRequest[0] = byte(len(sslRequestPayload))
		sslRequest[3] = 1
		copy(sslRequest[frontend.PacketHeaderLength:], sslRequestPayload)

		command := []byte{1, 0, 0, 0, 0x0e}
		stream := append(append([]byte{}, makeClientHandshakeResp()...), command...)
		clientDone := make(chan error, 1)
		releaseClient := make(chan struct{})
		clientClosed := make(chan struct{})
		var releaseClientOnce sync.Once
		releaseClientRead := func() {
			releaseClientOnce.Do(func() { close(releaseClient) })
		}
		t.Cleanup(func() {
			releaseClientRead()
			_ = remote.Close()
			<-clientClosed
		})
		go func() {
			defer close(clientClosed)
			if _, err := remote.Write(sslRequest); err != nil {
				clientDone <- err
				return
			}
			tlsClient := tls.Client(remote, &tls.Config{InsecureSkipVerify: true}) //nolint:gosec
			if err := tlsClient.Handshake(); err != nil {
				clientDone <- err
				return
			}
			_, err := tlsClient.Write(stream)
			clientDone <- err
			<-releaseClient
			_, _ = io.Copy(io.Discard, tlsClient)
			_ = tlsClient.Close()
		}()

		require.NoError(t, client.handleHandshakeResp())
		require.NoError(t, <-clientDone)
		require.IsType(t, &handshakeBufferedConn{}, client.RawConn())
		require.NoError(t, client.RawConn().SetReadDeadline(time.Now().Add(time.Second)))
		got := make([]byte, len(command))
		_, err = io.ReadFull(client.RawConn(), got)
		require.NoError(t, err)
		require.Equal(t, command, got)
		buffered := session.(goetty.BufferedIOSession)
		require.Nil(t, buffered.InBuf().RawBuf(), "phase-owned handshake buffer must be released")

		releaseClientRead()
		require.NoError(t, client.Close())
		<-clientClosed
		require.NoError(t, session.Close())
		require.True(t, allocator.CheckBalance())
	})
}

func TestClientConnRejectsProxyHeaderAfterAddresslessFrame(t *testing.T) {
	cc, cleanup := createNewClientConn(t)
	defer cleanup()
	client := cc.(*clientConn)

	local, remote := net.Pipe()
	defer remote.Close()
	client.conn.UseConn(local)
	client.mysqlProto.UseConn(local)

	localHeader := make([]byte, ProxyHeaderLength)
	copy(localHeader, ProxyProtocolV2Signature)
	localHeader[12] = 0x20
	localHeader[13] = unspec

	addressHeader := make([]byte, ProxyHeaderLength+12)
	copy(addressHeader, ProxyProtocolV2Signature)
	addressHeader[12] = 0x21
	addressHeader[13] = tcpOverIPv4
	binary.BigEndian.PutUint16(addressHeader[14:], 12)
	copy(addressHeader[ProxyHeaderLength:], []byte{
		10, 0, 0, 1,
		10, 0, 0, 2,
		0x1f, 0x40,
		0x17, 0x71,
	})

	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		_, _ = remote.Write(append(localHeader, addressHeader...))
	}()

	_, err := client.readPacketBefore(time.Now().Add(time.Second))
	require.ErrorContains(t, err, "duplicate PROXY protocol header")
	<-writeDone
	require.True(t, client.proxyHeaderReceived)
}

func TestClientConnHandshakeTimeout(t *testing.T) {
	t.Run("silent client times out", func(t *testing.T) {
		cc, cleanup := createNewClientConn(t)
		defer cleanup()
		client := cc.(*clientConn)
		client.clientHandshakeTimeout = 20 * time.Millisecond

		local, remote := net.Pipe()
		defer remote.Close()
		client.conn.UseConn(local)
		client.mysqlProto.UseConn(local)

		err := client.handleHandshakeResp()
		require.Error(t, err)
		var netErr net.Error
		require.ErrorAs(t, err, &netErr)
		require.True(t, netErr.Timeout())
	})

	t.Run("fragmented client cannot renew the absolute deadline", func(t *testing.T) {
		cc, cleanup := createNewClientConn(t)
		defer cleanup()
		client := cc.(*clientConn)
		client.clientHandshakeTimeout = 100 * time.Millisecond

		local, remote := net.Pipe()
		client.conn.UseConn(local)
		client.mysqlProto.UseConn(local)

		writeDone := make(chan struct{})
		go func() {
			defer close(writeDone)
			payload := makeClientHandshakeResp()
			ticker := time.NewTicker(20 * time.Millisecond)
			defer ticker.Stop()
			for i := range payload {
				<-ticker.C
				_ = remote.SetWriteDeadline(time.Now().Add(40 * time.Millisecond))
				if _, err := remote.Write(payload[i : i+1]); err != nil {
					return
				}
			}
		}()

		start := time.Now()
		errC := make(chan error, 1)
		go func() {
			errC <- client.handleHandshakeResp()
		}()
		select {
		case err := <-errC:
			require.Error(t, err)
			var netErr net.Error
			require.ErrorAs(t, err, &netErr)
			require.True(t, netErr.Timeout())
			require.Less(t, time.Since(start), 8*client.clientHandshakeTimeout)
		case <-time.After(10 * client.clientHandshakeTimeout):
			t.Fatal("fragmented handshake outlived its absolute deadline")
		}
		require.NoError(t, remote.Close())
		<-writeDone
	})

	t.Run("successful handshake clears read deadline", func(t *testing.T) {
		cc, cleanup := createNewClientConn(t)
		defer cleanup()
		client := cc.(*clientConn)
		client.clientHandshakeTimeout = time.Second

		local, remote := net.Pipe()
		defer remote.Close()
		tracked := &readDeadlineTrackingConn{Conn: local}
		client.conn.UseConn(tracked)
		client.mysqlProto.UseConn(tracked)

		peerDone := make(chan struct{})
		go func() {
			defer close(peerDone)
			header := make([]byte, 4)
			if _, err := io.ReadFull(remote, header); err != nil {
				return
			}
			payloadLength := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
			payload := make([]byte, payloadLength)
			if _, err := io.ReadFull(remote, payload); err != nil {
				return
			}
			_, _ = remote.Write(makeClientHandshakeResp())
		}()

		_, err := client.BuildConnWithServer("")
		require.Error(t, err) // no router is configured after the handshake
		<-peerDone
		require.Same(t, tracked, client.RawConn())
		require.True(t, tracked.readDeadline().IsZero())
	})
}

func TestOversizedHandshakeErrorPreservesPacketSequence(t *testing.T) {
	for _, sequenceID := range []uint8{1, 2} {
		t.Run(fmt.Sprintf("sequence-%d", sequenceID), func(t *testing.T) {
			local, remote := net.Pipe()
			defer remote.Close()
			session := goetty.NewIOSession(
				goetty.WithSessionConn(1, local),
				goetty.WithSessionCodec(WithProxyProtocolCodec(frontend.NewSqlCodec(
					frontend.WithSQLCodecMaxPayloadSize(16),
				))),
			)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cc, err := newClientConn(
				ctx,
				&Config{ClientHandshakePacketLimit: 16},
				runtime.DefaultRuntime().Logger(),
				newCounterSet(),
				session,
				nil, nil, nil, nil, nil, nil, nil,
			)
			require.NoError(t, err)
			defer cc.Close()
			client := cc.(*clientConn)
			client.mysqlProto.SetSequenceID(sequenceID)

			writeDone := make(chan struct{})
			go func() {
				defer close(writeDone)
				_, _ = remote.Write([]byte{17, 0, 0, sequenceID})
			}()
			_, err = client.readPacketBefore(time.Now().Add(time.Second))
			<-writeDone
			require.ErrorIs(t, err, frontend.ErrPacketTooLarge)

			sendDone := make(chan struct{})
			go func() {
				defer close(sendDone)
				client.SendErrToClient(err)
			}()
			header := make([]byte, 4)
			_, err = io.ReadFull(remote, header)
			require.NoError(t, err)
			payloadLength := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
			payload := make([]byte, payloadLength)
			_, err = io.ReadFull(remote, payload)
			require.NoError(t, err)
			<-sendDone
			require.Equal(t, sequenceID+1, header[3])
			require.Equal(t, byte(0xff), payload[0])
			require.Equal(t, moerr.ER_SERVER_NET_PACKET_TOO_LARGE, binary.LittleEndian.Uint16(payload[1:3]))
		})
	}
}

func TestClientConn_ReadPacket(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cc, cleanup := createNewClientConn(t)
	defer cleanup()
	c, ok := cc.(*clientConn)
	require.True(t, ok)
	require.NotNil(t, c)

	local, remote := net.Pipe()
	require.NotNil(t, local)
	require.NotNil(t, remote)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		addr := &ProxyAddr{
			SourceAddress: []byte{10, 10, 10, 10},
			SourcePort:    1000,
			TargetAddress: []byte{20, 20, 20, 20},
			TargetPort:    2000,
		}

		b := buf.NewByteBuf(1000)

		b.WriteString(ProxyProtocolV2Signature)
		err := b.WriteByte(0)
		require.NoError(t, err)
		err = b.WriteByte(0)
		require.NoError(t, err)
		b.WriteUint16(12)
		n, err := b.Write(addr.SourceAddress)
		require.Equal(t, 4, n)
		require.NoError(t, err)
		n, err = b.Write(addr.TargetAddress)
		require.Equal(t, 4, n)
		require.NoError(t, err)
		b.WriteUint16(addr.SourcePort)
		b.WriteUint16(addr.TargetPort)

		n, d := b.ReadAll()
		require.Equal(t, 28, n)
		err = binary.Write(remote, binary.BigEndian, d)
		require.NoError(t, err)

		// little endian
		err = b.WriteByte(9)
		require.NoError(t, err)
		err = b.WriteByte(0)
		require.NoError(t, err)
		err = b.WriteByte(0)
		require.NoError(t, err)
		err = b.WriteByte(0)
		require.NoError(t, err)
		err = b.WriteByte(3)
		require.NoError(t, err)
		b.WriteString("select 1")

		n, d = b.ReadAll()
		require.Equal(t, 13, n)
		err = binary.Write(remote, binary.LittleEndian, d)
		require.NoError(t, err)
	}()

	c.conn.UseConn(local)
	ret, err := c.readPacket()
	require.NoError(t, err)
	require.NotNil(t, ret)
	require.Equal(t, 9, int(ret.Length))
	require.Equal(t, 0, int(ret.SequenceID))
	require.Equal(t, 3, int(ret.Payload[0]))
	require.Equal(t, "select 1", string(ret.Payload[1:]))

	wg.Wait()
}

func TestClientConn_ConnID(t *testing.T) {
	parallel := 100
	clientBaseConnID = 1
	var wg sync.WaitGroup
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func() {
			nextClientConnID()
			defer wg.Done()
		}()
	}
	wg.Wait()
	require.Equal(t, 101, int(clientBaseConnID))
}

func TestClientConn_SendErrToClient(t *testing.T) {
	local, remote := net.Pipe()
	require.NotNil(t, local)
	require.NotNil(t, remote)

	cc, cleanup := createNewClientConn(t)
	defer cleanup()
	c, ok := cc.(*clientConn)
	require.True(t, ok)
	require.NotNil(t, c)
	c.conn.UseConn(local)
	c.mysqlProto.UseConn(local)
	require.Equal(t, "", string(cc.GetTenant()))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		b := make([]byte, 100)
		// client reads init handshake.
		n, err := remote.Read(b)
		require.NoError(t, err)
		require.NotEqual(t, 0, n)

		// client sends handshake resp.
		resp := makeClientHandshakeResp()
		n, err = remote.Write(resp)
		require.NoError(t, err)
		require.Equal(t, len(resp), n)

		n, err = remote.Read(b)
		require.NoError(t, err)
		require.Equal(t, 33, n)
		require.True(t, strings.Contains(string(b[4+1+2+1+5:n]), "internal error: msg1"))
	}()

	_, err := cc.BuildConnWithServer("")
	require.Error(t, err) // just test client, no router set
	require.Equal(t, "tenant1", string(cc.GetTenant()))
	require.NotNil(t, cc.GetHandshakePack())
	cc.SendErrToClient(moerr.NewInternalErrorNoCtx("msg1"))
	wg.Wait()
}

var _ Router = &testRouter{}

const (
	routerReturnErrSecondTime = 1
)

type testRouter struct {
	mod int
	cnt int
}

type shortHandshakeServerConn struct {
	*mockServerConn
	closeCount           int
	setConnResponseCount int
}

func (s *shortHandshakeServerConn) Close() error {
	s.closeCount++
	return nil
}

func (s *shortHandshakeServerConn) SetConnResponse([]byte) {
	s.setConnResponseCount++
}

type shortHandshakeRouter struct {
	testRouter
	response []byte
	sc       *shortHandshakeServerConn
}

func (r *shortHandshakeRouter) Connect(*CNServer, *frontend.Packet, *tunnel) (ServerConn, []byte, error) {
	return r.sc, r.response, nil
}

type writeCountingConn struct {
	net.Conn
	writeCount int
}

func (c *writeCountingConn) Write(b []byte) (int, error) {
	c.writeCount++
	return len(b), nil
}

func (router *testRouter) Route(ctx context.Context, sid string, client clientInfo, filter func(string) bool) (*CNServer, error) {
	if router.mod == routerReturnErrSecondTime {
		if router.cnt >= 1 {
			return nil, moerr.NewInternalErrorNoCtx("route return error")
		}
		router.cnt++
	}
	return &CNServer{}, nil
}

func (router *testRouter) SelectByConnID(connID uint32) (*CNServer, error) {
	//TODO implement me
	panic("implement me")
}

func (router *testRouter) AllServers(sid string) ([]*CNServer, error) {
	//TODO implement me
	panic("implement me")
}

func (router *testRouter) Connect(c *CNServer, handshakeResp *frontend.Packet, t *tunnel) (ServerConn, []byte, error) {
	return newMockServerConn(nil), nil, nil
}

type testRouteSelectedRouter struct {
	connectCalls              int
	connectRouteSelectedCalls int
	routeCalls                int
	routeForTransferCalls     int
}

func (r *testRouteSelectedRouter) Route(ctx context.Context, sid string, client clientInfo, filter func(string) bool) (*CNServer, error) {
	r.routeCalls++
	addr := "127.0.0.1:6001"
	if filter != nil && filter(addr) {
		return nil, noCNServerErr
	}
	return &CNServer{addr: addr}, nil
}

func (r *testRouteSelectedRouter) RouteForTransfer(ctx context.Context, sid string, client clientInfo, filter func(string) bool) (*CNServer, error) {
	r.routeForTransferCalls++
	return r.Route(ctx, sid, client, filter)
}

func (r *testRouteSelectedRouter) SelectByConnID(connID uint32) (*CNServer, error) {
	return nil, nil
}

func (r *testRouteSelectedRouter) AllServers(sid string) ([]*CNServer, error) {
	return nil, nil
}

func (r *testRouteSelectedRouter) Connect(c *CNServer, handshakeResp *frontend.Packet, t *tunnel) (ServerConn, []byte, error) {
	r.connectCalls++
	return nil, nil, newConnectErr(moerr.NewInternalErrorNoCtx("connect failed"))
}

func (r *testRouteSelectedRouter) ConnectRouteSelected(c *CNServer, handshakeResp *frontend.Packet, t *tunnel) (ServerConn, []byte, error) {
	r.connectRouteSelectedCalls++
	return nil, nil, newConnectErr(moerr.NewInternalErrorNoCtx("connect failed"))
}

var _ client.QueryClient = &testQueryClient{}

type testQueryClient struct {
}

func (client *testQueryClient) ServiceID() string {
	//TODO implement me
	panic("implement me")
}

func (client *testQueryClient) SendMessage(ctx context.Context, address string, req *query.Request) (*query.Response, error) {
	return nil, moerr.NewInternalErrorNoCtx("return error")
}

func (client *testQueryClient) NewRequest(method query.CmdMethod) *query.Request {
	return &query.Request{}
}

func (client *testQueryClient) Release(response *query.Response) {
	//TODO implement me
	panic("implement me")
}

func (client *testQueryClient) Close() error {
	//TODO implement me
	panic("implement me")
}

var _ clusterservice.MOCluster = &testCluster{}

type testCluster struct {
}

func (cluster *testCluster) GetCNService(selector clusterservice.Selector, apply func(metadata.CNService) bool) {
}

func (cluster *testCluster) GetTNService(selector clusterservice.Selector, apply func(metadata.TNService) bool) {
	//TODO implement me
	panic("implement me")
}

func (cluster *testCluster) GetAllTNServices() []metadata.TNService {
	//TODO implement me
	panic("implement me")
}

func (cluster *testCluster) GetCNServiceWithoutWorkingState(selector clusterservice.Selector, apply func(metadata.CNService) bool) {
	//TODO implement me
	panic("implement me")
}

func (cluster *testCluster) ForceRefresh(sync bool) {
	//TODO implement me
	panic("implement me")
}

func (cluster *testCluster) Close() {
	//TODO implement me
	panic("implement me")
}

func (cluster *testCluster) DebugUpdateCNLabel(uuid string, kvs map[string][]string) error {
	//TODO implement me
	panic("implement me")
}

func (cluster *testCluster) DebugUpdateCNWorkState(uuid string, state int) error {
	//TODO implement me
	panic("implement me")
}

func (cluster *testCluster) RemoveCN(id string) {
	//TODO implement me
	panic("implement me")
}

func (cluster *testCluster) AddCN(service metadata.CNService) {
	//TODO implement me
	panic("implement me")
}

func (cluster *testCluster) UpdateCN(service metadata.CNService) {
	//TODO implement me
	panic("implement me")
}

func Test_connectToBackend(t *testing.T) {
	rt := runtime.DefaultRuntime()
	logger := rt.Logger()
	cConn := &clientConn{
		ctx:         context.Background(),
		router:      &testRouter{mod: routerReturnErrSecondTime},
		mysqlProto:  &frontend.MysqlProtocolImpl{},
		queryClient: &testQueryClient{},
		moCluster:   &testCluster{},
		log:         logger,
	}
	sConn, err := cConn.connectToBackend("127.0.0.1")
	require.Error(t, err)
	require.Nil(t, sConn)
}

func Test_connectToBackend_ShortHandshakeResponse(t *testing.T) {
	for responseLen := 0; responseLen < 5; responseLen++ {
		t.Run(fmt.Sprintf("length_%d", responseLen), func(t *testing.T) {
			cc, cleanup := createNewClientConn(t)
			defer cleanup()

			client, ok := cc.(*clientConn)
			require.True(t, ok)

			local, remote := net.Pipe()
			defer remote.Close()
			clientConn := &writeCountingConn{Conn: local}
			client.mysqlProto.UseConn(clientConn)

			serverConn := &shortHandshakeServerConn{mockServerConn: newMockServerConn(nil)}
			client.router = &shortHandshakeRouter{
				response: make([]byte, responseLen),
				sc:       serverConn,
			}
			failuresBefore := testutil.ToFloat64(v2.ProxyConnectCommonFailCounter)

			var (
				got ServerConn
				err error
			)
			require.NotPanics(t, func() {
				got, err = client.connectToBackend("")
			})
			require.ErrorContains(t, err, "response from cn server")
			require.Nil(t, got)
			require.Equal(t, 1, serverConn.closeCount)
			require.Equal(t, 0, serverConn.setConnResponseCount)
			require.Equal(t, 0, clientConn.writeCount)
			require.Equal(t, failuresBefore+1, testutil.ToFloat64(v2.ProxyConnectCommonFailCounter))
		})
	}
}

func Test_connectToBackend_SkipCacheOnMigration(t *testing.T) {
	rt := runtime.DefaultRuntime()
	logger := rt.Logger()
	cache := &popCountConnCache{}
	cConn := &clientConn{
		ctx:        context.Background(),
		router:     &routeErrRouter{},
		mysqlProto: &frontend.MysqlProtocolImpl{},
		connCache:  cache,
		log:        logger,
	}
	sConn, err := cConn.connectToBackend("127.0.0.1:6001")
	require.Error(t, err)
	require.Nil(t, sConn)
	require.Equal(t, 0, cache.popCount)
}

func Test_connectToBackend_SkipCacheWhenPluginRouterEnabled(t *testing.T) {
	cache := &popCountConnCache{}
	cc, cleanup := createNewClientConn(t)
	defer cleanup()
	cConn, ok := cc.(*clientConn)
	require.True(t, ok)
	cConn.connCache = cache
	cConn.router = newPluginRouter("", &routeErrRouter{}, &mockPlugin{
		mockRecommendCNFn: func(ctx context.Context, clientInfo clientInfo) (*plugin.Recommendation, error) {
			return &plugin.Recommendation{Action: plugin.Bypass}, nil
		},
	})

	sConn, err := cConn.connectToBackend("")
	require.Error(t, err)
	require.Nil(t, sConn)
	require.Equal(t, 0, cache.popCount, "plugin routing must not reuse cached sessions")
}

func Test_connectToBackend_UsesRouteSelectedOnlyForFirstLogin(t *testing.T) {
	router := &testRouteSelectedRouter{}
	cc, cleanup := createNewClientConn(t)
	defer cleanup()
	cConn, ok := cc.(*clientConn)
	require.True(t, ok)
	cConn.router = router

	// First login: Route-selected new-session connect should use
	// ConnectRouteSelected.
	sConn, err := cConn.connectToBackend("")
	require.Error(t, err)
	require.Nil(t, sConn)
	require.Greater(t, router.connectRouteSelectedCalls, 0, "first login must use ConnectRouteSelected")
	require.Equal(t, 0, router.connectCalls)
	require.Greater(t, router.routeCalls, 0, "first login must route through Route")
	require.Equal(t, 0, router.routeForTransferCalls)

	// Migration / transfer: must use plain Connect and NOT mutate breaker
	// state via ConnectRouteSelected.
	sConn, err = cConn.connectToBackend("127.0.0.1:7000")
	require.Error(t, err)
	require.Nil(t, sConn)
	require.GreaterOrEqual(t, router.connectRouteSelectedCalls, 1)
	require.Greater(t, router.connectCalls, 0, "migration must use plain Connect")
	require.Greater(t, router.routeForTransferCalls, 0, "migration must use RouteForTransfer when available")
}

func TestHandleSetVar(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var cc clientConn
	cc.migration.setVarStmtMap = make(map[string]struct{})
	e0 := &setVarEvent{
		baseEvent: baseEvent{waitC: make(chan struct{}, 5)},
		stmt:      "set autocommit=0",
	}
	require.NoError(t, cc.handleSetVar(e0))
	require.Equal(t, 1, len(cc.migration.setVarStmtMap))
	require.Equal(t, 1, len(cc.migration.setVarStmts))
	require.Equal(t, e0.stmt, cc.migration.setVarStmts[len(cc.migration.setVarStmts)-1])

	require.NoError(t, cc.handleSetVar(e0))
	require.Equal(t, 1, len(cc.migration.setVarStmtMap))
	require.Equal(t, 1, len(cc.migration.setVarStmts))
	require.Equal(t, e0.stmt, cc.migration.setVarStmts[len(cc.migration.setVarStmts)-1])

	e1 := &setVarEvent{
		baseEvent: baseEvent{waitC: make(chan struct{}, 5)},
		stmt:      "set autocommit=1",
	}
	require.NoError(t, cc.handleSetVar(e1))
	require.Equal(t, 2, len(cc.migration.setVarStmtMap))
	require.Equal(t, 2, len(cc.migration.setVarStmts))
	require.Equal(t, e1.stmt, cc.migration.setVarStmts[len(cc.migration.setVarStmts)-1])

	require.NoError(t, cc.handleSetVar(e0))
	require.Equal(t, 2, len(cc.migration.setVarStmtMap))
	require.Equal(t, 2, len(cc.migration.setVarStmts))
	require.Equal(t, e0.stmt, cc.migration.setVarStmts[len(cc.migration.setVarStmts)-1])

	require.NoError(t, cc.handleSetVar(e1))
	require.Equal(t, 2, len(cc.migration.setVarStmtMap))
	require.Equal(t, 2, len(cc.migration.setVarStmts))
	require.Equal(t, e1.stmt, cc.migration.setVarStmts[len(cc.migration.setVarStmts)-1])
}

// testTimeoutRouter is a router that simulates timeout errors for all CN servers.
type testTimeoutRouter struct {
	servers   []*CNServer
	callCount int
}

func (r *testTimeoutRouter) Route(ctx context.Context, sid string, client clientInfo, filter func(string) bool) (*CNServer, error) {
	// Filter out bad servers
	for _, s := range r.servers {
		if filter == nil || !filter(s.addr) {
			return s, nil
		}
	}
	// All servers filtered out
	return nil, noCNServerErr
}

func (r *testTimeoutRouter) SelectByConnID(connID uint32) (*CNServer, error) {
	return nil, nil
}

func (r *testTimeoutRouter) AllServers(sid string) ([]*CNServer, error) {
	return r.servers, nil
}

func (r *testTimeoutRouter) Connect(c *CNServer, handshakeResp *frontend.Packet, t *tunnel) (ServerConn, []byte, error) {
	r.callCount++
	// Always return timeout error to simulate busy CN servers
	return nil, nil, newTimeoutConnectErr(moerr.NewInternalErrorNoCtx("handshake timeout"))
}

// TestBuildConnWithServer_AllCNServersBusy tests the scenario where all CN servers
// are busy (timeout during handshake), which should return allCNServersBusyErr.
func TestBuildConnWithServer_AllCNServersBusy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	logger := rt.Logger()

	// Create a router with multiple CN servers that all timeout
	router := &testTimeoutRouter{
		servers: []*CNServer{
			{uuid: "cn1", addr: "127.0.0.1:6001"},
			{uuid: "cn2", addr: "127.0.0.1:6002"},
		},
	}

	// Create a mock connection for RawConn()
	local, _ := net.Pipe()
	mockConn := newMockNetConn("127.0.0.1", 30001, "127.0.0.1", 30010, local)
	mockIOSession := goetty.NewIOSession(
		goetty.WithSessionConn(1, mockConn),
		goetty.WithSessionCodec(WithProxyProtocolCodec(frontend.NewSqlCodec())),
	)

	cc := &clientConn{
		ctx:        context.Background(),
		router:     router,
		conn:       mockIOSession,
		mysqlProto: &frontend.MysqlProtocolImpl{},
		log:        logger,
		clientInfo: clientInfo{
			labelInfo: labelInfo{
				Tenant: "test_tenant",
			},
		},
	}

	// Call connectToBackend which should try all CN servers and fail with timeout
	sc, err := cc.connectToBackend("")
	require.Error(t, err)
	require.Nil(t, sc)
	// Should return allCNServersBusyErr since all servers timed out
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrAllCNServersBusy),
		"expected ErrAllCNServersBusy, got: %v", err)
	// Should have tried both CN servers
	require.Equal(t, 2, router.callCount)
}
