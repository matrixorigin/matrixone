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
	"errors"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/petermattis/goid"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	pb "github.com/matrixorigin/matrixone/pkg/pb/proxy"
)

// serverBaseConnID is the base connection ID for server.
var serverBaseConnID uint32 = 1000

// ServerConn is the connection to the backend server.
type ServerConn interface {
	// ConnID returns connection ID of the backend CN server.
	ConnID() uint32
	// RawConn return the raw connection.
	RawConn() net.Conn
	// HandleHandshake handles the handshake communication with CN server.
	// handshakeResp is a auth packet received from client.
	HandleHandshake(handshakeResp *frontend.Packet, timeout time.Duration) (*frontend.Packet, error)
	// ExecStmt executes a simple statement, it sends a query to backend server.
	// After it finished, server connection should be closed immediately because
	// it is a temp connection.
	// The first return value indicates that if the execution result is OK.
	// NB: the stmt can only be simple stmt, which returns OK or Err only.
	ExecStmt(stmt internalStmt, resp chan<- []byte) (bool, error)
	// GetCNServer returns the cn server instance of the server connection.
	GetCNServer() *CNServer
	// SetConnResponse sets the login response which is returned from a cn server.
	// It is mainly used in connection cache. When a connection is reused, the response
	// will be sent to client.
	SetConnResponse(r []byte)
	// GetConnResponse returns the connection response which is got from a cn server.
	GetConnResponse() []byte
	// CreateTime return the creation time of the server connection.
	CreateTime() time.Time
	// Quit sends quit command to cn server and disconnect from connection manager
	// and close the TCP connection.
	Quit() error
	// Close disconnect with connection manager.
	Close() error
}

// serverConn is the connection between proxy and CN server.
type serverConn struct {
	// cnServer is the backend CN server.
	cnServer *CNServer
	// conn is the raw TCP connection between proxy and server.
	conn net.Conn
	// connID is the proxy connection ID, which is not useful in most case.
	connID uint32
	// mysqlProto is used to build handshake info.
	mysqlProto *frontend.MysqlProtocolImpl
	// rebalancer is used to track connections between proxy and server.
	rebalancer *rebalancer
	// tun is the tunnel which this server connection belongs to.
	tun *tunnel
	// connResp is the response bytes which is got from cn server when
	// connect to the cn.
	connResp []byte
	// createTime is the creation time of this connection.
	createTime time.Time
	// closeOnce only close the connection once.
	closeOnce sync.Once
}

var _ ServerConn = (*serverConn)(nil)

type contextHandshakeHandler interface {
	HandleHandshakeContext(
		ctx context.Context,
		handshakeResp *frontend.Packet,
		timeout time.Duration,
	) (*frontend.Packet, error)
}

type contextStmtExecutor interface {
	ExecStmtContext(
		ctx context.Context,
		stmt internalStmt,
		resp chan<- []byte,
	) (bool, error)
}

func execStmtWithContext(
	ctx context.Context,
	sc ServerConn,
	stmt internalStmt,
	resp chan<- []byte,
) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if cause := operationContextCause(ctx); cause != nil {
		return false, cause
	}
	if contextual, ok := sc.(contextStmtExecutor); ok {
		return contextual.ExecStmtContext(ctx, stmt, resp)
	}
	return sc.ExecStmt(stmt, resp)
}

const (
	// Proxy only needs a small retained buffer for normal MySQL packets.
	// Larger packets use frontend.Conn's dynamic path and are released after use.
	proxyIOSessionBufferSize = 16 * 1024
	// A backend connection is used only for authentication and bounded Proxy
	// control statements before the raw tunnel owns it. It must not accept the
	// general 16 MiB MySQL payload maximum: doing so lets one CN response create
	// an unaccounted, connection-lifetime buffer in the Proxy.
	proxyBackendPacketLimit = 64 * 1024
	// A successful authentication response is retained for connection-cache
	// reuse. Normal OK packets are tiny; cap this distinct lifetime class so a
	// CN cannot turn the transient packet allowance into steady memory.
	proxyBackendAuthResponseLimit     = 1024
	proxyBackendRetainedResponseLimit = proxyBackendAuthResponseLimit + frontend.PacketHeaderLength
)

// newServerConn creates a connection to CN server.
func newServerConn(
	cn *CNServer,
	tun *tunnel,
	r *rebalancer,
	timeout time.Duration,
	allocators ...frontend.Allocator,
) (ServerConn, error) {
	return newServerConnContext(
		context.Background(), cn, tun, r, timeout, allocators...,
	)
}

func newServerConnContext(
	ctx context.Context,
	cn *CNServer,
	tun *tunnel,
	r *rebalancer,
	timeout time.Duration,
	allocators ...frontend.Allocator,
) (ServerConn, error) {
	var logger *zap.Logger
	if r != nil && r.logger != nil {
		logger = r.logger.RawLogger()
	}
	c, err := cn.ConnectContext(ctx, logger, timeout)
	if err != nil {
		return nil, err
	}
	owned := true
	defer func() {
		if owned {
			_ = c.Close()
		}
	}()
	s := &serverConn{
		cnServer:   cn,
		conn:       c,
		connID:     nextServerConnID(),
		rebalancer: r,
		tun:        tun,
		createTime: time.Now(),
	}
	fp := config.FrontendParameters{}
	fp.SetDefaultValues()
	pu := config.NewParameterUnit(&fp, nil, nil, nil)
	frontend.InitServerLevelVars(cn.uuid)
	var allocator frontend.Allocator
	if len(allocators) > 0 {
		allocator = allocators[0]
	}
	if allocator == nil {
		allocator = frontend.NewSessionAllocator(pu)
	}
	ios, err := frontend.NewIOSessionWithOptions(
		c,
		pu,
		cn.uuid,
		frontend.WithIOSessionBufferSize(proxyIOSessionBufferSize),
		frontend.WithIOSessionAllowedPacketSize(proxyBackendPacketLimit),
		frontend.WithIOSessionAllocator(allocator),
	)
	if err != nil {
		return nil, err
	}
	s.mysqlProto = frontend.NewMysqlClientProtocol(cn.uuid, s.connID, ios, 0, &fp)
	owned = false
	return s, nil
}

// wrappedConn wraps the connection to disconnect from connection manager.
type wrappedConn struct {
	net.Conn
	sync.Once
	closeFn func()
}

// Close closes the wrapped connection, which calls closeFn to disconnect
// from the connection manager.
func (w *wrappedConn) Close() error {
	if w != nil && w.closeFn != nil {
		w.Once.Do(w.closeFn)
	}
	return w.Conn.Close()
}

// ConnID implements the ServerConn interface.
func (s *serverConn) ConnID() uint32 {
	return s.connID
}

// RawConn implements the ServerConn interface.
func (s *serverConn) RawConn() net.Conn {
	if s != nil && s.conn != nil {
		if s.cnServer != nil && s.rebalancer != nil {
			return &wrappedConn{
				Conn: s.conn,
				closeFn: func() {
					s.rebalancer.connManager.disconnect(s.cnServer, s.tun)
				},
			}
		}
		return s.conn
	}
	return nil
}

// HandleHandshake implements the ServerConn interface.
func (s *serverConn) HandleHandshake(
	handshakeResp *frontend.Packet, timeout time.Duration,
) (*frontend.Packet, error) {
	return s.HandleHandshakeContext(context.Background(), handshakeResp, timeout)
}

func (s *serverConn) HandleHandshakeContext(
	parent context.Context,
	handshakeResp *frontend.Packet,
	timeout time.Duration,
) (*frontend.Packet, error) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeoutCause(parent, timeout, moerr.CauseHandleHandshake)
	defer cancel()
	if s == nil || s.conn == nil {
		return nil, moerr.NewInternalErrorNoCtx("backend connection is unavailable")
	}
	raw := s.conn
	if raw == nil {
		return nil, moerr.NewInternalErrorNoCtx("backend connection is unavailable")
	}
	if cause := operationContextCause(ctx); cause != nil {
		_ = raw.Close()
		return nil, newTimeoutConnectErr(cause)
	}
	joinInterrupt := interruptConnectionOnDone(ctx, raw)
	defer joinInterrupt()

	type result struct {
		resp *frontend.Packet
		err  error
	}
	// Buffered so a worker that finishes after the caller has timed out can
	// still publish its result and exit, rather than blocking forever on send.
	resultC := make(chan result, 1)
	go func() {
		// Step 1, read initial handshake from CN server.
		if err := s.readInitialHandshake(); err != nil {
			resultC <- result{err: err}
			return
		}
		// Step 2, write the handshake response to CN server, which is
		// received from client earlier.
		resp, err := s.writeHandshakeResp(handshakeResp)
		resultC <- result{resp: resp, err: err}
	}()

	select {
	case ret := <-resultC:
		// Join cancellation before accepting success. If cancellation won the
		// race, the transport may already be closed and cannot enter the tunnel.
		joinInterrupt()
		if cause := operationContextCause(ctx); cause != nil {
			_ = raw.Close()
			return nil, newTimeoutConnectErr(errors.Join(ret.err, cause))
		}
		return ret.resp, ret.err
	case <-ctx.Done():
		// A caller may release or reuse handshakeResp as soon as this method
		// returns. Close only the transport first, then join the worker before
		// returning; calling s.Close here would free mysqlProto buffers while
		// the worker may still be using them. net.Conn.Close unblocks both the
		// goetty read and frontend write paths.
		_ = raw.Close()
		<-resultC
		joinInterrupt()
		logutil.Errorf("handshake to cn %s timeout %v, conn ID: %d goId:%d",
			s.cnServer.addr, timeout, s.connID, goid.Get())
		// Return a retryable error with timeout flag set.
		return nil, newTimeoutConnectErr(moerr.AttachCause(ctx, context.Cause(ctx)))
	}
}

func operationContextCause(ctx context.Context) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

// interruptConnectionOnDone makes cancellation a terminal event for a
// phase-owned transport. Client and backend handshake connections are
// sacrificial until their phase succeeds, so closing is both the strongest I/O
// interrupt and avoids a wrapper or deadline check in the steady tunnel hot
// path. The returned join function prevents a late cancellation callback from
// closing a connection after ownership has been handed off.
func interruptConnectionOnDone(ctx context.Context, conn net.Conn) func() {
	if ctx == nil || conn == nil {
		return func() {}
	}
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = conn.Close()
		close(done)
	})
	var once sync.Once
	return func() {
		once.Do(func() {
			if !stop() {
				<-done
			}
		})
	}
}

// ExecStmt implements the ServerConn interface.
func (s *serverConn) ExecStmt(stmt internalStmt, resp chan<- []byte) (bool, error) {
	req := make([]byte, 1, len(stmt.s)+1)
	req[0] = byte(stmt.cmdType)
	req = append(req, []byte(stmt.s)...)
	s.mysqlProto.SetSequenceID(0)
	if err := s.mysqlProto.WritePacket(req); err != nil {
		return false, err
	}
	execOK := true
	for {
		// readPacket makes sure return value is a whole MySQL packet.
		res, err := s.readPacket()
		if err != nil {
			return false, err
		}
		bs := packetToBytes(res)
		if resp != nil {
			sendResp(bs, resp)
		}
		if isEOFPacket(bs) || isOKPacket(bs) {
			break
		}
		if isErrPacket(bs) {
			execOK = false
			break
		}
	}
	return execOK, nil
}

func (s *serverConn) ExecStmtContext(
	ctx context.Context,
	stmt internalStmt,
	resp chan<- []byte,
) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil || s.conn == nil {
		return false, moerr.NewInternalErrorNoCtx("backend connection is unavailable")
	}
	raw := s.conn
	if raw == nil {
		return false, moerr.NewInternalErrorNoCtx("backend connection is unavailable")
	}
	if cause := operationContextCause(ctx); cause != nil {
		_ = raw.Close()
		return false, cause
	}
	joinInterrupt := interruptConnectionOnDone(ctx, raw)
	ok, err := s.ExecStmt(stmt, resp)
	joinInterrupt()
	if cause := operationContextCause(ctx); cause != nil {
		_ = raw.Close()
		return false, errors.Join(err, cause)
	}
	return ok, err
}

// GetCNServer implements the ServerConn interface.
func (s *serverConn) GetCNServer() *CNServer {
	return s.cnServer
}

// SetConnResponse implements the ServerConn interface.
func (s *serverConn) SetConnResponse(r []byte) {
	s.connResp = r
}

// GetConnResponse implements the ServerConn interface.
func (s *serverConn) GetConnResponse() []byte {
	return s.connResp
}

// CreateTime implements the ServerConn interface.
func (s *serverConn) CreateTime() time.Time {
	return s.createTime
}

func (s *serverConn) Quit() error {
	defer func() {
		_ = s.Close()
	}()
	_, err := s.ExecStmt(internalStmt{
		cmdType: cmdQuit,
	}, nil)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

// Close implements the ServerConn interface.
func (s *serverConn) Close() error {
	s.closeOnce.Do(func() {
		if s.mysqlProto != nil {
			tcpConn := s.mysqlProto.GetTcpConnection()
			if tcpConn != nil {
				_ = tcpConn.Close()
			}
			s.mysqlProto.Close()
		} else if s.conn != nil {
			_ = s.conn.Close()
		}
	})
	if s.rebalancer != nil {
		// Un-track the connection.
		s.rebalancer.connManager.disconnect(s.cnServer, s.tun)
	}
	return nil
}

// readPacket reads packet from CN server, usually used in handshake phase.
func (s *serverConn) readPacket() (*frontend.Packet, error) {
	if s == nil || s.mysqlProto == nil || s.mysqlProto.GetTcpConnection() == nil {
		return nil, moerr.NewInternalErrorNoCtx("backend protocol is unavailable")
	}
	payload, err := s.mysqlProto.GetTcpConnection().Read()
	if err != nil {
		return nil, err
	}
	// frontend.Conn advances the expected sequence after each successful read.
	sequenceID := s.mysqlProto.GetTcpConnection().GetSequenceID() - 1
	return &frontend.Packet{
		Length:     int32(len(payload)),
		SequenceID: int8(sequenceID),
		Payload:    payload,
	}, nil
}

// nextServerConnID increases baseConnID by 1 and returns the result.
func nextServerConnID() uint32 {
	return atomic.AddUint32(&serverBaseConnID, 1)
}

// CNServer represents the backend CN server, including salt, tenant, uuid and address.
// When there is a new client connection, a new CNServer will be created.
type CNServer struct {
	// connID is the backend CN server's connection ID, which is global unique
	// and is tracked in connManager.
	connID uint32
	// salt is generated in proxy module and will be sent to backend
	// server when build connection.
	salt []byte
	// reqLabel is the client requests, but not the label which CN server really has.
	reqLabel labelInfo
	// cnLabel is the labels that CN server has.
	cnLabel map[string]metadata.LabelList
	// hash keep the hash in it.
	hash LabelHash
	// uuid of the CN server.
	uuid string
	// addr is the net address of CN server.
	addr string
	// internalConn indicates the connection is from internal network. Default is false,
	internalConn bool
	// clientAddr is the real client address.
	clientAddr string
}

// Connect connects to a backend server and writes the Proxy ExtraInfo preface.
func (s *CNServer) Connect(logger *zap.Logger, timeout time.Duration) (net.Conn, error) {
	return s.ConnectContext(context.Background(), logger, timeout)
}

func (s *CNServer) ConnectContext(
	parent context.Context,
	_ *zap.Logger,
	timeout time.Duration,
) (net.Conn, error) {
	if parent == nil {
		parent = context.Background()
	}
	ctx := parent
	cancel := func() {}
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(parent, timeout)
	}
	defer cancel()

	network, address, err := parseBackendAddress(s.addr)
	if err != nil {
		return nil, newConnectErr(err)
	}
	raw, err := (&net.Dialer{}).DialContext(ctx, network, address)
	if err != nil {
		logutil.Errorf("failed to connect to cn server, timeout: %v, conn ID: %d, cn: %s, goId: %d, error: %v",
			timeout, s.connID, s.addr, goid.Get(), err)
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return nil, newTimeoutConnectErr(err)
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil, newTimeoutConnectErr(err)
		}
		return nil, newConnectErr(err)
	}
	owned := true
	defer func() {
		if owned {
			_ = raw.Close()
		}
	}()
	joinInterrupt := interruptConnectionOnDone(ctx, raw)
	defer joinInterrupt()
	if len(s.salt) != 20 {
		return nil, moerr.NewInternalErrorNoCtx("salt is empty")
	}
	info := pb.ExtraInfo{
		Salt:         s.salt,
		InternalConn: s.internalConn,
		ConnectionID: s.connID,
		Label:        s.reqLabel.allLabels(),
		ClientAddr:   s.clientAddr,
	}
	data, err := info.Encode()
	if err != nil {
		return nil, err
	}
	// When build connection with backend server, proxy send its salt, request
	// labels and other information to the backend server.
	if err := writeAll(raw, data); err != nil {
		joinInterrupt()
		if cause := operationContextCause(ctx); cause != nil {
			return nil, newTimeoutConnectErr(errors.Join(err, cause))
		}
		return nil, err
	}
	joinInterrupt()
	if cause := operationContextCause(ctx); cause != nil {
		return nil, newTimeoutConnectErr(cause)
	}
	owned = false
	return raw, nil
}

func writeAll(conn net.Conn, data []byte) error {
	for len(data) > 0 {
		n, err := conn.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func parseBackendAddress(address string) (string, string, error) {
	if !strings.Contains(address, "//") {
		return "tcp4", address, nil
	}
	u, err := url.Parse(address)
	if err != nil {
		return "", "", err
	}
	if strings.EqualFold(u.Scheme, "unix") {
		return u.Scheme, u.Path, nil
	}
	return u.Scheme, u.Host, nil
}
