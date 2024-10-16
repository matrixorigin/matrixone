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
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty/v2"
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
	conn goetty.IOSession
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

// newServerConn creates a connection to CN server.
func newServerConn(cn *CNServer, tun *tunnel, r *rebalancer, timeout time.Duration) (ServerConn, error) {
	var logger *zap.Logger
	if r != nil && r.logger != nil {
		logger = r.logger.RawLogger()
	}
	c, err := cn.Connect(logger, timeout)
	if err != nil {
		return nil, err
	}
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
	frontend.SetSessionAlloc(cn.uuid, frontend.NewSessionAllocator(pu))
	ios, err := frontend.NewIOSession(c.RawConn(), pu, cn.uuid)
	if err != nil {
		return nil, err
	}
	s.mysqlProto = frontend.NewMysqlClientProtocol(cn.uuid, s.connID, ios, 0, &fp)
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
	if s != nil {
		if s.cnServer != nil {
			return &wrappedConn{
				Conn: s.conn.RawConn(),
				closeFn: func() {
					s.rebalancer.connManager.disconnect(s.cnServer, s.tun)
				},
			}
		}
		return s.conn.RawConn()
	}
	return nil
}

// HandleHandshake implements the ServerConn interface.
func (s *serverConn) HandleHandshake(
	handshakeResp *frontend.Packet, timeout time.Duration,
) (*frontend.Packet, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var r *frontend.Packet
	var err error
	ch := make(chan struct{})
	go func() {
		// Step 1, read initial handshake from CN server.
		if err = s.readInitialHandshake(); err != nil {
			return
		}
		// Step 2, write the handshake response to CN server, which is
		// received from client earlier.
		r, err = s.writeHandshakeResp(handshakeResp)
		ch <- struct{}{}
	}()

	select {
	case <-ch:
		return r, err
	case <-ctx.Done():
		logutil.Errorf("handshake to cn %s timeout %v, conn ID: %d",
			s.cnServer.addr, timeout, s.connID)
		// Return a retryable error.
		return nil, newConnectErr(context.DeadlineExceeded)
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
		// Disconnect from the connection manager, also, close the
		// raw TCP connection.
		_ = s.RawConn().Close()
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
	msg, err := s.conn.Read(goetty.ReadOptions{})
	if err != nil {
		return nil, err
	}
	packet, ok := msg.(*frontend.Packet)
	if !ok {
		return nil, moerr.NewInternalErrorNoCtx("message is not a Packet")
	}
	return packet, nil
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

// Connect connects to backend server and returns IOSession.
func (s *CNServer) Connect(logger *zap.Logger, timeout time.Duration) (goetty.IOSession, error) {
	c := goetty.NewIOSession(
		goetty.WithSessionCodec(frontend.NewSqlCodec()),
		goetty.WithSessionLogger(logger),
	)
	err := c.Connect(s.addr, timeout)
	if err != nil {
		logutil.Errorf("failed to connect to cn server, timeout: %v, conn ID: %d, cn: %s, error: %v",
			timeout, s.connID, s.addr, err)
		return nil, newConnectErr(err)
	}
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
	if err := c.Write(data, goetty.WriteOptions{Flush: true}); err != nil {
		return nil, err
	}
	return c, nil
}
