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
	"strings"
	"sync/atomic"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"go.uber.org/zap"
)

// clientBaseConnID is the base connection ID for client.
var clientBaseConnID uint32 = 1000

// accountInfo contains username and tenant. It is parsed from
// user login information.
type accountInfo struct {
	tenant   Tenant
	username string
}

// parse parses the account information from whole username.
func (a *accountInfo) parse(full string) error {
	var delimiter byte = ':'
	if strings.IndexByte(full, '#') >= 0 {
		delimiter = '#'
	}
	var tenant, username string
	pos := strings.IndexByte(full, delimiter)
	if pos >= 0 {
		tenant = strings.TrimSpace(full[:pos])
		username = strings.TrimSpace(full[pos+1:])
	} else {
		username = strings.TrimSpace(full[:])
	}
	if len(username) == 0 {
		return moerr.NewInternalErrorNoCtx("invalid username '%s'", full)
	}
	a.tenant = Tenant(tenant)
	a.username = username
	return nil
}

// ClientConn is the connection to the client.
type ClientConn interface {
	// ConnID returns the connection ID.
	ConnID() uint32
	// GetSalt returns the salt value of this connection.
	GetSalt() []byte
	// GetHandshakePack returns the handshake response packet
	// which is received from client.
	GetHandshakePack() *frontend.Packet
	// RawConn return the raw connection.
	RawConn() net.Conn
	// GetTenant returns the tenant which this connection belongs to.
	GetTenant() Tenant
	// SendErrToClient sends access error to MySQL client.
	SendErrToClient(errMsg string)
	// BuildConnWithServer selects a CN server and connects to it, then
	// returns the connection. If sendToClient is true, means that the
	// packet received from CN server should be sent to client because
	// it is the first time to build connection and login. And if it is
	// false, means that the packet received from CN server should NOT
	// be sent to client because we are transferring CN server connection,
	// and it is not the first time to build connection and login has been
	// finished already.
	// If handshake is false, ignore the handshake phase.
	BuildConnWithServer(handshake bool) (ServerConn, error)
	// HandleEvent handles event that comes from tunnel data flow.
	HandleEvent(ctx context.Context, e IEvent, resp chan<- []byte) error
	// Close closes the client connection.
	Close() error
}

// clientConn is the connection between proxy and client.
type clientConn struct {
	ctx context.Context
	log *log.MOLogger
	// counterSet counts the events in proxy.
	counterSet *counterSet
	// conn is the raw TCP connection between proxy and client.
	conn goetty.IOSession
	// mysqlProto is mainly used to build handshake.
	mysqlProto *frontend.MysqlProtocolImpl
	// handshakePack is a cached info, used in connection migration.
	// When connection is transferred, we use it to rebuild handshake.
	handshakePack *frontend.Packet
	// connID records the connection ID.
	connID uint32
	// account is parsed from login information.
	account accountInfo
	// labelInfo is the information of labels.
	labelInfo labelInfo
	// moCluster is the CN server cache, which used to filter CN servers
	// by CN labels.
	moCluster clusterservice.MOCluster
	// Router is used to select and connect to a best CN server.
	router Router
	// tun is the tunnel which this client connection belongs to.
	tun *tunnel
	// originIP is the original IP of client, which is used in whitelist.
	originIP net.IP
	// setVarStmts keeps all set user variable statements. When connection
	// is transferred, set all these variables first.
	setVarStmts []string
	// testHelper is used for testing.
	testHelper struct {
		connectToBackend func() (ServerConn, error)
	}
}

var _ ClientConn = (*clientConn)(nil)

// newClientConn creates a new client connection.
func newClientConn(
	ctx context.Context,
	logger *log.MOLogger,
	cs *counterSet,
	conn goetty.IOSession,
	mc clusterservice.MOCluster,
	router Router,
	tun *tunnel,
) (ClientConn, error) {
	host, _, err := net.SplitHostPort(conn.RemoteAddress())
	if err != nil {
		return nil, err
	}
	c := &clientConn{
		ctx:        ctx,
		log:        logger,
		counterSet: cs,
		conn:       conn,
		connID:     nextClientConnID(),
		moCluster:  mc,
		router:     router,
		tun:        tun,
		originIP:   net.ParseIP(host),
	}
	fp := config.FrontendParameters{}
	fp.SetDefaultValues()
	c.mysqlProto = frontend.NewMysqlClientProtocol(c.connID, c.conn, 0, &fp)
	return c, nil
}

// ConnID implements the ClientConn interface.
func (c *clientConn) ConnID() uint32 {
	return c.connID
}

// GetSalt implements the ClientConn interface.
func (c *clientConn) GetSalt() []byte {
	return c.mysqlProto.GetSalt()
}

// GetHandshakePack implements the ClientConn interface.
func (c *clientConn) GetHandshakePack() *frontend.Packet {
	return c.handshakePack
}

// RawConn implements the ClientConn interface.
func (c *clientConn) RawConn() net.Conn {
	if c != nil {
		return c.conn.RawConn()
	}
	return nil
}

// GetTenant implements the ClientConn interface.
func (c *clientConn) GetTenant() Tenant {
	if c != nil {
		return c.account.tenant
	}
	return EmptyTenant
}

// SendErrToClient implements the ClientConn interface.
func (c *clientConn) SendErrToClient(errMsg string) {
	accErr := moerr.MysqlErrorMsgRefer[moerr.ER_ACCESS_DENIED_ERROR]
	p := c.mysqlProto.MakeErrPayload(accErr.ErrorCode, accErr.SqlStates[0], errMsg)
	if err := c.mysqlProto.WritePacket(p); err != nil {
		c.log.Error("failed to send access error to client", zap.Error(err))
	}
}

// BuildConnWithServer implements the ClientConn interface.
func (c *clientConn) BuildConnWithServer(handshake bool) (ServerConn, error) {
	if handshake {
		// Step 1, proxy write initial handshake to client.
		if err := c.writeInitialHandshake(); err != nil {
			c.log.Error("failed to write Handshake packet", zap.Error(err))
			return nil, err
		}
		// Step 2, client send handshake response, which is auth request,
		// to proxy.
		if err := c.handleHandshakeResp(); err != nil {
			c.log.Error("failed to handle Handshake response", zap.Error(err))
			return nil, err
		}
	}
	// Step 3, proxy connects to a CN server to build connection.
	conn, err := c.connectToBackend(handshake)
	if err != nil {
		c.log.Error("failed to connect to backend", zap.Error(err))
		return nil, err
	}
	return conn, nil
}

// HandleEvent implements the ClientConn interface.
func (c *clientConn) HandleEvent(ctx context.Context, e IEvent, resp chan<- []byte) error {
	switch ev := e.(type) {
	case *killQueryEvent:
		return c.handleKillQuery(ev, resp)
	case *setVarEvent:
		return c.handleSetVar(ev)
	default:
	}
	return nil
}

// handleKillQuery handles the kill query event.
func (c *clientConn) handleKillQuery(e *killQueryEvent, resp chan<- []byte) error {
	sendErr := func(errMsg string) {
		fail := moerr.MysqlErrorMsgRefer[moerr.ER_ACCESS_DENIED_ERROR]
		payload := c.mysqlProto.MakeErrPayload(
			fail.ErrorCode, fail.SqlStates[0], errMsg)
		r := &frontend.Packet{
			Length:     0,
			SequenceID: 1,
			Payload:    payload,
		}
		sendResp(packetToBytes(r), resp)
	}

	cn, err := c.router.SelectByConnID(e.connID)
	if err != nil {
		c.log.Error("failed to select CN server", zap.Error(err))
		sendErr(err.Error())
		return err
	}
	// Before connect to backend server, update the salt.
	cn.salt = c.mysqlProto.GetSalt()

	sc, r, err := c.router.Connect(cn, c.handshakePack, c.tun)
	if err != nil {
		c.log.Error("failed to connect to backend server", zap.Error(err))
		sendErr(err.Error())
		return err
	}
	defer func() { _ = sc.Close() }()

	if !isOKPacket(r) {
		c.log.Error("failed to connect to cn to handle kill query event",
			zap.String("query", e.stmt), zap.String("error", string(r)))
		sendResp(r, resp)
		return moerr.NewInternalErrorNoCtx("access error")
	}

	err = sc.ExecStmt(e.stmt, resp)
	if err != nil {
		c.log.Error("failed to send query %s to server",
			zap.String("query", e.stmt), zap.Error(err))
	}
	return nil
}

// handleSetVar handles the set variable event.
func (c *clientConn) handleSetVar(e *setVarEvent) error {
	c.setVarStmts = append(c.setVarStmts, e.stmt)
	return nil
}

// Close implements the ClientConn interface.
func (c *clientConn) Close() error {
	return nil
}

// connectToBackend connect to the real CN server.
func (c *clientConn) connectToBackend(sendToClient bool) (ServerConn, error) {
	// Testing path.
	if c.testHelper.connectToBackend != nil {
		return c.testHelper.connectToBackend()
	}

	if c.router == nil {
		return nil, moerr.NewInternalErrorNoCtx("no router available")
	}

	// Select the best CN server from backend.
	//
	// NB: The selected CNServer must have label hash in it.
	cn, err := c.router.SelectByLabel(c.labelInfo)
	if err != nil {
		return nil, err
	}
	// Set the salt value of cn server.
	cn.salt = c.mysqlProto.GetSalt()
	sc, r, err := c.router.Connect(cn, c.handshakePack, c.tun)
	if err != nil {
		return nil, err
	}
	if sendToClient {
		// r is the packet received from CN server, send r to client.
		if err := c.mysqlProto.WritePacket(r[4:]); err != nil {
			return nil, err
		}
	}
	if !isOKPacket(r) {
		return nil, withCode(moerr.NewInternalErrorNoCtx("access error"),
			codeAuthFailed)
	}

	// Set the label session variable.
	if err := sc.ExecStmt(c.labelInfo.genSetVarStmt(), nil); err != nil {
		return nil, err
	}
	// Set the use defined variables, including session variables and user variables.
	for _, stmt := range c.setVarStmts {
		if err := sc.ExecStmt(stmt, nil); err != nil {
			return nil, err
		}
	}
	return sc, nil
}

// readPacket reads MySQL packets from clients. It is mainly used in
// handshake phase.
func (c *clientConn) readPacket() (*frontend.Packet, error) {
	msg, err := c.conn.Read(goetty.ReadOptions{})
	if err != nil {
		return nil, err
	}
	if proxyAddr, ok := msg.(*ProxyAddr); ok {
		if proxyAddr.SourceAddress != nil {
			c.originIP = proxyAddr.SourceAddress
		}
		return c.readPacket()
	}
	packet, ok := msg.(*frontend.Packet)
	if !ok {
		return nil, moerr.NewInternalError(c.ctx, "message is not a Packet")
	}
	return packet, nil
}

// nextClientConnID increases baseConnID by 1 and returns the result.
func nextClientConnID() uint32 {
	return atomic.AddUint32(&clientBaseConnID, 1)
}
