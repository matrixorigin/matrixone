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
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"go.uber.org/zap"
)

// clientBaseConnID is the base connection ID for client.
var clientBaseConnID uint32 = 1000

// parse parses the account information from whole username.
func (a *clientInfo) parse(full string) error {
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
	a.labelInfo.Tenant = Tenant(tenant)
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
	// clientInfo is the information of the client.
	clientInfo clientInfo
	// haKeeperClient is the client of HAKeeper.
	haKeeperClient logservice.ClusterHAKeeperClient
	// moCluster is the CN server cache, which used to filter CN servers
	// by CN labels.
	moCluster clusterservice.MOCluster
	// Router is used to select and connect to a best CN server.
	router Router
	// tun is the tunnel which this client connection belongs to.
	tun *tunnel
	// setVarStmts keeps all set user variable statements. When connection
	// is transferred, set all these variables first.
	setVarStmts []string
	// tlsConfig is the config of TLS.
	tlsConfig *tls.Config
	// testHelper is used for testing.
	testHelper struct {
		connectToBackend func() (ServerConn, error)
	}
}

var _ ClientConn = (*clientConn)(nil)

// newClientConn creates a new client connection.
func newClientConn(
	ctx context.Context,
	cfg *Config,
	logger *log.MOLogger,
	cs *counterSet,
	conn goetty.IOSession,
	haKeeperClient logservice.ClusterHAKeeperClient,
	mc clusterservice.MOCluster,
	router Router,
	tun *tunnel,
) (ClientConn, error) {
	var originIP net.IP
	host, _, err := net.SplitHostPort(conn.RemoteAddress())
	if err == nil {
		originIP = net.ParseIP(host)
	}
	c := &clientConn{
		ctx:            ctx,
		log:            logger,
		counterSet:     cs,
		conn:           conn,
		haKeeperClient: haKeeperClient,
		moCluster:      mc,
		router:         router,
		tun:            tun,
		clientInfo: clientInfo{
			originIP: originIP,
		},
	}
	c.connID, err = c.genConnID()
	if err != nil {
		return nil, err
	}
	fp := config.FrontendParameters{
		EnableTls: cfg.TLSEnabled,
	}
	fp.SetDefaultValues()
	c.mysqlProto = frontend.NewMysqlClientProtocol(c.connID, c.conn, 0, &fp)
	if cfg.TLSEnabled {
		tlsConfig, err := frontend.ConstructTLSConfig(
			ctx, cfg.TLSCAFile, cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return nil, err
		}
		c.tlsConfig = tlsConfig
	}
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
		return c.clientInfo.Tenant
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
	case *suspendAccountEvent:
		return c.handleSuspendAccount(ev)
	case *dropAccountEvent:
		return c.handleDropAccount(ev)
	default:
	}
	return nil
}

func (c *clientConn) sendErr(err error, resp chan<- []byte) {
	var errCode uint16
	var sqlState, errMsg string
	switch myErr := err.(type) {
	case *moerr.Error:
		if myErr.MySQLCode() != moerr.ER_UNKNOWN_ERROR {
			errCode = myErr.MySQLCode()
		} else {
			errCode = myErr.ErrorCode()
		}
		errMsg = myErr.Error()
		sqlState = myErr.SqlState()
	default:
		fail := moerr.MysqlErrorMsgRefer[moerr.ER_ACCESS_DENIED_ERROR]
		errCode = fail.ErrorCode
		sqlState = fail.SqlStates[0]
		errMsg = err.Error()
	}
	payload := c.mysqlProto.MakeErrPayload(
		errCode, sqlState, errMsg)
	r := &frontend.Packet{
		Length:     0,
		SequenceID: 1,
		Payload:    payload,
	}
	sendResp(packetToBytes(r), resp)
}

func (c *clientConn) connAndExec(cn *CNServer, stmt string, resp chan<- []byte) error {
	sc, r, err := c.router.Connect(cn, c.handshakePack, c.tun)
	if err != nil {
		c.log.Error("failed to connect to backend server", zap.Error(err))
		if resp != nil {
			c.sendErr(err, resp)
		}
		return err
	}
	defer func() { _ = sc.Close() }()

	if !isOKPacket(r) {
		c.log.Error("failed to connect to cn to handle event",
			zap.String("query", stmt), zap.String("error", string(r)))
		if resp != nil {
			sendResp(r, resp)
		}
		return moerr.NewInternalErrorNoCtx("access error")
	}

	ok, err := sc.ExecStmt(stmt, resp)
	if err != nil {
		c.log.Error("failed to send query to server",
			zap.String("query", stmt), zap.Error(err))
		return err
	}
	if !ok {
		return moerr.NewInternalErrorNoCtx("exec error")
	}
	return nil
}

// handleKillQuery handles the kill query event.
func (c *clientConn) handleKillQuery(e *killQueryEvent, resp chan<- []byte) error {
	cn, err := c.router.SelectByConnID(e.connID)
	if err != nil {
		c.log.Error("failed to select CN server", zap.Error(err))
		c.sendErr(err, resp)
		return err
	}
	// Before connect to backend server, update the salt.
	cn.salt = c.mysqlProto.GetSalt()

	return c.connAndExec(cn, fmt.Sprintf("KILL QUERY %d", cn.backendConnID), resp)
}

// handleSetVar handles the set variable event.
func (c *clientConn) handleSetVar(e *setVarEvent) error {
	c.setVarStmts = append(c.setVarStmts, e.stmt)
	return nil
}

// handleSuspendAccountEvent handles the suspend account event.
func (c *clientConn) handleSuspendAccount(e *suspendAccountEvent) error {
	// handle kill connection.
	cns, err := c.router.SelectByTenant(e.account)
	if err != nil {
		return err
	}
	if len(cns) == 0 {
		return nil
	}
	for _, cn := range cns {
		// Before connect to backend server, update the salt.
		cn.salt = c.mysqlProto.GetSalt()

		go func(s *CNServer) {
			query := fmt.Sprintf("kill connection %d", s.backendConnID)
			// No client to receive the result, so pass nil as the third
			// parameter to ignore the result.
			if err := c.connAndExec(s, query, nil); err != nil {
				c.log.Error("failed to send query to server",
					zap.String("query", query), zap.Error(err))
				return
			}
			c.log.Info("kill connection on server succeeded",
				zap.String("query", query), zap.String("server", s.addr))
		}(cn)
	}
	return nil
}

// handleDropAccountEvent handles the drop account event.
func (c *clientConn) handleDropAccount(e *dropAccountEvent) error {
	se := &suspendAccountEvent{
		baseEvent: e.baseEvent,
		stmt:      e.stmt,
		account:   e.account,
	}
	return c.handleSuspendAccount(se)
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
	cn, err := c.router.Route(c.ctx, c.clientInfo)
	if err != nil {
		return nil, err
	}
	// We have to set proxy connection ID after cn is returned.
	cn.proxyConnID = c.connID

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
	if len(c.clientInfo.allLabels()) > 0 {
		if _, err := sc.ExecStmt(c.clientInfo.genSetVarStmt(), nil); err != nil {
			return nil, err
		}
	}
	// Set the use defined variables, including session variables and user variables.
	for _, stmt := range c.setVarStmts {
		if _, err := sc.ExecStmt(stmt, nil); err != nil {
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
			c.clientInfo.originIP = proxyAddr.SourceAddress
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

// genConnID is used to generate globally unique connection ID.
func (c *clientConn) genConnID() (uint32, error) {
	if c.haKeeperClient == nil {
		return nextClientConnID(), nil
	}
	ctx, cancel := context.WithTimeout(c.ctx, time.Second*3)
	defer cancel()
	// Use the same key with frontend module to make sure the connection ID
	// is unique globally.
	connID, err := c.haKeeperClient.AllocateIDByKey(ctx, frontend.ConnIDAllocKey)
	if err != nil {
		return 0, err
	}
	// Convert uint64 to uint32 to adapt MySQL protocol.
	return uint32(connID), nil
}
