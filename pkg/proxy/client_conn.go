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
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

// clientBaseConnID is the base connection ID for client.
var clientBaseConnID uint32 = 1000

// parse parses the account information from whole username.
// The whole username parameter is like: tenant1:user1:role1?key1:value1,key2:value2
func (c *clientInfo) parse(full string) error {
	var labelPart string
	labelDelPos := strings.IndexByte(full, '?')
	userPart := full[:]
	if labelDelPos >= 0 {
		userPart = full[:labelDelPos]
		if len(full) > labelDelPos+1 {
			labelPart = full[labelDelPos+1:]
		}
	}
	tenant, err := frontend.GetTenantInfo(context.Background(), userPart)
	if err != nil {
		return err
	}
	c.labelInfo.Tenant = Tenant(tenant.Tenant)
	c.username = tenant.GetUser()

	// For label part.
	if len(labelPart) > 0 {
		labels, err := frontend.ParseLabel(strings.TrimSpace(labelPart))
		if err != nil {
			return err
		}
		c.labelInfo.Labels = labels
	}
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
	SendErrToClient(err error)
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
	// redoStmts keeps all the statements that need to re-execute in the
	// new session after it is transferred. It should contain:
	// 1. set variable stmts
	// 2. prepare stmts
	// 3. use stmts
	redoStmts []internalStmt
	// tlsConfig is the config of TLS.
	tlsConfig *tls.Config
	// ipNetList is the list of ip net, which is parsed from CIDRs.
	ipNetList []*net.IPNet
	// testHelper is used for testing.
	testHelper struct {
		connectToBackend func() (ServerConn, error)
	}
}

// internalStmt is used internally in proxy, which indicates the stmt
// need to execute.
type internalStmt struct {
	cmdType MySQLCmd
	s       string
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
	ipNetList []*net.IPNet,
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
		ipNetList: ipNetList,
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
func (c *clientConn) SendErrToClient(err error) {
	errorCode, sqlState, msg := frontend.RewriteError(err, "")
	p := c.mysqlProto.MakeErrPayload(errorCode, sqlState, msg)
	if err := c.mysqlProto.WritePacket(p); err != nil {
		c.log.Error("failed to send access error to client", zap.Error(err))
	}
}

// BuildConnWithServer implements the ClientConn interface.
func (c *clientConn) BuildConnWithServer(handshake bool) (ServerConn, error) {
	if handshake {
		// Step 1, proxy write initial handshake to client.
		if err := c.writeInitialHandshake(); err != nil {
			c.log.Debug("failed to write Handshake packet", zap.Error(err))
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
	case *prepareEvent:
		return c.handlePrepare(ev)
	case *initDBEvent:
		return c.handleInitDB(ev)
	default:
	}
	return nil
}

func (c *clientConn) sendErr(err error, resp chan<- []byte) {
	errCode, sqlState, errMsg := frontend.RewriteError(err, "")
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

	ok, err := sc.ExecStmt(internalStmt{cmdType: cmdQuery, s: stmt}, resp)
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
	c.redoStmts = append(c.redoStmts, internalStmt{cmdType: cmdQuery, s: e.stmt})
	return nil
}

// handleSetVar handles the prepare event.
func (c *clientConn) handlePrepare(e *prepareEvent) error {
	c.redoStmts = append(c.redoStmts, internalStmt{cmdType: cmdQuery, s: e.stmt})
	return nil
}

// handleUse handles the use event.
func (c *clientConn) handleInitDB(e *initDBEvent) error {
	c.redoStmts = append(c.redoStmts, internalStmt{cmdType: cmdInitDB, s: e.db})
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
		v2.ProxyConnectCommonFailCounter.Inc()
		return nil, moerr.NewInternalErrorNoCtx("no router available")
	}

	badCNServers := make(map[string]struct{})
	filterFn := func(uuid string) bool {
		if _, ok := badCNServers[uuid]; ok {
			return true
		}
		return false
	}

	var err error
	var cn *CNServer
	var sc ServerConn
	var r []byte
	for {
		// Select the best CN server from backend.
		//
		// NB: The selected CNServer must have label hash in it.
		cn, err = c.router.Route(c.ctx, c.clientInfo, filterFn)
		if err != nil {
			v2.ProxyConnectRouteFailCounter.Inc()
			return nil, err
		}
		// We have to set proxy connection ID after cn is returned.
		cn.proxyConnID = c.connID

		// Set the salt value of cn server.
		cn.salt = c.mysqlProto.GetSalt()

		// Update the internal connection.
		cn.internalConn = containIP(c.ipNetList, c.clientInfo.originIP)

		// After select a CN server, we try to connect to it. If connect
		// fails, and it is a retryable error, we reselect another CN server.
		sc, r, err = c.router.Connect(cn, c.handshakePack, c.tun)
		if err != nil {
			if isRetryableErr(err) {
				v2.ProxyConnectRetryCounter.Inc()
				badCNServers[cn.uuid] = struct{}{}
				c.log.Warn("failed to connect to CN server, will retry",
					zap.String("current server uuid", cn.uuid),
					zap.String("current server address", cn.addr),
					zap.Any("bad backend servers", badCNServers))
				continue
			} else {
				v2.ProxyConnectCommonFailCounter.Inc()
				return nil, err
			}
		} else {
			break
		}
	}

	if sendToClient {
		// r is the packet received from CN server, send r to client.
		if err := c.mysqlProto.WritePacket(r[4:]); err != nil {
			v2.ProxyConnectCommonFailCounter.Inc()
			return nil, err
		}
	}
	if !isOKPacket(r) {
		v2.ProxyConnectCommonFailCounter.Inc()
		return nil, withCode(moerr.NewInternalErrorNoCtx("access error"),
			codeAuthFailed)
	}

	// Re-execute the statements.
	for _, stmt := range c.redoStmts {
		if _, err := sc.ExecStmt(stmt, nil); err != nil {
			v2.ProxyConnectCommonFailCounter.Inc()
			return nil, err
		}
	}

	v2.ProxyConnectSuccessCounter.Inc()
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
