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
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/petermattis/goid"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
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
	c.labelInfo.Tenant = Tenant(tenant.GetTenant())
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
	// prevAddr is empty if it is the first time to build connection with
	// a cn server; otherwise, it is the address of the previous cn node
	// when it is transferring connection and the handshake phase is ignored.
	BuildConnWithServer(prevAddr string) (ServerConn, error)
	// HandleEvent handles event that comes from tunnel data flow.
	HandleEvent(ctx context.Context, e IEvent, resp chan<- []byte) error
	// Close closes the client connection.
	Close() error
}

type migration struct {
	setVarStmtMap map[string]struct{}
	setVarStmts   []string
}

// clientConn is the connection between proxy and client.
type clientConn struct {
	ctx context.Context
	sid string
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
	// tlsConfig is the config of TLS.
	tlsConfig *tls.Config
	// tlsConnectTimeout is the TLS connect timeout value.
	tlsConnectTimeout time.Duration
	// ipNetList is the list of ip net, which is parsed from CIDRs.
	ipNetList []*net.IPNet
	// queryClient is used to send query request to CN servers.
	queryClient qclient.QueryClient
	// testHelper is used for testing.
	testHelper struct {
		connectToBackend func() (ServerConn, error)
	}
	migration migration
	// sc is the server connection bond with the client conn.
	sc ServerConn
	// connCache is the cache of the connections.
	connCache ConnCache
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
	qc qclient.QueryClient,
	connCache ConnCache,
) (ClientConn, error) {
	var originIP net.IP
	var port int
	host, portStr, err := net.SplitHostPort(conn.RemoteAddress())
	if err == nil {
		originIP = net.ParseIP(host)
		port, _ = strconv.Atoi(portStr)
	}
	c := &clientConn{
		ctx:            ctx,
		sid:            cfg.UUID,
		counterSet:     cs,
		conn:           conn,
		haKeeperClient: haKeeperClient,
		moCluster:      mc,
		router:         router,
		tun:            tun,
		clientInfo: clientInfo{
			originIP:   originIP,
			originPort: uint16(port),
		},
		ipNetList: ipNetList,
		// set the connection timeout value.
		tlsConnectTimeout: cfg.TLSConnectTimeout.Duration,
		queryClient:       qc,
		connCache:         connCache,
	}
	c.connID, err = c.genConnID()
	if err != nil {
		return nil, err
	}
	c.log = logger.With(zap.Uint32("ConnID", c.connID))
	fp := config.FrontendParameters{
		EnableTls: cfg.TLSEnabled,
	}
	fp.SetDefaultValues()
	pu := config.NewParameterUnit(&fp, nil, nil, nil)
	frontend.InitServerLevelVars(cfg.UUID)
	frontend.SetSessionAlloc(cfg.UUID, frontend.NewSessionAllocator(pu))
	ios, err := frontend.NewIOSession(c.RawConn(), pu, cfg.UUID)
	if err != nil {
		return nil, err
	}
	c.mysqlProto = frontend.NewMysqlClientProtocol(cfg.UUID, c.connID, ios, 0, &fp)
	if cfg.TLSEnabled {
		tlsConfig, err := frontend.ConstructTLSConfig(
			ctx, cfg.TLSCAFile, cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return nil, err
		}
		c.tlsConfig = tlsConfig
	}
	c.migration.setVarStmtMap = make(map[string]struct{})
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
func (c *clientConn) BuildConnWithServer(prevAddr string) (ServerConn, error) {
	if prevAddr == "" {
		// Step 1, proxy write initial handshake to client.
		if err := c.writeInitialHandshake(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, err
			}
			c.log.Error("failed to write Handshake packet", zap.Error(err))
			return nil, err
		}
		// Step 2, client send handshake response, which is auth request,
		// to proxy.
		if err := c.handleHandshakeResp(); err != nil {
			// This connection may come from heartbeat of LB, and receive EOF error
			// from it. Just return error and do not log it.
			if errors.Is(err, io.EOF) {
				return nil, err
			}
			c.log.Error("failed to handle Handshake response", zap.Error(err))
			return nil, err
		}
	}
	// Step 3, proxy connects to a CN server to build connection.
	conn, err := c.connectToBackend(prevAddr)
	if err != nil {
		c.log.Error("failed to connect to backend", zap.Error(err))
		return nil, err
	}
	// bind the server connection to the client connection.
	c.sc = conn

	return conn, nil
}

// HandleEvent implements the ClientConn interface.
func (c *clientConn) HandleEvent(ctx context.Context, e IEvent, resp chan<- []byte) error {
	switch ev := e.(type) {
	case *killEvent:
		return c.handleKill(ev, resp)
	case *setVarEvent:
		return c.handleSetVar(ev)
	case *quitEvent:
		// Notify/finish the event immediately.
		ev.notify()
		// Then handle the quit event async.
		go func() {
			if err := c.handleQuitEvent(ctx); err != nil {
				c.log.Error("failed to exec quit cmd", zap.Error(err))
			}
		}()
		return nil
	case *upgradeEvent:
		return c.handleUpgradeEvent(ev, resp)
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

// handleKill handles the kill event.
func (c *clientConn) handleKill(e *killEvent, resp chan<- []byte) error {
	cn, err := c.router.SelectByConnID(e.connID)
	if err != nil {
		// If no server found, means that the query has been terminated.
		if errors.Is(err, noCNServerErr) {
			sendResp(makeOKPacket(8), resp)
			return nil
		}
		c.log.Error("failed to select CN server", zap.Error(err))
		c.sendErr(err, resp)
		return err
	}
	// Before connect to backend server, update the salt.
	cn.salt = c.mysqlProto.GetSalt()

	// And also update the connection ID.
	cid, err := c.genConnID()
	if err != nil {
		c.log.Error("failed to generate conn ID for kill query", zap.Error(err))
		c.sendErr(err, resp)
		return err
	}
	cn.connID = cid

	return c.connAndExec(cn, e.stmt, resp)
}

// handleSetVar handles the set variable event.
func (c *clientConn) handleSetVar(e *setVarEvent) error {
	defer e.notify()
	_, ok := c.migration.setVarStmtMap[e.stmt]
	if ok {
		for i := 0; i < len(c.migration.setVarStmts); i++ {
			if c.migration.setVarStmts[i] == e.stmt {
				c.migration.setVarStmts = append(c.migration.setVarStmts[:i], c.migration.setVarStmts[i+1:]...)
				i--
			}
		}
	} else {
		c.migration.setVarStmtMap[e.stmt] = struct{}{}
	}
	c.migration.setVarStmts = append(c.migration.setVarStmts, e.stmt)
	return nil
}

func (c *clientConn) handleQuitEvent(ctx context.Context) error {
	// Get server->client pipe and set it to pause.
	_, scp := c.tun.getPipes()
	if err := scp.pause(ctx); err != nil {
		if err := c.sc.Quit(); err != nil {
			c.log.Error("failed to quit from cn server", zap.Error(err))
		}
		return err
	}
	// After the server->client pipe is paused, push the
	// connection to cache.
	if !c.connCache.Push(c.clientInfo.hash, c.sc) {
		if err := c.sc.Quit(); err != nil {
			c.log.Error("failed to quit from cn server", zap.Error(err))
		}
	}
	return nil
}

func (c *clientConn) handleUpgradeEvent(e *upgradeEvent, resp chan<- []byte) error {
	defer e.notify()

	if !c.clientInfo.isSuperTenant() {
		err := moerr.NewInternalErrorNoCtx("do not have privilege to execute the statement")
		c.log.Error("failed to exec upgrade event", zap.Error(err))
		c.sendErr(err, resp)
		return err
	}

	servers, err := c.router.AllServers(c.sid)
	if err != nil {
		c.log.Error("failed to select CN server", zap.Error(err))
		c.sendErr(err, resp)
		return err
	}

	for _, cn := range servers {
		// Before connect to backend server, update the salt.
		cn.salt = c.mysqlProto.GetSalt()

		// And also update the connection ID.
		cid, err := c.genConnID()
		if err != nil {
			c.log.Error("failed to generate conn ID for upgrade query", zap.Error(err))
			c.sendErr(err, resp)
			return err
		}
		cn.connID = cid

		// In the loop, do not pass the resp, because it only receives response once.
		// It everything is ok, send ok response at last out of the loop.
		if err := c.connAndExec(cn, e.stmt, nil); err != nil {
			c.log.Error("failed to execute upgrade query", zap.Error(err))
			c.sendErr(err, resp)
			return err
		}
	}
	if resp != nil {
		sendResp(makeOKPacket(8), resp)
	}
	return nil
}

// Close implements the ClientConn interface.
func (c *clientConn) Close() error {
	if c.mysqlProto != nil {
		tcpConn := c.mysqlProto.GetTcpConnection()
		if tcpConn != nil {
			if err := tcpConn.Close(); err != nil {
				c.log.Error("failed to close tcp connection", zap.Error(err))
			}
		}
		c.mysqlProto.Close()
	}
	return nil
}

// connectToBackend connect to the real CN server.
func (c *clientConn) connectToBackend(prevAdd string) (ServerConn, error) {
	// Testing path.
	if c.testHelper.connectToBackend != nil {
		return c.testHelper.connectToBackend()
	}

	if c.router == nil {
		v2.ProxyConnectCommonFailCounter.Inc()
		return nil, moerr.NewInternalErrorNoCtx("no router available")
	}

	var sc ServerConn
	// If connCache is enabled, try to get connection from the cache.
	if c.connCache != nil {
		sc = c.connCache.Pop(c.clientInfo.hash, c.connID, c.mysqlProto.GetSalt(), c.mysqlProto.GetAuthResponse())
		if sc != nil {
			// get the response from the cn server.
			re := sc.GetConnResponse()
			if err := c.sendPacketToClient(re, sc); err != nil {
				return nil, err
			}
			v2.ProxyConnectSuccessCounter.Inc()
			return sc, nil
		}
	}

	badCNServers := make(map[string]struct{})
	if prevAdd != "" {
		badCNServers[prevAdd] = struct{}{}
	}
	filterFn := func(str string) bool {
		if _, ok := badCNServers[str]; ok {
			return true
		}
		return false
	}

	var err error
	var cn *CNServer
	var r []byte
	for {
		// Select the best CN server from backend.
		//
		// NB: The selected CNServer must have label hash in it.
		cn, err = c.router.Route(c.ctx, c.sid, c.clientInfo, filterFn)
		if err != nil {
			v2.ProxyConnectRouteFailCounter.Inc()
			c.log.Error("route failed", zap.Error(err))
			return nil, err
		}
		// We have to set connection ID after cn is returned.
		cn.connID = c.connID

		// Set the salt value of cn server.
		cn.salt = c.mysqlProto.GetSalt()

		// Update the internal connection.
		cn.internalConn = containIP(c.ipNetList, c.clientInfo.originIP)
		cn.clientAddr = fmt.Sprintf("%s:%d", c.clientInfo.originIP.String(), c.clientInfo.originPort)

		// After select a CN server, we try to connect to it. If connect
		// fails, and it is a retryable error, we reselect another CN server.
		sc, r, err = c.router.Connect(cn, c.handshakePack, c.tun)
		if err != nil {
			if isRetryableErr(err) {
				v2.ProxyConnectRetryCounter.Inc()
				badCNServers[cn.addr] = struct{}{}
				c.log.Warn("failed to connect to CN server, will retry",
					zap.String("current server uuid", cn.uuid),
					zap.String("current server address", cn.addr),
					zap.Any("bad backend servers", badCNServers),
					zap.String("client->proxy",
						fmt.Sprintf("%s -> %s", c.RawConn().RemoteAddr(),
							c.RawConn().LocalAddr())),
					zap.Error(err),
				)
				continue
			} else {
				v2.ProxyConnectCommonFailCounter.Inc()
				c.log.Error("failed to connect to CN server, cannot retry", zap.Error(err))
				return nil, err
			}
		}

		if prevAdd == "" {
			if len(r) < 5 {
				c.log.Error("the response from cn server is not correct",
					zap.Int("length", len(r)))
				if sc != nil {
					closeErr := sc.Close()
					if closeErr != nil {
						c.log.Error("failed to close server connection", zap.Error(closeErr))
					}
				}
			}

			// set the response from the cn server.
			sc.SetConnResponse(r[4:])

			// whatever the response is, we always send it to client.
			if err := c.sendPacketToClient(r[4:], sc); err != nil {
				return nil, err
			}
		} else {
			// The connection has been transferred to a new server, but migration fails,
			// but we don't return error, which will cause unknown issue.
			if err := c.migrateConn(prevAdd, sc); err != nil {
				closeErr := sc.Close()
				if closeErr != nil {
					c.log.Error("failed to close server connection", zap.Error(closeErr))
				}
				c.log.Error("failed to migrate connection to cn, will retry",
					zap.Uint32("conn ID", c.connID),
					zap.String("current uuid", cn.uuid),
					zap.String("current addr", cn.addr),
					zap.Any("bad backend servers", badCNServers),
					zap.Int64("goId", goid.Get()),
					zap.Error(err),
				)
				badCNServers[cn.addr] = struct{}{}
				continue
			}
		}

		// connection to cn server successfully.
		break
	}

	if !isOKPacket(r) {
		c.log.Error("response is not OK", zap.Any("packet", err))
		// If we do not close here, there will be a lot of unused connections
		// in connManager.
		if sc != nil {
			if closeErr := sc.Close(); closeErr != nil {
				c.log.Error("failed to close server connection", zap.Error(closeErr))
			}
		}
		v2.ProxyConnectCommonFailCounter.Inc()
		return nil, withCode(moerr.NewInternalErrorNoCtx("access error"),
			codeAuthFailed)
	}
	v2.ProxyConnectSuccessCounter.Inc()
	return sc, nil
}

func (c *clientConn) sendPacketToClient(r []byte, sc ServerConn) error {
	// r is the packet received from CN server, send r to client.
	if err := c.mysqlProto.WritePacket(r); err != nil {
		c.log.Error("failed to write packet to client", zap.Error(err))
		v2.ProxyConnectCommonFailCounter.Inc()
		closeErr := sc.Close()
		if closeErr != nil {
			c.log.Error("failed to close server connection", zap.Error(closeErr))
		}
		return err
	}
	return nil
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
			c.clientInfo.originPort = proxyAddr.SourcePort
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
	ctx, cancel := context.WithTimeoutCause(c.ctx, time.Second*3, moerr.CauseGenConnID)
	defer cancel()
	// Use the same key with frontend module to make sure the connection ID
	// is unique globally.
	connID, err := c.haKeeperClient.AllocateIDByKey(ctx, frontend.ConnIDAllocKey)
	if err != nil {
		return 0, moerr.AttachCause(ctx, err)
	}
	// Convert uint64 to uint32 to adapt MySQL protocol.
	return uint32(connID), nil
}
