// Copyright 2021 Matrix Origin
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

package frontend

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

// RelationName counter for the new connection
var initConnectionID uint32 = 1000

// ConnIDAllocKey is used get connection ID from HAKeeper.
var ConnIDAllocKey = "____server_conn_id"

// MOServer MatrixOne Server
type MOServer struct {
	addr        string
	uaddr       string
	rm          *RoutineManager
	readTimeout time.Duration
	handler     func(*Conn, []byte) error
	mu          sync.RWMutex
	wg          sync.WaitGroup
	running     bool

	pu        *config.ParameterUnit
	listeners []net.Listener
	service   string
}

// Server interface is for mock MOServer
type Server interface {
	GetRoutineManager() *RoutineManager
	Start() error
	Stop() error
}

// BaseService is an interface which indicates that the instance is
// the base CN service and should implement the following methods.
type BaseService interface {
	// ID returns the ID of the service.
	ID() string
	// SQLAddress returns the SQL listen address of the service.
	SQLAddress() string
	// SessionMgr returns the session manager instance of the service.
	SessionMgr() *queryservice.SessionManager
	// CheckTenantUpgrade used to upgrade tenant metadata if the tenant is old version.
	CheckTenantUpgrade(ctx context.Context, tenantID int64) error
	// GetFinalVersion Get mo final version, which is based on the current code
	GetFinalVersion() string
	// UpgradeTenant used to upgrade tenant
	UpgradeTenant(ctx context.Context, tenantName string, retryCount uint32, isALLAccount bool) error
}

func (mo *MOServer) GetRoutineManager() *RoutineManager {
	return mo.rm
}

func (mo *MOServer) Start() error {
	logutil.Infof("Server Listening on : %s ", mo.addr)
	mo.running = true
	mo.startListener()
	setMoServerStarted(mo.service, true)
	return nil
}

func (mo *MOServer) Stop() error {
	mo.mu.Lock()
	if !mo.running {
		mo.mu.Unlock()
		return nil
	}
	mo.running = false
	mo.mu.Unlock()

	var errors []error
	for _, listener := range mo.listeners {
		if err := listener.Close(); err != nil {
			errors = append(errors, err)
		}
	}
	if len(errors) > 0 {
		return errors[0]
	}

	logutil.Debug("application listener closed")
	mo.wg.Wait()

	mo.rm.cancelCtx()
	mo.rm.killNetConns()

	logutil.Debug("application stopped")
	return nil
}

func (mo *MOServer) IsRunning() bool {
	mo.mu.RLock()
	defer mo.mu.RUnlock()
	return mo.running
}

func (mo *MOServer) startListener() {
	logutil.Debug("mo server accept loop started")
	defer func() {
		logutil.Debug("mo server accept loop stopped")
	}()

	for _, listener := range mo.listeners {
		mo.wg.Add(1)
		go mo.startAccept(mo.rm.ctx, listener)
	}
}

func (mo *MOServer) startAccept(ctx context.Context, listener net.Listener) {
	defer mo.wg.Done()

	var tempDelay time.Duration
	quit := false
	for {
		select {
		case <-ctx.Done():
			quit = true
		default:

		}
		if quit {
			break
		}
		conn, err := listener.Accept()
		if err != nil {

			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				time.Sleep(tempDelay)
				continue
			}
			return
		}
		tempDelay = 0

		go mo.handleConn(ctx, conn)
	}
}
func (mo *MOServer) handleConn(ctx context.Context, conn net.Conn) {
	var rs *Conn
	var err error
	defer func() {
		if rs != nil {
			err = rs.Close()
			if err != nil {
				logutil.Error("Handle conn error", zap.Error(err))
				return
			}
		}
	}()

	rs, err = NewIOSession(conn, mo.pu, mo.service)
	if err != nil {
		logutil.Error("NewIOSession error", zap.Error(err))
		return
	}
	err = mo.rm.Created(rs)
	if err != nil {
		logutil.Error("Create routine error", zap.Error(err))
		return
	}
	err = mo.handshake(rs)
	if err != nil {
		logutil.Error("HandShake error", zap.Error(err))
		return
	}
	mo.handleLoop(ctx, rs)
}

func (mo *MOServer) handleLoop(ctx context.Context, rs *Conn) {
	if err := mo.handleMessage(ctx, rs); err != nil {
		logutil.Error("handle session failed", zap.Error(err))
	}
}

func (mo *MOServer) handshake(rs *Conn) error {
	var err error
	var isTlsHeader bool
	rm := mo.rm
	ctx, span := trace.Start(rm.getCtx(), "RoutineManager.Handler",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End()

	tempCtx, tempCancel := context.WithTimeout(ctx, getPu(mo.service).SV.SessionTimeout.Duration)
	defer tempCancel()

	routine := rm.getRoutine(rs)
	if routine == nil {
		logutil.Error(
			"Failed to handshake with server, routine does not exist...",
			zap.Error(err))
		return err
	}

	protocol := routine.getProtocol()

	ses := routine.getSession()
	ts := ses.timestampMap

	ts[TSEstablishStart] = time.Now()
	ses.Debugf(tempCtx, "HANDLE HANDSHAKE")

	err = protocol.WriteHandshake()

	if err != nil {
		logutil.Error(
			"Failed to handshake with server, quitting routine...",
			zap.Error(err))
		routine.killConnection(true)
		return err
	}

	ses.Debugf(rm.getCtx(), "have sent handshake packet to connection %s", rs.RemoteAddress())

	for !protocol.GetBool(ESTABLISHED) {
		var payload []byte
		payload, err = rs.Read()
		if err != nil {
			return err
		}

		if protocol.GetU32(CAPABILITY)&CLIENT_SSL != 0 && !protocol.GetBool(TLS_ESTABLISHED) {
			ses.Debugf(tempCtx, "setup ssl")
			isTlsHeader, err = protocol.HandleHandshake(tempCtx, payload)
			if err != nil {
				ses.Error(tempCtx,
					"An error occurred",
					zap.Error(err))
				return err
			}
			if isTlsHeader {
				ts[TSUpgradeTLSStart] = time.Now()
				ses.Debugf(tempCtx, "upgrade to TLS")
				// do upgradeTls
				tlsConn := tls.Server(rs.RawConn(), rm.getTlsConfig())
				ses.Debugf(tempCtx, "get TLS conn ok")
				tlsCtx, cancelFun := context.WithTimeout(tempCtx, 20*time.Second)
				if err = tlsConn.HandshakeContext(tlsCtx); err != nil {
					ses.Error(tempCtx,
						"Error occurred before cancel()",
						zap.Error(err))
					cancelFun()
					ses.Error(tempCtx,
						"Error occurred after cancel()",
						zap.Error(err))
					return err
				}
				cancelFun()
				ses.Debugf(tempCtx, "TLS handshake ok")
				rs.UseConn(tlsConn)
				ses.Debugf(tempCtx, "TLS handshake finished")

				// tls upgradeOk
				protocol.SetBool(TLS_ESTABLISHED, true)
				ts[TSUpgradeTLSEnd] = time.Now()
				v2.UpgradeTLSDurationHistogram.Observe(ts[TSUpgradeTLSEnd].Sub(ts[TSUpgradeTLSStart]).Seconds())
			} else {
				// client don't ask server to upgrade TLS
				if err := protocol.Authenticate(tempCtx); err != nil {
					return err
				}
				protocol.SetBool(TLS_ESTABLISHED, true)
				protocol.SetBool(ESTABLISHED, true)
			}
		} else {
			ses.Debugf(tempCtx, "handleHandshake")
			_, err = protocol.HandleHandshake(tempCtx, payload)
			if err != nil {
				ses.Error(tempCtx,
					"Error occurred",
					zap.Error(err))
				return err
			}
			if err = protocol.Authenticate(tempCtx); err != nil {
				return err
			}
			protocol.SetBool(ESTABLISHED, true)
		}
		ts[TSEstablishEnd] = time.Now()
		v2.EstablishDurationHistogram.Observe(ts[TSEstablishEnd].Sub(ts[TSEstablishStart]).Seconds())
		ses.Info(ctx, fmt.Sprintf("mo accept connection, time cost of Created: %s, Establish: %s, UpgradeTLS: %s, Authenticate: %s, SendErrPacket: %s, SendOKPacket: %s, CheckTenant: %s, CheckUser: %s, CheckRole: %s, CheckDbName: %s, InitGlobalSysVar: %s",
			ts[TSCreatedEnd].Sub(ts[TSCreatedStart]).String(),
			ts[TSEstablishEnd].Sub(ts[TSEstablishStart]).String(),
			ts[TSUpgradeTLSEnd].Sub(ts[TSUpgradeTLSStart]).String(),
			ts[TSAuthenticateEnd].Sub(ts[TSAuthenticateStart]).String(),
			ts[TSSendErrPacketEnd].Sub(ts[TSSendErrPacketStart]).String(),
			ts[TSSendOKPacketEnd].Sub(ts[TSSendOKPacketStart]).String(),
			ts[TSCheckTenantEnd].Sub(ts[TSCheckTenantStart]).String(),
			ts[TSCheckUserEnd].Sub(ts[TSCheckUserStart]).String(),
			ts[TSCheckRoleEnd].Sub(ts[TSCheckRoleStart]).String(),
			ts[TSCheckDbNameEnd].Sub(ts[TSCheckDbNameStart]).String(),
			ts[TSInitGlobalSysVarEnd].Sub(ts[TSInitGlobalSysVarStart]).String()))

		dbName := protocol.GetStr(DBNAME)
		if dbName != "" {
			ses.SetDatabaseName(dbName)
		}
		rm.sessionManager.AddSession(ses)
	}

	return nil
}

func nextConnectionID() uint32 {
	return atomic.AddUint32(&initConnectionID, 1)
}

var serverVarsMap sync.Map

func init() {
	InitServerLevelVars("")
}

func getServerLevelVars(service string) *ServerLevelVariables {
	//always there
	ret, _ := serverVarsMap.Load(service)
	return ret.(*ServerLevelVariables)
}

func InitServerLevelVars(service string) {
	serverVarsMap.LoadOrStore(service, &ServerLevelVariables{})
	getServerLevelVars(service)
}

func getSessionAlloc(service string) Allocator {
	return getServerLevelVars(service).sessionAlloc.Load().(Allocator)
}

func setSessionAlloc(service string, s Allocator) {
	getServerLevelVars(service).sessionAlloc.Store(s)
}

func SetSessionAlloc(service string, s Allocator) {
	setSessionAlloc(service, s)
}

func setRtMgr(service string, rtMgr *RoutineManager) {
	getServerLevelVars(service).RtMgr.Store(rtMgr)
}

func getRtMgr(service string) *RoutineManager {
	v := getServerLevelVars(service).RtMgr.Load()
	if v != nil {
		return v.(*RoutineManager)
	}
	return nil
}

func setPu(service string, pu *config.ParameterUnit) {
	getServerLevelVars(service).Pu.Store(pu)
}

func getPu(service string) *config.ParameterUnit {
	return getServerLevelVars(service).Pu.Load().(*config.ParameterUnit)
}

func setAicm(service string, aicm *defines.AutoIncrCacheManager) {
	getServerLevelVars(service).Aicm.Store(aicm)
}

func getAicm(service string) *defines.AutoIncrCacheManager {
	ret := getServerLevelVars(service).Aicm
	if ret.Load() != nil {
		return ret.Load().(*defines.AutoIncrCacheManager)
	}
	return nil
}

func MoServerIsStarted(service string) bool {
	return getServerLevelVars(service).moServerStarted.Load()
}

func setMoServerStarted(service string, b bool) {
	getServerLevelVars(service).moServerStarted.Store(b)
}

func NewMOServer(
	ctx context.Context,
	addr string,
	pu *config.ParameterUnit,
	aicm *defines.AutoIncrCacheManager,
	baseService BaseService,
) *MOServer {
	service := ""
	if baseService != nil {
		service = baseService.ID()
	}
	InitServerLevelVars(service)
	setPu(service, pu)
	setAicm(service, aicm)
	setSessionAlloc(service, NewSessionAllocator(pu))
	rm, err := NewRoutineManager(ctx, service)
	if err != nil {
		logutil.Panicf("start server failed with %+v", err)
	}
	setRtMgr(service, rm)
	rm.setBaseService(baseService)
	if baseService != nil {
		rm.setSessionMgr(baseService.SessionMgr())
	}
	// TODO asyncFlushBatch
	unixAddr := pu.SV.GetUnixSocketAddress()
	mo := &MOServer{
		addr:        addr,
		uaddr:       pu.SV.UnixSocketAddress,
		rm:          rm,
		readTimeout: pu.SV.SessionTimeout.Duration,
		pu:          pu,
		handler:     rm.Handler,
		service:     service,
	}
	listenerTcp, err := net.Listen("tcp", addr)
	if err != nil {
		logutil.Panicf("start server failed with %+v", err)
	}
	mo.listeners = append(mo.listeners, listenerTcp)
	if unixAddr != "" {
		listenerUnix, err := net.Listen("unix", unixAddr)
		if err != nil {
			logutil.Panicf("start server failed with %+v", err)
		}
		mo.listeners = append(mo.listeners, listenerUnix)
	}
	return mo
}

// handleMessage receives the message from the client and executes it
func (mo *MOServer) handleMessage(ctx context.Context, rs *Conn) error {
	quit := false
	for {
		select {
		case <-ctx.Done():
			quit = true
		default:
		}
		if quit {
			break
		}
		err := mo.handleRequest(rs)
		if err != nil {
			if err == io.EOF {
				return nil
			}

			logutil.Error("session read failed",
				zap.Error(err))
			return err
		}
	}
	return nil
}

func (mo *MOServer) handleRequest(rs *Conn) error {
	var msg []byte
	var err error
	if !mo.IsRunning() {
		return io.EOF
	}

	msg, err = rs.Read()
	if err != nil {
		if err == io.EOF {
			return err
		}

		logutil.Error("session read failed",
			zap.Error(err))
		return err
	}

	err = mo.rm.Handler(rs, msg)
	if err != nil {
		if skipClientQuit(err.Error()) {
			return nil
		} else {
			logutil.Error("session handle failed, close this session", zap.Error(err))
		}
		return err
	}
	return nil
}
