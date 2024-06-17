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
	"github.com/fagongzi/goetty/v2/buf"
	"github.com/fagongzi/goetty/v2/codec"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty/v2"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
)

// RelationName counter for the new connection
var initConnectionID uint32 = 1000

// ConnIDAllocKey is used get connection ID from HAKeeper.
var ConnIDAllocKey = "____server_conn_id"

// MOServer MatrixOne Server
type MOServer struct {
	addr        string
	uaddr       string
	app         goetty.NetApplication
	rm          *RoutineManager
	readTimeout time.Duration

	mu      sync.RWMutex
	wg      sync.WaitGroup
	running bool

	logger *zap.Logger
	codec  codec.Codec

	listeners []net.Listener
}

type IOSession struct {
	id                    uint64
	state                 int32
	conn                  net.Conn
	localAddr, remoteAddr string
	in                    *buf.ByteBuf
	out                   *buf.ByteBuf
	disableConnect        bool
	logger                *zap.Logger
	readCopyBuf           []byte
	writeCopyBuf          []byte
	codec                 codec.Codec
	allocator             buf.Allocator
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
	err := mo.startAcceptLoop()
	if err != nil {
		return err
	}
	setMoServerStarted(true)
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

	mo.logger.Debug("application listener closed")
	mo.wg.Wait()

	// now no new connection will added, close all active sessions
	for _, m := range s.sessions {
		m.Lock()
		for k, rs := range m.sessions {
			delete(m.sessions, k)
			if err := rs.Disconnect(); err != nil {
				s.logger.Error("session closed failed",
					zap.Error(err))
			}
		}
		m.Unlock()
	}
	s.logger.Debug("application stopped")
	return nil
}

func (mo *MOServer) startAcceptLoop() error {
	mo.logger.Debug("mo server accept loop started")
	defer func() {
		mo.logger.Debug("mo server accept loop stopped")
	}()

	listenFunc := func(listener net.Listener) {
		defer mo.wg.Done()

		var tempDelay time.Duration
		for {
			conn, err := listener.Accept()
			if err != nil {
				if !mo.isStarted() {
					return
				}

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

			rs := NewIOSession()
			if !s.addSession(rs) {
				if err := rs.Close(); err != nil {
					s.logger.Error("close session failed", zap.Error(err))
				}
				return
			}

			go func() {
				defer func() {
					if s.deleteSession(rs) {
						if err := rs.Close(); err != nil {
							s.logger.Error("close session failed", zap.Error(err))
						}
					}
				}()
				if err := handle(rs); err != nil {
					s.logger.Error("handle session failed", zap.Error(err))
				}
			}()
		}
	}

	for _, listener := range mo.listeners {
		mo.wg.Add(1)
		go listenFunc(listener)
	}
}

func (mo *MOServer) isStarted() bool {
	mo.mu.RLock()
	defer mo.mu.RUnlock()
	return mo.running
}

func nextConnectionID() uint32 {
	return atomic.AddUint32(&initConnectionID, 1)
}

var globalRtMgr atomic.Value
var globalPu atomic.Value
var globalAicm atomic.Value
var moServerStarted atomic.Bool

func setGlobalRtMgr(rtMgr *RoutineManager) {
	globalRtMgr.Store(rtMgr)
}

func getGlobalRtMgr() *RoutineManager {
	return globalRtMgr.Load().(*RoutineManager)
}

func setGlobalPu(pu *config.ParameterUnit) {
	globalPu.Store(pu)
}

func getGlobalPu() *config.ParameterUnit {
	return globalPu.Load().(*config.ParameterUnit)
}

func setGlobalAicm(aicm *defines.AutoIncrCacheManager) {
	globalAicm.Store(aicm)
}

func getGlobalAic() *defines.AutoIncrCacheManager {
	if globalAicm.Load() != nil {
		return globalAicm.Load().(*defines.AutoIncrCacheManager)
	}
	return nil
}

func MoServerIsStarted() bool {
	return moServerStarted.Load()
}

func setMoServerStarted(b bool) {
	moServerStarted.Store(b)
}
func NewIOSession(conn net.Conn) *IOSession {
	moio := &IOSession{
		id:             ,
		state:          0,
		conn:           nil,
		localAddr:      "",
		remoteAddr:     "",
		in:             nil,
		out:            nil,
		disableConnect: false,
		logger:         nil,
		readCopyBuf:    nil,
		writeCopyBuf:   nil,
		codec:          nil,
		allocator:      nil,
	}
	bio.adjust()
	bio.Ref()

	bio.readCopyBuf = make([]byte, bio.options.readCopyBufSize)
	bio.writeCopyBuf = make([]byte, bio.options.writeCopyBufSize)
	if bio.conn != nil {
		bio.initConn()
		bio.disableConnect = true
	}
	if bio.options.aware != nil {
		bio.options.aware.Created(bio)
	}
	return bio
}

func NewMOServer(
	ctx context.Context,
	addr string,
	pu *config.ParameterUnit,
	aicm *defines.AutoIncrCacheManager,
	baseService BaseService,
) *MOServer {
	setGlobalPu(pu)
	setGlobalAicm(aicm)
	codec := NewSqlCodec()
	rm, err := NewRoutineManager(ctx)
	if err != nil {
		logutil.Panicf("start server failed with %+v", err)
	}
	setGlobalRtMgr(rm)
	rm.setBaseService(baseService)
	if baseService != nil {
		rm.setSessionMgr(baseService.SessionMgr())
	}
	// TODO asyncFlushBatch
	addresses := []string{addr}
	unixAddr := pu.SV.GetUnixSocketAddress()
	if unixAddr != "" {
		addresses = append(addresses, "unix://"+unixAddr)
	}
	mo := &MOServer{
		addr:        addr,
		uaddr:       pu.SV.UnixSocketAddress,
		rm:          rm,
		readTimeout: pu.SV.SessionTimeout.Duration,
	}
	app, err := goetty.NewApplicationWithListenAddress(
		addresses,
		rm.Handler,
		goetty.WithAppLogger(logutil.GetGlobalLogger()),
		goetty.WithAppHandleSessionFunc(mo.handleMessage),
		goetty.WithAppSessionOptions(
			goetty.WithSessionCodec(codec),
			goetty.WithSessionLogger(logutil.GetGlobalLogger()),
			goetty.WithSessionRWBUfferSize(DefaultRpcBufferSize, DefaultRpcBufferSize),
			goetty.WithSessionAllocator(NewSessionAllocator(pu))),
		goetty.WithAppSessionAware(rm),
		//when the readTimeout expires the goetty will close the tcp connection.
		goetty.WithReadTimeout(pu.SV.SessionTimeout.Duration))
	if err != nil {
		logutil.Panicf("start server failed with %+v", err)
	}
	mo.app = app
	return mo
}

// handleMessage receives the message from the client and executes it
func (mo *MOServer) handleMessage(rs goetty.IOSession) error {
	received := uint64(0)
	option := goetty.ReadOptions{Timeout: mo.readTimeout}
	for {
		msg, err := rs.Read(option)
		if err != nil {
			if err == io.EOF {
				return nil
			}

			logutil.Error("session read failed",
				zap.Error(err))
			return err
		}

		received++

		err = mo.rm.Handler(rs, msg, received)
		if err != nil {
			if skipClientQuit(err.Error()) {
				return nil
			} else {
				logutil.Error("session handle failed, close this session", zap.Error(err))
			}
			return err
		}
	}
}
