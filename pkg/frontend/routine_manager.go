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
	"crypto/x509"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"go.uber.org/zap"
)

type RoutineManager struct {
	mu      sync.RWMutex
	ctx     context.Context
	clients map[*Conn]*Routine
	// routinesByID keeps the routines by connection ID.
	routinesByConnID map[uint32]*Routine
	tlsConfig        *tls.Config
	accountRoutine   *AccountRoutineManager
	baseService      BaseService
	sessionManager   *queryservice.SessionManager
	// reportSystemStatusTime is the time when report system status last time.
	reportSystemStatusTime atomic.Pointer[time.Time]
}

type AccountRoutineManager struct {
	ctx               context.Context
	killQueueMu       sync.RWMutex
	killIdQueue       map[int64]KillRecord
	accountRoutineMu  sync.RWMutex
	accountId2Routine map[int64]map[*Routine]uint64
}

type KillRecord struct {
	killTime time.Time
	version  uint64
}

func NewKillRecord(killtime time.Time, version uint64) KillRecord {
	return KillRecord{
		killTime: killtime,
		version:  version,
	}
}

func (ar *AccountRoutineManager) recordRoutine(tenantID int64, rt *Routine, version uint64) {
	if tenantID == sysAccountID || rt == nil {
		return
	}

	ar.accountRoutineMu.Lock()
	defer ar.accountRoutineMu.Unlock()
	logutil.Infof("[init account] set account id %d, connection id %d to account routine map", tenantID, rt.getConnectionID())
	if _, ok := ar.accountId2Routine[tenantID]; !ok {
		ar.accountId2Routine[tenantID] = make(map[*Routine]uint64)
	}
	ar.accountId2Routine[tenantID][rt] = version
}

func (ar *AccountRoutineManager) deleteRoutine(tenantID int64, rt *Routine) {
	if tenantID == sysAccountID || rt == nil {
		return
	}

	ar.accountRoutineMu.Lock()
	defer ar.accountRoutineMu.Unlock()
	_, ok := ar.accountId2Routine[tenantID]
	if ok {
		delete(ar.accountId2Routine[tenantID], rt)
	}
	if len(ar.accountId2Routine[tenantID]) == 0 {
		delete(ar.accountId2Routine, tenantID)
	}
}

func (ar *AccountRoutineManager) EnKillQueue(tenantID int64, version uint64) {
	if tenantID == sysAccountID {
		return
	}

	KillRecord := NewKillRecord(time.Now(), version)
	ar.killQueueMu.Lock()
	defer ar.killQueueMu.Unlock()
	logutil.Infof("[set suspend] set account id %d, version %d to kill queue at time %v, ", tenantID, version, KillRecord.killTime)
	ar.killIdQueue[tenantID] = KillRecord

}

func (ar *AccountRoutineManager) AlterRoutineStatue(tenantID int64, status string) {
	if tenantID == sysAccountID {
		return
	}

	ar.accountRoutineMu.Lock()
	defer ar.accountRoutineMu.Unlock()
	if rts, ok := ar.accountId2Routine[tenantID]; ok {
		for rt := range rts {
			if status == "restricted" {
				logutil.Infof("[set restricted] alter routine, set account id %d, connection id %d restricted", tenantID, rt.getConnectionID())
				rt.setResricted(true)
			} else {
				rt.setResricted(false)
			}
		}
	}
}

func (ar *AccountRoutineManager) deepCopyKillQueue() map[int64]KillRecord {
	ar.killQueueMu.RLock()
	defer ar.killQueueMu.RUnlock()

	tempKillQueue := make(map[int64]KillRecord, len(ar.killIdQueue))
	for account, record := range ar.killIdQueue {
		tempKillQueue[account] = record
	}
	return tempKillQueue
}

func (ar *AccountRoutineManager) deepCopyRoutineMap() map[int64]map[*Routine]uint64 {
	ar.accountRoutineMu.RLock()
	defer ar.accountRoutineMu.RUnlock()

	tempRoutineMap := make(map[int64]map[*Routine]uint64, len(ar.accountId2Routine))
	for account, rountine := range ar.accountId2Routine {
		tempRountines := make(map[*Routine]uint64, len(rountine))
		for rt, version := range rountine {
			tempRountines[rt] = version
		}
		tempRoutineMap[account] = tempRountines
	}
	return tempRoutineMap
}

func (rm *RoutineManager) getCtx() context.Context {
	return rm.ctx
}

func (rm *RoutineManager) setRoutine(rs *Conn, id uint32, r *Routine) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.clients[rs] = r
	rm.routinesByConnID[id] = r
}

func (rm *RoutineManager) getRoutine(rs *Conn) *Routine {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.clients[rs]
}

func (rm *RoutineManager) getRoutineByConnID(id uint32) *Routine {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	r, ok := rm.routinesByConnID[id]
	if ok {
		return r
	}
	return nil
}

func (rm *RoutineManager) deleteRoutine(rs *Conn) *Routine {
	var rt *Routine
	var ok bool
	rm.mu.Lock()
	defer rm.mu.Unlock()
	if rt, ok = rm.clients[rs]; ok {
		delete(rm.clients, rs)
	}
	if rt != nil {
		connID := rt.getConnectionID()
		if _, ok = rm.routinesByConnID[connID]; ok {
			delete(rm.routinesByConnID, connID)
		}
	}
	return rt
}

func (rm *RoutineManager) getTlsConfig() *tls.Config {
	return rm.tlsConfig
}

func (rm *RoutineManager) getConnID() (uint32, error) {
	// Only works in unit test.
	if getGlobalPu().HAKeeperClient == nil {
		return nextConnectionID(), nil
	}
	ctx, cancel := context.WithTimeout(rm.ctx, time.Second*2)
	defer cancel()
	connID, err := getGlobalPu().HAKeeperClient.AllocateIDByKey(ctx, ConnIDAllocKey)
	if err != nil {
		return 0, err
	}
	// Convert uint64 to uint32 to adapt MySQL protocol.
	return uint32(connID), nil
}

func (rm *RoutineManager) setBaseService(baseService BaseService) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.baseService = baseService
}

func (rm *RoutineManager) setSessionMgr(sessionMgr *queryservice.SessionManager) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.sessionManager = sessionMgr
}

func (rm *RoutineManager) GetAccountRoutineManager() *AccountRoutineManager {
	return rm.accountRoutine
}

func (rm *RoutineManager) Created(rs *Conn) error {
	logutil.Debugf("get the connection from %s", rs.RemoteAddress())
	createdStart := time.Now()
	connID, err := rm.getConnID()
	if err != nil {
		logutil.Errorf("failed to get connection ID from HAKeeper: %v", err)
		return err
	}
	sid := ""
	if rm.baseService != nil {
		sid = rm.baseService.ID()
	}
	pro := NewMysqlClientProtocol(sid, connID, rs, int(getGlobalPu().SV.MaxBytesInOutbufToFlush), getGlobalPu().SV)
	routine := NewRoutine(rm.getCtx(), pro, getGlobalPu().SV)
	v2.CreatedRoutineCounter.Inc()

	cancelCtx := routine.getCancelRoutineCtx()
	if rm.baseService != nil {
		cancelCtx = context.WithValue(cancelCtx, defines.NodeIDKey{}, rm.baseService.ID())
	}

	// XXX MPOOL pass in a nil mpool.
	// XXX MPOOL can choose to use a Mid sized mpool, if, we know
	// this mpool will be deleted.  Maybe in the following Closed method.
	ses := NewSession(cancelCtx, sid, routine.getProtocol(), nil)
	ses.SetFromRealUser(true)
	ses.setRoutineManager(rm)
	ses.setRoutine(routine)
	ses.clientAddr = pro.Peer()

	ses.timestampMap[TSCreatedStart] = createdStart
	defer func() {
		ses.timestampMap[TSCreatedEnd] = time.Now()
		v2.CreatedDurationHistogram.Observe(ses.timestampMap[TSCreatedEnd].Sub(ses.timestampMap[TSCreatedStart]).Seconds())
	}()

	routine.setSession(ses)
	pro.SetSession(ses)

	ses.Debugf(cancelCtx, "have done some preparation for the connection %s", rs.RemoteAddress())

	// With proxy module enabled, we try to update salt value and label info from proxy.
	if getGlobalPu().SV.ProxyEnabled {
		pro.receiveExtraInfo(rs)
	}
	rm.setRoutine(rs, pro.connectionID, routine)
	ses.UpdateDebugString()
	return nil
}

/*
When the io is closed, the Closed will be called.
*/
func (rm *RoutineManager) Closed(rs *Conn) {
	rt := rm.deleteRoutine(rs)
	if rt == nil {
		return
	}

	defer func() {
		v2.CloseRoutineCounter.Inc()
	}()

	ses := rt.getSession()
	if ses != nil {
		ses.Debugf(rm.getCtx(), "clean resource of the connection %d:%s", rs.ID(), rs.RemoteAddress())
		defer func() {
			ses.Debugf(rm.getCtx(), "resource of the connection %d:%s has been cleaned", rs.ID(), rs.RemoteAddress())
		}()
		rt.decreaseCount(func() {
			account := ses.GetTenantInfo()
			accountName := sysAccountName
			if account != nil {
				accountName = account.GetTenant()
			}
			metric.ConnectionCounter(accountName).Dec()
			rm.accountRoutine.deleteRoutine(int64(account.GetTenantID()), rt)
		})
		rm.sessionManager.RemoveSession(ses)
		ses.Debugf(rm.getCtx(), "the io session was closed.")
	}
	rt.cleanup()
}

/*
kill a connection or query.
if killConnection is true, the query will be canceled first, then the network will be closed.
if killConnection is false, only the query will be canceled. the connection keeps intact.
*/
func (rm *RoutineManager) kill(ctx context.Context, killConnection bool, idThatKill, id uint64, statementId string) error {
	rt := rm.getRoutineByConnID(uint32(id))

	killMyself := idThatKill == id
	if rt != nil {
		ses := rt.getSession()
		if killConnection {
			ses.Infof(ctx, "kill connection %d", id)
			rt.killConnection(killMyself)
			rm.accountRoutine.deleteRoutine(int64(rt.ses.GetTenantInfo().GetTenantID()), rt)
		} else {
			ses.Infof(ctx, "kill query %s on the connection %d", statementId, id)
			rt.killQuery(killMyself, statementId)
		}
	} else {
		return moerr.NewInternalErrorf(ctx, "Unknown connection id %d", id)
	}
	return nil
}

func getConnectionInfo(rs *Conn) string {
	conn := rs.RawConn()
	if conn != nil {
		return fmt.Sprintf("connection from %s to %s", conn.RemoteAddr(), conn.LocalAddr())
	}
	return fmt.Sprintf("connection from %s", rs.RemoteAddress())
}

func (rm *RoutineManager) Handler(rs *Conn, msg []byte) error {
	logutil.Debugf("get request from %d:%s", rs.ID(), rs.RemoteAddress())
	defer func() {
		logutil.Debugf("request from %d:%s has been processed", rs.ID(), rs.RemoteAddress())
	}()
	var err error
	ctx, span := trace.Start(rm.getCtx(), "RoutineManager.Handler",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End()
	routine := rm.getRoutine(rs)
	if routine == nil {
		connectionInfo := getConnectionInfo(rs)
		err = moerr.NewInternalError(ctx, "routine does not exist")
		logutil.Errorf("%s error:%v", connectionInfo, err)
		return err
	}
	routine.updateGoroutineId()
	routine.setInProcessRequest(true)
	defer routine.setInProcessRequest(false)
	payload := msg

	ses := routine.getSession()

	req := ToRequest(payload)
	//handle request
	err = routine.handleRequest(req)
	if err != nil {
		if !skipClientQuit(err.Error()) {
			ses.Error(ctx,
				"Error occurred",
				zap.Error(err))
		}
		return err
	}

	return nil
}

// clientCount returns the count of the clients
func (rm *RoutineManager) clientCount() int {
	var count int
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	count = len(rm.clients)
	return count
}

func (rm *RoutineManager) cleanKillQueue() {
	ar := rm.accountRoutine
	ar.killQueueMu.Lock()
	defer ar.killQueueMu.Unlock()
	for toKillAccount, killRecord := range ar.killIdQueue {
		if time.Since(killRecord.killTime) > time.Duration(getGlobalPu().SV.CleanKillQueueInterval)*time.Minute {
			delete(ar.killIdQueue, toKillAccount)
		}
	}
}

func (rm *RoutineManager) KillRoutineConnections() {
	ar := rm.accountRoutine
	tempKillQueue := ar.deepCopyKillQueue()
	accountId2RoutineMap := ar.deepCopyRoutineMap()

	for account, killRecord := range tempKillQueue {
		if rtMap, ok := accountId2RoutineMap[account]; ok {
			for rt, version := range rtMap {
				if rt != nil && ((version+1)%math.MaxUint64)-1 <= killRecord.version {
					//kill connect of this routine
					if rt.getProtocol() != nil {
						logutil.Infof("[kill connection] do kill connection account id %d, version %d, connection id %d, ", account, killRecord.version, rt.getConnectionID())
						rt.killConnection(false)
					}
					ar.deleteRoutine(account, rt)
				}
			}
		}
	}

	rm.cleanKillQueue()
}

func (rm *RoutineManager) MigrateConnectionTo(ctx context.Context, req *query.MigrateConnToRequest) error {
	routine := rm.getRoutineByConnID(req.ConnID)
	if routine == nil {
		return moerr.NewInternalErrorf(ctx, "cannot get routine to migrate connection %d", req.ConnID)
	}
	return routine.migrateConnectionTo(ctx, req)
}

func (rm *RoutineManager) MigrateConnectionFrom(req *query.MigrateConnFromRequest, resp *query.MigrateConnFromResponse) error {
	routine := rm.getRoutineByConnID(req.ConnID)
	if routine == nil {
		return moerr.NewInternalErrorf(rm.ctx, "cannot get routine to migrate connection %d", req.ConnID)
	}
	return routine.migrateConnectionFrom(resp)
}

func (rm *RoutineManager) ResetSession(req *query.ResetSessionRequest, resp *query.ResetSessionResponse) error {
	routine := rm.getRoutineByConnID(req.ConnID)
	if routine == nil {
		return moerr.NewInternalErrorf(rm.ctx, "cannot get routine to clear session %d", req.ConnID)
	}
	return routine.resetSession(rm.baseService.ID(), resp)
}

func NewRoutineManager(ctx context.Context) (*RoutineManager, error) {
	accountRoutine := &AccountRoutineManager{
		killQueueMu:       sync.RWMutex{},
		accountId2Routine: make(map[int64]map[*Routine]uint64),
		accountRoutineMu:  sync.RWMutex{},
		killIdQueue:       make(map[int64]KillRecord),
		ctx:               ctx,
	}
	rm := &RoutineManager{
		ctx:              ctx,
		clients:          make(map[*Conn]*Routine),
		routinesByConnID: make(map[uint32]*Routine),
		accountRoutine:   accountRoutine,
	}
	if getGlobalPu().SV.EnableTls {
		err := initTlsConfig(rm, getGlobalPu().SV)
		if err != nil {
			return nil, err
		}
	}

	// add kill connect routine
	go func() {
		for {
			select {
			case <-rm.ctx.Done():
				return
			default:
			}
			rm.KillRoutineConnections()
			time.Sleep(time.Duration(time.Duration(getGlobalPu().SV.KillRountinesInterval) * time.Second))
		}
	}()

	return rm, nil
}

func initTlsConfig(rm *RoutineManager, SV *config.FrontendParameters) error {
	if len(SV.TlsCertFile) == 0 || len(SV.TlsKeyFile) == 0 {
		return moerr.NewInternalError(rm.ctx, "init TLS config error : cert file or key file is empty")
	}

	cfg, err := ConstructTLSConfig(rm.ctx, SV.TlsCaFile, SV.TlsCertFile, SV.TlsKeyFile)
	if err != nil {
		return moerr.NewInternalErrorf(rm.ctx, "init TLS config error: %v", err)
	}

	rm.tlsConfig = cfg
	logutil.Info("init TLS config finished")
	return nil
}

// ConstructTLSConfig creates the TLS config.
func ConstructTLSConfig(ctx context.Context, caFile, certFile, keyFile string) (*tls.Config, error) {
	var err error
	var tlsCert tls.Certificate

	tlsCert, err = tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, moerr.NewInternalError(ctx, "construct TLS config error: load x509 failed")
	}

	clientAuthPolicy := tls.NoClientCert
	var certPool *x509.CertPool
	if len(caFile) > 0 {
		var caCert []byte
		caCert, err = os.ReadFile(caFile)
		if err != nil {
			return nil, moerr.NewInternalError(ctx, "construct TLS config error: read TLS ca failed")
		}
		certPool = x509.NewCertPool()
		if certPool.AppendCertsFromPEM(caCert) {
			clientAuthPolicy = tls.VerifyClientCertIfGiven
		}
	}

	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		ClientCAs:    certPool,
		ClientAuth:   clientAuthPolicy,
	}, nil
}
