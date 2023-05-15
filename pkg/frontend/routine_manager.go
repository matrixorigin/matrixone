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
	"bytes"
	"container/list"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/metric"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

type RoutineManager struct {
	mu             sync.RWMutex
	ctx            context.Context
	clients        map[goetty.IOSession]*Routine
	pu             *config.ParameterUnit
	skipCheckUser  atomic.Bool
	tlsConfig      *tls.Config
	accountRoutine *AccountRoutineManager
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

func (ar *AccountRoutineManager) recordRountine(tenantID int64, rt *Routine, version uint64) {
	if tenantID == sysAccountID || rt != nil {
		return
	}

	ar.accountRoutineMu.Lock()
	defer ar.accountRoutineMu.Unlock()
	if _, ok := ar.accountId2Routine[tenantID]; !ok {
		ar.accountId2Routine[tenantID] = make(map[*Routine]uint64)
	}
	ar.accountId2Routine[tenantID][rt] = version
}

func (ar *AccountRoutineManager) deleteRoutine(tenantID int64, rt *Routine) {
	if tenantID == sysAccountID || rt != nil {
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

func (ar *AccountRoutineManager) enKillQueue(tenantID int64, version uint64) {
	if tenantID == sysAccountID {
		return
	}

	KillRecord := NewKillRecord(time.Now(), version)
	ar.killQueueMu.Lock()
	defer ar.killQueueMu.Unlock()
	ar.killIdQueue[tenantID] = KillRecord

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
		tempRoutineMap[account] = rountine
	}
	return tempRoutineMap
}

func (rm *RoutineManager) SetSkipCheckUser(b bool) {
	rm.skipCheckUser.Store(b)
}

func (rm *RoutineManager) GetSkipCheckUser() bool {
	return rm.skipCheckUser.Load()
}

func (rm *RoutineManager) getParameterUnit() *config.ParameterUnit {
	return rm.pu
}

func (rm *RoutineManager) getCtx() context.Context {
	return rm.ctx
}

func (rm *RoutineManager) setRoutine(rs goetty.IOSession, r *Routine) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.clients[rs] = r
}

func (rm *RoutineManager) getRoutine(rs goetty.IOSession) *Routine {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.clients[rs]
}

func (rm *RoutineManager) getTlsConfig() *tls.Config {
	return rm.tlsConfig
}

func (rm *RoutineManager) getConnID() (uint32, error) {
	// Only works in unit test.
	if rm.pu.HAKeeperClient == nil {
		return nextConnectionID(), nil
	}
	ctx, cancel := context.WithTimeout(rm.ctx, time.Second*2)
	defer cancel()
	connID, err := rm.pu.HAKeeperClient.AllocateIDByKey(ctx, ConnIDAllocKey)
	if err != nil {
		return 0, err
	}
	// Convert uint64 to uint32 to adapt MySQL protocol.
	return uint32(connID), nil
}

func (rm *RoutineManager) Created(rs goetty.IOSession) {
	logutil.Debugf("get the connection from %s", rs.RemoteAddress())
	pu := rm.getParameterUnit()
	connID, err := rm.getConnID()
	if err != nil {
		logutil.Errorf("failed to get connection ID from HAKeeper: %v", err)
		return
	}
	pro := NewMysqlClientProtocol(connID, rs, int(pu.SV.MaxBytesInOutbufToFlush), pu.SV)
	pro.SetSkipCheckUser(rm.GetSkipCheckUser())
	exe := NewMysqlCmdExecutor()
	exe.SetRoutineManager(rm)
	exe.ChooseDoQueryFunc(pu.SV.EnableDoComQueryInProgress)

	routine := NewRoutine(rm.getCtx(), pro, exe, pu.SV, rs)

	// XXX MPOOL pass in a nil mpool.
	// XXX MPOOL can choose to use a Mid sized mpool, if, we know
	// this mpool will be deleted.  Maybe in the following Closed method.
	ses := NewSession(routine.getProtocol(), nil, pu, GSysVariables, true)
	ses.SetRequestContext(routine.getCancelRoutineCtx())
	ses.SetConnectContext(routine.getCancelRoutineCtx())
	ses.SetFromRealUser(true)
	ses.setSkipCheckPrivilege(rm.GetSkipCheckUser())
	ses.setRoutineManager(rm)
	ses.setRoutine(routine)

	routine.setSession(ses)
	pro.SetSession(ses)

	logDebugf(pro.GetDebugString(), "have done some preparation for the connection %s", rs.RemoteAddress())

	// With proxy module enabled, we try to update salt value from proxy.
	// The MySQL protocol is broken a little: when connection is built, read
	// the salt from proxy and update the salt, then go on with the handshake
	// phase.
	if rm.pu.SV.ProxyEnabled {
		pro.tryUpdateSalt(rs)
	}

	hsV10pkt := pro.makeHandshakeV10Payload()
	err = pro.writePackets(hsV10pkt)
	if err != nil {
		logErrorf(pro.GetDebugString(), "failed to handshake with server, quiting routine... %s", err)
		routine.killConnection(true)
		return
	}

	logDebugf(pro.GetDebugString(), "have sent handshake packet to connection %s", rs.RemoteAddress())
	rm.setRoutine(rs, routine)
}

/*
When the io is closed, the Closed will be called.
*/
func (rm *RoutineManager) Closed(rs goetty.IOSession) {
	logutil.Debugf("clean resource of the connection %d:%s", rs.ID(), rs.RemoteAddress())
	defer func() {
		logutil.Debugf("resource of the connection %d:%s has been cleaned", rs.ID(), rs.RemoteAddress())
	}()
	var rt *Routine
	var ok bool

	rm.mu.Lock()
	rt, ok = rm.clients[rs]
	if ok {
		delete(rm.clients, rs)
	}
	rm.mu.Unlock()

	if rt != nil {
		ses := rt.getSession()
		if ses != nil {
			rt.decreaseCount(func() {
				account := ses.GetTenantInfo()
				accountName := sysAccountName
				if account != nil {
					accountName = account.GetTenant()
				}
				metric.ConnectionCounter(accountName).Dec()
				rm.accountRoutine.deleteRoutine(int64(account.GetTenantID()), rt)
			})
			logDebugf(ses.GetDebugString(), "the io session was closed.")
		}
		rt.cleanup()
	}
}

/*
kill a connection or query.
if killConnection is true, the query will be canceled first, then the network will be closed.
if killConnection is false, only the query will be canceled. the connection keeps intact.
*/
func (rm *RoutineManager) kill(ctx context.Context, killConnection bool, idThatKill, id uint64, statementId string) error {
	var rt *Routine = nil
	rm.mu.RLock()
	for _, value := range rm.clients {
		if uint64(value.getConnectionID()) == id {
			rt = value
			break
		}
	}
	rm.mu.RUnlock()

	killMyself := idThatKill == id
	if rt != nil {
		if killConnection {
			logutil.Infof("kill connection %d", id)
			rt.killConnection(killMyself)
			rm.accountRoutine.deleteRoutine(int64(rt.ses.GetTenantInfo().GetTenantID()), rt)
		} else {
			logutil.Infof("kill query %s on the connection %d", statementId, id)
			rt.killQuery(killMyself, statementId)
		}
	} else {
		return moerr.NewInternalError(ctx, "Unknown connection id %d", id)
	}
	return nil
}

func getConnectionInfo(rs goetty.IOSession) string {
	conn := rs.RawConn()
	if conn != nil {
		return fmt.Sprintf("connection from %s to %s", conn.RemoteAddr(), conn.LocalAddr())
	}
	return fmt.Sprintf("connection from %s", rs.RemoteAddress())
}

func (rm *RoutineManager) Handler(rs goetty.IOSession, msg interface{}, received uint64) error {
	logutil.Debugf("get request from %d:%s", rs.ID(), rs.RemoteAddress())
	defer func() {
		logutil.Debugf("request from %d:%s has been processed", rs.ID(), rs.RemoteAddress())
	}()
	var err error
	var isTlsHeader bool
	ctx, span := trace.Start(rm.getCtx(), "RoutineManager.Handler")
	defer span.End()
	connectionInfo := getConnectionInfo(rs)
	routine := rm.getRoutine(rs)
	if routine == nil {
		err = moerr.NewInternalError(ctx, "routine does not exist")
		logutil.Errorf("%s error:%v", connectionInfo, err)
		return err
	}
	routine.setInProcessRequest(true)
	defer routine.setInProcessRequest(false)
	protocol := routine.getProtocol()
	protoInfo := protocol.GetDebugString()
	packet, ok := msg.(*Packet)

	protocol.SetSequenceID(uint8(packet.SequenceID + 1))
	var seq = protocol.GetSequenceId()
	if !ok {
		err = moerr.NewInternalError(ctx, "message is not Packet")
		logErrorf(protoInfo, "error:%v", err)
		return err
	}

	length := packet.Length
	payload := packet.Payload
	for uint32(length) == MaxPayloadSize {
		msg, err = protocol.GetTcpConnection().Read(goetty.ReadOptions{})
		if err != nil {
			logErrorf(protoInfo, "read message failed. error:%s", err)
			return err
		}

		packet, ok = msg.(*Packet)
		if !ok {
			err = moerr.NewInternalError(ctx, "message is not Packet")
			logErrorf(protoInfo, "error:%v", err)
			return err
		}

		protocol.SetSequenceID(uint8(packet.SequenceID + 1))
		seq = protocol.GetSequenceId()
		payload = append(payload, packet.Payload...)
		length = packet.Length
	}

	// finish handshake process
	if !protocol.IsEstablished() {
		logDebugf(protoInfo, "HANDLE HANDSHAKE")

		/*
			di := MakeDebugInfo(payload,80,8)
			logutil.Infof("RP[%v] Payload80[%v]",rs.RemoteAddr(),di)
		*/
		ses := routine.getSession()
		if protocol.GetCapability()&CLIENT_SSL != 0 && !protocol.IsTlsEstablished() {
			logDebugf(protoInfo, "setup ssl")
			isTlsHeader, err = protocol.HandleHandshake(ctx, payload)
			if err != nil {
				logErrorf(protoInfo, "error:%v", err)
				return err
			}
			if isTlsHeader {
				logDebugf(protoInfo, "upgrade to TLS")
				// do upgradeTls
				tlsConn := tls.Server(rs.RawConn(), rm.getTlsConfig())
				logDebugf(protoInfo, "get TLS conn ok")
				newCtx, cancelFun := context.WithTimeout(ctx, 20*time.Second)
				if err = tlsConn.HandshakeContext(newCtx); err != nil {
					logErrorf(protoInfo, "before cancel() error:%v", err)
					cancelFun()
					logErrorf(protoInfo, "after cancel() error:%v", err)
					return err
				}
				cancelFun()
				logDebugf(protoInfo, "TLS handshake ok")
				rs.UseConn(tlsConn)
				logDebugf(protoInfo, "TLS handshake finished")

				// tls upgradeOk
				protocol.SetTlsEstablished()
			} else {
				// client don't ask server to upgrade TLS
				if err := protocol.Authenticate(ctx); err != nil {
					return err
				}
				protocol.SetTlsEstablished()
				protocol.SetEstablished()
			}
		} else {
			logDebugf(protoInfo, "handleHandshake")
			_, err = protocol.HandleHandshake(ctx, payload)
			if err != nil {
				logErrorf(protoInfo, "error:%v", err)
				return err
			}
			if err = protocol.Authenticate(ctx); err != nil {
				return err
			}
			protocol.SetEstablished()
		}

		dbName := protocol.GetDatabaseName()
		if ses != nil && dbName != "" {
			ses.SetDatabaseName(dbName)
		}
		return nil
	}

	req := protocol.GetRequest(payload)
	req.seq = seq

	//handle request
	err = routine.handleRequest(req)
	if err != nil {
		logErrorf(protoInfo, "error:%v", err)
		return err
	}

	return nil
}

// clientCount returns the count of the clients
func (rm *RoutineManager) clientCount() int {
	var count int
	rm.mu.RLock()
	count = len(rm.clients)
	rm.mu.RUnlock()
	return count
}

func (rm *RoutineManager) printDebug() {
	type info struct {
		id    uint32
		peer  string
		count []uint64
	}
	infos := list.New()
	rm.mu.RLock()
	for _, routine := range rm.clients {
		proto := routine.getProtocol()
		infos.PushBack(&info{
			proto.ConnectionID(),
			proto.Peer(),
			proto.resetDebugCount(),
		})
	}
	rm.mu.RUnlock()

	bb := bytes.Buffer{}
	bb.WriteString("Clients:")
	bb.WriteString(fmt.Sprintf("(%d)\n", infos.Len()))
	for e := infos.Front(); e != nil; e = e.Next() {
		d := e.Value.(*info)
		if d == nil {
			continue
		}
		bb.WriteString(fmt.Sprintf("%d|%s|", d.id, d.peer))
		for i, u := range d.count {
			bb.WriteString(fmt.Sprintf("%d:0x%x ", i, u))
		}
		bb.WriteByte('\n')
	}
	logutil.Info(bb.String())
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
					rt.killConnection(false)
					ar.deleteRoutine(account, rt)
				}
			}
		}
	}

	ar.killQueueMu.Lock()
	for toKillAccount, killRecord := range ar.killIdQueue {
		if time.Since(killRecord.killTime) > time.Duration(rm.pu.SV.CleanKillQueueInterval)*time.Minute {
			delete(ar.killIdQueue, toKillAccount)
		}
	}
	ar.killQueueMu.Unlock()
}

func NewRoutineManager(ctx context.Context, pu *config.ParameterUnit) (*RoutineManager, error) {
	accountRoutine := &AccountRoutineManager{
		killQueueMu:       sync.RWMutex{},
		accountId2Routine: make(map[int64]map[*Routine]uint64),
		accountRoutineMu:  sync.RWMutex{},
		killIdQueue:       make(map[int64]KillRecord),
		ctx:               ctx,
	}
	rm := &RoutineManager{
		ctx:            ctx,
		clients:        make(map[goetty.IOSession]*Routine),
		pu:             pu,
		accountRoutine: accountRoutine,
	}

	if pu.SV.EnableTls {
		err := initTlsConfig(rm, pu.SV)
		if err != nil {
			return nil, err
		}
	}

	//add debug routine
	if pu.SV.PrintDebug {
		go func() {
			for {
				select {
				case <-rm.ctx.Done():
					return
				default:
				}
				rm.printDebug()
				time.Sleep(time.Duration(pu.SV.PrintDebugInterval) * time.Minute)
			}
		}()
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
			time.Sleep(time.Duration(time.Duration(pu.SV.KillRountinesInterval) * time.Minute))
		}
	}()

	return rm, nil
}

func initTlsConfig(rm *RoutineManager, SV *config.FrontendParameters) error {
	if len(SV.TlsCertFile) == 0 || len(SV.TlsKeyFile) == 0 {
		return moerr.NewInternalError(rm.ctx, "init TLS config error : cert file or key file is empty")
	}

	var tlsCert tls.Certificate
	var err error
	tlsCert, err = tls.LoadX509KeyPair(SV.TlsCertFile, SV.TlsKeyFile)
	if err != nil {
		return moerr.NewInternalError(rm.ctx, "init TLS config error :load x509 failed")
	}

	clientAuthPolicy := tls.NoClientCert
	var certPool *x509.CertPool
	if len(SV.TlsCaFile) > 0 {
		var caCert []byte
		caCert, err = os.ReadFile(SV.TlsCaFile)
		if err != nil {
			return moerr.NewInternalError(rm.ctx, "init TLS config error :read TlsCaFile failed")
		}
		certPool = x509.NewCertPool()
		if certPool.AppendCertsFromPEM(caCert) {
			clientAuthPolicy = tls.VerifyClientCertIfGiven
		}
	}

	// This excludes ciphers listed in tls.InsecureCipherSuites() and can be used to filter out more
	// var cipherSuites []uint16
	// var cipherNames []string
	// for _, sc := range tls.CipherSuites() {
	// cipherSuites = append(cipherSuites, sc.ID)
	// switch sc.ID {
	// case tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA, tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA,
	// 	tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305, tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305:
	// logutil.Info("Disabling weak cipherSuite", zap.String("cipherSuite", sc.Name))
	// default:
	// cipherNames = append(cipherNames, sc.Name)
	// cipherSuites = append(cipherSuites, sc.ID)
	// }
	// }
	// logutil.Info("Enabled ciphersuites", zap.Strings("cipherNames", cipherNames))

	rm.tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		ClientCAs:    certPool,
		ClientAuth:   clientAuthPolicy,
		// MinVersion:   tls.VersionTLS13,
		// CipherSuites: cipherSuites,
	}
	logutil.Info("init TLS config finished")
	return nil
}
