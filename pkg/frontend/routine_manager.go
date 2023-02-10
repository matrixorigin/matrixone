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
	"os"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/metric"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

type RoutineManager struct {
	mu             sync.Mutex
	ctx            context.Context
	clients        map[goetty.IOSession]*Routine
	pu             *config.ParameterUnit
	skipCheckUser  bool
	tlsConfig      *tls.Config
	autoIncrCaches defines.AutoIncrCaches
}

func (rm *RoutineManager) GetAutoIncrCache() defines.AutoIncrCaches {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return rm.autoIncrCaches
}

func (rm *RoutineManager) SetSkipCheckUser(b bool) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.skipCheckUser = b
}

func (rm *RoutineManager) GetSkipCheckUser() bool {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return rm.skipCheckUser
}

func (rm *RoutineManager) getParameterUnit() *config.ParameterUnit {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return rm.pu
}

func (rm *RoutineManager) getCtx() context.Context {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return rm.ctx
}

func (rm *RoutineManager) setRoutine(rs goetty.IOSession, r *Routine) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.clients[rs] = r
}

func (rm *RoutineManager) getRoutine(rs goetty.IOSession) *Routine {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return rm.clients[rs]
}

func (rm *RoutineManager) getTlsConfig() *tls.Config {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	return rm.tlsConfig
}

func (rm *RoutineManager) Created(rs goetty.IOSession) {
	logutil.Debugf("get the connection from %s", rs.RemoteAddress())
	pu := rm.getParameterUnit()
	pro := NewMysqlClientProtocol(nextConnectionID(), rs, int(pu.SV.MaxBytesInOutbufToFlush), pu.SV)
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
	ses.SetFromRealUser(true)
	ses.setSkipCheckPrivilege(rm.GetSkipCheckUser())

	// Add  autoIncrCaches in session structure.
	ses.SetAutoIncrCaches(rm.autoIncrCaches)

	routine.setSession(ses)
	pro.SetSession(ses)

	logDebugf(pro.GetConciseProfile(), "have done some preparation for the connection %s", rs.RemoteAddress())

	hsV10pkt := pro.makeHandshakeV10Payload()
	err := pro.writePackets(hsV10pkt)
	if err != nil {
		logErrorf(pro.GetConciseProfile(), "failed to handshake with server, quiting routine... %s", err)
		routine.killConnection(true)
		return
	}

	logDebugf(pro.GetConciseProfile(), "have sent handshake packet to connection %s", rs.RemoteAddress())
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
			})
			logDebugf(ses.GetConciseProfile(), "the io session was closed.")
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
	rm.mu.Lock()
	for _, value := range rm.clients {
		if uint64(value.getConnectionID()) == id {
			rt = value
			break
		}
	}
	rm.mu.Unlock()

	killMyself := idThatKill == id
	if rt != nil {
		if killConnection {
			logutil.Infof("kill connection %d", id)
			rt.killConnection(killMyself)
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
	protoProfile := protocol.GetConciseProfile()
	packet, ok := msg.(*Packet)

	protocol.SetSequenceID(uint8(packet.SequenceID + 1))
	var seq = protocol.GetSequenceId()
	if !ok {
		err = moerr.NewInternalError(ctx, "message is not Packet")
		logErrorf(protoProfile, "error:%v", err)
		return err
	}

	length := packet.Length
	payload := packet.Payload
	for uint32(length) == MaxPayloadSize {
		msg, err = protocol.GetTcpConnection().Read(goetty.ReadOptions{})
		if err != nil {
			logErrorf(protoProfile, "read message failed. error:%s", err)
			return err
		}

		packet, ok = msg.(*Packet)
		if !ok {
			err = moerr.NewInternalError(ctx, "message is not Packet")
			logErrorf(protoProfile, "error:%v", err)
			return err
		}

		protocol.SetSequenceID(uint8(packet.SequenceID + 1))
		seq = protocol.GetSequenceId()
		payload = append(payload, packet.Payload...)
		length = packet.Length
	}

	// finish handshake process
	if !protocol.IsEstablished() {
		logDebugf(protoProfile, "HANDLE HANDSHAKE")

		/*
			di := MakeDebugInfo(payload,80,8)
			logutil.Infof("RP[%v] Payload80[%v]",rs.RemoteAddr(),di)
		*/
		ses := routine.getSession()
		if protocol.GetCapability()&CLIENT_SSL != 0 && !protocol.IsTlsEstablished() {
			logDebugf(protoProfile, "setup ssl")
			isTlsHeader, err = protocol.HandleHandshake(ctx, payload)
			if err != nil {
				logErrorf(protoProfile, "error:%v", err)
				return err
			}
			if isTlsHeader {
				logDebugf(protoProfile, "upgrade to TLS")
				// do upgradeTls
				tlsConn := tls.Server(rs.RawConn(), rm.getTlsConfig())
				logDebugf(protoProfile, "get TLS conn ok")
				newCtx, cancelFun := context.WithTimeout(ctx, 20*time.Second)
				if err = tlsConn.HandshakeContext(newCtx); err != nil {
					logErrorf(protoProfile, "before cancel() error:%v", err)
					cancelFun()
					logErrorf(protoProfile, "after cancel() error:%v", err)
					return err
				}
				cancelFun()
				logDebugf(protoProfile, "TLS handshake ok")
				rs.UseConn(tlsConn)
				logDebugf(protoProfile, "TLS handshake finished")

				// tls upgradeOk
				protocol.SetTlsEstablished()
			} else {
				// client don't ask server to upgrade TLS
				protocol.SetTlsEstablished()
				protocol.SetEstablished()
			}
		} else {
			logDebugf(protoProfile, "handleHandshake")
			_, err = protocol.HandleHandshake(ctx, payload)
			if err != nil {
				logErrorf(protoProfile, "error:%v", err)
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

	req := routine.getProtocol().GetRequest(payload)
	req.seq = seq

	//handle request
	err = routine.handleRequest(req)
	if err != nil {
		logErrorf(protoProfile, "error:%v", err)
		return err
	}

	return nil
}

// clientCount returns the count of the clients
func (rm *RoutineManager) clientCount() int {
	var count int
	rm.mu.Lock()
	count = len(rm.clients)
	rm.mu.Unlock()
	return count
}

func NewRoutineManager(ctx context.Context, pu *config.ParameterUnit) (*RoutineManager, error) {
	rm := &RoutineManager{
		ctx:     ctx,
		clients: make(map[goetty.IOSession]*Routine),
		pu:      pu,
	}

	// Initialize auto incre cache.
	rm.autoIncrCaches.AutoIncrCaches = make(map[string]defines.AutoIncrCache)
	rm.autoIncrCaches.Mu = &rm.mu

	if pu.SV.EnableTls {
		err := initTlsConfig(rm, pu.SV)
		if err != nil {
			return nil, err
		}
	}
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
