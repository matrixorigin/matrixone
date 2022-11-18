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

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

type RoutineManager struct {
	mu            sync.Mutex
	ctx           context.Context
	clients       map[goetty.IOSession]*Routine
	pu            *config.ParameterUnit
	skipCheckUser bool
	tlsConfig     *tls.Config
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

	routine := NewRoutine(rm.getCtx(), pro, exe, pu)
	routine.SetRoutineMgr(rm)

	// XXX MPOOL pass in a nil mpool.
	// XXX MPOOL can choose to use a Mid sized mpool, if, we know
	// this mpool will be deleted.  Maybe in the following Closed method.
	ses := NewSession(routine.GetClientProtocol(), nil, pu, gSysVariables, true)
	ses.SetRequestContext(routine.GetCancelRoutineCtx())
	ses.SetFromRealUser(true)
	routine.SetSession(ses)
	pro.SetSession(ses)

	logDebugf(pro.GetConciseProfile(), "have done some preparation for the connection %s", rs.RemoteAddress())

	hsV10pkt := pro.makeHandshakeV10Payload()
	err := pro.writePackets(hsV10pkt)
	if err != nil {
		logError(pro.GetConciseProfile(), "failed to handshake with server, quiting routine...")
		routine.Quit()
		return
	}

	logDebugf(pro.GetConciseProfile(), "have sent handshake packet to connection %s", rs.RemoteAddress())
	rm.setRoutine(rs, routine)
}

/*
When the io is closed, the Closed will be called.
*/
func (rm *RoutineManager) Closed(rs goetty.IOSession) {
	var rt *Routine
	var ok bool

	rm.mu.Lock()
	rt, ok = rm.clients[rs]
	if ok {
		delete(rm.clients, rs)
	}
	rm.mu.Unlock()

	ses := rt.GetSession()
	logDebugf(ses.GetConciseProfile(), "will close io session.")
	if rt != nil {
		rt.Quit()
	}
}

/*
KILL statement
*/
func (rm *RoutineManager) killStatement(id uint64) error {
	var rt *Routine = nil
	rm.mu.Lock()
	for _, value := range rm.clients {
		if uint64(value.getConnID()) == id {
			rt = value
			break
		}
	}
	rm.mu.Unlock()

	if rt != nil {
		logutil.Infof("will close the statement %d", id)
		rt.notifyClose()
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
	var err error
	var isTlsHeader bool
	connectionInfo := getConnectionInfo(rs)
	routine := rm.getRoutine(rs)
	if routine == nil {
		err = moerr.NewInternalError("routine does not exist")
		logutil.Errorf("%s error:%v", connectionInfo, err)
		return err
	}

	protocol := routine.GetClientProtocol().(*MysqlProtocolImpl)
	protoProfile := protocol.GetConciseProfile()
	packet, ok := msg.(*Packet)

	protocol.SetSequenceID(uint8(packet.SequenceID + 1))
	var seq = protocol.GetSequenceId()
	if !ok {
		err = moerr.NewInternalError("message is not Packet")
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
			err = moerr.NewInternalError("message is not Packet")
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
		ses := protocol.GetSession()
		if protocol.GetCapability()&CLIENT_SSL != 0 && !protocol.IsTlsEstablished() {
			logDebugf(protoProfile, "setup ssl")
			isTlsHeader, err = protocol.handleHandshake(payload)
			if err != nil {
				logErrorf(protoProfile, "error:%v", err)
				return err
			}
			if isTlsHeader {
				logDebugf(protoProfile, "upgrade to TLS")
				// do upgradeTls
				tlsConn := tls.Server(rs.RawConn(), rm.getTlsConfig())
				logDebugf(protoProfile, "get TLS conn ok")
				newCtx, cancelFun := context.WithTimeout(ses.GetRequestContext(), 20*time.Second)
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
			_, err = protocol.handleHandshake(payload)
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

	req := routine.GetClientProtocol().GetRequest(payload)
	req.seq = seq
	ch := routine.GetRequestChannel()
	chLen := len(ch)
	capLen := cap(ch)
	if chLen+1 > capLen {
		logDebugf(protoProfile, "the request channel will block. length %d capacity %d", chLen, capLen)
	}
	ch <- req

	return nil
}

func NewRoutineManager(ctx context.Context, pu *config.ParameterUnit) (*RoutineManager, error) {
	rm := &RoutineManager{
		ctx:     ctx,
		clients: make(map[goetty.IOSession]*Routine),
		pu:      pu,
	}
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
		return moerr.NewInternalError("init TLS config error : cert file or key file is empty")
	}

	var tlsCert tls.Certificate
	var err error
	tlsCert, err = tls.LoadX509KeyPair(SV.TlsCertFile, SV.TlsKeyFile)
	if err != nil {
		return moerr.NewInternalError("init TLS config error :load x509 failed")
	}

	clientAuthPolicy := tls.NoClientCert
	var certPool *x509.CertPool
	if len(SV.TlsCaFile) > 0 {
		var caCert []byte
		caCert, err = os.ReadFile(SV.TlsCaFile)
		if err != nil {
			return moerr.NewInternalError("init TLS config error :read TlsCaFile failed")
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
