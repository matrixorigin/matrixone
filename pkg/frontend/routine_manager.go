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
	"os"
	"sync"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

type RoutineManager struct {
	rwlock        sync.RWMutex
	ctx           context.Context
	clients       map[goetty.IOSession]*Routine
	pu            *config.ParameterUnit
	skipCheckUser bool
	tlsConfig     *tls.Config
}

func (rm *RoutineManager) SetSkipCheckUser(b bool) {
	rm.rwlock.Lock()
	defer rm.rwlock.Unlock()
	rm.skipCheckUser = b
}

func (rm *RoutineManager) GetSkipCheckUser() bool {
	rm.rwlock.RLock()
	defer rm.rwlock.RUnlock()
	return rm.skipCheckUser
}

func (rm *RoutineManager) getParameterUnit() *config.ParameterUnit {
	return rm.pu
}

func (rm *RoutineManager) Created(rs goetty.IOSession) {
	pro := NewMysqlClientProtocol(nextConnectionID(), rs, int(rm.pu.SV.MaxBytesInOutbufToFlush), rm.pu.SV)
	pro.SetSkipCheckUser(rm.GetSkipCheckUser())
	exe := NewMysqlCmdExecutor()
	exe.SetRoutineManager(rm)

	routine := NewRoutine(rm.ctx, pro, exe, rm.pu)
	routine.SetRoutineMgr(rm)

	// XXX MPOOL pass in a nil mpool.
	// XXX MPOOL can choose to use a Mid sized mpool, if, we know
	// this mpool will be deleted.  Maybe in the following Closed method.
	ses := NewSession(routine.protocol, nil, rm.pu, gSysVariables)
	ses.SetRequestContext(routine.cancelRoutineCtx)
	ses.SetFromRealUser(true)
	routine.SetSession(ses)
	pro.SetSession(ses)

	hsV10pkt := pro.makeHandshakeV10Payload()
	err := pro.writePackets(hsV10pkt)
	if err != nil {
		panic(err)
	}

	rm.rwlock.Lock()
	defer rm.rwlock.Unlock()
	rm.clients[rs] = routine
}

/*
When the io is closed, the Closed will be called.
*/
func (rm *RoutineManager) Closed(rs goetty.IOSession) {
	rm.rwlock.Lock()
	defer rm.rwlock.Unlock()
	defer delete(rm.clients, rs)

	rt, ok := rm.clients[rs]
	if !ok {
		return
	}
	logutil.Infof("will close iosession")
	rt.Quit()
}

/*
KILL statement
*/
func (rm *RoutineManager) killStatement(id uint64) error {
	rm.rwlock.Lock()
	defer rm.rwlock.Unlock()
	var rt *Routine = nil
	for _, value := range rm.clients {
		if uint64(value.getConnID()) == id {
			rt = value
			break
		}
	}

	if rt != nil {
		logutil.Infof("will close the statement %d", id)
		rt.notifyClose()
	}
	return nil
}

func (rm *RoutineManager) Handler(rs goetty.IOSession, msg interface{}, received uint64) error {
	rm.rwlock.RLock()
	routine, ok := rm.clients[rs]
	rm.rwlock.RUnlock()
	if !ok {
		return moerr.NewInternalError("routine does not exist")
	}

	protocol := routine.protocol.(*MysqlProtocolImpl)

	packet, ok := msg.(*Packet)

	protocol.m.Lock()
	protocol.sequenceId = uint8(packet.SequenceID + 1)
	var seq = protocol.sequenceId
	protocol.m.Unlock()
	if !ok {
		return moerr.NewInternalError("message is not Packet")
	}

	length := packet.Length
	payload := packet.Payload
	for uint32(length) == MaxPayloadSize {
		var err error
		msg, err = protocol.tcpConn.Read(goetty.ReadOptions{})
		if err != nil {
			return moerr.NewInternalError("read msg error")
		}

		packet, ok = msg.(*Packet)
		if !ok {
			return moerr.NewInternalError("message is not Packet")
		}

		protocol.sequenceId = uint8(packet.SequenceID + 1)
		seq = protocol.sequenceId
		payload = append(payload, packet.Payload...)
		length = packet.Length
	}

	// finish handshake process
	if !protocol.IsEstablished() {
		logutil.Infof("HANDLE HANDSHAKE")

		/*
			di := MakeDebugInfo(payload,80,8)
			logutil.Infof("RP[%v] Payload80[%v]",rs.RemoteAddr(),di)
		*/

		if protocol.capability&CLIENT_SSL != 0 && !protocol.IsTlsEstablished() {
			isTlsHeader, err := protocol.handleHandshake(payload)
			if err != nil {
				return err
			}
			if isTlsHeader {
				logutil.Infof("upgrade to TLS")
				// do upgradeTls
				tlsConn := tls.Server(rs.RawConn(), rm.tlsConfig)
				logutil.Infof("get TLS conn ok")
				newCtx, cancelFun := context.WithTimeout(protocol.ses.requestCtx, 20*time.Second)
				if err := tlsConn.HandshakeContext(newCtx); err != nil {
					cancelFun()
					return err
				}
				cancelFun()
				logutil.Infof("TLS handshake ok")
				rs.UseConn(tlsConn)
				logutil.Infof("TLS handshake finished")

				// tls upgradeOk
				protocol.SetTlsEstablished()
			} else {
				// client don't ask server to upgrade TLS
				protocol.SetTlsEstablished()
				protocol.SetEstablished()
			}
		} else {
			_, err := protocol.handleHandshake(payload)
			if err != nil {
				return err
			}
			protocol.SetEstablished()
		}

		if protocol.ses != nil && protocol.database != "" {
			protocol.ses.SetDatabaseName(protocol.database)
		}
		return nil
	}

	req := routine.protocol.GetRequest(payload)
	req.seq = seq
	routine.requestChan <- req

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
