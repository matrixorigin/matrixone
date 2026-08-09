// Copyright 2024 Matrix Origin
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
	"errors"
	"net"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
)

type closeErrorListener struct {
	net.Listener
	err error
}

type scriptedSQLAdmissionListener struct {
	accept func() (net.Conn, error)
}

func (l *scriptedSQLAdmissionListener) Accept() (net.Conn, error) {
	return l.accept()
}

func (l *scriptedSQLAdmissionListener) Close() error { return nil }

func (l *scriptedSQLAdmissionListener) Addr() net.Addr { return nil }

type testMOServerBaseService struct {
	MockBaseService
	id string
}

func (s *testMOServerBaseService) ID() string {
	return s.id
}

func (s *testMOServerBaseService) SessionMgr() *queryservice.SessionManager {
	return nil
}

func (l closeErrorListener) Close() error {
	_ = l.Listener.Close()
	return l.err
}

func TestMOServerStopCompletesCleanupAfterListenerCloseError(t *testing.T) {
	service := t.Name()
	InitServerLevelVars(service)
	listenerErr := errors.New("listener close failed")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	pu.SV.SetDefaultValues()
	setPu(service, pu)
	setSessionAlloc(service, NewLeakCheckAllocator())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rm, err := NewRoutineManager(ctx, service)
	require.NoError(t, err)

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	rs, err := NewIOSession(serverConn, pu, service)
	require.NoError(t, err)
	rm.setRoutine(rs, 1, &Routine{})

	mo := &MOServer{
		rm:        rm,
		running:   true,
		listeners: []net.Listener{closeErrorListener{Listener: listener, err: listenerErr}},
	}

	err = mo.Stop()
	require.ErrorIs(t, err, listenerErr)
	require.False(t, mo.IsRunning())
	if err := clientConn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		return
	}
	_, err = clientConn.Read(make([]byte, 1))
	require.Error(t, err)
}

func TestMOServerStopBeforeStartReleasesListener(t *testing.T) {
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	pu.SV.SetDefaultValues()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mo := NewMOServer(ctx, "127.0.0.1:0", pu, nil, &testMOServerBaseService{id: t.Name()})
	addr := mo.listeners[0].Addr().String()

	require.NoError(t, mo.Stop())
	require.NoError(t, mo.Stop())

	rebound, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	require.NoError(t, rebound.Close())
}

func TestMOServerRejectsDirectSQLWhenCNAdmissionIsClosed(t *testing.T) {
	serverSide, clientSide := net.Pipe()
	defer clientSide.Close()
	sentinel := errors.New("listener stopped")
	step := 0
	listener := &scriptedSQLAdmissionListener{accept: func() (net.Conn, error) {
		step++
		if step == 1 {
			return serverSide, nil
		}
		return nil, sentinel
	}}
	mo := &MOServer{canAcceptNewConnections: func() bool { return false }}
	mo.wg.Add(1)
	mo.startAccept(context.Background(), listener)

	buf := make([]byte, 1)
	_, err := clientSide.Read(buf)
	require.Error(t, err, "CN admission must close the direct SQL socket before session creation")
}

func Test_handshake(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//before anything using the configuration
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	_, err := toml.DecodeFile("test/system_vars_config.toml", pu.SV)
	require.NoError(t, err)
	pu.SV.SkipCheckUser = true
	pu.SV.KillRountinesInterval = 0
	setSessionAlloc("", NewLeakCheckAllocator())
	setPu("", pu)

	rm, _ := NewRoutineManager(ctx, "")
	setRtMgr("", rm)
	sv := MOServer{
		rm: rm,
	}

	tConn := &testConn{
		mod:  testConnModReadBuffer,
		rbuf: makePacket([]byte{0, 0}, 1),
	}

	ioses, err := NewIOSession(tConn, pu, "")
	if err != nil {
		panic(err)
	}
	proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)

	ses := NewSession(ctx, "", proto, nil)
	proto.ses = ses

	rt := &Routine{}
	rt.protocol.Store(&holder[MysqlRrWr]{value: proto})
	rt.ses = ses

	rm.setRoutine(ioses, 0, rt)

	err = sv.handshake(ioses)
	assert.Error(t, err)

	////SSL handshake
	data := gIO.AppendUint32(nil, DefaultCapability|CLIENT_SSL) //capability
	data = gIO.AppendUint32(data, MaxPayloadSize)               //payload size
	data = gIO.AppendUint8(data, 1)                             //collationid
	data = append(data, make([]byte, 23)...)
	tConn.rbuf = makePacket(data, 1)
	err = sv.handshake(ioses)
	assert.Error(t, err)

	////no SSL handshake
	data = gIO.AppendUint32(nil, DefaultCapability) //capability
	data = gIO.AppendUint32(data, MaxPayloadSize)   //payload size
	data = gIO.AppendUint8(data, 1)                 //collationid
	data = append(data, make([]byte, 23)...)
	data = append(data, []byte("abc")...) //user name
	data = append(data, 0)

	lenencBuffer := make([]byte, 9)
	l := proto.writeIntLenEnc(lenencBuffer, 0, 3)
	data = append(data, lenencBuffer[:l]...) //password length
	data = append(data, []byte("111")...)    //password

	data = append(data, []byte("db")...) //db name
	data = append(data, 0)

	data = append(data, []byte(AuthNativePassword)...) //plugin
	data = append(data, 0)

	l = proto.writeIntLenEnc(lenencBuffer, 0, 0)
	data = append(data, lenencBuffer[:l]...) //connect attrs

	tConn.rbuf = makePacket(data, 1)
	err = sv.handshake(ioses)
	assert.NoError(t, err)
}
