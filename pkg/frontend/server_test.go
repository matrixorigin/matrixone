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
	"sync"
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

func TestMOServerStopJoinsConnectionCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	mo := &MOServer{rm: &RoutineManager{ctx: ctx, cancel: cancel}, running: true}
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() { _ = clientConn.Close() })
	require.True(t, mo.admitConnection(serverConn), "accepted before session registration")
	release := make(chan struct{})
	defer close(release)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		defer mo.releaseConnection(serverConn)
		<-release // deferred routine cleanup may still use transaction state
	}()
	stopped := make(chan error, 2)
	go func() { stopped <- mo.Stop() }()
	go func() { stopped <- mo.Stop() }()
	<-ctx.Done() // Stop has sealed connection admission.
	late, peer := net.Pipe()
	defer late.Close()
	defer peer.Close()
	require.False(t, mo.admitConnection(late))
	_, err := clientConn.Read(make([]byte, 1))
	require.Error(t, err, "Stop interrupts pre-registration socket I/O")
	select {
	case <-stopped:
		t.Fatal("Stop returned before connection cleanup")
	default:
	}
	// Release explicitly, while retaining cleanup if an assertion failed.
	release <- struct{}{}
	<-finished
	require.NoError(t, <-stopped)
	require.NoError(t, <-stopped)
	require.NoError(t, mo.Stop())
	require.Empty(t, mo.connections)
}

type blockedSessionAllocator struct {
	entered chan struct{}
	release chan struct{}
}

func (a *blockedSessionAllocator) Alloc(int) ([]byte, error) {
	close(a.entered)
	<-a.release
	return nil, errors.New("injected session allocation failure")
}

func (*blockedSessionAllocator) Free([]byte) {}

func TestMOServerStopJoinsAcceptedSessionInitialization(t *testing.T) {
	service := t.Name()
	InitServerLevelVars(service)
	allocator := &blockedSessionAllocator{entered: make(chan struct{}), release: make(chan struct{})}
	setSessionAlloc(service, allocator)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	mo := &MOServer{service: service, pu: pu, running: true,
		rm: &RoutineManager{ctx: ctx, cancel: cancel}, listeners: []net.Listener{listener}}
	var release sync.Once
	unblock := func() { release.Do(func() { close(allocator.release) }) }
	t.Cleanup(func() { unblock(); require.NoError(t, mo.Stop()) })
	mo.wg.Add(1)
	go mo.startAccept(ctx, listener)
	conn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	<-allocator.entered // real accept -> handleConn -> NewIOSession, before rm.Created
	stopped := make(chan error, 1)
	go func() { stopped <- mo.Stop() }()
	<-ctx.Done()
	_, err = conn.Read(make([]byte, 1))
	require.Error(t, err, "Stop must find even an unregistered connection")
	select {
	case <-stopped:
		t.Fatal("Stop returned while session initialization still owned the connection")
	default:
	}
	unblock()
	require.NoError(t, <-stopped)
	require.Empty(t, mo.connections)
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
