// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"io"
	"math/big"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func terminalSocketEvents(socket syscall.Conn) (int16, error) {
	raw, err := socket.SyscallConn()
	if err != nil {
		return 0, err
	}
	var events int16
	var pollErr error
	err = raw.Control(func(fd uintptr) {
		fdset := [1]unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN | unix.POLLRDHUP}}
		_, pollErr = unix.Poll(fdset[:], 0)
		events = fdset[0].Revents
	})
	if err != nil {
		return 0, err
	}
	return events, pollErr
}

func terminalSocketHasEvent(socket syscall.Conn, flag int16) bool {
	events, err := terminalSocketEvents(socket)
	return err == nil && events&flag != 0
}

func TestTerminalProbeAfterTLSHandshake(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	certTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), DNSNames: []string{"localhost"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, certTemplate, certTemplate, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	server, client := tcpConnectionPair(t)
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	require.NoError(t, server.SetDeadline(time.Now().Add(5*time.Second)))
	require.NoError(t, client.SetDeadline(time.Now().Add(5*time.Second)))
	tlsServer := tls.Server(server, &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}})
	tlsClient := tls.Client(client, &tls.Config{RootCAs: roots, ServerName: "localhost"})
	serverHandshake := make(chan error, 1)
	go func() { serverHandshake <- tlsServer.Handshake() }()
	require.NoError(t, tlsClient.Handshake())
	require.NoError(t, <-serverHandshake)
	probe := newTerminalSocketProbe(terminalSocketConn(tlsServer))
	require.NotNil(t, probe)
	_, err = tlsClient.Write([]byte("request"))
	require.NoError(t, err)
	var request [7]byte
	_, err = io.ReadFull(tlsServer, request[:])
	require.NoError(t, err)
	require.Equal(t, "request", string(request[:]))
	require.NoError(t, client.(*net.TCPConn).CloseWrite())
	require.Eventually(t, func() bool {
		return terminalSocketHasEvent(server.(*net.TCPConn), unix.POLLRDHUP)
	}, 3*time.Second, time.Millisecond)
	require.False(t, probe.terminal())
	_, err = tlsServer.Write([]byte("reply"))
	require.NoError(t, err)
	var reply [5]byte
	_, err = io.ReadFull(tlsClient, reply[:])
	require.NoError(t, err)
	require.Equal(t, "reply", string(reply[:]))
	require.NoError(t, client.(*net.TCPConn).SetLinger(0))
	require.NoError(t, client.Close())
	require.Eventually(t, probe.terminal, 3*time.Second, time.Millisecond, "the TLS transport reset must be observed")
}

func TestTerminalConnectionMonitorCancelsWithoutConsumingInput(t *testing.T) {
	server, client := tcpConnectionPair(t)
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	t.Cleanup(routine.cancelRoutineFunc)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	routine.setCancelRequestFunc(cancel)
	require.True(t, routine.setInProcessRequest(true))
	conn := &Conn{conn: server}
	monitor := connectionLivenessMonitor{rm: &RoutineManager{clients: map[*Conn]*Routine{conn: routine}}}
	_, err := client.Write([]byte("xy"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return terminalSocketHasEvent(server.(*net.TCPConn), unix.POLLIN)
	}, 3*time.Second, time.Millisecond)
	monitor.scan(time.Now())
	require.NoError(t, ctx.Err())
	require.NoError(t, client.(*net.TCPConn).SetLinger(0))
	require.NoError(t, client.Close())
	require.Eventually(t, func() bool {
		monitor.scan(time.Now())
		return ctx.Err() != nil
	}, 3*time.Second, time.Millisecond)
	require.True(t, routine.isTerminalRetired())
	var payload [2]byte
	_, err = io.ReadFull(server, payload[:])
	require.NoError(t, err)
	require.Equal(t, "xy", string(payload[:]))
	monitor.scan(time.Now())
	require.Empty(t, monitor.probes, "retired connections must leave the worker cache")
}

func TestTerminalConnectionMonitorReusesAndExpiresIdleProbe(t *testing.T) {
	server, client := tcpConnectionPair(t)
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	t.Cleanup(routine.cancelRoutineFunc)
	conn := &Conn{conn: server}
	monitor := connectionLivenessMonitor{rm: &RoutineManager{clients: map[*Conn]*Routine{conn: routine}}}
	require.True(t, routine.setInProcessRequest(true))
	monitor.scan(time.Now())
	probe := monitor.probes[conn]
	require.NotNil(t, probe)
	monitor.scan(time.Now())
	require.Same(t, probe, monitor.probes[conn], "active connections should reuse the probe")
	require.True(t, routine.setInProcessRequest(false))
	monitor.scan(time.Now())
	require.Nil(t, monitor.probes, "an empty cache must release its retained buckets")
	require.True(t, routine.setInProcessRequest(true))
	monitor.scan(time.Now())
	require.NotNil(t, monitor.probes[conn], "a later request must recreate its probe")
}

func TestTerminalConnectionMonitorReplacedSocketAtCacheLimit(t *testing.T) {
	oldServer, oldClient := tcpConnectionPair(t)
	newServer, newClient := tcpConnectionPair(t)
	t.Cleanup(func() {
		_ = oldServer.Close()
		_ = oldClient.Close()
		_ = newServer.Close()
		_ = newClient.Close()
	})
	routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	t.Cleanup(routine.cancelRoutineFunc)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	routine.setCancelRequestFunc(cancel)
	now := time.Now()
	routine.requestStartedAt.Store(clientRequestClockValue(now.Add(-time.Second)))
	conn := &Conn{conn: oldServer}
	monitor := connectionLivenessMonitor{
		rm:     &RoutineManager{clients: make(map[*Conn]*Routine)},
		probes: make(map[*Conn]*terminalSocketProbe, terminalConnectionProbeCacheLimit),
	}
	monitor.rm.clients[conn] = routine
	monitor.probes[conn] = newTerminalSocketProbe(oldServer.(*net.TCPConn))
	unused := &terminalSocketProbe{}
	for len(monitor.probes) < terminalConnectionProbeCacheLimit {
		monitor.probes[&Conn{}] = unused
	}
	// Keep the cache full through the contraction check; only conn has a TCP
	// socket, so the other entries do not affect the probe result.
	for len(monitor.rm.clients) < terminalConnectionProbeCacheLimit/4 {
		monitor.rm.clients[&Conn{}] = routine
	}
	conn.conn = newServer
	require.NoError(t, newClient.(*net.TCPConn).SetLinger(0))
	require.NoError(t, newClient.Close())
	require.Eventually(t, func() bool {
		return terminalSocketHasEvent(newServer.(*net.TCPConn), unix.POLLERR)
	}, 3*time.Second, time.Millisecond)
	monitor.scan(now)
	require.ErrorIs(t, ctx.Err(), context.Canceled, "a full cache must not reuse the old socket's probe")
	require.Nil(t, monitor.probes, "stale and retired probes must be evicted")
}

const formerDisconnectProbeGrace = 30 * time.Second

func TestConnectionLivenessMonitorSnapshotsRequestAge(t *testing.T) {
	now := time.Now()
	client := func(start time.Time, closing bool) (*Conn, *Routine) {
		routine := &Routine{}
		if !start.IsZero() {
			routine.requestStartedAt.Store(clientRequestClockValue(start))
		}
		routine.closing.Store(closing)
		return &Conn{}, routine
	}
	recentConn, recent := client(now.Add(-formerDisconnectProbeGrace+time.Nanosecond), false)
	exactConn, exact := client(now.Add(-formerDisconnectProbeGrace), false)
	oldConn, old := client(now.Add(-formerDisconnectProbeGrace-time.Second), false)
	futureConn, future := client(now.Add(time.Nanosecond), false)
	idleConn, idle := client(time.Time{}, false)
	closingConn, closing := client(now.Add(-time.Minute), true)
	monitor := connectionLivenessMonitor{rm: &RoutineManager{clients: map[*Conn]*Routine{
		recentConn: recent, exactConn: exact, oldConn: old,
		futureConn: future, idleConn: idle, closingConn: closing,
		nil: old, {}: nil,
	}}}
	monitor.snapshot(now)
	selected := make(map[*Conn]bool, len(monitor.requests))
	for _, request := range monitor.requests {
		selected[request.conn] = true
	}
	require.Len(t, selected, 3)
	require.Contains(t, selected, recentConn)
	require.Contains(t, selected, exactConn)
	require.Contains(t, selected, oldConn)

}

func TestConnectionLivenessMonitorPreservesFINAcrossFormerGrace(t *testing.T) {
	server, client := tcpConnectionPair(t)
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	t.Cleanup(routine.cancelRoutineFunc)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	routine.setCancelRequestFunc(cancel)
	now := time.Now()
	routine.requestStartedAt.Store(clientRequestClockValue(now.Add(-formerDisconnectProbeGrace + time.Nanosecond)))
	monitor := connectionLivenessMonitor{rm: &RoutineManager{clients: map[*Conn]*Routine{
		{conn: server}: routine,
	}}}
	require.NoError(t, client.(*net.TCPConn).CloseWrite())
	require.Eventually(t, func() bool {
		return terminalSocketHasEvent(server.(*net.TCPConn), unix.POLLRDHUP)
	}, 3*time.Second, time.Millisecond)
	for _, at := range []time.Time{now, now.Add(time.Nanosecond), now.Add(5 * time.Second), now.Add(time.Minute)} {
		monitor.scan(at)
		require.NoError(t, ctx.Err(), "FIN must not cancel a request at any age")
	}
	_, err := server.Write([]byte("reply"))
	require.NoError(t, err)
	var response [5]byte
	_, err = io.ReadFull(client, response[:])
	require.NoError(t, err)
	require.Equal(t, "reply", string(response[:]))
}

func TestConnectionLivenessMonitorRetriesBusyAndSkipsUnknown(t *testing.T) {
	for _, which := range []string{"routine", "controller", "operation"} {
		t.Run(which, func(t *testing.T) {
			busy := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
			ready := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
			t.Cleanup(busy.cancelRoutineFunc)
			t.Cleanup(ready.cancelRoutineFunc)
			busyCtx, cancelBusy := context.WithCancel(context.Background())
			readyCtx, cancelReady := context.WithCancel(context.Background())
			t.Cleanup(cancelBusy)
			t.Cleanup(cancelReady)
			busy.setCancelRequestFunc(cancelBusy)
			ready.setCancelRequestFunc(cancelReady)
			require.True(t, busy.setInProcessRequest(true))
			require.True(t, ready.setInProcessRequest(true))
			busyServer, busyClient := tcpConnectionPair(t)
			readyServer, readyClient := tcpConnectionPair(t)
			pipeServer, pipeClient := net.Pipe()
			t.Cleanup(func() {
				_ = busyServer.Close()
				_ = busyClient.Close()
				_ = readyServer.Close()
				_ = readyClient.Close()
				_ = pipeServer.Close()
				_ = pipeClient.Close()
			})
			for _, client := range []net.Conn{busyClient, readyClient} {
				require.NoError(t, client.(*net.TCPConn).SetLinger(0))
				require.NoError(t, client.Close())
			}
			for _, server := range []net.Conn{busyServer, readyServer} {
				require.Eventually(t, func() bool {
					return terminalSocketHasEvent(server.(*net.TCPConn), unix.POLLERR)
				}, 3*time.Second, time.Millisecond)
			}
			invalidTCP := &net.TCPConn{}
			require.Nil(t, newTerminalSocketProbe(invalidTCP))
			monitor := connectionLivenessMonitor{rm: &RoutineManager{clients: map[*Conn]*Routine{
				{conn: busyServer}: busy, {conn: readyServer}: ready,
				{conn: nil}: busy, {conn: pipeServer}: busy, {conn: invalidTCP}: busy,
			}}}
			var unlock func()
			switch which {
			case "routine":
				busy.mu.Lock()
				unlock = busy.mu.Unlock
			case "controller":
				busy.mc.Lock()
				unlock = busy.mc.Unlock
			case "operation":
				require.True(t, busy.mc.beginOperation())
				unlock = busy.mc.endOperation
			}
			var once sync.Once
			release := func() { once.Do(unlock) }
			t.Cleanup(release)
			monitor.scan(time.Now())
			require.NoError(t, busyCtx.Err(), "a busy routine must retain ownership")
			require.ErrorIs(t, readyCtx.Err(), context.Canceled, "a busy or unknown connection must not block other resets")
			require.Empty(t, monitor.requests)
			for _, request := range monitor.requests[:cap(monitor.requests)] {
				require.Empty(t, request, "the snapshot must release session references")
			}
			release()
			monitor.scan(time.Now())
			require.ErrorIs(t, busyCtx.Err(), context.Canceled, "a later scan must retry the busy reset")
		})
	}
}

func TestTerminalTCPProbe(t *testing.T) {
	t.Run("IPv6 reset", func(t *testing.T) {
		listener, err := net.Listen("tcp6", "[::1]:0")
		if err != nil {
			t.Skipf("IPv6 loopback unavailable: %v", err)
		}
		t.Cleanup(func() { _ = listener.Close() })
		accepted := make(chan net.Conn, 1)
		go func() {
			if conn, acceptErr := listener.Accept(); acceptErr == nil {
				accepted <- conn
			}
		}()
		client, err := net.Dial("tcp6", listener.Addr().String())
		require.NoError(t, err)
		t.Cleanup(func() { _ = client.Close() })
		var server net.Conn
		select {
		case server = <-accepted:
		case <-time.After(3 * time.Second):
			t.Fatal("IPv6 accept timed out")
		}
		t.Cleanup(func() { _ = server.Close() })
		probe := newTerminalSocketProbe(server.(*net.TCPConn))
		require.NotNil(t, probe)
		require.NoError(t, client.(*net.TCPConn).SetLinger(0))
		require.NoError(t, client.Close())
		require.Eventually(t, probe.terminal, 3*time.Second, time.Millisecond)
	})

	t.Run("reset keeps the socket error for the owner", func(t *testing.T) {
		server, client := tcpConnectionPair(t)
		t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
		tcpServer, tcpClient := server.(*net.TCPConn), client.(*net.TCPConn)
		probe := newTerminalSocketProbe(tcpServer)
		require.NotNil(t, probe)
		require.False(t, probe.terminal())
		require.NoError(t, tcpClient.SetLinger(0))
		require.NoError(t, tcpClient.Close())
		require.Eventually(t, probe.terminal, 3*time.Second, time.Millisecond)
		events, err := terminalSocketEvents(tcpServer)
		require.NoError(t, err)
		require.NotZero(t, events&unix.POLLERR)
		raw, err := tcpServer.SyscallConn()
		require.NoError(t, err)
		var recvErr error
		require.NoError(t, raw.Control(func(fd uintptr) {
			var one [1]byte
			_, _, recvErr = unix.Recvfrom(int(fd), one[:], unix.MSG_PEEK|unix.MSG_DONTWAIT)
		}))
		require.ErrorIs(t, recvErr, syscall.ECONNRESET)
	})

	t.Run("FIN and half-close remain usable for the fast probe", func(t *testing.T) {
		server, client := tcpConnectionPair(t)
		t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
		tcpServer, tcpClient := server.(*net.TCPConn), client.(*net.TCPConn)
		probe := newTerminalSocketProbe(tcpServer)
		require.NotNil(t, probe)
		require.NoError(t, tcpClient.CloseWrite())
		require.Eventually(t, func() bool { return terminalSocketHasEvent(tcpServer, unix.POLLRDHUP) }, 3*time.Second, time.Millisecond)
		require.False(t, probe.terminal())
		_, err := server.Write([]byte("x"))
		require.NoError(t, err)
		var received [1]byte
		_, err = io.ReadFull(client, received[:])
		require.NoError(t, err)
		require.Equal(t, byte('x'), received[0])
	})

	t.Run("full FIN is not fast-retired", func(t *testing.T) {
		server, client := tcpConnectionPair(t)
		t.Cleanup(func() { _ = server.Close() })
		tcpServer := server.(*net.TCPConn)
		probe := newTerminalSocketProbe(tcpServer)
		require.NoError(t, client.Close())
		require.Eventually(t, func() bool { return terminalSocketHasEvent(tcpServer, unix.POLLRDHUP) }, 3*time.Second, time.Millisecond)
		require.False(t, probe.terminal())
	})

	t.Run("healthy socket with an error queue", func(t *testing.T) {
		server, client := tcpConnectionPair(t)
		t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
		tcpServer := server.(*net.TCPConn)
		raw, err := tcpServer.SyscallConn()
		require.NoError(t, err)
		var optionErr error
		require.NoError(t, raw.Control(func(fd uintptr) {
			optionErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_TIMESTAMPING, unix.SOF_TIMESTAMPING_TX_SOFTWARE|unix.SOF_TIMESTAMPING_SOFTWARE)
		}))
		require.NoError(t, optionErr)
		_, err = server.Write([]byte("a"))
		require.NoError(t, err)
		var received [1]byte
		_, err = io.ReadFull(client, received[:])
		require.NoError(t, err)
		require.Eventually(t, func() bool { return terminalSocketHasEvent(tcpServer, unix.POLLERR) }, 3*time.Second, time.Millisecond)
		probe := newTerminalSocketProbe(tcpServer)
		require.False(t, probe.terminal())
		events, err := terminalSocketEvents(tcpServer)
		require.NoError(t, err)
		require.NotZero(t, events&unix.POLLERR)
		_, err = server.Write([]byte("b"))
		require.NoError(t, err)
		_, err = io.ReadFull(client, received[:])
		require.NoError(t, err)
		require.Equal(t, byte('b'), received[0])
	})

	t.Run("TLS unwrap and unsupported transport", func(t *testing.T) {
		server, client := tcpConnectionPair(t)
		t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
		tlsServer := tls.Server(server, &tls.Config{})
		require.Same(t, server, terminalSocketConn(tlsServer))
		pipe1, pipe2 := net.Pipe()
		t.Cleanup(func() { _ = pipe1.Close(); _ = pipe2.Close() })
		require.Nil(t, terminalSocketConn(pipe1))
		require.Nil(t, terminalSocketConn(nil))
		probe := newTerminalSocketProbe(server.(*net.TCPConn))
		require.NoError(t, server.Close())
		require.False(t, probe.terminal())
	})
}

func TestTerminalConnectionMonitorIgnoresReplacedSocketReset(t *testing.T) {
	oldServer, oldClient := tcpConnectionPair(t)
	newServer, newClient := tcpConnectionPair(t)
	t.Cleanup(func() {
		_ = oldServer.Close()
		_ = oldClient.Close()
		_ = newServer.Close()
		_ = newClient.Close()
	})
	routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	t.Cleanup(routine.cancelRoutineFunc)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	routine.setCancelRequestFunc(cancel)
	require.True(t, routine.setInProcessRequest(true))
	conn := &Conn{conn: oldServer}
	monitor := connectionLivenessMonitor{rm: &RoutineManager{clients: map[*Conn]*Routine{conn: routine}}}
	monitor.scan(time.Now())
	require.NotNil(t, monitor.probes[conn])
	require.NoError(t, oldClient.(*net.TCPConn).SetLinger(0))
	require.NoError(t, oldClient.Close())
	require.Eventually(t, func() bool { return terminalSocketHasEvent(oldServer.(*net.TCPConn), unix.POLLERR) }, 3*time.Second, time.Millisecond)
	conn.UseConn(newServer)
	monitor.scan(time.Now())
	require.NoError(t, ctx.Err(), "the old socket's reset cannot retire its replacement")
	require.NoError(t, newClient.(*net.TCPConn).SetLinger(0))
	require.NoError(t, newClient.Close())
	require.Eventually(t, func() bool { return terminalSocketHasEvent(newServer.(*net.TCPConn), unix.POLLERR) }, 3*time.Second, time.Millisecond)
	monitor.scan(time.Now())
	require.ErrorIs(t, ctx.Err(), context.Canceled, "the replacement's own reset must be detected")
}
