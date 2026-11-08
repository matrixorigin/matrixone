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

func terminalSocketEvents(tcp *net.TCPConn) (int16, error) {
	raw, err := tcp.SyscallConn()
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

func terminalSocketHasEvent(tcp *net.TCPConn, flag int16) bool {
	events, err := terminalSocketEvents(tcp)
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
	probe := newTerminalTCPProbe(terminalTCPConn(tlsServer))
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
	monitor := terminalConnectionMonitor{rm: &RoutineManager{clients: map[*Conn]*Routine{conn: routine}}}
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
	monitor := terminalConnectionMonitor{rm: &RoutineManager{clients: map[*Conn]*Routine{conn: routine}}}
	require.True(t, routine.setInProcessRequest(true))
	monitor.scan(time.Now())
	probe := monitor.probes[conn]
	require.NotNil(t, probe)
	require.True(t, routine.setInProcessRequest(false))
	for i := 0; i < terminalConnectionProbeIdlePasses-1; i++ {
		monitor.scan(time.Now())
	}
	require.True(t, routine.setInProcessRequest(true))
	monitor.scan(time.Now())
	require.Same(t, probe, monitor.probes[conn], "briefly idle connections should reuse the probe")
	require.True(t, routine.setInProcessRequest(false))
	for i := 0; i < terminalConnectionProbeIdlePasses; i++ {
		monitor.scan(time.Now())
	}
	require.Nil(t, monitor.probes, "an empty cache must release its retained buckets")
	require.True(t, routine.setInProcessRequest(true))
	monitor.scan(time.Now())
	require.NotNil(t, monitor.probes[conn], "a later request must recreate its probe")
}

func TestTerminalConnectionMonitorStopsWithRoutineManager(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	server := &MOServer{rm: &RoutineManager{ctx: ctx}}
	server.startTerminalConnectionMonitor()
	cancel()
	stopped := make(chan struct{})
	go func() {
		server.wg.Wait()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("terminal connection monitor did not stop")
	}
}

func TestTerminalConnectionMonitorProgressDuringBlockedLegacyClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	legacy := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	terminal := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	t.Cleanup(legacy.cancelRoutineFunc)
	t.Cleanup(terminal.cancelRoutineFunc)

	legacyCtx, cancelLegacy := context.WithCancel(context.Background())
	terminalCtx, cancelTerminal := context.WithCancel(context.Background())
	t.Cleanup(cancelLegacy)
	t.Cleanup(cancelTerminal)
	legacy.setCancelRequestFunc(cancelLegacy)
	terminal.setCancelRequestFunc(cancelTerminal)
	require.True(t, legacy.setInProcessRequest(true))
	require.True(t, terminal.setInProcessRequest(true))
	now := time.Now()
	legacy.requestStartedAt.Store(clientRequestClockValue(now.Add(-clientDisconnectProbeGrace - time.Second)))
	terminal.requestStartedAt.Store(clientRequestClockValue(now))

	server, client := tcpConnectionPair(t)
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	require.NoError(t, client.(*net.TCPConn).SetLinger(0))
	require.NoError(t, client.Close())
	require.Eventually(t, func() bool {
		return terminalSocketHasEvent(server.(*net.TCPConn), unix.POLLERR)
	}, 3*time.Second, time.Millisecond)

	rm := &RoutineManager{ctx: ctx, clients: map[*Conn]*Routine{
		{remoteAddr: "blocked legacy"}: legacy,
		{conn: server}:                 terminal,
	}}
	mo := &MOServer{rm: rm}
	legacyProbed := make(chan struct{})
	legacyDone := make(chan struct{})
	legacy.mc.Lock()
	var releaseOnce sync.Once
	releaseLegacy := func() { releaseOnce.Do(legacy.mc.Unlock) }
	t.Cleanup(func() {
		releaseLegacy()
		cancel()
		select {
		case <-legacyDone:
		case <-time.After(3 * time.Second):
			t.Error("legacy scan did not finish after releasing the lock")
		}
		stopped := make(chan struct{})
		go func() { mo.wg.Wait(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(3 * time.Second):
			t.Error("terminal monitor did not stop during cleanup")
		}
	})
	go func() {
		defer close(legacyDone)
		rm.cancelDisconnectedRequests(now, clientDisconnectProbeGrace, func(net.Conn) (bool, error) {
			close(legacyProbed)
			return true, nil
		})
	}()
	select {
	case <-legacyProbed:
	case <-time.After(3 * time.Second):
		t.Fatal("legacy scan did not reach the blocked routine")
	}
	select {
	case <-legacyCtx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("legacy close did not reach lifecycle cancellation")
	}
	select {
	case <-legacyDone:
		t.Fatal("legacy close finished while its lifecycle lock was held")
	default:
	}

	mo.startTerminalConnectionMonitor()
	select {
	case <-terminalCtx.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("a blocked legacy close delayed terminal cancellation")
	}
	cancel()
	fastStopped := make(chan struct{})
	go func() { mo.wg.Wait(); close(fastStopped) }()
	select {
	case <-fastStopped:
	case <-time.After(3 * time.Second):
		t.Fatal("terminal monitor did not stop while the legacy close was blocked")
	}

	releaseLegacy()
	select {
	case <-legacyDone:
	case <-time.After(3 * time.Second):
		t.Fatal("legacy scan did not finish after releasing the lock")
	}
	require.ErrorIs(t, legacyCtx.Err(), context.Canceled)
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
		probe := newTerminalTCPProbe(server.(*net.TCPConn))
		require.NotNil(t, probe)
		require.NoError(t, client.(*net.TCPConn).SetLinger(0))
		require.NoError(t, client.Close())
		require.Eventually(t, probe.terminal, 3*time.Second, time.Millisecond)
	})

	t.Run("reset keeps the socket error for the owner", func(t *testing.T) {
		server, client := tcpConnectionPair(t)
		t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
		tcpServer, tcpClient := server.(*net.TCPConn), client.(*net.TCPConn)
		probe := newTerminalTCPProbe(tcpServer)
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
		probe := newTerminalTCPProbe(tcpServer)
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
		probe := newTerminalTCPProbe(tcpServer)
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
		probe := newTerminalTCPProbe(tcpServer)
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
		require.Same(t, server, terminalTCPConn(tlsServer))
		pipe1, pipe2 := net.Pipe()
		t.Cleanup(func() { _ = pipe1.Close(); _ = pipe2.Close() })
		require.Nil(t, terminalTCPConn(pipe1))
		require.Nil(t, terminalTCPConn(nil))
		probe := newTerminalTCPProbe(server.(*net.TCPConn))
		require.NoError(t, server.Close())
		require.False(t, probe.terminal())
	})
}
