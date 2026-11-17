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
	"fmt"
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
	require.True(t, probe.terminal(), "a TLS transport FIN must trigger retirement")
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
	require.True(t, routine.setInProcessRequest(true))
	now := time.Now()
	routine.requestStartedAt.Store(clientRequestClockValue(now.Add(-time.Second)))
	conn := &Conn{conn: oldServer}
	monitor := connectionLivenessMonitor{
		rm:     &RoutineManager{clients: make(map[*Conn]*Routine)},
		probes: make(map[*Conn]*terminalTCPProbe, terminalConnectionProbeCacheLimit),
	}
	monitor.rm.clients[conn] = routine
	monitor.probes[conn] = newTerminalTCPProbe(oldServer.(*net.TCPConn))
	unused := &terminalTCPProbe{}
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
	recentConn, recent := client(now.Add(-clientDisconnectProbeGrace+time.Nanosecond), false)
	exactConn, exact := client(now.Add(-clientDisconnectProbeGrace), false)
	oldConn, old := client(now.Add(-clientDisconnectProbeGrace-time.Second), false)
	futureConn, future := client(now.Add(time.Nanosecond), false)
	idleConn, idle := client(time.Time{}, false)
	closingConn, closing := client(now.Add(-time.Minute), true)
	monitor := connectionLivenessMonitor{rm: &RoutineManager{clients: map[*Conn]*Routine{
		recentConn: recent, exactConn: exact, oldConn: old,
		futureConn: future, idleConn: idle, closingConn: closing,
		nil: old, {}: nil,
	}}}
	monitor.snapshot(now)
	eligible := make(map[*Conn]bool, len(monitor.requests))
	for _, request := range monitor.requests {
		eligible[request.conn] = request.legacyEligible
	}
	require.Len(t, eligible, 4)
	require.Contains(t, eligible, recentConn)
	require.False(t, eligible[recentConn])
	require.True(t, eligible[exactConn])
	require.True(t, eligible[oldConn])
	require.False(t, eligible[futureConn], "a request started after the tick is still eligible for the fast pass")

	// The old monitor also selected by the ticker time before probing sockets.
	old.requestStartedAt.Store(clientRequestClockValue(now))
	for _, request := range monitor.requests {
		if request.conn == oldConn {
			require.True(t, request.legacyEligible)
		}
	}
}

func TestConnectionLivenessMonitorCancelsFINWithoutGrace(t *testing.T) {
	server, client := tcpConnectionPair(t)
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	t.Cleanup(routine.cancelRoutineFunc)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	routine.setCancelRequestFunc(cancel)
	require.True(t, routine.setInProcessRequest(true))
	now := time.Now()
	// The queued tick can precede admission of the active request.
	routine.requestStartedAt.Store(clientRequestClockValue(now.Add(time.Nanosecond)))
	monitor := connectionLivenessMonitor{rm: &RoutineManager{clients: map[*Conn]*Routine{
		{conn: server}: routine,
	}}}
	_, err := client.Write([]byte("xy"))
	require.NoError(t, err)
	require.NoError(t, client.(*net.TCPConn).CloseWrite())
	require.Eventually(t, func() bool {
		return terminalSocketHasEvent(server.(*net.TCPConn), unix.POLLRDHUP)
	}, 3*time.Second, time.Millisecond)
	monitor.scan(now)
	require.ErrorIs(t, ctx.Err(), context.Canceled, "FIN must cancel a recent active request")
	require.True(t, routine.isTerminalRetired())
	require.False(t, routine.setInProcessRequest(true), "a retired connection cannot admit buffered requests")
	var pending [2]byte
	_, err = io.ReadFull(server, pending[:])
	require.NoError(t, err)
	require.Equal(t, "xy", string(pending[:]), "the FIN probe must not consume queued input")
}

func TestConnectionLivenessMonitorRetriesBusyLegacyWithoutBlockingTCP(t *testing.T) {
	legacy := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	terminal := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	late := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	t.Cleanup(legacy.cancelRoutineFunc)
	t.Cleanup(terminal.cancelRoutineFunc)
	t.Cleanup(late.cancelRoutineFunc)

	legacyCtx, cancelLegacy := context.WithCancel(context.Background())
	terminalCtx, cancelTerminal := context.WithCancel(context.Background())
	lateCtx, cancelLate := context.WithCancel(context.Background())
	t.Cleanup(cancelLegacy)
	t.Cleanup(cancelTerminal)
	t.Cleanup(cancelLate)
	legacy.setCancelRequestFunc(cancelLegacy)
	terminal.setCancelRequestFunc(cancelTerminal)
	late.setCancelRequestFunc(cancelLate)
	require.True(t, legacy.setInProcessRequest(true))
	require.True(t, terminal.setInProcessRequest(true))
	require.True(t, late.setInProcessRequest(true))
	now := time.Now()
	legacy.requestStartedAt.Store(clientRequestClockValue(now.Add(-clientDisconnectProbeGrace - time.Second)))
	terminal.requestStartedAt.Store(clientRequestClockValue(now.Add(-time.Second)))
	late.requestStartedAt.Store(clientRequestClockValue(now.Add(-time.Second)))
	unsupported := &Routine{}
	invalid := &Routine{}
	unsupported.requestStartedAt.Store(clientRequestClockValue(now.Add(-clientDisconnectProbeGrace - time.Second)))
	invalid.requestStartedAt.Store(clientRequestClockValue(now.Add(-clientDisconnectProbeGrace - time.Second)))
	pipeServer, pipeClient := net.Pipe()
	t.Cleanup(func() { _ = pipeServer.Close(); _ = pipeClient.Close() })
	invalidTCP := &net.TCPConn{}
	require.Nil(t, newTerminalTCPProbe(invalidTCP))

	terminalServer, terminalClient := tcpConnectionPair(t)
	lateServer, lateClient := tcpConnectionPair(t)
	t.Cleanup(func() {
		_ = terminalServer.Close()
		_ = terminalClient.Close()
		_ = lateServer.Close()
		_ = lateClient.Close()
	})
	require.NoError(t, terminalClient.(*net.TCPConn).SetLinger(0))
	require.NoError(t, terminalClient.Close())
	require.Eventually(t, func() bool {
		return terminalSocketHasEvent(terminalServer.(*net.TCPConn), unix.POLLERR)
	}, 3*time.Second, time.Millisecond)

	legacyConn := &Conn{remoteAddr: "blocked legacy"}
	unsupportedConn := &Conn{conn: pipeServer}
	invalidConn := &Conn{conn: invalidTCP}
	monitor := connectionLivenessMonitor{rm: &RoutineManager{clients: map[*Conn]*Routine{
		legacyConn:             legacy,
		unsupportedConn:        unsupported,
		invalidConn:            invalid,
		{conn: terminalServer}: terminal,
		{conn: lateServer}:     late,
	}}}
	legacy.mc.Lock()
	var releaseOnce sync.Once
	releaseLegacy := func() { releaseOnce.Do(legacy.mc.Unlock) }
	done := make(chan struct{})
	t.Cleanup(func() {
		releaseLegacy()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("liveness pass did not finish after releasing the lock")
		}
	})
	go func() {
		defer close(done)
		monitor.scan(now)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("a busy legacy lifecycle lock blocked the monitor")
	}
	require.NoError(t, legacyCtx.Err(), "a contended close must be retried")
	require.ErrorIs(t, terminalCtx.Err(), context.Canceled)
	require.NoError(t, lateCtx.Err())
	for _, request := range monitor.requests[len(monitor.requests):cap(monitor.requests)] {
		require.Empty(t, request, "ineligible snapshot slots must release references")
	}

	require.NoError(t, lateClient.(*net.TCPConn).SetLinger(0))
	require.NoError(t, lateClient.Close())
	require.Eventually(t, func() bool {
		return terminalSocketHasEvent(lateServer.(*net.TCPConn), unix.POLLERR)
	}, 3*time.Second, time.Millisecond)
	monitor.scan(now)
	require.ErrorIs(t, lateCtx.Err(), context.Canceled, "later TCP scans must progress while the legacy lock is busy")
	require.NoError(t, legacyCtx.Err())
	releaseLegacy()
	monitor.scan(now)
	require.ErrorIs(t, legacyCtx.Err(), context.Canceled, "a contended legacy close must be retried")
}

func TestConnectionLivenessMonitorUnixLegacyAgeAndRetry(t *testing.T) {
	addr := fmt.Sprintf("@mo-32436-%d", time.Now().UnixNano())
	listener, err := net.Listen("unix", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	client, err := net.Dial("unix", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	server, err := listener.Accept()
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	raw, err := server.(syscall.Conn).SyscallConn()
	require.NoError(t, err)
	_, err = client.Write([]byte("xy"))
	require.NoError(t, err)
	require.NoError(t, client.Close())
	require.Eventually(t, func() bool {
		var events int16
		var pollErr error
		err := raw.Control(func(fd uintptr) {
			fds := [1]unix.PollFd{{Fd: int32(fd)}}
			_, pollErr = unix.Poll(fds[:], 0)
			events = fds[0].Revents
		})
		return err == nil && pollErr == nil && events&unix.POLLHUP != 0
	}, 3*time.Second, time.Millisecond)

	routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	t.Cleanup(routine.cancelRoutineFunc)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	routine.setCancelRequestFunc(cancel)
	require.True(t, routine.setInProcessRequest(true))
	now := time.Now()
	routine.requestStartedAt.Store(clientRequestClockValue(now.Add(-clientDisconnectProbeGrace + time.Nanosecond)))
	monitor := connectionLivenessMonitor{rm: &RoutineManager{clients: map[*Conn]*Routine{{conn: server}: routine}}}
	monitor.scan(now)
	require.NoError(t, ctx.Err(), "non-TCP transports retain the existing age policy")

	routine.mc.Lock()
	var unlockOnce sync.Once
	unlock := func() { unlockOnce.Do(routine.mc.Unlock) }
	t.Cleanup(unlock)
	done := make(chan struct{})
	go func() { defer close(done); monitor.scan(now.Add(time.Nanosecond)) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		unlock()
		<-done
		t.Fatal("a busy Unix lifecycle lock blocked the scan")
	}
	require.NoError(t, ctx.Err())
	unlock()
	monitor.scan(now.Add(time.Nanosecond))
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.False(t, routine.isTerminalRetired(), "Unix connections use the legacy close policy")
	var pending [2]byte
	_, err = io.ReadFull(server, pending[:])
	require.NoError(t, err)
	require.Equal(t, "xy", string(pending[:]), "the legacy probe must not consume queued input")
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

	t.Run("write half-close is terminal but leaves the response direction usable", func(t *testing.T) {
		server, client := tcpConnectionPair(t)
		t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
		tcpServer, tcpClient := server.(*net.TCPConn), client.(*net.TCPConn)
		probe := newTerminalTCPProbe(tcpServer)
		require.NotNil(t, probe)
		require.NoError(t, tcpClient.CloseWrite())
		require.Eventually(t, func() bool { return terminalSocketHasEvent(tcpServer, unix.POLLRDHUP) }, 3*time.Second, time.Millisecond)
		require.True(t, probe.terminal())
		_, err := server.Write([]byte("x"))
		require.NoError(t, err)
		var received [1]byte
		_, err = io.ReadFull(client, received[:])
		require.NoError(t, err)
		require.Equal(t, byte('x'), received[0])
	})

	t.Run("full FIN is fast-retired", func(t *testing.T) {
		server, client := tcpConnectionPair(t)
		t.Cleanup(func() { _ = server.Close() })
		tcpServer := server.(*net.TCPConn)
		probe := newTerminalTCPProbe(tcpServer)
		require.NoError(t, client.Close())
		require.Eventually(t, func() bool { return terminalSocketHasEvent(tcpServer, unix.POLLRDHUP) }, 3*time.Second, time.Millisecond)
		events, err := terminalSocketEvents(tcpServer)
		require.NoError(t, err)
		require.Zero(t, events&unix.POLLERR, "this control must be an orderly FIN, not a reset")
		require.True(t, probe.terminal())
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
		routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
		t.Cleanup(routine.cancelRoutineFunc)
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		routine.setCancelRequestFunc(cancel)
		require.True(t, routine.setInProcessRequest(true))
		now := time.Now()
		routine.requestStartedAt.Store(clientRequestClockValue(now.Add(-clientDisconnectProbeGrace - time.Second)))
		monitor := connectionLivenessMonitor{rm: &RoutineManager{clients: map[*Conn]*Routine{{conn: server}: routine}}}
		monitor.scan(now)
		require.NoError(t, ctx.Err(), "a healthy TCP error queue must not fall through to the legacy probe")
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
