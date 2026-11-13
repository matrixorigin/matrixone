// Copyright 2021 - 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build darwin || linux

package frontend

import (
	"crypto/tls"
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

// socketLivenessProbe caches the non-owning RawConn handle and callback state
// for one frontend connection. RoutineManager serializes monitor passes, so a
// probe is never used concurrently and the hot path needs no per-probe lock or
// allocation.
type socketLivenessProbe struct {
	rawConn    syscall.RawConn
	initErr    error
	pollFDs    [1]unix.PollFd
	peerClosed bool
	unixSocket bool
	probeErr   error
	control    func(uintptr)
}

func newSocketLivenessProbe(conn net.Conn) *socketLivenessProbe {
	probe := &socketLivenessProbe{}
	if conn == nil {
		return probe
	}
	if tlsConn, ok := conn.(*tls.Conn); ok {
		if tlsConn == nil {
			return probe
		}
		conn = tlsConn.NetConn()
	}
	// Known transports only: TLS can wrap either directly, or retain a
	// protocol prefix in a bufferedNetConn during the upgrade.
	transport := conn
	if buffered, ok := transport.(*bufferedNetConn); ok {
		if buffered == nil {
			return probe
		}
		transport = buffered.Conn
	}
	switch c := transport.(type) {
	case *net.TCPConn:
		if c == nil {
			return probe
		}
	case *net.UnixConn:
		if c == nil {
			return probe
		}
		probe.unixSocket = true
	default:
		return probe
	}
	syscallConn, ok := conn.(syscall.Conn)
	if !ok {
		return probe
	}
	probe.rawConn, probe.initErr = syscallConn.SyscallConn()
	if probe.initErr == nil {
		probe.control = probe.probeFD
	}
	return probe
}

// connectionPeerClosed reports only a confirmed terminal transport. A read
// EOF or FIN alone does not mean the peer cannot receive the response.
func (probe *socketLivenessProbe) connectionPeerClosed() (bool, error) {
	if probe == nil {
		return false, nil
	}
	if probe.initErr != nil {
		return false, probe.initErr
	}
	if probe.rawConn == nil {
		return false, nil
	}
	probe.peerClosed = false
	probe.probeErr = nil
	if err := probe.rawConn.Control(probe.control); err != nil {
		return false, err
	}
	return probe.peerClosed, probe.probeErr
}

func (probe *socketLivenessProbe) probeFD(fd uintptr) {
	probe.peerClosed, probe.probeErr = terminalSocketFD(fd, &probe.pollFDs, probe.unixSocket)
}

func rawConnectionPeerClosed(conn net.Conn) (bool, error) {
	return newSocketLivenessProbe(conn).connectionPeerClosed()
}

func connectionPeerClosed(conn *Conn) (bool, error) {
	if conn == nil {
		return false, nil
	}
	if conn.livenessProbe == nil {
		return rawConnectionPeerClosed(conn.RawConn())
	}
	return conn.livenessProbe.connectionPeerClosed()
}
