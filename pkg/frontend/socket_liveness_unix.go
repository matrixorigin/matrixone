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
	"errors"
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

// socketLivenessProbe caches a non-owning RawConn and bounded callback storage.
// RoutineManager serializes monitor passes, so the mutable probe state is never
// used concurrently. RawConn.Control pins the descriptor against close/reuse.
type socketLivenessProbe struct {
	rawConn    syscall.RawConn
	initErr    error
	pollFDs    [1]unix.PollFd
	peek       [1]byte
	n          int
	peerClosed bool
	probeErr   error
	control    func(uintptr)
}

func newSocketLivenessProbe(conn net.Conn) *socketLivenessProbe {
	probe := &socketLivenessProbe{}
	if tlsConn, ok := conn.(*tls.Conn); ok {
		conn = tlsConn.NetConn()
	}
	if syscallConn, ok := conn.(syscall.Conn); ok {
		probe.rawConn, probe.initErr = syscallConn.SyscallConn()
		if probe.initErr == nil {
			probe.control = probe.probeFD
		}
	}
	return probe
}

// Only interpret transport closure, never protocol or encrypted TLS bytes.
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
	probe.n = -1
	probe.peerClosed = false
	probe.probeErr = nil
	probe.pollFDs[0].Revents = 0
	if err := probe.rawConn.Control(probe.control); err != nil {
		return false, err
	}
	if probe.peerClosed {
		return true, nil
	}
	if probe.probeErr == nil {
		return probe.n == 0, nil
	}
	if errors.Is(probe.probeErr, unix.EAGAIN) ||
		errors.Is(probe.probeErr, unix.EWOULDBLOCK) ||
		errors.Is(probe.probeErr, unix.EINTR) {
		return false, nil
	}
	if errors.Is(probe.probeErr, unix.ECONNRESET) ||
		errors.Is(probe.probeErr, unix.ENOTCONN) ||
		errors.Is(probe.probeErr, unix.EBADF) {
		return true, nil
	}
	return false, probe.probeErr
}

func (probe *socketLivenessProbe) probeFD(fd uintptr) {
	probe.pollFDs[0] = unix.PollFd{
		Fd:     int32(fd),
		Events: unix.POLLIN | unix.POLLHUP | unix.POLLERR | socketReadHangupPollEvent(),
	}
	if _, probe.probeErr = unix.Poll(probe.pollFDs[:], 0); probe.probeErr != nil {
		return
	}
	revents := probe.pollFDs[0].Revents
	if revents&(unix.POLLHUP|unix.POLLERR|unix.POLLNVAL|socketReadHangupPollEvent()) != 0 {
		probe.peerClosed = true
		return
	}
	if revents&unix.POLLIN == 0 {
		return
	}
	probe.n, _, probe.probeErr = unix.Recvfrom(int(fd), probe.peek[:], unix.MSG_PEEK|unix.MSG_DONTWAIT)
}

func connectionPeerClosed(conn *Conn) (bool, error) {
	if conn == nil {
		return false, nil
	}
	return conn.livenessProbe.Load().connectionPeerClosed()
}
