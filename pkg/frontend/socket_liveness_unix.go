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

// connectionPeerClosed checks the socket read side without consuming protocol
// bytes. For TLS, peeking at the underlying encrypted stream is sufficient:
// only EOF/error is interpreted, never payload.
func connectionPeerClosed(conn net.Conn) (bool, error) {
	if conn == nil {
		return true, nil
	}
	if tlsConn, ok := conn.(*tls.Conn); ok {
		conn = tlsConn.NetConn()
	}
	syscallConn, ok := conn.(syscall.Conn)
	if !ok {
		return false, nil
	}
	rawConn, err := syscallConn.SyscallConn()
	if err != nil {
		return false, err
	}

	var (
		n          int
		peerClosed bool
		probeErr   error
	)
	if err = rawConn.Control(func(fd uintptr) {
		pollFDs := []unix.PollFd{{
			Fd:     int32(fd),
			Events: unix.POLLIN | unix.POLLHUP | unix.POLLERR | socketReadHangupPollEvent(),
		}}
		if _, probeErr = unix.Poll(pollFDs, 0); probeErr != nil {
			return
		}
		if pollFDs[0].Revents&(unix.POLLHUP|unix.POLLERR|unix.POLLNVAL|socketReadHangupPollEvent()) != 0 {
			peerClosed = true
			return
		}
		var one [1]byte
		n, _, probeErr = unix.Recvfrom(int(fd), one[:], unix.MSG_PEEK|unix.MSG_DONTWAIT)
	}); err != nil {
		return false, err
	}
	if peerClosed {
		return true, nil
	}
	if probeErr == nil {
		return n == 0, nil
	}
	if errors.Is(probeErr, unix.EAGAIN) ||
		errors.Is(probeErr, unix.EWOULDBLOCK) ||
		errors.Is(probeErr, unix.EINTR) {
		return false, nil
	}
	if errors.Is(probeErr, unix.ECONNRESET) ||
		errors.Is(probeErr, unix.ENOTCONN) ||
		errors.Is(probeErr, unix.EBADF) {
		return true, nil
	}
	return false, probeErr
}
