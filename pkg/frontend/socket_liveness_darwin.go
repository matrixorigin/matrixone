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

package frontend

import (
	"errors"

	"golang.org/x/sys/unix"
)

// A read EOF alone is also produced by a write-half-close. Inspect TCP state
// only after HUP and leave the socket's pending error and input to its owner.
func terminalSocketFD(fd uintptr, pollFD *[1]unix.PollFd, unixSocket bool) (bool, error) {
	// AF_UNIX HUP semantics have not been established on Darwin.
	if unixSocket {
		return false, nil
	}
	pollFD[0] = unix.PollFd{Fd: int32(fd), Events: unix.POLLIN | unix.POLLHUP}
	if _, err := unix.Poll(pollFD[:], 0); err != nil {
		return false, err
	}
	if pollFD[0].Revents&unix.POLLNVAL != 0 || pollFD[0].Revents&unix.POLLHUP == 0 {
		return false, nil
	}
	info, err := unix.GetsockoptTCPConnectionInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_CONNECTION_INFO)
	if errors.Is(err, unix.ECONNRESET) {
		return true, nil
	}
	const darwinTCPClosed = 0 // TCPS_CLOSED in XNU bsd/netinet/tcp_fsm.h.
	return err == nil && info.State == darwinTCPClosed, err
}
