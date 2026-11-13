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

import "golang.org/x/sys/unix"

// terminalSocketFD observes a full Unix close or a confirmed TCP reset
// without consuming input or SO_ERROR. TCP POLLERR alone can also mean a
// healthy connection has an unrelated error queue.
func terminalSocketFD(fd uintptr, pollFD *[1]unix.PollFd, unixSocket bool) (bool, error) {
	pollFD[0] = unix.PollFd{Fd: int32(fd)}
	if _, err := unix.Poll(pollFD[:], 0); err != nil {
		return false, err
	}
	if pollFD[0].Revents&unix.POLLNVAL != 0 {
		return false, nil
	}
	if unixSocket {
		// Linux AF_UNIX reports HUP on a full peer close, but not SHUT_WR.
		return pollFD[0].Revents&unix.POLLHUP != 0, nil
	}
	if pollFD[0].Revents&unix.POLLERR == 0 {
		return false, nil
	}
	info, err := unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
	const linuxTCPClosed = 7 // TCP_CLOSE in Linux include/net/tcp_states.h.
	return err == nil && info.State == linuxTCPClosed, err
}
