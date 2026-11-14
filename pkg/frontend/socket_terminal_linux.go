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
	"crypto/tls"
	"net"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/matrixorigin/matrixone/pkg/logutil"
)

const (
	// Bound retained memory even if the number of connections spikes. Entries
	// beyond the cap are still probed, without retaining their probe objects.
	terminalConnectionProbeCacheLimit = 1 << 17
	terminalRequestScratchLimit       = 1 << 17
	// TCP_CLOSE from Linux's include/net/tcp_states.h.
	linuxTCPClosed = 7
)

// terminalTCPProbe holds a non-owning RawConn, never a numeric file
// descriptor. It and its mutable callback state belong to one monitor worker.
type terminalTCPProbe struct {
	tcp     *net.TCPConn
	raw     syscall.RawConn
	control func(uintptr)
	pollFD  [1]unix.PollFd
	err     error
	closed  bool
	seen    bool
}

func terminalTCPConn(conn net.Conn) *net.TCPConn {
	if tlsConn, ok := conn.(*tls.Conn); ok {
		conn = tlsConn.NetConn()
	}
	tcp, _ := conn.(*net.TCPConn)
	return tcp
}

func newTerminalTCPProbe(tcp *net.TCPConn) *terminalTCPProbe {
	if tcp == nil {
		return nil
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		return nil
	}
	probe := &terminalTCPProbe{tcp: tcp, raw: raw}
	probe.control = probe.checkFD
	return probe
}

func (probe *terminalTCPProbe) checkFD(fd uintptr) {
	probe.pollFD[0] = unix.PollFd{Fd: int32(fd), Events: unix.POLLRDHUP}
	_, probe.err = unix.Poll(probe.pollFD[:], 0)
	if probe.err != nil || probe.pollFD[0].Revents&unix.POLLNVAL != 0 {
		return
	}
	// A peer FIN and a write-half-close are indistinguishable here. Both end
	// request admission on this connection, even if input remains unread.
	if probe.pollFD[0].Revents&unix.POLLRDHUP != 0 {
		probe.closed = true
		return
	}
	if probe.pollFD[0].Revents&unix.POLLERR == 0 {
		return
	}
	// POLLERR alone may indicate an error queue on a usable socket. TCP_INFO
	// does not clear the pending socket error, unlike SO_ERROR or recv.
	info, err := unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
	probe.err = err
	probe.closed = err == nil && info.State == linuxTCPClosed
}

func (probe *terminalTCPProbe) terminal() bool {
	probe.err = nil
	probe.closed = false
	return probe.raw.Control(probe.control) == nil && probe.err == nil && probe.closed
}

type livenessRequest struct {
	activeClientRequest
	legacyEligible bool
}

type connectionLivenessMonitor struct {
	rm       *RoutineManager
	requests []livenessRequest
	probes   map[*Conn]*terminalTCPProbe
}

// snapshot records both policies against the ticker time and the same request
// start. The legacy close path can then use its original age selection even if
// the connection begins another request during the terminal pass.
func (monitor *connectionLivenessMonitor) snapshot(now time.Time) {
	monitor.requests = monitor.requests[:0]
	nowValue := clientRequestClockValue(now)
	monitor.rm.mu.RLock()
	defer monitor.rm.mu.RUnlock()
	for conn, routine := range monitor.rm.clients {
		if conn == nil || routine == nil {
			continue
		}
		startedAt := routine.activeRequestStartedAt()
		if startedAt == 0 {
			continue
		}
		monitor.requests = append(monitor.requests, livenessRequest{
			activeClientRequest: activeClientRequest{conn: conn, routine: routine},
			legacyEligible:      nowValue-startedAt >= clientDisconnectProbeGrace.Nanoseconds(),
		})
	}
}

func (monitor *connectionLivenessMonitor) scan(now time.Time) {
	monitor.snapshot(now)
	// A map retains its buckets after delete. Drop an oversized cache after a
	// contraction instead of retaining the peak for the server's lifetime.
	if len(monitor.probes) > 1024 && len(monitor.requests)*4 < len(monitor.probes) {
		monitor.probes = nil
	}
	legacy := monitor.requests[:0]
	for _, request := range monitor.requests {
		tcp := terminalTCPConn(request.conn.RawConn())
		if tcp == nil {
			if request.legacyEligible {
				legacy = append(legacy, request)
			}
			continue
		}
		probe := monitor.probes[request.conn]
		if probe == nil || probe.tcp != tcp {
			probe = newTerminalTCPProbe(tcp)
			if probe == nil {
				continue
			}
			if len(monitor.probes) < terminalConnectionProbeCacheLimit {
				if monitor.probes == nil {
					monitor.probes = make(map[*Conn]*terminalTCPProbe)
				}
				monitor.probes[request.conn] = probe
			}
		}
		probe.seen = true
		if probe.terminal() && request.routine.retireTerminalConnection() {
			delete(monitor.probes, request.conn)
			logutil.Infof("cancel active query after client connection shutdown: connection=%s", request.conn.RemoteAddress())
		}
	}
	for conn, probe := range monitor.probes {
		if !probe.seen {
			delete(monitor.probes, conn)
		} else {
			probe.seen = false
		}
	}
	// Gradual eviction can leave an empty map holding its peak buckets.
	if len(monitor.probes) == 0 {
		monitor.probes = nil
	}

	// Non-TCP transports retain the legacy probe and request-age policy. The
	// close attempt must not hold up future TCP scans when a lifecycle lock is
	// busy; retry inconclusive or contended requests on a later scan.
	clear(monitor.requests[len(legacy):])
	monitor.requests = legacy
	for i := range monitor.requests {
		request := monitor.requests[i].activeClientRequest
		monitor.requests[i] = livenessRequest{}
		closed, err := connectionPeerClosed(request.conn.RawConn())
		if err != nil {
			logutil.Debugf("failed to probe active client connection %s: %v", request.conn.RemoteAddress(), err)
			continue
		}
		if closed && request.routine.tryBeginCloseForRequest(clientRequestClockValue(now), clientDisconnectProbeGrace) {
			logutil.Infof("cancel active query after client disconnect: connection=%s", request.conn.RemoteAddress())
		}
	}
	if cap(monitor.requests) > terminalRequestScratchLimit {
		monitor.requests = nil
	} else {
		monitor.requests = monitor.requests[:0]
	}
}
