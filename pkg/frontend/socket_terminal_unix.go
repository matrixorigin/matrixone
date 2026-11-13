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

//go:build darwin || linux

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
)

// terminalSocketProbe holds a non-owning RawConn, never a numeric file
// descriptor. It and its mutable callback state belong to one monitor worker.
type terminalSocketProbe struct {
	conn    net.Conn
	isUnix  bool
	raw     syscall.RawConn
	control func(uintptr)
	pollFD  [1]unix.PollFd
	err     error
	closed  bool
	seen    bool
}

func terminalSocketConn(conn net.Conn) net.Conn {
	if tlsConn, ok := conn.(*tls.Conn); ok {
		if tlsConn == nil {
			return nil
		}
		conn = tlsConn.NetConn()
	}
	switch c := conn.(type) {
	case *net.TCPConn:
		if c != nil {
			return c
		}
	case *net.UnixConn:
		if c != nil {
			return c
		}
	}
	return nil
}

func newTerminalSocketProbe(conn net.Conn) *terminalSocketProbe {
	conn = terminalSocketConn(conn)
	if conn == nil {
		return nil
	}
	raw, err := conn.(syscall.Conn).SyscallConn()
	if err != nil {
		return nil
	}
	_, isUnix := conn.(*net.UnixConn)
	probe := &terminalSocketProbe{conn: conn, isUnix: isUnix, raw: raw}
	probe.control = probe.checkFD
	return probe
}

func (probe *terminalSocketProbe) checkFD(fd uintptr) {
	probe.closed, probe.err = terminalSocketFD(fd, &probe.pollFD, probe.isUnix)
}

func (probe *terminalSocketProbe) terminal() bool {
	probe.err = nil
	probe.closed = false
	return probe.raw.Control(probe.control) == nil && probe.err == nil && probe.closed
}

type connectionLivenessMonitor struct {
	rm       *RoutineManager
	requests []activeClientRequest
	probes   map[*Conn]*terminalSocketProbe
}

// snapshot records active requests without holding the manager lock while probing.
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
		if startedAt == 0 || nowValue-startedAt < 0 {
			continue
		}
		monitor.requests = append(monitor.requests, activeClientRequest{conn: conn, routine: routine})
	}
}

func (monitor *connectionLivenessMonitor) scan(now time.Time) {
	monitor.snapshot(now)
	// A map retains its buckets after delete. Drop an oversized cache after a
	// contraction instead of retaining the peak for the server's lifetime.
	if len(monitor.probes) > 1024 && len(monitor.requests)*4 < len(monitor.probes) {
		monitor.probes = nil
	}
	for _, request := range monitor.requests {
		socket := terminalSocketConn(request.conn.RawConn())
		if socket == nil {
			continue
		}
		probe := monitor.probes[request.conn]
		if probe == nil || probe.conn != socket {
			probe = newTerminalSocketProbe(socket)
			if probe == nil {
				continue
			}
			if len(monitor.probes) < terminalConnectionProbeCacheLimit {
				if monitor.probes == nil {
					monitor.probes = make(map[*Conn]*terminalSocketProbe)
				}
				monitor.probes[request.conn] = probe
			}
		}
		probe.seen = true
		if probe.terminal() && request.routine.retireTerminalConnection() {
			delete(monitor.probes, request.conn)
			logutil.Infof("cancel active query after terminal client connection error: connection=%s", request.conn.RemoteAddress())
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

	// Do not retain sessions after the pass, including skipped probes.
	clear(monitor.requests)
	if cap(monitor.requests) > terminalRequestScratchLimit {
		monitor.requests = nil
	} else {
		monitor.requests = monitor.requests[:0]
	}
}
