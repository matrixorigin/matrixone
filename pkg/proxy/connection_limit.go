// Copyright 2021 - 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"encoding/binary"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

var errProxyConnectionLimit = moerr.NewInvalidInputNoCtx("proxy connection limit exceeded")

// connectionLimiter provides two-stage admission. The global slot is acquired
// before allocating per-connection protocol state. The tenant slot is bound
// only after a backend or cached authenticator accepts the login credentials;
// the tenant name in an unauthenticated packet is not an admission identity.
type connectionLimiter struct {
	mu           sync.Mutex
	maxTotal     int
	maxPerTenant int
	total        int
	byTenant     map[Tenant]int
}

type connectionLease struct {
	limiter  *connectionLimiter
	tenant   Tenant
	bound    bool
	released bool
}

// connectionAdmissionListener keeps sockets that have not acquired a global
// connection slot out of Goetty. Accept is the last boundary before Goetty
// materializes an IOSession, inserts it into its session map, and starts a
// handler goroutine, so admission must happen here rather than in the handler.
type connectionAdmissionListener struct {
	net.Listener
	limiter *connectionLimiter
	reject  func(net.Conn)
}

func newConnectionAdmissionListener(
	listener net.Listener,
	limiter *connectionLimiter,
	reject func(net.Conn),
) net.Listener {
	return &connectionAdmissionListener{
		Listener: listener,
		limiter:  limiter,
		reject:   reject,
	}
}

func (l *connectionAdmissionListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		lease, ok := l.limiter.acquire()
		if ok {
			return &connectionAdmissionConn{Conn: conn, lease: lease}, nil
		}
		if l.reject != nil {
			l.reject(conn)
		} else {
			writeConnectionLimitError(conn)
		}
		_ = conn.Close()
	}
}

// connectionAdmissionConn owns the listener-acquired lease until the handler
// atomically takes it. If Goetty fails before starting the handler, IOSession
// cleanup closes this connection and releases the lease. Once taken, the
// handler's existing defer is the sole release owner.
type connectionAdmissionConn struct {
	net.Conn
	mu    sync.Mutex
	lease *connectionLease
}

func (c *connectionAdmissionConn) takeAdmission() *connectionLease {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	lease := c.lease
	c.lease = nil
	return lease
}

func (c *connectionAdmissionConn) Close() error {
	lease := c.takeAdmission()
	if lease != nil {
		lease.release()
	}
	if c == nil || c.Conn == nil {
		return nil
	}
	return c.Conn.Close()
}

func takeConnectionAdmission(conn net.Conn) (*connectionLease, bool) {
	admitted, ok := conn.(*connectionAdmissionConn)
	if !ok {
		return nil, false
	}
	return admitted.takeAdmission(), true
}

func newConnectionLimiter(maxTotal, maxPerTenant int) *connectionLimiter {
	return &connectionLimiter{
		maxTotal:     maxTotal,
		maxPerTenant: maxPerTenant,
		byTenant:     make(map[Tenant]int),
	}
}

func (l *connectionLimiter) acquire() (*connectionLease, bool) {
	if l == nil {
		return nil, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.total >= l.maxTotal {
		return nil, false
	}
	l.total++
	return &connectionLease{limiter: l}, true
}

func (l *connectionLease) bindTenant(tenant Tenant) bool {
	if l == nil || l.limiter == nil {
		return false
	}
	tenant = Tenant(strings.ToLower(string(tenant)))
	limiter := l.limiter
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	if l.released {
		return false
	}
	if l.bound {
		return l.tenant == tenant
	}
	if limiter.byTenant[tenant] >= limiter.maxPerTenant {
		return false
	}
	limiter.byTenant[tenant]++
	l.tenant = tenant
	l.bound = true
	return true
}

// release is idempotent so error cleanup and normal cleanup may safely
// converge without counter underflow.
func (l *connectionLease) release() {
	if l == nil || l.limiter == nil {
		return
	}
	limiter := l.limiter
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	if l.released {
		return
	}
	l.released = true
	limiter.total--
	if !l.bound {
		return
	}
	remaining := limiter.byTenant[l.tenant] - 1
	if remaining == 0 {
		delete(limiter.byTenant, l.tenant)
	} else {
		limiter.byTenant[l.tenant] = remaining
	}
}

// writeConnectionLimitError writes a protocol-level MySQL error before any
// handshake allocation. The write is bounded because this path exists to shed
// load and must never become another unbounded resource wait.
func writeConnectionLimitError(conn net.Conn) {
	if conn == nil {
		return
	}
	definition := moerr.MysqlErrorMsgRefer[moerr.ER_CON_COUNT_ERROR]
	message := []byte(definition.ErrorMsgOrFormat)
	payloadLength := 1 + 2 + 1 + 5 + len(message)
	packet := make([]byte, 4+payloadLength)
	packet[0] = byte(payloadLength)
	packet[1] = byte(payloadLength >> 8)
	packet[2] = byte(payloadLength >> 16)
	packet[3] = 0
	packet[4] = 0xff
	binary.LittleEndian.PutUint16(packet[5:7], definition.ErrorCode)
	packet[7] = '#'
	copy(packet[8:13], definition.SqlStates[0])
	copy(packet[13:], message)

	if err := conn.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		return
	}
	for len(packet) > 0 {
		n, err := conn.Write(packet)
		if err != nil || n == 0 {
			return
		}
		packet = packet[n:]
	}
}
