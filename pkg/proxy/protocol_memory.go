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

package proxy

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"golang.org/x/sync/semaphore"
)

// protocolMemoryBudget separates long-lived memory, which is reserved by
// connection admission, from short-lived phase overlap. The minimum transient
// headroom is deliberately sized for the larger of:
//
//   - initial login forwarding: pipelined client prefix + dynamic backend write
//   - migration/control: one additional backend session + dynamic login or
//     captured control-statement write
//
// The shared allocator remains the byte-level hard limit. Weighted transient
// leases prevent individually valid phase transitions from racing for the same
// unreserved headroom. One backend operation is reserved for the background
// lane so a long-running UPGRADE cannot consume login and migration capacity.
type protocolMemoryBudget struct {
	steadyBytes       uint64
	headroomBytes     uint64
	transientBytes    uint64
	backgroundBytes   uint64
	initialBytes      uint64
	backendBytes      uint64
	loginDynamicBytes uint64
}

func calculateProtocolMemoryBudget(c *Config) (protocolMemoryBudget, error) {
	maxConnections := c.MaxConnections
	if maxConnections == 0 {
		maxConnections = defaultMaxConnections
	}
	if maxConnections < 0 {
		return protocolMemoryBudget{}, moerr.NewInternalErrorNoCtx(
			"proxy max-connections must be positive")
	}

	handshakeLimit := uint64(c.ClientHandshakePacketLimit)
	if handshakeLimit == 0 {
		handshakeLimit = uint64(defaultClientHandshakePacketLimit)
	}
	proxyBodyLimit := uint64(c.ProxyProtocolBodyLimit)
	if proxyBodyLimit == 0 {
		proxyBodyLimit = uint64(defaultProxyProtocolBodyLimit)
	}
	memoryLimit := uint64(c.ProtocolMemoryLimit)
	if memoryLimit == 0 {
		memoryLimit = uint64(defaultProtocolMemoryLimit)
	}
	if memoryLimit > math.MaxInt64 {
		return protocolMemoryBudget{}, protocolMemoryConfigOverflow()
	}

	cachedSessions := uint64(0)
	if c.ConnCacheEnabled && c.Plugin == nil {
		cachedSessions = defaultMaxNumTotal
	}
	connections := uint64(maxConnections)
	fixedSessions, ok := checkedMulUint64(connections, 2)
	if !ok {
		return protocolMemoryBudget{}, protocolMemoryConfigOverflow()
	}
	fixedSessions, ok = checkedAddUint64(fixedSessions, cachedSessions)
	if !ok {
		return protocolMemoryBudget{}, protocolMemoryConfigOverflow()
	}
	fixedBytes, ok := checkedMulUint64(fixedSessions, proxyIOSessionBufferSize)
	if !ok {
		return protocolMemoryBudget{}, protocolMemoryConfigOverflow()
	}
	// Before authentication, one Goetty read may contain both a complete PROXY
	// frame and the following MySQL login. After authentication, the retained
	// login copy replaces that input. Reserve the larger pre-auth state per
	// admitted connection so the independent wire limits still compose into a
	// global memory bound.
	proxyFrameBytes, ok := checkedAddUint64(ProxyHeaderLength, proxyBodyLimit)
	if !ok {
		return protocolMemoryBudget{}, protocolMemoryConfigOverflow()
	}
	preAuthBytes, ok := checkedAddUint64(proxyFrameBytes, handshakeLimit)
	if !ok {
		return protocolMemoryBudget{}, protocolMemoryConfigOverflow()
	}
	retainedLoginBytes, ok := checkedMulUint64(connections, preAuthBytes)
	if !ok {
		return protocolMemoryBudget{}, protocolMemoryConfigOverflow()
	}
	backendSessions, ok := checkedAddUint64(connections, cachedSessions)
	if !ok {
		return protocolMemoryBudget{}, protocolMemoryConfigOverflow()
	}
	retainedBackendResponseBytes, ok := checkedMulUint64(
		backendSessions,
		proxyBackendRetainedResponseLimit,
	)
	if !ok {
		return protocolMemoryBudget{}, protocolMemoryConfigOverflow()
	}
	steadyBytes, ok := checkedAddUint64(fixedBytes, retainedLoginBytes)
	if !ok {
		return protocolMemoryBudget{}, protocolMemoryConfigOverflow()
	}
	steadyBytes, ok = checkedAddUint64(steadyBytes, retainedBackendResponseBytes)
	if !ok {
		return protocolMemoryBudget{}, protocolMemoryConfigOverflow()
	}

	loginDynamicBytes, ok := dynamicProtocolWriteBytes(handshakeLimit)
	if !ok {
		return protocolMemoryBudget{}, protocolMemoryConfigOverflow()
	}
	// Only complete statements inside msgBuf's fixed event window are captured
	// for migration/control replay. Larger client packets are forwarded without
	// becoming events, so they cannot reach ServerConn.ExecStmt here.
	controlDynamicBytes, ok := dynamicProtocolWriteBytes(defaultBufLen + defaultExtraBufLen)
	if !ok {
		return protocolMemoryBudget{}, protocolMemoryConfigOverflow()
	}
	initialBytes, ok := checkedAddUint64(handshakeLimit, loginDynamicBytes)
	if !ok {
		return protocolMemoryBudget{}, protocolMemoryConfigOverflow()
	}
	initialBytes, ok = checkedAddUint64(initialBytes, proxyBackendPacketLimit)
	if !ok {
		return protocolMemoryBudget{}, protocolMemoryConfigOverflow()
	}
	backendBytes, ok := checkedAddUint64(
		proxyIOSessionBufferSize,
		max(loginDynamicBytes, controlDynamicBytes),
	)
	if !ok {
		return protocolMemoryBudget{}, protocolMemoryConfigOverflow()
	}
	// frontend.Conn returns a payload allocation and packetToBytes builds the
	// header-bearing response consumed by the event path. Account both while
	// they overlap; successful authentication is covered by the smaller initial
	// response cap and this is deliberately conservative for control results.
	backendReadBytes, ok := checkedMulUint64(2, proxyBackendPacketLimit)
	if !ok {
		return protocolMemoryBudget{}, protocolMemoryConfigOverflow()
	}
	backendBytes, ok = checkedAddUint64(backendBytes, backendReadBytes)
	if !ok {
		return protocolMemoryBudget{}, protocolMemoryConfigOverflow()
	}
	criticalBytes := max(initialBytes, backendBytes)
	// UPGRADE may legitimately run for the client lifetime. Reserve it a
	// separate single-operation lane instead of imposing an arbitrary SQL
	// timeout or allowing it to starve login, migration and kill operations.
	transientBytes, ok := checkedAddUint64(criticalBytes, backendBytes)
	if !ok {
		return protocolMemoryBudget{}, protocolMemoryConfigOverflow()
	}
	minimumBytes, ok := checkedAddUint64(steadyBytes, transientBytes)
	if !ok {
		return protocolMemoryBudget{}, protocolMemoryConfigOverflow()
	}
	if memoryLimit < minimumBytes {
		return protocolMemoryBudget{}, moerr.NewInternalErrorNoCtx(
			"proxy protocol-memory-limit is smaller than steady protocol buffers plus one transient operation")
	}
	return protocolMemoryBudget{
		steadyBytes:       steadyBytes,
		headroomBytes:     memoryLimit - steadyBytes,
		transientBytes:    transientBytes,
		backgroundBytes:   backendBytes,
		initialBytes:      initialBytes,
		backendBytes:      backendBytes,
		loginDynamicBytes: loginDynamicBytes,
	}, nil
}

// dynamicProtocolWriteBytes mirrors frontend.Conn.AppendPart: WritePacket adds
// a four-byte header, consumes the fixed IOSession block first, then rounds one
// dynamic allocation up to an IOSession block boundary.
func dynamicProtocolWriteBytes(payloadBytes uint64) (uint64, bool) {
	packetBytes, ok := checkedAddUint64(payloadBytes, frontend.PacketHeaderLength)
	if !ok {
		return 0, false
	}
	if packetBytes <= proxyIOSessionBufferSize {
		return 0, true
	}
	remaining := packetBytes - proxyIOSessionBufferSize
	allocation := max(uint64(proxyIOSessionBufferSize), remaining)
	return roundUpUint64(allocation, proxyIOSessionBufferSize)
}

func protocolMemoryConfigOverflow() error {
	return moerr.NewInternalErrorNoCtx("proxy protocol memory configuration is too large")
}

func checkedAddUint64(a, b uint64) (uint64, bool) {
	if a > math.MaxUint64-b {
		return 0, false
	}
	return a + b, true
}

func checkedMulUint64(a, b uint64) (uint64, bool) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, false
	}
	return a * b, true
}

func roundUpUint64(value, unit uint64) (uint64, bool) {
	if unit == 0 {
		return 0, false
	}
	remainder := value % unit
	if remainder == 0 {
		return value, true
	}
	return checkedAddUint64(value, unit-remainder)
}

type protocolMemoryLimiter struct {
	critical           *semaphore.Weighted
	criticalBytes      uint64
	background         *semaphore.Weighted
	backgroundCapacity uint64
	budget             protocolMemoryBudget
	used               atomic.Int64
}

type protocolMemoryLane uint8

const (
	protocolMemoryCritical protocolMemoryLane = iota
	protocolMemoryBackground
)

func newProtocolMemoryLimiter(c *Config) (*protocolMemoryLimiter, error) {
	budget, err := calculateProtocolMemoryBudget(c)
	if err != nil {
		return nil, err
	}
	return newProtocolMemoryLimiterWithBudget(budget), nil
}

func newProtocolMemoryLimiterWithBudget(budget protocolMemoryBudget) *protocolMemoryLimiter {
	backgroundBytes := min(budget.backgroundBytes, budget.headroomBytes)
	criticalBytes := budget.headroomBytes - backgroundBytes
	return &protocolMemoryLimiter{
		critical:           semaphore.NewWeighted(int64(criticalBytes)),
		criticalBytes:      criticalBytes,
		background:         semaphore.NewWeighted(int64(backgroundBytes)),
		backgroundCapacity: backgroundBytes,
		budget:             budget,
	}
}

// acquire waits instead of spuriously failing a valid phase transition, but
// every caller supplies a lifecycle deadline so admission cannot hang.
func (l *protocolMemoryLimiter) acquire(
	ctx context.Context,
	bytes uint64,
) (*protocolMemoryLease, error) {
	if l == nil {
		return nil, nil
	}
	return l.acquireFrom(ctx, l.critical, l.criticalBytes, bytes)
}

func (l *protocolMemoryLimiter) acquireBackground(
	ctx context.Context,
	bytes uint64,
) (*protocolMemoryLease, error) {
	if l == nil {
		return nil, nil
	}
	return l.acquireFrom(ctx, l.background, l.backgroundCapacity, bytes)
}

func (l *protocolMemoryLimiter) acquireFrom(
	ctx context.Context,
	weighted *semaphore.Weighted,
	capacity uint64,
	bytes uint64,
) (*protocolMemoryLease, error) {
	if l == nil || bytes == 0 {
		return nil, nil
	}
	if weighted == nil || bytes > capacity || bytes > math.MaxInt64 {
		return nil, errProxyConnectionLimit
	}
	if ctx == nil {
		ctx = context.Background()
	}
	amount := int64(bytes)
	if err := weighted.Acquire(ctx, amount); err != nil {
		return nil, errors.Join(errProxyConnectionLimit, context.Cause(ctx))
	}
	l.used.Add(amount)
	lease := &protocolMemoryLease{limiter: l, weighted: weighted, bytes: amount}
	lease.refs.Store(1)
	return lease, nil
}

type protocolMemoryLease struct {
	limiter  *protocolMemoryLimiter
	weighted *semaphore.Weighted
	bytes    int64
	refs     atomic.Int32
}

// retain adds another concrete owner (for example, a pipelined prefix whose
// lifetime extends beyond backend authentication). It is only called before
// that owner becomes concurrently visible.
func (l *protocolMemoryLease) retain() {
	if l == nil {
		return
	}
	for {
		refs := l.refs.Load()
		if refs <= 0 {
			panic("retaining released proxy protocol memory lease")
		}
		if l.refs.CompareAndSwap(refs, refs+1) {
			return
		}
	}
}

func (l *protocolMemoryLease) release() {
	if l == nil {
		return
	}
	for {
		refs := l.refs.Load()
		if refs <= 0 {
			panic("proxy protocol memory lease released more than retained")
		}
		if !l.refs.CompareAndSwap(refs, refs-1) {
			continue
		}
		if refs == 1 {
			l.limiter.used.Add(-l.bytes)
			l.weighted.Release(l.bytes)
		}
		return
	}
}

// protocolMemoryServerConn owns transient overlap while a newly authenticated
// backend coexists with the old backend. Close covers every failure path;
// promote releases the overlap only after the tunnel has closed the old side.
type protocolMemoryServerConn struct {
	ServerConn
	lease *protocolMemoryLease
	once  sync.Once
}

func (c *protocolMemoryServerConn) releaseProtocolMemory() {
	if c == nil {
		return
	}
	c.once.Do(func() {
		c.lease.release()
		c.lease = nil
	})
}

func (c *protocolMemoryServerConn) promoteProtocolMemory() {
	c.releaseProtocolMemory()
}

func (c *protocolMemoryServerConn) Quit() error {
	defer c.releaseProtocolMemory()
	return c.ServerConn.Quit()
}

func (c *protocolMemoryServerConn) Close() error {
	defer c.releaseProtocolMemory()
	return c.ServerConn.Close()
}

func (c *protocolMemoryServerConn) ExecStmtContext(
	ctx context.Context,
	stmt internalStmt,
	resp chan<- []byte,
) (bool, error) {
	return execStmtWithContext(ctx, c.ServerConn, stmt, resp)
}

func acquireProtocolMemoryBefore(
	ctx context.Context,
	limiter *protocolMemoryLimiter,
	bytes uint64,
	deadline time.Time,
) (*protocolMemoryLease, error) {
	if limiter == nil {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	acquireCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	return limiter.acquire(acquireCtx, bytes)
}

func acquireBackgroundProtocolMemoryBefore(
	ctx context.Context,
	limiter *protocolMemoryLimiter,
	bytes uint64,
	deadline time.Time,
) (*protocolMemoryLease, error) {
	if limiter == nil {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	acquireCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	return limiter.acquireBackground(acquireCtx, bytes)
}
