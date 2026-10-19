// Copyright 2024 Matrix Origin
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

package fileservice

import (
	"crypto/tls"
	"net/http/httptrace"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

type traceInfo struct {
	mu    sync.Mutex
	times traceTimes
	trace *httptrace.ClientTrace
}

// A phase without a callback identity is measurable only when its start and
// completion are unambiguous. Overlapping/repeated starts must not fabricate a
// duration or pair a late completion with another attempt.
type tracePhase struct {
	started time.Time
	starts  int
	pending int
}

func (p *tracePhase) start() {
	p.starts++
	p.pending++
	if p.starts == 1 {
		p.started = time.Now()
	}
}

func (p *tracePhase) finish() time.Time {
	if p.pending == 0 {
		return time.Time{}
	}
	p.pending--
	if p.starts != 1 || p.pending != 0 {
		return time.Time{}
	}
	return p.started
}

type traceConnectKey struct{ network, address string }

type traceTimes struct {
	GetConn       tracePhase
	GotConn       time.Time
	DNS           tracePhase
	TLS           tracePhase
	Connect       map[traceConnectKey]*tracePhase
	firstResponse bool
}

func newTraceInfo() *traceInfo {
	info := new(traceInfo)
	info.trace = &httptrace.ClientTrace{
		GetConn:              info.GetConn,
		GotConn:              info.GotConn,
		PutIdleConn:          info.PutIdleConn,
		GotFirstResponseByte: info.GotFirstResponseByte,
		DNSStart:             info.DNSStart,
		DNSDone:              info.DNSDone,
		ConnectStart:         info.ConnectStart,
		ConnectDone:          info.ConnectDone,
		TLSHandshakeStart:    info.TLSHandshakeStart,
		TLSHandshakeDone:     info.TLSHandshakeDone,
	}
	return info
}

func (t *traceInfo) GetConn(hostPort string) {
	t.mu.Lock()
	t.times.GetConn.start()
	t.mu.Unlock()
	metric.FSHTTPTraceCounter.WithLabelValues("GetConn").Inc()
}

func (t *traceInfo) GotConn(info httptrace.GotConnInfo) {
	t.mu.Lock()
	start := t.times.GetConn.finish()
	if !start.IsZero() {
		t.times.GotConn = time.Now()
	}
	t.mu.Unlock()
	metric.FSHTTPTraceCounter.WithLabelValues("GotConn").Inc()
	if info.Reused {
		metric.FSHTTPTraceCounter.WithLabelValues("GotConnReused").Inc()
	}
	// WasIdle indicates whether the connection was obtained from an idle pool
	// IdleTime is only meaningful when WasIdle is true, and can be 0 if the
	// connection was just put into the idle pool and immediately retrieved
	if info.WasIdle {
		metric.FSHTTPTraceCounter.WithLabelValues("GotConnIdle").Inc()
		// Connection was idle, now it's active
		// Use the connection object as the key (info.Conn is the underlying connection)
		if info.Conn != nil {
			trackConnActive(info.Conn)
		}
	} else {
		// New connection, mark as active
		if info.Conn != nil {
			trackConnActive(info.Conn)
		}
	}
	if !start.IsZero() {
		metric.S3GetConnDurationHistogram.Observe(time.Since(start).Seconds())
	}
}

func (t *traceInfo) PutIdleConn(err error) {
	if err != nil {
		logutil.Info("PutIdleConn error",
			zap.Error(err),
		)
		// If there's an error, the connection might be closed
		return
	}
	// Connection is being put back to idle pool
	// Note: httptrace.PutIdleConn doesn't provide the connection object,
	// so we track connections by their remote address in GotConn callback.
	// When PutIdleConn is called, we know a connection was returned to the pool,
	// but we can't identify which one. We'll use a heuristic approach:
	// track active connections and estimate idle based on pool configuration.
}

func (t *traceInfo) GotFirstResponseByte() {
	t.mu.Lock()
	start := t.times.GotConn
	if t.times.GetConn.starts != 1 || t.times.firstResponse {
		start = time.Time{}
	}
	t.times.firstResponse = true
	t.mu.Unlock()
	if !start.IsZero() {
		metric.S3GotFirstResponseDurationHistogram.Observe(time.Since(start).Seconds())
	}
}

func (t *traceInfo) DNSStart(di httptrace.DNSStartInfo) {
	t.mu.Lock()
	t.times.DNS.start()
	t.mu.Unlock()
	metric.FSHTTPTraceCounter.WithLabelValues("DNSStart").Inc()
}

func (t *traceInfo) DNSDone(di httptrace.DNSDoneInfo) {
	if di.Err != nil {
		// Log DNS resolution failure for debugging
		logutil.Debug("S3 DNS resolution failed",
			zap.Error(di.Err),
		)
	}
	// Record duration regardless of success/failure to track all DNS resolution attempts
	t.mu.Lock()
	start := t.times.DNS.finish()
	t.mu.Unlock()
	if !start.IsZero() {
		metric.S3DNSResolveDurationHistogram.Observe(time.Since(start).Seconds())
	}
}

func (t *traceInfo) ConnectStart(network, addr string) {
	t.mu.Lock()
	if t.times.Connect == nil {
		t.times.Connect = make(map[traceConnectKey]*tracePhase)
	}
	key := traceConnectKey{network, addr}
	phase := t.times.Connect[key]
	if phase == nil {
		phase = new(tracePhase)
		t.times.Connect[key] = phase
	}
	phase.start()
	t.mu.Unlock()
	metric.FSHTTPTraceCounter.WithLabelValues("ConnectStart").Inc()
}

func (t *traceInfo) ConnectDone(network, addr string, err error) {
	if err != nil {
		// Log connection failure for debugging
		logutil.Debug("S3 connection failed",
			zap.String("network", network),
			zap.String("addr", addr),
			zap.Error(err),
		)
	}
	// Record duration regardless of success/failure to track all connection attempts
	t.mu.Lock()
	key := traceConnectKey{network, addr}
	var start time.Time
	if phase := t.times.Connect[key]; phase != nil {
		start = phase.finish()
		if phase.pending == 0 {
			delete(t.times.Connect, key)
		}
	}
	t.mu.Unlock()
	if !start.IsZero() {
		metric.S3ConnectDurationHistogram.Observe(time.Since(start).Seconds())
	}
}

func (t *traceInfo) TLSHandshakeStart() {
	metric.FSHTTPTraceCounter.WithLabelValues("TLSHandshakeStart").Inc()
	t.mu.Lock()
	t.times.TLS.start()
	t.mu.Unlock()
}

func (t *traceInfo) TLSHandshakeDone(cs tls.ConnectionState, err error) {
	if err != nil {
		// Log TLS handshake failure for debugging
		logutil.Debug("S3 TLS handshake failed",
			zap.Error(err),
		)
	}
	// Record duration regardless of success/failure to track all TLS handshake attempts
	t.mu.Lock()
	start := t.times.TLS.finish()
	t.mu.Unlock()
	if !start.IsZero() {
		metric.S3TLSHandshakeDurationHistogram.Observe(time.Since(start).Seconds())
	}
}
