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
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

type traceInfo struct {
	times traceTimes
	trace *httptrace.ClientTrace
}

type traceTimes struct {
	GetConn           time.Time
	GotConn           time.Time
	DNSStart          time.Time
	ConnectStart      time.Time
	TSLHandshakeStart time.Time
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

func init() {
	reuse.CreatePool(
		newTraceInfo,
		resetTracePoint,
		reuse.DefaultOptions[traceInfo]().
			WithEnableChecker())
}

func (traceInfo) TypeName() string {
	return "fileservice.traceInfo"
}

func resetTracePoint(info *traceInfo) {
	info.times = traceTimes{}
}

func (t *traceInfo) GetConn(hostPort string) {
	t.times.GetConn = time.Now()
	metric.FSHTTPTraceCounter.WithLabelValues("GetConn").Inc()
}

func (t *traceInfo) GotConn(info httptrace.GotConnInfo) {
	t.times.GotConn = time.Now()
	metric.FSHTTPTraceCounter.WithLabelValues("GotConn").Inc()
	metric.FSHTTPTraceCounter.WithLabelValues("GotConnReused").Inc()
	metric.FSHTTPTraceCounter.WithLabelValues("GotConnIdle").Inc()
	metric.S3GetConnDurationHistogram.Observe(time.Since(t.times.GetConn).Seconds())
}

func (t *traceInfo) PutIdleConn(err error) {
	if err != nil {
		logutil.Info("PutIdleConn error",
			zap.Error(err),
		)
	}
}

func (t *traceInfo) GotFirstResponseByte() {
	metric.S3GotFirstResponseDurationHistogram.Observe(time.Since(t.times.GotConn).Seconds())
}

func (t *traceInfo) DNSStart(di httptrace.DNSStartInfo) {
	t.times.DNSStart = time.Now()
	metric.FSHTTPTraceCounter.WithLabelValues("DNSStart").Inc()
}

func (t *traceInfo) DNSDone(di httptrace.DNSDoneInfo) {
	metric.S3DNSResolveDurationHistogram.Observe(time.Since(t.times.DNSStart).Seconds())
}

func (t *traceInfo) ConnectStart(network, addr string) {
	t.times.ConnectStart = time.Now()
	metric.FSHTTPTraceCounter.WithLabelValues("ConnectStart").Inc()
}

func (t *traceInfo) ConnectDone(network, addr string, err error) {
	metric.S3ConnectDurationHistogram.Observe(time.Since(t.times.ConnectStart).Seconds())
}

func (t *traceInfo) TLSHandshakeStart() {
	metric.FSHTTPTraceCounter.WithLabelValues("TLSHandshakeStart").Inc()
	t.times.TSLHandshakeStart = time.Now()
}

func (t *traceInfo) TLSHandshakeDone(cs tls.ConnectionState, err error) {
	metric.S3TLSHandshakeDurationHistogram.Observe(time.Since(t.times.TSLHandshakeStart).Seconds())
}
