// Copyright 2021 - 2022 Matrix Origin
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

package morpc

import (
	"errors"
	"io"
	"net"
	"os"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	name                          string
	sendCounter                   prometheus.Counter
	receiveCounter                prometheus.Counter
	createCounter                 prometheus.Counter
	closeCounter                  prometheus.Counter
	connectCounter                prometheus.Counter
	connectFailedCounter          prometheus.Counter
	inputBytesCounter             prometheus.Counter
	outputBytesCounter            prometheus.Counter
	sendingQueueSizeGauge         prometheus.Gauge
	sendingBatchSizeGauge         prometheus.Gauge
	poolSizeGauge                 prometheus.Gauge
	activeRequestsGauge           prometheus.Gauge
	writeQueueLengthGauge         prometheus.Gauge
	busyGauge                     prometheus.Gauge
	writeLatencyDurationHistogram prometheus.Observer
	writeDurationHistogram        prometheus.Observer
	connectDurationHistogram      prometheus.Observer
	doneDurationHistogram         prometheus.Observer
	autoCreateTimeoutCounter      prometheus.Counter // tracks auto-create wait timeouts
	backendUnavailableCounter     prometheus.Counter // tracks backend unavailable (pool has backends but all down)
}

func newMetrics(name string) *metrics {
	return &metrics{
		name:                          name,
		sendCounter:                   v2.NewRPCMessageSendCounterByName(name),
		receiveCounter:                v2.NewRPCMessageReceiveCounterByName(name),
		createCounter:                 v2.NewRPCBackendCreateCounterByName(name),
		closeCounter:                  v2.NewRPCBackendCloseCounterByName(name),
		connectCounter:                v2.NewRPCBackendConnectCounterByName(name),
		connectFailedCounter:          v2.NewRPCBackendConnectFailedCounterByName(name),
		poolSizeGauge:                 v2.NewRPCBackendPoolSizeGaugeByName(name),
		sendingQueueSizeGauge:         v2.NewRPCBackendSendingQueueSizeGaugeByName(name),
		sendingBatchSizeGauge:         v2.NewRPCBackendSendingBatchSizeGaugeByName(name),
		activeRequestsGauge:           v2.NewRPCBackendActiveRequestsGaugeByName(name),
		writeQueueLengthGauge:         v2.NewRPCBackendWriteQueueLengthGaugeByName(name),
		busyGauge:                     v2.NewRPCBackendBusyGaugeByName(name),
		writeDurationHistogram:        v2.NewRPCBackendWriteDurationHistogramByName(name),
		connectDurationHistogram:      v2.NewRPCBackendConnectDurationHistogramByName(name),
		doneDurationHistogram:         v2.NewRPCBackendDoneDurationHistogramByName(name),
		writeLatencyDurationHistogram: v2.NewRPCBackendWriteLatencyDurationHistogramByName(name),
		inputBytesCounter:             v2.NewRPCInputCounter(),
		outputBytesCounter:            v2.NewRPCOutputCounter(),
		autoCreateTimeoutCounter:      v2.NewRPCBackendAutoCreateTimeoutCounterByName(name),
		backendUnavailableCounter:     v2.NewRPCBackendUnavailableCounterByName(name),
	}
}

func (m *metrics) observeBackendError(backend, phase string, err error) {
	if m == nil || err == nil {
		return
	}
	v2.NewRPCBackendErrorCounter(m.name, backend, phase, rpcMetricErrorType(err)).Inc()
}

func rpcMetricErrorType(err error) string {
	if err == nil {
		return "none"
	}
	if moerr.IsMoErrCode(err, moerr.ErrRPCTimeout) {
		return "rpc_timeout"
	}
	if moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect) {
		return "backend_cannot_connect"
	}
	if moerr.IsMoErrCode(err, moerr.ErrBackendClosed) || errors.Is(err, backendClosed) {
		return "backend_closed"
	}
	if moerr.IsMoErrCode(err, moerr.ErrUnexpectedEOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "unexpected_eof"
	}
	if errors.Is(err, io.EOF) {
		return "eof"
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	errText := err.Error()
	switch {
	case strings.Contains(errText, "i/o timeout"),
		strings.Contains(errText, "deadline exceeded"),
		strings.Contains(errText, "timeout"):
		return "timeout"
	case strings.Contains(errText, "unexpected EOF"):
		return "unexpected_eof"
	case strings.Contains(errText, "EOF"):
		return "eof"
	}
	return "other"
}

type serverMetrics struct {
	sendCounter                   prometheus.Counter
	receiveCounter                prometheus.Counter
	inputBytesCounter             prometheus.Counter
	outputBytesCounter            prometheus.Counter
	sendingQueueSizeGauge         prometheus.Gauge
	sessionSizeGauge              prometheus.Gauge
	sendingBatchSizeGauge         prometheus.Gauge
	writeDurationHistogram        prometheus.Observer
	writeLatencyDurationHistogram prometheus.Observer
}

func newServerMetrics(name string) *serverMetrics {
	return &serverMetrics{
		sendCounter:                   v2.NewRPCMessageSendCounterByName(name),
		receiveCounter:                v2.NewRPCMessageReceiveCounterByName(name),
		writeDurationHistogram:        v2.NewRPCServerWriteDurationHistogramByName(name),
		sendingBatchSizeGauge:         v2.NewRPCServerSendingBatchSizeGaugeByName(name),
		sendingQueueSizeGauge:         v2.NewRPCServerSendingQueueSizeGaugeByName(name),
		writeLatencyDurationHistogram: v2.NewRPCServerWriteLatencyDurationHistogramByName(name),
		sessionSizeGauge:              v2.NewRPCServerSessionSizeGaugeByName(name),
		inputBytesCounter:             v2.NewRPCInputCounter(),
		outputBytesCounter:            v2.NewRPCOutputCounter(),
	}
}
