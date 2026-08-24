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
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus"
)

type requestOutcome uint8

// Request outcomes are deliberately fixed and transport-level. A syntactically
// valid response is success here even when its application payload represents
// an error; MORPC cannot generically inspect application-specific messages.
const (
	requestOutcomeSuccess requestOutcome = iota
	requestOutcomeTimeout
	requestOutcomeCanceled
	requestOutcomeSendError
	requestOutcomeBackendError
	requestOutcomeAbandoned
	requestOutcomeCount
)

var requestOutcomeNames = [...]string{
	requestOutcomeSuccess:      "success",
	requestOutcomeTimeout:      "timeout",
	requestOutcomeCanceled:     "canceled",
	requestOutcomeSendError:    "send_error",
	requestOutcomeBackendError: "backend_error",
	requestOutcomeAbandoned:    "abandoned",
}

type metrics struct {
	name                          string
	requestStartedCounter         prometheus.Counter
	requestCompletedCounters      [requestOutcomeCount]prometheus.Counter
	requestDurationHistogram      prometheus.Observer
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
	poolSizeMu                    sync.Mutex
	poolSize                      int
	activeRequestsGauge           prometheus.Gauge
	writeQueueLengthGauge         prometheus.Gauge
	busyGauge                     prometheus.Gauge
	writeLatencyDurationHistogram prometheus.Observer
	writeDurationHistogram        prometheus.Observer
	connectDurationHistogram      prometheus.Observer
	doneDurationHistogram         prometheus.Observer
	autoCreateTimeoutCounter      prometheus.Counter // tracks auto-create wait timeouts
	autoCreateTimeoutEventCounter prometheus.Counter // tracks distinct create states causing wait timeouts
	backendUnavailableCounter     prometheus.Counter // tracks backend unavailable (pool has backends but all down)
}

func newMetrics(name string) *metrics {
	m := &metrics{
		name:                          name,
		requestStartedCounter:         v2.NewRPCClientRequestStartedCounterByName(name),
		requestDurationHistogram:      v2.NewRPCClientRequestDurationHistogramByName(name),
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
		autoCreateTimeoutEventCounter: v2.NewRPCBackendAutoCreateTimeoutEventCounterByName(name),
		backendUnavailableCounter:     v2.NewRPCBackendUnavailableCounterByName(name),
	}
	for outcome := requestOutcome(0); outcome < requestOutcomeCount; outcome++ {
		m.requestCompletedCounters[outcome] =
			v2.NewRPCClientRequestCompletedCounterByNameAndOutcome(name, requestOutcomeNames[outcome])
	}
	return m
}

func (m *metrics) requestStarted() {
	if m != nil {
		m.requestStartedCounter.Inc()
	}
}

func (m *metrics) setBackendPoolSize(size int) {
	if m == nil {
		return
	}
	m.poolSizeMu.Lock()
	defer m.poolSizeMu.Unlock()
	delta := size - m.poolSize
	m.poolSize = size
	if delta != 0 {
		m.poolSizeGauge.Add(float64(delta))
	}
}

func (m *metrics) requestCompleted(start time.Time, outcome requestOutcome) {
	if m == nil {
		return
	}
	if outcome >= requestOutcomeCount {
		panic("invalid MORPC request outcome")
	}
	m.requestDurationHistogram.Observe(time.Since(start).Seconds())
	m.requestCompletedCounters[outcome].Inc()
}

func requestOutcomeForError(err error, fallback requestOutcome) requestOutcome {
	switch {
	case errors.Is(err, context.Canceled):
		return requestOutcomeCanceled
	}
	if errors.Is(err, context.DeadlineExceeded) ||
		moerr.IsMoErrCode(err, moerr.ErrRPCTimeout) ||
		rpcMetricErrorType(err) == "timeout" {
		return requestOutcomeTimeout
	}
	return fallback
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
	if errors.Is(err, ErrBackendCreateTimeout) {
		return "backend_create_timeout"
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
	receivedStreamStateGauge      prometheus.Gauge
	sentStreamStateGauge          prometheus.Gauge
	messageCacheStateGauge        prometheus.Gauge
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
		receivedStreamStateGauge:      v2.NewRPCServerStreamStateGaugeByName(name, "received_sequence"),
		sentStreamStateGauge:          v2.NewRPCServerStreamStateGaugeByName(name, "sent_sequence"),
		messageCacheStateGauge:        v2.NewRPCServerStreamStateGaugeByName(name, "message_cache"),
		inputBytesCounter:             v2.NewRPCInputCounter(),
		outputBytesCounter:            v2.NewRPCOutputCounter(),
	}
}
