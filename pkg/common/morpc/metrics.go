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
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus"
)

type metrics struct {
	sendCounter                   prometheus.Counter
	receiveCounter                prometheus.Counter
	createCounter                 prometheus.Counter
	closeCounter                  prometheus.Counter
	connectCounter                prometheus.Counter
	connectFailedCounter          prometheus.Counter
	sendingQueueSizeGauge         prometheus.Gauge
	sendingBatchSizeGauge         prometheus.Gauge
	poolSizeGauge                 prometheus.Gauge
	writeLatencyDurationHistogram prometheus.Observer
	writeDurationHistogram        prometheus.Observer
	connectDurationHistogram      prometheus.Observer
	doneDurationHistogram         prometheus.Observer
}

func newMetrics(name string) *metrics {
	return &metrics{
		sendCounter:                   v2.NewRPCMessageSendCounterByName(name),
		receiveCounter:                v2.NewRPCMessageReceiveCounterByName(name),
		createCounter:                 v2.NewRPCBackendCreateCounterByName(name),
		closeCounter:                  v2.NewRPCBackendCloseCounterByName(name),
		connectCounter:                v2.NewRPCBackendConnectCounterByName(name),
		connectFailedCounter:          v2.NewRPCBackendConnectFailedCounterByName(name),
		poolSizeGauge:                 v2.NewRPCBackendPoolSizeGaugeByName(name),
		sendingQueueSizeGauge:         v2.NewRPCBackendSendingQueueSizeGaugeByName(name),
		sendingBatchSizeGauge:         v2.NewRPCBackendSendingBatchSizeGaugeByName(name),
		writeDurationHistogram:        v2.NewRPCBackendWriteDurationHistogramByName(name),
		connectDurationHistogram:      v2.NewRPCBackendConnectDurationHistogramByName(name),
		doneDurationHistogram:         v2.NewRPCBackendDoneDurationHistogramByName(name),
		writeLatencyDurationHistogram: v2.NewRPCBackendWriteLatencyDurationHistogramByName(name),
	}
}

type serverMetrics struct {
	sendCounter                   prometheus.Counter
	receiveCounter                prometheus.Counter
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
	}
}
