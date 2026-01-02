// Copyright 2023 Matrix Origin
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

package v2

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	RPCClientCreateCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "client_create_total",
			Help:      "Total number of morpc client created.",
		}, []string{"name"})

	rpcMessageCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "message_total",
			Help:      "Total number of morpc message transfer.",
		}, []string{"name", "type"})

	rpcBackendCreateCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "backend_create_total",
			Help:      "Total number of morpc backend created.",
		}, []string{"name"})

	rpcBackendClosedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "backend_close_total",
			Help:      "Total number of morpc backend created.",
		}, []string{"name"})

	rpcBackendConnectCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "backend_connect_total",
			Help:      "Total number of morpc backend connect.",
		}, []string{"name", "type"})

	rpcNetworkBytesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "network_bytes_total",
			Help:      "Total bytes of rpc network transfer.",
		}, []string{"type"})

	rpcGCChannelDropCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "gc_channel_drop_total",
			Help:      "Total number of GC task requests dropped due to channel full.",
		}, []string{"type"})

	rpcGCIdleBackendsCleanedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "gc_idle_backends_cleaned_total",
			Help:      "Total number of idle backends cleaned by GC idle loop.",
		})

	rpcGCInactiveProcessedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "gc_inactive_processed_total",
			Help:      "Total number of inactive backend cleanup requests processed.",
		})

	rpcGCCreateProcessedCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "gc_create_processed_total",
			Help:      "Total number of backend creation requests processed.",
		})
)

var (
	rpcBackendPoolSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "backend_pool_size",
			Help:      "Size of backend connection pool size.",
		}, []string{"name"})

	rpcSendingQueueSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "sending_queue_size",
			Help:      "Size of sending queue size.",
		}, []string{"name", "side"})

	rpcSendingBatchSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "sending_batch_size",
			Help:      "Size of sending batch size.",
		}, []string{"name", "side"})

	rpcServerSessionSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "server_session_size",
			Help:      "Size of server sessions size.",
		}, []string{"name"})

	rpcGCRegisteredClientsGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "gc_registered_clients_total",
			Help:      "Number of clients registered with the global GC manager.",
		})

	rpcGCChannelQueueLengthGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "gc_channel_queue_length",
			Help:      "Current queue length of GC task channels.",
		}, []string{"type"})

	rpcBackendActiveRequestsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "backend_active_requests",
			Help:      "Current number of active requests (futures) per backend.",
		}, []string{"name"})

	rpcBackendWriteQueueLengthGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "backend_write_queue_length",
			Help:      "Current length of write queue (writeC channel) per backend.",
		}, []string{"name"})

	rpcBackendBusyGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "backend_busy",
			Help:      "Whether backend is busy (1) or not (0).",
		}, []string{"name"})

	rpcClientActiveGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "client_active",
			Help:      "Current number of active RPC clients.",
		}, []string{"name"})
)

var (
	rpcBackendConnectDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "backend_connect_duration_seconds",
			Help:      "Bucketed histogram of write data into socket duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"name"})

	rpcWriteDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "write_duration_seconds",
			Help:      "Bucketed histogram of write data into socket duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"name", "side"})

	rpcWriteLatencyDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "write_latency_duration_seconds",
			Help:      "Bucketed histogram of write latency duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"name", "side"})

	rpcBackendDoneDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "backend_done_duration_seconds",
			Help:      "Bucketed histogram of request done duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"name"})
)

func NewRPCMessageSendCounterByName(name string) prometheus.Counter {
	return rpcMessageCounter.WithLabelValues(name, "send")
}

func NewRPCMessageReceiveCounterByName(name string) prometheus.Counter {
	return rpcMessageCounter.WithLabelValues(name, "receive")
}

func NewRPCBackendCreateCounterByName(name string) prometheus.Counter {
	return rpcBackendCreateCounter.WithLabelValues(name)
}

func NewRPCBackendCloseCounterByName(name string) prometheus.Counter {
	return rpcBackendClosedCounter.WithLabelValues(name)
}

func NewRPCBackendPoolSizeGaugeByName(name string) prometheus.Gauge {
	return rpcBackendPoolSizeGauge.WithLabelValues(name)
}

func NewRPCBackendConnectCounterByName(name string) prometheus.Counter {
	return rpcBackendConnectCounter.WithLabelValues(name, "total")
}

func NewRPCBackendConnectFailedCounterByName(name string) prometheus.Counter {
	return rpcBackendConnectCounter.WithLabelValues(name, "failed")
}

func NewRPCBackendSendingQueueSizeGaugeByName(name string) prometheus.Gauge {
	return rpcSendingQueueSizeGauge.WithLabelValues(name, "client")
}

func NewRPCServerSendingQueueSizeGaugeByName(name string) prometheus.Gauge {
	return rpcSendingQueueSizeGauge.WithLabelValues(name, "server")
}

func NewRPCBackendSendingBatchSizeGaugeByName(name string) prometheus.Gauge {
	return rpcSendingBatchSizeGauge.WithLabelValues(name, "client")
}

func NewRPCServerSendingBatchSizeGaugeByName(name string) prometheus.Gauge {
	return rpcSendingBatchSizeGauge.WithLabelValues(name, "server")
}

func NewRPCBackendWriteDurationHistogramByName(name string) prometheus.Observer {
	return rpcWriteDurationHistogram.WithLabelValues(name, "client")
}

func NewRPCServerWriteDurationHistogramByName(name string) prometheus.Observer {
	return rpcWriteDurationHistogram.WithLabelValues(name, "server")
}

func NewRPCBackendWriteLatencyDurationHistogramByName(name string) prometheus.Observer {
	return rpcWriteLatencyDurationHistogram.WithLabelValues(name, "client")
}

func NewRPCServerWriteLatencyDurationHistogramByName(name string) prometheus.Observer {
	return rpcWriteLatencyDurationHistogram.WithLabelValues(name, "server")
}

func NewRPCBackendConnectDurationHistogramByName(name string) prometheus.Observer {
	return rpcBackendConnectDurationHistogram.WithLabelValues(name)
}

func NewRPCBackendDoneDurationHistogramByName(name string) prometheus.Observer {
	return rpcBackendDoneDurationHistogram.WithLabelValues(name)
}

func NewRPCServerSessionSizeGaugeByName(name string) prometheus.Gauge {
	return rpcServerSessionSizeGauge.WithLabelValues(name)
}

func NewRPCInputCounter() prometheus.Counter {
	return rpcNetworkBytesCounter.WithLabelValues("input")
}

func NewRPCOutputCounter() prometheus.Counter {
	return rpcNetworkBytesCounter.WithLabelValues("output")
}

func NewRPCGCChannelDropCounter(channelType string) prometheus.Counter {
	return rpcGCChannelDropCounter.WithLabelValues(channelType)
}

func GetRPCGCIdleBackendsCleanedCounter() prometheus.Counter {
	return rpcGCIdleBackendsCleanedCounter
}

func GetRPCGCInactiveProcessedCounter() prometheus.Counter {
	return rpcGCInactiveProcessedCounter
}

func GetRPCGCCreateProcessedCounter() prometheus.Counter {
	return rpcGCCreateProcessedCounter
}

func GetRPCGCRegisteredClientsGauge() prometheus.Gauge {
	return rpcGCRegisteredClientsGauge
}

func NewRPCGCChannelQueueLengthGauge(channelType string) prometheus.Gauge {
	return rpcGCChannelQueueLengthGauge.WithLabelValues(channelType)
}

func NewRPCBackendActiveRequestsGaugeByName(name string) prometheus.Gauge {
	return rpcBackendActiveRequestsGauge.WithLabelValues(name)
}

func NewRPCBackendWriteQueueLengthGaugeByName(name string) prometheus.Gauge {
	return rpcBackendWriteQueueLengthGauge.WithLabelValues(name)
}

func NewRPCBackendBusyGaugeByName(name string) prometheus.Gauge {
	return rpcBackendBusyGauge.WithLabelValues(name)
}

func NewRPCClientActiveGaugeByName(name string) prometheus.Gauge {
	return rpcClientActiveGauge.WithLabelValues(name)
}
