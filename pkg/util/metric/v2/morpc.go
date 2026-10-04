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
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	RPCClientCreateCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "client_create_total",
			Help:      "Total number of MORPC clients created.",
		}, []string{"name"})

	rpcMessageCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "message_total",
			Help:      "Total MORPC messages sent or received, including unary, stream, and internal traffic.",
		}, []string{"name", "type"})

	rpcClientRequestStartedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "client_request_started_total",
			Help:      "Total number of non-internal unary MORPC requests admitted to a client backend.",
		}, []string{"name"})

	rpcClientRequestCompletedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "client_request_completed_total",
			Help:      "Total number of non-internal unary MORPC requests completed, classified by transport outcome.",
		}, []string{"name", "outcome"})

	rpcBackendCreateCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "backend_create_total",
			Help:      "Total number of MORPC backends created.",
		}, []string{"name"})

	rpcBackendClosedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "backend_close_total",
			Help:      "Total number of MORPC backends closed.",
		}, []string{"name"})

	rpcBackendConnectCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "backend_connect_total",
			Help:      "Total MORPC backend connection attempts, classified as total attempts or failed attempts.",
		}, []string{"name", "type"})

	rpcNetworkBytesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "network_bytes_total",
			Help:      "Total MORPC network bytes transferred by direction.",
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

	rpcBackendAutoCreateTimeoutCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "backend_auto_create_timeout_total",
			Help:      "Total number of auto-create backend wait timeouts.",
		}, []string{"name"})

	rpcBackendAutoCreateTimeoutEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "backend_auto_create_timeout_event_total",
			Help:      "Total number of distinct backend-create states that caused one or more auto-create wait timeouts.",
		}, []string{"name"})

	rpcBackendUnavailableCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "backend_unavailable_total",
			Help:      "Total backend-unavailable errors when a pool exists but none of its backends can accept traffic.",
		}, []string{"name"})

	rpcCircuitBreakerStateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "circuit_breaker_state",
			Help:      "Circuit breaker state (0=closed, 1=half-open, 2=open).",
		}, []string{"name", "backend"})

	rpcCircuitBreakerTripsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "circuit_breaker_trips_total",
			Help:      "Total number of circuit breaker trips (closed -> open).",
		}, []string{"name", "backend"})

	rpcBackendErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "backend_error_total",
			Help:      "Total classified MORPC backend errors by remote endpoint and lifecycle phase.",
		}, []string{"name", "backend", "phase", "error_type"})

	lockserviceRemoteRPCErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "lockservice",
			Name:      "remote_rpc_error_total",
			Help:      "Total number of classified lockservice remote RPC errors.",
		}, []string{"method", "error_type"})
)

var (
	rpcBackendPoolSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "backend_pool_size",
			Help:      "Current backend connections aggregated across MORPC clients.",
		}, []string{"name"})

	rpcSendingQueueSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "sending_queue_size",
			Help:      "Current queued outbound messages aggregated across MORPC backends or server sessions.",
		}, []string{"name", "side"})

	rpcSendingBatchSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "sending_batch_size",
			Help:      "Size of the most recently processed MORPC sending batch.",
		}, []string{"name", "side"})

	rpcServerSessionSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "server_session_size",
			Help:      "Current number of MORPC server sessions.",
		}, []string{"name"})

	rpcServerStreamStateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "server_stream_state_size",
			Help:      "Current server-side stream sequence and fragment-cache entries.",
		}, []string{"name", "type"})

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
			Help:      "Current queued tasks in MORPC GC manager channels.",
		}, []string{"type"})

	rpcBackendActiveRequestsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "backend_active_requests",
			Help:      "Current number of active Futures aggregated across MORPC client backends.",
		}, []string{"name"})

	rpcBackendWriteQueueLengthGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "backend_write_queue_length",
			Help:      "Deprecated compatibility alias of client-side mo_rpc_sending_queue_size.",
		}, []string{"name"})

	rpcBackendBusyGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "backend_busy",
			Help:      "Current number of MORPC client backends whose sending queue reached the busy threshold.",
		}, []string{"name"})

	rpcClientActiveGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "client_active",
			Help:      "Current number of active MORPC clients.",
		}, []string{"name"})
)

var (
	rpcBackendConnectDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "backend_connect_duration_seconds",
			Help:      "Bucketed histogram of backend connection recovery duration, including retries and backoff.",
			Buckets:   getDurationBuckets(),
		}, []string{"name"})

	rpcWriteDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "write_duration_seconds",
			Help:      "Bucketed histogram of MORPC batch encode and socket flush duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"name", "side"})

	rpcWriteLatencyDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "write_latency_duration_seconds",
			Help:      "Bucketed histogram of MORPC outbound queue wait duration before batch processing.",
			Buckets:   getDurationBuckets(),
		}, []string{"name", "side"})

	rpcBackendDoneDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "backend_done_duration_seconds",
			Help:      "Bucketed histogram of response dispatch overhead after a response has been read; this is not request round-trip latency.",
			Buckets:   getDurationBuckets(),
		}, []string{"name"})

	rpcClientRequestDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "rpc",
			Name:      "client_request_duration_seconds",
			Help:      "Bucketed histogram of non-internal unary MORPC request duration from backend admission to the first terminal transport outcome.",
			// Request duration is a per-client-name histogram. Use a bounded,
			// purpose-built bucket set instead of the generic 100ns-to-10h set
			// (about 147 buckets) to keep the added series cost predictable.
			Buckets: append(
				prometheus.ExponentialBucketsRange(
					float64(time.Microsecond)/float64(time.Second),
					float64(time.Hour)/float64(time.Second),
					48),
				float64(10*time.Hour)/float64(time.Second)),
		}, []string{"name"})
)

func NewRPCMessageSendCounterByName(name string) prometheus.Counter {
	return rpcMessageCounter.WithLabelValues(name, "send")
}

func NewRPCMessageReceiveCounterByName(name string) prometheus.Counter {
	return rpcMessageCounter.WithLabelValues(name, "receive")
}

func NewRPCClientRequestStartedCounterByName(name string) prometheus.Counter {
	return rpcClientRequestStartedCounter.WithLabelValues(name)
}

func NewRPCClientRequestCompletedCounterByNameAndOutcome(name, outcome string) prometheus.Counter {
	return rpcClientRequestCompletedCounter.WithLabelValues(name, outcome)
}

func NewRPCClientRequestDurationHistogramByName(name string) prometheus.Observer {
	return rpcClientRequestDurationHistogram.WithLabelValues(name)
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

func NewRPCServerStreamStateGaugeByName(name, stateType string) prometheus.Gauge {
	return rpcServerStreamStateGauge.WithLabelValues(name, stateType)
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

func NewRPCBackendAutoCreateTimeoutCounterByName(name string) prometheus.Counter {
	return rpcBackendAutoCreateTimeoutCounter.WithLabelValues(name)
}

func NewRPCBackendAutoCreateTimeoutEventCounterByName(name string) prometheus.Counter {
	return rpcBackendAutoCreateTimeoutEventCounter.WithLabelValues(name)
}

func NewRPCBackendUnavailableCounterByName(name string) prometheus.Counter {
	return rpcBackendUnavailableCounter.WithLabelValues(name)
}

func NewRPCCircuitBreakerStateGauge(name, backend string) prometheus.Gauge {
	return rpcCircuitBreakerStateGauge.WithLabelValues(name, backend)
}

func NewRPCCircuitBreakerTripsCounter(name, backend string) prometheus.Counter {
	return rpcCircuitBreakerTripsCounter.WithLabelValues(name, backend)
}

func NewRPCBackendErrorCounter(name, backend, phase, errorType string) prometheus.Counter {
	return rpcBackendErrorCounter.WithLabelValues(name, backend, phase, errorType)
}

func NewLockserviceRemoteRPCErrorCounter(method, errorType string) prometheus.Counter {
	return lockserviceRemoteRPCErrorCounter.WithLabelValues(method, errorType)
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
