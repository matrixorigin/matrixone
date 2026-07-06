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

import "github.com/prometheus/client_golang/prometheus"

var (
	proxyConnectCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "proxy",
			Name:      "connect_counter",
			Help:      "Count of proxy connect to backend",
		}, []string{"type"})
	ProxyConnectAcceptedCounter   = proxyConnectCounter.WithLabelValues("accepted")
	ProxyConnectCurrentCounter    = proxyConnectCounter.WithLabelValues("current")
	ProxyConnectSuccessCounter    = proxyConnectCounter.WithLabelValues("success")
	ProxyConnectRouteFailCounter  = proxyConnectCounter.WithLabelValues("route-fail")
	ProxyConnectCommonFailCounter = proxyConnectCounter.WithLabelValues("common-fail")
	ProxyConnectRetryCounter      = proxyConnectCounter.WithLabelValues("retry")
	ProxyConnectSelectCounter     = proxyConnectCounter.WithLabelValues("select")
	ProxyConnectRejectCounter     = proxyConnectCounter.WithLabelValues("reject")
	// ProxyConnectCNHealthTripCounter counts how many times a CN server's
	// health breaker tripped (was marked temporarily unhealthy).
	ProxyConnectCNHealthTripCounter = proxyConnectCounter.WithLabelValues("cn-health-trip")
	// ProxyConnectCNHealthRetripCounter counts re-trips, i.e. a half-open
	// probe failed and the CN was put back into cooldown with a longer backoff.
	ProxyConnectCNHealthRetripCounter = proxyConnectCounter.WithLabelValues("cn-health-retrip")
	// ProxyConnectCNHealthProbeCounter counts half-open probe connections
	// handed out to a recovering CN server.
	ProxyConnectCNHealthProbeCounter = proxyConnectCounter.WithLabelValues("cn-health-probe")
	// ProxyConnectCNAllBusyCounter counts route attempts that fast-failed
	// because every candidate CN server was temporarily unhealthy.
	ProxyConnectCNAllBusyCounter = proxyConnectCounter.WithLabelValues("cn-all-busy")

	proxyDisconnectCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "proxy",
			Name:      "disconnect_counter",
			Help:      "Count of proxy disconnect with server or client",
		}, []string{"type"})
	ProxyServerDisconnectCounter = proxyDisconnectCounter.WithLabelValues("server")
	ProxyClientDisconnectCounter = proxyDisconnectCounter.WithLabelValues("client")

	proxyTransferCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "proxy",
			Name:      "connection_transfer_counter",
			Help:      "Count of proxy transfer connections",
		}, []string{"type"})
	ProxyTransferSuccessCounter = proxyTransferCounter.WithLabelValues("success")
	ProxyTransferFailCounter    = proxyTransferCounter.WithLabelValues("fail")
	ProxyTransferAbortCounter   = proxyTransferCounter.WithLabelValues("abort")

	ProxyTransferDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "proxy",
			Name:      "connection_transfer_duration",
			Help:      "Histogram of proxy transfer connections duration",
			Buckets:   prometheus.ExponentialBuckets(0.00001, 2.0, 20),
		})

	ProxyDrainCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "proxy",
			Name:      "drain_counter",
			Help:      "Count of proxy drain CN servers",
		})

	ProxyAvailableBackendServerNumGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "proxy",
			Name:      "available_backend_server_num",
			Help:      "Count of available backend servers",
		}, []string{"account"})

	ProxyTransferQueueSizeGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "proxy",
			Name:      "transfer_queue_size",
			Help:      "Size of proxy transfer queue",
		})

	ProxyConnectionsNeedToTransferGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "proxy",
			Name:      "connections_need_to_transfer",
			Help:      "Proxy connections need to transfer",
		})

	ProxyConnectionsTransferIntentGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "proxy",
			Name:      "connections_transfer_intent",
			Help:      "Proxy connections in transfer intent state",
		})
)
