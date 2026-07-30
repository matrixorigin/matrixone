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

package v2

import "github.com/prometheus/client_golang/prometheus"

var (
	PipelineServerDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "pipeline",
			Name:      "server_duration_seconds",
			Help:      "Bucketed histogram of server processing duration seconds of pipeline.",
			Buckets:   getDurationBuckets(),
		})

	// Gauge (not Counter): "living" senders must Inc() on create and Dec() on close.
	// Counter only ever increases; calling .Desc() does not decrement (Desc is metric metadata).
	pipelineStreamGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "pipeline",
			Name:      "stream_connection",
			Help:      "Current number of stream connections to send messages to other CN (living senders).",
		}, []string{"type"})
	PipelineMessageSenderGauge = pipelineStreamGauge.WithLabelValues("living")

	PipelineCleanupEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "pipeline",
			Name:      "cleanup_event",
			Help:      "Total number of abnormal pipeline cleanup events.",
		}, []string{"event"})

	PipelineStreamTeardownCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "pipeline",
			Name:      "stream_teardown_total",
			Help:      "Pipeline stream teardown outcomes by event.",
		}, []string{"event"})

	PipelineStreamLifecycleGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "pipeline",
			Name:      "stream_lifecycle_active",
			Help:      "Current number of server-side FIN lifecycle registrations.",
		})

	PipelineStreamFinishDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "pipeline",
			Name:      "stream_finish_duration_seconds",
			Help:      "FIN to FIN_ACK completion latency in seconds.",
			Buckets:   getDurationBuckets(),
		})

	PipelineRemoteReceiverWaitDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "pipeline",
			Name:      "remote_receiver_wait_duration_seconds",
			Help:      "Remote receiver registration wait duration by terminal outcome.",
			// Registration is normally sub-millisecond but may legitimately
			// wait through scheduler or overload delays. This compact range
			// covers multi-minute lifecycle waits while avoiding the
			// repository-wide high-resolution duration buckets.
			Buckets: prometheus.ExponentialBuckets(0.000001, 4, 15),
		}, []string{"outcome"})
	PipelineRemoteReceiverWaitReadyHistogram            = PipelineRemoteReceiverWaitDurationHistogram.WithLabelValues("ready")
	PipelineRemoteReceiverWaitConnectionClosedHistogram = PipelineRemoteReceiverWaitDurationHistogram.WithLabelValues(
		"connection_closed",
	)
	PipelineRemoteReceiverWaitMessageCanceledHistogram = PipelineRemoteReceiverWaitDurationHistogram.WithLabelValues(
		"message_canceled",
	)
	PipelineRemoteReceiverWaitAlreadyAttachedHistogram = PipelineRemoteReceiverWaitDurationHistogram.WithLabelValues(
		"already_attached",
	)
	PipelineRemoteReceiverWaitAlreadyClosedHistogram = PipelineRemoteReceiverWaitDurationHistogram.WithLabelValues(
		"already_closed",
	)
	PipelineRemoteNotifyRetryCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "pipeline",
			Name:      "remote_notify_legacy_retry_total",
			Help:      "Total compatibility retries after a peer reports that a remote receiver is not registered yet.",
		})
)
