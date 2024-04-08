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
	LogtailLoadCheckpointCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "logtail",
			Name:      "load_checkpoint_total",
			Help:      "Total number of load checkpoint handled.",
		})

	logtailReceivedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "logtail",
			Name:      "received_total",
			Help:      "Total number of received logtail.",
		}, []string{"type"})
	LogtailTotalReceivedCounter       = logtailReceivedCounter.WithLabelValues("total")
	LogtailSubscribeReceivedCounter   = logtailReceivedCounter.WithLabelValues("subscribe")
	LogtailUnsubscribeReceivedCounter = logtailReceivedCounter.WithLabelValues("unsubscribe")
	LogtailUpdateReceivedCounter      = logtailReceivedCounter.WithLabelValues("update")
	LogtailHeartbeatReceivedCounter   = logtailReceivedCounter.WithLabelValues("heartbeat")
)

var (
	logTailQueueSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "logtail",
			Name:      "queue_size",
			Help:      "Size of logtail queue size.",
		}, []string{"type"})
	LogTailSendQueueSizeGauge    = logTailQueueSizeGauge.WithLabelValues("send")
	LogTailReceiveQueueSizeGauge = logTailQueueSizeGauge.WithLabelValues("receive")
	LogTailApplyQueueSizeGauge   = logTailQueueSizeGauge.WithLabelValues("apply")
)

var (
	LogTailBytesHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "logtail",
			Name:      "bytes",
			Help:      "Bucketed histogram of logtail log bytes.",
			Buckets:   prometheus.ExponentialBuckets(1, 2.0, 10),
		})

	logTailApplyDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "logtail",
			Name:      "apply_duration_seconds",
			Help:      "Bucketed histogram of apply log tail into mem-table duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"step"})
	LogTailApplyDurationHistogram              = logTailApplyDurationHistogram.WithLabelValues("apply")
	LogTailApplyLatencyDurationHistogram       = logTailApplyDurationHistogram.WithLabelValues("apply-latency")
	LogTailApplyNotifyDurationHistogram        = logTailApplyDurationHistogram.WithLabelValues("apply-notify")
	LogTailApplyNotifyLatencyDurationHistogram = logTailApplyDurationHistogram.WithLabelValues("apply-notify-latency")

	LogTailAppendDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "logtail",
			Name:      "append_duration_seconds",
			Help:      "Bucketed histogram of append log tail into logservice duration.",
			Buckets:   getDurationBuckets(),
		})

	logTailSendDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "logtail",
			Name:      "send_duration_seconds",
			Help:      "Bucketed histogram of send logtail log duration.",
			Buckets:   prometheus.ExponentialBuckets(0.00001, 2.0, 10),
		}, []string{"step"})
	LogtailSendTotalHistogram   = logTailSendDurationHistogram.WithLabelValues("total")
	LogtailSendLatencyHistogram = logTailSendDurationHistogram.WithLabelValues("latency")
	LogtailSendNetworkHistogram = logTailSendDurationHistogram.WithLabelValues("network")

	LogTailLoadCheckpointDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "logtail",
			Name:      "load_checkpoint_duration_seconds",
			Help:      "Bucketed histogram of load check point duration.",
			Buckets:   getDurationBuckets(),
		})

	LogTailCollectDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "logtail",
			Name:      "collect_duration_seconds",
			Help:      "Bucketed histogram of logtail collecting duration.",
			Buckets:   getDurationBuckets(),
		})
)

var (
	LogTailSubscriptionCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "logtail",
			Name:      "subscription_request_total",
			Help:      "Total numbers of logtail subscription the tn have received.",
		})
)
