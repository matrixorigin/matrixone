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

	LogTailApplyDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "logtail",
			Name:      "apply_duration_seconds",
			Help:      "Bucketed histogram of apply log tail into mem-table duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	LogTailAppendDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "logtail",
			Name:      "append_duration_seconds",
			Help:      "Bucketed histogram of append log tail into logservice duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	logTailSendDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "logtail",
			Name:      "send_duration_seconds",
			Help:      "Bucketed histogram of send logtail log duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 10),
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
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	LogTailCollectDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "logtail",
			Name:      "collect_duration_seconds",
			Help:      "Bucketed histogram of logtail collecting duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
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
