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
	LogTailApplyDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "cn",
			Subsystem: "logtail",
			Name:      "apply_log_tail_duration_seconds",
			Help:      "Bucketed histogram of apply log tail duration.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2.0, 20),
		})

	LogTailWaitDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "cn",
			Subsystem: "logtail",
			Name:      "wait_log_tail_duration_seconds",
			Help:      "Bucketed histogram of wait log tail apply duration.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2.0, 20),
		})

	LogTailAppendDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tn",
			Subsystem: "logtail",
			Name:      "append_log_tail_duration_seconds",
			Help:      "Bucketed histogram of append log tail into logservice duration.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2.0, 20),
		})

	LogTailBytesHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tn",
			Subsystem: "logtail",
			Name:      "log_tail_bytes",
			Help:      "Bucketed histogram of logtail log bytes.",
			Buckets:   prometheus.ExponentialBuckets(1, 2.0, 10),
		}, []string{"type"})

	LogTailSendDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tn",
			Subsystem: "logtail",
			Name:      "send_log_tail_duration_seconds",
			Help:      "Bucketed histogram of send logtail log duration.",
			Buckets:   prometheus.ExponentialBuckets(1, 2.0, 10),
		})
)

func GetWriteLogTailBytesHistogram() prometheus.Observer {
	return LogTailBytesHistogram.WithLabelValues("write")
}

func GetSendLogTailBytesHistogram() prometheus.Observer {
	return LogTailBytesHistogram.WithLabelValues("send")
}

func GetReceiveLogTailBytesHistogram() prometheus.Observer {
	return LogTailBytesHistogram.WithLabelValues("receive")
}
