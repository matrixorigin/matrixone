// Copyright 2021 - 2024 Matrix Origin
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
	LogServiceAppendDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "logservice",
			Name:      "append_duration_seconds",
			Help:      "Bucketed histogram of logservice append duration.",
			Buckets:   getDurationBuckets(),
		})

	LogServiceAppendCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "logservice",
			Name:      "append_total",
			Help:      "Total number of logservice append count.",
		})

	LogServiceAppendBytesHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "logservice",
			Name:      "append_bytes",
			Help:      "Bucketed histogram of logservice append bytes.",
			Buckets:   prometheus.ExponentialBuckets(1, 2.0, 30),
		})
)
