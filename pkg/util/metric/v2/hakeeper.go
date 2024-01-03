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
	HeartbeatHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "hakeeper",
			Subsystem: "heartbeat_send",
			Name:      "duration_seconds",
			Help:      "hakeeper heartbeat send durations",
			Buckets:   getDurationBuckets(),
		}, []string{"type"})

	HeartbeatFailureCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "hakeeper",
			Subsystem: "heartbeat_send",
			Name:      "failed_total",
			Help:      "hakeeper heartbeat failed count",
		}, []string{"type"})

	HeartbeatRecvHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "hakeeper",
			Subsystem: "heartbeat_recv",
			Name:      "duration_seconds",
			Help:      "hakeeper heartbeat recv durations",
			Buckets:   getDurationBuckets(),
		}, []string{"type"})

	HeartbeatRecvFailureCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "hakeeper",
			Subsystem: "heartbeat_recv",
			Name:      "failed_total",
			Help:      "hakeeper heartbeat recv failed count",
		}, []string{"type"})

	CNHeartbeatHistogram          = HeartbeatHistogram.WithLabelValues("cn")
	CNHeartbeatFailureCounter     = HeartbeatFailureCounter.WithLabelValues("cn")
	CNHeartbeatRecvHistogram      = HeartbeatRecvHistogram.WithLabelValues("cn")
	CNHeartbeatRecvFailureCounter = HeartbeatRecvFailureCounter.WithLabelValues("cn")

	TNHeartbeatHistogram          = HeartbeatHistogram.WithLabelValues("tn")
	TNHeartbeatFailureCounter     = HeartbeatFailureCounter.WithLabelValues("tn")
	TNHeartbeatRecvHistogram      = HeartbeatRecvHistogram.WithLabelValues("tn")
	TNHeartbeatRecvFailureCounter = HeartbeatRecvFailureCounter.WithLabelValues("tn")

	LogHeartbeatHistogram          = HeartbeatHistogram.WithLabelValues("log")
	LogHeartbeatFailureCounter     = HeartbeatFailureCounter.WithLabelValues("log")
	LogHeartbeatRecvHistogram      = HeartbeatRecvHistogram.WithLabelValues("log")
	LogHeartbeatRecvFailureCounter = HeartbeatRecvFailureCounter.WithLabelValues("log")
)
