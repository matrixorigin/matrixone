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

	pipelineStreamCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "pipeline",
			Name:      "stream_connection",
			Help:      "Total number of stream connections to send messages to other CN.",
		}, []string{"type"})
	PipelineMessageSenderCounter = pipelineStreamCounter.WithLabelValues("living")
)
