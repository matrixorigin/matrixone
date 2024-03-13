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

// trace.go observe motrace, mometric those packages' behavior

var (
	traceCollectorDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "trace",
			Name:      "collector_duration_seconds",
			Help:      "Bucketed histogram of trace collector duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"type"})
	TraceCollectorCollectDurationHistogram              = traceCollectorDurationHistogram.WithLabelValues("collect")
	TraceCollectorGenerateAwareDurationHistogram        = traceCollectorDurationHistogram.WithLabelValues("generate_awake")
	TraceCollectorGenerateAwareDiscardDurationHistogram = traceCollectorDurationHistogram.WithLabelValues("generate_awake_discard")
	TraceCollectorGenerateDelayDurationHistogram        = traceCollectorDurationHistogram.WithLabelValues("generate_delay")
	TraceCollectorGenerateDurationHistogram             = traceCollectorDurationHistogram.WithLabelValues("generate")
	TraceCollectorGenerateDiscardDurationHistogram      = traceCollectorDurationHistogram.WithLabelValues("generate_discard")
	TraceCollectorExportDurationHistogram               = traceCollectorDurationHistogram.WithLabelValues("export")

	traceNegativeCUCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "trace",
			Name:      "negative_cu_total",
			Help:      "Count of negative cu to backend",
		}, []string{"type"})
)

func GetTraceNegativeCUCounter(typ string) prometheus.Counter {
	return traceNegativeCUCounter.WithLabelValues(typ)
}
