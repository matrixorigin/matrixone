// Copyright 2026 Matrix Origin
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
	VectorIndexCacheFreshnessSweepEntriesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "vector_index_cache",
			Name:      "freshness_sweep_entries_total",
			Help:      "Total number of vector index cache freshness checks by terminal outcome.",
		}, []string{"outcome"})

	VectorIndexCacheFreshnessSweepDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "vector_index_cache",
			Name:      "freshness_sweep_duration_seconds",
			Help:      "Duration of FULLTEXT2 vector index cache freshness sweeps.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 13),
		})
)

func initVectorIndexCacheMetrics() {
	registry.MustRegister(VectorIndexCacheFreshnessSweepEntriesCounter)
	registry.MustRegister(VectorIndexCacheFreshnessSweepDurationHistogram)
}
