// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v2

import "github.com/prometheus/client_golang/prometheus"

var (
	ISCPMaterializedViewRefreshDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo", Subsystem: "iscp", Name: "materialized_view_refresh_duration_seconds",
			Help:    "Duration of materialized-view refresh transactions.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 14),
		}, []string{"mode", "result"})
	ISCPMaterializedViewRows = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo", Subsystem: "iscp", Name: "materialized_view_rows_total",
			Help: "Source rows processed by incremental materialized-view refreshes.",
		}, []string{"operation"})
	ISCPMaterializedViewFallback = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mo", Subsystem: "iscp", Name: "materialized_view_fallback_total",
			Help: "Incremental materialized-view refreshes that fell back to full recomputation.",
		})
	ISCPMaterializedViewWatermarkLag = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo", Subsystem: "iscp", Name: "materialized_view_watermark_lag_seconds",
			Help:    "Wall-clock lag of a successfully refreshed materialized-view watermark.",
			Buckets: []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300},
		})
)

func init() {
	registry.MustRegister(ISCPMaterializedViewRefreshDuration)
	registry.MustRegister(ISCPMaterializedViewRows)
	registry.MustRegister(ISCPMaterializedViewFallback)
	registry.MustRegister(ISCPMaterializedViewWatermarkLag)
}
