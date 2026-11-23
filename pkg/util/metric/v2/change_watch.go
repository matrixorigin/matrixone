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
	changeWatchDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "change_watch_phase_duration_seconds",
			Help:      "Changed-table watch phase durations. Histogram counts record attempts, including failures.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 21),
		},
		[]string{"phase"},
	)
	ChangeWatchBarrierDurationHistogram        = changeWatchDurationHistogram.WithLabelValues("barrier")
	ChangeWatchCatalogDurationHistogram        = changeWatchDurationHistogram.WithLabelValues("catalog")
	ChangeWatchRecheckBarrierDurationHistogram = changeWatchDurationHistogram.WithLabelValues("recheck-barrier")
	ChangeWatchRecheckCatalogDurationHistogram = changeWatchDurationHistogram.WithLabelValues("recheck-catalog")
	ChangeWatchCapabilityRPCDurationHistogram  = changeWatchDurationHistogram.WithLabelValues("capability-rpc")
	ChangeWatchScanRPCDurationHistogram        = changeWatchDurationHistogram.WithLabelValues("scan-rpc")
	ChangeWatchTNScanDurationHistogram         = changeWatchDurationHistogram.WithLabelValues("tn-scan")
)
