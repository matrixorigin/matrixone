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
	taskShortDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "task",
			Name:      "short_duration_seconds",
			Help:      "Bucketed histogram of short tn task execute duration.",
			Buckets:   prometheus.ExponentialBuckets(0.00001, 2.0, 20),
		}, []string{"type"})

	TaskFlushTableTailDurationHistogram   = taskShortDurationHistogram.WithLabelValues("flush_table_tail")
	TaskGCkpCollectUsageDurationHistogram = taskShortDurationHistogram.WithLabelValues("gckp_collect_usage")
	TaskICkpCollectUsageDurationHistogram = taskShortDurationHistogram.WithLabelValues("ickp_collect_uage")

	taskLongDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "task",
			Name:      "long_duration_seconds",
			Help:      "Bucketed histogram of long tn task execute duration.",
			Buckets:   prometheus.ExponentialBuckets(1, 2.0, 13),
		}, []string{"type"})

	TaskCkpEntryPendingDurationHistogram = taskLongDurationHistogram.WithLabelValues("ckp_entry_pending")
)

var (
	taskScheduledByCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "task",
			Name:      "scheduled_by_total",
			Help:      "Total number of task have been scheduled.",
		}, []string{"type"})

	TaskMergeScheduledByCounter = taskScheduledByCounter.WithLabelValues("merge")

	taskGeneratedStuffCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "task",
			Name:      "execute_results_total",
			Help:      "Total number of stuff a task have generated",
		}, []string{"type"})

	TaskMergedBlocksCounter   = taskGeneratedStuffCounter.WithLabelValues("merged_block")
	TasKMergedSizeCounter     = taskGeneratedStuffCounter.WithLabelValues("merged_size")
	TaskGCkpLoadObjectCounter = taskGeneratedStuffCounter.WithLabelValues("gckp_load_object")
	TaskICkpLoadObjectCounter = taskGeneratedStuffCounter.WithLabelValues("ickp_load_object")

	taskSelectivityCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "task",
			Name:      "selectivity",
			Help:      "Selectivity counter for read filter, block etc.",
		}, []string{"type"})

	TaskSelReadFilterTotal = taskSelectivityCounter.WithLabelValues("readfilter_total")
	TaskSelReadFilterHit   = taskSelectivityCounter.WithLabelValues("readfilter_hit")
	TaskSelBlockTotal      = taskSelectivityCounter.WithLabelValues("block_total")
	TaskSelBlockHit        = taskSelectivityCounter.WithLabelValues("block_hit")
	TaskSelColumnTotal     = taskSelectivityCounter.WithLabelValues("column_total")
	TaskSelColumnHit       = taskSelectivityCounter.WithLabelValues("column_hit")
)

var (
	TaskMergeTransferPageLengthGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "task",
			Name:      "merge_transfer_page_size",
			Help:      "Size of merge generated transfer page",
		})
)
