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
			Buckets:   getDurationBuckets(),
		}, []string{"type"})

	TaskFlushTableTailDurationHistogram     = taskShortDurationHistogram.WithLabelValues("flush_table_tail")
	TaskCommitTableTailDurationHistogram    = taskShortDurationHistogram.WithLabelValues("commit_table_tail")
	TaskCommitMergeObjectsDurationHistogram = taskShortDurationHistogram.WithLabelValues("commit_merge_objects")
	GetObjectStatsDurationHistogram         = taskShortDurationHistogram.WithLabelValues("get_object_stats")

	// storage usage / show accounts metrics
	TaskGCkpCollectUsageDurationHistogram          = taskShortDurationHistogram.WithLabelValues("gckp_collect_usage")
	TaskICkpCollectUsageDurationHistogram          = taskShortDurationHistogram.WithLabelValues("ickp_collect_usage")
	TaskStorageUsageReqDurationHistogram           = taskShortDurationHistogram.WithLabelValues("handle_usage_request")
	TaskShowAccountsGetTableStatsDurationHistogram = taskShortDurationHistogram.WithLabelValues("show_accounts_get_table_stats")
	TaskShowAccountsGetUsageDurationHistogram      = taskShortDurationHistogram.WithLabelValues("show_accounts_get_storage_usage")
	TaskShowAccountsTotalDurationHistogram         = taskShortDurationHistogram.WithLabelValues("show_accounts_total_duration")

	taskLongDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "task",
			Name:      "long_duration_seconds",
			Help:      "Bucketed histogram of long tn task execute duration.",
			Buckets:   prometheus.ExponentialBuckets(1, 2.0, 13),
		}, []string{"type"})

	taskBytesHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "task",
			Name:      "hist_bytes",
			Help:      "Bucketed histogram of task result bytes.",
			Buckets:   prometheus.ExponentialBuckets(1, 2.0, 30),
		}, []string{"type"})

	taskCountHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "task",
			Name:      "hist_total",
			Help:      "Bucketed histogram of task result count.",
			Buckets:   prometheus.ExponentialBuckets(1, 2.0, 30),
		}, []string{"type"})

	TaskCkpEntryPendingDurationHistogram = taskLongDurationHistogram.WithLabelValues("ckp_entry_pending")
	TaskLoadMemDeletesPerBlockHistogram  = taskCountHistogram.WithLabelValues("load_mem_deletes_per_block")
	TaskFlushDeletesCountHistogram       = taskCountHistogram.WithLabelValues("flush_deletes_count")
	TaskFlushDeletesSizeHistogram        = taskBytesHistogram.WithLabelValues("flush_deletes_size")
)

var (
	taskScheduledByCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "task",
			Name:      "scheduled_by_total",
			Help:      "Total number of task have been scheduled.",
		}, []string{"type", "nodetype"})

	TaskDNMergeScheduledByCounter = taskScheduledByCounter.WithLabelValues("merge", "dn")
	TaskCNMergeScheduledByCounter = taskScheduledByCounter.WithLabelValues("merge", "cn")

	taskGeneratedStuffCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "task",
			Name:      "execute_results_total",
			Help:      "Total number of stuff a task have generated",
		}, []string{"type", "nodetype"})

	TaskDNMergedSizeCounter = taskGeneratedStuffCounter.WithLabelValues("merged_size", "dn")
	TaskCNMergedSizeCounter = taskGeneratedStuffCounter.WithLabelValues("merged_size", "cn")

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

	TaskStorageUsageCacheMemUsedGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "task",
			Name:      "storage_usage_cache_size",
			Help:      "Size of the storage usage cache used",
		})
)

var (
	transferPageHitHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "mo",
		Subsystem: "task",
		Name:      "transfer_page_hit_count",
		Help:      "The total number of transfer hit counter.",
	}, []string{"type"})

	TransferPageTotalHitHistogram = transferPageHitHistogram.WithLabelValues("total")
)

var (
	TransferPageRowHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "mo",
		Subsystem: "task",
		Name:      "transfer_page_row",
		Help:      "The total number of transfer row.",
	})
)

var (
	transferDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "mo",
		Subsystem: "task",
		Name:      "transfer_duration",
		Buckets:   getDurationBuckets(),
	}, []string{"type"})

	TransferDiskLatencyHistogram           = transferDurationHistogram.WithLabelValues("disk_latency")
	TransferPageSinceBornDurationHistogram = transferDurationHistogram.WithLabelValues("page_since_born_duration")
	TransferTableRunTTLDurationHistogram   = transferDurationHistogram.WithLabelValues("table_run_ttl_duration")
	TransferPageFlushLatencyHistogram      = transferDurationHistogram.WithLabelValues("page_flush_latency")
	TransferPageMergeLatencyHistogram      = transferDurationHistogram.WithLabelValues("page_merge_latency")

	transferShortDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "mo",
		Subsystem: "task",
		Name:      "transfer_short_duration",
		Buckets:   getShortDurationBuckets(),
	}, []string{"type"})

	TransferMemLatencyHistogram = transferShortDurationHistogram.WithLabelValues("mem_latency")
)
