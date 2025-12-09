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
	// GC execution counters
	gcExecutionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "gc",
			Name:      "execution_total",
			Help:      "Total number of GC executions.",
		}, []string{"type", "status"})

	GCCheckpointExecutionCounter      = gcExecutionCounter.WithLabelValues("checkpoint", "success")
	GCCheckpointExecutionErrorCounter = gcExecutionCounter.WithLabelValues("checkpoint", "error")
	GCMergeExecutionCounter           = gcExecutionCounter.WithLabelValues("merge", "success")
	GCMergeExecutionErrorCounter      = gcExecutionCounter.WithLabelValues("merge", "error")
	GCSnapshotExecutionCounter        = gcExecutionCounter.WithLabelValues("snapshot", "success")
	GCSnapshotExecutionErrorCounter   = gcExecutionCounter.WithLabelValues("snapshot", "error")

	// GC file deletion counters
	gcFileDeletionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "gc",
			Name:      "file_deletion_total",
			Help:      "Total number of files deleted by GC.",
		}, []string{"type", "reason"})

	GCDataFileDeletionCounter       = gcFileDeletionCounter.WithLabelValues("data", "expired")
	GCTombstoneFileDeletionCounter  = gcFileDeletionCounter.WithLabelValues("tombstone", "expired")
	GCCheckpointFileDeletionCounter = gcFileDeletionCounter.WithLabelValues("checkpoint", "merged")
	GCMetaFileDeletionCounter       = gcFileDeletionCounter.WithLabelValues("meta", "stale")
	GCSnapshotFileDeletionCounter   = gcFileDeletionCounter.WithLabelValues("snapshot", "stale")

	// GC file size statistics
	gcFileSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "gc",
			Name:      "file_size_bytes",
			Help:      "Bucketed histogram of GC deleted file sizes.",
			Buckets:   prometheus.ExponentialBuckets(1024, 2.0, 20), // 1KB to 1GB
		}, []string{"type"})

	GCDataFileSizeHistogram       = gcFileSizeHistogram.WithLabelValues("data")
	GCTombstoneFileSizeHistogram  = gcFileSizeHistogram.WithLabelValues("tombstone")
	GCCheckpointFileSizeHistogram = gcFileSizeHistogram.WithLabelValues("checkpoint")
	GCMetaFileSizeHistogram       = gcFileSizeHistogram.WithLabelValues("meta")
	GCSnapshotFileSizeHistogram   = gcFileSizeHistogram.WithLabelValues("snapshot")

	// GC execution duration statistics
	gcDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "gc",
			Name:      "duration_seconds",
			Help:      "Bucketed histogram of GC execution duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"type", "phase"})

	// Checkpoint GC phases
	GCCheckpointScanDurationHistogram   = gcDurationHistogram.WithLabelValues("checkpoint", "scan")
	GCCheckpointFilterDurationHistogram = gcDurationHistogram.WithLabelValues("checkpoint", "filter")
	GCCheckpointDeleteDurationHistogram = gcDurationHistogram.WithLabelValues("checkpoint", "delete")
	GCCheckpointTotalDurationHistogram  = gcDurationHistogram.WithLabelValues("checkpoint", "total")

	// Merge GC phases
	GCMergeScanDurationHistogram    = gcDurationHistogram.WithLabelValues("merge", "scan")
	GCMergeCollectDurationHistogram = gcDurationHistogram.WithLabelValues("merge", "collect")
	GCMergeWriteDurationHistogram   = gcDurationHistogram.WithLabelValues("merge", "write")
	GCMergeTotalDurationHistogram   = gcDurationHistogram.WithLabelValues("merge", "total")

	// Snapshot GC phases
	GCSnapshotScanDurationHistogram    = gcDurationHistogram.WithLabelValues("snapshot", "scan")
	GCSnapshotCollectDurationHistogram = gcDurationHistogram.WithLabelValues("snapshot", "collect")
	GCSnapshotDeleteDurationHistogram  = gcDurationHistogram.WithLabelValues("snapshot", "delete")
	GCSnapshotTotalDurationHistogram   = gcDurationHistogram.WithLabelValues("snapshot", "total")

	// GC object statistics
	gcObjectCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "gc",
			Name:      "object_total",
			Help:      "Total number of objects processed by GC.",
		}, []string{"type", "action"})

	GCObjectScannedCounter   = gcObjectCounter.WithLabelValues("object", "scanned")
	GCObjectDeletedCounter   = gcObjectCounter.WithLabelValues("object", "deleted")
	GCObjectProtectedCounter = gcObjectCounter.WithLabelValues("object", "protected")
	GCObjectSkippedCounter   = gcObjectCounter.WithLabelValues("object", "skipped")

	// GC table statistics
	gcTableCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "gc",
			Name:      "table_total",
			Help:      "Total number of tables processed by GC.",
		}, []string{"type", "action"})

	GCTableScannedCounter   = gcTableCounter.WithLabelValues("table", "scanned")
	GCTableDeletedCounter   = gcTableCounter.WithLabelValues("table", "deleted")
	GCTableProtectedCounter = gcTableCounter.WithLabelValues("table", "protected")
	GCTableSkippedCounter   = gcTableCounter.WithLabelValues("table", "skipped")

	// GC snapshot statistics
	gcSnapshotCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "gc",
			Name:      "snapshot_total",
			Help:      "Total number of snapshots processed by GC.",
		}, []string{"level", "action"})

	GCSnapshotClusterCounter  = gcSnapshotCounter.WithLabelValues("cluster", "processed")
	GCSnapshotAccountCounter  = gcSnapshotCounter.WithLabelValues("account", "processed")
	GCSnapshotDatabaseCounter = gcSnapshotCounter.WithLabelValues("database", "processed")
	GCSnapshotTableCounter    = gcSnapshotCounter.WithLabelValues("table", "processed")

	// GC PITR statistics
	gcPitrCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "gc",
			Name:      "pitr_total",
			Help:      "Total number of PITR processed by GC.",
		}, []string{"level", "action"})

	GCPitrClusterCounter  = gcPitrCounter.WithLabelValues("cluster", "processed")
	GCPitrAccountCounter  = gcPitrCounter.WithLabelValues("account", "processed")
	GCPitrDatabaseCounter = gcPitrCounter.WithLabelValues("database", "processed")
	GCPitrTableCounter    = gcPitrCounter.WithLabelValues("table", "processed")

	// GC memory usage statistics
	gcMemoryGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "gc",
			Name:      "memory_bytes",
			Help:      "Memory usage of GC operations.",
		}, []string{"type"})

	GCMemoryBufferGauge  = gcMemoryGauge.WithLabelValues("buffer")
	GCMemoryCacheGauge   = gcMemoryGauge.WithLabelValues("cache")
	GCMemoryObjectsGauge = gcMemoryGauge.WithLabelValues("objects")

	// GC queue statistics
	gcQueueGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "gc",
			Name:      "queue_size",
			Help:      "Size of GC queues.",
		}, []string{"type"})

	GCQueuePendingGauge    = gcQueueGauge.WithLabelValues("pending")
	GCQueueProcessingGauge = gcQueueGauge.WithLabelValues("processing")
	GCQueueCompletedGauge  = gcQueueGauge.WithLabelValues("completed")

	// GC error statistics
	gcErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "gc",
			Name:      "error_total",
			Help:      "Total number of GC errors.",
		}, []string{"type", "error"})

	GCErrorFileNotFoundCounter     = gcErrorCounter.WithLabelValues("file", "not_found")
	GCErrorPermissionDeniedCounter = gcErrorCounter.WithLabelValues("file", "permission_denied")
	GCErrorIOErrorCounter          = gcErrorCounter.WithLabelValues("file", "io_error")
	GCErrorTimeoutCounter          = gcErrorCounter.WithLabelValues("operation", "timeout")
	GCErrorContextCanceledCounter  = gcErrorCounter.WithLabelValues("operation", "context_canceled")

	// GC alert metrics
	gcAlertGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "gc",
			Name:      "alert",
			Help:      "GC alert status (1 = alerting, 0 = normal).",
		}, []string{"type"})

	GCAlertNoDeletionGauge    = gcAlertGauge.WithLabelValues("no_deletion")
	GCAlertHighMemoryGauge    = gcAlertGauge.WithLabelValues("high_memory")
	GCAlertSlowExecutionGauge = gcAlertGauge.WithLabelValues("slow_execution")
	GCAlertErrorRateGauge     = gcAlertGauge.WithLabelValues("error_rate")

	// GC last execution time
	gcLastExecutionGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "gc",
			Name:      "last_execution_timestamp",
			Help:      "Timestamp of last GC execution.",
		}, []string{"type"})

	GCLastCheckpointExecutionGauge = gcLastExecutionGauge.WithLabelValues("checkpoint")
	GCLastMergeExecutionGauge      = gcLastExecutionGauge.WithLabelValues("merge")
	GCLastSnapshotExecutionGauge   = gcLastExecutionGauge.WithLabelValues("snapshot")

	// GC last deletion time
	gcLastDeletionGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "gc",
			Name:      "last_deletion_timestamp",
			Help:      "Timestamp of last GC file deletion.",
		}, []string{"type"})

	GCLastDataDeletionGauge       = gcLastDeletionGauge.WithLabelValues("data")
	GCLastTombstoneDeletionGauge  = gcLastDeletionGauge.WithLabelValues("tombstone")
	GCLastCheckpointDeletionGauge = gcLastDeletionGauge.WithLabelValues("checkpoint")
	GCLastMetaDeletionGauge       = gcLastDeletionGauge.WithLabelValues("meta")
	GCLastSnapshotDeletionGauge   = gcLastDeletionGauge.WithLabelValues("snapshot")
)

func initGCMetrics() {
	registry.MustRegister(gcExecutionCounter)
	registry.MustRegister(gcFileDeletionCounter)
	registry.MustRegister(gcFileSizeHistogram)
	registry.MustRegister(gcDurationHistogram)
	registry.MustRegister(gcObjectCounter)
	registry.MustRegister(gcTableCounter)
	registry.MustRegister(gcSnapshotCounter)
	registry.MustRegister(gcPitrCounter)
	registry.MustRegister(gcMemoryGauge)
	registry.MustRegister(gcQueueGauge)
	registry.MustRegister(gcErrorCounter)
	registry.MustRegister(gcAlertGauge)
	registry.MustRegister(gcLastExecutionGauge)
	registry.MustRegister(gcLastDeletionGauge)
}
