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
	GCCheckpointFileDeletionCounter = gcFileDeletionCounter.WithLabelValues("checkpoint", "merged")
	GCMetaFileDeletionCounter       = gcFileDeletionCounter.WithLabelValues("meta", "stale")
	GCSnapshotFileDeletionCounter   = gcFileDeletionCounter.WithLabelValues("snapshot", "stale")

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
	GCTableProtectedCounter = gcTableCounter.WithLabelValues("table", "protected")

	// GC memory usage statistics
	gcMemoryGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "gc",
			Name:      "memory_bytes",
			Help:      "Memory usage of GC operations.",
		}, []string{"type"})

	GCMemoryBufferGauge  = gcMemoryGauge.WithLabelValues("buffer")
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

	GCErrorIOErrorCounter = gcErrorCounter.WithLabelValues("file", "io_error")

	// GC checkpoint statistics
	gcCheckpointCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "gc",
			Name:      "checkpoint_total",
			Help:      "Total number of checkpoints processed by GC.",
		}, []string{"action"})

	GCCheckpointMergedCounter = gcCheckpointCounter.WithLabelValues("merged")
	GCCheckpointDeletedCounter = gcCheckpointCounter.WithLabelValues("deleted")

	// GC checkpoint row statistics
	gcCheckpointRowCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "gc",
			Name:      "checkpoint_rows_total",
			Help:      "Total number of checkpoint rows processed by GC.",
		}, []string{"type"})

	GCCheckpointRowsMergedCounter = gcCheckpointRowCounter.WithLabelValues("merged")
	GCCheckpointRowsScannedCounter = gcCheckpointRowCounter.WithLabelValues("scanned")

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
	GCLastCheckpointDeletionGauge = gcLastDeletionGauge.WithLabelValues("checkpoint")
	GCLastMetaDeletionGauge       = gcLastDeletionGauge.WithLabelValues("meta")
	GCLastSnapshotDeletionGauge   = gcLastDeletionGauge.WithLabelValues("snapshot")
)

func initGCMetrics() {
	registry.MustRegister(gcExecutionCounter)
	registry.MustRegister(gcFileDeletionCounter)
	registry.MustRegister(gcDurationHistogram)
	registry.MustRegister(gcObjectCounter)
	registry.MustRegister(gcTableCounter)
	registry.MustRegister(gcMemoryGauge)
	registry.MustRegister(gcQueueGauge)
	registry.MustRegister(gcErrorCounter)
	registry.MustRegister(gcCheckpointCounter)
	registry.MustRegister(gcCheckpointRowCounter)
	registry.MustRegister(gcLastExecutionGauge)
	registry.MustRegister(gcLastDeletionGauge)
}
