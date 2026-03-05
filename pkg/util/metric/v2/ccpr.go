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

// ============================================================================
// CCPR Task Counters
// ============================================================================

var (
	ccprTaskCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "ccpr",
			Name:      "task_total",
			Help:      "Total number of CCPR tasks by status.",
		}, []string{"status"})
	CCPRTaskPendingCounter   = ccprTaskCounter.WithLabelValues("pending")
	CCPRTaskRunningCounter   = ccprTaskCounter.WithLabelValues("running")
	CCPRTaskCompletedCounter = ccprTaskCounter.WithLabelValues("completed")
	CCPRTaskErrorCounter     = ccprTaskCounter.WithLabelValues("error")
	CCPRTaskCanceledCounter  = ccprTaskCounter.WithLabelValues("canceled")
)

// ============================================================================
// CCPR Iteration Counters
// ============================================================================

var (
	ccprIterationCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "ccpr",
			Name:      "iteration_total",
			Help:      "Total number of CCPR iterations by status.",
		}, []string{"status"})
	CCPRIterationStartedCounter   = ccprIterationCounter.WithLabelValues("started")
	CCPRIterationCompletedCounter = ccprIterationCounter.WithLabelValues("completed")
	CCPRIterationErrorCounter     = ccprIterationCounter.WithLabelValues("error")
)

// ============================================================================
// CCPR Object Processing Counters
// ============================================================================

var (
	ccprObjectCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "ccpr",
			Name:      "object_total",
			Help:      "Total number of objects processed by type and operation.",
		}, []string{"type", "operation"})
	CCPRDataInsertCounter      = ccprObjectCounter.WithLabelValues("data", "insert")
	CCPRDataDeleteCounter      = ccprObjectCounter.WithLabelValues("data", "delete")
	CCPRTombstoneInsertCounter = ccprObjectCounter.WithLabelValues("tombstone", "insert")
	CCPRTombstoneDeleteCounter = ccprObjectCounter.WithLabelValues("tombstone", "delete")

	ccprObjectBytesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "ccpr",
			Name:      "object_bytes_total",
			Help:      "Total bytes of objects processed.",
		}, []string{"type"})
	CCPRObjectReadBytesCounter  = ccprObjectBytesCounter.WithLabelValues("read")
	CCPRObjectWriteBytesCounter = ccprObjectBytesCounter.WithLabelValues("write")
)

// ============================================================================
// CCPR Job Counters
// ============================================================================

var (
	ccprJobCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "ccpr",
			Name:      "job_total",
			Help:      "Total number of jobs by type and status.",
		}, []string{"type", "status"})
	CCPRFilterObjectJobCompletedCounter = ccprJobCounter.WithLabelValues("filter_object", "completed")
	CCPRFilterObjectJobErrorCounter     = ccprJobCounter.WithLabelValues("filter_object", "error")
	CCPRGetChunkJobCompletedCounter     = ccprJobCounter.WithLabelValues("get_chunk", "completed")
	CCPRGetChunkJobErrorCounter         = ccprJobCounter.WithLabelValues("get_chunk", "error")
	CCPRWriteObjectJobCompletedCounter  = ccprJobCounter.WithLabelValues("write_object", "completed")
	CCPRWriteObjectJobErrorCounter      = ccprJobCounter.WithLabelValues("write_object", "error")
	CCPRGetMetaJobCompletedCounter      = ccprJobCounter.WithLabelValues("get_meta", "completed")
	CCPRGetMetaJobErrorCounter          = ccprJobCounter.WithLabelValues("get_meta", "error")
)

// ============================================================================
// CCPR Error and Retry Counters
// ============================================================================

var (
	ccprErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "ccpr",
			Name:      "error_total",
			Help:      "Total number of errors by type.",
		}, []string{"type"})
	CCPRRetryableErrorCounter    = ccprErrorCounter.WithLabelValues("retryable")
	CCPRNonRetryableErrorCounter = ccprErrorCounter.WithLabelValues("non_retryable")
	CCPRTimeoutErrorCounter      = ccprErrorCounter.WithLabelValues("timeout")
	CCPRNetworkErrorCounter      = ccprErrorCounter.WithLabelValues("network")

	CCPRRetryCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "ccpr",
			Name:      "retry_total",
			Help:      "Total number of retries.",
		})
)

// ============================================================================
// CCPR DDL Counters
// ============================================================================

var (
	ccprDDLCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "ccpr",
			Name:      "ddl_total",
			Help:      "Total number of DDL operations by type.",
		}, []string{"type"})
	CCPRDDLCreateCounter = ccprDDLCounter.WithLabelValues("create")
	CCPRDDLAlterCounter  = ccprDDLCounter.WithLabelValues("alter")
	CCPRDDLDropCounter   = ccprDDLCounter.WithLabelValues("drop")
)

// ============================================================================
// CCPR Duration Histograms
// ============================================================================

var (
	ccprIterationDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "ccpr",
			Name:      "iteration_duration_seconds",
			Help:      "Bucketed histogram of iteration duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"step"})
	CCPRIterationTotalDurationHistogram      = ccprIterationDurationHistogram.WithLabelValues("total")
	CCPRIterationInitDurationHistogram       = ccprIterationDurationHistogram.WithLabelValues("init")
	CCPRIterationDDLDurationHistogram        = ccprIterationDurationHistogram.WithLabelValues("ddl")
	CCPRIterationSnapshotDurationHistogram   = ccprIterationDurationHistogram.WithLabelValues("snapshot")
	CCPRIterationObjectListDurationHistogram = ccprIterationDurationHistogram.WithLabelValues("object_list")
	CCPRIterationApplyDurationHistogram      = ccprIterationDurationHistogram.WithLabelValues("apply")
	CCPRIterationCommitDurationHistogram     = ccprIterationDurationHistogram.WithLabelValues("commit")
)

var (
	ccprJobDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "ccpr",
			Name:      "job_duration_seconds",
			Help:      "Bucketed histogram of job duration by type.",
			Buckets:   getDurationBuckets(),
		}, []string{"type"})
	CCPRFilterObjectJobDurationHistogram = ccprJobDurationHistogram.WithLabelValues("filter_object")
	CCPRGetChunkJobDurationHistogram     = ccprJobDurationHistogram.WithLabelValues("get_chunk")
	CCPRWriteObjectJobDurationHistogram  = ccprJobDurationHistogram.WithLabelValues("write_object")
	CCPRGetMetaJobDurationHistogram      = ccprJobDurationHistogram.WithLabelValues("get_meta")
)

var (
	CCPRObjectSizeBytesHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "ccpr",
			Name:      "object_size_bytes",
			Help:      "Bucketed histogram of object sizes in bytes.",
			Buckets:   prometheus.ExponentialBuckets(1024, 2.0, 20), // 1KB to 1GB
		})

	CCPRChunkSizeBytesHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "ccpr",
			Name:      "chunk_size_bytes",
			Help:      "Bucketed histogram of chunk sizes in bytes.",
			Buckets:   prometheus.ExponentialBuckets(1024, 2.0, 20), // 1KB to 1GB
		})
)

// ============================================================================
// CCPR Queue Size Gauges
// ============================================================================

var (
	ccprQueueSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "ccpr",
			Name:      "queue_size",
			Help:      "Current size of CCPR queues.",
		}, []string{"type"})
	CCPRFilterObjectQueueSizeGauge = ccprQueueSizeGauge.WithLabelValues("filter_object")
	CCPRGetChunkQueueSizeGauge     = ccprQueueSizeGauge.WithLabelValues("get_chunk")
	CCPRWriteObjectQueueSizeGauge  = ccprQueueSizeGauge.WithLabelValues("write_object")
	CCPRPublicationQueueSizeGauge  = ccprQueueSizeGauge.WithLabelValues("publication")
)

// ============================================================================
// CCPR Running Gauges
// ============================================================================

var (
	ccprRunningGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "ccpr",
			Name:      "running",
			Help:      "Current number of running jobs/tasks.",
		}, []string{"type"})
	CCPRRunningTasksGauge            = ccprRunningGauge.WithLabelValues("tasks")
	CCPRRunningIterationsGauge       = ccprRunningGauge.WithLabelValues("iterations")
	CCPRRunningFilterObjectJobsGauge = ccprRunningGauge.WithLabelValues("filter_object_jobs")
	CCPRRunningGetChunkJobsGauge     = ccprRunningGauge.WithLabelValues("get_chunk_jobs")
	CCPRRunningWriteObjectJobsGauge  = ccprRunningGauge.WithLabelValues("write_object_jobs")
)

// ============================================================================
// CCPR AObject Map Gauge
// ============================================================================

var (
	CCPRAObjectMapSizeGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "ccpr",
			Name:      "aobject_map_size",
			Help:      "Current size of AObject mapping table.",
		})
)

// ============================================================================
// CCPR Snapshot Metrics
// ============================================================================

var (
	ccprSnapshotCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "ccpr",
			Name:      "snapshot_total",
			Help:      "Total number of snapshot operations.",
		}, []string{"operation"})
	CCPRSnapshotCreateCounter  = ccprSnapshotCounter.WithLabelValues("create")
	CCPRSnapshotWaitCounter    = ccprSnapshotCounter.WithLabelValues("wait")
	CCPRSnapshotCleanupCounter = ccprSnapshotCounter.WithLabelValues("cleanup")
)

// ============================================================================
// CCPR GC Metrics
// ============================================================================

var (
	CCPRGCRunCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "ccpr",
			Name:      "gc_run_total",
			Help:      "Total number of GC runs.",
		})

	CCPRGCDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "ccpr",
			Name:      "gc_duration_seconds",
			Help:      "Bucketed histogram of GC duration.",
			Buckets:   getDurationBuckets(),
		})
)

// ============================================================================
// CCPR Sync Protection Metrics
// ============================================================================

var (
	ccprSyncProtectionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "ccpr",
			Name:      "sync_protection_total",
			Help:      "Total number of sync protection operations.",
		}, []string{"operation"})
	CCPRSyncProtectionRegisterCounter   = ccprSyncProtectionCounter.WithLabelValues("register")
	CCPRSyncProtectionRenewCounter      = ccprSyncProtectionCounter.WithLabelValues("renew")
	CCPRSyncProtectionUnregisterCounter = ccprSyncProtectionCounter.WithLabelValues("unregister")
	CCPRSyncProtectionExpiredCounter    = ccprSyncProtectionCounter.WithLabelValues("expired")
)
