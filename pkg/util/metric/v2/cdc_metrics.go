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

import (
	"github.com/prometheus/client_golang/prometheus"
)

// CDC Task Lifecycle Metrics
var (
	// CdcTaskTotalGauge tracks total number of CDC tasks by state
	CdcTaskTotalGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "task_total",
			Help:      "Total number of CDC tasks by state (running, paused, failed)",
		}, []string{"state"})

	// CdcTaskStateChangeCounter tracks task state changes
	CdcTaskStateChangeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "task_state_change_total",
			Help:      "Total number of CDC task state changes",
		}, []string{"from_state", "to_state"})

	// CdcTaskErrorCounter tracks task errors by type
	CdcTaskErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "task_error_total",
			Help:      "Total number of CDC task errors by type",
		}, []string{"error_type", "retryable"})
)

// CDC Table Stream Metrics
var (
	// CdcTableStreamTotalGauge tracks active table streams
	CdcTableStreamTotalGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "table_stream_total",
			Help:      "Total number of active CDC table streams by state",
		}, []string{"state"})

	// CdcTableStreamRoundCounter tracks processing rounds
	CdcTableStreamRoundCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "table_stream_round_total",
			Help:      "Total number of table stream processing rounds",
		}, []string{"table", "status"})

	// CdcTableStreamRoundDuration tracks round processing duration
	CdcTableStreamRoundDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "table_stream_round_duration_seconds",
			Help:      "Duration of table stream processing rounds",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms to 16s
		}, []string{"table"})

	// CdcTableStreamRetryCounter tracks retry attempts by error type and outcome
	CdcTableStreamRetryCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "table_stream_retry_total",
			Help:      "Total number of table stream retry attempts by error type and outcome",
		}, []string{"table", "error_type", "outcome"}) // outcome: attempted, succeeded, exhausted, failed

	// CdcTableStreamRetryDelayHistogram tracks retry backoff delays
	CdcTableStreamRetryDelayHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "table_stream_retry_delay_seconds",
			Help:      "Retry backoff delay duration for table streams",
			Buckets:   prometheus.ExponentialBuckets(0.005, 2, 12), // 5ms to 10s
		}, []string{"table", "error_type"})

	// CdcTableStreamAuxiliaryErrorCounter tracks auxiliary errors that don't replace original errors
	CdcTableStreamAuxiliaryErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "table_stream_auxiliary_error_total",
			Help:      "Total number of auxiliary errors encountered during retries (preserved original error)",
		}, []string{"table", "auxiliary_error_type"})

	// CdcTableStreamOriginalErrorPreservedCounter tracks when original errors are preserved
	CdcTableStreamOriginalErrorPreservedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "table_stream_original_error_preserved_total",
			Help:      "Total number of times original error was preserved during retries",
		}, []string{"table", "original_error_type"})
)

// CDC Data Processing Metrics
var (
	// CdcRowsProcessedCounter tracks rows processed (read and sink separately)
	CdcRowsProcessedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "rows_processed_total",
			Help:      "Total number of rows processed by CDC",
		}, []string{"operation", "table"}) // operation: read, insert, delete

	// CdcBytesProcessedCounter tracks bytes processed
	CdcBytesProcessedCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "bytes_processed_total",
			Help:      "Total number of bytes processed by CDC",
		}, []string{"operation", "table"})

	// CdcBatchSizeHistogram tracks batch sizes
	CdcBatchSizeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "batch_size_rows",
			Help:      "Distribution of CDC batch sizes in rows",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 20), // 1 to 1M rows
		}, []string{"type"}) // type: snapshot, tail
)

// CDC Watermark Metrics
var (
	// CdcWatermarkUpdateCounter tracks watermark updates
	CdcWatermarkUpdateCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "watermark_update_total",
			Help:      "Total number of watermark updates",
		}, []string{"table", "update_type"}) // update_type: commit, heartbeat

	// CdcWatermarkLagSeconds tracks watermark lag (current time - watermark time)
	CdcWatermarkLagSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "watermark_lag_seconds",
			Help:      "Time lag between current time and watermark",
		}, []string{"table"})

	// CdcWatermarkLagRatio tracks the ratio of actual lag to expected lag
	// Value < 2: normal, 2-5: warning, > 5: critical
	// This metric is frequency-agnostic and suitable for unified alerting
	CdcWatermarkLagRatio = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "watermark_lag_ratio",
			Help:      "Ratio of actual watermark lag to expected lag (based on task frequency)",
		}, []string{"table"})

	// CdcWatermarkCommitDuration tracks watermark commit duration
	CdcWatermarkCommitDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "watermark_commit_duration_seconds",
			Help:      "Duration of watermark commit to database",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15), // 0.1ms to 1.6s
		})

	// CdcWatermarkCacheGauge tracks watermark cache sizes
	CdcWatermarkCacheGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "watermark_cache_size",
			Help:      "Number of watermarks in each cache tier",
		}, []string{"tier"}) // tier: uncommitted, committing, committed

	// CdcWatermarkCommitErrorCounter tracks commit failures by reason
	CdcWatermarkCommitErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "watermark_commit_error_total",
			Help:      "Total number of watermark commit errors grouped by reason",
		}, []string{"reason"}) // reason: sql, circuit_skip

	// CdcWatermarkCircuitEventCounter tracks circuit breaker events
	CdcWatermarkCircuitEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "watermark_circuit_event_total",
			Help:      "Total number of watermark circuit breaker events grouped by type",
		}, []string{"event"}) // event: opened, reset, skip

	// CdcWatermarkCircuitOpenGauge tracks the number of open circuits
	CdcWatermarkCircuitOpenGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "watermark_circuit_open_total",
			Help:      "Current number of watermark circuit breakers that are open",
		})
)

// CDC Sinker Metrics
var (
	// CdcSinkerTransactionCounter tracks transaction operations
	CdcSinkerTransactionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "sinker_transaction_total",
			Help:      "Total number of sinker transaction operations",
		}, []string{"operation", "status"}) // operation: begin, commit, rollback; status: success, error

	// CdcSinkerSQLCounter tracks SQL execution
	CdcSinkerSQLCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "sinker_sql_total",
			Help:      "Total number of SQLs executed by sinker",
		}, []string{"sql_type", "status"}) // sql_type: insert, delete, ddl

	// CdcSinkerSQLDuration tracks SQL execution duration
	CdcSinkerSQLDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "sinker_sql_duration_seconds",
			Help:      "Duration of SQL execution in sinker",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 15),
		}, []string{"sql_type"})

	// CdcSinkerRetryCounter tracks retry attempts and outcomes
	CdcSinkerRetryCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "sinker_retry_total",
			Help:      "Total number of sinker retry attempts grouped by sink type, reason and result",
		}, []string{"sink", "reason", "result"})

	// CdcSinkerCircuitStateGauge tracks circuit breaker state per sink
	CdcSinkerCircuitStateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "sinker_circuit_state",
			Help:      "Circuit breaker state for sinkers (0=closed, 1=open)",
		}, []string{"sink"})
)

// CDC Health Metrics
var (
	// CdcHeartbeatCounter tracks heartbeat updates (watermark advance without data)
	CdcHeartbeatCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "heartbeat_total",
			Help:      "Total number of heartbeat watermark updates (no data)",
		}, []string{"table"})

	// CdcTableStuckGauge indicates tables with stuck watermark
	CdcTableStuckGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "table_stuck",
			Help:      "Tables with watermark stuck for >threshold time (1=stuck, 0=normal)",
		}, []string{"table"})

	// CdcTableLastActivityTimestamp tracks last activity time for each table
	CdcTableLastActivityTimestamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "table_last_activity_timestamp",
			Help:      "Unix timestamp of last activity for each table",
		}, []string{"table"})

	// CdcTableNoProgressCounter counts rounds where snapshot did not advance
	CdcTableNoProgressCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "table_snapshot_no_progress_total",
			Help:      "Number of times a table round observed snapshot timestamp not advancing",
		}, []string{"table"})
)

// CDC Initial Sync Metrics
var (
	// CdcInitialSyncStatusGauge tracks initial sync status per table
	CdcInitialSyncStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "initial_sync_status",
			Help:      "Initial synchronization status per table (0=not started, 1=running, 2=success, 3=failed)",
		}, []string{"table"})

	// CdcInitialSyncStartTimestamp records initial sync start time
	CdcInitialSyncStartTimestamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "initial_sync_start_timestamp",
			Help:      "Unix timestamp when initial synchronization started",
		}, []string{"table"})

	// CdcInitialSyncEndTimestamp records initial sync end time
	CdcInitialSyncEndTimestamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "initial_sync_end_timestamp",
			Help:      "Unix timestamp when initial synchronization finished (success or failure)",
		}, []string{"table"})

	// CdcInitialSyncDurationHistogram tracks initial sync duration
	CdcInitialSyncDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "initial_sync_duration_seconds",
			Help:      "Duration of CDC initial synchronization for a table",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 20), // 1s to ~2^19 s (~6 days)
		}, []string{"table"})

	// CdcInitialSyncRowsGauge tracks number of rows processed during initial sync
	CdcInitialSyncRowsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "initial_sync_rows",
			Help:      "Total rows processed during initial synchronization",
		}, []string{"table"})

	// CdcInitialSyncBytesGauge tracks estimated bytes processed during initial sync
	CdcInitialSyncBytesGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "initial_sync_bytes",
			Help:      "Estimated bytes processed during initial synchronization",
		}, []string{"table"})

	// CdcInitialSyncSQLGauge tracks SQL statements executed during initial sync
	CdcInitialSyncSQLGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "initial_sync_sql_total",
			Help:      "Number of SQL statements executed during initial synchronization",
		}, []string{"table"})
)

// CDC Performance Metrics
var (
	// CdcThroughputRowsPerSecond tracks current throughput
	CdcThroughputRowsPerSecond = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "throughput_rows_per_second",
			Help:      "Current CDC throughput in rows per second",
		}, []string{"table"})

	// CdcLatencyHistogram tracks end-to-end latency (write to source -> sink to target)
	CdcLatencyHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "latency_seconds",
			Help:      "End-to-end CDC latency from source write to target sink",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 15), // 10ms to 163s
		}, []string{"table"})
)

func init() {
	initCDCMetrics()
}

func initCDCMetrics() {
	// Task metrics
	registry.MustRegister(CdcTaskTotalGauge)
	registry.MustRegister(CdcTaskStateChangeCounter)
	registry.MustRegister(CdcTaskErrorCounter)

	// Table stream metrics
	registry.MustRegister(CdcTableStreamTotalGauge)
	registry.MustRegister(CdcTableStreamRoundCounter)
	registry.MustRegister(CdcTableStreamRoundDuration)
	registry.MustRegister(CdcTableStreamRetryCounter)
	registry.MustRegister(CdcTableStreamRetryDelayHistogram)
	registry.MustRegister(CdcTableStreamAuxiliaryErrorCounter)
	registry.MustRegister(CdcTableStreamOriginalErrorPreservedCounter)

	// Data processing metrics
	registry.MustRegister(CdcRowsProcessedCounter)
	registry.MustRegister(CdcBytesProcessedCounter)
	registry.MustRegister(CdcBatchSizeHistogram)

	// Watermark metrics
	registry.MustRegister(CdcWatermarkUpdateCounter)
	registry.MustRegister(CdcWatermarkLagSeconds)
	registry.MustRegister(CdcWatermarkLagRatio)
	registry.MustRegister(CdcWatermarkCommitDuration)
	registry.MustRegister(CdcWatermarkCacheGauge)
	registry.MustRegister(CdcWatermarkCommitErrorCounter)
	registry.MustRegister(CdcWatermarkCircuitEventCounter)
	registry.MustRegister(CdcWatermarkCircuitOpenGauge)

	// Sinker metrics
	registry.MustRegister(CdcSinkerTransactionCounter)
	registry.MustRegister(CdcSinkerSQLCounter)
	registry.MustRegister(CdcSinkerSQLDuration)
	registry.MustRegister(CdcSinkerRetryCounter)
	registry.MustRegister(CdcSinkerCircuitStateGauge)

	// Health metrics
	registry.MustRegister(CdcHeartbeatCounter)
	registry.MustRegister(CdcTableStuckGauge)
	registry.MustRegister(CdcTableLastActivityTimestamp)
	registry.MustRegister(CdcTableNoProgressCounter)

	// Initial sync metrics
	registry.MustRegister(CdcInitialSyncStatusGauge)
	registry.MustRegister(CdcInitialSyncStartTimestamp)
	registry.MustRegister(CdcInitialSyncEndTimestamp)
	registry.MustRegister(CdcInitialSyncDurationHistogram)
	registry.MustRegister(CdcInitialSyncRowsGauge)
	registry.MustRegister(CdcInitialSyncBytesGauge)
	registry.MustRegister(CdcInitialSyncSQLGauge)

	// Performance metrics
	registry.MustRegister(CdcThroughputRowsPerSecond)
	registry.MustRegister(CdcLatencyHistogram)
}
