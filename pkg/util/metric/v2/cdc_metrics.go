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

	// CdcSinkerRetryCounter tracks retry attempts
	CdcSinkerRetryCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "cdc",
			Name:      "sinker_retry_total",
			Help:      "Total number of sinker retry attempts",
		}, []string{"reason"})
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

	// Sinker metrics
	registry.MustRegister(CdcSinkerTransactionCounter)
	registry.MustRegister(CdcSinkerSQLCounter)
	registry.MustRegister(CdcSinkerSQLDuration)
	registry.MustRegister(CdcSinkerRetryCounter)

	// Health metrics
	registry.MustRegister(CdcHeartbeatCounter)
	registry.MustRegister(CdcTableStuckGauge)
	registry.MustRegister(CdcTableLastActivityTimestamp)

	// Performance metrics
	registry.MustRegister(CdcThroughputRowsPerSecond)
	registry.MustRegister(CdcLatencyHistogram)
}
