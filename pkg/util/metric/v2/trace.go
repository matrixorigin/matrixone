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

// trace.go observe motrace, mometric those packages' behavior

func initTraceMetrics() {
	registry.MustRegister(traceCollectorDurationHistogram)
	registry.MustRegister(traceCollectorDiscardCounter)
	registry.MustRegister(traceCollectorCollectHungCounter)
	registry.MustRegister(traceCollectorDiscardItemCounter)
	registry.MustRegister(traceCollectorStatusCounter)
	registry.MustRegister(traceCollectorQueueLength)
	registry.MustRegister(traceNegativeCUCounter)
	registry.MustRegister(traceETLMergeCounter)
	registry.MustRegister(traceMOLoggerExportDataHistogram)
	registry.MustRegister(traceCheckStorageUsageCounter)
	registry.MustRegister(traceMOLoggerErrorCounter)

	// debug
	registry.MustRegister(traceMOLoggerMetricBufferCounter)
}

var (
	traceCollectorDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "trace",
			Name:      "collector_duration_seconds",
			Help:      "Bucketed histogram of trace collector duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2.0, 20),
		}, []string{"type"})
	TraceCollectorCollectDurationHistogram              = traceCollectorDurationHistogram.WithLabelValues("collect")
	TraceCollectorConsumeDurationHistogram              = traceCollectorDurationHistogram.WithLabelValues("consume")
	TraceCollectorConsumeDelayDurationHistogram         = traceCollectorDurationHistogram.WithLabelValues("consume_delay")
	TraceCollectorGenerateAwareDurationHistogram        = traceCollectorDurationHistogram.WithLabelValues("generate_awake")
	TraceCollectorGenerateAwareDiscardDurationHistogram = traceCollectorDurationHistogram.WithLabelValues("generate_awake_discard")
	TraceCollectorGenerateDelayDurationHistogram        = traceCollectorDurationHistogram.WithLabelValues("generate_delay")
	TraceCollectorGenerateDurationHistogram             = traceCollectorDurationHistogram.WithLabelValues("generate")
	TraceCollectorGenerateDiscardDurationHistogram      = traceCollectorDurationHistogram.WithLabelValues("generate_discard")
	TraceCollectorExportDurationHistogram               = traceCollectorDurationHistogram.WithLabelValues("export")

	traceCollectorDiscardCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "trace",
			Name:      "collector_discard_total",
			Help:      "Count of trace collector discard wait-generate total.",
		}, []string{"type"})

	traceCollectorCollectHungCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "trace",
			Name:      "collector_collect_hung_total",
			Help:      "Count of trace collector hung collect total",
		}, []string{"type"})

	traceCollectorDiscardItemCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "trace",
			Name:      "collector_discard_item_total",
			Help:      "Count of trace collector discard item total.",
		}, []string{"type"})

	traceCollectorStatusCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "trace",
			Name:      "collector_status_total",
			Help:      "Count of trace collector discard total.",
		}, []string{"type"})
	TraceCollectorDisposedCounter = traceCollectorStatusCounter.WithLabelValues("disposed")
	TraceCollectorTimeoutCounter  = traceCollectorStatusCounter.WithLabelValues("timeout")
	TraceCollectorEmptyCounter    = traceCollectorStatusCounter.WithLabelValues("empty")

	traceCollectorQueueLength = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "trace",
			Name:      "collector_queue_length",
			Help:      "The itmes that mologger collector queue hold.",
		})

	traceNegativeCUCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "trace",
			Name:      "negative_cu_total",
			Help:      "Count of negative cu to backend",
		}, []string{"type"})

	traceETLMergeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "trace",
			Name:      "etl_merge_total",
			Help:      "Count of background task ETLMerge",
		}, []string{"type"})
	TraceETLMergeSuccessCounter = traceETLMergeCounter.WithLabelValues("success")
	// TraceETLMergeExistCounter record already exist, against delete failed.
	TraceETLMergeExistCounter        = traceETLMergeCounter.WithLabelValues("exist")
	TraceETLMergeOpenFailedCounter   = traceETLMergeCounter.WithLabelValues("open_failed")
	TraceETLMergeReadFailedCounter   = traceETLMergeCounter.WithLabelValues("read_failed")
	TraceETLMergeParseFailedCounter  = traceETLMergeCounter.WithLabelValues("parse_failed")
	TraceETLMergeWriteFailedCounter  = traceETLMergeCounter.WithLabelValues("write_failed")
	TraceETLMergeDeleteFailedCounter = traceETLMergeCounter.WithLabelValues("delete_failed")

	traceMOLoggerExportDataHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "trace",
			Name:      "mologger_export_data_bytes",
			Help:      "Bucketed histogram of mo_logger exec sql bytes, or write bytes.",
			Buckets:   prometheus.ExponentialBuckets(128, 2.0, 20),
		}, []string{"type"})
	TraceMOLoggerExportSqlHistogram = traceMOLoggerExportDataHistogram.WithLabelValues("sql")
	TraceMOLoggerExportCsvHistogram = traceMOLoggerExportDataHistogram.WithLabelValues("csv")

	traceCheckStorageUsageCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "trace",
			Name:      "check_storage_usage_total",
			Help:      "Count of cron_task MetricStorageUsage.",
		}, []string{"type"})

	traceMOLoggerErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "trace",
			Name:      "mologger_error_total",
			Help:      "Count of mologger error",
		}, []string{"type"})
	TraceMOLoggerErrorWriteItemCounter = traceMOLoggerErrorCounter.WithLabelValues("write_item")
	TraceMOLoggerErrorFlushCounter     = traceMOLoggerErrorCounter.WithLabelValues("flush")
	TraceMOLoggerErrorConnDBCounter    = traceMOLoggerErrorCounter.WithLabelValues("conn_db")

	traceMOLoggerMetricBufferCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "trace",
			Name:      "mologger_metric_buffer_total",
			Help:      "Count of mologger metric used buffer count",
		}, []string{"type"})
	TraceMOLoggerBufferAlloc            = traceMOLoggerMetricBufferCounter.WithLabelValues("alloc")
	TraceMOLoggerBufferFree             = traceMOLoggerMetricBufferCounter.WithLabelValues("free")
	TraceMOLoggerBufferSetCallBack      = traceMOLoggerMetricBufferCounter.WithLabelValues("set_callback")
	TraceMOLoggerBufferSetCallBackNil   = traceMOLoggerMetricBufferCounter.WithLabelValues("set_callback_nil")
	TraceMOLoggerBufferNoFree           = traceMOLoggerMetricBufferCounter.WithLabelValues("no_free")
	TraceMOLoggerBufferWriteSQL         = traceMOLoggerMetricBufferCounter.WithLabelValues("write_sql")
	TraceMOLoggerBufferWriteCSV         = traceMOLoggerMetricBufferCounter.WithLabelValues("write_csv")
	TraceMOLoggerBufferWriteFailed      = traceMOLoggerMetricBufferCounter.WithLabelValues("write_failed")
	TraceMOLoggerBufferReactWrite       = traceMOLoggerMetricBufferCounter.WithLabelValues("react_write")
	TraceMOLoggerBufferReactWriteFailed = traceMOLoggerMetricBufferCounter.WithLabelValues("react_write_failed")
)

func GetTraceNegativeCUCounter(typ string) prometheus.Counter {
	return traceNegativeCUCounter.WithLabelValues(typ)
}

// GetTraceCollectorDiscardCounter count wait-generate discard.
func GetTraceCollectorDiscardCounter(typ string) prometheus.Counter {
	return traceCollectorDiscardCounter.WithLabelValues(typ)
}

func GetTraceCheckStorageUsageAllCounter() prometheus.Counter {
	return traceCheckStorageUsageCounter.WithLabelValues("all")
}
func GetTraceCheckStorageUsageNewCounter() prometheus.Counter {
	return traceCheckStorageUsageCounter.WithLabelValues("new")
}
func GetTraceCheckStorageUsageNewIncCounter() prometheus.Counter {
	return traceCheckStorageUsageCounter.WithLabelValues("inc")
}

func GetTraceCollectorDiscardItemCounter(typ string) prometheus.Counter {
	return traceCollectorDiscardItemCounter.WithLabelValues(typ)
}

func GetTraceCollectorCollectHungCounter(typ string) prometheus.Counter {
	return traceCollectorCollectHungCounter.WithLabelValues(typ)
}

func GetTraceCollectorQueueLength() prometheus.Gauge { return traceCollectorQueueLength }
