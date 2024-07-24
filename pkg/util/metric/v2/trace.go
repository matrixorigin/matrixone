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
			Help:      "Count of trace collector discard total.",
		}, []string{"type"})

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
)

func GetTraceNegativeCUCounter(typ string) prometheus.Counter {
	return traceNegativeCUCounter.WithLabelValues(typ)
}

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
