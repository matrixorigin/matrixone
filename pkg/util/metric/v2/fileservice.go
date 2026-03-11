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
	fsReadCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "read_total",
			Help:      "Total number of read count.",
		}, []string{"type"})
	FSReadS3Counter        = fsReadCounter.WithLabelValues("s3")
	FSReadLocalCounter     = fsReadCounter.WithLabelValues("local")
	FSReadHitMemCounter    = fsReadCounter.WithLabelValues("hit-mem")
	FSReadHitDiskCounter   = fsReadCounter.WithLabelValues("hit-disk")
	FSReadHitRemoteCounter = fsReadCounter.WithLabelValues("hit-remote")
	FSReadHitMetaCounter   = fsReadCounter.WithLabelValues("hit-meta")
	FSReadReadMemCounter   = fsReadCounter.WithLabelValues("read-mem")
	FSReadReadDiskCounter  = fsReadCounter.WithLabelValues("read-disk")
	FSReadReadMetaCounter  = fsReadCounter.WithLabelValues("read-meta")
)

var (
	s3IOBytesHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "s3_io_bytes",
			Help:      "Bucketed histogram of s3 io bytes.",
			Buckets:   prometheus.ExponentialBuckets(1, 2.0, 10),
		}, []string{"type"})
	S3WriteIOBytesHistogram = s3IOBytesHistogram.WithLabelValues("write")
	S3ReadIOBytesHistogram  = s3IOBytesHistogram.WithLabelValues("read")

	s3ConnDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "s3_conn_duration_seconds",
			Help:      "Bucketed histogram of s3 get conn duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"type"})
	S3GetConnDurationHistogram          = s3ConnDurationHistogram.WithLabelValues("get-conn")
	S3GotFirstResponseDurationHistogram = s3ConnDurationHistogram.WithLabelValues("got-first-response")
	S3DNSResolveDurationHistogram       = s3ConnDurationHistogram.WithLabelValues("dns-resolve")
	S3ConnectDurationHistogram          = s3ConnDurationHistogram.WithLabelValues("connect")
	S3TLSHandshakeDurationHistogram     = s3ConnDurationHistogram.WithLabelValues("tls-handshake")

	localIOBytesHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "local_io_bytes",
			Help:      "Bucketed histogram of local io bytes.",
			Buckets:   prometheus.ExponentialBuckets(1, 2.0, 10),
		}, []string{"type"})
	LocalWriteIOBytesHistogram = localIOBytesHistogram.WithLabelValues("write")
	LocalReadIOBytesHistogram  = localIOBytesHistogram.WithLabelValues("read")
)

var (
	ioMergerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "io_merger_counter",
			Help:      "io merger counter",
		},
		[]string{"type"},
	)
	IOMergerCounterInitiate = ioMergerCounter.WithLabelValues("initiate")
	IOMergerCounterWait     = ioMergerCounter.WithLabelValues("wait")

	ioMergerDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "io_merger_duration_seconds",
			Help:      "io merger duration seconds",
			Buckets:   getDurationBuckets(),
		}, []string{"type"})
	IOMergerDurationInitiate = ioMergerDuration.WithLabelValues("initiate")
	IOMergerDurationWait     = ioMergerDuration.WithLabelValues("wait")
)

var (
	fsReadWriteDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "read_write_duration",
			Help:      "read write duration",
			Buckets:   getDurationBuckets(),
		},
		[]string{"type"},
	)
	FSReadDurationReadVectorCache   = fsReadWriteDuration.WithLabelValues("read-vector-cache")
	FSReadDurationUpdateVectorCache = fsReadWriteDuration.WithLabelValues("update-vector-cache")
	FSReadDurationReadMemoryCache   = fsReadWriteDuration.WithLabelValues("read-memory-cache")
	FSReadDurationUpdateMemoryCache = fsReadWriteDuration.WithLabelValues("update-memory-cache")
	FSReadDurationReadDiskCache     = fsReadWriteDuration.WithLabelValues("read-disk-cache")
	FSReadDurationUpdateDiskCache   = fsReadWriteDuration.WithLabelValues("update-disk-cache")
	FSReadDurationReadRemoteCache   = fsReadWriteDuration.WithLabelValues("read-remote-cache")
	FSReadDurationGetReader         = fsReadWriteDuration.WithLabelValues("get-reader")
	FSReadDurationGetContent        = fsReadWriteDuration.WithLabelValues("get-content")
	FSReadDurationGetEntryData      = fsReadWriteDuration.WithLabelValues("get-entry-data")
	FSReadDurationWriteToWriter     = fsReadWriteDuration.WithLabelValues("write-to-writer")
	FSReadDurationSetCachedData     = fsReadWriteDuration.WithLabelValues("set-cached-data")
	FSReadDurationDiskCacheSetFile  = fsReadWriteDuration.WithLabelValues("disk-cache-set-file")
	FSReadDurationList              = fsReadWriteDuration.WithLabelValues("list")
	FSReadDurationStat              = fsReadWriteDuration.WithLabelValues("stat")
	FSReadDurationIOReadAll         = fsReadWriteDuration.WithLabelValues("io-read-all")

	FSWriteDurationWrite = fsReadWriteDuration.WithLabelValues("write")

	// S3FS Read internal phase metrics
	FSReadDurationTotal    = fsReadWriteDuration.WithLabelValues("s3fs-read-total")
	FSReadDurationIOMerger = fsReadWriteDuration.WithLabelValues("s3fs-read-io-merger")
	FSReadDurationS3Read   = fsReadWriteDuration.WithLabelValues("s3fs-read-s3")

	// S3FS Write internal phase metrics
	FSWriteDurationExists       = fsReadWriteDuration.WithLabelValues("s3fs-write-exists")
	FSWriteDurationStorage      = fsReadWriteDuration.WithLabelValues("s3fs-write-storage")
	FSWriteDurationDiskCacheSet = fsReadWriteDuration.WithLabelValues("s3fs-write-disk-cache-setfile")
)

var (
	FSObjectStorageOperations = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "object_storage_operations",
			Help:      "object storage operations",
		},
		[]string{
			"name",
			"op",
		},
	)
)

var (
	fsCacheBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "cache_bytes",
			Help:      "Total bytes of fs cache.",
		}, []string{"component", "type"})
)

// GetFsCacheBytesGauge return inuse, cap Gauge metric
// {typ} should be [mem, disk, meta]
func GetFsCacheBytesGauge(name, typ string) (inuse prometheus.Gauge, capacity prometheus.Gauge) {
	var component string
	if name == "" {
		component = typ
	} else {
		component = name + "-" + typ
	}
	return fsCacheBytes.WithLabelValues(component, "inuse"),
		fsCacheBytes.WithLabelValues(component, "cap")
}

var (
	FSHTTPTraceCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "http_trace",
			Help:      "http trace statistics",
		},
		[]string{
			"op",
		},
	)
)

// S3 connection pool state metrics
// Tracks the current number of active (in-use) HTTP connections
var (
	// S3ConnActiveGauge tracks the current number of active S3 HTTP connections
	// Active connections are connections currently being used for HTTP requests
	S3ConnActiveGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "s3_conn_active",
			Help:      "Current number of active S3 HTTP connections (connections currently in use)",
		},
	)
)

// Disk cache metrics
var (
	// FSDiskCacheEvictCounter tracks the number of disk cache evictions
	// Evictions occur when cache capacity is exceeded and old entries are removed
	FSDiskCacheEvictCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "disk_cache_evict_total",
			Help:      "Total number of disk cache evictions",
		},
	)

	// FSDiskCacheErrorCounter tracks the number of disk cache errors
	// Errors include file I/O failures, permission issues, etc.
	FSDiskCacheErrorCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "disk_cache_error_total",
			Help:      "Total number of disk cache errors",
		},
	)
)
