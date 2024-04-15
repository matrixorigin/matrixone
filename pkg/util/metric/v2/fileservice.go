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
	S3ConnectCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "s3_connect_total",
			Help:      "Total number of s3 connect count.",
		})

	S3DNSResolveCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "s3_dns_resolve_total",
			Help:      "Total number of s3 dns resolve count.",
		})

	fsReadCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "read_total",
			Help:      "Total number of read count.",
		}, []string{"type"})
	FSReadS3Counter        = fsReadCounter.WithLabelValues("s3")
	FSReadHitMemCounter    = fsReadCounter.WithLabelValues("hit-mem")
	FSReadHitDiskCounter   = fsReadCounter.WithLabelValues("hit-disk")
	FSReadHitRemoteCounter = fsReadCounter.WithLabelValues("hit-remote")

	fsWriteCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "write_total",
			Help:      "Total number of write count.",
		}, []string{"type"})
	FSWriteS3Counter    = fsWriteCounter.WithLabelValues("s3")
	FSWriteLocalCounter = fsWriteCounter.WithLabelValues("local")
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

	s3IODurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "s3_io_duration_seconds",
			Help:      "Bucketed histogram of s3 io duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"type"})
	S3WriteIODurationHistogram = s3IODurationHistogram.WithLabelValues("write")
	S3ReadIODurationHistogram  = s3IODurationHistogram.WithLabelValues("read")

	s3ConnDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "s3_conn_duration_seconds",
			Help:      "Bucketed histogram of s3 get conn duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"type"})
	S3GetConnDurationHistogram      = s3ConnDurationHistogram.WithLabelValues("get-conn")
	S3DNSResolveDurationHistogram   = s3ConnDurationHistogram.WithLabelValues("dns-resolve")
	S3ConnectDurationHistogram      = s3ConnDurationHistogram.WithLabelValues("connect")
	S3TLSHandshakeDurationHistogram = s3ConnDurationHistogram.WithLabelValues("tls-handshake")

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

	localIODurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "local_io_duration_seconds",
			Help:      "Bucketed histogram of local io duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"type"})
	LocalWriteIODurationHistogram = localIODurationHistogram.WithLabelValues("write")
	LocalReadIODurationHistogram  = localIODurationHistogram.WithLabelValues("read")
)

var (
	ioLockCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "io_lock_counter",
			Help:      "io lock counter",
		},
		[]string{"type"},
	)
	IOLockCounterLocked = ioLockCounter.WithLabelValues("locked")
	IOLockCounterWait   = ioLockCounter.WithLabelValues("wait")

	ioLockDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "io_lock_duration_seconds",
			Help:      "io lock duration seconds",
			Buckets:   getDurationBuckets(),
		}, []string{"type"})
	IOLockDurationLocked = ioLockDuration.WithLabelValues("locked")
	IOLockDurationWait   = ioLockDuration.WithLabelValues("wait")
)
