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
	S3IOBytesHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "cn",
			Subsystem: "fs",
			Name:      "s3_io_bytes",
			Help:      "Bucketed histogram of s3 io bytes.",
			Buckets:   prometheus.ExponentialBuckets(1, 2.0, 10),
		}, []string{"type"})

	S3IODurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "cn",
			Subsystem: "fs",
			Name:      "s3_io_duration_seconds",
			Help:      "Bucketed histogram of s3 io duration.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2.0, 20),
		}, []string{"type"})

	LocalIODurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "cn",
			Subsystem: "fs",
			Name:      "local_io_duration_seconds",
			Help:      "Bucketed histogram of local io duration.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2.0, 20),
		}, []string{"type"})

	S3GetConnDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "cn",
			Subsystem: "fs",
			Name:      "s3_conn_duration_seconds",
			Help:      "Bucketed histogram of s3 get conn duration.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2.0, 20),
		})

	S3DNSDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "cn",
			Subsystem: "fs",
			Name:      "s3_dns_duration_seconds",
			Help:      "Bucketed histogram of s3 resolve dns duration.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2.0, 20),
		})

	S3ConnectDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "cn",
			Subsystem: "fs",
			Name:      "s3_connect_duration_seconds",
			Help:      "Bucketed histogram of s3 connect duration.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2.0, 20),
		})

	S3TLSHandshakeDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "cn",
			Subsystem: "fs",
			Name:      "s3_tls_handshake_duration_seconds",
			Help:      "Bucketed histogram of s3 tls handshake duration.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2.0, 20),
		})

	LocalIOBytesHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "cn",
			Subsystem: "fs",
			Name:      "local_io_bytes",
			Help:      "Bucketed histogram of local io bytes.",
			Buckets:   prometheus.ExponentialBuckets(1, 2.0, 10),
		}, []string{"type"})

	MemIOBytesHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "cn",
			Subsystem: "fs",
			Name:      "mem_io_bytes",
			Help:      "Bucketed histogram of memory io bytes.",
			Buckets:   prometheus.ExponentialBuckets(1, 2.0, 10),
		}, []string{"type"})
)

func GetS3ReadDurationHistogram() prometheus.Observer {
	return S3IODurationHistogram.WithLabelValues("read")
}

func GetS3WriteDurationHistogram() prometheus.Observer {
	return S3IODurationHistogram.WithLabelValues("write")
}

func GetLocalReadDurationHistogram() prometheus.Observer {
	return LocalIODurationHistogram.WithLabelValues("read")
}

func GetLocalWriteDurationHistogram() prometheus.Observer {
	return LocalIODurationHistogram.WithLabelValues("write")
}

func GetS3FSWriteBytesHistogram() prometheus.Observer {
	return S3IOBytesHistogram.WithLabelValues("write")
}

func GetS3FSReadBytesHistogram() prometheus.Observer {
	return S3IOBytesHistogram.WithLabelValues("read")
}

func GetLocalFSWriteBytesHistogram() prometheus.Observer {
	return LocalIOBytesHistogram.WithLabelValues("write")
}

func GetLocalFSReadBytesHistogram() prometheus.Observer {
	return LocalIOBytesHistogram.WithLabelValues("read")
}

func GetMemFSWriteBytesHistogram() prometheus.Observer {
	return MemIOBytesHistogram.WithLabelValues("write")
}

func GetMemFSReadBytesHistogram() prometheus.Observer {
	return MemIOBytesHistogram.WithLabelValues("read")
}
