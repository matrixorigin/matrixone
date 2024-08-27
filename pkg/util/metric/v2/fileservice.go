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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	Backup                         = "backup"
	Bootstrap                      = "bootstrap"
	ExternalReadFile               = "external-read-file"
	ExternalReadFileOffsetNoStrict = "external-offset-no-strict"
	ExternalReadFileOffsetStrict   = "external-offset-strict"
	ExternalStats                  = "external-stats"
	ReadFromFileOffsetSize         = "read-from-file-offset-size"
	ReadExtent                     = "read-extent"
	ReadOneBlockWithMeta           = "read-one-block-with-meta"
	ReadMultiBlocksWithMeta        = "read-multi-blocks-with-meta"
	ReadAllBlocksWithMeta          = "read-all-blocks-with-meta"
	ReadOneBlockAllColumns         = "read-one-block-all-columns"
	ReadMultiSubBlocks             = "read-multi-sub-blocks"
	ReadParquet                    = "read-parquet"
	UDFGet                         = "udf-get"
	NewCSVReadr                    = "new-csv-reader"
	UtilReadFile                   = "util-read-file"
	LoadTable                      = "load-table"
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

	fsWriteCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "write_total",
			Help:      "Total number of write count.",
		}, []string{"type"})
	FSWriteTransferPageCounter  = fsWriteCounter.WithLabelValues("transfer-page")
	FSWriteTraceCounter         = fsWriteCounter.WithLabelValues("trace")
	FSWriteSaveBatchCounter     = fsWriteCounter.WithLabelValues("save-batch")
	FSWriteSaveMeta1Counter     = fsWriteCounter.WithLabelValues("save-meta1")
	FSWriteTransferMapsCounter  = fsWriteCounter.WithLabelValues("transfer-maps")
	FSWriteSyncCounter          = fsWriteCounter.WithLabelValues("sync")
	FSWriteMergeCkpMetaCounter  = fsWriteCounter.WithLabelValues("merge-ckp-meta")
	FSWriteSaveCkpCounter       = fsWriteCounter.WithLabelValues("save-ckp")
	FSWriteSaveTableCounter     = fsWriteCounter.WithLabelValues("save-table")
	FSWriteSaveFullTableCounter = fsWriteCounter.WithLabelValues("save-full-table")
	FSWriteSaveMeta2Counter     = fsWriteCounter.WithLabelValues("save-meta2")
	FSWriteSaveTableInfoCounter = fsWriteCounter.WithLabelValues("save-table-info")

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

	// for debug
	FSReadBackupCounter                         = fsReadCounter.WithLabelValues(Backup)
	FSReadBoostrapCounter                       = fsReadCounter.WithLabelValues(Bootstrap)
	FSReadExternalReadFileCounter               = fsReadCounter.WithLabelValues(ExternalReadFile)
	FSReadExternalReadFileOffsetNoStrictCounter = fsReadCounter.WithLabelValues(ExternalReadFileOffsetNoStrict)
	FSReadExternalReadFileOffsetStrictCounter   = fsReadCounter.WithLabelValues(ExternalReadFileOffsetStrict)
	FSReadExternalStatsCounter                  = fsReadCounter.WithLabelValues(ExternalStats)
	FSReadReadFromFileOffsetSizeCounter         = fsReadCounter.WithLabelValues(ReadFromFileOffsetSize)
	FSReadReadExtentCounter                     = fsReadCounter.WithLabelValues(ReadExtent)
	FSReadReadOneBlockWithMetaCounter           = fsReadCounter.WithLabelValues(ReadOneBlockWithMeta)
	FSReadReadMultiBlocksWithMetaCounter        = fsReadCounter.WithLabelValues(ReadMultiBlocksWithMeta)
	FSReadReadAllBlocksWithMetaCounter          = fsReadCounter.WithLabelValues(ReadAllBlocksWithMeta)
	FSReadReadOneBlockAllColumnsCounter         = fsReadCounter.WithLabelValues(ReadOneBlockAllColumns)
	FSReadReadMultiSubBlocksCounter             = fsReadCounter.WithLabelValues(ReadMultiSubBlocks)
	FSReadReadParquetCounter                    = fsReadCounter.WithLabelValues(ReadParquet)
	FSReadUDFGetCounter                         = fsReadCounter.WithLabelValues(UDFGet)
	FSReadNewCSVReadrCounter                    = fsReadCounter.WithLabelValues(NewCSVReadr)
	FSReadUtilReadFileCounter                   = fsReadCounter.WithLabelValues(UtilReadFile)
	FSReadLoadTableCounter                      = fsReadCounter.WithLabelValues(LoadTable)
)

func ReadCounterByModule(num float64, module string) {
	switch module {
	case Backup:
		FSReadBackupCounter.Add(num)
	case Bootstrap:
		FSReadBoostrapCounter.Add(num)
	case ExternalReadFile:
		FSReadExternalReadFileCounter.Add(num)
	case ExternalReadFileOffsetNoStrict:
		FSReadExternalReadFileOffsetNoStrictCounter.Add(num)
	case ExternalReadFileOffsetStrict:
		FSReadExternalReadFileOffsetStrictCounter.Add(num)
	case ExternalStats:
		FSReadExternalStatsCounter.Add(num)
	case ReadFromFileOffsetSize:
		FSReadReadFromFileOffsetSizeCounter.Add(num)
	case ReadExtent:
		FSReadReadExtentCounter.Add(num)
	case ReadOneBlockWithMeta:
		FSReadReadOneBlockWithMetaCounter.Add(num)
	case ReadMultiBlocksWithMeta:
		FSReadReadMultiBlocksWithMetaCounter.Add(num)
	case ReadAllBlocksWithMeta:
		FSReadReadAllBlocksWithMetaCounter.Add(num)
	case ReadOneBlockAllColumns:
		FSReadReadOneBlockAllColumnsCounter.Add(num)
	case ReadMultiSubBlocks:
		FSReadReadMultiSubBlocksCounter.Add(num)
	case ReadParquet:
		FSReadReadParquetCounter.Add(num)
	case UDFGet:
		FSReadUDFGetCounter.Add(num)
	case NewCSVReadr:
		FSReadNewCSVReadrCounter.Add(num)
	case UtilReadFile:
		FSReadUtilReadFileCounter.Add(num)
	case LoadTable:
		FSReadLoadTableCounter.Add(num)
	default:
		logutil.Infof("unknown module: %s", module)
	}
}

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
