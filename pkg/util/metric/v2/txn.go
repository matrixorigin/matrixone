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
	txnCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "total",
			Help:      "Total number of txn created.",
		}, []string{"type"})
	TxnUserCounter        = txnCounter.WithLabelValues("user")
	TxnInternalCounter    = txnCounter.WithLabelValues("internal")
	TxnLeakCounter        = txnCounter.WithLabelValues("leak")
	TxnLongRunningCounter = txnCounter.WithLabelValues("long running")
	TxnInCommitCounter    = txnCounter.WithLabelValues("stuck in commit")
	TxnInRollbackCounter  = txnCounter.WithLabelValues("stuck in rollback")

	txnStatementCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "statement_total",
			Help:      "Total number of txn statement executed.",
		}, []string{"type"})
	TxnStatementTotalCounter = txnStatementCounter.WithLabelValues("total")
	TxnStatementRetryCounter = txnStatementCounter.WithLabelValues("retry")

	txnCommitCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "commit_total",
			Help:      "Total number of txn commit handled.",
		}, []string{"type"})
	TxnCNCommitCounter        = txnCommitCounter.WithLabelValues("cn")
	TxnTNReceiveCommitCounter = txnCommitCounter.WithLabelValues("tn-receive")
	TxnTNCommitHandledCounter = txnCommitCounter.WithLabelValues("tn-handle")

	TxnRollbackCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "rollback_total",
			Help:      "Total number of txn rollback handled.",
		})

	txnLockCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "lock_total",
			Help:      "Total number of lock op counter.",
		}, []string{"type"})
	TxnLockTotalCounter       = txnLockCounter.WithLabelValues("total")
	TxnLocalLockTotalCounter  = txnLockCounter.WithLabelValues("local")
	TxnRemoteLockTotalCounter = txnLockCounter.WithLabelValues("remote")
)

var (
	txnQueueSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "queue_size",
			Help:      "Size of txn queues.",
		}, []string{"type"})
	TxnCommitQueueSizeGauge     = txnQueueSizeGauge.WithLabelValues("commit")
	TxnWaitActiveQueueSizeGauge = txnQueueSizeGauge.WithLabelValues("wait-active")
	TxnActiveQueueSizeGauge     = txnQueueSizeGauge.WithLabelValues("active")
	TxnLockRPCQueueSizeGauge    = txnQueueSizeGauge.WithLabelValues("lock-rpc")

	txnCNCommittedLocationQuantityGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "cn_committed_location_quantity_size",
			Help:      "Quantity of object location the cn have committed to tn.",
		}, []string{"type"})

	TxnCNCommittedMetaLocationQuantityGauge  = txnCNCommittedLocationQuantityGauge.WithLabelValues("meta_location")
	TxnCNCommittedDeltaLocationQuantityGauge = txnCNCommittedLocationQuantityGauge.WithLabelValues("delta_location")
)

var (
	txnCommitDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "commit_duration_seconds",
			Help:      "Bucketed histogram of txn commit duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"type"})
	TxnCNCommitDurationHistogram            = txnCommitDurationHistogram.WithLabelValues("cn")
	TxnCNSendCommitDurationHistogram        = txnCommitDurationHistogram.WithLabelValues("cn-send")
	TxnCNCommitResponseDurationHistogram    = txnCommitDurationHistogram.WithLabelValues("cn-resp")
	TxnCNCommitWaitLogtailDurationHistogram = txnCommitDurationHistogram.WithLabelValues("cn-wait-logtail")
	TxnTNCommitDurationHistogram            = txnCommitDurationHistogram.WithLabelValues("tn")

	TxnLifeCycleDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "life_duration_seconds",
			Help:      "Bucketed histogram of txn life cycle duration.",
			Buckets:   getDurationBuckets(),
		})

	TxnLifeCycleStatementsTotalHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "life_statements_total",
			Help:      "Bucketed histogram of statement total in a txn.",
			Buckets:   prometheus.ExponentialBuckets(1, 2.0, 10),
		})

	TxnUnlockTableTotalHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "unlock_table_total",
			Help:      "Size of txn unlock tables count.",
			Buckets:   prometheus.ExponentialBuckets(1, 2.0, 4),
		})

	txnCreateDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "create_duration_seconds",
			Help:      "Bucketed histogram of txn create txn duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"type"})
	TxnCreateTotalDurationHistogram       = txnCreateDurationHistogram.WithLabelValues("total")
	TxnDetermineSnapshotDurationHistogram = txnCreateDurationHistogram.WithLabelValues("determine-snapshot")
	TxnWaitActiveDurationHistogram        = txnCreateDurationHistogram.WithLabelValues("wait-active")

	txnStatementDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "statement_duration_seconds",
			Help:      "Bucketed histogram of txn statement duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"type"})
	TxnStatementBuildPlanDurationHistogram      = txnStatementDurationHistogram.WithLabelValues("build-plan")
	TxnStatementExecuteDurationHistogram        = txnStatementDurationHistogram.WithLabelValues("execute")
	TxnStatementExecuteLatencyDurationHistogram = txnStatementDurationHistogram.WithLabelValues("execute-latency")
	TxnStatementCompileDurationHistogram        = txnStatementDurationHistogram.WithLabelValues("compile")
	TxnStatementScanDurationHistogram           = txnStatementDurationHistogram.WithLabelValues("scan")
	TxnStatementExternalScanDurationHistogram   = txnStatementDurationHistogram.WithLabelValues("external-scan")
	TxnStatementInsertS3DurationHistogram       = txnStatementDurationHistogram.WithLabelValues("insert-s3")
	TxnStatementStatsDurationHistogram          = txnStatementDurationHistogram.WithLabelValues("stats")
	TxnStatementResolveDurationHistogram        = txnStatementDurationHistogram.WithLabelValues("resolve")
	TxnStatementResolveUdfDurationHistogram     = txnStatementDurationHistogram.WithLabelValues("resolve-udf")
	TxnStatementUpdateStatsDurationHistogram    = txnStatementDurationHistogram.WithLabelValues("update-stats")
	TxnStatementUpdateInfoFromZonemapHistogram  = txnStatementDurationHistogram.WithLabelValues("update-info-from-zonemap")
	TxnStatementUpdateStatsInfoMapHistogram     = txnStatementDurationHistogram.WithLabelValues("update-stats-info-map")
	TxnStatementNodesHistogram                  = txnStatementDurationHistogram.WithLabelValues("nodes")
	TxnStatementCompileScopeHistogram           = txnStatementDurationHistogram.WithLabelValues("compileScope")
	TxnStatementCompileQueryHistogram           = txnStatementDurationHistogram.WithLabelValues("compileQuery")
	TxnStatementCompilePlanScopeHistogram       = txnStatementDurationHistogram.WithLabelValues("compilePlanScope")
	TxnStatementBuildPlanHistogram              = txnStatementDurationHistogram.WithLabelValues("BuildPlan")
	TxnStatementBuildSelectHistogram            = txnStatementDurationHistogram.WithLabelValues("BuildSelect")
	TxnStatementBuildInsertHistogram            = txnStatementDurationHistogram.WithLabelValues("BuildInsert")
	TxnStatementBuildExplainHistogram           = txnStatementDurationHistogram.WithLabelValues("BuildExplain")
	TxnStatementBuildReplaceHistogram           = txnStatementDurationHistogram.WithLabelValues("BuildReplace")
	TxnStatementBuildUpdateHistogram            = txnStatementDurationHistogram.WithLabelValues("BuildUpdate")
	TxnStatementBuildDeleteHistogram            = txnStatementDurationHistogram.WithLabelValues("BuildDelete")
	TxnStatementBuildLoadHistogram              = txnStatementDurationHistogram.WithLabelValues("BuildLoad")

	txnLockDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "lock_duration_seconds",
			Help:      "Bucketed histogram of acquire lock duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"type"})
	TxnAcquireLockDurationHistogram     = txnLockDurationHistogram.WithLabelValues("acquire")
	TxnAcquireLockWaitDurationHistogram = txnLockDurationHistogram.WithLabelValues("acquire-wait")
	TxnHoldLockDurationHistogram        = txnLockDurationHistogram.WithLabelValues("hold")

	txnUnlockDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "unlock_duration_seconds",
			Help:      "Bucketed histogram of release lock duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"type"})
	TxnUnlockDurationHistogram             = txnUnlockDurationHistogram.WithLabelValues("total")
	TxnUnlockBtreeGetLockDurationHistogram = txnUnlockDurationHistogram.WithLabelValues("btree-get-lock")
	TxnUnlockBtreeTotalDurationHistogram   = txnUnlockDurationHistogram.WithLabelValues("btree-total")
	TxnLockWorkerHandleDurationHistogram   = txnUnlockDurationHistogram.WithLabelValues("worker-handle")

	TxnLockWaitersTotalHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "lock_waiters_total",
			Help:      "Bucketed histogram of waiters count in one lock.",
			Buckets:   prometheus.ExponentialBuckets(1, 2.0, 10),
		})

	TxnTableRangeDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "ranges_duration_seconds",
			Help:      "Bucketed histogram of txn table ranges duration.",
			Buckets:   getDurationBuckets(),
		})

	TxnCheckPKDupDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "check_pk_dup_duration_seconds",
			Help:      "Bucketed histogram of txn check pk dup duration.",
			Buckets:   getDurationBuckets(),
		})

	txnTableRangeTotalHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "ranges_selected_block_cnt_total",
			Help:      "Bucketed histogram of txn table ranges selected block cnt.",
			Buckets:   prometheus.ExponentialBuckets(1, 2.0, 15),
		}, []string{"type"})

	TxnRangesSlowPathSelectedBlockCntHistogram = txnTableRangeTotalHistogram.WithLabelValues("slow_path_selected_block_cnt")
	TxnRangesFastPathSelectedBlockCntHistogram = txnTableRangeTotalHistogram.WithLabelValues("fast_path_selected_block_cnt")
	TxnRangesFastPathLoadObjCntHistogram       = txnTableRangeTotalHistogram.WithLabelValues("fast_path_load_obj_cnt")
	TxnRangesSlowPathLoadObjCntHistogram       = txnTableRangeTotalHistogram.WithLabelValues("slow_path_load_obj_cnt")

	txnTNSideDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "tn_side_duration_seconds",
			Help:      "Bucketed histogram of txn duration on tn side.",
			Buckets:   getDurationBuckets(),
		}, []string{"step"})

	TxnPreparingWaitDurationHistogram  = txnTNSideDurationHistogram.WithLabelValues("1-PreparingWait")
	TxnPreparingDurationHistogram      = txnTNSideDurationHistogram.WithLabelValues("2-Preparing")
	TxnPrepareWalWaitDurationHistogram = txnTNSideDurationHistogram.WithLabelValues("3-PrepareWalWait")
	TxnPrepareWalDurationHistogram     = txnTNSideDurationHistogram.WithLabelValues("4-PrepareWal")
	TxnPreparedWaitDurationHistogram   = txnTNSideDurationHistogram.WithLabelValues("5-PreparedWait")
	TxnPreparedDurationHistogram       = txnTNSideDurationHistogram.WithLabelValues("6-Prepared")

	TxnBeforeCommitDurationHistogram = txnTNSideDurationHistogram.WithLabelValues("before_txn_commit")

	txnTNDeduplicateDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "tn_deduplicate_duration_seconds",
			Help:      "Bucketed histogram of txn duration on tn side",
			Buckets:   getDurationBuckets(),
		}, []string{"type"})

	TxnTNAppendDeduplicateDurationHistogram     = txnTNDeduplicateDurationHistogram.WithLabelValues("append_deduplicate")
	TxnTNPrePrepareDeduplicateDurationHistogram = txnTNDeduplicateDurationHistogram.WithLabelValues("prePrepare_deduplicate")

	txnMpoolDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "mpool_duration_seconds",
			Help:      "Bucketed histogram of txn mpool duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"type"})
	TxnMpoolNewDurationHistogram    = txnMpoolDurationHistogram.WithLabelValues("new")
	TxnMpoolDeleteDurationHistogram = txnMpoolDurationHistogram.WithLabelValues("delete")

	txnReaderDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "reader_duration_seconds",
			Help:      "Bucketed histogram of reader read duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"type"})
	TxnBlockReaderDurationHistogram      = txnReaderDurationHistogram.WithLabelValues("block-reader")
	TxnMergeReaderDurationHistogram      = txnReaderDurationHistogram.WithLabelValues("merge-reader")
	TxnBlockMergeReaderDurationHistogram = txnReaderDurationHistogram.WithLabelValues("block-merge-reader")

	txnRangesSelectivityHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "ranges_selectivity_percentage",
			Help:      "Bucketed histogram of fast ranges selectivity percentage.",
			Buckets:   prometheus.ExponentialBucketsRange(0.001, 1, 21),
		}, []string{"type"})
	TxnRangesSlowPathBlockSelectivityHistogram          = txnRangesSelectivityHistogram.WithLabelValues("slow_path_block_selectivity")
	TxnRangesFastPathBlkTotalSelectivityHistogram       = txnRangesSelectivityHistogram.WithLabelValues("fast_path_block_selectivity")
	TxnRangesFastPathObjSortKeyZMapSelectivityHistogram = txnRangesSelectivityHistogram.WithLabelValues("fast_path_obj_sort_key_zm_selectivity")
	TxnRangesFastPathObjColumnZMapSelectivityHistogram  = txnRangesSelectivityHistogram.WithLabelValues("fast_path_obj_column_zm_selectivity")
	TxnRangesFastPathBlkColumnZMapSelectivityHistogram  = txnRangesSelectivityHistogram.WithLabelValues("fast_path_blk_column_zm_selectivity")
)

var (
	TxnReaderScannedTotalTombstoneHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "reader_scanned_total_tombstone",
			Help:      "Bucketed histogram of read scanned total tombstone.",
			Buckets:   prometheus.ExponentialBuckets(1, 2.0, 11),
		})

	TxnReaderEachBLKLoadedTombstoneHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "reader_each_blk_loaded",
			Help:      "Bucketed histogram of read each blk loaded.",
			Buckets:   prometheus.ExponentialBuckets(1, 2.0, 11),
		})

	txnReaderTombstoneSelectivityHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "reader_tombstone_selectivity",
			Help:      "Bucketed histogram of read tombstone.",
			Buckets:   prometheus.ExponentialBucketsRange(0.001, 1, 21),
		}, []string{"type"})

	TxnReaderTombstoneZMSelectivityHistogram = txnReaderTombstoneSelectivityHistogram.WithLabelValues("zm_selectivity")
	TxnReaderTombstoneBLSelectivityHistogram = txnReaderTombstoneSelectivityHistogram.WithLabelValues("bl_selectivity")
	TransferTombstonesCountHistogram         = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "mo",
		Subsystem: "txn",
		Name:      "transfer_tombstones_count",
		Help:      "The total number of transfer tombstones.",
	})

	txnTransferDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "transfer_duration",
			Help:      "Bucketed histogram of tombstones transfer durations.",
			Buckets:   getDurationBuckets(),
		}, []string{"type"})

	TransferTombstonesDurationHistogram      = txnTransferDurationHistogram.WithLabelValues("tombstones")
	BatchTransferTombstonesDurationHistogram = txnTransferDurationHistogram.WithLabelValues("batch")
)
