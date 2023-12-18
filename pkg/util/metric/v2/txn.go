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
	TxnUserCounter     = txnCounter.WithLabelValues("user")
	TxnInternalCounter = txnCounter.WithLabelValues("internal")
	TxnLeakCounter     = txnCounter.WithLabelValues("leak")

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

	TxnRangesLoadedObjectMetaTotalCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "ranges_loaded_object_meta_total",
			Help:      "Total number of ranges loaded object meta.",
		})
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
			Buckets:   prometheus.ExponentialBuckets(0.00001, 2.0, 20),
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
			Buckets:   prometheus.ExponentialBuckets(0.00001, 2.0, 20),
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
			Buckets:   prometheus.ExponentialBuckets(0.00001, 2.0, 20),
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
			Buckets:   prometheus.ExponentialBuckets(0.00001, 2.0, 20),
		}, []string{"type"})
	TxnStatementBuildPlanDurationHistogram      = txnStatementDurationHistogram.WithLabelValues("build-plan")
	TxnStatementExecuteDurationHistogram        = txnStatementDurationHistogram.WithLabelValues("execute")
	TxnStatementExecuteLatencyDurationHistogram = txnStatementDurationHistogram.WithLabelValues("execute-latency")
	TxnStatementCompileDurationHistogram        = txnStatementDurationHistogram.WithLabelValues("compile")

	txnLockDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "lock_duration_seconds",
			Help:      "Bucketed histogram of acquire lock duration.",
			Buckets:   prometheus.ExponentialBuckets(0.00001, 2.0, 20),
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
			Buckets:   prometheus.ExponentialBuckets(0.00001, 2.0, 20),
		}, []string{"type"})
	TxnUnlockDurationHistogram             = txnUnlockDurationHistogram.WithLabelValues("total")
	TxnUnlockBtreeGetLockDurationHistogram = txnUnlockDurationHistogram.WithLabelValues("btree-get-lock")
	TxnUnlockBtreeTotalDurationHistogram   = txnUnlockDurationHistogram.WithLabelValues("btree-total")

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
			Buckets:   prometheus.ExponentialBuckets(0.00001, 2.0, 20),
		})

	TxnTableRangeSizeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "ranges_duration_size",
			Help:      "Bucketed histogram of txn table ranges size.",
			Buckets:   prometheus.ExponentialBuckets(1, 2.0, 20),
		})

	txnTNSideDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "tn_side_duration_seconds",
			Help:      "Bucketed histogram of txn duration on tn side.",
			Buckets:   prometheus.ExponentialBuckets(0.00001, 2.0, 20),
		}, []string{"step"})

	TxnOnPrepareWALPrepareWALDurationHistogram = txnTNSideDurationHistogram.WithLabelValues("on_prepare_wal_prepare_wal")
	TxnOnPrepareWALEndPrepareDurationHistogram = txnTNSideDurationHistogram.WithLabelValues("on_prepare_wal_end_prepare")
	TxnOnPrepareWALFlushQueueDurationHistogram = txnTNSideDurationHistogram.WithLabelValues("on_prepare_wal_flush_queue")
	TxnOnPrepareWALTotalDurationHistogram      = txnTNSideDurationHistogram.WithLabelValues("on_prepare_wal_total")

	TxnDequeuePreparingDurationHistogram = txnTNSideDurationHistogram.WithLabelValues("dequeue_preparing")
	TxnDequeuePreparedDurationHistogram  = txnTNSideDurationHistogram.WithLabelValues("dequeue_prepared")
	TxnBeforeCommitDurationHistogram     = txnTNSideDurationHistogram.WithLabelValues("before_txn_commit")

	TxnShowAccountsDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "show_accounts_duration_seconds",
			Help:      "Bucketed histogram of show accounts duration.",
			Buckets:   prometheus.ExponentialBuckets(0.00001, 2.0, 20),
		})

	txnMpoolDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "mpool_duration_seconds",
			Help:      "Bucketed histogram of txn mpool duration.",
			Buckets:   prometheus.ExponentialBuckets(0.00001, 2.0, 20),
		}, []string{"type"})
	TxnMpoolNewDurationHistogram    = txnMpoolDurationHistogram.WithLabelValues("new")
	TxnMpoolAllocDurationHistogram  = txnMpoolDurationHistogram.WithLabelValues("alloc")
	TxnMpoolFreeDurationHistogram   = txnMpoolDurationHistogram.WithLabelValues("free")
	TxnMpoolDeleteDurationHistogram = txnMpoolDurationHistogram.WithLabelValues("delete")

	txnReaderDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "reader_duration_seconds",
			Help:      "Bucketed histogram of reader read duration.",
			Buckets:   prometheus.ExponentialBuckets(0.00001, 2.0, 20),
		}, []string{"type"})
	TxnBlockReaderDurationHistogram      = txnReaderDurationHistogram.WithLabelValues("block-reader")
	TxnMergeReaderDurationHistogram      = txnReaderDurationHistogram.WithLabelValues("merge-reader")
	TxnBlockMergeReaderDurationHistogram = txnReaderDurationHistogram.WithLabelValues("block-merge-reader")
)
