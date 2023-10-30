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

	TxnFastLoadObjectMetaTotalCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "tn_side_fast_load_object_meta_total",
			Help:      "Total number of fast loaded object meta on tn side.",
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
)

var (
	txnCommitDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "commit_duration_seconds",
			Help:      "Bucketed histogram of txn commit duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
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
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	txnCreateDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "create_duration_seconds",
			Help:      "Bucketed histogram of txn create txn duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
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
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		}, []string{"type"})
	TxnStatementBuildPlanDurationHistogram = txnStatementDurationHistogram.WithLabelValues("build-plan")
	TxnStatementExecuteDurationHistogram   = txnStatementDurationHistogram.WithLabelValues("execute")

	txnLockDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "lock_duration_seconds",
			Help:      "Bucketed histogram of acquire lock duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		}, []string{"type"})
	TxnAcquireLockDurationHistogram = txnLockDurationHistogram.WithLabelValues("acquire")
	TxnHoldLockDurationHistogram    = txnLockDurationHistogram.WithLabelValues("hold")

	TxnUnlockDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "unlock_duration_seconds",
			Help:      "Bucketed histogram of release lock duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	TxnTableRangeDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "ranges_duration_seconds",
			Help:      "Bucketed histogram of txn table ranges duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	txnTNSideDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "txn",
			Name:      "tn_side_duration_seconds",
			Help:      "Bucketed histogram of txn duration on tn side.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		}, []string{"step"})

	TxnOnPrepareWALDurationHistogram     = txnTNSideDurationHistogram.WithLabelValues("on_prepare_wal")
	TxnDequeuePreparingDurationHistogram = txnTNSideDurationHistogram.WithLabelValues("dequeue_preparing")
	TxnDequeuePreparedDurationHistogram  = txnTNSideDurationHistogram.WithLabelValues("dequeue_prepared")
	TxnBeforeCommitDurationHistogram     = txnTNSideDurationHistogram.WithLabelValues("before_txn_commit")
)
