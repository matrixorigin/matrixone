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
	TxnTotalCostDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "cn",
			Subsystem: "txn",
			Name:      "total_cost_duration_seconds",
			Help:      "Bucketed histogram of txn total cost duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	TxnDetermineSnapshotDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "cn",
			Subsystem: "txn",
			Name:      "determine_snapshot_duration_seconds",
			Help:      "Bucketed histogram of determine snapshot duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	TxnWaitActiveDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "cn",
			Subsystem: "txn",
			Name:      "wait_active_duration_seconds",
			Help:      "Bucketed histogram of wait active duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	TxnLockDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "cn",
			Subsystem: "txn",
			Name:      "lock_duration_seconds",
			Help:      "Bucketed histogram of acquire lock duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	TxnUnlockDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "cn",
			Subsystem: "txn",
			Name:      "unlock_duration_seconds",
			Help:      "Bucketed histogram of release lock duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	TxnCommitDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "cn",
			Subsystem: "txn",
			Name:      "commit_duration_seconds",
			Help:      "Bucketed histogram of txn commit duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	TxnTableRangeDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "cn",
			Subsystem: "txn",
			Name:      "ranges_duration_seconds",
			Help:      "Bucketed histogram of txn table ranges duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	TxnSendRequestDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "cn",
			Subsystem: "txn",
			Name:      "send_request_duration_seconds",
			Help:      "Bucketed histogram of handle send txn request duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	TxnHandleQueueInDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tn",
			Subsystem: "txn",
			Name:      "handle_queue_in_duration_seconds",
			Help:      "Bucketed histogram of add request into handle queue duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	TxnHandleCommitDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tn",
			Subsystem: "txn",
			Name:      "handle_commit_duration_seconds",
			Help:      "Bucketed histogram of handle txn commit duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})
)
