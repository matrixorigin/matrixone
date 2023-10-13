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
	TxnCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "cn",
			Subsystem: "txn",
			Name:      "txn_total",
			Help:      "Total number of txn handled.",
		}, []string{"type"})

	TxnStatementCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "cn",
			Subsystem: "txn",
			Name:      "statement_total",
			Help:      "Total number of txn statement handled.",
		})

	TxnStatementRetryCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "cn",
			Subsystem: "txn",
			Name:      "statement_retry_total",
			Help:      "Total number of txn retry statement handled.",
		})

	TxnHandleCommitCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tn",
			Subsystem: "txn",
			Name:      "handle_commit_total",
			Help:      "Total number of txn commit requests handled.",
		})
)

func GetInternalTxnCounter() prometheus.Counter {
	return TxnCounter.WithLabelValues("internal")
}

func GetUserTxnCounter() prometheus.Counter {
	return TxnCounter.WithLabelValues("user")
}
