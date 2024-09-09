// Copyright 2021-2024 Matrix Origin
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
	replicaOperatorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "sharding",
			Name:      "schedule_replica_total",
			Help:      "Total number of replica schedule.",
		}, []string{"type"})
	AddReplicaOperatorCounter               = replicaOperatorCounter.WithLabelValues("apply-add")
	DeleteReplicaOperatorCounter            = replicaOperatorCounter.WithLabelValues("apply-delete")
	DeleteAllReplicaOperatorCounter         = replicaOperatorCounter.WithLabelValues("apply-delete-all")
	ReplicaScheduleSkipWithNoCNCounter      = replicaOperatorCounter.WithLabelValues("skip-no-cn")
	ReplicaScheduleSkipFreezeCNCounter      = replicaOperatorCounter.WithLabelValues("skip-freeze-cn")
	ReplicaScheduleAllocateReplicaCounter   = replicaOperatorCounter.WithLabelValues("allocate")
	ReplicaScheduleReAllocateReplicaCounter = replicaOperatorCounter.WithLabelValues("re-allocate")
	ReplicaScheduleBalanceReplicaCounter    = replicaOperatorCounter.WithLabelValues("balance")

	replicaReadCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "sharding",
			Name:      "replica_read_total",
			Help:      "Total number of replica read.",
		}, []string{"type"})
	ReplicaLocalReadCounter  = replicaReadCounter.WithLabelValues("local")
	ReplicaRemoteReadCounter = replicaReadCounter.WithLabelValues("remote")
)

var (
	ReplicaCountGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "sharding",
			Name:      "replica_count",
			Help:      "Count of running replica.",
		})

	ReplicaFreezeCNCountGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "sharding",
			Name:      "schedule_freeze_cn_count",
			Help:      "Count of freeze cn.",
		})
)
