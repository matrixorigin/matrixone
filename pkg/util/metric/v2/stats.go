// Copyright 2021 - 2024 Matrix Origin
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

import "github.com/prometheus/client_golang/prometheus"

var (
	statsTriggerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "stats",
			Name:      "trigger_count",
			Help:      "Total count of stats trigger.",
		}, []string{"type"})
	StatsTriggerForcedCounter   = statsTriggerCounter.WithLabelValues("forced")
	StatsTriggerUnforcedCounter = statsTriggerCounter.WithLabelValues("unforced")

	StatsUpdateBlockCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "stats",
			Name:      "update_block_count",
			Help:      "Total number of stats update block count.",
		})
)
