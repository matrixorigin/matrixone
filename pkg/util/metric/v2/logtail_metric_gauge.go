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
	LogTailSendQueueSizeGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "tn",
			Subsystem: "logtail",
			Name:      "sending_queue_size",
			Help:      "Size of sending logtail queue size.",
		})

	LogTailReceiveQueueSizeGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "cn",
			Subsystem: "logtail",
			Name:      "receive_queue_size",
			Help:      "Size of receiving logtail queue size.",
		})
)
