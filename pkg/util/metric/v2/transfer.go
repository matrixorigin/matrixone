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

import "github.com/prometheus/client_golang/prometheus"

var (
	transferHitCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "mo",
		Subsystem: "transfer",
		Name:      "hit_counter",
		Help:      "The total number of transfer hit counter.",
	}, []string{"type"})

	TransferMemoryHitCounter = transferHitCounter.WithLabelValues("memory")
	TransferDiskHitCounter   = transferHitCounter.WithLabelValues("disk")
	TransferTotalHitCounter  = transferHitCounter.WithLabelValues("total")
)

var (
	transferRowCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "mo",
		Subsystem: "transfer",
		Name:      "row_counter",
		Help:      "The total number of transfer row.",
	}, []string{"type"})

	TransferRowTotalCounter = transferRowCounter.WithLabelValues("total")
	TransferRowHitCounter   = transferRowCounter.WithLabelValues("hit")
)

var (
	transferDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "mo",
		Subsystem: "transfer",
		Name:      "duration",
		Help:      "The duration of transfer.",
	}, []string{"type"})

	TransferDurationMemoryHistogram = transferDurationHistogram.WithLabelValues("memory")
	TransferDurationDiskHistogram   = transferDurationHistogram.WithLabelValues("disk")
)

var (
	TransferPageInChannelHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "mo",
		Subsystem: "transfer",
		Name:      "page_in_channel",
		Help:      "The number of page in channel.",
	})
)

var (
	transferPageWriteDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "mo",
		Subsystem: "transfer",
		Name:      "page_write_duration",
		Help:      "The duration of transfer.",
	}, []string{"type"})

	TransferPageFlushDurationHistogram = transferPageWriteDurationHistogram.WithLabelValues("flush")
	TransferPageMergeDurationHistogram = transferPageWriteDurationHistogram.WithLabelValues("merge")
)

var (
	TransferPageSinceBornDurationHistogram = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "mo",
		Subsystem: "transfer",
		Name:      "duration_since_born",
		Help:      "The duration of transfer.",
	})
)
