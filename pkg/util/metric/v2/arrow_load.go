// Copyright 2026 Matrix Origin
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
	ArrowLoadObjectCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "mo", Subsystem: "arrow_load", Name: "objects_total",
		Help: "Arrow object or object-shard open attempts by bounded outcome."}, []string{"outcome"})
	ArrowLoadShardCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "mo", Subsystem: "arrow_load", Name: "shards_total",
		Help: "Arrow record-batch shards opened successfully."})
	ArrowLoadRecordCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "mo", Subsystem: "arrow_load", Name: "records_total",
		Help: "Non-empty Arrow record batches accepted by the External reader."})
	ArrowLoadBatchCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "mo", Subsystem: "arrow_load", Name: "batches_total",
		Help: "MatrixOne batches published by the Arrow External reader."})
	ArrowLoadRowCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "mo", Subsystem: "arrow_load", Name: "rows_total",
		Help: "Rows published by the Arrow External reader."})
	ArrowLoadPayloadBytesCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "mo", Subsystem: "arrow_load", Name: "payload_bytes_total",
		Help: "Arrow bridge payload bytes by bounded eligibility and ownership kind."}, []string{"kind"})
	ArrowLoadCopyBytesCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "mo", Subsystem: "arrow_load", Name: "copy_bytes_total",
		Help: "Bytes copied at an Arrow LOAD data-plane layer."}, []string{"layer"})
	ArrowLoadConversionColumnCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "mo", Subsystem: "arrow_load", Name: "conversion_columns_total",
		Help: "Arrow columns converted by bounded ownership mode."}, []string{"mode"})
	ArrowLoadFallbackCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "mo", Subsystem: "arrow_load", Name: "fallbacks_total",
		Help: "Arrow zero-copy fallbacks by bounded reason."}, []string{"reason"})
	ArrowLoadErrorCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "mo", Subsystem: "arrow_load", Name: "errors_total",
		Help: "Arrow reader failures by stable category without source-identifying labels."}, []string{"category"})
	ArrowLoadPhaseDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "mo", Subsystem: "arrow_load", Name: "phase_duration_seconds",
		Help: "Arrow reader phase duration by bounded phase and outcome.", Buckets: getDurationBuckets()}, []string{"phase", "outcome"})
	ArrowLoadPinnedBytesGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "mo", Subsystem: "arrow_load", Name: "pinned_bytes",
		Help: "Current Arrow FileService range and decoded-buffer capacity held by live leases."})
	ArrowLoadPinnedBytesHighWaterGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "mo", Subsystem: "arrow_load", Name: "pinned_bytes_high_water",
		Help: "Process-lifetime high-water mark of Arrow range and decoded-buffer capacity."})
)
