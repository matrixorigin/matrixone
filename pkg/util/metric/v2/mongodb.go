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
	MongoDBScanDocumentsCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "mo", Subsystem: "mongodb", Name: "scan_documents_total",
		Help: "MongoDB documents converted by MongoScan."})
	MongoDBScanRawBytesCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "mo", Subsystem: "mongodb", Name: "scan_raw_bytes_total",
		Help: "Raw BSON bytes converted by MongoScan."})
	MongoDBConversionErrorCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "mo", Subsystem: "mongodb", Name: "conversion_errors_total",
		Help: "BSON values mapped to NULL by MongoDB try_null conversion."})
	MongoDBCursorEventCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "mo", Subsystem: "mongodb", Name: "cursor_events_total",
		Help: "MongoDB cursor lifecycle outcomes without source-identifying labels."}, []string{"event"})
	MongoDBPhaseDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "mo", Subsystem: "mongodb", Name: "phase_duration_seconds",
		Help: "MongoDB source wait and BSON decode/vector append time.", Buckets: getDurationBuckets()}, []string{"phase"})
	MongoDBDriverCommandCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "mo", Subsystem: "mongodb", Name: "driver_commands_total",
		Help: "MongoDB driver command attempts and outcomes with bounded command labels."}, []string{"command", "outcome"})
	MongoDBDriverCommandDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "mo", Subsystem: "mongodb", Name: "driver_command_duration_seconds",
		Help: "MongoDB driver command duration without namespace, endpoint, or query labels.", Buckets: getDurationBuckets()}, []string{"command", "outcome"})
	MongoDBRetryableFindCounter = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "mo", Subsystem: "mongodb", Name: "retryable_find_failures_total",
		Help: "Find command attempts that failed with a retryable network error."})
	MongoDBPoolEventCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "mo", Subsystem: "mongodb", Name: "pool_events_total",
		Help: "MongoDB driver connection-pool events with bounded labels."}, []string{"event", "reason"})
	MongoDBPoolEventDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "mo", Subsystem: "mongodb", Name: "pool_event_duration_seconds",
		Help: "MongoDB connection-pool checkout and connection event duration.", Buckets: getDurationBuckets()}, []string{"event"})
	MongoDBPoolCheckedOutGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "mo", Subsystem: "mongodb", Name: "pool_checked_out_connections",
		Help: "MongoDB driver connections currently checked out across CN-local clients."})
	MongoDBSelectedServerRoleCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "mo", Subsystem: "mongodb", Name: "selected_server_roles_total",
		Help: "MongoDB commands by selected server role; endpoint identity is never exported."}, []string{"role"})
	MongoDBServerHeartbeatDurationHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "mo", Subsystem: "mongodb", Name: "server_heartbeat_duration_seconds",
		Help: "MongoDB server heartbeat duration and outcome without endpoint labels.", Buckets: getDurationBuckets()}, []string{"outcome"})
)
