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
	acceptConnDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "frontend",
			Name:      "accept_connection_duration",
			Help:      "Bucketed histogram of accept connection duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"label"})
	CreatedDurationHistogram          = acceptConnDurationHistogram.WithLabelValues("created")
	EstablishDurationHistogram        = acceptConnDurationHistogram.WithLabelValues("establish")
	UpgradeTLSDurationHistogram       = acceptConnDurationHistogram.WithLabelValues("upgradeTLS")
	AuthenticateDurationHistogram     = acceptConnDurationHistogram.WithLabelValues("authenticate")
	SendErrPacketDurationHistogram    = acceptConnDurationHistogram.WithLabelValues("send-err-packet")
	SendOKPacketDurationHistogram     = acceptConnDurationHistogram.WithLabelValues("send-ok-packet")
	CheckTenantDurationHistogram      = acceptConnDurationHistogram.WithLabelValues("check-tenant")
	CheckUserDurationHistogram        = acceptConnDurationHistogram.WithLabelValues("check-user")
	CheckRoleDurationHistogram        = acceptConnDurationHistogram.WithLabelValues("check-role")
	CheckDbNameDurationHistogram      = acceptConnDurationHistogram.WithLabelValues("check-dbname")
	InitGlobalSysVarDurationHistogram = acceptConnDurationHistogram.WithLabelValues("init-global-sys-var")

	routineCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "frontend",
			Name:      "routine_count",
			Help:      "routine counter.",
		}, []string{"label"})
	CreatedRoutineCounter = routineCounter.WithLabelValues("created")
	CloseRoutineCounter   = routineCounter.WithLabelValues("close")

	requestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "frontend",
			Name:      "request_count",
			Help:      "request counter.",
		}, []string{"label"})
	StartHandleRequestCounter = requestCounter.WithLabelValues("start-handle")
	EndHandleRequestCounter   = requestCounter.WithLabelValues("end-handle")

	resolveDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "frontend",
			Name:      "resolve_duration",
			Help:      "Bucketed histogram of txnCompilerContext.Resolve duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"label"})

	TotalResolveDurationHistogram   = resolveDurationHistogram.WithLabelValues("total-resolve")
	EnsureDatabaseDurationHistogram = resolveDurationHistogram.WithLabelValues("ensure-database")
	GetSubMetaDurationHistogram     = resolveDurationHistogram.WithLabelValues("get-sub-meta")
	CheckSubValidDurationHistogram  = resolveDurationHistogram.WithLabelValues("check-sub-valid")
	GetRelationDurationHistogram    = resolveDurationHistogram.WithLabelValues("get-relation")
	OpenDBDurationHistogram         = resolveDurationHistogram.WithLabelValues("open-db")
	OpenTableDurationHistogram      = resolveDurationHistogram.WithLabelValues("open-table")
	GetTmpTableDurationHistogram    = resolveDurationHistogram.WithLabelValues("get-tmp-table")

	createAccountDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "frontend",
			Name:      "create_account_duration",
			Help:      "Bucketed histogram of Create Account duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"label"})

	TotalCreateDurationHistogram              = createAccountDurationHistogram.WithLabelValues("total-create")
	Step1DurationHistogram                    = createAccountDurationHistogram.WithLabelValues("step1")
	Step2DurationHistogram                    = createAccountDurationHistogram.WithLabelValues("step2")
	CreateTablesInMoCatalogDurationHistogram  = createAccountDurationHistogram.WithLabelValues("create-tables-in-mo-catalog")
	ExecDDL1DurationHistogram                 = createAccountDurationHistogram.WithLabelValues("exec-ddl1")
	InitData1DurationHistogram                = createAccountDurationHistogram.WithLabelValues("init-data1")
	CreateTablesInSystemDurationHistogram     = createAccountDurationHistogram.WithLabelValues("create-tables-in-system")
	CreateTablesInInfoSchemaDurationHistogram = createAccountDurationHistogram.WithLabelValues("create-tables-in-info-schema")

	pubSubDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "frontend",
			Name:      "pub_sub_duration",
			Help:      "Bucketed histogram of pub sub duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"label"})
	CreatePubHistogram = pubSubDurationHistogram.WithLabelValues("create-pub")
	AlterPubHistogram  = pubSubDurationHistogram.WithLabelValues("alter-pub")
	DropPubHistogram   = pubSubDurationHistogram.WithLabelValues("drop-pub")
	ShowPubHistogram   = pubSubDurationHistogram.WithLabelValues("show-pub")
	ShowSubHistogram   = pubSubDurationHistogram.WithLabelValues("show-sub")

	sqlLengthHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "frontend",
			Name:      "input_sql_length",
			Help:      "Bucketed histogram of Input SQL Length",
			Buckets:   prometheus.ExponentialBuckets(64, 2, 20), //from 64, to 64 * 2 ^20
		}, []string{"label"})

	TotalSQLLengthHistogram          = sqlLengthHistogram.WithLabelValues("total-sql-length")
	LoadDataInlineSQLLengthHistogram = sqlLengthHistogram.WithLabelValues("load-data-inline-sql-length")
	OtherSQLLengthHistogram          = sqlLengthHistogram.WithLabelValues("other-sql-length")

	cdcRecordCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "frontend",
			Name:      "cdc_record_count",
			Help:      "Count of records cdc read and sink",
		}, []string{"type"})
	CdcReadRecordCounter = cdcRecordCounter.WithLabelValues("read")
	CdcSinkRecordCounter = cdcRecordCounter.WithLabelValues("sink")

	cdcErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "frontend",
			Name:      "cdc_error_count",
			Help:      "Count of error",
		}, []string{"type"})
	CdcMysqlConnErrorCounter = cdcErrorCounter.WithLabelValues("mysql-conn")
	CdcMysqlSinkErrorCounter = cdcErrorCounter.WithLabelValues("mysql-sink")

	cdcRetryCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "frontend",
			Name:      "cdc_retry_count",
			Help:      "Count of CDC retry attempts",
		}, []string{"component"})
	CdcReaderRetryCounter = cdcRetryCounter.WithLabelValues("reader")
	CdcSinkerRetryCounter = cdcRetryCounter.WithLabelValues("sinker")

	cdcProcessingRecordCountGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "frontend",
			Name:      "cdc_processing_record_count",
			Help:      "Count of records cdc has read but not sunk",
		}, []string{"type"})
	CdcTotalProcessingRecordCountGauge = cdcProcessingRecordCountGauge.WithLabelValues("total")

	cdcMemoryGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "frontend",
			Name:      "cdc_memory",
			Help:      "Memory used by cdc",
		}, []string{"type"})
	CdcHoldChangesBytesGauge = cdcMemoryGauge.WithLabelValues("hold-changes")
	CdcMpoolInUseBytesGauge  = cdcMemoryGauge.WithLabelValues("mpool-inuse")

	cdcDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "frontend",
			Name:      "cdc_duration",
			Help:      "Bucketed histogram of each phase duration of cdc task.",
			Buckets:   getDurationBuckets(),
		}, []string{"type"})
	CdcReadDurationHistogram    = cdcDurationHistogram.WithLabelValues("read")
	CdcAppendDurationHistogram  = cdcDurationHistogram.WithLabelValues("append")
	CdcSinkDurationHistogram    = cdcDurationHistogram.WithLabelValues("sink")
	CdcSendSqlDurationHistogram = cdcDurationHistogram.WithLabelValues("send-sql")
)
