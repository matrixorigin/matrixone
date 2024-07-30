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

	sqlLengthHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "frontend",
			Name:      "input_sql_length",
			Help:      "Bucketed histogram of Input SQL Length",
			Buckets:   getDurationBuckets(),
		}, []string{"label"})

	TotalSQLLengthHistogram          = sqlLengthHistogram.WithLabelValues("total-sql-length")
	LoadDataInlineSQLLengthHistogram = sqlLengthHistogram.WithLabelValues("load-data-inline-sql-length")
	OtherSQLLengthHistogram          = sqlLengthHistogram.WithLabelValues("other-sql-length")
)
