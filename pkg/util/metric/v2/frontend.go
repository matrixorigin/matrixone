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
)
