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
	AcceptConnDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "frontend",
			Name:      "accept_connection_duration",
			Help:      "Bucketed histogram of accept connection duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"label"})
	CreatedDurationHistogram          = AcceptConnDurationHistogram.WithLabelValues("created")
	EstablishDurationHistogram        = AcceptConnDurationHistogram.WithLabelValues("establish")
	UpgradeTLSDurationHistogram       = AcceptConnDurationHistogram.WithLabelValues("upgradeTLS")
	AuthenticateDurationHistogram     = AcceptConnDurationHistogram.WithLabelValues("authenticate")
	CheckTenantDurationHistogram      = AcceptConnDurationHistogram.WithLabelValues("check-tenant")
	CheckUserDurationHistogram        = AcceptConnDurationHistogram.WithLabelValues("check-user")
	CheckRoleDurationHistogram        = AcceptConnDurationHistogram.WithLabelValues("check-role")
	CheckDbNameDurationHistogram      = AcceptConnDurationHistogram.WithLabelValues("check-dbname")
	InitGlobalSysVarDurationHistogram = AcceptConnDurationHistogram.WithLabelValues("init-global-sys-var")
)
