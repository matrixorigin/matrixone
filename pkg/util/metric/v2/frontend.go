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
