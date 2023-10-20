package v2

import "github.com/prometheus/client_golang/prometheus"

var (
	HeartbeatHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "hakeeper",
			Subsystem: "heartbeat_send",
			Name:      "duration_seconds",
			Help:      "hakeeper heartbeat send durations",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		}, []string{"type"})

	HeartbeatFailureCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "hakeeper",
			Subsystem: "heartbeat_send",
			Name:      "failed_total",
			Help:      "hakeeper heartbeat failed count",
		}, []string{"type"})
)
