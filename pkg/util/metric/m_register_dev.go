package metric

import prom "github.com/prometheus/client_golang/prometheus"

var (
	DefaultDevMetricRegistry = prom.NewRegistry()
)

func RegisterDevMetric(c prom.Collector) {
	DefaultDevMetricRegistry.MustRegister(c)
}

var FullyQualifiedName = func(name string) string {
	return "dev_metrics_" + name
}
