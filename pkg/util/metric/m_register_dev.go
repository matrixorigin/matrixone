package metric

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	prom "github.com/prometheus/client_golang/prometheus"
)

var (
	DefaultDevMetricRegistry = prom.NewRegistry()
)

// RegisterDevMetric We register collector to global dev metric registry. Similar to https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#Register
func RegisterDevMetric(c prom.Collector) {
	if err := DefaultDevMetricRegistry.Register(c); err != nil {
		// err is either registering a collector more than once or metrics have duplicate description.
		// in any case, we respect the existing collectors in the prom registry
		logutil.Debugf("[Metric] register to prom register: %v", err)
	}
}

var FullyQualifiedName = func(name string) string {
	return "dev_metrics_" + name
}
