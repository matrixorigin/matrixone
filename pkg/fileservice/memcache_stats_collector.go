package fileservice

import (
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/prometheus/client_golang/prometheus"
)

type memcacheStatsCollector struct {
	memcache *MemCache

	numRead *prometheus.Desc
	numHit  *prometheus.Desc
}

// NewMemCacheStatsCollector Creates a Prometheus Batch Stats Collector for MemCache
// Implementation is similar to https://pkg.go.dev/github.com/prometheus/client_golang/prometheus/collectors#NewDBStatsCollector
func NewMemCacheStatsCollector(memcache *MemCache, familyName string) prometheus.Collector {

	return &memcacheStatsCollector{
		memcache: memcache,
		numRead: prometheus.NewDesc(
			metric.FullyQualifiedName("memcache_read_count"),
			"Number of Reads in MemCache.",
			nil, prometheus.Labels{"family_name": familyName},
		),
		numHit: prometheus.NewDesc(
			metric.FullyQualifiedName("memcache_hit_count"),
			"Number of Hits in MemCache.",
			nil, prometheus.Labels{"family_name": familyName},
		),
	}
}

func (m *memcacheStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- m.numRead
	ch <- m.numHit
}

func (m *memcacheStatsCollector) Collect(ch chan<- prometheus.Metric) {
	stats := m.memcache.CacheStats()
	ch <- prometheus.MustNewConstMetric(m.numRead, prometheus.CounterValue, float64(stats.NumRead))
	ch <- prometheus.MustNewConstMetric(m.numHit, prometheus.CounterValue, float64(stats.NumHit))

}

var _ prometheus.Collector = new(memcacheStatsCollector)
