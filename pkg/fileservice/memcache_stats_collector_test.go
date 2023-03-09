package fileservice

import (
	"github.com/prometheus/client_golang/prometheus"
	"testing"
)

// Reference implementation : https://github.com/prometheus/client_golang/blob/3d2cf0b338e19b0eaf277496058fc77bd3add440/prometheus/collectors/dbstats_collector_test.go#L23
func TestMemCacheStatsCollector(t *testing.T) {
	reg := prometheus.NewRegistry()

	db := NewMemCache(1)
	if err := reg.Register(NewMemCacheStatsCollector(db, "MemCache")); err != nil {
		t.Fatal(err)
	}

	mfs, err := reg.Gather()

	if err != nil {
		t.Fatal(err)
	}

	names := []string{
		"dev_metrics_memcache_read_count",
		"dev_metrics_memcache_hit_count",
	}
	type result struct {
		found bool
	}
	results := make(map[string]result)
	for _, name := range names {
		results[name] = result{found: false}
	}
	for _, mf := range mfs {
		m := mf.GetMetric()
		if len(m) != 1 {
			t.Errorf("expected 1 metrics bug got %d", len(m))
		}

		labelA := m[0].GetLabel()[0]
		if name := labelA.GetName(); name != "family_name" {
			t.Errorf("expected to get label \"db_name\" but got %s", name)
		}
		if value := labelA.GetValue(); value != "MemCache" {
			t.Errorf("expected to get value \"MemCache\" but got %s", value)
		}

		for _, name := range names {
			if name == mf.GetName() {
				results[name] = result{found: true}
				break
			}
		}
	}

	for name, result := range results {
		if !result.found {
			t.Errorf("%s not found", name)
		}
	}
}
