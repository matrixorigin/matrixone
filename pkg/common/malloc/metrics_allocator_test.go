// Copyright 2024 Matrix Origin
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

package malloc

import (
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type cappedTestAllocator struct {
	capacity int
}

type blockingGauge struct {
	prometheus.Gauge
	mu      sync.Mutex
	started chan struct{}
	release chan struct{}
}

func (g *blockingGauge) blockNextAdd() (<-chan struct{}, func()) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.started = make(chan struct{})
	g.release = make(chan struct{})
	release := g.release
	return g.started, func() { close(release) }
}

func (g *blockingGauge) Add(v float64) {
	g.mu.Lock()
	started, release := g.started, g.release
	g.started, g.release = nil, nil
	g.mu.Unlock()
	if started != nil {
		close(started)
		<-release
	}
	g.Gauge.Add(v)
}

func (a cappedTestAllocator) Allocate(size uint64, _ Hints) ([]byte, Deallocator, error) {
	return make([]byte, int(size), a.capacity), FuncDeallocator(func() {}), nil
}

func TestMetricsAllocator(t *testing.T) {
	testAllocator(t, func() Allocator {
		return NewMetricsAllocator(
			newUpstreamAllocatorForTest(),
			nil, nil, nil, nil, nil,
		)
	})
}

func TestMetricsAllocatorAccountsBackingCapacity(t *testing.T) {
	allocator := NewMetricsAllocator(
		cappedTestAllocator{capacity: 1024},
		nil, nil, nil, nil, nil,
	)
	_, dec, err := allocator.Allocate(700, NoHints)
	if err != nil {
		t.Fatal(err)
	}
	dec.Deallocate()
}

func TestMetricsAllocatorAggregatesAbsoluteInuseGauge(t *testing.T) {
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_metrics_allocator_absolute_inuse"})
	first := NewMetricsAllocator(cappedTestAllocator{capacity: 100}, nil, nil, nil, nil, gauge)
	second := NewMetricsAllocator(cappedTestAllocator{capacity: 200}, nil, nil, nil, nil, gauge)

	_, firstDec, err := first.Allocate(1, NoHints)
	if err != nil {
		t.Fatal(err)
	}
	_, secondDec, err := second.Allocate(1, NoHints)
	if err != nil {
		t.Fatal(err)
	}
	assertGaugeValue(t, gauge, 300)

	firstDec.Deallocate()
	assertGaugeValue(t, gauge, 200)

	secondDec.Deallocate()
	assertGaugeValue(t, gauge, 0)
}

func TestMetricsAllocatorRefreshRearmsAfterAllocateDuringPublish(t *testing.T) {
	base := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_metrics_allocator_allocate_during_publish"})
	gauge := &blockingGauge{Gauge: base}
	allocator := NewMetricsAllocator(cappedTestAllocator{capacity: 100}, nil, gauge, nil, nil, nil)

	started, release := gauge.blockNextAdd()
	_, first, err := allocator.Allocate(1, NoHints)
	requireNoError(t, err)
	<-started
	_, second, err := allocator.Allocate(1, NoHints)
	requireNoError(t, err)
	release()

	assertGaugeValue(t, base, 200)
	first.Deallocate()
	second.Deallocate()
	assertGaugeValue(t, base, 0)
}

func TestMetricsAllocatorRefreshRearmsAfterReleaseDuringPublish(t *testing.T) {
	base := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_metrics_allocator_release_during_publish"})
	gauge := &blockingGauge{Gauge: base}
	allocator := NewMetricsAllocator(cappedTestAllocator{capacity: 100}, nil, gauge, nil, nil, nil)

	_, first, err := allocator.Allocate(1, NoHints)
	requireNoError(t, err)
	assertGaugeValue(t, base, 100)

	started, release := gauge.blockNextAdd()
	_, second, err := allocator.Allocate(1, NoHints)
	requireNoError(t, err)
	<-started
	first.Deallocate()
	release()

	assertGaugeValue(t, base, 100)
	second.Deallocate()
	assertGaugeValue(t, base, 0)
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func assertGaugeValue(t *testing.T, gauge prometheus.Gauge, want float64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if testutil.ToFloat64(gauge) == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("gauge = %v, want %v", testutil.ToFloat64(gauge), want)
}

func BenchmarkMetricsAllocator(b *testing.B) {
	for _, n := range benchNs {
		benchmarkAllocator(b, func() Allocator {
			return NewMetricsAllocator(
				newUpstreamAllocatorForTest(),
				nil, nil, nil, nil, nil,
			)
		}, n)
	}
}

func FuzzMetricsAllocator(f *testing.F) {
	fuzzAllocator(f, func() Allocator {
		return NewMetricsAllocator(
			newUpstreamAllocatorForTest(),
			nil, nil, nil, nil, nil,
		)
	})
}
