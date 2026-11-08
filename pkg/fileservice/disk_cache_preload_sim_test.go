// Copyright 2026 Matrix Origin
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

package fileservice

import (
	"container/list"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

// simNetwork is the latency and bandwidth of object storage.
type simNetwork struct {
	latency   time.Duration
	bandwidth float64 // bytes per second
}

// simWorkload is a group that reads share of each of objects objects of objectSize
// bytes, in reads range reads, rounds times in random order, through a disk cache of
// capacity bytes.
type simWorkload struct {
	objects    int
	objectSize int64
	share      float64
	reads      int
	rounds     int
	capacity   int64
}

type simEntry struct {
	key  string
	size int64
}

// simLRU is a byte-bounded LRU cache that reports evictions.
type simLRU struct {
	capacity int64
	used     int64
	order    *list.List
	entries  map[string]*list.Element
	onEvict  func(key string, size int64)
}

func newSimLRU(capacity int64, onEvict func(string, int64)) *simLRU {
	return &simLRU{capacity: capacity, order: list.New(), entries: map[string]*list.Element{}, onEvict: onEvict}
}

func (c *simLRU) get(key string) bool {
	if e, ok := c.entries[key]; ok {
		c.order.MoveToFront(e)
		return true
	}
	return false
}

func (c *simLRU) set(key string, size int64) {
	c.entries[key] = c.order.PushFront(&simEntry{key: key, size: size})
	c.used += size
	for c.used > c.capacity {
		e := c.order.Back()
		v := e.Value.(*simEntry)
		c.order.Remove(e)
		delete(c.entries, v.key)
		c.used -= v.size
		if c.onEvict != nil {
			c.onEvict(v.key, v.size)
		}
	}
}

type simMode int

const (
	simWhole simMode = iota
	simRanges
	simAdaptive
)

// simulate returns the seconds the workload spends on object storage.
func simulate(net simNetwork, w simWorkload, mode simMode, cost preloadCostConfig) float64 {
	const group = 1
	lat := net.latency.Seconds()
	rangeBytes := int64(float64(w.objectSize) * w.share)
	wholeSeconds := lat + float64(w.objectSize)/net.bandwidth
	rangeSeconds := float64(w.reads)*lat + float64(rangeBytes)/net.bandwidth

	p, clock := newTestPreloadUsage()
	p.cost = cost
	cache := newSimLRU(w.capacity, func(key string, size int64) {
		if isFullFileCachePath(key) {
			p.removed(key, size)
		}
	})
	fits := func() bool { return cache.used+w.objectSize <= cache.capacity }

	rng := rand.New(rand.NewSource(1))
	var total float64
	for r := 0; r < w.rounds; r++ {
		for _, i := range rng.Perm(w.objects) {
			whole := fmt.Sprintf("/cache/00%d/full%d.mofscache", i%10, i)
			ranges := fmt.Sprintf("/cache/00%d/ranges%d", i%10, i)
			if cache.get(whole) {
				p.read(whole, rangeBytes, int64(w.reads))
				continue
			}
			if cache.get(ranges) {
				continue
			}
			narrow := mode == simRanges
			if mode == simAdaptive {
				narrow = p.narrowRead(whole, group, fits)
			}
			var seconds float64
			if narrow {
				seconds = rangeSeconds
				for j := 0; j < w.reads; j++ {
					p.rangeRead(whole, group, rangeBytes/int64(w.reads), w.objectSize)
				}
				cache.set(ranges, rangeBytes)
			} else {
				seconds = wholeSeconds
				cache.set(whole, w.objectSize)
				p.cached(whole)
				p.read(whole, rangeBytes, int64(w.reads))
			}
			total += seconds
			clock.t = clock.t.Add(time.Duration(seconds * float64(time.Second)))
		}
	}
	return total
}

func envFloat(name string, def float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(name), 64); err == nil {
		return v
	}
	return def
}

// TestPreloadCostSimulation sweeps network speeds and read patterns and reports how far
// the adaptive rule is from the cheaper of always whole objects and always ranges.
// MO_SIM_RANGE_MARGIN and MO_SIM_WHOLE_MARGIN override the margins; -v prints the table.
func TestPreloadCostSimulation(t *testing.T) {
	cost := defaultPreloadCostConfig()
	cost.rangeMargin = envFloat("MO_SIM_RANGE_MARGIN", cost.rangeMargin)
	cost.wholeMargin = envFloat("MO_SIM_WHOLE_MARGIN", cost.wholeMargin)

	const objectSize = 128 << 20
	networks := []simNetwork{
		{time.Millisecond, 2000 << 20},
		{5 * time.Millisecond, 500 << 20},
		{25 * time.Millisecond, 100 << 20},
		{25 * time.Millisecond, 500 << 20},
		{100 * time.Millisecond, 50 << 20},
	}
	shares := []float64{0.05, 0.2, 0.47, 0.8}
	reads := []int{4, 32, 256}
	workingSets := []int{8, 24, 64} // objects, against a 16-object cache

	var worst, sum float64
	n := 0
	for _, net := range networks {
		for _, share := range shares {
			for _, k := range reads {
				for _, objects := range workingSets {
					w := simWorkload{
						objects: objects, objectSize: objectSize, share: share, reads: k,
						rounds: 8, capacity: 16 * objectSize,
					}
					costModels(t, net.latency, net.bandwidth, net.bandwidth)
					whole := simulate(net, w, simWhole, cost)
					ranges := simulate(net, w, simRanges, cost)
					adaptive := simulate(net, w, simAdaptive, cost)
					best := min(whole, ranges)
					ratio := adaptive / best
					worst = max(worst, ratio)
					sum += ratio
					n++
					if testing.Verbose() {
						t.Logf("lat %5v bw %5dMB/s share %.2f reads %3d objects %2d: whole %8.1fs ranges %8.1fs adaptive %8.1fs  x%.2f",
							net.latency, int(net.bandwidth)>>20, share, k, objects, whole, ranges, adaptive, ratio)
					}
				}
			}
		}
	}
	t.Logf("range margin %.2f whole margin %.2f: adaptive/best mean x%.3f worst x%.2f over %d cases",
		cost.rangeMargin, cost.wholeMargin, sum/float64(n), worst, n)
}
