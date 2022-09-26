// Copyright 2022 Matrix Origin
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

package metric

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	prom "github.com/prometheus/client_golang/prometheus"
)

type cacheKey = int

const (
	cacheKeyMemStats cacheKey = iota
	cacheKeyProcess
	cacheKeyDiskIO
	cacheKeyNetIO
)

type statCaches struct {
	newest int
	//TODO(aptend): use array
	entries map[cacheKey]cacheEntry
}

type cacheEntry struct {
	version int
	value   any
}

func (c *statCaches) invalidateAll() {
	c.newest += 1
}

func (c *statCaches) get(key cacheKey) (any, bool) {
	if entry, ok := c.entries[key]; !ok {
		return nil, false
	} else if entry.version != c.newest {
		return nil, false
	} else {
		return entry.value, true
	}
}

func (c *statCaches) put(key cacheKey, val any) {
	c.entries[key] = cacheEntry{
		version: c.newest,
		value:   val,
	}
}

func (c *statCaches) getOrInsert(key cacheKey, f func() any) any {
	if val, ok := c.get(key); ok {
		return val
	}
	toPut := f()
	c.put(key, toPut)
	return toPut
}

type simpleEntry interface {
	Desc() *prom.Desc
	// entry return the metric for now. it can fetch from the caches or just compute by itself
	Metric(*statCaches) (prom.Metric, error)
}

type batchStatsCollector struct {
	selfAsPromCollector
	entris    []simpleEntry
	caches    *statCaches
	collected bool
	sync.Mutex
}

func newBatchStatsCollector(entries ...simpleEntry) Collector {
	c := &batchStatsCollector{
		entris: entries,
		caches: &statCaches{
			newest:  1,
			entries: make(map[int]cacheEntry),
		},
	}
	c.init(c)
	return c
}

// Describe returns all descriptions of the collector.
func (c *batchStatsCollector) Describe(ch chan<- *prom.Desc) {
	for _, e := range c.entris {
		ch <- e.Desc()
	}
}

// Collect returns the current state of all metrics of the collector.
func (c *batchStatsCollector) Collect(ch chan<- prom.Metric) {
	c.Lock()
	defer c.Unlock()
	c.caches.invalidateAll()
	for _, e := range c.entris {
		m, err := e.Metric(c.caches)
		if err != nil {
			if err.Error() == "not implemented yet" && c.collected {
				// log not implemented once, otherwise it is too annoying
				continue
			}
			logutil.Warnf("[Metric] %s collect a error: %v", e.Desc().String(), err)
		} else {
			// as we logged already, no need to issue a InvalidMetric
			ch <- m
		}
	}
	c.collected = true
}

type multiSimpleEntry interface {
	Desc() *prom.Desc
	Metrics() ([]prom.Metric, error)
}

type batchMetricVec struct {
	selfAsPromCollector

	multiSimpleEntry
	mux sync.Mutex
}

func newBatchMetricVec(e multiSimpleEntry) *batchMetricVec {
	c := &batchMetricVec{
		multiSimpleEntry: e,
	}
	c.init(c)
	return c
}

// Describe returns all descriptions of the collector.
func (c *batchMetricVec) Describe(ch chan<- *prom.Desc) {
	ch <- c.multiSimpleEntry.Desc()
}

func (c *batchMetricVec) Collect(ch chan<- prom.Metric) {
	c.mux.Lock()
	defer c.mux.Unlock()

	if ms, err := c.multiSimpleEntry.Metrics(); err != nil {
		logutil.Warnf("[Metric] %s collect a error: %v", c.Desc().String(), err)
	} else {
		for _, m := range ms {
			ch <- m
		}
	}
}

/*
type batchMetricVec struct {
	inner *prom.MetricVec
}

func (bmv *batchMetricVec) GetMetricWithLabelValues(lvs ...string) (Observer, error) {
	metric, err := bmv.inner.GetMetricWithLabelValues(lvs...)
	if metric != nil {
		return metric.(prom.Observer), err
	}
	return nil, err
}

// Describe implements Collector.
func (bmv *batchMetricVec) Describe(ch chan<- *prom.Desc) { bmv.inner.Describe(ch) }

// Collect implements Collector.
func (bmv *batchMetricVec) Collect(ch chan<- prom.Metric) {
	bmv.inner.Collect(ch)
}
*/
