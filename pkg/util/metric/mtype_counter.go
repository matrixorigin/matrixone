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
	"math"
	"sync"
	"sync/atomic"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type Counter interface {
	prom.Counter
}

type CounterOpts = prom.CounterOpts

func NewCounter(opts prom.CounterOpts) *ratecounter {
	mustValidLbls(opts.Name, opts.ConstLabels, nil)
	return newRateCounter(opts)
}

func NewCounterVec(opts CounterOpts, lvs []string, doAvg bool) *rateCounterVec {
	mustValidLbls(opts.Name, opts.ConstLabels, lvs)
	return newRateCounterVec(opts, lvs, doAvg)
}

type counter struct {
	selfAsPromCollector
	prom.Counter
}

func newCounter(opts CounterOpts) *counter {
	c := &counter{
		Counter: prom.NewCounter(opts),
	}
	c.init(c)
	return c
}

type counterVec struct {
	selfAsPromCollector
	*prom.CounterVec
}

func newCounterVec(opts CounterOpts, lvs []string) *counterVec {
	cv := &counterVec{
		CounterVec: prom.NewCounterVec(opts, lvs),
	}
	cv.init(cv)
	return cv
}

type ratecounter struct {
	compat  *compatCounter
	valBits uint64
	valInt  uint64

	mu struct {
		sync.Mutex
		prevV float64
		prevT time.Time
		doAvg bool
	}

	desc       *prom.Desc
	labelPairs []*dto.LabelPair

	now func() time.Time
}

func newRateCounter(opts CounterOpts) *ratecounter {
	desc := prom.NewDesc(
		prom.BuildFQName(opts.Namespace, opts.Subsystem, opts.Name),
		opts.Help,
		nil,
		opts.ConstLabels,
	)
	return newRateCounterBase(desc, true)
}

func newRateCounterBase(desc *prom.Desc, doAvg bool, lvs ...string) *ratecounter {
	c := &ratecounter{desc: desc, labelPairs: prom.MakeLabelPairs(desc, lvs), now: time.Now}
	c.mu.doAvg = doAvg
	c.compat = &compatCounter{c}
	return c
}

func (c *ratecounter) Add(v float64) {
	if v < 0 {
		panic("counter cannot decrease in value")
	}

	ival := uint64(v)
	if float64(ival) == v {
		atomic.AddUint64(&c.valInt, ival)
		return
	}

	for {
		oldBits := atomic.LoadUint64(&c.valBits)
		newBits := math.Float64bits(math.Float64frombits(oldBits) + v)
		if atomic.CompareAndSwapUint64(&c.valBits, oldBits, newBits) {
			return
		}
	}
}

// this is used only in disk IO counter, can be deleted if we don't need to calc rate in Write
func (c *ratecounter) Set(v uint64) {
	atomic.StoreUint64(&c.valInt, v)
}

func (c *ratecounter) Inc() { atomic.AddUint64(&c.valInt, 1) }

func (c *ratecounter) Desc() *prom.Desc { return c.desc }

var zeroValue float64 = 0.0

func (c *ratecounter) Write(out *dto.Metric) error {
	fval := math.Float64frombits(atomic.LoadUint64(&c.valBits))
	ival := atomic.LoadUint64(&c.valInt)
	val := fval + float64(ival)

	out.Label = c.labelPairs
	instant := c.now()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mu.doAvg {
		if c.mu.prevT.IsZero() {
			out.Counter = &dto.Counter{Value: &zeroValue}
		} else {
			rate := (val - c.mu.prevV) / instant.Sub(c.mu.prevT).Seconds()
			out.Counter = &dto.Counter{Value: &rate}
		}
	} else {
		rate := val - c.mu.prevV
		out.Counter = &dto.Counter{Value: &rate}
	}

	c.mu.prevT = instant
	c.mu.prevV = val

	return nil
}

// Describe implements prom.Collector.
func (c *ratecounter) Describe(ch chan<- *prom.Desc) { ch <- c.Desc() }

// Collect implements prom.Collector.
func (c *ratecounter) Collect(ch chan<- prom.Metric) { ch <- c }

// CancelToProm implements mo.Collector.
func (c *ratecounter) CancelToProm() {}

// CollectorToProm implements mo.Collector.
func (c *ratecounter) CollectorToProm() prom.Collector { return c.compat }

type compatCounter struct {
	raw *ratecounter
}

// Describe implements Collector.
func (cc *compatCounter) Describe(ch chan<- *prom.Desc) { ch <- cc.raw.Desc() }

// Collect implements Collector.
func (cc *compatCounter) Collect(ch chan<- prom.Metric) { ch <- cc }

// Desc impls Metric
func (cc *compatCounter) Desc() *prom.Desc {
	return cc.raw.desc
}

// Write impls Metric
func (cc *compatCounter) Write(out *dto.Metric) error {
	fval := math.Float64frombits(atomic.LoadUint64(&cc.raw.valBits))
	ival := atomic.LoadUint64(&cc.raw.valInt)
	val := fval + float64(ival)
	out.Label = cc.raw.labelPairs
	out.Counter = &dto.Counter{Value: &val}
	return nil
}

type rateCounterVec struct {
	compat *compatCounterVec
	inner  *prom.MetricVec
}

func newRateCounterVec(opts CounterOpts, labelNames []string, doAvg bool) *rateCounterVec {
	desc := prom.NewDesc(
		prom.BuildFQName(opts.Namespace, opts.Subsystem, opts.Name),
		opts.Help,
		labelNames,
		opts.ConstLabels,
	)

	vec := &rateCounterVec{
		inner: prom.NewMetricVec(desc, func(lvs ...string) prom.Metric {
			return newRateCounterBase(desc, doAvg, lvs...)
		})}

	vec.compat = &compatCounterVec{vec}
	return vec
}

func (v *rateCounterVec) CancelToProm() {}

func (v *rateCounterVec) CollectorToProm() prom.Collector {
	return v.compat
}

func (v *rateCounterVec) Collect(ch chan<- prom.Metric) {
	v.inner.Collect(ch)
}

func (v *rateCounterVec) Describe(ch chan<- *prom.Desc) {
	v.inner.Describe(ch)
}

func (v *rateCounterVec) GetMetricWithLabelValues(lvs ...string) (Counter, error) {
	metric, err := v.inner.GetMetricWithLabelValues(lvs...)
	if metric != nil {
		return metric.(Counter), err
	}
	return nil, err
}

func (v *rateCounterVec) WithLabelValues(lvs ...string) Counter {
	h, err := v.GetMetricWithLabelValues(lvs...)
	if err != nil {
		panic(err)
	}
	return h
}

type compatCounterVec struct {
	raw *rateCounterVec
}

// Describe implements Collector.
func (ccv *compatCounterVec) Describe(ch chan<- *prom.Desc) { ccv.raw.inner.Describe(ch) }

// Collect implements Collector.
func (ccv *compatCounterVec) Collect(ch chan<- prom.Metric) {
	rawch := make(chan prom.Metric, 20)
	go func() { ccv.raw.inner.Collect(rawch); close(rawch) }()
	for m := range rawch {
		ch <- m.(*ratecounter).compat
	}
}
