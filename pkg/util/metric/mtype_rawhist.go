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
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/metric"
	prom "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type Observer interface {
	prom.Observer
}

type HistogramOpts = prom.HistogramOpts

// ExponentialBuckets produces list like `[start, start * factor, start * factor^2, ..., start * factor^(count-1)]`
func ExponentialBuckets(start, factor float64, count int) []float64 {
	return prom.ExponentialBuckets(start, factor, count)
}

// record appends a new RawHist.Sample into the inner buffer.
// if the length of the buffer exceeds limit, a new buffer will be created
// and all existed samples will be added to MetricExporter.
func (r *rawHist) record(t int64, v float64) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.buf = append(r.buf, &pb.Sample{
		Datetime: t,
		Value:    v,
	})
	if int32(len(r.buf)) >= GetRawHistBufLimit() {
		buf := r.buf
		r.buf = make([]*pb.Sample, 0, GetRawHistBufLimit()/2)
		go r.assembleAndSend(buf)
	}
}

func (r *rawHist) assembleAndSend(buf []*pb.Sample) {
	if r.exporter == nil || r.exporter.Get() == nil {
		// rawHist has no exporter yet, ignore collection
		return
	}
	o := r.opts
	name := prom.BuildFQName(o.Namespace, o.Subsystem, o.Name)
	rawHistMetric := &pb.RawHist{
		Samples: buf,
	}
	metric := &pb.Metric{
		Label:   pb.P2MLabelPairs(r.labelPairs),
		RawHist: rawHistMetric,
	}
	mf := &pb.MetricFamily{
		Name:   name,
		Help:   o.Help,
		Type:   pb.MetricType_RAWHIST,
		Metric: []*pb.Metric{metric},
	}

	if err := r.exporter.Get().ExportMetricFamily(context.TODO(), mf); err != nil {
		logutil.Errorf("[Metric] rawhist send error: %v", err)
	}
}

type rawHist struct {
	// Meta

	compat_inner prom.Observer       // a prometheus histogram struct
	desc         *prom.Desc          // Used for Collector interface
	opts         *prom.HistogramOpts // Desc has nothing in public, we have to hold the origin opts
	labelPairs   []*dto.LabelPair

	// For buf write

	buf   []*pb.Sample
	mutex *sync.Mutex
	// store a pointer to interface because newRawHist can be called before creating exporter
	exporter *ExporterHolder

	// For tests

	now func() int64
}

func NewRawHist(opts prom.HistogramOpts) *rawHist {
	mustValidLbls(opts.Name, opts.ConstLabels, nil)
	compat := prom.NewHistogram(opts)
	return newRawHist(
		prom.NewDesc(
			prom.BuildFQName(opts.Namespace, opts.Subsystem, opts.Name),
			opts.Help,
			nil,
			opts.ConstLabels,
		),
		compat,
		&opts,
	)
}

func newRawHist(desc *prom.Desc, compatHist prom.Observer, opts *prom.HistogramOpts, lvs ...string) *rawHist {
	raw := &rawHist{
		compat_inner: compatHist,
		desc:         desc,
		opts:         opts,
		labelPairs:   prom.MakeLabelPairs(desc, lvs),

		mutex:    &sync.Mutex{},
		exporter: &exporterHolder,
		now:      func() int64 { return time.Now().UnixMicro() },
	}
	return raw
}

// Describe implements Collector.
func (r *rawHist) Describe(ch chan<- *prom.Desc) {
	ch <- r.Desc()
}

// Collect implements Collector.
func (r *rawHist) Collect(ch chan<- prom.Metric) {
	ch <- r
}

// Write impls Metric
func (r *rawHist) Write(out *dto.Metric) error {
	// prometheus registry will check if the label value is valid utf8
	// which means we can't serialize samples into label value.
	// here Write is just seen as a signal for sending samples to exporter
	out.Untyped = &dto.Untyped{}
	out.Label = r.labelPairs

	// TODO: just curious, can we make race detector ignore this read?
	// if len(r.buf) == 0 {
	// 	return nil
	// }

	// swap the buf with mutex locked
	var buf []*pb.Sample
	r.mutex.Lock()
	if len(r.buf) == 0 {
		r.mutex.Unlock()
		return nil
	}
	buf = r.buf
	r.buf = make([]*pb.Sample, 0, len(buf))
	r.mutex.Unlock()

	r.assembleAndSend(buf)

	return nil
}

// Desc impls Metric
func (r *rawHist) Desc() *prom.Desc {
	return r.desc
}

// Observe records the value as a sample
func (r *rawHist) Observe(value float64) {
	if r.compat_inner != nil {
		r.compat_inner.Observe(value)
	}
	r.record(r.now(), value)
}

// CancelToProm stops recording samples to prometheus.
// rawHist is born with a innner prometheus histogram struct, CancelToProm can be used to remove the inner one when init metric
func (r *rawHist) CancelToProm() {
	if r.compat_inner == nil {
		return
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.compat_inner = nil
}

func (r *rawHist) CollectorToProm() prom.Collector {
	return r.compat_inner.(prom.Collector)
}

// WithExporter only for test, should call before Registry) MustRegister
func (r *rawHist) WithExporter(e *ExporterHolder) {
	r.exporter = e
}

// WithNowFunction only for test, should call before Registry) MustRegister
func (r *rawHist) WithNowFunction(now func() int64) {
	r.now = now
}

// RawHistVec is a Collector that bundles a set of RawHist that all share the
// same Desc, but have different values for their variable labels. It can be
// used as a factory for a series of Observers
type RawHistVec struct {
	compat *prom.HistogramVec
	inner  *prom.MetricVec
}

// NewRawHistVec creates a new NewRawHistVec based on the provided HistogramOpts and
// partitioned by the given label names.
func NewRawHistVec(opts prom.HistogramOpts, labelNames []string) *RawHistVec {
	mustValidLbls(opts.Name, opts.ConstLabels, labelNames)
	desc := prom.NewDesc(
		prom.BuildFQName(opts.Namespace, opts.Subsystem, opts.Name),
		opts.Help,
		labelNames,
		opts.ConstLabels,
	)
	r := &RawHistVec{compat: prom.NewHistogramVec(opts, labelNames)}

	inner := prom.NewMetricVec(desc, func(lvs ...string) prom.Metric {
		var compat_observer prom.Observer
		if r.compat != nil {
			compat_observer = r.compat.WithLabelValues(lvs...)
		}
		return newRawHist(desc, compat_observer, &opts, lvs...)
	})
	r.inner = inner
	return r
}

func (v *RawHistVec) CancelToProm() {
	v.compat = nil

	ch := make(chan prom.Metric, 100)
	go func() { v.Collect(ch); close(ch) }()
	for m := range ch {
		m.(*rawHist).CancelToProm()
	}
}

func (v *RawHistVec) CollectorToProm() prom.Collector {
	return v.compat
}

func (v *RawHistVec) Collect(ch chan<- prom.Metric) {
	v.inner.Collect(ch)
}

func (v *RawHistVec) Describe(ch chan<- *prom.Desc) {
	v.inner.Describe(ch)
}

func (v *RawHistVec) GetMetricWithLabelValues(lvs ...string) (Observer, error) {
	metric, err := v.inner.GetMetricWithLabelValues(lvs...)
	if metric != nil {
		return metric.(prom.Observer), err
	}
	return nil, err
}

func (v *RawHistVec) WithLabelValues(lvs ...string) prom.Observer {
	h, err := v.GetMetricWithLabelValues(lvs...)
	if err != nil {
		panic(err)
	}
	return h
}
