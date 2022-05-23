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
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/metric/pb"
)

const CHAN_CAPACITY = 10000

type MetricCollector interface {
	SendMetrics(context.Context, []*pb.MetricFamily) error
	Start()
	Stop()
}

type collectorOpts struct {
	// if a MetricFamily has `metricThreshold` Metrics or more
	// it deserves a flush operation
	metricThreshold int
	// if a RawHist MetricFamily has `sampleThreshold` Samples or more
	// it deserves a flush operation
	sampleThreshold int
	// if we can't flush a MetricFamily for the reason of `metricThreshold` or `sampleThreshold`
	// after `flushInterval`, we will flush it anyway
	flushInterval time.Duration
	// the number of goroutines to execute insert into sql, default is runtime.NumCPU()
	sqlWorkerNum int
}

func defaultCollectorOpts() collectorOpts {
	return collectorOpts{
		metricThreshold: 10,
		sampleThreshold: 50,
		flushInterval:   15 * time.Second,
		sqlWorkerNum:    runtime.NumCPU(),
	}
}

type collectorOpt interface {
	ApplyTo(*collectorOpts)
}

type WithMetricThreshold int

func (x WithMetricThreshold) ApplyTo(o *collectorOpts) {
	o.metricThreshold = int(x)
}

type WithSampleThreshold int

func (x WithSampleThreshold) ApplyTo(o *collectorOpts) {
	o.sampleThreshold = int(x)
}

type WithSqlWorkerNum int

func (x WithSqlWorkerNum) ApplyTo(o *collectorOpts) {
	o.sqlWorkerNum = int(x)
}

type WithFlushInterval time.Duration

func (x WithFlushInterval) ApplyTo(o *collectorOpts) {
	o.flushInterval = time.Duration(x)
}

type metricCollector struct {
	ieFactory         func() ie.InternalExecutor
	isRunning         int32
	opts              collectorOpts
	mf_ch             chan *pb.MetricFamily
	sql_ch            chan string
	sqlWorkerCancel   context.CancelFunc
	mergeWorkerCancel context.CancelFunc
}

func newMetricCollector(factory func() ie.InternalExecutor, opts ...collectorOpt) MetricCollector {
	initOpts := defaultCollectorOpts()
	for _, o := range opts {
		o.ApplyTo(&initOpts)
	}
	c := &metricCollector{
		ieFactory: factory,
		opts:      initOpts,
		sql_ch:    make(chan string, CHAN_CAPACITY),
		mf_ch:     make(chan *pb.MetricFamily, CHAN_CAPACITY),
	}
	return c
}

func (c *metricCollector) SendMetrics(ctx context.Context, mfs []*pb.MetricFamily) error {
	for _, mf := range mfs {
		c.mf_ch <- mf
	}
	return nil
}

func (c *metricCollector) Start() {
	if atomic.SwapInt32(&c.isRunning, 1) == 1 {
		return
	}
	c.startSqlWorker()
	c.startMergeWorker()
}

func (c *metricCollector) Stop() {
	c.sqlWorkerCancel()
	c.mergeWorkerCancel()
	atomic.StoreInt32(&c.isRunning, 0)
}

func (c *metricCollector) startSqlWorker() {
	ctx, cancel := context.WithCancel(context.Background())
	c.sqlWorkerCancel = cancel
	for i := 0; i < c.opts.sqlWorkerNum; i++ {
		exec := c.ieFactory()
		exec.ApplySessionOverride(ie.NewOptsBuilder().Database(METRIC_DB).Internal(true).Finish())
		go c.sqlWorker(ctx, exec)
	}
}

func (c *metricCollector) startMergeWorker() {
	ctx, cancel := context.WithCancel(context.Background())
	c.mergeWorkerCancel = cancel
	go c.mergeWorker(ctx)
}

func (c *metricCollector) mergeWorker(ctx context.Context) {
	mfByNames := make(map[string]*pb.MetricFamily)
	sqlbuf := new(bytes.Buffer)
	reminder := newReminder()
	defer reminder.CleanAll()

	doFlush := func(mf *pb.MetricFamily) {
		c.pushToSqlCh(mf, sqlbuf)
		mf.Metric = mf.Metric[:0]
		reminder.Reset(mf.GetName(), c.opts.flushInterval)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case mf := <-c.mf_ch:
			if isFullBatchRawHist(mf) {
				c.pushToSqlCh(mf, sqlbuf)
				continue
			}
			name := mf.GetName()
			entryMf := mfByNames[name]
			if entryMf != nil {
				entryMf.Metric = append(entryMf.Metric, mf.Metric...)
			} else {
				mfByNames[name] = mf
				entryMf = mf
				reminder.Register(name, c.opts.flushInterval)
			}
			if c.shouldFlush(entryMf) {
				doFlush(entryMf)
			}
		case name := <-reminder.C:
			if entryMf := mfByNames[name]; entryMf != nil && len(entryMf.Metric) > 0 {
				doFlush(entryMf)
			}
		}
	}
}

func (c *metricCollector) pushToSqlCh(mf *pb.MetricFamily, buf *bytes.Buffer) {
	if sql := c.sqlFromMetricFamily(mf, buf); sql != "" {
		c.sql_ch <- sql
	}
}

func (c *metricCollector) shouldFlush(mf *pb.MetricFamily) bool {
	switch mf.GetType() {
	case pb.MetricType_COUNTER, pb.MetricType_GAUGE:
		return len(mf.Metric) > c.opts.metricThreshold
	case pb.MetricType_RAWHIST:
		cnt := 0
		for _, m := range mf.Metric {
			cnt += len(m.RawHist.Samples)
		}
		return cnt > c.opts.sampleThreshold
	default:
		return false
	}
}

func (c *metricCollector) sqlWorker(ctx context.Context, exec ie.InternalExecutor) {
	for {
		select {
		case <-ctx.Done():
			return
		case sql := <-c.sql_ch:
			if err := exec.Exec(sql, ie.NewOptsBuilder().Finish()); err != nil {
				logutil.Errorf("[Metric] insert error. sql: %s; err: %v", sql, err)
			}
		}
	}
}

// sqlFromMetricFamily extracts a insert sql from a MetricFamily and the bytes.Buffer is
// used to mitigate memory allocation
func (c *metricCollector) sqlFromMetricFamily(mf *pb.MetricFamily, buf *bytes.Buffer) string {

	buf.Reset()
	buf.WriteString(fmt.Sprintf("insert into %s.%s values ", METRIC_DB, mf.GetName()))
	lblsBuf := new(bytes.Buffer)
	writeValues := func(t string, v float64, lbls string) {
		buf.WriteString("(")
		buf.WriteString(fmt.Sprintf("%q, %f", t, v))
		buf.WriteString(lbls)
		buf.WriteString("),")
	}

	for _, metric := range mf.Metric {
		for _, lbl := range metric.Label {
			if lbl.GetName() == LBL_NODE { // Node type is int
				lblsBuf.WriteString("," + lbl.GetValue())
				continue
			}
			lblsBuf.WriteString(",\"")
			lblsBuf.WriteString(lbl.GetValue())
			lblsBuf.WriteRune('"')
		}
		lbls := lblsBuf.String()
		lblsBuf.Reset()

		switch mf.GetType() {
		case pb.MetricType_COUNTER:
			time := types.Datetime(metric.GetCollecttime()).String()
			writeValues(time, metric.Counter.GetValue(), lbls)
		case pb.MetricType_GAUGE:
			time := types.Datetime(metric.GetCollecttime()).String()
			writeValues(time, metric.Gauge.GetValue(), lbls)
		case pb.MetricType_RAWHIST:
			for _, sample := range metric.RawHist.Samples {
				time := types.Datetime(sample.GetDatetime()).String()
				writeValues(time, sample.GetValue(), lbls)
			}
		default:
			panic(fmt.Sprintf("unsupported metric type %v", mf.GetType()))
		}
	}
	sql := buf.String()
	// metric has at least one row, so we can remove the tail comma safely
	sql = sql[:len(sql)-1]
	return sql
}

type reminder struct {
	registry map[string]*time.Timer
	C        chan string
}

func newReminder() *reminder {
	return &reminder{
		registry: make(map[string]*time.Timer),
		C:        make(chan string, CHAN_CAPACITY),
	}
}

func (r *reminder) Register(name string, after time.Duration) {
	if r.registry[name] != nil {
		panic(fmt.Sprintf("%s already registered", name))
	}
	r.registry[name] = time.AfterFunc(after, func() { r.C <- name })
}

func (r *reminder) Reset(name string, after time.Duration) {
	if t := r.registry[name]; t != nil {
		t.Reset(after)
	}
}

func (r *reminder) CleanAll() {
	for _, timer := range r.registry {
		timer.Stop()
	}
}
