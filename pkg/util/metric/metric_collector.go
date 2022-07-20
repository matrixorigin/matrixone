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
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/metric"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

const CHAN_CAPACITY = 10000

type MetricCollector interface {
	SendMetrics(context.Context, []*pb.MetricFamily) error
	Start()
	Stop() (<-chan struct{}, bool)
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
		metricThreshold: 1000,
		sampleThreshold: 4096,
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
	mfCh              chan *pb.MetricFamily
	sqlCh             chan string
	stopWg            sync.WaitGroup
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
		sqlCh:     make(chan string, CHAN_CAPACITY),
		mfCh:      make(chan *pb.MetricFamily, CHAN_CAPACITY),
	}
	return c
}

func (c *metricCollector) SendMetrics(ctx context.Context, mfs []*pb.MetricFamily) error {
	for _, mf := range mfs {
		c.mfCh <- mf
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

func (c *metricCollector) Stop() (<-chan struct{}, bool) {
	if atomic.SwapInt32(&c.isRunning, 0) == 0 {
		return nil, false
	}
	c.sqlWorkerCancel()
	c.mergeWorkerCancel()
	stopCh := make(chan struct{})
	go func() { c.stopWg.Wait(); close(stopCh) }()
	return stopCh, true
}

func (c *metricCollector) startSqlWorker() {
	ctx, cancel := context.WithCancel(context.Background())
	c.sqlWorkerCancel = cancel
	for i := 0; i < c.opts.sqlWorkerNum; i++ {
		exec := c.ieFactory()
		exec.ApplySessionOverride(ie.NewOptsBuilder().Database(METRIC_DB).Internal(true).Finish())
		c.stopWg.Add(1)
		go c.sqlWorker(ctx, exec)
	}
}

func (c *metricCollector) startMergeWorker() {
	ctx, cancel := context.WithCancel(context.Background())
	c.mergeWorkerCancel = cancel
	c.stopWg.Add(1)
	go c.mergeWorker(ctx)
}

func (c *metricCollector) mergeWorker(ctx context.Context) {
	defer c.stopWg.Done()
	mfByNames := make(map[string]*mfset)
	sqlbuf := new(bytes.Buffer)
	reminder := newReminder()
	defer reminder.CleanAll()

	doFlush := func(name string, set *mfset) {
		c.pushToSqlCh(set, sqlbuf)
		set.reset()
		reminder.Reset(name, c.opts.flushInterval)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case mf := <-c.mfCh:
			if isFullBatchRawHist(mf) {
				c.pushToSqlCh(newMfset(mf), sqlbuf)
				continue
			}
			name := mf.GetName()
			entryMfs := mfByNames[name]
			if entryMfs != nil {
				entryMfs.add(mf)
			} else {
				entryMfs = newMfset(mf)
				mfByNames[name] = entryMfs
				reminder.Register(name, c.opts.flushInterval)
			}
			if entryMfs.shouldFlush(&c.opts) {
				doFlush(name, entryMfs)
			}
		case name := <-reminder.C:
			if entryMfs := mfByNames[name]; entryMfs != nil && entryMfs.rows > 0 {
				doFlush(name, entryMfs)
			} else {
				reminder.Reset(name, c.opts.flushInterval)
			}
		}
	}
}

func (c *metricCollector) pushToSqlCh(set *mfset, buf *bytes.Buffer) {
	if sql := set.getSql(buf); sql != "" {
		c.sqlCh <- sql
	}
}

func (c *metricCollector) sqlWorker(ctx context.Context, exec ie.InternalExecutor) {
	defer c.stopWg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case sql := <-c.sqlCh:
			if err := exec.Exec(sql, ie.NewOptsBuilder().Finish()); err != nil {
				logutil.Errorf("[Metric] insert error. sql: %s; err: %v", sql, err)
			}
		}
	}
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

type mfset struct {
	mfs  []*pb.MetricFamily
	typ  pb.MetricType
	rows int // how many rows it would take when flushing to db
}

func newMfset(mfs ...*pb.MetricFamily) *mfset {
	set := &mfset{}
	for _, mf := range mfs {
		set.add(mf)
	}
	return set
}

func (s *mfset) add(mf *pb.MetricFamily) {
	if s.typ == mf.GetType() {
		s.typ = mf.GetType()
	}
	switch s.typ {
	case pb.MetricType_COUNTER, pb.MetricType_GAUGE:
		s.rows += len(mf.Metric)
	case pb.MetricType_RAWHIST:
		for _, m := range mf.Metric {
			s.rows += len(m.RawHist.Samples)
		}
	}
	s.mfs = append(s.mfs, mf)
}

func (s *mfset) shouldFlush(opts *collectorOpts) bool {
	switch s.typ {
	case pb.MetricType_COUNTER, pb.MetricType_GAUGE:
		return s.rows > opts.metricThreshold
	case pb.MetricType_RAWHIST:
		return s.rows > opts.sampleThreshold
	default:
		return false
	}
}

func (s *mfset) reset() {
	s.mfs = s.mfs[:0]
	s.typ = pb.MetricType_COUNTER // 0
	s.rows = 0
}

// getSql extracts a insert sql from a set of MetricFamily. the bytes.Buffer is
// used to mitigate memory allocation. getSql assume there is at least one row in mfset
func (s *mfset) getSql(buf *bytes.Buffer) string {
	buf.Reset()
	buf.WriteString(fmt.Sprintf("insert into %s.%s values ", METRIC_DB, s.mfs[0].GetName()))
	lblsBuf := new(bytes.Buffer)
	writeValues := func(t string, v float64, lbls string) {
		buf.WriteString("(")
		buf.WriteString(fmt.Sprintf("%q, %f", t, v))
		buf.WriteString(lbls)
		buf.WriteString("),")
	}
	for _, mf := range s.mfs {
		for _, metric := range mf.Metric {
			// reserved labels
			lblsBuf.WriteString(fmt.Sprintf(",%d,%q", mf.GetNode(), mf.GetRole()))
			// custom labels
			for _, lbl := range metric.Label {
				lblsBuf.WriteString(",\"")
				lblsBuf.WriteString(lbl.GetValue())
				lblsBuf.WriteRune('"')
			}
			lbls := lblsBuf.String()
			lblsBuf.Reset()

			switch mf.GetType() {
			case pb.MetricType_COUNTER:
				time := localTimeStr(metric.GetCollecttime())
				writeValues(time, metric.Counter.GetValue(), lbls)
			case pb.MetricType_GAUGE:
				time := localTimeStr(metric.GetCollecttime())
				writeValues(time, metric.Gauge.GetValue(), lbls)
			case pb.MetricType_RAWHIST:
				for _, sample := range metric.RawHist.Samples {
					time := localTimeStr(sample.GetDatetime())
					writeValues(time, sample.GetValue(), lbls)
				}
			default:
				panic(fmt.Sprintf("unsupported metric type %v", mf.GetType()))
			}
		}
	}
	sql := buf.String()
	// metric has at least one row, so we can remove the tail comma safely
	sql = sql[:len(sql)-1]
	return sql
}

var _, localOffset = time.Now().Zone()

// temp timezone workaround, fix it in 0.6 version
func localTimeStr(time int64) string {
	return types.Datetime((time>>20 + int64(localOffset)) << 20).String()
}
