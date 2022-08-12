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
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/metric"
	bp "github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

const CHAN_CAPACITY = 10000

type MetricCollector interface {
	SendMetrics(context.Context, []*pb.MetricFamily) error
	Start() bool
	Stop(graceful bool) (<-chan struct{}, bool)
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
	*bp.BaseBatchPipe[*pb.MetricFamily, string]
	ieFactory func() ie.InternalExecutor
	opts      collectorOpts
}

func newMetricCollector(factory func() ie.InternalExecutor, opts ...collectorOpt) MetricCollector {
	initOpts := defaultCollectorOpts()
	for _, o := range opts {
		o.ApplyTo(&initOpts)
	}
	c := &metricCollector{
		ieFactory: factory,
		opts:      initOpts,
	}
	base := bp.NewBaseBatchPipe[*pb.MetricFamily, string](c, bp.PipeWithBatchWorkerNum(c.opts.sqlWorkerNum))
	c.BaseBatchPipe = base
	return c
}

func (c *metricCollector) SendMetrics(ctx context.Context, mfs []*pb.MetricFamily) error {
	for _, mf := range mfs {
		if err := c.SendItem(mf); err != nil {
			return err
		}
	}
	return nil
}

func (c *metricCollector) NewItemBatchHandler() func(batch string) {
	exec := c.ieFactory()
	exec.ApplySessionOverride(ie.NewOptsBuilder().Database(metricDBConst).Internal(true).Finish())
	return func(batch string) {
		if err := exec.Exec(batch, ie.NewOptsBuilder().Finish()); err != nil {
			logutil.Errorf("[Trace] insert error. sql: %s; err: %v", batch, err)
		}
	}
}

func (c *metricCollector) NewItemBuffer(_ string) bp.ItemBuffer[*pb.MetricFamily, string] {
	return &mfset{
		Reminder:        bp.NewConstantClock(c.opts.flushInterval),
		metricThreshold: c.opts.metricThreshold,
		sampleThreshold: c.opts.sampleThreshold,
	}
}

type mfset struct {
	bp.Reminder
	mfs             []*pb.MetricFamily
	typ             pb.MetricType
	rows            int // how many buffered rows
	metricThreshold int // haw many rows should be flushed as a batch
	sampleThreshold int // treat rawhist samples differently because it has higher generate rate
}

func (s *mfset) Add(mf *pb.MetricFamily) {
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

func (s *mfset) ShouldFlush() bool {
	switch s.typ {
	case pb.MetricType_COUNTER, pb.MetricType_GAUGE:
		return s.rows > s.metricThreshold
	case pb.MetricType_RAWHIST:
		return s.rows > s.sampleThreshold
	default:
		return false
	}
}

func (s *mfset) Reset() {
	s.mfs = s.mfs[:0]
	s.typ = pb.MetricType_COUNTER // 0
	s.rows = 0
	s.RemindReset()
}

func (s *mfset) IsEmpty() bool {
	return len(s.mfs) == 0
}

// getSql extracts a insert sql from a set of MetricFamily. the bytes.Buffer is
// used to mitigate memory allocation
func (s *mfset) GetBatch(buf *bytes.Buffer) string {
	buf.Reset()
	buf.WriteString(fmt.Sprintf("insert into %s.%s values ", metricDBConst, s.mfs[0].GetName()))
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
