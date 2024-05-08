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

package mometric

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"runtime"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	pb "github.com/matrixorigin/matrixone/pkg/pb/metric"
	bp "github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

const CHAN_CAPACITY = 10000

type MetricCollector interface {
	SendMetrics(context.Context, []*pb.MetricFamily) error
	Start(context.Context) bool
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
	var defaultSqlWorkerNum = int(math.Ceil(float64(runtime.NumCPU()) * 0.1))
	return collectorOpts{
		metricThreshold: 1000,
		sampleThreshold: 4096,
		flushInterval:   15 * time.Second,
		sqlWorkerNum:    defaultSqlWorkerNum,
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

var _ MetricCollector = (*metricCollector)(nil)

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
		if err := c.SendItem(ctx, mf); err != nil {
			return err
		}
	}
	return nil
}

func (c *metricCollector) NewItemBatchHandler(ctx context.Context) func(batch string) {
	exec := c.ieFactory()
	exec.ApplySessionOverride(ie.NewOptsBuilder().Database(MetricDBConst).Internal(true).Finish())
	return func(batch string) {
		if err := exec.Exec(ctx, batch, ie.NewOptsBuilder().Finish()); err != nil {
			logutil.Errorf("[Metric] insert error. sql: %s; err: %v", batch, err)
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

// GetBatch
// getSql extracts a insert sql from a set of MetricFamily. the bytes.Buffer is
// used to mitigate memory allocation
func (s *mfset) GetBatch(ctx context.Context, buf *bytes.Buffer) string {
	buf.Reset()
	buf.WriteString(fmt.Sprintf("insert into %s.%s values ", MetricDBConst, s.mfs[0].GetName()))
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
			lblsBuf.WriteString(fmt.Sprintf(",%q,%q", mf.GetNode(), mf.GetRole()))
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

var _ MetricCollector = (*metricFSCollector)(nil)

type metricFSCollector struct {
	*bp.BaseBatchPipe[*pb.MetricFamily, table.ExportRequests]
	writerFactory table.WriterFactory
	opts          collectorOpts
}

func (c *metricFSCollector) SendMetrics(ctx context.Context, mfs []*pb.MetricFamily) error {
	for _, mf := range mfs {
		if err := c.SendItem(ctx, mf); err != nil {
			return err
		}
	}
	return nil
}

func newMetricFSCollector(writerFactory table.WriterFactory, opts ...collectorOpt) MetricCollector {
	initOpts := defaultCollectorOpts()
	for _, o := range opts {
		o.ApplyTo(&initOpts)
	}
	c := &metricFSCollector{
		writerFactory: writerFactory,
		opts:          initOpts,
	}
	pipeOpts := []bp.BaseBatchPipeOpt{bp.PipeWithBatchWorkerNum(c.opts.sqlWorkerNum),
		bp.PipeWithBufferWorkerNum(1), // only one table
		bp.PipeWithItemNameFormatter(func(bp.HasName) string {
			return SingleMetricTable.GetName()
		}),
	}
	base := bp.NewBaseBatchPipe[*pb.MetricFamily, table.ExportRequests](c, pipeOpts...)
	c.BaseBatchPipe = base
	return c
}

func (c *metricFSCollector) NewItemBatchHandler(ctx context.Context) func(batch table.ExportRequests) {
	return func(batchs table.ExportRequests) {
		for _, batch := range batchs {
			if _, err := batch.Handle(); err != nil {
				logutil.Errorf("[Metric] failed to write, err: %v", err)
			}
		}
	}
}

func (c *metricFSCollector) NewItemBuffer(_ string) bp.ItemBuffer[*pb.MetricFamily, table.ExportRequests] {
	return &mfsetETL{
		mfset: mfset{
			Reminder:        bp.NewConstantClock(c.opts.flushInterval),
			metricThreshold: c.opts.metricThreshold,
			sampleThreshold: c.opts.sampleThreshold,
		},
		collector: c,
	}
}

type mfsetETL struct {
	mfset
	collector *metricFSCollector
}

// GetBatch implements table.Table.GetBatch.
// Write metric into two tables: one for metric table, another for sql_statement_cu table.
func (s *mfsetETL) GetBatch(ctx context.Context, buf *bytes.Buffer) table.ExportRequests {
	buf.Reset()

	ts := time.Now()
	buffer := make(map[string]table.RowWriter, 2)
	writeValues := func(row *table.Row) error {
		w, exist := buffer[row.Table.GetName()]
		if !exist {
			w = s.collector.writerFactory.GetRowWriter(ctx, row.GetAccount(), row.Table, ts)
			buffer[row.Table.GetName()] = w
		}
		if err := w.WriteRow(row); err != nil {
			return err
		}
		return nil
	}
	rows := make(map[string]*table.Row, 2)
	defer func() {
		for _, r := range rows {
			r.Free()
		}
	}()
	getRow := func(metricName string) *table.Row {
		tbl := SingleMetricTable
		if metricName == SqlStatementCUTable.GetName() {
			tbl = SqlStatementCUTable
		}
		row, exist := rows[tbl.GetName()]
		if !exist {
			row = tbl.GetRow(ctx)
			rows[tbl.GetName()] = row
		}
		return row
	}

	for _, mf := range s.mfs {
		for _, metric := range mf.Metric {
			// reserved labels
			row := getRow(mf.GetName())
			row.Reset()
			// table `metric` NEED column `metric_name`
			// table `sql_statement_cu` NO column `metric_name`
			if row.Table.GetName() == SingleMetricTable.GetName() {
				row.SetColumnVal(metricNameColumn, table.StringField(mf.GetName()))
			}
			row.SetColumnVal(metricNodeColumn, table.StringField(mf.GetNode()))
			row.SetColumnVal(metricRoleColumn, table.StringField(mf.GetRole()))
			// custom labels
			for _, lbl := range metric.Label {
				row.SetVal(lbl.GetName(), table.StringField(lbl.GetValue()))
			}

			switch mf.GetType() {
			case pb.MetricType_COUNTER:
				time := localTime(metric.GetCollecttime())
				row.SetColumnVal(metricCollectTimeColumn, table.TimeField(time))
				row.SetColumnVal(metricValueColumn, table.Float64Field(metric.Counter.GetValue()))
				_ = writeValues(row)
			case pb.MetricType_GAUGE:
				time := localTime(metric.GetCollecttime())
				row.SetColumnVal(metricCollectTimeColumn, table.TimeField(time))
				row.SetColumnVal(metricValueColumn, table.Float64Field(metric.Gauge.GetValue()))
				_ = writeValues(row)
			case pb.MetricType_RAWHIST:
				for _, sample := range metric.RawHist.Samples {
					time := localTime(sample.GetDatetime())
					row.SetColumnVal(metricCollectTimeColumn, table.TimeField(time))
					row.SetColumnVal(metricValueColumn, table.Float64Field(sample.GetValue()))
					_ = writeValues(row)
				}
			default:
				panic(moerr.NewInternalError(ctx, "unsupported metric type %v", mf.GetType()))
			}
		}
	}

	reqs := make([]table.WriteRequest, 0, len(buffer))
	for _, w := range buffer {
		reqs = append(reqs, table.NewRowRequest(w))
	}

	return reqs
}

func localTime(value int64) time.Time {
	return time.UnixMicro(value).In(time.Local)
}

func localTimeStr(value int64) string {
	return time.UnixMicro(value).In(time.Local).Format("2006-01-02 15:04:05.000000")
}
