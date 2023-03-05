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
	"regexp"
	"testing"
	"time"

	pb "github.com/matrixorigin/matrixone/pkg/pb/metric"
	"github.com/matrixorigin/matrixone/pkg/util/export/etl"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

func init() {
	m1 := &SubSystem{"m1", "", false}
	m2 := &SubSystem{"m2", "", false}
	AllSubSystem[m1.Name] = m1
	AllSubSystem[m2.Name] = m2
}

type dummySqlExecutor struct {
	opts ie.SessionOverrideOptions
	ch   chan<- string
}

func (e *dummySqlExecutor) ApplySessionOverride(opts ie.SessionOverrideOptions) {
	e.opts = opts
}

func (e *dummySqlExecutor) Exec(ctx context.Context, sql string, opts ie.SessionOverrideOptions) error {
	select {
	case e.ch <- sql:
	default:
	}
	return nil
}

func (e *dummySqlExecutor) Query(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
	return nil
}

func newExecutorFactory(sqlch chan string) func() ie.InternalExecutor {
	return func() ie.InternalExecutor {
		return &dummySqlExecutor{
			opts: ie.NewOptsBuilder().Finish(),
			ch:   sqlch,
		}
	}
}

func TestCollectorOpts(t *testing.T) {
	c := newMetricCollector(
		nil, // this nil pointer won't be touched when SqlWorkerNum is set to 0
		WithFlushInterval(time.Second),
		WithMetricThreshold(3),
		WithSampleThreshold(10),
		WithSqlWorkerNum(0),
	).(*metricCollector)
	o := c.opts
	if o.flushInterval != time.Second || o.metricThreshold != 3 || o.sampleThreshold != 10 {
		t.Errorf("collectorOpts doesn't apply correctly")
	}
}

func TestCollector(t *testing.T) {
	sqlch := make(chan string, 100)
	factory := newExecutorFactory(sqlch)
	collector := newMetricCollector(factory, WithFlushInterval(200*time.Millisecond), WithMetricThreshold(2))
	collector.Start(context.TODO())
	defer collector.Stop(false)
	names := []string{"m1", "m2"}
	nodes := []string{"e669d136-24f3-11ed-ba8c-d6aee46d73fa", "e9b89520-24f3-11ed-ba8c-d6aee46d73fa"}
	roles := []string{"ping", "pong"}
	ts := time.Now().UnixMicro()
	go func() {
		_ = collector.SendMetrics(context.TODO(), []*pb.MetricFamily{
			{Name: names[0], Type: pb.MetricType_COUNTER, Node: nodes[0], Role: roles[0], Metric: []*pb.Metric{
				{
					Counter: &pb.Counter{Value: 12.0}, Collecttime: ts,
				},
			}},
			{Name: names[1], Type: pb.MetricType_RAWHIST, Metric: []*pb.Metric{
				{
					Label:   []*pb.LabelPair{{Name: "type", Value: "select"}, {Name: "account", Value: "user"}},
					RawHist: &pb.RawHist{Samples: []*pb.Sample{{Datetime: ts, Value: 12.0}, {Datetime: ts, Value: 12.0}}},
				},
			}},
		})

		_ = collector.SendMetrics(context.TODO(), []*pb.MetricFamily{
			{Name: names[0], Type: pb.MetricType_COUNTER, Node: nodes[1], Role: roles[1], Metric: []*pb.Metric{
				{
					Counter: &pb.Counter{Value: 21.0}, Collecttime: ts,
				},
				{
					Counter: &pb.Counter{Value: 66.0}, Collecttime: ts,
				},
			}},
		})
	}()
	instant := time.Now()
	valuesRe := regexp.MustCompile(`\([^)]*\),?\s?`) // find pattern like (1,2,3)
	nameRe := regexp.MustCompile(`\.(\w+)\svalues`)  // find table name
	nameAndValueCnt := func(s string) (name string, cnt int) {
		cnt = len(valuesRe.FindAllString(s, -1))
		matches := nameRe.FindStringSubmatch(s)
		if len(matches) > 1 {
			name = matches[1]
		} else {
			name = "<nil>"
		}
		return name, cnt
	}

	name, cnt := nameAndValueCnt(<-sqlch)
	if name != names[0] || cnt != 3 {
		t.Errorf("m1 metric should be flushed first with 3 rows, got %s with %d rows", name, cnt)
	}

	sql := <-sqlch
	if time.Since(instant) < 200*time.Millisecond {
		t.Errorf("m2 should be flushed after a period")
	}
	name, cnt = nameAndValueCnt(sql)
	if name != names[1] || cnt != 2 {
		t.Errorf("m2 metric should be flushed first with 2 rows, got %s with %d rows", name, cnt)
	}
}

type dummyStringWriter struct {
	name string
	ch   chan string
	// csvWriter
	writer table.RowWriter
}

func (w *dummyStringWriter) WriteString(s string) (n int, err error) {
	n = len(s)
	w.ch <- w.name
	w.ch <- s
	return n, nil
}

func (w *dummyStringWriter) WriteRow(row *table.Row) error {
	return w.writer.WriteRow(row)
}

func (w *dummyStringWriter) FlushAndClose() (int, error) {
	return w.writer.FlushAndClose()
}

func (w *dummyStringWriter) GetContent() string { return "" }

func newDummyFSWriterFactory(csvCh chan string) table.WriterFactory {
	return table.WriterFactory(func(_ context.Context, account string, tbl *table.Table, ts time.Time) table.RowWriter {
		w := &dummyStringWriter{name: tbl.Table, ch: csvCh}
		w.writer = etl.NewCSVWriter(context.TODO(), bytes.NewBuffer(nil), w)
		return w
	})
}

func dummyInitView(ctx context.Context, tbls []string) {
	for _, tbl := range tbls {
		GetMetricViewWithLabels(ctx, tbl, []string{metricTypeColumn.Name, metricAccountColumn.Name})
	}
}

func TestFSCollector(t *testing.T) {
	ctx := context.Background()
	csvCh := make(chan string, 100)
	factory := newDummyFSWriterFactory(csvCh)
	collector := newMetricFSCollector(factory, WithFlushInterval(3*time.Second), WithMetricThreshold(4), ExportMultiTable(false))
	collector.Start(context.TODO())
	defer collector.Stop(false)
	names := []string{"m1", "m2"}
	nodes := []string{"e669d136-24f3-11ed-ba8c-d6aee46d73fa", "e9b89520-24f3-11ed-ba8c-d6aee46d73fa"}
	roles := []string{"ping", "pong"}
	ts := time.Now().UnixMicro()
	dummyInitView(ctx, names)
	go func() {
		_ = collector.SendMetrics(context.TODO(), []*pb.MetricFamily{
			{Name: names[0], Type: pb.MetricType_COUNTER, Node: nodes[0], Role: roles[0], Metric: []*pb.Metric{
				{
					Label:   []*pb.LabelPair{{Name: "account", Value: "user"}},
					Counter: &pb.Counter{Value: 12.0}, Collecttime: ts,
				},
			}},
			{Name: names[1], Type: pb.MetricType_RAWHIST, Metric: []*pb.Metric{
				{
					Label:   []*pb.LabelPair{{Name: "type", Value: "select"}, {Name: "account", Value: "user"}},
					RawHist: &pb.RawHist{Samples: []*pb.Sample{{Datetime: ts, Value: 12.0}, {Datetime: ts, Value: 12.0}}},
				},
			}},
		})

		_ = collector.SendMetrics(context.TODO(), []*pb.MetricFamily{
			{Name: names[0], Type: pb.MetricType_COUNTER, Node: nodes[1], Role: roles[1], Metric: []*pb.Metric{
				{
					Label:   []*pb.LabelPair{{Name: "account", Value: "user"}},
					Counter: &pb.Counter{Value: 21.0}, Collecttime: ts,
				},
				{
					Label:   []*pb.LabelPair{{Name: "account", Value: "user"}},
					Counter: &pb.Counter{Value: 66.0}, Collecttime: ts,
				},
			}},
		})
	}()
	M1ValuesRe := regexp.MustCompile(`m1,(.*[,]?)+\n`) // find pattern like m1,...,...,...\n
	M2ValuesRe := regexp.MustCompile(`m2,(.*[,]?)+\n`) // find pattern like m2,...,...,...\n
	nameAndValueCnt := func(n, s string, re *regexp.Regexp) (name string, cnt int) {
		t.Logf("name: %s, csv: %s", n, s)
		cnt = len(re.FindAllString(s, -1))
		if cnt > 0 {
			name = n
		} else {
			name = "<nil>"
		}
		return name, cnt
	}

	n, s := <-csvCh, <-csvCh
	name, cnt := nameAndValueCnt(n, s, M1ValuesRe)
	if name != SingleMetricTable.GetName() || cnt != 3 {
		t.Errorf("m1 metric should be flushed first with 3 rows, got %s with %d rows", name, cnt)
	}

	name, cnt = nameAndValueCnt(n, s, M2ValuesRe)
	if name != SingleMetricTable.GetName() || cnt != 2 {
		t.Errorf("m2 metric should be flushed first with 2 rows, got %s with %d rows", name, cnt)
	}
}
